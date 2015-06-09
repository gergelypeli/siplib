from __future__ import print_function, unicode_literals
import socket
import datetime
import re
from async import WeakMethod

class Error(Exception): pass


class Bunch(object):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class Request(Bunch): pass
class Response(Bunch): pass
class Stream(Bunch): pass


class Msgp(object):
    DEFAULT_ACK_TIMEOUT = datetime.timedelta(milliseconds=500)
    DEFAULT_RESPONSE_TIMEOUT = datetime.timedelta(milliseconds=5000)
    DEFAULT_RETRANSMIT_INTERVAL = datetime.timedelta(milliseconds=100)
    DEFAULT_CLEANUP_INTERVAL = DEFAULT_RETRANSMIT_INTERVAL
    DEFAULT_LINGER_TIMEOUT = DEFAULT_RESPONSE_TIMEOUT
    
    def __init__(self, metapoll, local_addr, request_handler):
        self.metapoll = metapoll
        self.local_addr = local_addr
        self.streams_by_id = {}
        self.request_handler = request_handler
        
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(self.local_addr)
        
        self.metapoll.register_reader(self.socket, WeakMethod(self.recv))
        self.metapoll.register_timeout(self.DEFAULT_CLEANUP_INTERVAL, WeakMethod(self.cleanup))


    def get_stream(self, sid):
        s = self.streams_by_id.get(sid, None)
        
        if not s:
            s = Stream(
                last_sent = 0,
                last_received = 0,
                requests_by_seq = {},
                responses_by_seq = {}
            )
            self.streams_by_id[sid] = s
            
        return s
        
        
    def send_request(self, sid, message, callback=None, need_response=False):
        now = datetime.datetime.now()
        s = self.get_stream(sid)
        
        r = Request(
            message=message,
            callback=callback,
            need_response=need_response,
            ack_deadline=now + self.DEFAULT_ACK_TIMEOUT,
            response_deadline=now + self.DEFAULT_RESPONSE_TIMEOUT if need_response else None,
            retransmit_deadline=now
        )
        
        s.last_sent += 1
        seq = s.last_sent
        mid = (sid, seq)
        
        s.requests_by_seq[seq] = r
        self.retransmit_request(mid)


    def send_response(self, mid, message):
        now = datetime.datetime.now()
        sid, seq = mid
        s = self.get_stream(sid)
        
        r = Response(
            message=message,
            linger_deadline=now + self.DEFAULT_LINGER_TIMEOUT
        )
        
        s.responses_by_seq[seq] = r
        self.retransmit_response(mid)
    
    
    def retransmit_request(self, mid):
        sid, seq = mid
        s = self.get_stream(sid)
        r = s.requests_by_seq[seq]
        raddr, none = sid
        
        print("Transmitting request %r" % (mid,))
        packet = b"!%s#%d %s" % ("request", seq, r.message)
        self.socket.sendto(packet, raddr)
        r.retransmit_deadline += self.DEFAULT_RETRANSMIT_INTERVAL


    def retransmit_response(self, mid):
        sid, seq = mid
        s = self.get_stream(sid)
        r = s.responses_by_seq.get(seq)
        raddr, none = sid
        
        if r:
            print("Transmitting response %r" % (mid,))
            packet = b"!%s#%d %s" % ("response", seq, r.message)
        else:
            print("Transmitting ack %r" % (mid,))
            packet = b"!%s#%d" % ("ack", seq)
            
        self.socket.sendto(packet, raddr)

        
    def recv(self):
        packet, raddr = self.socket.recvfrom(65535)
        m = re.search(r"^!(\w+)#(\d+)( (.*))?", packet)
        if not m:
            print("Bad message: %r!" % packet[:32])
            return
            
        type = m.group(1)
        seq = int(m.group(2))
        message = m.group(4)
        sid = (raddr, None)
        mid = (sid, seq)
        s = self.get_stream(sid)

        if type == "request":
            if seq <= s.last_received:
                self.retransmit_response(mid)
            elif seq > s.last_received + 1:
                print("Out of order: %d >> %d!" % (seq, s.last_received))
                return  # TODO: cache and ack!
            else:
                s.last_received = seq
                mid = (sid, seq)
                self.request_handler(mid, message)
                if seq not in s.responses_by_seq:
                    self.retransmit_response(mid)  # at least an ack
        elif type == "ack":
            r = s.requests_by_seq.get(seq, None)
            if r:
                print("Got ack for %d" % seq)
                if r.need_response:
                    r.ack_deadline = None
                    r.retransmit_deadline = None
                else:
                    if r.callback:
                        r.callback(True)
                    s.requests_by_seq.pop(seq)
        elif type == "response":
            r = s.requests_by_seq.pop(seq, None)
            if r:
                print("Got response for %d" % seq)
                if r.callback:
                    r.callback(message if r.need_response else True)
        else:
            print("Invalid packet type %r!" % type)

        
    def cleanup(self):
        now = datetime.datetime.now()
        
        for sid, s in self.streams_by_id.items():
            expired_seqs = []
        
            for seq, r in s.requests_by_seq.items():
                if r.ack_deadline and r.ack_deadline < now:
                    expired_seqs.append(seq)
                elif r.response_deadline and r.response_deadline < now:
                    expired_seqs.append(seq)
                elif r.retransmit_deadline and r.retransmit_deadline < now:
                    mid = (sid, seq)
                    self.retransmit_request(mid)
        
            for seq in expired_seqs:
                r = s.requests_by_seq.pop(seq)
                if r.callback:
                    r.callback(None if r.need_response else False)
                mid = (sid, seq)
                print("Cleaned up request %r" % (mid,))

            expired_seqs = []
        
            for seq, r in s.responses_by_seq.items():
                if r.linger_deadline < now:
                    expired_seqs.append(seq)
        
            for seq in expired_seqs:
                s.responses_by_seq.pop(seq)
                mid = (sid, seq)
                print("Cleaned up response %r" % (mid,))
