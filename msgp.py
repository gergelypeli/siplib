from __future__ import print_function, unicode_literals
import socket
import datetime
import json
from async import WeakMethod


class Message(object):
    def __init__(self, target, body, response_handler, ack_timeout, response_timeout):
        now = datetime.datetime.now()

        self.target = target
        self.body = body
        self.response_handler = response_handler
        self.retransmit_deadline = now
        self.ack_deadline = now + ack_timeout
        self.response_deadline = now + response_timeout if response_timeout else None


class Stream(object):
    def __init__(self):
        self.is_unopened = False
        self.is_closed = False
        self.last_sent_seq = 0
        self.last_received_seq = 0
        self.sent_messages_by_seq = {}
        self.pending_ack_seqs = set()
        
        
def prid(sid, seq=None):
    addr, label = sid
    host, port = addr
    
    return "%s:%d@%s%s" % (host, port, label, "#%d" % seq if seq is not None else "")


class Msgp(object):
    # Legend:
    # addr = (host, port)
    # sid = (addr, label)
    
    DEFAULT_ACK_TIMEOUT = datetime.timedelta(milliseconds=500)
    DEFAULT_RESPONSE_TIMEOUT = datetime.timedelta(milliseconds=2000)
    DEFAULT_RETRANSMIT_INTERVAL = datetime.timedelta(milliseconds=100)
    DEFAULT_CLEANUP_INTERVAL = DEFAULT_RETRANSMIT_INTERVAL
    
    
    def __init__(self, metapoll, local_addr, request_handler,
        ack_timeout=None, response_timeout=None, retransmit_interval=None, cleanup_interval=None):
        self.metapoll = metapoll
        self.local_addr = local_addr
        self.streams_by_id = {}
        self.request_handler = request_handler
        
        self.ack_timeout = ack_timeout or self.DEFAULT_ACK_TIMEOUT
        self.response_timeout = response_timeout or self.DEFAULT_RESPONSE_TIMEOUT
        self.retransmit_interval = retransmit_interval or self.DEFAULT_RETRANSMIT_INTERVAL
        self.cleanup_interval = cleanup_interval or self.DEFAULT_CLEANUP_INTERVAL
        
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(self.local_addr)
        
        self.metapoll.register_reader(self.socket, WeakMethod(self.recved))
        self.metapoll.register_timeout(self.cleanup_interval, WeakMethod(self.cleanup), repeat=True)


    def encode_body(self, body):
        return body
        
        
    def decode_body(self, body):
        return body
        
        
    def get_stream(self, sid):
        return self.streams_by_id.get(sid)
        

    def transmit_message(self, sid, seq):
        s = self.get_stream(sid)
        if not s:
            return
        
        msg = s.sent_messages_by_seq[seq]
        raddr, label = sid
        
        try:
            int(msg.target)
            what = "response"
        except ValueError:
            what = "request"
        
        print("MSGP transmitting %s %s" % (what, prid(sid, seq)))
        header = b"@%s #%d ^%s" % (label, seq, msg.target)
        
        if msg.body is not None:
            header += " +%d" % len(msg.body)
            packet = header + "\n" + msg.body + "\n"
        else:
            packet = header + "\n"

        self.socket.sendto(packet, raddr)
        msg.retransmit_deadline += self.retransmit_interval


    def transmit_ack(self, sid, seq):
        raddr, label = sid
        
        print("MSGP transmitting ack %s" % prid(sid, seq))
        packet = b"@%s ^%s\n" % (label, seq)

        self.socket.sendto(packet, raddr)


    def transmit_nope(self, sid):
        raddr, label = sid
        
        print("MSGP transmitting nope %s" % prid(sid, seq))
        packet = b"@%s ^%s\n" % (label, "nope")

        self.socket.sendto(packet, raddr)


    def add_stream(self, sid):
        s = self.get_stream(sid)
        if s:
            if s.is_unopened:
                print("Accepting stream %s" % prid(sid))
                s.is_unopened = False
            else:
                raise Error("Stream already added!")
        else:
            self.streams_by_id[sid] = Stream()


    def remove_stream(self, sid):
        s = self.get_stream(sid)
        
        if not s:
            return
            
        if not s.sent_messages_by_seq:
            print("MSGP removing closed stream %s" % prid(sid))
            self.streams_by_id.pop(sid)

        # Get rid of it once our requests are replied
        print("MSGP will remove closed stream %s" % prid(sid))
        s.is_closed = True
        
        
    def send_message(self, sid, target, body, response_handler=None, ack_timeout=None, response_timeout=None):
        s = self.get_stream(sid)
        if not s:
            raise Error("No such stream!")
        elif s.is_closed:
            raise Error("Stream is closed!")
        elif s.is_unopened:
            raise Error("Stream not opened!")
        
        msg = Message(
            target, self.encode_body(body), response_handler,
            ack_timeout or self.ack_timeout,
            response_timeout or self.response_timeout
        )
        
        s.last_sent_seq += 1
        seq = s.last_sent_seq
        
        s.sent_messages_by_seq[seq] = msg
        self.transmit_message(sid, seq)
        
        try:
            tseq = int(target)
        except ValueError:
            pass
        else:
            # Don't send explicit ACKs as this response suffices
            s.pending_ack_seqs.remove(tseq)
            
        
    def process_message(self, raddr, label, seq, target, body):
        sid = (raddr, label)

        try:
            tseq = int(target)
        except ValueError:
            tseq = None
        
        s = self.get_stream(sid)
        if not s:
            if seq == 1 and tseq is None:
                self.add_stream(sid)
                s = self.get_stream(sid)
                s.is_unopened = True  # will be removed unless confirmed
            else:
                print("MSGP message for unknown stream: %s!" % prid(sid))
                self.transmit_nope(sid)
                return

        if tseq is not None:
            # First ACK a sent message
            msg = s.sent_messages_by_seq.get(tseq)
            
            if msg and msg.ack_deadline:
                if seq is None:
                    print("MSGP got ack for %s" % prid(sid, tseq))
                    
                msg.ack_deadline = None
                msg.retransmit_deadline = None
            
        if seq is not None:
            # Then process the incoming one
            
            if seq <= s.last_received_seq:
                self.transmit_ack(sid, seq)
                return
                
            if seq > s.last_received_seq + 1:
                print("MSGP out of order: %d >> %d!" % (seq, s.last_received_seq))
                return  # TODO: cache and ack!

            s.last_received_seq = seq

            # We don't need an explicit ACK if a response is sent immediately
            s.pending_ack_seqs.add(seq)
                
            if tseq is not None:
                # Process as a response
                msg = s.sent_messages_by_seq.pop(tseq)
                
                if msg:
                    print("MSGP got response for %s" % prid(sid, tseq))
                    if msg.response_handler:
                        msg.response_handler(sid, seq, self.decode_body(body))
            else:
                # Process as a request
                self.request_handler(sid, seq, self.decode_body(body), target)
                
            if s.is_unopened:
                # Stream still not confirmed, reject
                print("MSGP removing rejected stream %s" % prid(sid))
                self.streams_by_id.pop(sid)
                self.transmit_nope(sid)
            elif seq in s.pending_ack_seqs:
                s.pending_ack_seqs.remove(seq)
                print("MSGP needs explicit ACK %s" % prid(sid, seq))
                self.transmit_ack(sid, seq)
        elif tseq is None:
            # Or the service message
            
            if target == "nope":
                print("Stream rejected by peer %s" % prid(sid))
                self.remove_stream(sid)
            else:
                print("Unknown service message %s from %s" % (target, prid(sid)))


    def recved(self):
        rest, raddr = self.socket.recvfrom(65535)
        
        while rest:
            header, rest = rest.split("\n", 1)
            label, seq, target, length = None, None, None, None
            
            for field in header.split(" "):
                key, value = field[0], field[1:]
                
                if key == "@":
                    label = value
                elif key == "#":
                    seq = int(value)
                elif key == "^":
                    target = value
                elif key == "+":
                    length = int(value)
                else:
                    print("MSGP bad message header field: %r!" % field)
                    
            if length is not None:
                if len(rest) < length + 1:
                    print("MSGP bad message length: %d!" % length)
                    return
                
                if rest[length] != "\n":
                    print("MSGP bad message body: %d!" % length)
                    return
            
                body = rest[:length]
                rest = rest[length + 1:]
            else:
                body = None
            
            hb = (header, body + "\n" if body is not None else "")
            print("MSGP IN from %s:%d to %s:%d\n%s\n%s" % (raddr + self.local_addr + hb))
            self.process_message(raddr, label, seq, target, body)
        
        
    def cleanup(self):
        now = datetime.datetime.now()
        
        for sid, s in self.streams_by_id.items():
            for seq, msg in s.sent_messages_by_seq.items():
                if msg.ack_deadline and msg.ack_deadline < now:
                    print("MSGP gave up on ACK for message %s" % prid(sid, seq))
                    
                    if msg.response_handler:
                        msg.response_handler(sid, None, None)
                        
                    s.sent_messages_by_seq.pop(seq)
                elif msg.response_deadline and msg.response_deadline < now:
                    if not s.is_closed:
                        print("MSGP gave up on response for message %s" % prid(sid, seq))
                        
                    if msg.response_handler:
                        msg.response_handler(sid, None, None)
                        
                    s.sent_messages_by_seq.pop(seq)
                elif msg.retransmit_deadline and msg.retransmit_deadline < now:
                    self.transmit_message(sid, seq)
                    
            if s.is_closed and not s.sent_messages_by_seq:
                print("MSGP finally removing closed stream %s" % prid(sid))
                self.streams_by_id.pop(sid)


class JsonMsgp(Msgp):
    def encode_body(self, params):
        return json.dumps(params)
        
        
    def decode_body(self, body):
        return json.loads(body)
