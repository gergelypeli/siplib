from __future__ import print_function, unicode_literals
import socket
import datetime
from async import WeakMethod


class Stream(object):
    def __init__(self):
        self.last_sent_seq = 0
        self.last_received_seq = 0
        self.sent_messages_by_seq = {}
        self.pending_ack_seqs = set()
        

class Message(object):
    def __init__(self, target, body, callback, ack_timeout, response_timeout):
        now = datetime.datetime.now()

        self.target = target
        self.body = body
        self.callback = callback
        self.retransmit_deadline = now
        self.ack_deadline = now + ack_timeout
        self.response_deadline = now + response_timeout if response_timeout else None


class Msgp(object):
    # Legend:
    # addr = (host, port)
    # sid = (addr, label)
    # mid = (sid, seqno)
    
    DEFAULT_ACK_TIMEOUT = datetime.timedelta(milliseconds=500)
    DEFAULT_RESPONSE_TIMEOUT = datetime.timedelta(milliseconds=5000)
    DEFAULT_RETRANSMIT_INTERVAL = datetime.timedelta(milliseconds=100)
    DEFAULT_CLEANUP_INTERVAL = DEFAULT_RETRANSMIT_INTERVAL
    
    
    def __init__(self, metapoll, local_addr, message_handler,
        ack_timeout=None, response_timeout=None, retransmit_interval=None, cleanup_interval=None):
        self.metapoll = metapoll
        self.local_addr = local_addr
        self.streams_by_id = {}
        self.message_handler = message_handler
        
        self.ack_timeout = ack_timeout or self.DEFAULT_ACK_TIMEOUT
        self.response_timeout = response_timeout or self.DEFAULT_RESPONSE_TIMEOUT
        self.retransmit_interval = retransmit_interval or self.DEFAULT_RETRANSMIT_INTERVAL
        self.cleanup_interval = cleanup_interval or self.DEFAULT_CLEANUP_INTERVAL
        
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(self.local_addr)
        
        self.metapoll.register_reader(self.socket, WeakMethod(self.recved))
        self.metapoll.register_timeout(self.cleanup_interval, WeakMethod(self.cleanup))


    def get_stream(self, sid, create=False):
        s = self.streams_by_id.get(sid)
        
        if not s and create:
            s = Stream()
            self.streams_by_id[sid] = s
            
        return s
        
        
    def transmit_message(self, mid):
        sid, seq = mid
        s = self.get_stream(sid)
        if not s:
            return
        
        msg = s.sent_messages_by_seq[seq]
        raddr, label = sid
        
        print("Transmitting request %r" % (mid,))
        header = b"@%s #%d ^%s" % (label, seq, msg.target)
        
        if msg.body is not None:
            header += " +%d" % len(msg.body)
            packet = header + "\n" + msg.body + "\n"
        else:
            packet = header + "\n"

        self.socket.sendto(packet, raddr)
        msg.retransmit_deadline += self.retransmit_interval


    def transmit_ack(self, mid):
        sid, seq = mid
        raddr, label = sid
        
        print("Transmitting ack %r" % (mid,))
        packet = b"@%s ^%s\n" % (label, seq)
            
        self.socket.sendto(packet, raddr)
        
        
    def send_message(self, sid, target, body, callback=None, ack_timeout=None, response_timeout=None):
        s = self.get_stream(sid, create=True)
        
        msg = Message(
            target, body, callback,
            ack_timeout or self.ack_timeout,
            response_timeout or self.response_timeout
        )
        
        s.last_sent_seq += 1
        seq = s.last_sent_seq
        mid = (sid, seq)
        
        s.sent_messages_by_seq[seq] = msg
        self.transmit_message(mid)
        
        if target.isdigit():
            # Don't send explicit ACKs as this response suffices
            s.pending_ack_seqs.remove(int(target))
            
        
    def process_message(self, raddr, label, seq, target, body):
        sid = (raddr, label)
        mid = (sid, seq) if seq is not None else None

        tseq = int(target) if target.isdigit() else None
        tmid = (sid, tseq) if tseq is not None else None
        
        s = self.get_stream(sid, create=(seq == 0))
        if not s:
            print("Message for unknown stream: %r!" % sid)
            return

        if tmid:
            # First ACK a sent message
            msg = s.sent_messages_by_seq.get(tseq)
            
            if msg and msg.ack_deadline:
                print("Got ack for %r" % (tmid,))
                msg.ack_deadline = None
                msg.retransmit_deadline = None
            
        if mid:
            # Then process the incoming one
            
            if seq <= s.last_received_seq:
                self.transmit_ack(mid)
                return
                
            if seq > s.last_received_seq + 1:
                print("Out of order: %d >> %d!" % (seq, s.last_received_seq))
                return  # TODO: cache and ack!

            # We don't need an explicit ACK if a response is sent immediately
            s.pending_ack_seqs.add(seq)
                
            if tmid:
                # Process as a response
                msg = s.sent_messages_by_seq.pop(tseq)
                
                if msg:
                    print("Got response for %r" % (tmid,))
                    if msg.callback:
                        msg.callback(seq, body)
            else:
                # Process as a request
                self.message_handler(sid, seq, target, body)
                
            if seq in s.pending_ack_seqs:
                s.pending_ack_seqs.remove(seq)
                self.transmit_ack(mid)


    def recved(self):
        rest, raddr = self.socket.recvfrom(65535)
        
        while rest:
            header, rest = packet.split("\n", 1)
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
                    print("Bad message header field: %r!" % field)
                    
            if length is not None:
                if len(rest) < length + 1:
                    print("Bad message length: %d!" % length)
                    return
                
                if rest[length] != "\n":
                    print("Bad message body: %d!" % length)
                    return
            
                body = rest[:length]
                rest = rest[length + 1:]
            else:
                body = None
            
            self.process_message(raddr, label, seq, target, body)
        
        
    def cleanup(self):
        now = datetime.datetime.now()
        
        for sid, s in self.streams_by_id.items():
            for seq, msg in s.sent_messages_by_seq.items():
                mid = (sid, seq)
                
                if msg.ack_deadline and msg.ack_deadline < now:
                    print("Gave up on ACK for message %r" % (mid,))
                    if msg.callback:
                        msg.callback(None, None)
                        
                    s.sent_messages_by_seq.pop(seq)
                elif msg.response_deadline and msg.response_deadline < now:
                    print("Gave up on response for message %r" % (mid,))
                    if msg.callback:
                        msg.callback(None, None)
                        
                    s.sent_messages_by_seq.pop(seq)
                elif msg.retransmit_deadline and msg.retransmit_deadline < now:
                    self.transmit_message(mid)


class JsonMsgp(object):
    def __init__(self, metapoll, local_addr, message_handler):
        self.msgp = Msgp(metapoll, local_addr, WeakMethod(self.process_message))
        self.message_handler = message_handler
        
        
    def send_message(self, sid, target, params, callback):
        body = json.dumps(params)
        
        if callback:
            callback = lambda s, b: callback(s, json.loads(b) if b else None)
            
        self.msgp.send_message(sid, target, body, callback)
        
        
    def process_message(self, sid, seq, target, body):
        params = json.loads(body) if body is not None else None
        self.message_handler(sid, seq, target, params)
