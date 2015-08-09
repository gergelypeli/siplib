from __future__ import print_function, unicode_literals
import socket
import datetime
import json
from async import WeakMethod


class Error(Exception): pass


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
        self.is_unconfirmed = False
        self.is_closed = False
        self.last_sent_seq = 0
        self.last_received_seq = 0
        self.sent_messages_by_seq = {}
        self.pending_ack_seqs = set()
        self.request_handler = None
        
        
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
        

    def transmit_message(self, sid, sseq):
        s = self.get_stream(sid)
        if not s:
            return
        
        msg = s.sent_messages_by_seq[sseq]
        raddr, label = sid
        
        what = "response" if msg.target.isdigit() else "request"
        print("MSGP transmitting %s %s" % (what, prid(sid, sseq)))

        header = b"@%s #%d ^%s" % (label, sseq, msg.target)
        
        if msg.body is not None:
            header += " +%d" % len(msg.body)
            packet = header + "\n" + msg.body + "\n"
        else:
            packet = header + "\n"

        self.socket.sendto(packet, raddr)
        msg.retransmit_deadline += self.retransmit_interval


    def transmit_ack(self, sid, sseq):
        raddr, label = sid
        
        print("MSGP transmitting ack %s" % prid(sid, sseq))
        packet = b"@%s #ack ^%s\n" % (label, sseq)

        self.socket.sendto(packet, raddr)


    def transmit_nak(self, sid, sseq):
        raddr, label = sid
        
        print("MSGP transmitting nak %s" % prid(sid))
        packet = b"@%s #nak ^%s\n" % (label, sseq)

        self.socket.sendto(packet, raddr)


    def check_closed_stream(self, sid, s):
        if s.is_closed and not s.sent_messages_by_seq and not s.pending_ack_seqs:
            print("MSGP finally removing closed stream %s" % prid(sid))
            self.streams_by_id.pop(sid)


    def pop_sent_message(self, sid, seq):
        s = self.get_stream(sid)
        msg = s.sent_messages_by_seq.pop(seq, None)
        self.check_closed_stream(sid, s)
            
        return msg
            

    def message_stopped(self, sid, seq, ok):
        s = self.get_stream(sid)
        msg = s.sent_messages_by_seq[seq]

        if not msg.ack_deadline:
            return  # already stopped
        
        if ok:
            print("MSGP got ack for %s" % prid(sid, seq))
            
            if msg.response_handler:
                # Stop retransmission, wait for response
                msg.ack_deadline = None
                msg.retransmit_deadline = None
            else:
                # Clean up, don't wait for unexpected response
                self.pop_sent_message(sid, seq)
        else:
            # Report ack timeout as a fake request, cancel response handling
            print("MSGP gave up waiting for ack for %s" % prid(sid, seq))
            
            request_handler = s.request_handler or self.request_handler
            request_handler(sid, None, self.decode_body(msg.body), msg.target)
                
            self.pop_sent_message(sid, seq)


    def message_responded(self, sid, seq, source, body):
        msg = self.pop_sent_message(sid, seq)
        if not msg:
            return  # probably a duplicate response

        if not msg.response_handler:
            if not source:
                print("MSGP gave up waiting unnecessarily for response for %s" % prid(sid, seq))
            else:
                print("MSGP got unexpected response for %s" % prid(sid, seq))
        else:
            if not source:
                print("MSGP gave up waiting for response for %s" % prid(sid, seq))
            else:
                print("MSGP got response for %s" % prid(sid, seq))
                
            msg.response_handler(sid, source, self.decode_body(body))


    def add_stream(self, sid, request_handler=None):
        s = self.get_stream(sid)
        if s:
            if s.is_unconfirmed:
                print("Accepting stream %s" % prid(sid))
                s.is_unconfirmed = False
            else:
                raise Error("Stream already added!")
        else:
            s = Stream()
            self.streams_by_id[sid] = s
            
        s.request_handler = request_handler


    def remove_stream(self, sid):
        s = self.get_stream(sid)
        if not s:
            return
            
        # A little grace period is given to closed streams before removing them.
        # If outgoing messages are not ACK-ed, we'll wait for those ACK-s first.
        # If an incoming message is not ACK-ed yet, we'll wait until it's either
        # implicitly, or explicitely ACK-ed.
        if not s.sent_messages_by_seq and not s.pending_ack_seqs:
            # Remove stream now
            print("MSGP removing closed stream %s" % prid(sid))
            self.streams_by_id.pop(sid)
        else:
            # Remove stream once pending messages are ACK-ed
            print("MSGP will remove closed stream %s" % prid(sid))
            s.is_closed = True
        
        
    def send_message(self, sid, target, body, response_handler=None, ack_timeout=None, response_timeout=None):
        s = self.get_stream(sid)
        if not s:
            raise Error("No such stream!")
        elif s.is_closed:
            # Let the user send responses to unacked requests after closing
            if not s.pending_ack_seqs:
                raise Error("Stream is closed!")

        # Note: it is allowed to send a message on an unconfirmed stream, such as
        # an error message, but we'll close it right afterwards
        
        msg = Message(
            target, self.encode_body(body), response_handler,
            ack_timeout or self.ack_timeout,
            response_timeout or self.response_timeout
        )
        
        s.last_sent_seq += 1
        sseq = s.last_sent_seq
        
        s.sent_messages_by_seq[sseq] = msg
        self.transmit_message(sid, sseq)

        tseq = int(target) if target.isdigit() else 0
        if tseq:
            # Don't send explicit ACKs as this response suffices
            s.pending_ack_seqs.remove(tseq)
            self.check_closed_stream(sid, s)


    def process_message(self, raddr, label, source, target, body):
        sid = (raddr, label)
        sseq = int(source) if source.isdigit() else 0
        tseq = int(target) if target.isdigit() else 0

        s = self.get_stream(sid)
        if not s:
            if sseq == 1 and not tseq:
                self.add_stream(sid)
                s = self.get_stream(sid)
                s.is_unconfirmed = True  # will be removed unless confirmed
            elif sseq > 1:
                print("MSGP message for unknown stream: %s!" % prid(sid))
                self.transmit_nak(sid, sseq)
                return
            else:
                print("MSGP service message for unknown stream: %s!" % prid(sid))
                return

        if sseq:
            # Then process the incoming one
            
            if sseq <= s.last_received_seq:
                self.transmit_ack(sid, s.last_received_seq)  # be polite
                return
                
            if sseq > s.last_received_seq + 1:
                print("MSGP out of order: %d >> %d!" % (sseq, s.last_received_seq))
                return  # TODO: cache and ack!

            s.last_received_seq = sseq

            # We don't need an explicit ACK if a response is sent immediately
            s.pending_ack_seqs.add(sseq)
                
            if tseq:
                self.message_responded(sid, tseq, source, body)
                
                for mseq in s.sent_messages_by_seq.keys():
                    if mseq <= tseq:
                        self.message_stopped(sid, mseq, True)
            else:
                # Process as a request
                request_handler = s.request_handler or self.request_handler
                request_handler(sid, source, self.decode_body(body), target)
                
            if s.is_unconfirmed:
                # Stream still not confirmed, reject
                print("MSGP rejecting stream %s" % prid(sid))
                self.streams_by_id.pop(sid)
                # Don't send unsolicited nak-s
            elif sseq in s.pending_ack_seqs:
                s.pending_ack_seqs.remove(sseq)
                print("MSGP sending explicit ACK %s" % prid(sid, sseq))
                self.transmit_ack(sid, sseq)
                self.check_closed_stream(sid, s)

        elif source == "ack":
            for mseq in s.sent_messages_by_seq.keys():
                if mseq <= tseq:
                    self.message_stopped(sid, mseq, True)
        elif source == "nak":
            for mseq in s.sent_messages_by_seq.keys():  # TODO: sorted?
                if mseq >= tseq:
                    self.message_stopped(sid, mseq, False)
        else:
            print("Unknown service message %s from %s" % (source, prid(sid)))


    def recved(self):
        rest, raddr = self.socket.recvfrom(65535)
        
        while rest:
            header, rest = rest.split("\n", 1)
            label, source, target, length = None, None, None, None
            
            for field in header.split(" "):
                key, value = field[0], field[1:]
                
                if key == "@":
                    label = value
                elif key == "#":
                    source = value
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
            self.process_message(raddr, label, source, target, body)
        
        
    def cleanup(self):
        now = datetime.datetime.now()
        
        for sid, s in self.streams_by_id.items():
            for seq, msg in s.sent_messages_by_seq.items():
                if msg.ack_deadline and msg.ack_deadline < now:
                    self.message_stopped(sid, seq, False)
                elif msg.response_deadline and msg.response_deadline < now:
                    self.message_responded(sid, seq, None, None)
                elif msg.retransmit_deadline and msg.retransmit_deadline < now:
                    self.transmit_message(sid, seq)


class JsonMsgp(Msgp):
    def encode_body(self, params):
        return json.dumps(params) if params is not None else None
        
        
    def decode_body(self, body):
        return json.loads(body) if body is not None else None
