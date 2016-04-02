import uuid
import json
import collections
import datetime
import socket
import errno
import types

from async_base import WeakMethod
from async_net import TcpReconnector, TcpListener
from util import Loggable, build_oid


def generate_id():
    return uuid.uuid4().hex[:8]


class MessagePipe(Loggable):
    def __init__(self, metapoll, socket):
        Loggable.__init__(self)
        
        self.metapoll = metapoll
        self.socket = socket

        self.outgoing_buffer = b""
        self.incoming_buffer = b""
        self.incoming_header = None

        self.metapoll.register_reader(self.socket, WeakMethod(self.readable))


    def __del__(self):
        self.metapoll.register_reader(self.socket, None)
        self.metapoll.register_writer(self.socket, None)
        
        
    def get_remote_addr(self):
        return self.socket.getpeername()


    def writable(self):
        """Called when the socket becomes writable."""
        
        try:
            sent = self.socket.send(self.outgoing_buffer)
        except IOError as e:
            self.logger.error("Socket error while sending: %s" % e)
            self.metapoll.register_writer(self.socket, None)
            
            self.failed()
            return

        self.outgoing_buffer = self.outgoing_buffer[sent:]

        if not self.outgoing_buffer:
            self.metapoll.register_writer(self.socket, None)
            self.flushed()
            
            
    def parse_header(self, buffer):
        raise NotImplementedError()
        
        
    def parse_body(self, header, buffer):
        raise NotImplementedError()


    def print_message(self, message):
        raise NotImplementedError()


    def recved(self, message):
        raise NotImplementedError()


    def flushed(self):
        raise NotImplementedError()


    def failed(self):
        raise NotImplementedError()
        

    def readable(self):
        """Called when the socket becomes readable."""
        
        # Must read all available data
        while True:
            recved = None

            try:
                recved = self.socket.recv(65536)
            except socket.error as e:
                if e.errno == errno.EAGAIN:
                    break

                self.logger.error("Socket error while receiving: %s" % e)
                self.metapoll.register_reader(self.socket, None)
                self.failed()
                return

            if not recved:
                self.logger.warning("Socket closed while receiving: %s" % e)
                self.metapoll.register_reader(self.socket, None)
                self.failed()
                break

            self.incoming_buffer += recved

        # Must process all available messages
        while True:
            if not self.incoming_header:
                # No header processed yet, look for the next one
                self.incoming_header, self.incoming_buffer = self.parse_header(self.incoming_buffer)

            if self.incoming_header:
                # If have a header, get the body
                message, self.incoming_buffer = self.parse_body(self.incoming_header, self.incoming_buffer)
                
                if message:
                    self.incoming_header = None
                    self.recved(message)
                    continue
                    
            break
            

    def try_sending(self, message):
        """Send a Message tuple to the peer."""
        
        if self.outgoing_buffer:
            return False
            
        self.outgoing_buffer = self.print_message(message)
        if not isinstance(self.outgoing_buffer, (bytes, bytearray)):
            raise Exception("Printed message is not bytes!")
        
        self.metapoll.register_writer(self.socket, WeakMethod(self.writable))
        
        return True




class TimedMessagePipe(MessagePipe):
    class Item(types.SimpleNamespace): pass
    #Item = collections.namedtuple('Item', "target body ack_handle response_handle response_handler")):
    
    def __init__(self, metapoll, socket):
        MessagePipe.__init__(self, metapoll, socket)
        
        self.request_handler = None
        self.error_handler = None
        
        self.outgoing_items_by_seq = collections.OrderedDict()
        self.pending_ack = None
        self.last_accepted_seq = 0  # For validation
        self.last_received_seq = 0  # For the owner
        
        self.ack_timeout = datetime.timedelta(seconds=1)  # TODO
        self.response_timeout = datetime.timedelta(seconds=2)


    def __del__(self):
        for seq in list(self.outgoing_items_by_seq.keys()):
            self.timed_out(seq)
    
    
    def set_handlers(self, request_handler, error_handler):
        self.request_handler = request_handler
        self.error_handler = error_handler
    
    
    def acked(self, tseq):
        for seq, item in list(self.outgoing_items_by_seq.items()):
            if seq <= tseq:
                self.metapoll.unregister_timeout(item.ack_handle)
                
                if not item.response_handle:
                    self.outgoing_items_by_seq.pop(seq)


    def timed_out(self, seq):
        item = self.outgoing_items_by_seq.pop(seq)
        
        if item.response_handle:
            self.metapoll.unregister_timeout(item.ack_handle)
            self.metapoll.unregister_timeout(item.response_handle)
            item.response_handler(None, None)
            
        self.error_handler()


    def recved(self, message):
        source, target, body = message
        sseq = int(source) if source.isdigit() else None  # Can't be 0
        tseq = int(target) if target.isdigit() else None  # Can't be 0
        
        if sseq:
            if sseq <= self.last_received_seq:
                # Hey, we got a retransmission over the same pipe???
                pass
                
            self.last_received_seq = sseq
        
        if tseq:
            self.acked(tseq)
                    
            item = self.outgoing_items_by_seq.get(tseq)
            
            if not item:
                #if sseq:
                #    logging.warning("Response ignored!")  # FIXME: use logger
                return
            
            if sseq:
                self.pending_ack = sseq
                
                self.metapoll.unregister_timeout(item.response_handle)
                self.outgoing_items_by_seq.pop(tseq)
                item.response_handler(sseq, body)
                
                self.flush()
        else:
            if not sseq:
                raise Exception("Non numeric source and target???")
        
            # Process, and ACK it if necessary
            self.pending_ack = sseq
            
            self.request_handler(target, sseq, body)
            
            self.flush()
            
        
    def flush(self):
        for seq, item in self.outgoing_items_by_seq.items():
            if item.is_piped:
                continue

            message = (seq, item.target, item.body)
            item.is_piped = self.try_sending(message)
            tseq = int(item.target) if item.target.isdigit() else None  # Can't be 0
            
            if tseq and self.pending_ack and self.pending_ack <= tseq:
                # implicit ACK, clear even if the response is not yet piped
                self.pending_ack = None
            
            return
            
        if self.pending_ack:
            # explicit ACK
            # Note: maybe ACK-s should have precedence over outgoing requests
            
            message = ("ack", self.pending_ack, None)
            is_piped = self.try_sending(message)
            
            if is_piped:
                # clear only if piped
                self.pending_ack = None
                
        
    def send(self, seq, target, body, response_handler=None, response_timeout=None):
        if seq <= self.last_accepted_seq:
            raise Exception("Duplicate outgoing message seq!")
            
        if not isinstance(target, str):
            raise Exception("Pls, use string targets here!")
            
        self.last_accepted_seq = seq

        ack_handle = self.metapoll.register_timeout(self.ack_timeout, WeakMethod(self.timed_out, seq))

        if response_handler:
            rt = response_timeout or self.response_timeout
            response_handle = self.metapoll.register_timeout(rt, WeakMethod(self.timed_out, seq))
        else:
            response_handle = None

        self.outgoing_items_by_seq[seq] = self.Item(
            target=target,
            body=body,
            ack_handle=ack_handle,
            response_handle=response_handle,
            response_handler=response_handler,
            is_piped=False
        )

        self.flush()


    def flushed(self):
        self.flush()


    def failed(self):
        self.error_handler()




class SimpleJsonPipe:
#class MsgpPipe(TimedMessagePipe):
    def parse_header(self, buffer):
        header, separator, rest = buffer.partition(b"\n")
        if not separator:
            return None, buffer

        fields = header.decode('ascii').split(" ")
        source = fields[0]
        target = fields[1]
        length = int(fields[2]) if len(fields) > 2 else None

        header = (source, target, length)
        return header, rest


    def parse_body(self, header, buffer):
        source, target, length = header

        if length is None:
            return header, buffer

        if len(self.incoming_buffer) < length + 1:
            return None, buffer
            
        body = buffer[:length]
        rest = buffer[length + 1:]
        
        body = json.loads(body.decode('ascii'))
        message = (source, target, body)
        return message, rest


    def print_message(self, message):
        source, target, body = message
        
        if body is not None:
            body = json.dumps(body, ensure_ascii=True)
            return ("%s %s %d\n%s\n" % (source, target, len(body), body)).encode('ascii')
        else:
            return ("%s %s\n" % (source, target)).encode('ascii')




class MsgpPipe(SimpleJsonPipe, TimedMessagePipe):
    # Base class order matters, because of the MRO. Formatting functions
    # must be found first, before the default implementation!
    pass
    



# TODO: now that we no longer check for duplicate incoming messages, the peers
# must agree not to send them again by exchanging the seq-s during the handshake!
class MsgpStream:
    def __init__(self):
        self.pipe = None
        self.last_sent_seq = 0
        self.last_recved_seq = 0


    def connect(self, pipe):
        self.pipe = pipe


    def disconnect(self):
        if self.pipe.last_received_seq > self.last_recved_seq:
            self.last_recved_seq = self.pipe.last_received_seq
            
        self.pipe = None
        
        
    def send(self, target, body, response_handler=None, response_timeout=None):
        self.last_sent_seq += 1
        seq = self.last_sent_seq
        
        self.pipe.send(seq, target, body, response_handler, response_timeout)
        
        
        
        
class MsgpDispatcher(Loggable):
    class Handshake:
        def __init__(self, pipe):
            self.pipe = pipe
            self.name = None
            self.accepted_here = None
            self.accepted_there = None
            
            
    def __init__(self, metapoll, request_handler):
        Loggable.__init__(self)
        
        self.metapoll = metapoll
        self.request_handler = request_handler
        
        self.streams_by_name = {}
        self.handshakes_by_addr = {}
        self.local_id = generate_id()
        
        
    def add_unidentified_pipe(self, socket):
        addr = socket.getpeername()
        self.logger.debug("Adding handshake with %s" % (addr,))
        
        pipe = MsgpPipe(self.metapoll, socket)
        pipe.set_oid(build_oid(self.oid, "addr", str(addr)))
        pipe.set_handlers(
            WeakMethod(self.handshake_recved_challenge, addr),
            WeakMethod(self.handshake_failed, addr)
        )
        
        self.handshakes_by_addr[addr] = self.Handshake(pipe)
        
        self.handshake_need_challenge(addr)
        
        
    def handshake_recved_challenge(self, target, source, body, addr):
        self.logger.debug("Received handshake request: %r, %r, %r" % (target, source, body))
        
        if source == 1 and target == "hello":
            self.logger.debug("Performing step one: %s" % (body,))
            self.handshake_need_response(addr, body)
        else:
            self.logger.error("Invalid handshake hello!")


    def handshake_recved_response(self, source, body, addr):
        # Receiving hello response with solution
        h = self.handshakes_by_addr.get(addr)
        name = body["name"]
        h.name = name
        
        self.handshake_need_conclusion(addr, body)
        
        
    def handshake_recved_conclusion(self, source, body, addr):
        # Receiving hello conclusion
        h = self.handshakes_by_addr.get(addr)
        ok = body["ok"]
        h.accepted_there = ok
        self.handshake_check(addr)
        # TODO: report it somehow? Or just an error?


    def handshake_send_challenge(self, addr, body):
        # Sending hello request with challenge
        h = self.handshakes_by_addr.get(addr)
        w = WeakMethod(self.handshake_recved_response, addr)
        h.pipe.send(1, "hello", body, w)


    def handshake_send_response(self, addr, body):
        # Sending hello response with solution
        h = self.handshakes_by_addr.get(addr)
        w = WeakMethod(self.handshake_recved_conclusion, addr)
        h.pipe.send(2, "1", body, w)


    def handshake_send_conclusion(self, addr, ok):
        # Sending hello conclusion
        h = self.handshakes_by_addr.get(addr)
        body = dict(ok=ok)
        h.pipe.send(3, "2", body, None)
        
        h.accepted_here = ok
        self.handshake_check(addr)


    def handshake_check(self, addr):
        h = self.handshakes_by_addr.get(addr)
        
        if h.accepted_here is None or h.accepted_there is None:
            return

        self.handshakes_by_addr.pop(addr)

        if not h.accepted_here or not h.accepted_there:
            self.logger.debug("Handshake ended with failure for %s" % (addr,))
            return
            
        stream = self.streams_by_name.get(h.name)
    
        if not stream:
            self.logger.debug("Creating new stream for %s" % h.name)
            stream = MsgpStream()
            self.streams_by_name[h.name] = stream

            #if self.status_handler:
            #    self.status_handler(h.name, h.pipe.get_remote_addr())
        else:
            self.logger.debug("Already having a stream for %s" % h.name)

        wrapped_request_handler = WeakMethod(self.request_wrapper, h.name)
        wrapped_failure_handler = WeakMethod(self.request_wrapper, None, None, None, h.name)
        h.pipe.set_handlers(wrapped_request_handler, wrapped_failure_handler)
        stream.connect(h.pipe)
    
    
    def handshake_failed(self, addr):
        self.logger.error("Handshake failed???")
        self.handshakes_by_addr.pop(addr)


    def request_wrapper(self, target, source, body, name):
        if source is not None:
            self.logger.debug("Received request %s/%s" % (name, source))
        else:
            self.logger.debug("Not received request %s/-" % (name,))
            # TODO: error handling here
        
        msgid = (name, source)
        self.request_handler(target, msgid, body)


    def response_wrapper(self, source, body, response_handler, name):
        if source is not None:
            self.logger.debug("Received response %s/%s" % (name, source))
        else:
            self.logger.debug("Not received response %s/-" % (name,))
            
        msgid = (name, source)
        response_handler(msgid, body)
    
    
    def send(self, msgid, body, response_handler=None, response_timeout=None):
        self.logger.debug("Sending message %s/%s" % msgid)
        name, target = msgid
        
        stream = self.streams_by_name.get(name)
        
        if stream:
            wrapped_response_handler = WeakMethod(self.response_wrapper, response_handler, name) if response_handler else None
            stream.send(target, body, wrapped_response_handler, response_timeout)
        else:
            raise Exception("No such stream!")
        
        
    def handshake_need_challenge(self, addr):
        raise NotImplementedError()


    def handshake_need_response(self, addr, body):
        raise NotImplementedError()


    def handshake_need_conclusion(self, addr, body):
        raise NotImplementedError()
        
        
        
        
class MsgpPeer(MsgpDispatcher):
    def __init__(self, metapoll, local_addr, request_handler):
        MsgpDispatcher.__init__(self, metapoll, request_handler)
        
        self.name = generate_id()
        
        if local_addr:
            self.listener = TcpListener(metapoll, local_addr, WeakMethod(self.accepted))
        else:
            self.listener = None

        self.reconnectors_by_addr = {}


    def add_remote_addr(self, remote_addr):
        reconnector = TcpReconnector(self.metapoll, remote_addr, datetime.timedelta(seconds=1), WeakMethod(self.connected))
        reconnector.start()
        self.reconnectors_by_addr[remote_addr] = reconnector
        
        
    def accepted(self, socket, id):
        self.add_unidentified_pipe(socket)


    def connected(self, socket):
        self.add_unidentified_pipe(socket)


    def handshake_need_challenge(self, addr):
        self.handshake_send_challenge(addr, None)


    def handshake_need_response(self, addr, body):
        self.handshake_send_response(addr, dict(name=self.name))


    def handshake_need_conclusion(self, addr, body):
        self.handshake_send_conclusion(addr, True)
