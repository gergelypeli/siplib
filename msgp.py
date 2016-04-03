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
from format import Addr


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
                self.logger.warning("Socket closed while receiving!")
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
        #self.logger.debug("Got ack %s" % tseq)
        
        for seq, item in list(self.outgoing_items_by_seq.items()):
            if seq <= tseq:
                #self.logger.debug("Acked %s" % seq)
                self.metapoll.unregister_timeout(item.ack_handle)
                
                if not item.response_handle:
                    self.outgoing_items_by_seq.pop(seq)


    def timed_out(self, seq, is_unacked):
        self.logger.warning("Message %s %s timed out!" % ("ACK" if is_unacked else "response", seq))
        item = self.outgoing_items_by_seq.pop(seq)
        
        if item.response_handle:
            other_handle = item.response_handle if is_unacked else item.ack_handle
            self.metapoll.unregister_timeout(other_handle)
            item.response_handler(None, None)
            
        if is_unacked:
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
            
            if sseq:
                self.pending_ack = sseq
                
                if item:
                    self.metapoll.unregister_timeout(item.response_handle)
                    self.outgoing_items_by_seq.pop(tseq)
                    #self.logger.debug("Invoking response handler")
                    item.response_handler(sseq, body)
                    #self.logger.debug("Response handler completed")
                else:
                    self.logger.warning("Response ignored: %s" % body)
                
                self.flush()
        else:
            if not sseq:
                raise Exception("Non-numeric source and target???")
        
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
            
        self.last_accepted_seq = seq

        ack_handle = self.metapoll.register_timeout(self.ack_timeout, WeakMethod(self.timed_out, seq, True))

        if response_handler:
            rt = response_timeout or self.response_timeout
            response_handle = self.metapoll.register_timeout(rt, WeakMethod(self.timed_out, seq, False))
        else:
            response_handle = None

        self.outgoing_items_by_seq[seq] = self.Item(
            target=str(target),
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
        self.logger.error("Pipe failed!")
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
        
        if pipe.last_accepted_seq > self.last_sent_seq:
            self.last_sent_seq = pipe.last_accepted_seq


    def disconnect(self):
        pipe = self.pipe
        self.pipe = None

        if pipe.last_received_seq > self.last_recved_seq:
            self.last_recved_seq = pipe.last_received_seq
        
        return pipe
        
        
    def send(self, target, body, response_handler=None, response_timeout=None):
        self.last_sent_seq += 1
        seq = self.last_sent_seq
        
        self.pipe.send(seq, target, body, response_handler, response_timeout)




class Handshake(MsgpStream):
    def __init__(self):
        MsgpStream.__init__(self)
        
        self.name = None
        self.accepted_locally = None
        self.accepted_remotely = None

        
        
        
class MsgpDispatcher(Loggable):
    def __init__(self, metapoll, request_handler, status_handler):
        Loggable.__init__(self)
        
        self.metapoll = metapoll
        self.request_handler = request_handler
        self.status_handler = status_handler
        
        self.streams_by_name = {}
        self.handshakes_by_addr = {}
        self.local_id = generate_id()
        
        
    def add_unidentified_pipe(self, socket):
        addr = Addr(*socket.getpeername())
        self.logger.debug("Adding handshake with %s" % (addr,))
        
        pipe = MsgpPipe(self.metapoll, socket)
        pipe.set_oid(build_oid(self.oid, "addr", str(addr)))

        request_handler = WeakMethod(self.handshake_recved, addr)
        error_handler = WeakMethod(self.handshake_failed, addr)
        pipe.set_handlers(request_handler, error_handler)
        
        handshake = Handshake()
        handshake.connect(pipe)
        self.handshakes_by_addr[addr] = handshake
        
        msgid = (addr, "hello")
        self.make_handshake(msgid)
        
        
    def handshake_recved(self, target, source, body, addr):
        self.logger.debug("Received handshake request: %r, %r, %r" % (target, source, body))

        if target == "hello":
            msgid = (addr, source)
            self.take_handshake(msgid, body)
        else:
            self.logger.error("Invalid handshake hello!")


    def handshake_failed(self, addr):
        self.logger.error("Handshake failed with: %s" % (addr,))
        self.handshakes_by_addr.pop(addr)
        
        
    def handshake_check(self, addr):
        h = self.handshakes_by_addr.get(addr)
        
        if h.accepted_locally is None or h.accepted_remotely is None:
            return

        self.handshakes_by_addr.pop(addr)

        if not h.accepted_locally or not h.accepted_remotely:
            self.logger.debug("Handshake ended with failure for %s" % (addr,))
            return
            
        name = h.name
        pipe = h.disconnect()
        stream = self.streams_by_name.get(name)
    
        if not stream:
            self.logger.debug("Creating new stream for %s" % name)
            stream = MsgpStream()
            self.streams_by_name[name] = stream

            if self.status_handler:
                self.status_handler(name, addr)
        else:
            self.logger.debug("Already having a stream for %s" % name)

        request_handler = WeakMethod(self.request_wrapper, self.request_handler, name)
        error_handler = WeakMethod(self.stream_failed, name)
        pipe.set_handlers(request_handler, error_handler)
        
        pipe.set_oid(build_oid(self.oid, "name", name))  # Hah, pipe renamed here!
        stream.connect(pipe)
    

    def done_handshake(self, msgid, ok, name=None):
        addr, target = msgid
        h = self.handshakes_by_addr[addr]
        h.accepted_locally = ok
        h.name = name

        body = dict(ok=ok)
        h.send(target, body)

        self.handshake_check(addr)
        # TODO: report it somehow? Or just an error?


    def conclude_handshake(self, source, body, addr):
        h = self.handshakes_by_addr[addr]
        ok = body["ok"]
        h.accepted_remotely = ok

        self.handshake_check(addr)


    def send_handshake(self, msgid, body, response_handler=None, response_timeout=None):
        self.logger.debug("Sending handshake message %s/%s" % msgid)
        addr, target = msgid
        
        h = self.handshakes_by_addr.get(addr)
        
        if h:
            if response_handler:
                r = WeakMethod(self.response_wrapper, response_handler, addr)
            else:
                r = WeakMethod(self.conclude_handshake, addr)
                
            h.send(target, body, r, response_timeout)
        else:
            raise Exception("No such handshake: %s" % (addr,))
        
    
    def request_wrapper(self, target, source, body, request_handler, name):
        if source is not None:
            self.logger.debug("Received request %s/%s" % (name, source))
        else:
            self.logger.debug("Not received request %s/-" % (name,))
            # TODO: error handling here
        
        msgid = (name, source)
        request_handler(target, msgid, body)


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
            r = WeakMethod(self.response_wrapper, response_handler, name) if response_handler else None
            stream.send(target, body, r, response_timeout)
        else:
            raise Exception("No such stream: %s" % (name,))
        
        
    def stream_failed(self, name):
        self.logger.error("Stream failed with: %s" % (name,))
        self.streams_by_name.pop(name)
        
        if self.status_handler:
            self.status_handler(name, None)
        
        
    def make_handshake(self, msgid):
        raise NotImplementedError()


    def take_handshake(self, target, msgid, body):
        raise NotImplementedError()

        
        
        
class MsgpPeer(MsgpDispatcher):
    def __init__(self, metapoll, local_addr, request_handler, status_handler):
        MsgpDispatcher.__init__(self, metapoll, request_handler, status_handler)
        
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


    def make_handshake(self, msgid):
        self.send_handshake(msgid, dict(name=self.name))


    def take_handshake(self, msgid, body):
        name = body["name"]
        self.done_handshake(msgid, True, name)
