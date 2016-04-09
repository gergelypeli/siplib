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
        
        
    def write(self, data):
        try:
            sent = self.socket.send(data)
        except IOError as e:
            self.logger.error("Socket error while sending: %s" % e)
            self.failed()
            return None
        else:
            return data[sent:]
    
        
    def writable(self):
        """Called when the socket becomes writable."""
        
        self.outgoing_buffer = self.write(self.outgoing_buffer)
        
        if not self.outgoing_buffer:
            self.metapoll.register_writer(self.socket, None)
            
            if self.outgoing_buffer is not None:
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
        # Failure should only be reported after processing all available messages
        has_failed = False
        
        # Must read all available data
        while True:
            recved = None

            try:
                recved = self.socket.recv(65536)
            except socket.error as e:
                if e.errno == errno.EAGAIN:
                    break

                self.logger.error("Socket error while receiving: %s" % e)
                has_failed = True
                return

            if not recved:
                self.metapoll.register_reader(self.socket, None)
                
                self.logger.warning("Socket closed while receiving!")
                has_failed = True
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
                    #self.logger.debug("Recved: %s" % (message,))
                    self.recved(message)
                    continue
                    
            break

        if has_failed:
            self.metapoll.register_reader(self.socket, None)
            self.failed()
            

    def try_sending(self, message):
        """Send a Message tuple to the peer."""
        
        if self.outgoing_buffer:
            return False  # we will report flushed eventually
            
        #self.logger.debug("Sent: %s" % (message,))
        data = self.print_message(message)
        if not isinstance(data, (bytes, bytearray)):
            raise Exception("Printed message is not bytes!")
            
        rest = self.write(data)
        
        if rest:
            self.outgoing_buffer = rest
            self.metapoll.register_writer(self.socket, WeakMethod(self.writable))
        
        return True  # message sending is in progress




class TimedMessagePipe(MessagePipe):
    def __init__(self, metapoll, socket):
        MessagePipe.__init__(self, metapoll, socket)
        
        self.request_handler = None
        self.response_handler = None
        self.ack_handler = None
        self.flush_handler = None
        self.error_handler = None
        
        self.pending_ack = None
        self.ack_handles_by_seq = {}
        
        self.last_accepted_seq = 0  # For validation
        self.last_received_seq = 0  # For the owner
        
        self.ack_timeout = datetime.timedelta(seconds=1)  # TODO
        self.keepalive_interval = datetime.timedelta(seconds=10)  # TODO
        
        self.keepalive_probing = False
        self.keepalive_handle = None


    def set_handlers(self, request_handler, response_handler, ack_handler, flush_handler, error_handler):
        self.request_handler = request_handler
        self.response_handler = response_handler
        self.ack_handler = ack_handler
        self.flush_handler = flush_handler
        self.error_handler = error_handler
        
        self.reset_keepalive(False)
    
    
    def acked(self, tseq):
        #self.logger.debug("Got ack %s" % tseq)
        
        for seq, handle in list(self.ack_handles_by_seq.items()):
            if seq <= tseq:
                #self.logger.debug("Acked %s" % seq)
                self.metapoll.unregister_timeout(handle)
                self.ack_handles_by_seq.pop(seq)
                
                self.ack_handler(seq)


    def ack_timed_out(self, seq):
        self.logger.warning("Message ACK %s timed out!" % seq)
        self.ack_handles_by_seq.pop(seq)
        self.error_handler()


    def emit_keepalive(self):
        message = ("keep", "alive", None)
        self.try_sending(message)  # it's ok if not piped, then we have traffic


    def reset_keepalive(self, is_probing):
        self.metapoll.unregister_timeout(self.keepalive_handle)
        
        interval = self.ack_timeout if is_probing else self.keepalive_interval
        self.keepalive_handle = self.metapoll.register_timeout(interval, WeakMethod(self.keepalive))
        self.keepalive_probing = is_probing


    def keepalive(self):
        if self.keepalive_probing:
            # Timed out
            self.logger.warning("Keepalive timed out!")
            self.failed()
        else:
            self.keepalive_probing = True
            self.emit_keepalive()
            self.reset_keepalive(True)


    def recved(self, message):
        source, target, body = message
        sseq = int(source) if source.isdigit() else None  # Can't be 0
        tseq = int(target) if target.isdigit() else None  # Can't be 0
        
        is_keepalive = (source == "keep" and target == "alive")
        
        if is_keepalive and not self.keepalive_probing:
            self.emit_keepalive()

        self.reset_keepalive(False)

        if is_keepalive:
            return
        
        if sseq:
            if sseq <= self.last_received_seq:
                # Hey, we got a retransmission over the same pipe???
                pass
                
            self.last_received_seq = sseq
        
        if tseq:
            self.acked(tseq)
                    
            #item = self.outgoing_items_by_seq.get(tseq)
            
            if sseq:
                self.pending_ack = sseq
                self.response_handler(tseq, sseq, body)
                self.try_acking(None)
        else:
            if not sseq:
                raise Exception("Non-numeric source and target???")
        
            # Process, and ACK it if necessary
            self.pending_ack = sseq
            self.request_handler(target, sseq, body)
            self.try_acking(None)


    def try_acking(self, tseq):
        # Returns True if no ACK needed, or it went out successfully, False if stuck
        
        if self.pending_ack:
            if not tseq or tseq < self.pending_ack:
                # Must send the pending ACK first
                message = ("ack", self.pending_ack, None)
                is_piped = MessagePipe.try_sending(self, message)
            
                if is_piped:
                    # clear only if piped
                    self.pending_ack = None
                    
                return is_piped
                
            # implicit ACK, clear even if the response is not yet piped
            self.pending_ack = None
            
        return True  # nothing to pipe, consider it done
            
        
    def try_sending(self, message):
        source, target, body = message
        
        sseq = int(source) if source.isdigit() else None  # Can't be 0
        tseq = int(target) if target.isdigit() else None  # Can't be 0

        # This makes sure if a response handler sends a message, the pending
        # ack goes out first
        is_piped = self.try_acking(tseq)
        if not is_piped:
            return False  # even our ack is stuck, can't send message, too
        
        is_piped = MessagePipe.try_sending(self, message)
        if sseq and is_piped:
            self.ack_handles_by_seq[sseq] = self.metapoll.register_timeout(self.ack_timeout, WeakMethod(self.ack_timed_out, sseq))

        return is_piped
        
        
    def flushed(self):
        # This makes sure if a pending ack could not be piped because of
        # a full outgoing buffer, then it goes out as soon as possible.
        is_piped = self.try_acking(None)
        if not is_piped:
            raise Exception("How can an ACK be not piped after a flush?")
            
        self.flush_handler()


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
class MsgpStream(Loggable):
    #Item = collections.namedtuple("Item", [ "target", "body", "response_handle", "response_handler" ])
    class Item(types.SimpleNamespace): pass
    
    def __init__(self, metapoll, request_handler, error_handler):
        self.metapoll = metapoll
        self.request_handler = request_handler
        self.error_handler = error_handler
        
        self.pipe = None
        self.last_sent_seq = 0
        self.last_recved_seq = 0
        self.outgoing_items_by_seq = collections.OrderedDict()

        self.response_timeout = datetime.timedelta(seconds=5)


    def __del__(self):
        for item in self.outgoing_items_by_seq.values():
            if item.response_handle:
                self.metapoll.unregister_timeout(item.response_handle)
                item.response_handler(None, None)


    def connect(self, pipe):
        if self.pipe:
            if self.pipe.last_received_seq > self.last_recved_seq:
                self.last_recved_seq = self.pipe.last_received_seq

        self.pipe = pipe
        
        if self.pipe.last_accepted_seq > self.last_sent_seq:
            self.last_sent_seq = self.pipe.last_accepted_seq

        pipe.set_handlers(
            WeakMethod(self.pipe_requested),
            WeakMethod(self.pipe_responded),
            WeakMethod(self.pipe_acked),
            WeakMethod(self.pipe_flushed),
            WeakMethod(self.pipe_failed)
        )

        
    def send(self, target, body, response_handler=None, response_timeout=None):
        self.last_sent_seq += 1
        seq = self.last_sent_seq

        if response_handler:
            rt = response_timeout or self.response_timeout
            response_handle = self.metapoll.register_timeout(rt, WeakMethod(self.response_timed_out, seq))
        else:
            response_handle = None

        #self.logger.debug("Queueing message %s" % seq)
        self.outgoing_items_by_seq[seq] = self.Item(
            target=target,
            body=body,
            response_handle=response_handle,
            response_handler=response_handler,
            is_acked=False,
            is_piped=False
        )
        
        self.flush()


    def flush(self):
        for seq, item in self.outgoing_items_by_seq.items():
            if item.is_piped:
                continue

            message = (str(seq), str(item.target), item.body)
            item.is_piped = self.pipe.try_sending(message)
            
            return


    def pop(self, seq):
        #self.logger.debug("Popping message %s" % seq)
        self.outgoing_items_by_seq.pop(seq, None)
        
        if not self.outgoing_items_by_seq:
            self.flushed()
            

    def response_timed_out(self, seq):
        self.logger.warning("Response timed out: %s" % seq)
        item = self.outgoing_items_by_seq.get(seq)
        item.response_handler(None, None)
        self.pop(seq)
    
    
    def pipe_requested(self, target, seq, body):
        self.request_handler(target, seq, body)
        
            
    def pipe_responded(self, tseq, seq, body):
        item = self.outgoing_items_by_seq.get(tseq)
        
        if item:
            self.metapoll.unregister_timeout(item.response_handle)
            item.response_handler(seq, body)
            self.pop(tseq)
        else:
            self.logger.debug("Response ignored!")
    
    
    def pipe_acked(self, tseq):
        item = self.outgoing_items_by_seq.get(tseq)
        
        if item:
            if item.response_handler:
                item.is_acked = True
            else:
                self.pop(tseq)
        else:
            self.logger.debug("Unknown message ACK-ed: %s" % tseq)
    
            
    def pipe_flushed(self):
        self.flush()
            
            
    def pipe_failed(self):
        self.error_handler()


    def flushed(self):
        pass



class Handshake(MsgpStream):
    def __init__(self, m, r, e, c):
        MsgpStream.__init__(self, m, r, e)
        
        self.complete_handler = c
        
        self.name = None
        self.accepted_locally = None
        self.accepted_remotely = None


    def disconnect(self):
        if self.outgoing_items_by_seq:
            raise Exception("Not now!")
            
        return self.pipe


    def check_complete(self):
        if not self.outgoing_items_by_seq and self.accepted_locally is not None and self.accepted_remotely is not None:
            self.complete_handler()
        
        
    def accept_locally(self, ok, name):
        self.accepted_locally = ok
        self.name = name
        self.check_complete()
        
        
    def accept_remotely(self, ok):
        self.accepted_remotely = ok
        self.check_complete()
        
        
    def flushed(self):
        self.check_complete()
        
        
        
        
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
        pipe.set_oid(build_oid(self.oid, "pipe", str(addr)))

        request_handler = WeakMethod(self.handshake_recved, addr)
        error_handler = WeakMethod(self.handshake_failed, addr)
        complete_handler = WeakMethod(self.handshake_completed, addr)
        #pipe.set_handlers(request_handler, error_handler)
        
        handshake = Handshake(self.metapoll, request_handler, error_handler, complete_handler)
        handshake.set_oid(build_oid(self.oid, "handshake", str(addr)))
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
        
        
    def handshake_completed(self, addr):
        h = self.handshakes_by_addr.pop(addr)

        if not h.accepted_locally or not h.accepted_remotely:
            self.logger.debug("Handshake ended with failure for %s" % (addr,))
            return  # TODO: report?
            
        name = h.name
        pipe = h.disconnect()
        stream = self.streams_by_name.get(name)
    
        if not stream:
            self.logger.debug("Creating new stream for %s" % name)
            request_handler = WeakMethod(self.request_wrapper, self.request_handler, name)
            error_handler = WeakMethod(self.stream_failed, name)
            stream = MsgpStream(self.metapoll, request_handler, error_handler)
            stream.set_oid(build_oid(self.oid, "stream", name))
            self.streams_by_name[name] = stream

            if self.status_handler:
                self.status_handler(name, addr)
        else:
            self.logger.debug("Already having a stream for %s" % name)

        #pipe.set_handlers(request_handler, error_handler)
        pipe.set_oid(build_oid(self.oid, "pipe", name))  # Hah, pipe renamed here!
        stream.connect(pipe)
    

    def done_handshake(self, msgid, ok, name=None):
        addr, target = msgid
        h = self.handshakes_by_addr[addr]

        body = dict(ok=ok)
        h.send(target, body)

        #self.logger.debug("XXX accept locally: %s" % ok)
        h.accept_locally(ok, name)
        # TODO: report it somehow? Or just an error?


    def conclude_handshake(self, source, body, addr):
        h = self.handshakes_by_addr[addr]
        ok = body["ok"]
        
        #self.logger.debug("XXX accept remotely: %s" % ok)
        h.accept_remotely(ok)


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


    def set_name(self, name):
        self.name = name


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
