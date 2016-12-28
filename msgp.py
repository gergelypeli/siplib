import uuid
import json
import collections
import datetime
import socket
import errno
import types

from async_net import TcpReconnector, TcpListener
from util import Loggable
from format import Addr
import zap


def generate_id():
    return uuid.uuid4().hex[:8]


class MessagePipe(Loggable):
    def __init__(self, socket):
        Loggable.__init__(self)
        
        self.socket = socket

        self.outgoing_buffer = b""
        self.incoming_buffer = b""
        self.incoming_header = None

        self.readable_plug = zap.read_slot(self.socket).plug(self.readable)
        self.writable_plug = None


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
            self.writable_plug.unplug()
            
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
                self.readable_plug.unplug()
                
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
            self.readable_plug.unplug()
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
            self.writable_plug = zap.write_slot(self.socket).plug(self.writable)
        
        return True  # message sending is in progress




class TimedMessagePipe(MessagePipe):
    def __init__(self, socket):
        MessagePipe.__init__(self, socket)
        
        self.request_slot = zap.EventSlot()
        self.response_slot = zap.EventSlot()
        self.ack_slot = zap.EventSlot()
        self.flush_slot = zap.Slot()
        self.error_slot = zap.Slot()
        
        self.pending_ack = None
        self.ack_plugs_by_seq = {}
        
        self.last_accepted_seq = 0  # For validation
        self.last_received_seq = 0  # For the owner
        
        self.ack_timeout = datetime.timedelta(seconds=1)  # TODO
        self.keepalive_interval = datetime.timedelta(seconds=10)  # TODO
        
        self.keepalive_probing = False
        self.keepalive_plug = None
        
        self.reset_keepalive(False)


    def acked(self, tseq):
        #self.logger.debug("Got ack %s" % tseq)
        
        for seq, plug in list(self.ack_plugs_by_seq.items()):
            if seq <= tseq:
                #self.logger.debug("Acked %s" % seq)
                plug.unplug()
                self.ack_plugs_by_seq.pop(seq)
                
                self.ack_slot.zap(seq)


    def ack_timed_out(self, seq):
        self.logger.warning("Message ACK %s timed out!" % seq)
        self.ack_plugs_by_seq.pop(seq)
        self.error_slot.zap()


    def emit_keepalive(self):
        message = ("keep", "alive", None)
        self.try_sending(message)  # it's ok if not piped, then we have traffic


    def reset_keepalive(self, is_probing):
        if self.keepalive_plug:
            self.keepalive_plug.unplug()
        
        interval = self.ack_timeout if is_probing else self.keepalive_interval
        self.keepalive_plug = zap.time_slot(interval).plug(self.keepalive)
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
                    
            if sseq:
                self.try_acking(sseq)
                self.response_slot.zap(tseq, sseq, body)
        else:
            if not sseq:
                raise Exception("Non-numeric source and target???")
        
            self.try_acking(sseq)
            self.request_slot.zap(target, sseq, body)


    def try_acking(self, seq=None):
        if seq:
            if self.pending_ack:
                self.pending_ack = max(self.pending_ack, seq)
            else:
                self.pending_ack = seq
        
        if not self.pending_ack:
            return
            
        message = ("ack", self.pending_ack, None)
        is_piped = MessagePipe.try_sending(self, message)
    
        if is_piped:
            self.pending_ack = None
            
        
    def try_sending(self, message):
        source, target, body = message
        
        sseq = int(source) if source.isdigit() else None  # Can't be 0
        tseq = int(target) if target.isdigit() else None  # Can't be 0

        # Or message can replace the pending ack, if it responds to a later message
        if self.pending_ack:
            if tseq and self.pending_ack <= tseq:
                self.pending_ack = None
            else:
                self.try_acking()
                
                if self.pending_ack:
                    return False  # even our ack is stuck, can't send message, too
            
        is_piped = MessagePipe.try_sending(self, message)
        if sseq and is_piped:
            self.ack_plugs_by_seq[sseq] = zap.time_slot(self.ack_timeout).plug(self.ack_timed_out, seq=sseq)

        return is_piped
        
        
    def flushed(self):
        # This makes sure if a pending ack could not be piped because of
        # a full outgoing buffer, then it goes out as soon as possible.
        self.try_acking()
        
        if self.pending_ack:
            raise Exception("How can an ACK be not piped after a flush?")
            
        self.flush_slot.zap()


    def failed(self):
        self.logger.error("Pipe failed!")
        self.error_slot.zap()




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
    
    def __init__(self):
        Loggable.__init__(self)
        
        self.request_slot = zap.EventSlot()
        self.response_slot = zap.EventSlot()
        self.error_slot = zap.Slot()

        self.pipe = None
        self.last_sent_seq = 0
        self.last_recved_seq = 0
        self.outgoing_items_by_seq = collections.OrderedDict()

        self.response_timeout = datetime.timedelta(seconds=5)  # FIXME: make configurable!


    def __del__(self):
        for item in self.outgoing_items_by_seq.values():
            if item.response_plug:
                item.response_plug.unplug()
                self.response_slot.zap(item.origin, None, None)


    def connect(self, pipe):
        # TODO: is this seq thing implemented?
        if self.pipe:
            if self.pipe.last_received_seq > self.last_recved_seq:
                self.last_recved_seq = self.pipe.last_received_seq

        self.pipe = pipe
        
        if self.pipe.last_accepted_seq > self.last_sent_seq:
            self.last_sent_seq = self.pipe.last_accepted_seq

        pipe.request_slot.plug(self.pipe_requested)
        pipe.response_slot.plug(self.pipe_responded)
        pipe.ack_slot.plug(self.pipe_acked)
        pipe.flush_slot.plug(self.pipe_flushed)
        pipe.error_slot.plug(self.pipe_failed)

        
    def send(self, target, body, origin=None, response_timeout=None):
        self.last_sent_seq += 1
        seq = self.last_sent_seq

        if origin:
            rt = response_timeout or self.response_timeout
            response_plug = zap.time_slot(rt).plug(self.response_timed_out, seq=seq)
        else:
            response_plug = None

        #self.logger.debug("Queueing message %s" % seq)
        self.outgoing_items_by_seq[seq] = self.Item(
            target=target,
            body=body,
            response_plug=response_plug,
            origin=origin,
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
        self.logger.warning("Response timed out for message #%d" % seq)
        item = self.outgoing_items_by_seq.get(seq)
        self.response_slot.zap(item.origin, None, None)
        self.pop(seq)
    
    
    def pipe_requested(self, target, seq, body):
        self.request_slot.zap(target, seq, body)
        
            
    def pipe_responded(self, tseq, seq, body):
        item = self.outgoing_items_by_seq.get(tseq)
        
        if item:
            item.response_plug.unplug()
            self.response_slot.zap(item.origin, seq, body)
            self.pop(tseq)
        else:
            self.logger.warning("Response ignored for message #%d!" % tseq)
    
    
    def pipe_acked(self, tseq):
        item = self.outgoing_items_by_seq.get(tseq)
        
        if item:
            if item.origin:
                item.is_acked = True
            else:
                self.pop(tseq)
        else:
            self.logger.debug("Got ACK for nonexistent message #%d" % tseq)
    
            
    def pipe_flushed(self):
        self.flush()
            
            
    def pipe_failed(self):
        self.error_slot.zap()


    def flushed(self):
        pass



# TODO: since we can simplify the handshake, this may be unnecessary, and
# we may just use pipes during the handshake for hello/bello requests.
class Handshake(MsgpStream):
    def __init__(self):
        MsgpStream.__init__(self)
        
        self.complete_slot = zap.Slot()
        
        self.name = None
        self.accepted_locally = None
        self.accepted_remotely = None


    def disconnect(self):
        if self.outgoing_items_by_seq:
            raise Exception("Not now!")
            
        return self.pipe


    def check_complete(self):
        if not self.outgoing_items_by_seq and self.accepted_locally is not None and self.accepted_remotely is not None:
            self.complete_slot.zap()
        
        
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
    def __init__(self):
        Loggable.__init__(self)
        
        self.request_slot = zap.EventSlot()
        self.response_slot = zap.EventSlot()
        self.status_slot = zap.EventSlot()
        
        self.streams_by_name = {}
        self.handshakes_by_addr = {}
        self.local_id = generate_id()
        
        
    def add_unidentified_pipe(self, socket):
        addr = Addr(*socket.getpeername())
        self.logger.debug("Adding handshake with %s" % (addr,))
        
        pipe = MsgpPipe(socket)
        pipe.set_oid(self.oid.add("pipe", str(addr)))

        handshake = Handshake()
        handshake.set_oid(self.oid.add("handshake", str(addr)))
        handshake.request_slot.plug(self.process_handshake, addr=addr)
        handshake.complete_slot.plug(self.complete_handshake, addr=addr)

        handshake.connect(pipe)
        self.handshakes_by_addr[addr] = handshake
        
        body = self.make_handshake(addr)
        handshake.send("hello", body)
        
        
    def process_handshake(self, target, source, body, addr):
        self.logger.debug("Received handshake request: %r, %r, %r" % (target, source, body))
        h = self.handshakes_by_addr[addr]

        if target == "hello":
            name = self.take_handshake(addr, body)
            ok = bool(name)
            body = dict(ok=ok)
            h.send("bello", body)
            h.accept_locally(ok, name)
        elif target == "bello":
            ok = body["ok"]
            h.accept_remotely(ok)
        else:
            self.logger.error("Invalid handshake hello!")


    # TODO
    def handshake_failed(self, addr):
        self.logger.error("Handshake failed with: %s" % (addr,))
        self.handshakes_by_addr.pop(addr)
        
        
    def complete_handshake(self, addr):
        h = self.handshakes_by_addr.pop(addr)

        if not h.accepted_locally or not h.accepted_remotely:
            self.logger.debug("Handshake ended with failure for %s" % (addr,))
            return  # TODO: report?
            
        name = h.name
        pipe = h.disconnect()
        stream = self.streams_by_name.get(name)
    
        if not stream:
            self.logger.debug("Creating new stream for %s" % name)
            stream = MsgpStream()
            stream.set_oid(self.oid.add("stream", name))
            stream.request_slot.plug(self.process_request, name=name)
            stream.response_slot.plug(self.process_response, name=name)
            stream.error_slot.plug(self.process_error, name=name)
            self.streams_by_name[name] = stream

            self.status_slot.zap(name, addr)
        else:
            self.logger.debug("Already having a stream for %s" % name)

        pipe.set_oid(self.oid.add("pipe", name))  # Hah, pipe renamed here!
        stream.connect(pipe)
    

    # NOTE: The reason why requests and responses are processed in almost the same
    # way is that the target/origin use a different namespace, and their values
    # may overlap, and we can't tell them apart.

    def process_request(self, target, source, body, name):
        if source is not None:
            self.logger.debug("Received request on @%s from #%d to :%s" % (name, source, target))
        else:
            self.logger.debug("Not received request on @%s" % (name,))
            # TODO: error handling here
        
        msgid = (name, source)
        self.request_slot.zap(target, msgid, body)


    def process_response(self, origin, source, body, name):
        if source is not None:
            self.logger.debug("Received response on @%s from #%d to :%s" % (name, source, origin))
        else:
            self.logger.debug("Not received response on @%s" % (name,))
            
        msgid = (name, source)
        self.response_slot.zap(origin, msgid, body)

    
    def process_error(self, name):
        self.logger.error("Stream failed @%s" % (name,))
        self.streams_by_name.pop(name)
        
        self.status_slot.zap(name, None)

    
    def send(self, msgid, body, origin=None, response_timeout=None):
        name, target = msgid
        self.logger.debug("Sending message on @%s from #%s to :%s" % (name, origin, target))
        
        stream = self.streams_by_name.get(name)
        
        if stream:
            stream.send(target, body, origin=origin, response_timeout=response_timeout)
        else:
            raise Exception("No such stream @%s" % (name,))


    def make_handshake(self, addr):
        raise NotImplementedError()


    def take_handshake(self, addr, body):
        raise NotImplementedError()

        
        
        
class MsgpPeer(MsgpDispatcher):
    def __init__(self, local_addr):
        MsgpDispatcher.__init__(self)
        
        self.name = "noname"
        self.session_id = generate_id()
        
        if local_addr:
            self.listener = TcpListener(local_addr)
            self.listener.accepted_slot.plug(self.accepted)
        else:
            self.listener = None

        self.reconnectors_by_addr = {}


    def set_oid(self, oid):
        MsgpDispatcher.set_oid(self, oid)
        
        if self.listener:
            self.listener.set_oid(self.oid.add("listener"))


    def set_name(self, name):
        self.name = name


    def add_remote_addr(self, remote_addr):
        reconnector = TcpReconnector(remote_addr, datetime.timedelta(seconds=1))
        reconnector.set_oid(self.oid.add("reconnector", str(remote_addr)))
        reconnector.connected_slot.plug(self.connected)
        self.reconnectors_by_addr[remote_addr] = reconnector
        reconnector.start()
        
        
    def accepted(self, socket, id):
        self.add_unidentified_pipe(socket)


    def connected(self, socket):
        self.add_unidentified_pipe(socket)


    def make_handshake(self, addr):
        name = "%s-%s" % (self.name, self.session_id)
        return dict(name=name)


    def take_handshake(self, addr, body):
        name = body["name"]
        return name
