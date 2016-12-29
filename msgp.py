import uuid
import json
import collections
import datetime
import socket
import errno
#import types

from async_net import TcpReconnector, TcpListener
from util import Loggable
from format import Addr
import zap


class MessagePipe(Loggable):
    def __init__(self, socket):
        Loggable.__init__(self)
        
        self.socket = socket

        self.incoming_buffer = b""
        self.incoming_header = None

        self.readable_plug = zap.read_slot(self.socket).plug(self.readable)


    def write(self, data):
        try:
            sent = self.socket.send(data)
        except IOError as e:
            self.logger.error("Socket error while sending: %s" % e)
            self.process_message(None)
            return None
        else:
            return data[sent:]
    
        
    def parse_header(self, buffer):
        raise NotImplementedError()
        
        
    def parse_body(self, header, buffer):
        raise NotImplementedError()


    def print_message(self, message):
        raise NotImplementedError()


    def process_message(self, message):  # may be None for errors
        raise NotImplementedError()


    def readable(self):
        """Called when the socket becomes readable."""
        # Failure should only be reported after processing all available messages
        has_failed = False
        
        # Must read all available data, even if we eventually fail
        while True:
            recved = None

            try:
                recved = self.socket.recv(65536)
            except socket.error as e:
                if e.errno == errno.EAGAIN:
                    break

                self.logger.error("Socket error while receiving: %s" % e)
                has_failed = True
                break

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
                    self.process_message(message)
                    continue
                    
            break

        if has_failed:
            self.readable_plug.unplug()
            self.process_message(None)
            

    def send_message(self, message):
        """Send a Message tuple to the peer."""
        
        #self.logger.debug("Sent: %s" % (message,))
        data = self.print_message(message)
        if not isinstance(data, (bytes, bytearray)):
            raise Exception("Printed message is not bytes!")
            
        rest = self.write(data)
        
        return not rest




class TimedMessagePipe(MessagePipe):
    def __init__(self, socket):
        MessagePipe.__init__(self, socket)
        
        self.process_slot = zap.EventSlot()
        self.ack_slot = zap.EventSlot()
        self.error_slot = zap.Slot()
        
        self.ack_plugs_by_source = {}
        
        self.ack_timeout = datetime.timedelta(seconds=1)  # TODO
        self.keepalive_interval = datetime.timedelta(seconds=10)  # TODO
        
        self.keepalive_active = False
        self.keepalive_plug = None
        
        self.reset_keepalive()


    def ack_timed_out(self, source):
        self.logger.warning("Message ACK %s timed out!" % source)
        self.ack_plugs_by_source.pop(source)
        self.error_slot.zap()


    def emit_keepalive(self):
        message = ("!keep", "!alive", None)
        is_piped = self.send_message(message)  # will handle timeouts for active ones only
        if not is_piped:
            self.error_slot.zap()


    def reset_keepalive(self):
        self.keepalive_active = False
        
        if self.keepalive_plug:
            self.keepalive_plug.unplug()
        
        self.keepalive_plug = zap.time_slot(self.keepalive_interval).plug(self.keepalive_needed)


    def keepalive_needed(self):
        if self.keepalive_active:
            # No response to a keepalive that we initiated
            self.logger.warning("Keepalive timed out!")
            self.error_slot.zap()
        else:
            # Initiate keepalive here
            self.emit_keepalive()
            self.keepalive_active = True
            
            if self.keepalive_plug:
                self.keepalive_plug.unplug()
                
            self.keepalive_plug = zap.time_slot(self.ack_timeout).plug(self.keepalive_needed)


    def process_message(self, message):
        if not message:
            self.error_slot.zap()
            return
        
        source, target, body = message
        
        if source == "!keep" and target == "!alive":
            if not self.keepalive_active:
                self.emit_keepalive()
                
            self.reset_keepalive()
            return
        else:
            self.reset_keepalive()
        
        ack_plug = self.ack_plugs_by_source.pop(target, None)
        if ack_plug:
            #self.logger.debug("Acked %s" % seq)
            ack_plug.unplug()
            self.ack_slot.zap(target)
            
        if source == "!ack":
            return

        ack_message = ("!ack", source, None)
        is_piped = self.send_message(ack_message)  # no ACK expected for an ACK
        if not is_piped:
            self.error_slot.zap()

        self.process_slot.zap(target, source, body)


    def try_sending(self, target, source, body):
        message = source, target, body
        is_piped = self.send_message(message)
        if not is_piped:
            return False
        
        self.ack_plugs_by_source[source] = zap.time_slot(self.ack_timeout).plug(self.ack_timed_out, source=source)
        return True
        
        


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

        if " " in source:
            raise Exception("Invalid source token %r" % source)
            
        if " " in target:
            raise Exception("Invalid target token %r" % target)
        
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
    Item = collections.namedtuple("Item", [ "is_response", "target", "body", "origin", "response_plug" ])
    #class Item(types.SimpleNamespace): pass
    
    def __init__(self):
        Loggable.__init__(self)
        
        self.request_slot = zap.EventSlot()
        self.response_slot = zap.EventSlot()
        self.error_slot = zap.Slot()

        self.pipe = None
        self.last_sent_seq = 0

        self.unacked_items_by_seq = collections.OrderedDict()
        self.unresponded_items_by_seq = collections.OrderedDict()

        self.response_timeout = datetime.timedelta(seconds=5)  # FIXME: make configurable!


    def __del__(self):
        for item in self.unresponded_items_by_seq.values():
            item.response_plug.unplug()
            self.response_slot.zap(item.origin, None, None)


    def connect(self, pipe):
        self.pipe = pipe
        
        # FIXME: on reconnects some items may have been already received by the peer
        # but this must be decided during the handshake. Assume now that all unacked
        # messages should be retried.
        for seq, item in self.unacked_items_by_seq.items():
            self.send_item(seq, item)

        pipe.process_slot.plug(self.pipe_processed)
        pipe.ack_slot.plug(self.pipe_acked)
        pipe.error_slot.plug(self.pipe_failed)


    def send_item(self, seq, item):
        source = "#%d" % seq
        target = "#%d" % item.target if item.is_response else "$%s" % item.target
        is_piped = self.pipe.try_sending(target, source, item.body)

        if not is_piped:
            self.pipe_failed()
            return

        
    def queue_message(self, is_response, target, body, origin=None, response_timeout=None):
        self.last_sent_seq += 1
        seq = self.last_sent_seq

        if origin:
            rt = response_timeout or self.response_timeout
            response_plug = zap.time_slot(rt).plug(self.response_timed_out, seq=seq)
        else:
            response_plug = None

        #self.logger.debug("Queueing message %s" % seq)
        item = self.Item(
            is_response=is_response,
            target=target,
            body=body,
            origin=origin,
            response_plug=response_plug
        )
        
        self.send_item(seq, item)
        self.unacked_items_by_seq[seq] = item


    def send_request(self, target, body, origin=None, response_timeout=None):
        self.queue_message(False, target, body, origin, response_timeout)


    def send_response(self, tseq, body, origin=None, response_timeout=None):
        self.queue_message(True, tseq, body, origin, response_timeout)
        

    def response_timed_out(self, seq):
        self.logger.warning("Response timed out for message #%d" % seq)
        item = self.unresponded_items_by_seq.pop(seq, None)
        
        if not item:
            item = self.unacked_items_by_seq.pop(seq, None)
            
        if not item:
            raise Exception("Response timeout for a nonexistent item!")
            
        self.response_slot.zap(item.origin, None, None)
    
    
    def pipe_processed(self, target, source, body):
        if source.startswith("#"):
            sseq = int(source[1:])
        else:
            raise Exception("WTF source?")
            
        if target.startswith("#"):
            tseq, ttag = int(target[1:]), None
        elif target.startswith("$"):
            tseq, ttag = None, target[1:]
        else:
            raise Exception("WTF target?")
            
        if ttag:
            self.request_slot.zap(ttag, sseq, body)
        elif tseq:
            item = self.unresponded_items_by_seq.pop(tseq, None)
        
            if item:
                item.response_plug.unplug()
                self.response_slot.zap(item.origin, sseq, body)
            else:
                self.logger.warning("Unexpected response for message #%d!" % tseq)
    
    
    def pipe_acked(self, target):
        if target.startswith("#"):
            tseq = int(target[1:])
        else:
            raise Exception("WTF ACK target?")
        
        item = self.unacked_items_by_seq.pop(tseq, None)
        
        if item:
            if item.response_plug:
                self.unresponded_items_by_seq[tseq] = item
        else:
            self.logger.debug("Unexpected ACK for message #%d" % tseq)
    
            
    def pipe_failed(self):
        self.error_slot.zap()



        
class Handshake:
    def __init__(self, pipe):
        self.pipe = pipe

        self.name = None
        self.accepted_locally = None
        self.accepted_remotely = None
        self.hello_acked = False
        self.bello_acked = False


class MsgpDispatcher(Loggable):
    def __init__(self):
        Loggable.__init__(self)
        
        self.request_slot = zap.EventSlot()
        self.response_slot = zap.EventSlot()
        self.status_slot = zap.EventSlot()
        
        self.streams_by_name = {}
        self.handshakes_by_addr = {}
        
        
    def add_unidentified_pipe(self, socket):
        addr = Addr(*socket.getpeername())
        self.logger.debug("Adding handshake with %s" % (addr,))
        
        pipe = MsgpPipe(socket)
        pipe.set_oid(self.oid.add("pipe", str(addr)))
        handshake = Handshake(pipe)
        
        handshake.process_plug = pipe.process_slot.plug(self.handshake_processed, addr=addr)
        handshake.ack_plug = pipe.ack_slot.plug(self.handshake_acked, addr=addr)
        handshake.error_plug = pipe.error_slot.plug(self.handshake_failed, addr=addr)

        self.handshakes_by_addr[addr] = handshake
        
        source = "@hello"
        target = "@dude"
        body = self.make_handshake(addr)
        self.logger.debug("Sending handshake from %s to %s as %r" % (source, target, body))
        is_piped = pipe.try_sending(target, source, body)
        
        if not is_piped:
            self.handshake_failed(addr)
        

    def make_handshake(self, addr):
        raise NotImplementedError()


    def take_handshake(self, addr, body):
        raise NotImplementedError()

        
    def handshake_processed(self, target, source, body, addr):
        self.logger.debug("Received handshake from %s to %s as %r" % (source, target, body))
        h = self.handshakes_by_addr[addr]

        if source == "@hello":
            name = self.take_handshake(addr, body)
            # TODO: here we should check if the stream exists, and what was the last
            # properly received seq, then tell it. Similarly, we should store the
            # seq in the opposite side. If both sides recognized each other, then
            # consider the queued outgoing messages up to that message acked.
            # There's no problem with skipping some messages, because we no longer
            # make checks for the tseq, it's TCP, lost messages must have been intentional.
            ok = bool(name)
            body = dict(ok=ok)
            
            source = "@bello"
            target = "@dude"
            self.logger.debug("Sending handshake from %s to %s as %r" % (source, target, body))
            is_piped = h.pipe.try_sending(target, source, body)
            if not is_piped:
                self.handshake_failed(addr)
                return
            
            h.name = name
            h.accepted_locally = ok
            
            self.handshake_completed(addr)
        elif source == "@bello":
            ok = body["ok"]
            h.accepted_remotely = ok
            
            self.handshake_completed(addr)
        else:
            self.logger.error("Invalid handshake source!")


    def handshake_acked(self, target, addr):
        self.logger.debug("Handshake received ack to %s" % target)
        h = self.handshakes_by_addr[addr]
        
        if target == "@hello":
            h.hello_acked = True
        elif target == "@bello":
            h.bello_acked = True
        else:
            raise Exception("WTF acked?")
        
        self.handshake_completed(addr)
        

    # TODO
    def handshake_failed(self, addr):
        self.logger.error("Handshake failed with: %s" % (addr,))
        self.handshakes_by_addr.pop(addr)
        
        
    def handshake_completed(self, addr):
        h = self.handshakes_by_addr[addr]
        
        if h.accepted_locally is None or h.accepted_remotely is None or not h.hello_acked or not h.bello_acked:
            # No, it's not completed yet
            return

        self.handshakes_by_addr.pop(addr)

        if not h.accepted_locally or not h.accepted_remotely:
            self.logger.debug("Handshake ended with failure for %s" % (addr,))
            return  # TODO: report?
            
        name = h.name
        pipe = h.pipe
        h.process_plug.unplug()
        h.ack_plug.unplug()
        h.error_plug.unplug()
        
        stream = self.streams_by_name.get(name)
    
        if not stream:
            self.logger.info("Creating new stream %s" % name)
            stream = MsgpStream()
            stream.set_oid(self.oid.add("stream", name))
            stream.request_slot.plug(self.process_request, name=name)
            stream.response_slot.plug(self.process_response, name=name)
            stream.error_slot.plug(self.process_error, name=name)
            self.streams_by_name[name] = stream

            self.status_slot.zap(name, addr)
        else:
            self.logger.info("Reconnecting stream %s" % name)

        stream.connect(pipe)
    

    # NOTE: The reason why requests and responses are processed in almost the same
    # way is that the target/origin use a different namespace, and their values
    # may overlap, and we can't tell them apart.

    def process_request(self, target, sseq, body, name):
        if sseq is not None:
            self.logger.debug("Received request on @%s from #%d to $%s" % (name, sseq, target))
        else:
            # FIXME: can this happen anymore?
            self.logger.error("Not received request on @%s to $%s" % (name, target))
        
        msgid = (name, sseq)
        self.request_slot.zap(target, msgid, body)


    def process_response(self, origin, sseq, body, name):
        if sseq is not None:
            self.logger.debug("Received response on @%s from #%d to %r" % (name, sseq, origin))
        else:
            self.logger.debug("Not received response on @%s to %r" % (name, origin))
            
        msgid = (name, sseq)
        self.response_slot.zap(origin, msgid, body)

    
    def process_error(self, name):
        self.logger.error("Stream failed @%s" % (name,))
        # FIXME: how long shall we wait for reconnection?
        #self.streams_by_name.pop(name)
        
        self.status_slot.zap(name, None)

    
    def send_request(self, msgid, body, origin=None, response_timeout=None):
        name, target = msgid
        self.logger.debug("Sending request on @%s from %r to $%s" % (name, origin, target))
        
        stream = self.streams_by_name.get(name)
        
        if stream:
            stream.send_request(target, body, origin=origin, response_timeout=response_timeout)
        else:
            raise Exception("No such stream @%s" % (name,))


    def send_response(self, msgid, body, origin=None, response_timeout=None):
        name, tseq = msgid
        self.logger.debug("Sending response on @%s from %r to #%d" % (name, origin, tseq))
        
        stream = self.streams_by_name.get(name)
        
        if stream:
            stream.send_response(tseq, body, origin=origin, response_timeout=response_timeout)
        else:
            raise Exception("No such stream @%s" % (name,))

        
        
        
class MsgpPeer(MsgpDispatcher):
    def __init__(self, local_addr):
        MsgpDispatcher.__init__(self)
        
        self.name = "noname"
        self.session_id = self.generate_session_id()
        
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

        
    def generate_session_id(self):
        return uuid.uuid4().hex[:8]


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
