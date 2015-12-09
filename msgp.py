import uuid
import json
import collections
import datetime
import socket
import errno

from async import TcpReconnector, TcpListener, WeakMethod
from util import Loggable


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

        self.input_handler = None
        self.output_handler = None
        self.error_handler = None

        self.metapoll.register_reader(self.socket, WeakMethod(self.readable))
        #self.metapoll.register_writer(self.socket, WeakMethod(self.writable))


    def __del__(self):
        self.metapoll.register_reader(self.socket, None)
        self.metapoll.register_writer(self.socket, None)
        
        
    def get_remote_addr(self):
        return self.socket.getpeername()


    def set_handlers(self, input_handler, output_handler, error_handler):
        self.input_handler = input_handler
        self.output_handler = output_handler
        self.error_handler = error_handler
        

    def writable(self):
        """Called when the socket becomes writable."""
        
        try:
            sent = self.socket.send(self.outgoing_buffer)
        except IOError as e:
            self.logger.error("Socket error while sending: %s" % e)
            self.metapoll.register_writer(self.socket, None)
            
            if self.error_handler:
                self.error_handler()
                
            return

        self.outgoing_buffer = self.outgoing_buffer[sent:]

        if not self.outgoing_buffer:
            self.metapoll.register_writer(self.socket, None)
            
            if self.output_handler:
                self.output_handler()
            
            
    def parse_header(self, buffer):
        raise NotImplementedError()
        
        
    def parse_body(self, header, buffer):
        raise NotImplementedError()


    def print_message(self, message):
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
                
                if self.error_handler:
                    self.error_handler()
                    
                return

            if not recved:
                self.logger.warning("Socket closed while receiving: %s" % e)
                self.metapoll.register_reader(self.socket, None)
                
                if self.error_handler:
                    self.error_handler()
                    
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
                    
                    if self.input_handler:
                        self.input_handler(message)
                        
                    continue
                    
            break
            

    def send(self, message):
        """Send a Message tuple to the peer."""
        
        if self.outgoing_buffer:
            return False
            
        self.outgoing_buffer = self.print_message(message)
        if not isinstance(self.outgoing_buffer, (bytes, bytearray)):
            raise Exception("Printed message is not bytes!")
        
        self.metapoll.register_writer(self.socket, WeakMethod(self.writable))
        
        return True




class MsgpPipe(MessagePipe):
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




class MsgpStream(object):
    Item = collections.namedtuple('Item', "target body ack_handle response_handle response_handler")
    
    def __init__(self, metapoll, request_handler):
        self.metapoll = metapoll
        self.request_handler = request_handler
        
        self.pipe = None
        self.last_sent_seq = 0
        self.last_recved_seq = 0
        self.last_piped_seq = 0
        
        self.outgoing_items_by_seq = {}
        self.outgoing_ack = None
        
        self.ack_timeout = datetime.timedelta(seconds=1)  # TODO
        self.response_timeout = datetime.timedelta(seconds=2)


    def __del__(self):
        for seq in list(sorted(self.outgoing_items_by_seq.keys())):
            self.timed_out(seq)
    
    
    def connect(self, pipe):
        self.pipe = pipe
        pipe.set_handlers(WeakMethod(self.recved), WeakMethod(self.flushed), WeakMethod(self.failed))
        self.prod()
        
        
    def acked(self, tseq):
        for seq, item in list(self.outgoing_items_by_seq.items()):
            if seq <= tseq:
                self.metapoll.unregister_timeout(item.ack_handle)
                
                if not item.response_handle:
                    self.outgoing_items_by_seq.pop(seq)


    def recved(self, message):
        source, target, body = message
        sseq = int(source) if source.isdigit() else None  # Can't be 0
        
        if sseq:
            if sseq <= self.last_recved_seq:
                # Retransmission after a broken pipe, ACK it, and return
                self.outgoing_ack = self.last_recved_seq
                self.prod()
                return
                
            self.last_recved_seq = sseq
        
        if target.isdigit():
            tseq = int(target)
            self.acked(tseq)
                    
            item = self.outgoing_items_by_seq.get(tseq)
            
            if not item:
                #if sseq:
                #    logging.warning("Response ignored!")  # FIXME: use logger
                return
            
            if sseq:
                self.outgoing_ack = self.last_recved_seq
                
                self.metapoll.unregister_timeout(item.response_handle)
                self.outgoing_items_by_seq.pop(tseq)
                item.response_handler(source, body)
                
                self.prod()
        else:
            # Process, and ACK it if necessary
            self.outgoing_ack = self.last_recved_seq
            
            self.request_handler(target, source, body)
            
            self.prod()
            
        
    def prod(self):
        seq = self.last_piped_seq + 1
        
        while seq <= self.last_sent_seq:
            item = self.outgoing_items_by_seq.get(seq)
            
            if not item:
                # skip expired message
                seq += 1
                continue
                
            message = (seq, item.target, item.body)
            is_piped = self.pipe.send(message)
            
            if is_piped:
                self.last_piped_seq = seq
            else:
                self.last_piped_seq = seq - 1

            if item.target.isdigit():   
                # implicit ACK
                
                if self.outgoing_ack and self.outgoing_ack <= int(item.target):
                    self.outgoing_ack = None
            
            return
            
        if self.outgoing_ack:
            # explicit ACK
            
            message = ("ack", self.outgoing_ack, None)
            is_piped = self.pipe.send(message)
            
            if is_piped:
                self.outgoing_ack = None
                
        
    def send(self, target, body, response_handler=None, response_timeout=None):
        self.last_sent_seq += 1
        seq = self.last_sent_seq

        ack_handle = self.metapoll.register_timeout(self.ack_timeout, WeakMethod(self.timed_out, seq))
        
        if response_handler:
            rt = response_timeout or self.response_timeout
            response_handle = self.metapoll.register_timeout(rt, WeakMethod(self.timed_out, seq))
        else:
            response_handle = None
        
        item = self.Item(target, body, ack_handle, response_handle, response_handler)
        self.outgoing_items_by_seq[seq] = item
        
        self.prod()


    def flushed(self):
        self.prod()
        
        
    def failed(self):
        self.pipe = None
        
        
    def timed_out(self, seq):
        item = self.outgoing_items_by_seq.pop(seq)
        
        if item.response_handle:
            self.metapoll.unregister_timeout(item.ack_handle)
            self.metapoll.unregister_timeout(item.response_handle)
            item.response_handler(None, None)
            

class MsgpDispatcher(Loggable):
    def __init__(self, metapoll, request_handler, status_handler):
        Loggable.__init__(self)
        
        self.metapoll = metapoll
        self.request_handler = request_handler
        self.status_handler = status_handler
        
        self.streams_by_id = {}
        self.unidentified_pipes_by_id = {}
        self.local_id = generate_id()
        
        
    def add_unidentified_pipe(self, socket):
        self.logger.debug("Adding unidentified pipe.")
        
        pipe = MsgpPipe(self.metapoll, socket)
        pipe.set_oid(self.oid)
        pipe.set_handlers(
            WeakMethod(self.recved, id(pipe)), None, WeakMethod(self.failed, id(pipe))
        )
        self.unidentified_pipes_by_id[id(pipe)] = pipe
        
        message = (self.local_id, 0, None)
        pipe.send(message)
        
        
    def recved(self, message, pipe_id):
        self.logger.debug("Receiving identification.")
        # TODO: sending the last_recved_seq is more complicated that expected, so
        # we just send a zero now
        remote_id, zero, none = message
        
        pipe = self.unidentified_pipes_by_id.pop(pipe_id)
        stream = self.streams_by_id.get(remote_id)
        
        if not stream:
            self.logger.debug("Creating new stream for %s" % remote_id)
            wrapped_request_handler = WeakMethod(self.wrap_request_handler, remote_id)
            stream = MsgpStream(self.metapoll, wrapped_request_handler)
            self.streams_by_id[remote_id] = stream

            if self.status_handler:
                self.status_handler(remote_id, pipe.get_remote_addr())
        else:
            self.logger.debug("Already having a stream for %s" % remote_id)
            
        stream.connect(pipe)
        

    def wrap_request_handler(self, target, source, body, remote_id):
        self.logger.debug("Received request %s:%s" % (remote_id, source))
        msgid = (remote_id, source)
        self.request_handler(target, msgid, body)


    def wrap_response_handler(self, source, body, response_handler, remote_id):
        self.logger.debug("Received response %s:%s" % (remote_id, source))
        msgid = (remote_id, source)
        response_handler(msgid, body)
    
    
    def failed(self, pipe_id):
        self.logger.debug("Unidentified pipe failed!")
        self.unidentified_pipes_by_id.pop(pipe_id)
        
    
    def send(self, msgid, body, response_handler=None, response_timeout=None):
        self.logger.debug("Sending message %s:%s" % msgid)
        remote_id, target = msgid
        
        stream = self.streams_by_id.get(remote_id)
        
        if stream:
            wrapped_response_handler = WeakMethod(self.wrap_response_handler, response_handler, remote_id) if response_handler else None
            stream.send(target, body, wrapped_response_handler, response_timeout)
        else:
            raise Exception("No such stream!")
        
        
        
        
class MsgpServer(MsgpDispatcher):
    def __init__(self, metapoll, request_handler, status_handler, addr):
        MsgpDispatcher.__init__(self, metapoll, request_handler, status_handler)
        
        self.listener = TcpListener(metapoll, addr, WeakMethod(self.accepted))
        
        
    def accepted(self, socket, id):
        self.add_unidentified_pipe(socket)




class MsgpClient(MsgpDispatcher):
    def __init__(self, metapoll, request_handler, status_handler, addr):
        MsgpDispatcher.__init__(self, metapoll, request_handler, status_handler)

        self.reconnector = TcpReconnector(metapoll, addr, datetime.timedelta(seconds=1), WeakMethod(self.connected))
        self.reconnector.start()
        
        
    def connected(self, socket):
        self.add_unidentified_pipe(socket)
