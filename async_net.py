import socket
import errno
#import logging
import os

import zap
from util import Loggable


__all__ = [
    'lookup_host_alias',
    'Listener', 'TcpListener', 'UnixListener',
    'Reconnector', 'TcpReconnector',
    'Message', 'Connection'
]


# TODO: Message header fields should probably be unicode strings, currently just bytestrings.

def lookup_host_alias(host):
    try:
        for line in open(os.environ['HOSTALIASES'], "r").readlines():
            alias, ip = line.strip().split()
            if ip == host:
                return alias
    except (KeyError, OSError):
        pass

    return None


class Listener(Loggable):
    """
    Bind to a port and accept incoming connections, invoking a handler for each
    created socket.
    """

    def __init__(self, type, addr):
        Loggable.__init__(self)
        
        self.type = type
        self.accepted_slot = zap.EventSlot()

        self.socket = self.create_socket()
        self.socket.setblocking(False)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(addr)
        self.socket.listen()

        self.incoming_plug = zap.read_slot(self.socket).plug(self.incoming)

        # This is the actual address (allocated by the kernel if necessary)
        self.addr = self.socket.getsockname()
        # FIXME: can't log from constructor without oid!
        #self.logger.info("Listening for %s on %s" % (type, self.addr))


    def get_socket(self):
        return self.socket


    def incoming(self):
        """Handles new connections to the listening socket"""
        self.logger.debug("Accepting")
        s, addr = self.socket.accept()
        s.setblocking(False)

        id = self.identify(s, addr)
        self.logger.debug("Accepted %s connection from %s" % (self.type, id))

        self.accepted_slot.zap(s, addr)


    def create_socket(self):
        """To be overloaded"""
        raise NotImplementedError()


    def identify(self, s, addr):
        """To be overloaded"""
        raise NotImplementedError()


class TcpListener(Listener):
    """
    A specialization for TCP sockets. Can be used with fake identification.
    """
    def __init__(self, *args):
        Listener.__init__(self, "TCP", *args)


    def create_socket(self):
        return socket.socket(socket.AF_INET, socket.SOCK_STREAM)


    def identify(self, s, addr):
        # Well, not exactly identification, but...
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        host, port = addr
        return lookup_host_alias(host) or host


class UnixListener(Listener):
    """
    A specialization for UNIX sockets. Since connections to UD sockets are anonymous,
    explicit numbering is used to give an id for each accepted connection socket.
    """
    next_id = 1

    def __init__(self, *args):
        # TODO: detect intelligently if somebody is bound to it before removing it!
        #if os.path.exists(addr):
        #    os.unlink(addr)

        Listener.__init__(self, "UNIX", *args)

        os.chmod(self.addr, 0o666)


    def __del__(self):
        # If socket creation failed, this attribute does not exist
        if hasattr(self, 'addr'):
            os.unlink(self.addr)


    def create_socket(self):
        return socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)


    def identify(self, s, addr):
        n = UnixListener.next_id
        UnixListener.next_id += 1
        return "UNIX-%d" % n


class Reconnector(Loggable):
    """
    Creates a stream connection. If fails, it retries in intervals until succeeds, then
    call the given callback with the created socket, and stops. Must be restarted manually
    after the user thinks the connection is broken.
    """
    def __init__(self, type, addr, timeout):
        Loggable.__init__(self)
        
        self.type = type
        self.addr = addr
        self.timeout = timeout
        self.connected_slot = zap.EventSlot()
        self.socket = None
        self.reconnecting_plug = None  # MethodPlug(self.reconnect)
        self.completing_plug = None  # MethodPlug(self.complete)
        self.local_addr = (os.environ.get('VFTESTHOST', ''), 0)


    def start(self):
        """
        Initiate a connection.
        """
        # If currently in progress, just wait for that
        if self.socket:
            return

        # If currently scheduled, then cancel that and try immediately
        if self.reconnecting_plug:
            self.reconnecting_plug.unplug()
        else:
            self.logger.info("Started %s reconnection to %s" % (self.type, self.addr))

        if self.timeout:
            self.reconnecting_plug = zap.time_slot(self.timeout, repeat=True).plug(self.reconnect)

        self.reconnect()


    def reconnect(self):
        """Attempt a reconnection."""

        #if self.socket:
        #    logging.debug("%s connection attempt to %s timed out, retrying now" %
        #            (self.type, self.addr))
        #else:
        #    logging.debug("Attempting %s reconnection to %s" % (self.type, self.addr))

        self.logger.debug("Reconnecting")
        self.socket = self.create_socket()
        self.socket.setblocking(False)
        self.socket.bind(self.local_addr)

        try:
            self.socket.connect(self.addr)
        except socket.error as e:
            if e.errno != errno.EINPROGRESS:
                self.logger.debug("%s connection attempt to %s failed immediately, %s retry" %
                        (self.type, self.addr, "will" if self.timeout else "won't"))
                self.socket = None
                return
        else:
            self.logger.debug("%s connect unexpectedly succeeded" % self.type)

        self.completing_plug = zap.write_slot(self.socket).plug(self.complete)
        #self.completing_plug = MethodPlug(self.connected).attach(zap.write_slot(self.socket))
        #zap.write_slot(self.socket).plug(self.connected)


    def complete(self):
        """Handle the results of a connection attempt."""
        self.logger.debug("Completing")
        self.completing_plug.unplug()

        if self.socket.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR) != 0:
            self.logger.debug("%s connection attempt to %s failed, %s retry" %
                    (self.type, self.addr, "will" if self.timeout else "won't"))
            self.socket = None
            return

        self.logger.debug("Successful %s reconnection to %s" % (self.type, self.addr))

        if self.reconnecting_plug:
            self.reconnecting_plug.unplug()

        s = self.socket
        self.socket = None

        self.connected_slot.zap(s)


    def create_socket(self):
        """To be overloaded"""
        raise NotImplementedError()


class TcpReconnector(Reconnector):
    def __init__(self, *args):
        Reconnector.__init__(self, "TCP", *args)


    def create_socket(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        return s


class Message(object):
    """
    Represents a HTTP-style message.
    """
    def __init__(self, initial_line, header_fields=None, body=None):
        if not initial_line:
            raise Exception("Empty initial message line")

        self.initial_line = initial_line
        self.header_fields = header_fields or dict()
        self.body = body or b""


    @classmethod
    def from_string(cls, s):
        """Parse a string and return the result."""
        head, sep, body = s.partition(b"\n\n")
        lines = head.split(b"\n")

        if not lines:
            return None

        fields = {}

        for line in lines[1:]:
            k, s, v = line.partition(b": ")
            fields[k] = v

        return cls(lines[0], fields, body)


    def to_string(self):
        """Format as a string."""
        head = [ b"%s: %s" % (k, v) for k, v in self.header_fields.iteritems() if v is not None ]

        return b"\n\n".join([ b"\n".join([ self.initial_line ] + head), self.body ])


class Connection(object):
    """
    Handles sending and receiving HTTP-style messages over as asynchronous socket.
    Reports events to its user as:
        process - called after receiving full messages
        disconnect - called on network errors
        exhausted - called when the input direction is closed by the peer
        flushed - called when the output buffer gets empty
    """

    def __init__(self, socket, keepalive_interval=None):
        self.socket = socket

        self.outgoing_buffer = b""
        self.outgoing_queue = []
        self.incoming_buffer = b""
        self.incoming_message = None
        
        self.process_slot = zap.EventSlot()
        self.disconnect_slot = zap.Slot()
        self.exhausted_slot = zap.Slot()
        self.flushed_slot = zap.Slot()

        self.read_plug = zap.read_slot(self.socket).plug(self.readable)
        self.write_plug = zap.write_slot(self.socket).plug(self.writable)
        
        if keepalive_interval:
            self.time_plug = zap.time_slot(keepalive_interval, repeat=True).plug(self.keepalive)


    def keepalive(self):
        if not self.outgoing_buffer:
            # Send empty lines now and then
            self.outgoing_buffer = b"\n"
            self.write_plug = zap.write_slot(self.socket).plug(self.writable)


    def writable(self):
        """Called when the socket becomes writable."""
        if self.outgoing_buffer:
            try:
                sent = self.socket.send(self.outgoing_buffer)
            except IOError as e:
                # Note: SCTP sockets doesn't raise the specialized socket.error!
                self.logger.error("Socket error while sending: %s" % e)
                self.write_plug.unplug()
                self.disconnect_slot.zap()
                return

            self.outgoing_buffer = self.outgoing_buffer[sent:]

        if not self.outgoing_buffer:
            if self.outgoing_queue:
                self.outgoing_buffer = self.outgoing_queue.pop(0)
            else:
                self.write_plug.unplug()
                self.flushed_slot.zap()


    def check_incoming_message(self):
        """Check and parse messages from the incoming buffer."""
        if self.incoming_message is None:
            # No header processed yet, look for the next one

            while self.incoming_buffer.startswith(b"\n"):
                # Found a keepalive empty line, get rid of it
                self.incoming_buffer = self.incoming_buffer[1:]

            header, separator, rest = self.incoming_buffer.partition(b"\n\n")

            if separator:
                # Found a header, create the Message with no body yet
                self.incoming_buffer = rest
                #self.logger.info("Incoming message with header %r" % header)
                lines = header.split(b"\n")

                msg = Message(lines[0])
                self.incoming_message = msg

                for line in lines[1:]:
                    k, v = line.split(b": ")
                    msg.header_fields[k] = v

        if self.incoming_message is not None:
            # If have a header, get the body

            length = int(self.incoming_message.header_fields.get(b"Length", 0))

            if len(self.incoming_buffer) >= length:
                # Already read the whole body, return the complete Message

                msg = self.incoming_message
                self.incoming_message = None

                msg.body = self.incoming_buffer[:length]
                self.incoming_buffer = self.incoming_buffer[length:]

                return msg

        # No complete Message yet
        return None


    def readable(self):
        """Called when the socket becomes readable."""
        exhausted = False
        disconnected = False

        while True:
            recved = None

            try:
                recved = self.socket.recv(65536)
            except socket.error as e:
                if e.errno == errno.EAGAIN:
                    break

                self.logger.error("Socket error while receiving: %s" % e)
                self.read_plug.unplug()
                disconnected = True
                break

            if not recved:
                self.read_plug.unplug()
                exhausted = True
                break

            self.incoming_buffer += recved

        # Since the user will get no further notifications if some incoming messages remain
        # buffered, all available messages must be got. To help the user not to forget this,
        # we'll return all available messages in a list.
        while True:
            msg = self.check_incoming_message()

            if msg:
                self.process_slot.zap(msg)
            else:
                break

        if exhausted:
            self.exhausted_slot.zap()

        if disconnected:
            self.disconnect_slot.zap()


    def put_message(self, message):
        """Send a Message object to the peer."""
        if message.body:
            message.header_fields[b"Length"] = len(message.body)
        else:
            message.header_fields.pop(b"Length", None)

        buffer = message.to_string()

        if self.outgoing_buffer:
            self.outgoing_queue.append(buffer)
        else:
            self.outgoing_buffer = buffer
            self.write_plug = zap.write_slot(self.socket).plug(self.writable)
