import socket
import errno
import os

import zap
from log import Loggable


__all__ = [
    'lookup_host_alias',
    'Listener', 'TcpListener', 'UnixListener',
    'Reconnector', 'TcpReconnector'
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
                
                if not self.timeout:
                    self.connected_slot.zap(None)
                    
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


def parse_http_like_header(header, list_header_fields):
    lines = header.decode().split("\r\n")
    initial_line = None
    headers = { field: [] for field in list_header_fields }  # TODO: auth related?
    last_field = None

    for line in lines:
        if initial_line is None:
            initial_line = line
            continue
            
        if line and line[0].isspace():
            if not last_field:
                raise Exception("First header is a continuation!")
                
            if last_field in list_header_fields:
                headers[last_field][-1] += line
            else:
                headers[last_field] += line
        else:
            field, colon, value = line.partition(":")
            if not colon:
                raise Exception("Invalid header line: %r" % line)
                
            field = field.strip().replace("-", "_").lower()
            value = value.strip()

            if field not in headers:
                headers[field] = value
            elif field in list_header_fields:
                headers[field].append(value)
            else:
                raise Exception("Duplicate header received: %s" % field)

    content_length = headers.pop("content_length", "0")
    
    try:
        content_length = int(content_length.strip())
    except ValueError:
        raise Exception("Invalid content length received: %s!" % content_length)
    
    return initial_line, headers, content_length


def print_http_like_header(initial_line, headers, content_length):
    lines = [ initial_line ]
    
    for field, value in headers.items():
        field = field.replace("_", "-").title()

        if isinstance(value, list):
            # Some header types cannot be joined into a comma separated list,
            # such as the authorization ones, since they contain a comma themselves.
            # So output separate headers always.
            lines.extend(["%s: %s" % (field, v) for v in value])
        else:
            lines.append("%s: %s" % (field, value))

    # Mandatory for TCP
    lines.append("Content-Length: %d" % content_length)
        
    lines.append("")
    lines.append("")
    
    return "\r\n".join(lines).encode("utf8")


class HttpLikeMessage:
    LIST_HEADER_FIELDS = []
    
    def __init__(self, initial_line=None, headers=None, body=None):
        self.initial_line = initial_line
        self.headers = headers
        self.body = body
        
        
    @classmethod
    def parse(cls, header):
        i, h, b = parse_http_like_header(header, cls.LIST_HEADER_FIELDS)
        return cls(i, h, b)


    def print(self):
        return print_http_like_header(self.initial_line, self.headers, len(self.body)) + (self.body or b"")
        

# Copy pasted from Connection not to use Message
class HttpLikeStream(Loggable):
    """
    Handles sending and receiving HTTP-style messages over an asynchronous socket.
    Reports events to its user as:
        process - called after receiving full messages
        disconnect - called on network errors
        exhausted - called when the input direction is closed by the peer
        flushed - called when the output buffer gets empty
    """

    def __init__(self, socket, keepalive_interval=None, message_class=HttpLikeMessage):
        Loggable.__init__(self)
        
        self.socket = socket
        self.message_class = message_class

        self.outgoing_buffer = b""
        #self.outgoing_queue = []
        self.incoming_buffer = b""
        self.incoming_message = None
        
        self.process_slot = zap.EventSlot()
        self.disconnect_slot = zap.Slot()
        self.exhausted_slot = zap.Slot()
        self.flushed_slot = zap.Slot()

        self.read_plug = zap.read_slot(self.socket).plug(self.readable)
        self.write_plug = None
        
        if keepalive_interval:
            self.time_plug = zap.time_slot(keepalive_interval, repeat=True).plug(self.keepalive)


    def keepalive(self):
        if not self.outgoing_buffer:
            # Send empty lines now and then
            self.outgoing_buffer = b"\r\n"
            self.write_plug = zap.write_slot(self.socket).plug(self.writable)


    def writable(self):
        """Called when the socket becomes writable."""
        if self.outgoing_buffer:
            try:
                sent = self.socket.send(self.outgoing_buffer)
            except IOError as e:
                self.logger.error("Socket error while sending: %s" % e)
                self.write_plug.unplug()
                self.disconnect_slot.zap()
                return

            self.outgoing_buffer = self.outgoing_buffer[sent:]

        if not self.outgoing_buffer:
            if self.write_plug:
                self.write_plug.unplug()
                
            self.flushed_slot.zap()
        else:
            if not self.write_plug:
                self.write_plug = zap.write_slot(self.socket).plug(self.writable)


    def check_incoming_message(self):
        """Check and parse messages from the incoming buffer."""
        if not self.incoming_message:
            # No header processed yet, look for the next one

            while self.incoming_buffer.startswith(b"\r\n"):
                # Found a keepalive empty line, get rid of it
                self.incoming_buffer = self.incoming_buffer[2:]

            header, separator, rest = self.incoming_buffer.partition(b"\r\n\r\n")

            if separator:
                # Found a header, find the message length
                self.incoming_buffer = rest
                #self.logger.info("Incoming message with header %r" % header)
                self.incoming_message = self.message_class.parse(header)

        if self.incoming_message:
            # If have a header, get the body
            content_length = self.incoming_message.body

            if len(self.incoming_buffer) >= content_length:
                # Already read the whole body, return the complete message

                body = self.incoming_buffer[:content_length]
                self.incoming_buffer = self.incoming_buffer[content_length:]

                message = self.incoming_message
                message.body = body
                self.incoming_message = None

                return message

        # No complete message yet
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
        """Send a message to the peer."""

        if self.outgoing_buffer:
            self.logger.warning("Outgoing buffer overflow, dropping message!")
            self.process_slot.zap(None)
        else:
            self.outgoing_buffer = message.print()
            self.writable()
