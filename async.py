from __future__ import unicode_literals, print_function, absolute_import
import weakref
import select
import socket
import errno
import datetime
import logging
import os


__all__ = [
    'lookup_host_alias',
    'WeakMethod', 'Weak', 'Metapoll',
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


def Weak(x):
    return x if isinstance(x, weakref.ProxyTypes) else weakref.proxy(x)


class WeakMethod(object):
    """
    Encapsulate a bound method object with optional bound (positional/keyword) arguments,
    and a weak reference to the implicit self object. This way storing this object
    don't express an ownership relation, and helps breaking strong reference cycles.
    """

    def __init__(self, bound_method, *bound_args, **bound_kwargs):
        self.func = bound_method.__func__
        self.wself = weakref.ref(bound_method.__self__)
        self.args = bound_args
        self.kwargs = bound_kwargs
        self.bound_front = False


    def bind_front(self):
        self.bound_front = True
        return self
        

    def make_selfarg(self):
        return self.wself()
        

    def __call__(self, *args, **kwargs):
        selfarg = self.make_selfarg()

        if not selfarg:
            return None
            
        if self.bound_front:
            args = list(self.args) + list(args)
            kwargs = dict(self.kwargs, **kwargs)
        else:
            args = list(args) + list(self.args)
            kwargs = dict(kwargs, **self.kwargs)
            
        return self.func(selfarg, *args, **kwargs)


    def __repr__(self):
        return "WeakMethod<%s, %s>" % (self.wself(), self.func)
        
        
    def rebind(self, *args, **kwargs):
        sself = self.wself()
        bound_method = self.func.__get__(sself)
        return self.__class__(bound_method, *args, **kwargs)


class WeakGeneratorMethod(WeakMethod):
    def make_selfarg(self):
        sself = self.wself()
        return weakref.proxy(sself) if sself else None

        
    def __repr__(self):
        return "WeakGeneratorMethod<%s, %s>" % (self.wself(), self.func)
    

class Metapoll(object):
    """
    The central object for event driven processing.
    """

    def __init__(self):
        self.poll = select.poll()
        self.readers_by_fd = {}
        self.writers_by_fd = {}
        self.timeout_handlers_by_key = {}
        self.next_timeout_key = 1


    def update(self, fd):
        """Updates the poll object's state from ours."""
        events = (select.POLLIN if fd in self.readers_by_fd else 0) |\
                (select.POLLOUT if fd in self.writers_by_fd else 0)

        if events:
            self.poll.register(fd, events)
        else:
            try:
                self.poll.unregister(fd)
            except KeyError:
                pass


    def register_writer(self, s, handler):
        """
        Registers an event handler to be called when the given socket becomes writable.
        """
        fd = (s if isinstance(s, int) else s.fileno())

        if handler:
            self.writers_by_fd[fd] = handler
        else:
            self.writers_by_fd.pop(fd, None)

        self.update(fd)


    def register_reader(self, s, handler):
        """
        Registers an event handler to be called when the given socket becomes readable.
        """
        fd = (s if isinstance(s, int) else s.fileno())

        if handler:
            self.readers_by_fd[fd] = handler
        else:
            self.readers_by_fd.pop(fd, None)

        self.update(fd)


    def register_timeout(self, timeout, handler, repeat=False):
        """
        Registers an event handler to be called when the given timeout expires.
        The timeout must be a datetime.timedelta object.
        Returns a handle that can be used to cancel this event.
        """
        #logging.debug("Registering a timeout of %s as %s" % (timeout, self.next_timeout_key))

        # OK, number timeout values are also accepted
        if isinstance(timeout, (int, float)):
            timeout = datetime.timedelta(seconds=timeout)

        now = datetime.datetime.now()
        delta = timeout if repeat else None
        self.timeout_handlers_by_key[self.next_timeout_key] = (now + timeout, handler, delta)
        self.next_timeout_key += 1

        return self.next_timeout_key - 1


    def unregister_timeout(self, key):
        """
        Cancels a previously scheduled timeout event, using its handle.
        """
        t = self.timeout_handlers_by_key.pop(key, None)

        if t:
            pass
            #deadline, handler, delta = t
            #now = datetime.datetime.now()
            #logging.debug("Unregistering a timeout of %s as %s" % (deadline - now, key))


    def do_poll(self):
        """
        An iteration of the event loop. Suspends execution until the next event,
        invokes the necessary event handlers, and returns.
        """
        if not self.timeout_handlers_by_key:
            timeout = None
        else:
            soonest_deadline = min([ x[0] for x in self.timeout_handlers_by_key.values() ])
            now = datetime.datetime.now()

            if soonest_deadline <= now:
                timeout = 0
            else:
                timeout = (soonest_deadline - now).total_seconds() * 1000 + 1  # Must round up

        #logging.debug("Readers: %r" % self.readers_by_fd)
        #logging.debug("Writers: %r" % self.writers_by_fd)
        #logging.debug("Timeout: %s" % timeout)
        events = self.poll.poll(timeout)
        #logging.debug("Events: %r" % events)

        for fd, event in events:
            # It is allowed for handlers to unregister each other

            if event & select.POLLIN:
                reader = self.readers_by_fd.get(fd)
                if reader:
                    #logging.debug("Notifying reader %r" % reader)
                    reader()

            if event & select.POLLOUT:
                writer = self.writers_by_fd.get(fd)
                if writer:
                    #logging.debug("Notifying writer %r" % writer)
                    writer()

        if timeout is not None:
            now = datetime.datetime.now()

            # It is allowed for handlers to unregister each other
            for key in list(self.timeout_handlers_by_key.keys()):
                info = self.timeout_handlers_by_key.get(key)
                if not info:
                    continue

                deadline, handler, delta = info
                if deadline > now:
                    #logging.debug("Not yet handler %s: %s > %s" % (key, deadline, now))
                    continue

                if delta is not None:
                    self.timeout_handlers_by_key[key] = (deadline + delta, handler, delta)
                else:
                    self.timeout_handlers_by_key.pop(key)

                #logging.debug("Invoking timeout handler %s" % key)
                handler()


class Listener(object):
    """
    Bind to a port and accept incoming connections, invoking a handler for each
    created socket.
    """

    def __init__(self, type, metapoll, addr, handler):
        self.type = type
        self.metapoll = metapoll
        self.handler = handler

        self.socket = self.create_socket()
        self.socket.setblocking(0)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(addr)
        self.socket.listen(10)

        self.metapoll.register_reader(self.socket, WeakMethod(self.incoming))

        # This is the actual address (allocated by the kernel if necessary)
        self.addr = self.socket.getsockname()
        logging.info("Listening for %s on %s" % (type, self.addr))


    def get_socket(self):
        return self.socket


    def incoming(self):
        """Handles new connections to the listening socket"""
        s, addr = self.socket.accept()
        s.setblocking(0)

        id = self.identify(s, addr)
        logging.debug("Accepted %s connection from %s" % (self.type, id))

        self.handler(s, id)


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
        super(TcpListener, self).__init__("TCP", *args)


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

        super(UnixListener, self).__init__("UNIX", *args)

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


class Reconnector(object):
    """
    Creates a stream connection. If fails, it retries in intervals until succeeds, then
    call the given callback with the created socket, and stops. Must be restarted manually
    after the user thinks the connection is broken.
    """
    def __init__(self, type, metapoll, addr, timeout, connected_handler):
        self.type = type
        self.metapoll = metapoll
        self.addr = addr
        self.timeout = timeout
        self.connected_handler = connected_handler
        self.socket = None
        self.reconnecting_handle = None
        self.local_addr = (os.environ.get('VFTESTHOST', ''), 0)


    def start(self):
        """
        Initiate a connection.
        """
        # If currently in progress, just wait for that
        if self.socket:
            return

        # If currently scheduled, then cancel that and try immediately
        if self.reconnecting_handle:
            self.metapoll.unregister_timeout(self.reconnecting_handle)
        else:
            logging.info("Started %s reconnection to %s" % (self.type, self.addr))

        if self.timeout:
            self.reconnecting_handle = self.metapoll.register_timeout(
                    self.timeout, WeakMethod(self.reconnect), repeat=True)

        self.reconnect()


    def reconnect(self):
        """Attempt a reconnection."""

        #if self.socket:
        #    logging.debug("%s connection attempt to %s timed out, retrying now" %
        #            (self.type, self.addr))
        #else:
        #    logging.debug("Attempting %s reconnection to %s" % (self.type, self.addr))

        self.socket = self.create_socket()
        self.socket.setblocking(0)
        self.socket.bind(self.local_addr)

        try:
            self.socket.connect(self.addr)
        except socket.error as e:
            if e.errno != errno.EINPROGRESS:
                logging.debug("%s connection attempt to %s failed immediately, %s retry" %
                        (self.type, self.addr, "will" if self.timeout else "won't"))
                self.socket = None
                return
        else:
            logging.debug("%s connect unexpectedly succeeded" % self.type)

        self.metapoll.register_writer(self.socket, WeakMethod(self.connected))


    def connected(self):
        """Handle the results of a connection attempt."""
        self.metapoll.register_writer(self.socket, None)

        if self.socket.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR) != 0:
            logging.debug("%s connection attempt to %s failed, %s retry" %
                    (self.type, self.addr, "will" if self.timeout else "won't"))
            self.socket = None
            return

        logging.debug("Successful %s reconnection to %s" % (self.type, self.addr))

        if self.timeout:
            self.metapoll.unregister_timeout(self.reconnecting_handle)

        s = self.socket
        self.socket = None

        if self.connected_handler:
            self.connected_handler(s)


    def create_socket(self):
        """To be overloaded"""
        raise NotImplementedError()


class TcpReconnector(Reconnector):
    def __init__(self, *args):
        super(TcpReconnector, self).__init__("TCP", *args)


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

    def __init__(self, metapoll, socket, keepalive_interval=None):
        self.metapoll = metapoll
        self.socket = socket

        self.outgoing_buffer = b""
        self.outgoing_queue = []
        self.incoming_buffer = b""
        self.incoming_message = None

        self.process_handler = None
        self.disconnect_handler = None
        self.exhausted_handler = None
        self.flushed_handler = None

        self.metapoll.register_reader(self.socket, WeakMethod(self.readable))
        self.metapoll.register_writer(self.socket, WeakMethod(self.writable))

        if keepalive_interval:
            self.metapoll.register_timeout(keepalive_interval, WeakMethod(self.keepalive), repeat=True)


    def __del__(self):
        self.metapoll.register_reader(self.socket, None)
        self.metapoll.register_writer(self.socket, None)


    def keepalive(self):
        if not self.outgoing_buffer:
            # Send empty lines now and then
            self.outgoing_buffer = b"\n"
            self.metapoll.register_writer(self.socket, WeakMethod(self.writable))


    def register(self, process_handler, disconnect_handler, exhausted_handler, flushed_handler):
        """Set the event handling callbacks"""
        self.process_handler = process_handler
        self.disconnect_handler = disconnect_handler
        self.exhausted_handler = exhausted_handler
        self.flushed_handler = flushed_handler


    def writable(self):
        """Called when the socket becomes writable."""
        if self.outgoing_buffer:
            try:
                sent = self.socket.send(self.outgoing_buffer)
            except IOError as e:
                # Note: SCTP sockets doesn't raise the specialized socket.error!
                logging.error("Socket error while sending: %s" % e)
                self.metapoll.register_writer(self.socket, None)

                if self.disconnect_handler:
                    self.disconnect_handler()

                return

            self.outgoing_buffer = self.outgoing_buffer[sent:]

        if not self.outgoing_buffer:
            if self.outgoing_queue:
                self.outgoing_buffer = self.outgoing_queue.pop(0)
            else:
                self.metapoll.register_writer(self.socket, None)

                if self.flushed_handler:
                    self.flushed_handler()


    def get_incoming_message(self):
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
                #logging.info("Incoming message with header %r" % header)
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

                logging.error("Socket error while receiving: %s" % e)
                self.metapoll.register_reader(self.socket, None)
                disconnected = True
                break

            if not recved:
                self.metapoll.register_reader(self.socket, None)
                exhausted = True
                break

            self.incoming_buffer += recved

        # Since the user will get no further notifications if some incoming messages remain
        # buffered, all available messages must be got. To help the user not to forget this,
        # we'll return all available messages in a list.
        msgs = []

        while True:
            msg = self.get_incoming_message()

            if msg:
                msgs.append(msg)
            else:
                break

        if msgs:
            if self.process_handler:
                self.process_handler(msgs)
            else:
                logging.warning("Ignoring %d messages due to the lack of handler!" % len(msgs))

        if exhausted and self.exhausted_handler:
            self.exhausted_handler()

        if disconnected and self.disconnect_handler:
            self.disconnect_handler()


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
            self.metapoll.register_writer(self.socket, WeakMethod(self.writable))

