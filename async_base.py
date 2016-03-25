import weakref
import select
import datetime


__all__ = [
    'WeakMethod', 'WeakGeneratorMethod', 'Weak', 'Metapoll'
]


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
        self.store_self(bound_method.__self__)
        self.args = bound_args
        self.kwargs = bound_kwargs
        self.bound_front = False


    def bind_front(self):
        self.bound_front = True
        return self
        
        
    def store_self(self, myself):
        self.wself = weakref.ref(myself)
        

    def load_self(self):
        return self.wself()
        

    def __call__(self, *args, **kwargs):
        selfarg = self.load_self()

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
        return "WeakMethod<%s, %s>" % (self.load_self(), self.func)
        
        
    def rebind(self, *args, **kwargs):
        sself = self.load_self()
        bound_method = self.func.__get__(sself)
        return self.__class__(bound_method, *args, **kwargs)


class WeakGeneratorMethod(WeakMethod):
    def load_self(self):
        # Pass a weak self to the generator method
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


    def register_timeout(self, timeout, handler, repeat=False, align=None):
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
        if align:
            # Make sure it is aligned to improve the polling performance of bulk registrations
            usecs = align.microseconds - now.microsecond % align.microseconds
            now += datetime.timedelta(microseconds=usecs)
        
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
