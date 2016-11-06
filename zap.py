import select
import weakref
import heapq
import datetime
import collections
import logging

from util import Loggable


class Plug:
    def __init__(self, callable, kwargs):
        self.slot = None
        self.callable = callable
        #self.args = args
        self.kwargs = kwargs
        
        
    def __del__(self):
        self.unplug()
        
        
    def __call__(self, *args):
        method = self.callable()
        
        if method:
            method(*args, **self.kwargs)
            
        
    def unplug(self):
        try:
            if self.slot:
                self.slot.unplug(self)
        except ReferenceError:
            pass


class Slot:
    def __init__(self):
        self.plugs = set()
        
        
    def __del__(self):
        self.unplug_all()
        
        
    def inplug(self, plug):
        plug.slot = weakref.proxy(self)
        self.plugs.add(plug)
        self.postplug()
        
        
    def unplug(self, plug):
        self.preunplug()
        self.plugs.remove(plug)
        plug.slot = None
    
    
    def unplug_all(self):
        plugs = set(self.plugs)
        
        for plug in plugs:
            plug.unplug()
            
        return plugs
        

    def plug(self, method, **kwargs):
        plug = Plug(weakref.WeakMethod(method), kwargs)
        self.inplug(plug)
        return plug

    
    def postplug(self):
        pass
        
        
    def preunplug(self):
        pass


    def zap(self):
        for plug in self.plugs:
            schedule(plug)


class EventSlot(Slot):
    def __init__(self):
        Slot.__init__(self)
        
        self.queue = []
        

    def postplug(self):
        if len(self.plugs) == 1:
            queue = self.queue
            self.queue = []
            
            for args in queue:
                self.zap(*args)

    
    def zap(self, *args):
        if self.plugs:
            for plug in self.plugs:
                schedule(lambda plug=plug: plug(*args))
        else:
            self.queue.append(args)


class KernelSlot(Slot):
    def __init__(self, key):
        Slot.__init__(self)
        
        self.key = key
        
        
    def postplug(self):
        if len(self.plugs) == 1:
            kernel.register(self.key)
        
        
    def preunplug(self):
        if len(self.plugs) == 1:
            kernel.unregister(self.key)
            
            
class Kernel(Loggable):
    def __init__(self):
        self.poll = select.poll()
        self.registered_keys = set()
        self.slots_by_key = {}
        self.time_heap = []


    def update_poll(self, fd):
        """Updates the poll object's state from ours."""
        events = (
            (select.POLLIN if (fd, False) in self.registered_keys else 0) |
            (select.POLLOUT if (fd, True) in self.registered_keys else 0)
        )

        if events:
            self.poll.register(fd, events)
        else:
            try:
                self.poll.unregister(fd)
            except KeyError:
                pass

        
    def register(self, key):
        self.registered_keys.add(key)
        
        if key[1] in (False, True):
            self.update_poll(key[0])
            

    def unregister(self, key):
        self.registered_keys.remove(key)
        
        if key[1] in (False, True):
            self.update_poll(key[0])


    def add_slot(self, key):
        slot = self.slots_by_key.get(key)
        
        if not slot:
            slot = KernelSlot(key)
            self.slots_by_key[key] = slot
            
            if key[1] not in (False, True):
                heapq.heappush(self.time_heap, key)
                
        return weakref.proxy(slot)
        
        
    def time_slot(self, delay, repeat=False):
        if isinstance(delay, (int, float)):
            delay = datetime.timedelta(seconds=delay)

        # Align to milliseconds to improve the polling performance of bulk registrations
        deadline = datetime.datetime.now() + delay
        usecs = 1000 - deadline.microsecond % 1000
        deadline += datetime.timedelta(microseconds=usecs)

        delta = delay if repeat else datetime.timedelta()  # Must be comparable to others
        key = (deadline, delta)
        
        return self.add_slot(key)
        
        
    def get_earliest_key(self):
        key = self.time_heap[0]
        
        if key in self.registered_keys:
            return key
        else:
            # Clean up unregistered time slot
            heapq.heappop(self.time_heap)
            self.slots_by_key.pop(key)
            return None
        

    def do_poll(self):
        timeout = None
        
        while self.time_heap:
            key = self.get_earliest_key()
            if not key:
                continue
                
            deadline, delta = key
            now = datetime.datetime.now()
                
            if deadline <= now:
                timeout = 0
            else:
                timeout = (deadline - now).total_seconds() * 1000 + 1  # Must round up
                
            break
                    
        #logging.debug("Readers: %r" % self.readers_by_fd)
        #logging.debug("Writers: %r" % self.writers_by_fd)
        #self.logger.debug("Timeout: %s" % timeout)
        events = self.poll.poll(timeout)
        now = datetime.datetime.now()
        #self.logger.debug("Events: %r" % events)

        for fd, event in events:
            if event & select.POLLIN:
                self.slots_by_key[(fd, False)].zap()

            if event & select.POLLOUT:
                self.slots_by_key[(fd, True)].zap()

        while self.time_heap:
            key = self.get_earliest_key()
            if not key:
                continue
                
            deadline, delta = key
            if deadline > now:
                break
                
            slot = self.slots_by_key[key]
            slot.zap()
            plugs = slot.unplug_all()  # also unregisters
            self.get_earliest_key()    # also pops key
            
            if delta:
                new_key = (deadline + delta, delta)
                #self.logger.debug("Rescheduling task to: %s" % new_key[0])
                new_slot = self.add_slot(new_key)
                
                for plug in plugs:
                    new_slot.inplug(plug)  # registers new_slot if necessary
                    
        if len(self.slots_by_key) > 2 * len(self.registered_keys):
            self.logger.debug("Kernel slot maintenance")
            self.slots_by_key = { key: self.slots_by_key[key] for key in self.registered_keys }
            self.time_heap = [ key for key in self.time_heap if key in self.registered_keys ]
            heapq.heapify(self.time_heap)


class Plan(Loggable):
    def __init__(self):
        Loggable.__init__(self)

        self.generator = None
        self.event_queue = []
        self.event_slot = Slot()
        
        self.timeout_plug = None
        self.event_plug = None


    def finished(self, error):
        pass
        

    def start(self, *args, **kwargs):
        if self.generator:
            raise Exception("Plan already started!")
        
        # Storing the generator while it also has a strong reference to us is
        # a reference loop, but it can be broken by our owner.
        self.logger.debug("Starting plan.")
        self.generator = self.plan(*args, **kwargs)
        if not self.generator:
            raise Exception("Couldn't create plan generator!")
            
        #self.timeout_plug.zap()
        self.resume(None)


    def abort(self):
        if self.generator:
            try:
                self.generator.close()
            except Exception as e:
                self.logger.warning("Plan aborted with %s." % e)
                self.finished(e)
            else:
                self.logger.warning("Plan aborted.")
                self.finished(None)


    def queue(self, event):
        self.event_queue.append(event)
        self.event_slot.zap()


    def suspend(self, timeout):
        if timeout is not None:
            self.timeout_plug = time_slot(timeout).plug(self.resume, value=None)

        self.logger.debug("Suspending plan.")
    
        try:
            yield
        except Exception as e:
            self.logger.debug("Aborting plan.")
            raise e
        else:
            self.logger.debug("Resuming plan.")

        if timeout is not None:
            self.timeout_plug.unplug()


    def wait_event(self, timeout=None):
        if not self.event_queue:
            self.event_plug = self.event_slot.plug(self.resume, value=None)
            
            yield from self.suspend(timeout)
            
            self.event_plug.unplug()

            if not self.event_queue:
                self.logger.debug("Timed out waiting for an event.")
                return None

        self.logger.debug("Unqueueing event.")
        event = self.event_queue.pop(0)
        return event


    def sleep(self, timeout):
        yield from self.suspend(timeout)
        
        
    def resume(self, value):
        # TODO: seems like this value is not used anymore, so it can be
        # renamed to exception, and use it to abort the plan with it
        
        if not self.generator:
            raise Exception("Plan already finished!")
    
        try:
            # This just returns if the plan is suspended again, or
            # raises StopIteration if it ended.
            self.generator.send(value)
        except StopIteration as e:
            self.logger.debug("Terminated plan.")
            self.generator = None
            
            if e.value:
                self.logger.debug("Plan return value ignored!")
            
            self.finished(None)
        except Exception as e:
            self.logger.warning("Aborted plan with exception!", exc_info=True)
            self.generator = None
            
            self.finished(e)


    def plan(self):
        raise NotImplementedError()


kernel = Kernel()
kernel.set_oid("kernel")


def time_slot(delay, repeat=False):
    return kernel.time_slot(delay, repeat)


def read_slot(socket):
    if socket.gettimeout() != 0.0:
        raise Exception("Socket is still blocking!")
        
    return kernel.add_slot((socket.fileno(), False))


def write_slot(socket):
    if socket.gettimeout() != 0.0:
        raise Exception("Socket is still blocking!")

    return kernel.add_slot((socket.fileno(), True))


# Seems like we need to keep this ordered, because sometimes we want to
# schedule multiple plugs in a proper order (see async_net.Connection),
# and messing up the order would make things harder.
scheduled_tasks = collections.OrderedDict()

def schedule(task):
    global scheduled_tasks
    
    #kernel.logger.debug("Scheduling task")
    scheduled_tasks[task] = None
    
def loop():
    global scheduled_tasks
    
    while True:
        while scheduled_tasks:
            # Tasks may be scheduled while we run others
            tasks = scheduled_tasks
            scheduled_tasks = collections.OrderedDict()
        
            for task in tasks:
                #kernel.logger.debug("Running task...")
                task()
        
        #kernel.logger.debug("Polling")
        kernel.do_poll()
