import select
import weakref
import heapq
import datetime
import collections
import sys

from util import Loggable, Oid


class Plug:
    def __init__(self, weak_method, kwargs):
        self.slot = None
        self.weak_method = weak_method
        self.kwargs = kwargs
        
        
    def __del__(self):
        self.unplug()
        
            
    def __call__(self, *args):
        method = self.weak_method()
        
        if method:
            method(*args, **self.kwargs)

        
    def zap(self, *args):
        schedule(lambda: self(*args))


    def unplug(self):
        try:
            if self.slot:
                self.slot.plug_out(self)
        except ReferenceError:
            pass


class InstaPlug(Plug):
    def zap(self, *args):
        self(*args)
            

class Slot:
    def __init__(self):
        self.plugs = set()
        
        
    def __del__(self):
        for plug in set(self.plugs):
            plug.unplug()
        
        
    def plug_in(self, plug):  # to be called by Slot only
        plug.slot = weakref.proxy(self)
        self.plugs.add(plug)
        self.post_plug_in()
        
        
    def plug_out(self, plug):
        self.pre_plug_out()  # to be called by Slot only
        self.plugs.remove(plug)
        plug.slot = None
    
    
    def plug(self, method, **kwargs):
        plug = Plug(weakref.WeakMethod(method), kwargs)
        self.plug_in(plug)
        return plug


    def instaplug(self, method, **kwargs):
        plug = InstaPlug(weakref.WeakMethod(method), kwargs)
        self.plug_in(plug)
        return plug

    
    def post_plug_in(self):
        pass
        
        
    def pre_plug_out(self):
        pass


    def zap(self, *args):
        # Plugs may unplug themselves during zapping, so this must be done carefully
        for plug in list(self.plugs):
            if plug in self.plugs:
                plug.zap(*args)


class EventSlot(Slot):
    def __init__(self):
        Slot.__init__(self)
        
        self.queue = []
        

    def post_plug_in(self):
        if len(self.plugs) == 1:
            queue = self.queue
            self.queue = []
            
            for args in queue:
                self.zap(*args)

    
    def zap(self, *args):
        if not self.plugs:
            self.queue.append(args)
        else:
            Slot.zap(self, *args)


class KernelSlot(Slot):
    def __init__(self, key):
        Slot.__init__(self)
        
        self.key = key
        
        
    def post_plug_in(self):
        if len(self.plugs) == 1:
            kernel.register(self.key)
        
        
    def pre_plug_out(self):
        if len(self.plugs) == 1:
            kernel.unregister(self.key)


class Kernel(Loggable):
    def __init__(self):
        Loggable.__init__(self)
        
        self.poll = select.poll()
        self.registered_keys = set()
        self.slots_by_key = {}
        self.time_heap = []
        self.never_slot = Slot()


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


    def file_slot(self, socket, write):
        # Damn, multiprocessing.Pipe is different
        if hasattr(socket, "gettimeout") and socket.gettimeout() != 0.0:
            raise Exception("Socket is still blocking!")
        
        key = (socket.fileno(), write)
        
        return self.add_slot(key)
        
        
    def time_slot(self, delay, repeat=False):
        if delay is None:
            return self.never_slot
            
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
                
            if event & ~(select.POLLIN | select.POLLOUT):
                self.logger.error("Unexpected file descriptor event for fd %d, purging slot!" % fd)
                # Keep the Slot object, even registered, because it should eventually be
                # destroyed and unregistered then.
                self.poll.unregister(fd)

        while self.time_heap:
            key = self.get_earliest_key()
            if not key:
                continue
                
            deadline, delta = key
            if deadline > now:
                break
                
            slot = self.slots_by_key[key]
            slot.zap()
            
            plugs = set(slot.plugs)
            for plug in plugs:
                plug.unplug()  # also unregisters the slot when the last plug is unplugged
                
            self.get_earliest_key()    # also pops key
            
            if delta:
                new_key = (deadline + delta, delta)
                #self.logger.debug("Rescheduling task to: %s" % new_key[0])
                new_slot = self.add_slot(new_key)
                
                for plug in plugs:
                    new_slot.plug_in(plug)  # registers new_slot if necessary

        if len(self.slots_by_key) > 2 * len(self.registered_keys):
            self.logger.debug("Kernel slot maintenance")
            self.slots_by_key = { key: self.slots_by_key[key] for key in self.registered_keys }
            self.time_heap = [ key for key in self.time_heap if key in self.registered_keys ]
            heapq.heapify(self.time_heap)


class Plan(Loggable):
    def __init__(self):
        Loggable.__init__(self)

        self.generator = None
        self.resume_plugs = None
        self.resume_value = None
        self.finished_slot = EventSlot()


    def start(self, generator):
        if self.generator:
            raise Exception("Plan already started!")
        
        self.logger.debug("Starting plan.")
        self.generator = generator
        schedule(self.resume)


    def is_running(self):
        return self.generator is not None


    def finished(self, error):
        self.generator = None
        self.resume_plugs = None
        self.resume_value = None
        self.finished_slot.zap(error)
        

    def abort(self):
        if self.generator:
            try:
                self.generator.close()
            except Exception as e:
                self.logger.warning("Plan aborted with %s." % e)
                self.finished(e)
            else:
                # FIXME: commented out for stupid file logging reasons only
                #self.logger.warning("Plan aborted.")
                self.finished(None)


    def zapped(self, *args, slot_index):
        for plug in self.resume_plugs:
            plug.unplug()
            
        self.resume_plugs = None
        self.resume_value = (slot_index, args)
        schedule(self.resume)  # strong self is OK for scheduling now
        
        
    def resume(self):
        if not self.generator:
            raise Exception("Plan already finished!")
    
        try:
            # This just returns when the plan is suspended again, or
            # raises StopIteration if it terminated.
            value = self.resume_value
            self.resume_value = None
            
            if value is None:
                self.logger.debug("Starting plan.")
            else:
                self.logger.debug("Resuming plan by slot %d." % value[0])
                
            slots = self.generator.send(value)
            if not isinstance(slots, tuple): slots = (slots,)
            
            self.logger.debug("Suspended plan for %d slots." % len(slots))
            # This is tricky, since we use instaplugs with EventSlots, so they can
            # fire as soon as plugged in. So zapped must work during this loop, and we must
            # stop if we look like resumed. This would be nicer if we could create the Plug
            # first, then attach it to the Slot.
            self.resume_plugs = []
            
            for i, slot in enumerate(slots):
                plug = slot.instaplug(self.zapped, slot_index=i)
                
                if self.resume_plugs is None:
                    # Seems like we were instazapped here, get out, we're already scheduled
                    plug.unplug()
                    break
                    
                self.resume_plugs.append(plug)
                
        except StopIteration as e:
            self.logger.debug("Terminated plan.")
            if e.value:
                self.logger.warning("Plan return value ignored!")
                
            self.finished(None)
        except Exception as e:
            self.logger.warning("Aborted plan with exception!", exc_info=True)
            self.finished(e)


class Planned(Loggable):  # Oops, we may call base class methods twice
    def __init__(self):
        Loggable.__init__(self)
    
        # Keep us as lightweight as possible
        self.event_plan = None
        self.event_slot = None


    def __del__(self):
        self.abort_plan()


    def start_plan(self):
        generator = self.plan()
        
        if generator:
            self.event_plan = Plan()
            self.event_plan.set_oid(self.oid.add("plan"))
            self.event_plan.finished_slot.plug(self.plan_finished)
            self.event_plan.start(generator)

            self.event_slot = EventSlot()
            

    def send_event(self, *args):
        self.event_slot.zap(*args)


    def sleep(self, timeout):
        yield time_slot(timeout)
        

    def wait_event(self, timeout=None):  # TODO
        slot_index, args = yield time_slot(timeout), self.event_slot
        
        return args if slot_index == 1 else None


    def wait_input(self, prompt, timeout=None):
        print(prompt)
        
        slot_index, args = yield time_slot(timeout), read_slot(sys.stdin)
        
        return sys.stdin.readline() if slot_index == 1 else None


    def is_plan_running(self):
        return self.event_plan and self.event_plan.is_running()
        

    def abort_plan(self):
        if self.is_plan_running():
            self.event_plan.abort()


    def plan_finished(self, error):
        raise NotImplementedError()

        
    def plan(self):
        return None




kernel = Kernel()
kernel.set_oid(Oid("kernel"))


def time_slot(delay, repeat=False):
    return kernel.time_slot(delay, repeat)


def read_slot(socket):
    return kernel.file_slot(socket, False)


def write_slot(socket):
    return kernel.file_slot(socket, True)


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
                task()
        
        #kernel.logger.debug("Polling")
        try:
            kernel.do_poll()
        except KeyboardInterrupt:
            break
