# This needs at least Python 3.3 because of yield from!

from async import Metapoll, Weak, WeakMethod
import collections


PlannedEvent = collections.namedtuple("PlannedEvent", [ "tag", "event" ])
PlannedEvent.__new__.__defaults__ = (None,)


class Planner(object):
    def __init__(self, metapoll, generator_method, finish_handler=None):
        self.metapoll = metapoll
        self.generator = generator_method.__func__(Weak(generator_method.__self__), Weak(self))
        self.finish_handler = finish_handler
        self.timeout_handle = None
        
        print("Starting plan.")
        self.resume(None)


    def __del__(self):
        if self.generator:
            self.generator.close()
        
        if self.timeout_handle:
            self.metapoll.unregister_timeout(self.timeout_handle)
                    
        
    def poll(self, timeout=None):
        # self is actually a weak self, because this function is called from within the plan

        if timeout is not None:
            self.timeout_handle = self.metapoll.register_timeout(timeout, lambda: self.resume(PlannedEvent("timeout")))

        print("Suspending plan.")
        planned_event = yield
        print("Resuming plan.")
        
        return planned_event
        
        
    def expect(self, tag, timeout=None):
        planned_event = yield from self.poll(timeout=timeout)
        
        if planned_event.tag != tag:
            raise Exception("Expected %s, got %s!" % (tag, planned_event.tag))

        return planned_event.event
    
        
    def sleep(self, timeout):
        yield from self.expect("timeout", timeout=timeout)
        
        
    def resume(self, planned_event):
        try:
            if self.timeout_handle:
                self.metapoll.unregister_timeout(self.timeout_handle)
                self.timeout_handle = None
                
            # This just returns if the plan is suspended again, or
            # raises StopIteration if it ended.
            self.generator.send(planned_event)
        except StopIteration:
            print("Terminated plan.")
            self.generator = None
            
            if self.finish_handler:
                self.finish_handler()
        
        
def main():
    class Planned(object):
        def __init__(self, metapoll):
            self.planner = Planner(metapoll, self.plan)
        
    
        def plan(self, planner):
            print("One.")
            yield from planner.poll(timeout=2)
            print("Two.")
            yield from planner.poll(timeout=2)
            print("Three.")
        

    metapoll = Metapoll()
    s = Planned(metapoll)

    while True:
        metapoll.do_poll()
