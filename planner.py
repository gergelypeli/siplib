# This needs at least Python 3.3 because of yield from!

from async import Metapoll, Weak
import collections


PlannedEvent = collections.namedtuple("PlannedEvent", [ "tag", "event" ])
PlannedEvent.__new__.__defaults__ = (None,)


class Planner(object):
    def __init__(self, metapoll, generator_method, finish_handler=None):
        self.metapoll = metapoll
        self.generator = generator_method.__func__(Weak(generator_method.__self__), Weak(self))
        self.finish_handler = finish_handler
        self.timeout_handle = None
        self.event_queue = []
        
        print("Starting plan.")
        self.resume(None)


    def __del__(self):
        # Not necessary, self.generator.__del__ does the same
        if self.generator:
            self.generator.close()
        
        if self.timeout_handle:
            self.metapoll.unregister_timeout(self.timeout_handle)
                    
        
    def suspend(self, expect=None, timeout=None, strict=False):
        for i, planned_event in enumerate(self.event_queue):
            if not expect or expect == planned_event.tag:
                print("Unqueueing planned event '%s'." % planned_event.tag)
                return self.event_queue.pop(i)
            
        while True:
            # self is actually a weak self, because this function is called from within the plan
            if timeout is not None:
                handler = lambda: self.resume(PlannedEvent("timeout"))
                self.timeout_handle = self.metapoll.register_timeout(timeout, handler)

            print("Suspending plan.")
            planned_event = yield
            print("Resuming plan.")

            if timeout is not None:
                self.metapoll.unregister_timeout(self.timeout_handle)
                self.timeout_handle = None
        
            if not expect or expect == planned_event.tag:
                print("Got planned event '%s'." % planned_event.tag)
                return planned_event

            if strict:
                raise Exception("Expected planned event '%s', got '%s'!" % (expect, planned_event.tag))
                
            if planned_event.tag == "timeout":
                raise Exception("Timeout before planned event '%s'!" % expect)
                
            print("Queueing planned event '%s'." % planned_event.tag)
            self.event_queue.append(planned_event)


    def sleep(self, timeout):
        yield from self.suspend(expect="timeout", timeout=timeout)
        
        
    def resume(self, planned_event):
        if not self.generator:
            raise Exception("Plan already finished!")
    
        try:
            if isinstance(planned_event, Exception):
                self.generator.throw(planned_event)
            else:
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
        
    del s
