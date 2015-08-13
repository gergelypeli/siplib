# This needs at least Python 3.3 because of yield from!

from async import Metapoll, Weak, WeakMethod


class Planner(object):
    def __init__(self, metapoll, generator_method, finish_handler=None):
        self.metapoll = metapoll
        self.generator = generator_method.__func__(Weak(generator_method.__self__), Weak(self))
        self.finish_handler = finish_handler
        self.timeout_handle = None
        
        print("Starting plan.")
        self.resume(None)


    def __del__(self):
        if self.timeout_handle:
            self.metapoll.unregister_timeout(self.timeout_handle)
                    
        
    def poll(self, timeout=None):
        # self is actually a weak self, because this function is called from within the plan

        if timeout is not None:
            self.timeout_handle = self.metapoll.register_timeout(timeout, lambda: self.resume(("timeout", None)))

        print("Suspending plan.")
        resumed_value = yield
        print("Resuming plan.")
        
        return resumed_value
        
        
    def expect(self, type, timeout=None):
        event, details = yield from self.poll(timeout=timeout)
        assert event == type
        return details
    
        
    def sleep(self, timeout=None):
        yield from self.expect("timeout", timeout=timeout)
        
        
    def resume(self, resumed_value):
        try:
            if self.timeout_handle:
                self.metapoll.unregister_timeout(self.timeout_handle)
                self.timeout_handle = None
                
            # This just returns if the plan is suspended again, or
            # raises StopIteration if it ended.
            self.generator.send(resumed_value)
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
