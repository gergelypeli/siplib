from async import WeakMethod
from util import Loggable


#PlannedEvent = collections.namedtuple("PlannedEvent", [ "tag", "event" ])
#PlannedEvent.__new__.__defaults__ = (None,)


class Planned(Loggable):
    def __init__(self, metapoll, generator_method, finish_handler=None):
        Loggable.__init__(self)

        self.metapoll = metapoll
        self.generator_method = generator_method
        self.finish_handler = finish_handler

        self.generator = None
        self.is_executing = False
        self.timeout_handle = None
        self.event_queue = []


    def __del__(self):
        self.abort()
        

    def start_plan(self, *args, **kwargs):  # Hm, must have renamed it, collided with Leg.start
        if self.generator:
            raise Exception("Plan already started!")
        
        self.logger.debug("Starting plan.")
        self.generator = self.generator_method(*args, **kwargs)
        if not self.generator:
            raise Exception("Couldn't create plan generator!")
        self.resume(None, None)


    def abort(self):
        if self.generator:
            try:
                self.generator.close()
            except Exception as e:
                self.logger.warning("Force aborted plan.")
                if self.finish_handler:
                    self.finish_handler(e)
            else:
                self.logger.warning("Force terminated plan.")
                if self.finish_handler:
                    self.finish_handler(None)
        
        if self.timeout_handle:
            self.metapoll.unregister_timeout(self.timeout_handle)
                    
        
    def suspend(self, expect=None, timeout=None, strict=False):
        for i, planned_event in enumerate(self.event_queue):
            tag, event = planned_event
            
            if not expect or expect == tag:
                self.logger.debug("Unqueueing event '%s'." % tag)
                self.event_queue.pop(i)
                return tag, event
            elif strict:
                raise Exception("Expected event '%s', got '%s'!" % (expect, tag))
            
        while True:
            # self is actually a weak self, because this function is called from within the plan
            # But getting a bound method of a proxy binds to the strong object!
            # So self.resume can be passed to WeakMethod to weaken it again!
            if timeout is not None:
                handler = WeakMethod(self.resume, "timeout", None)
                self.timeout_handle = self.metapoll.register_timeout(timeout, handler)

            self.logger.debug("Suspending plan.")
            tag, event = yield
            
            if tag == "exception":
                self.logger.debug("Aborting plan.")
                raise event
            else:
                self.logger.debug("Resuming plan.")

            if timeout is not None:
                self.metapoll.unregister_timeout(self.timeout_handle)
                self.timeout_handle = None
        
            if not expect or expect == tag:
                self.logger.debug("Got event '%s'." % tag)
                return tag, event

            if strict:
                raise Exception("Expected event '%s', got '%s'!" % (expect, tag))
                
            if tag == "timeout":
                raise Exception("Timeout before event '%s'!" % expect)
                
            self.logger.debug("Queueing event '%s'." % tag)
            self.event_queue.append((tag, event))


    def sleep(self, timeout):
        yield from self.suspend(expect="timeout", timeout=timeout)
        
        
    def resume(self, tag, event):
        if not self.generator:
            raise Exception("Plan already finished!")
        elif self.is_executing:
            # During the wilderness of event handling, the code may try to resume
            # a Plan that is already executing, and it results in a
            # ValueError: generator already executing
            # so we must work around that thing using our is_executing flag.
            self.logger.debug("Already executing, queueing event '%s'." % tag)
            self.event_queue.append((tag, event))
            return
    
        try:
            self.is_executing = True
            
            # This just returns if the plan is suspended again, or
            # raises StopIteration if it ended.
            self.generator.send((tag, event) if tag else None)
                
            self.is_executing = False
        except StopIteration as e:
            self.logger.debug("Terminated plan.")
            self.generator = None
            self.is_executing = False
            
            if e.value:
                self.logger.debug("Plan return value ignored!")
            
            if self.finish_handler:
                #self.logger.debug("Plan finish handler start")
                self.finish_handler(None)
                #self.logger.debug("Plan finish handler end")
        except Exception as e:
            self.logger.warning("Aborted plan with exception!", exc_info=True)
            self.generator = None
            self.is_executing = False
            
            if self.finish_handler:
                self.finish_handler(e)
