# This needs at least Python 3.3 because of yield from!

import collections
from async import Metapoll, Weak, WeakMethod
from util import Logger
#import logging

#logger = logging.getLogger(__name__)


PlannedEvent = collections.namedtuple("PlannedEvent", [ "tag", "event" ])
PlannedEvent.__new__.__defaults__ = (None,)


class Planner(object):
    def __init__(self, metapoll, generator_method, finish_handler=None, error_handler=None):
        self.metapoll = metapoll
        self.generator_method = generator_method
        self.finish_handler = finish_handler
        self.error_handler = error_handler

        self.generator = None
        self.timeout_handle = None
        self.event_queue = []
        
        self.logger = Logger()


    def set_oid(self, oid):
        self.logger.set_oid(oid)


    def start(self, *args, **kwargs):
        if self.generator:
            raise Exception("Plan already started!")
        
        self.logger.debug("Starting plan.")
        self.generator = self.generator_method(Weak(self), *args, **kwargs)
        self.resume(None)


    def __del__(self):
        if self.generator:
            try:
                self.generator.close()
            except Exception as e:
                self.logger.warning("Force aborted plan.")
                if self.error_handler:
                    self.error_handler(e)
            else:
                self.logger.warning("Force terminated plan.")
                if self.error_handler:
                    self.error_handler(None)  # TODO: figure something out here!
        
        if self.timeout_handle:
            self.metapoll.unregister_timeout(self.timeout_handle)
                    
        
    def suspend(self, expect=None, timeout=None, strict=False):
        for i, planned_event in enumerate(self.event_queue):
            if not expect or expect == planned_event.tag:
                self.logger.debug("Unqueueing planned event '%s'." % planned_event.tag)
                return self.event_queue.pop(i)
            
        while True:
            # self is actually a weak self, because this function is called from within the plan
            # But getting a bound method of a proxy binds to the strong object!
            # So self.resume can be passed to WeakMethod to weaken it again!
            if timeout is not None:
                handler = WeakMethod(self.resume, PlannedEvent("timeout"))
                self.timeout_handle = self.metapoll.register_timeout(timeout, handler)

            self.logger.debug("Suspending plan.")
            planned_event = yield
            self.logger.debug("Resuming plan.")

            if timeout is not None:
                self.metapoll.unregister_timeout(self.timeout_handle)
                self.timeout_handle = None
        
            if not expect or expect == planned_event.tag:
                self.logger.debug("Got planned event '%s'." % planned_event.tag)
                return planned_event

            if strict:
                raise Exception("Expected planned event '%s', got '%s'!" % (expect, planned_event.tag))
                
            if planned_event.tag == "timeout":
                raise Exception("Timeout before planned event '%s'!" % expect)
                
            self.logger.debug("Queueing planned event '%s'." % planned_event.tag)
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
        except StopIteration as e:
            self.logger.debug("Terminated plan.")
            self.generator = None
            
            if self.finish_handler:
                self.finish_handler(e.value)
        except Exception as e:
            self.logger.warning("Aborted plan: %s" % e)
            self.generator = None
            
            if self.error_handler:
                self.error_handler(e)
        
        
def main():
    class Planned(object):
        def __init__(self, metapoll):
            self.planner = Planner(metapoll, self.plan)
            self.planner.start()
        
    
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
