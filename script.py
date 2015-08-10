# This needs at least Python 3.3 because of yield from!

from async import Metapoll, Weak, WeakMethod


class Scriptable(object):
    def __init__(self, metapoll):
        self.__metapoll = metapoll
        self.__generator = None
        self.__timeout_handle = None
        
        
    def poll(self, timeout=None):
        # self must be a weak self, since this is called from the script, which got
        # a weak reference from run
        
        if timeout is not None:
            def resume_timeout():
                self.__timeout_handle = None
                self.__resume(("timeout", None))
                
            self.__timeout_handle = self.__metapoll.register_timeout(timeout, resume_timeout)

        resumed_value = yield
        
        return resumed_value
        
        
    def run(self, generator_factory):
        self.__generator = generator_factory(Weak(self))
        self.__resume(None)
        
        
    def __resume(self, resumed_value):
        try:
            # This just returns if the script is suspended again, or
            # raises StopIteration if it ended.
            self.__generator.send(resumed_value)
        except StopIteration:
            print("The script terminated.")
            self.__generator = None


class Script(object):
    def __init__(self, metapoll, generator_method, finish_handler=None):
        self.metapoll = metapoll
        self.generator = generator_method.func(generator_method.wself, Weak(self))
        self.finish_handler = finish_handler
        self.timeout_handle = None
        
        print("Starting script.")
        self.resume(None)


    def __del__(self):
        if self.timeout_handle:
            self.metapoll.unregister_timeout(self.timeout_handle)
                    
        
    def poll(self, timeout=None):
        # self is actually a weak self, because this function is called from within the script

        if timeout is not None:
            self.timeout_handle = self.metapoll.register_timeout(timeout, self.resume_timeout)

        print("Suspending script.")
        resumed_value = yield
        print("Resuming script.")
        
        return resumed_value
        
        
    def resume(self, resumed_value):
        try:
            # This just returns if the script is suspended again, or
            # raises StopIteration if it ended.
            self.generator.send(resumed_value)
        except StopIteration:
            print("Terminated script.")
            self.generator = None
            
            if self.finish_handler:
                self.finish_handler()
        
        
    def resume_timeout(self):
        self.timeout_handle = None
        self.resume(("timeout", None))


class Scripted(object):
    def __init__(self, metapoll):
        self.script = Script(metapoll, WeakMethod(self.script_body))
        
    
    def script_body(self, script):
        print("One.")
        yield from script.poll(timeout=2)
        print("Two.")
        yield from script.poll(timeout=2)
        print("Three.")
        
        
metapoll = Metapoll()
s = Scripted(metapoll)

while True:
    metapoll.do_poll()
