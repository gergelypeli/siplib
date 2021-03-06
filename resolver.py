import socket
import multiprocessing

from zap import Plug, EventSlot, kernel
from log import Loggable, Oid


class Resolver(Loggable):
    def __init__(self):
        Loggable.__init__(self)
        
        self.slots_by_hostname = {}
        self.parent_pipe, self.child_pipe = multiprocessing.Pipe()
        Plug(self.finish).attach_read(self.parent_pipe)
        
        self.resolver_process = multiprocessing.Process(target=self.resolver)
        self.resolver_process.start()
        
        
    def resolver(self):
        # Don't log here, it's a background process
        socket.setdefaulttimeout(5)
        
        try:
            while True:
                try:
                    hostname = self.child_pipe.recv()
                except EOFError:
                    break
                
                try:
                    address = socket.gethostbyname(hostname)
                except OSError:
                    address = None
                
                try:
                    self.child_pipe.send((hostname, address))
                except EOFError:
                    break
        except KeyboardInterrupt:
            pass


    def begin(self, hostname):
        slot = self.slots_by_hostname.get(hostname)
    
        if slot:
            self.logger.debug("Hostname '%s' is already being resolved." % hostname)
        else:
            self.logger.debug("Hostname '%s' will be resolved." % hostname)
            slot = EventSlot()
            self.slots_by_hostname[hostname] = slot
            self.parent_pipe.send(hostname)
        
        return slot


    def finish(self):
        hostname, address = self.parent_pipe.recv()
        
        if address:
            self.logger.debug("Hostname '%s' was resolved to '%s'." % (hostname, address))
        else:
            self.logger.warning("Hostname '%s' couldn't be resolved." % (hostname,))
        
        slot = self.slots_by_hostname.pop(hostname)
        slot.zap(address)


resolver = Resolver()
resolver.set_oid(Oid("resolver"))


def resolve_slot(hostname):
    return resolver.begin(hostname)


def wait_resolve(hostname, timeout=None):
    slot_index, slot_args = yield kernel.time_slot(timeout), resolve_slot(hostname)
    
    if slot_index == 0:
        return None
    else:
        return slot_args[0]
