import socket
import multiprocessing

import zap
from util import Loggable


class Resolver(Loggable):
    def __init__(self):
        Loggable.__init__(self)
        
        self.slots_by_hostname = {}
        self.parent_pipe, self.child_pipe = multiprocessing.Pipe()
        zap.read_slot(self.parent_pipe).plug(self.finish)
        
        self.resolver_process = multiprocessing.Process(target=self.resolver)
        self.resolver_process.start()
        
        
    def resolver(self):
        # Don't log here, it's a background process
        
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
            slot = zap.EventSlot()
            self.slots_by_hostname[hostname] = slot
            self.parent_pipe.send(hostname)
        
        return slot


    def finish(self):
        hostname, address = self.parent_pipe.recv()
        self.logger.debug("Hostname '%s' was resolved to '%s'." % (hostname, address))
        
        slot = self.slots_by_hostname.pop(hostname)
        slot.zap(address)


resolver = Resolver()
resolver.set_oid("resolver")

def resolve_slot(hostname):
    return resolver.begin(hostname)
