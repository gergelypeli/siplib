import socket
from datetime import timedelta
#import logging

from format import Hop, Addr, parse_structured_message, print_structured_message
from async_base import WeakMethod
from util import Loggable


def indented(text, indent="  "):
    return "\n" + "\n".join(indent + line for line in text.split("\n"))


class TestTransport(Loggable):
    def __init__(self, metapoll, local_addr, reception):
        self.metapoll
        self.reception = reception
        self.transmission = None
        self.hop = Hop(local_addr=local_addr, remote_addr=None, interface="virt")
        
        
    def set_peer(self, peer):
        self.transmission = WeakMethod(peer.process)
        self.hop = Hop(local_addr=self.hop.local_addr, remote_addr=peer.hop.local_addr, interface=self.hop.interface)
        
        
    def get_hop(self, uri):
        return self.hop
        
        
    def send(self, msg):
        if msg["hop"] != self.hop:
            self.logger.debug("This transport can't send this!")
            return
        
        sip = print_structured_message(msg)
        self.logger.debug("Transport sending by %s:" % (self.hop,))
        self.logger.debug("\n" + "\n".join("  %s" % line for line in sip.split("\n")))
        
        self.metapoll.register_timeout(timedelta(), self.transmission.rebind(sip))


    def process(self, sip):
        #print("Transport receiving:")
        #print(s)
        msg = parse_structured_message(sip)
        msg["hop"] = self.hop
        self.reception(msg)


class UdpTransport(Loggable):
    def __init__(self, metapoll, local_addr, reception):
        self.metapoll = metapoll
        self.local_addr = local_addr.resolve()
        self.reception = reception
        
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(self.local_addr)

        self.metapoll.register_reader(self.socket, WeakMethod(self.recved))
        
        
    def get_hop(self, uri):
        raddr = Addr(uri.addr.host, uri.addr.port or 5060)
        return Hop(self.local_addr, raddr, "eth0")  # TODO: eth0?
        
        
    def send(self, msg):
        hop = msg["hop"]
        
        if hop.local_addr != self.local_addr:
            self.logger.error("This transport can't send on hop: %s" % (hop,))
            self.logger.debug("Our local addr is: %s" % (self.local_addr,))
            return
        
        sip = print_structured_message(msg)
        self.logger.debug("Sending to %s\n%s" % (hop.remote_addr, indented(sip)))
        
        sip = sip.encode()
        self.socket.sendto(sip, hop.remote_addr)


    def recved(self):
        sip, raddr = self.socket.recvfrom(65535)
        sip = sip.decode()
        
        hop = Hop(local_addr=self.local_addr, remote_addr=Addr(*raddr), interface="eth0")
        self.logger.debug("Receiving from %s\n%s" % (hop.remote_addr, indented(sip)))
        
        msg = parse_structured_message(sip)
        msg["hop"] = hop
        self.reception(msg)
