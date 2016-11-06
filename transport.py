import socket
#from datetime import timedelta
#import logging

from format import Hop, Addr, parse_structured_message, print_structured_message
from util import Loggable
import zap


def indented(text, indent="  "):
    return "\n" + "\n".join(indent + line for line in text.split("\n"))


class TestTransport(Loggable):
    def __init__(self, local_addr):
        self.process_slot = zap.EventSlot()
        self.exchange_slot = zap.EventSlot()
        self.exchange_plug = None
        self.hop = Hop(local_addr=local_addr, remote_addr=None, interface="virt")
        
        
    def set_peer(self, peer):
        self.exchange_plug = peer.exchange_slot.plug(self.process)
        self.hop = Hop(local_addr=self.hop.local_addr, remote_addr=peer.hop.local_addr, interface=self.hop.interface)
        
        
    def select_hop(self, uri):
        return self.hop
        
        
    def send(self, msg):
        if msg["hop"] != self.hop:
            self.logger.debug("This transport can't send this!")
            return
        
        sip = print_structured_message(msg)
        self.logger.debug("Transport sending by %s:" % (self.hop,))
        self.logger.debug("\n" + "\n".join("  %s" % line for line in sip.split("\n")))
        
        self.exchange_slot.zap(sip)


    def process(self, sip):
        #print("Transport receiving:")
        #print(s)
        msg = parse_structured_message(sip)
        msg["hop"] = self.hop
        self.process_slot.zap(msg)


class UdpTransport(Loggable):
    def __init__(self, local_addr):
        self.local_addr = local_addr.resolve()
        self.process_slot = zap.EventSlot()
        
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.setblocking(False)
        self.socket.bind(self.local_addr)

        self.recved_plug = zap.read_slot(self.socket).plug(self.recved)
        
        
    def select_hop(self, uri):
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
        self.process_slot.zap(msg)


# FIXME: create proper general Transport layer with these as subobjects!
