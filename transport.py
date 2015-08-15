import socket
from datetime import timedelta
from format import Hop, Addr, parse_structured_message, print_structured_message
from async import WeakMethod


def indented(text, indent="  "):
    return "\n" + "\n".join(indent + line for line in text.split("\n"))


class TestTransport(object):
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
            print("This transport can't send this!")
            return
        
        sip = print_structured_message(msg)
        print("Transport sending by %s:" % (self.hop,))
        print("\n" + "\n".join("  %s" % line for line in sip.split("\n")))
        
        self.metapoll.register_timeout(timedelta(), self.transmission.rebind(sip))


    def process(self, sip):
        #print("Transport receiving:")
        #print(s)
        msg = parse_structured_message(sip)
        msg["hop"] = self.hop
        self.reception(msg)


class UdpTransport(object):
    def __init__(self, metapoll, local_addr, reception):
        self.metapoll = metapoll
        self.local_addr = local_addr
        self.reception = reception
        
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(self.local_addr)

        self.metapoll.register_reader(self.socket, WeakMethod(self.recved))
        
        
    def get_hop(self, uri):
        return Hop(local_addr=self.local_addr, remote_addr=uri.addr, interface="eth0")
        
        
    def send(self, msg):
        hop = msg["hop"]
        
        if hop.local_addr != self.local_addr:
            print("This transport can't send this!")
            return
        
        sip = print_structured_message(msg)
        print("Transport sending from %s to %s:" % (hop.local_addr, hop.remote_addr))
        print(indented(sip))
        
        sip = sip.encode()
        self.socket.sendto(sip, hop.remote_addr)


    def recved(self):
        sip, raddr = self.socket.recvfrom(65535)
        sip = sip.decode()
        
        hop = Hop(local_addr=self.local_addr, remote_addr=Addr(*raddr), interface="eth0")
        print("Transport receiving from %s to %s:" % (hop.remote_addr, hop.local_addr))
        print(indented(sip))
        
        msg = parse_structured_message(sip)
        msg["hop"] = hop
        self.reception(msg)
