import socket
from weakref import proxy

from format import Hop, Addr, parse_structured_message, print_structured_message
from util import Loggable, build_oid
import zap
import resolver


def indented(packet, indent="  "):
    return "\n" + "\n".join(indent + line for line in packet.decode().split("\n"))


class Transport(Loggable):
    def __init__(self, manager, local_addr, interface):
        Loggable.__init__(self)
        local_addr.assert_resolved()
        
        self.manager = manager
        self.local_addr = local_addr
        self.interface = interface


class TestTransport(Transport):
    def __init__(self, manager, local_addr, interface):
        Transport.__init__(self, manager, local_addr, interface)
        
        self.remote_addr = None
        self.exchange_slot = zap.EventSlot()
        self.exchange_plug = None
        
        
    def set_peer(self, peer):
        self.remote_addr = peer.local_addr
        self.exchange_plug = peer.exchange_slot.plug(self.recved)
        
        
    def send(self, hop, packet):
        if hop.local_addr != self.local_addr or hop.interface != self.interface:
            return False
            
        self.logger.info("Sending from %s to %s\n%s" % (hop.local_addr, hop.remote_addr, indented(packet)))
        self.exchange_slot.zap(packet)
        return True


    def recved(self, packet):
        hop = Hop(local_addr=self.local_addr, remote_addr=self.remote_addr, interface=self.interface)
        self.logger.info("Receiving from %s to %s\n%s" % (hop.remote_addr, hop.local_addr, indented(packet)))

        self.manager.process_packet(hop, packet)


class UdpTransport(Transport):
    def __init__(self, manager, local_addr, interface):
        Transport.__init__(self, manager, local_addr, interface)
        
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.setblocking(False)
        self.socket.bind(self.local_addr)
        self.recved_plug = zap.read_slot(self.socket).plug(self.recved)
        
        
    def send(self, hop, packet):
        if hop.local_addr != self.local_addr or hop.interface != self.interface:
            return False
            
        self.logger.info("Sending from %s to %s\n%s" % (hop.local_addr, hop.remote_addr, indented(packet)))
        self.socket.sendto(packet, hop.remote_addr)
        return True


    def recved(self):
        packet, raddr = self.socket.recvfrom(65535)
        hop = Hop(local_addr=self.local_addr, remote_addr=Addr(*raddr), interface=self.interface)
        self.logger.info("Receiving from %s to %s\n%s" % (hop.remote_addr, hop.local_addr, indented(packet)))

        self.manager.process_packet(hop, packet)


class TransportManager(Loggable):
    def __init__(self):
        Loggable.__init__(self)
        
        self.transports = []
        self.process_slot = zap.EventSlot()
        
        
    def set_oid(self, oid):
        Loggable.set_oid(self, oid)
        
        for transport in self.transports:
            transport.set_oid(build_oid(self.oid, "iface", transport.interface))
            
        
    def add_udp_transport(self, local_addr, interface=None):
        transport = UdpTransport(proxy(self), local_addr, interface)
        self.transports.append(transport)


    def add_test_transport(self, local_addr, interface=None):
        transport = TestTransport(proxy(self), local_addr, interface)
        self.transports.append(transport)


    def select_hop_slot(self, next_uri):
        next_host = next_uri.addr.host
        next_port = next_uri.addr.port
        slot = zap.EventSlot()
        
        resolver.resolve_slot(next_host).plug(self.select_hop_finish, port=next_port, slot=slot)
        return slot


    def select_hop_finish(self, address, port, slot):
        transport = self.transports[0]
        hop = Hop(transport.local_addr, Addr(address, port), transport.interface)
        slot.zap(hop)
        
        
    def send_message(self, msg):
        sip = print_structured_message(msg)
        packet = sip.encode()
        hop = msg["hop"]
        
        for transport in self.transports:
            processed = transport.send(hop, packet)

            if processed:
                return
                
        self.logger.error("No transport to send message!")


    def process_packet(self, hop, packet):
        sip = packet.decode()
        msg = parse_structured_message(sip)
        msg["hop"] = hop
        
        self.process_slot.zap(msg)
