import socket

from async_net import TcpReconnector, TcpListener, HttpLikeStream
from format import Hop, Addr, parse_structured_message, print_structured_message
from util import Loggable
import zap
import resolver


def indented(packet, indent="  "):
    return "\n" + "\n".join(indent + line for line in packet.decode().split("\n"))


class Transport(Loggable):
    def __init__(self):
        Loggable.__init__(self)
        
        self.recved_slot = zap.EventSlot()


class TestTransport(Transport):
    def __init__(self):
        Transport.__init__(self)
        
        self.exchange_slot = zap.EventSlot()
        self.exchange_plug = None
        
        
    def set_peer(self, peer):
        self.exchange_plug = peer.exchange_slot.plug(self.exchanged)
        
        
    def send(self, packet, raddr):
        self.exchange_slot.zap(packet)


    def exchanged(self, packet):
        self.recved_slot.zap(packet, None)


class UdpTransport(Transport):
    def __init__(self, socket):
        Transport.__init__(self)
        
        self.socket = socket
        zap.read_slot(self.socket).plug(self.recved)
        
        
    def send(self, packet, raddr):
        self.socket.sendto(packet, raddr)


    def recved(self):
        packet, raddr = self.socket.recvfrom(65535)
        self.recved_slot.zap(packet, Addr(*raddr))


class TcpTransport(Transport):
    def __init__(self, socket):
        Transport.__init__(self)
        
        socket.setblocking(False)
        self.http_like_stream = HttpLikeStream(socket)
        self.http_like_stream.process_slot.plug(self.process)
        
        
    def send(self, packet, raddr):
        self.http_like_stream.put_message(packet)


    def process(self, packet):
        self.recved_slot.zap(packet, None)  # packet may be None for errors


class TransportManager(Loggable):
    def __init__(self):
        Loggable.__init__(self)
        
        # UDP transports are stored by hops where remote_addr is None.
        # TCP listen transports are stored by hops where remote_addr is None.
        # TCP server transports are stored by full hops.
        # TCP client transports are stored by hops where local_addr is None.
        self.default_hop = None
        self.transports_by_hop = {}
        self.tcp_reconnectors_by_hop = {}
        self.tcp_listeners_by_hop = {}
        self.process_slot = zap.EventSlot()
        
        
    #def set_oid(self, oid):
    #    Loggable.set_oid(self, oid)
        
    #    # FIXME: oid should be set first!
    #    for hop, transport in self.transports_by_hop.items():
    #        transport.set_oid(self.oid.add("hop", str(hop)))


    def add_transport(self, hop, transport):
        transport.set_oid(self.oid.add("hop", str(hop)))
        transport.recved_slot.plug(self.process_packet, hop=hop)
        self.transports_by_hop[hop] = transport
        
        if not self.default_hop:
            self.default_hop = hop
        
        
    def add_hop(self, hop):
        if hop.transport == "test":
            assert hop.local_addr is None and hop.remote_addr is None
            self.logger.info("Adding TEST transport %s" % (hop,))
            
            transport = TestTransport()
            self.add_transport(hop, transport)
        elif hop.transport == "udp":
            assert hop.remote_addr is None
            self.logger.info("Adding UDP transport %s" % (hop,))
        
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.setblocking(False)
            s.bind(hop.local_addr)  # TODO: bind to interface?
        
            transport = UdpTransport(s)
            self.add_transport(hop, transport)
        elif hop.transport == "tcp":
            assert hop.local_addr is None or hop.remote_addr is None
            
            if hop.local_addr is None:
                # Client connection
                self.logger.info("Adding TCP reconnector %s" % (hop,))
                
                reconnector = TcpReconnector(hop.remote_addr, timeout=None)
                reconnector.connected_slot.plug(self.tcp_reconnector_connected, hop=hop)
                self.tcp_reconnectors_by_hop[hop] = reconnector
            else:
                # Server connection
                self.logger.info("Adding TCP listener %s" % (hop,))

                listener = TcpListener(hop.local_addr)
                listener.accepted_slot.plug(self.tcp_listener_accepted, hop=hop)
                self.tcp_listeners_by_hop[hop] = listener
        else:
            raise Exception("Unknown transport type: %s" % hop.transport)

                
    def add_tcp_transport(self, socket, hop):
        self.logger.info("Adding TCP transport %s" % (hop,))
        socket.setblocking(False)
        transport = TcpTransport(socket)
        self.add_transport(hop, transport)


    def tcp_listener_accepted(self, socket, remote_addr, hop):
        hop = hop._replace(remote_addr=remote_addr)
        self.add_tcp_transport(socket, hop)


    def tcp_reconnector_connected(self, socket, hop):
        self.tcp_reconnectors_by_hop.pop(hop)
        
        if socket:
            self.add_tcp_transport(socket, hop)
    
    
    def select_hop_slot(self, next_uri):
        next_host = next_uri.addr.host
        next_port = next_uri.addr.port
        slot = zap.EventSlot()
        
        resolver.resolve_slot(next_host).plug(self.select_hop_finish, port=next_port, slot=slot)
        return slot


    def select_hop_finish(self, address, port, slot):
        if not self.default_hop:
            self.logger.error("No default hop yet!")
            
        hop = Hop(self.default_hop.transport, self.default_hop.interface, self.default_hop.local_addr, Addr(address, port))
        slot.zap(hop)
        
        
    def send_message(self, msg):
        hop = msg["hop"]
        sip = print_structured_message(msg)
        packet = sip.encode()
        self.logger.info("Sending via %s\n%s" % (hop, indented(packet)))
        
        if hop.transport == "udp":
            raddr = hop.remote_addr
            hop = hop._replace(remote_addr=None)
        else:
            raddr = None
        
        transport = self.transports_by_hop.get(hop)
        
        if transport:
            transport.send(packet, raddr)
        elif hop.transport == "tcp" and hop.local_addr is None:
            if hop in self.tcp_reconnectors_by_hop:
                self.logger.info("Connection already in progress for %s." % (hop,))
            else:
                self.logger.info("Initiating connection for %s." % (hop,))
                self.add_tcp_reconnector(hop.remote_addr, hop.local_addr, hop.interface)
        else:
            self.logger.error("No transport to send message via %s!" % (hop,))


    def process_packet(self, packet, raddr, hop):
        if packet is None:
            self.logger.warning("Transport broken for %s!" % (hop,))
            self.transports_by_hop.pop(hop)
            return
    
        if raddr:
            hop = hop._replace(remote_addr=raddr)
            
        self.logger.info("Receiving via %s\n%s" % (hop, indented(packet)))
        
        sip = packet.decode()
        msg = parse_structured_message(sip)
        msg["hop"] = hop
        
        self.process_slot.zap(msg)
