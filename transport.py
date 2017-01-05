import socket

from async_net import TcpReconnector, TcpListener, HttpLikeStream
from format import Hop, Addr, parse_structured_message, print_structured_message, SipMessage
from log import Loggable
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
        
        
    def send(self, message, raddr):
        self.exchange_slot.zap(message)


    def exchanged(self, message):
        self.recved_slot.zap(message, None)


class UdpTransport(Transport):
    def __init__(self, socket):
        Transport.__init__(self)
        
        self.socket = socket
        zap.read_slot(self.socket).plug(self.recved)
        
        
    def send(self, message, raddr):
        self.socket.sendto(message.print(), raddr)


    def recved(self):
        packet, raddr = self.socket.recvfrom(65535)
        
        header, separator, rest = packet.partition(b"\r\n\r\n")
        message = SipMessage.parse(header)
        message.body = rest[:message.body]

        self.recved_slot.zap(message, Addr(*raddr))


class TcpTransport(Transport):
    def __init__(self, socket):
        Transport.__init__(self)
        
        socket.setblocking(False)
        self.http_like_stream = HttpLikeStream(socket, message_class=SipMessage)
        self.http_like_stream.process_slot.plug(self.process)


    def set_oid(self, oid):
        Transport.set_oid(self, oid)
        
        self.http_like_stream.set_oid(self.oid.add("hls"))
        
        
    def send(self, message, raddr):
        self.http_like_stream.put_message(message)


    def process(self, message):
        self.recved_slot.zap(message, None)  # message may be None for errors


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
        
        
    def add_transport(self, hop, transport):
        transport.set_oid(self.oid.add("hop", str(hop)))
        transport.recved_slot.plug(self.process_message, hop=hop)
        self.transports_by_hop[hop] = transport
        
        if not self.default_hop:
            self.default_hop = hop
        
        
    def add_hop(self, hop):
        if hop.transport == "TEST":
            assert hop.local_addr is None and hop.remote_addr is None
            self.logger.info("Adding TEST transport %s" % (hop,))
            
            transport = TestTransport()
            self.add_transport(hop, transport)
        elif hop.transport == "UDP":
            assert hop.remote_addr is None
            self.logger.info("Adding UDP transport %s" % (hop,))
        
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.setblocking(False)
            s.bind(hop.local_addr)  # TODO: bind to interface?
        
            transport = UdpTransport(s)
            self.add_transport(hop, transport)
        elif hop.transport == "TCP":
            if hop.local_addr and hop.local_addr.port is None and hop.remote_addr is not None:
                # Client connection
                self.logger.info("Adding TCP reconnector %s" % (hop,))
                
                # TODO: add local address binding!
                reconnector = TcpReconnector(hop.remote_addr, None)
                reconnector.set_oid(self.oid.add("reconnector", str(hop)))
                reconnector.connected_slot.plug(self.tcp_reconnector_connected, hop=hop)
                reconnector.start()
                
                self.tcp_reconnectors_by_hop[hop] = reconnector
            elif hop.remote_addr is None and hop.local_addr is not None:
                # Server connection
                self.logger.info("Adding TCP listener %s" % (hop,))

                listener = TcpListener(hop.local_addr)
                listener.set_oid(self.oid.add("listener", str(hop)))
                listener.accepted_slot.plug(self.tcp_listener_accepted, hop=hop)
                
                self.tcp_listeners_by_hop[hop] = listener
            else:
                raise Exception("Wrong TCP hop addresses!")
        else:
            raise Exception("Unknown transport type: %s" % hop.transport)

                
    def add_tcp_transport(self, socket, hop):
        self.logger.info("Adding TCP transport %s" % (hop,))
        socket.setblocking(False)
        transport = TcpTransport(socket)
        self.add_transport(hop, transport)


    def tcp_listener_accepted(self, socket, remote_addr, hop):
        hop = hop._replace(remote_addr=Addr(*remote_addr))
        self.add_tcp_transport(socket, hop)


    def tcp_reconnector_connected(self, socket, hop):
        self.tcp_reconnectors_by_hop.pop(hop)
        
        if socket:
            self.add_tcp_transport(socket, hop)
    
    
    def select_hop_slot(self, next_uri):
        next_transport = next_uri.params.get("transport", "UDP")  # TODO: tcp for sips
        next_host = next_uri.addr.host
        next_port = next_uri.addr.port
        slot = zap.EventSlot()
        
        resolver.resolve_slot(next_host).plug(self.select_hop_finish, port=next_port, transport=next_transport, slot=slot)
        return slot


    def select_hop_finish(self, address, port, transport, slot):
        # TODO: this is a bit messy...
        if not self.default_hop:
            self.logger.error("No default hop yet!")

        dhop = self.default_hop
        raddr = Addr(address, port)
        
        if transport == "TCP":
            hop = Hop(transport, dhop.interface, Addr(dhop.local_addr.host, None), raddr)
        else:
            hop = Hop(dhop.transport, dhop.interface, dhop.local_addr, raddr)
            
        slot.zap(hop)
        
        
    def send_message(self, params):
        hop = params["hop"]
        message = print_structured_message(params)
        #packet = sip.encode()
        self.logger.debug("Sending via %s\n%s" % (hop, indented(message.print())))
        
        if hop.transport == "UDP":
            raddr = hop.remote_addr
            hop = hop._replace(remote_addr=None)
        else:
            raddr = None
        
        transport = self.transports_by_hop.get(hop)
        
        if transport:
            transport.send(message, raddr)
        elif hop.transport == "TCP" and hop.local_addr.port is None:
            if hop in self.tcp_reconnectors_by_hop:
                self.logger.info("Connection already in progress for %s." % (hop,))
            else:
                self.logger.info("Initiating connection for %s." % (hop,))
                self.add_hop(hop)
        else:
            self.logger.error("No transport to send message via %s!" % (hop,))


    def process_message(self, message, raddr, hop):
        if message is None:
            self.logger.warning("Transport broken for %s!" % (hop,))
            self.transports_by_hop.pop(hop)
            return
    
        if raddr:
            hop = hop._replace(remote_addr=raddr)
            
        self.logger.debug("Receiving via %s\n%s" % (hop, indented(message.print())))
        
        #sip = packet.decode()
        params = parse_structured_message(message)
        params["hop"] = hop
        
        self.process_slot.zap(params)
