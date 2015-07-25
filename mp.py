# ctx:n request:create recv_formats:100=x,101=y,102=z
# ctx:n response:ok

# ctx:n request:modify leg:0 local_port:5678 remote_host:1.2.3.4 send_formats:100=x,101=y,102=z
# ctx:n response:ok

# ctx:n request:delete
# ctx:n response:ok

# ctx:n report:dtmf digit:#
# ctx:n response:ok

from __future__ import print_function, unicode_literals
#import select
import socket
import weakref
import collections
import time
import struct
import msgp
from async import WeakMethod, Metapoll


class Error(Exception): pass


def get_payload_type(packet):
    return ord(packet[1]) & 0x7f
    
    
def set_payload_type(packet, pt):
    return packet[0:1] + chr(ord(packet[1]) & 0x80 | pt & 0x7f) + packet[2:]


def parse_formats(fmts):
    return { int(k): v for k, v in [ kv.split("=", 1) for kv in fmts.split(",") ] }


def print_formats(fmts):
    return ",".join("%d=%s" % (k, v) for k, v in fmts.items())
    
    
def revdict(d):
    return { v: k for k, v in d.items() }


def parse_msg(msg):
    return { k: v for k, v in [ kv.split(":", 1) for kv in msg.split() ] }
    
    
def print_msg(params):
    return " ".join("%s:%s" % (k, v) for k, v in params.items()) + "\n"
    
    
#class Connection(object):
#    def __init__(self, addr):
#        self.addr = addr
        
#        self.reconnect()
        
        
#    def reconnect(self):
#        pass
        
#    def send(self, msg):
#        self.socket


class Leg(object):
    def __init__(self, type, index, context):
        self.type = type
        self.index = index
        self.context = context
        self.send_pts_by_format = None
        self.recv_formats_by_pt = None
        
        self.name = "%s/%d" % (self.context.name, self.index)
        print("Created %s leg %s" % (type, self.name))
        
        
    def __del__(self):
        print("Deleted %s leg %s" % (self.type, self.name))
            
        
    def set(self, params):
        if "send_formats" in params:
            self.send_pts_by_format = revdict(parse_formats(params["send_formats"]))
            
        if "recv_formats" in params:
            self.recv_formats_by_pt = parse_formats(params["recv_formats"])


    def recv_format(self, packet):
        pt = get_payload_type(packet)
        
        try:
            format = self.recv_formats_by_pt[pt]
        except KeyError:
            raise Error("Ignoring unknown payload type %d" % pt)
            
        self.context.forward(self.index, format, packet)


    def send(self, packet):
        raise NotImplementedError()
        
            
    def send_format(self, format, packet):
        if not self.send_pts_by_format:
            return
            
        pt = self.send_pts_by_format.get(format)
        if not pt:
            return
            
        self.send(set_payload_type(packet, pt))


class NetLeg(Leg):
    def __init__(self, index, context):
        super(NetLeg, self).__init__("net", index, context)
        self.local_addr = None
        self.remote_addr = None
        self.socket = None
        
        
    def __del__(self):
        if self.socket and self.context:
            self.context.manager.unregister(self.socket.fileno())
            
        
    def set(self, params):
        super(NetLeg, self).set(params)
            
        if "local_host" in params and "local_port" in params:
            try:
                self.local_addr = (params["local_host"], int(params["local_port"]))
                self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                self.socket.bind(self.local_addr)
                self.context.manager.register(self.socket.fileno(), WeakMethod(self.recv))
            except Exception as e:
                raise Error("Couldn't set leg: %s" % e)
        elif "local_host" in params or "local_port" in params:
            raise Error("Local params error!")
            
        if "remote_host" in params:
            self.remote_addr = (params["remote_host"], None)
    
    
    def recv(self):
        print("Receiving on %s" % self.name)
        packet, addr = self.socket.recvfrom(65535)
        if self.remote_addr and self.remote_addr[0] != addr[0]:
            return
        
        if not self.remote_addr or not self.remote_addr[1]:
            self.context.detected(self.index, addr)
            self.remote_addr = addr
            
        self.recv_format(packet)
    
    
    def send(self, packet):
        if not self.remote_addr or not self.remote_addr[1]:
            return
            
        print("Sending on %s" % self.name)
        self.socket.sendto(packet, self.remote_addr)


class EchoLeg(Leg):
    def __init__(self, index, context):
        super(EchoLeg, self).__init__("echo", index, context)


    def send(self, packet):
        print("Echoing on %s" % self.name)
        self.recv_format(packet)


class Context(object):
    def __init__(self, sid, params, manager):
        self.manager = manager
        self.legs = []
        self.sid = sid
        print("Created context %s" % (self.sid,))
        
        
    def __del__(self):
        print("Deleted context %s" % (self.sid,))
        
        
    def set_leg(self, li, params):
        while li >= len(self.legs):
            self.legs.append(None)

        if not params:
            self.legs[li] = None
            return

        type = params.pop("type", None)
        if not type:
            raise Error("No leg type!")

        if not self.legs[li] or self.legs[li].type != type:
            if type == "net":
                leg = NetLeg(li, weakref.proxy(self))
            elif type == "echo":
                leg = EchoLeg(li, weakref.proxy(self))
            else:
                raise Error("Invalid leg type %r!" % type)
                
            self.legs[li] = leg
        
        self.legs[li].set(params)
        
        
    def detected(self, li, remote_addr):
        self.manager.detected(self.sid, li, remote_addr)
        
    
    def forward(self, li, format, packet):
        lj = (1 if li == 0 else 0)
        
        try:
            leg = self.legs[lj]
        except IndexError:
            raise Error("No outgoing leg!")

        print("Forwarding in %s a %s from %d to %d" % (self.sid, format, li, lj))
        
        leg.send_format(format, packet)


class ContextManager(object):
    def __init__(self, metapoll, mgw_addr):
        self.contexts_by_sid = {}
        self.metapoll = metapoll
        self.msgp = msgp.JsonMsgp(metapoll, mgw_addr, WeakMethod(self.process_message))

        
    def process_message(self, sid, seq, target, params):
        context = self.contexts_by_sid.get(sid)

        if not params:
            if context:
                self.contexts_by_sid.pop(sid)
        else:
            if not context:
                context = Context(sid, params, weakref.proxy(self))
                self.contexts_by_sid[sid] = context
                
            for li, leg_params in params.get("legs", {}):
                context.set_leg(li, leg_params)
                
        self.msgp.send_message(sid, seq, "ok")


    def detected(self, sid, li, remote_addr):
        params = { 'legs': { li: { 'remote_addr': remote_addr } } }
        self.msgp.send_message(sid, "detected", params)


class Controller(object):
    def __init__(self, metapoll, mgc_addr):
        self.mgc_addr = mgc_addr
        self.context_callbacks = {}
        
        self.metapoll = metapoll
        self.msgp = msgp.JsonMsgp(metapoll, mgc_addr, WeakMethod(self.process_message))
        
        
    def send_message(self, sid, target, params):
        self.msgp.send_message(sid, target, params)
        
        
    def process_message(self, sid, seq, target, params):
        callback = self.context_callbacks.get(sid)

        if callback:
            callback(seq, target, params)
        
        
    def create_context(self, sid, params, callback=None):
        self.context_callbacks[sid] = callback
        self.send_message(sid, "create", params)


    def modify_context(self, sid, params):
        self.send_message(sid, "modify", params)


    def delete_context(self, sid):
        self.context_callbacks.pop(sid)
        self.send_message(sid, "delete", None)
        
        
    def allocate_media(self, leg_count):
        raise NotImplemented()


class Rtp(object):
    def __init__(self, metapoll, local_addr, remote_addr, receiving_callback):
        self.metapoll = metapoll
        self.remote_addr = remote_addr
        self.receiving_callback = receiving_callback
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(local_addr)
        self.metapoll.register_reader(self.socket, WeakMethod(self.recv))
        
        self.ssrc = 0
        self.seq = 0
        self.timestamp = 0
        
        
    def send(self, type, payload):
        version = 2
        padding = 0
        extension = 0
        csrc_count = 0
        marker = 0
        self.seq += 1
        self.timestamp += 8000
        
        data = (
            chr(version << 6 | padding << 5 | extension << 4 | csrc_count) +
            chr(marker << 7 | type) +
            struct.pack('!H', self.seq) +
            struct.pack('!I', self.timestamp) +
            struct.pack('!I', self.ssrc) +
            payload
        )
        
        self.socket.sendto(data, self.remote_addr)
        
        
    def recv(self):
        data, addr = self.socket.recvfrom(65535)
        if addr != self.remote_addr:
            return
            
        type = ord(data[1]) & 0x7f
        payload = data[12:]
        
        if self.receiving_callback:
            self.receiving_callback(type, payload)