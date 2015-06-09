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
    
    
class Connection(object):
    def __init__(self, addr):
        self.addr = addr
        
        self.reconnect()
        
        
    def reconnect(self):
        pass
        
    def send(self, msg):
        self.socket


class Leg(object):
    def __init__(self, type, index, context):
        self.type = type
        self.index = index
        self.context = context
        self.send_pts_by_format = None
        
        self.name = "%s/%d" % (self.context.name, self.index)
        print("Created %s leg %s" % (type, self.name))
        
        
    def __del__(self):
        print("Deleted %s leg %s" % (self.type, self.name))
            
        
    def set(self, params):
        if "send_formats" in params:
            self.send_pts_by_format = revdict(parse_formats(params["send_formats"]))


    def forward(self, packet):
        self.context.forward(self.index, packet)


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
        super().__init__("net", index, context)
        self.local_addr = None
        self.remote_addr = None
        self.socket = None
        
        
    def __del__(self):
        if self.socket and self.context:
            self.context.manager.unregister(self.socket.fileno())
            
        
    def set(self, params):
        super().set(params)
            
        if "local_host" in params and "local_port" in params:
            try:
                self.local_addr = (params["local_host"], int(params["local_port"]))
                self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                self.socket.bind(self.local_addr)
                self.context.manager.register(self.socket.fileno(), WeakMethod(self.readable))
            except Exception as e:
                raise Error("Couldn't set leg: %s" % e)
        elif "local_host" in params or "local_port" in params:
            raise Error("Local params error!")
            
        if "remote_host" in params:
            self.remote_addr = (params["remote_host"], None)
    
    
    def readable(self):
        print("Receiving on %s" % self.name)
        packet, addr = self.socket.recvfrom(65535)
        if self.remote_addr and self.remote_addr[0] != addr[0]:
            return
        
        if not self.remote_addr or not self.remote_addr[1]:
            self.context.detected(self.index, addr)
            self.remote_addr = addr
            
        self.forward(packet)
    
    
    def send(self, packet):
        if not self.remote_addr or not self.remote_addr[1]:
            return
            
        print("Sending on %s" % self.name)
        self.socket.sendto(packet, self.remote_addr)


class EchoLeg(Leg):
    def __init__(self, index, context):
        super().__init__("echo", index, context)


    def send(self, packet):
        print("Echoing on %s" % self.name)
        self.forward(packet)


class Context(object):
    def __init__(self, name, params, manager):
        if "recv_formats" not in params:
            raise Error("Incomplete create context params: %s" % params)
            
        self.manager = manager
        self.legs = []
        self.name = name
        self.recv_formats_by_pt = parse_formats(params["recv_formats"])
        print("Created context %s" % self.name)
        
        
    def __del__(self):
        if hasattr(self, "name"):
            print("Deleted context %s" % self.name)
        
        
    def set_leg(self, index, params):
        while index >= len(self.legs):
            self.legs.append(None)

        if not params:
            self.legs[index] = None
            return

        type = params.pop("type", None)
        if not type:
            raise Error("No leg type!")

        if not self.legs[index] or self.legs[index].type != type:
            if type == "net":
                leg = NetLeg(index, weakref.proxy(self))
            elif type == "echo":
                leg = EchoLeg(index, weakref.proxy(self))
            else:
                raise Error("Invalid leg type %r!" % type)
                
            self.legs[index] = leg
        
        self.legs[index].set(params)
        
        
    def detected(self, incoming_index, remote_addr):
        self.manager.detected(self.name, incoming_index, remote_addr)
        
    
    def forward(self, incoming_index, packet):
        outgoing_index = (1 if incoming_index == 0 else 0)
        try:
            leg = self.legs[outgoing_index]
        except IndexError:
            raise Error("No outgoing leg!")

        pt = get_payload_type(packet)
        
        try:
            format = self.recv_formats_by_pt[pt]
        except KeyError:
            raise Error("Ignoring unknown payload type %d" % pt)
            
        print("Forwarding in %s a %s from %d to %d" % (self.name, format, incoming_index, outgoing_index))
        
        leg.send_format(format, packet)


class ContextManager(object):
    def __init__(self, metapoll, control_addr):
        self.contexts_by_name = {}
        self.metapoll = metapoll
        self.msgp = msgp.Msgp(metapoll, control_addr, WeakMethod(self.request_handler))
        #self.handlers_by_fd = {}
        self.controller_addr = None

        
    def register(self, fd, handler):
        self.metapoll.register_reader(fd, handler)
        #self.handlers_by_fd[fd] = handler
        #self.poll.register(fd, select.POLLIN)
    
    
    def unregister(self, fd):
        self.metapoll.register_reader(fd, None)
        #self.handlers_by_fd.pop(fd)
        #self.poll.unregister(fd)

    
    def request_handler(self, mid, message):
        try:
            params = parse_msg(message)
            response = self.process_request(params)
            sid, seq = mid
            addr, none = sid
            self.controller_addr = addr
        except Error:
            response = dict(response="invalid")
        
        message = print_msg(response)
        self.msgp.send_response(mid, message)
        
        
    def process_request(self, params):
        try:
            response = collections.OrderedDict()
            
            name = params.pop("ctx", None)
            if not name:
                raise Error("No ctx parameter!")
            
            response["ctx"] = name
            
            request = params.pop("request", None)
            if not request:
                raise Error("No request parameter!")
        
            context = self.contexts_by_name.get(name, None)
        
            if request == "create":
                if context:
                    raise Error("Context already exists!")
                
                self.contexts_by_name[name] = Context(name, params, weakref.proxy(self))
            elif request == "delete":
                if not context:
                    raise Error("Context does not exist!")
                
                self.contexts_by_name.pop(name)
            elif request == "modify":
                if not context:
                    raise Error("Context does not exist!")
                
                index = params.pop("leg", None)
                if not index:
                    raise Error("No leg parameter!")
                
                index = int(index)
                
                context.set_leg(index, params)
            else:
                raise Error("Invalid request!")
        except Error as e:
            response["response"] = "error"
        else:
            response["response"] = "ok"
            
        return response


    def detected(self, ctx, leg, addr):
        request = collections.OrderedDict()
        request["ctx"] = ctx
        request["request"] = "detected"
        request["leg"] = leg
        request["remote_host"] = addr[0]
        request["remote_port"] = addr[1]
        
        message = print_msg(request)
        sid = (self.controller_addr, None)
        self.msgp.send_request(sid, message)

        
def run_media_gateway(controller_addr):
    metapoll = Metapoll()
    cm = ContextManager(metapoll, controller_addr)
    
    while True:
        metapoll.do_poll()


class Controller(object):
    def __init__(self, metapoll, control_addr):
        self.control_addr = control_addr
        self.local_addr = ("", 0)
        self.context_callbacks = {}
        
        self.metapoll = metapoll
        self.msgp = msgp.Msgp(metapoll, self.local_addr, WeakMethod(self.recv_request))
        
        
    def send_request(self, params, callback):
        msg = print_msg(params)
        sid = (self.control_addr, None)
        self.msgp.send_request(sid, msg, callback=callback)
        
        
    def recv_request(self, mid, message):
        params = parse_msg(message)
        name = params["ctx"]
        cc = self.context_callbacks[name]
        if cc:
            cc(mid, params)
        
        
    def create_context(self, name, recv_formats, callback=None, context_callback=None):
        self.context_callbacks[name] = context_callback
        
        params = collections.OrderedDict()
        params["ctx"] = name
        params["request"] = "create"
        params["recv_formats"] = print_formats(recv_formats)
        self.send_request(params, callback)
        
        
    def modify_context(self, name, leg, type=None, local_host=None, local_port=None, remote_host=None, send_formats=None, callback=None):
        params = collections.OrderedDict()
        params["ctx"] = name
        params["request"] = "modify"
        params["leg"] = leg
        
        if type:
            params["type"] = type

        if local_host:
            params["local_host"] = local_host

        if local_port:
            params["local_port"] = local_port

        if remote_host:
            params["remote_host"] = remote_host
            
        if send_formats:
            params["send_formats"] = print_formats(send_formats)
            
        self.send_request(params, callback)


    def delete_context(self, name, callback=None):
        params = collections.OrderedDict()
        params["ctx"] = name
        params["request"] = "delete"
        self.send_request(params, callback)
        
        self.context_callbacks.pop(name)


    def loop(self, timeout=None):
        while self.poll.poll(timeout):
            self.msgp.recv()
            timeout = 0
            

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
