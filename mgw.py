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
import datetime
import wave
import g711
import msgp
from async import WeakMethod, Metapoll


class Error(Exception): pass


def get_payload_type(packet):
    return packet[1] & 0x7f
    
    
def set_payload_type(packet, pt):
    packet[1] = packet[1] & 0x80 | pt & 0x7f


def read_wav(filename):
    f = wave.open(filename, "rb")
    x = f.readframes(f.getnframes())
    f.close()
    
    return bytearray(x)  # TODO: Py3k!


def amplify_wav(samples, volume):
    BYTES_PER_SAMPLE = 2
    
    for i in range(len(samples) / BYTES_PER_SAMPLE):
        offset = i * BYTES_PER_SAMPLE
        old = struct.unpack_from("<h", samples, offset)[0]
        struct.pack_into('<h', samples, offset, int(volume * old))


def build_rtp(ssrc, seq, timestamp, payload_type, payload):
    version = 2
    padding = 0
    extension = 0
    csrc_count = 0
    marker = 0
    
    packet = bytearray(12 + len(payload))
    packet[0] = version << 6 | padding << 5 | extension << 4 | csrc_count
    packet[1] = marker << 7 | payload_type & 0x7f
    struct.pack_into('!H', packet, 2, seq)
    struct.pack_into('!I', packet, 4, timestamp)
    struct.pack_into('!I', packet, 8, ssrc)
    packet[12:] = payload
    
    return packet


def parse_rtp(packet):
    payload_type = ord(packet[1]) & 0x7f
    seq = struct.unpack_from("!H", packet, 2)[0]
    timestamp = struct.unpack_from("!I", packet, 4)[0]
    ssrc = struct.unpack_from("!I", packet, 8)[0]
    payload = packet[12:]

    return ssrc, seq, timestamp, payload_type, payload


def prid(sid):
    addr, label = sid
    host, port = addr
    
    return "%s:%d@%s" % (host, port, label)


class RtpPlayer(object):
    PTIME = 20
    PLAY_INFO = {
        ("PCMU", 8000): (1,),
        ("PCMA", 8000): (1,)
    }
    BYTES_PER_SAMPLE = 2
    
    def __init__(self, metapoll, format, data, handler, volume=1, fade=0):
        self.metapoll = metapoll
        self.data = data  # mono 16 bit LSB LPCM
        self.handler = handler
        
        self.ssrc = 0  # should be random
        self.base_seq = 0  # too
        self.base_timestamp = 0  # too
        
        self.offset = 0
        self.next_seq = 0
        self.volume = 0
        self.format = None
        
        self.set_format(format)
        self.set_volume(volume, fade)
        
        if data:
            ptime = datetime.timedelta(milliseconds=self.PTIME)
            self.handle = self.metapoll.register_timeout(ptime, WeakMethod(self.play), repeat=True)
        
        
    def __del__(self):
        if self.handle:
            self.metapoll.unregister_timeout(self.handle)


    def set_volume(self, volume, fade):
        self.fade_steps = int(fade * 1000 / self.PTIME) + 1
        self.fade_step = (volume - self.volume) / self.fade_steps
        

    def set_format(self, format):
        if format not in self.PLAY_INFO:
            raise Error("Unknown format for playing: %s" % format)
            
        self.format = format
        
        
    def play(self):
        seq = self.next_seq
        self.next_seq += 1
        
        encoding, clock = self.format
        bytes_per_sample, = self.PLAY_INFO[self.format]
        
        samples_per_packet = clock * self.PTIME / 1000
        timestamp = seq * samples_per_packet
        
        new_offset = self.offset + samples_per_packet * self.BYTES_PER_SAMPLE
        samples = self.data[self.offset:new_offset]
        self.offset = new_offset
        
        if self.fade_steps:
            self.fade_steps -= 1
            self.volume += self.fade_step
        
        amplify_wav(samples, self.volume)
        
        if encoding == "PCMA":
            payload = g711.encode_pcma(samples)
        elif encoding == "PCMU":
            payload = g711.encode_pcmu(samples)
        else:
            raise Error("WTF?")
            
        packet = build_rtp(self.ssrc, self.base_seq + seq, self.base_timestamp + timestamp, 127, payload)
        self.handler(self.format, packet)
        
        if self.offset >= len(self.data):
            self.metapoll.unregister_timeout(self.handle)
            self.handle = None

    
class Leg(object):
    def __init__(self, type, index, context):
        self.type = type
        self.index = index
        self.context = context
        
        self.name = "%s/%d" % (self.context.label, self.index)
        print("Created %s leg %s" % (type, self.name))
        
        
    def __del__(self):
        print("Deleted %s leg %s" % (self.type, self.name))
            
        
    def set(self, params):
        pass
        

    def recv_format(self, format, packet):
        self.context.forward(self.index, format, packet)


    def send_format(self, format, packet):
        raise NotImplementedError()


class NetLeg(Leg):
    def __init__(self, index, context):
        super(NetLeg, self).__init__("net", index, context)
        self.local_addr = None
        self.remote_addr = None
        self.socket = None
        self.metapoll = context.manager.metapoll
        self.send_pts_by_format = None
        self.recv_formats_by_pt = None
        
        
    def __del__(self):
        if self.socket:
            self.metapoll.register_reader(self.socket, None)
            
        
    def set(self, params):
        super(NetLeg, self).set(params)

        #print("Setting leg: %s" % (params,))
        
        if "send_formats" in params:
            self.send_pts_by_format = { tuple(v): int(k) for k, v in params["send_formats"].items() }
            
        #print("send_formats: %s" % (self.send_pts_by_format,))
            
        if "recv_formats" in params:
            self.recv_formats_by_pt = { int(k): tuple(v) for k, v in params["recv_formats"].items() }

        if "local_addr" in params:
            try:
                if self.socket:
                    self.metapoll.register_reader(self.socket, None)
                    
                self.local_addr = tuple(params["local_addr"])
                self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                self.socket.bind(self.local_addr)
                self.metapoll.register_reader(self.socket, WeakMethod(self.recved))
            except Exception as e:
                raise Error("Couldn't set up net leg: %s" % e)
            
        if "remote_addr" in params:
            self.remote_addr = tuple(params["remote_addr"])
    
    
    def recved(self):
        #print("Receiving on %s" % self.name)
        packet, addr = self.socket.recvfrom(65535)
        packet = bytearray(packet)  # TODO: use Py3
        
        if self.remote_addr:
            remote_host, remote_port = self.remote_addr
            
            if remote_host and remote_host != addr[0]:
                return
                
            if remote_port and remote_port != addr[1]:
                return
        
        if addr != self.remote_addr:
            self.context.detected(self.index, addr)
            self.remote_addr = addr
            
        pt = get_payload_type(packet)
        
        try:
            format = self.recv_formats_by_pt[pt]
        except KeyError:
            print("Ignoring received unknown payload type %d" % pt)
        else:
            self.recv_format(format, packet)
    

    def send_format(self, format, packet):
        try:
            pt = self.send_pts_by_format[format]  # pt can be 0, which is false...
        except (TypeError, KeyError):
            print("Ignoring sent unknown payload format %s" % (format,))
            return
            
        set_payload_type(packet, pt)
        
        if not self.remote_addr or not self.remote_addr[1]:
            return
            
        #print("Sending on %s" % self.name)
        self.socket.sendto(packet, self.remote_addr)


class EchoLeg(Leg):
    def __init__(self, index, context):
        super(EchoLeg, self).__init__("echo", index, context)


    def send_format(self, format, packet):
        # We don't care about payload types
        print("Echoing %s packet on %s" % (format, self.name))
        self.recv_format(format, packet)


class PlayerLeg(Leg):
    def __init__(self, index, context):
        super(PlayerLeg, self).__init__("player", index, context)
        
        self.metapoll = context.manager.metapoll
        self.rtp_player = None
        self.format = None
        self.volume = 1


    def set(self, params):
        super(PlayerLeg, self).set(params)

        # This is temporary, don't remember it across calls
        fade = 0

        if "fade" in params:
            fade = params["fade"]

        if "volume" in params:
            self.volume = params["volume"]
            
            if self.rtp_player:
                self.rtp_player.set_volume(self.volume, fade)

        if "format" in params:
            self.format = tuple(params["format"])
            
            if self.rtp_player:
                self.rtp_player.set_format(self.format)
                
        if "filename" in params:
            samples = read_wav(params["filename"])
            
            self.rtp_player = RtpPlayer(self.metapoll, self.format, samples, WeakMethod(self.recv_format), self.volume, fade)


    def send_format(self, format, packet):
        pass


class Context(object):
    def __init__(self, label, owner_addr, manager):
        self.label = label
        self.owner_addr = owner_addr
        self.manager = manager
        self.legs = []
        print("Created context %s" % self.label)
        
        
    def __del__(self):
        print("Deleted context %s" % self.label)
        
        
    def set_leg(self, li, params):
        while li >= len(self.legs):
            self.legs.append(None)

        if params is None:
            self.legs[li] = None
            return

        type = params.pop("type", None)

        if not self.legs[li] or (type and type != self.legs[li].type):
            if type == "net":
                leg = NetLeg(li, weakref.proxy(self))
            elif type == "echo":
                leg = EchoLeg(li, weakref.proxy(self))
            elif type == "player":
                leg = PlayerLeg(li, weakref.proxy(self))
            else:
                raise Error("Invalid leg type '%s'!" % type)
                
            self.legs[li] = leg
        
        self.legs[li].set(params)


    def modify(self, params):
        # TODO: implement leg deletion
        for li, leg_params in params.get("legs", {}).items():
            self.set_leg(int(li), leg_params)
            
        
    def detected(self, li, remote_addr):
        sid = (self.owner_addr, self.label)
        self.manager.detected(sid, li, remote_addr)
        
    
    def forward(self, li, format, packet):
        lj = (1 if li == 0 else 0)
        
        try:
            leg = self.legs[lj]
        except IndexError:
            raise Error("No outgoing leg!")

        #print("Forwarding in %s a %s from %d to %d" % (self.label, format, li, lj))
        
        leg.send_format(format, packet)


class ContextManager(object):
    def __init__(self, metapoll, mgw_addr):
        self.contexts_by_label = {}
        self.metapoll = metapoll
        self.msgp = msgp.JsonMsgp(metapoll, mgw_addr, WeakMethod(self.process_request))


    def add_context(self, label, owner_addr, type):
        if label in self.contexts_by_label:
            raise Error("Context already exists!")
        
        if type == "proxy":
            context = Context(label, owner_addr, weakref.proxy(self))
        else:
            raise Error("Invalid context type %s!" % type)
            
        self.contexts_by_label[label] = context
        return context


    def modify_context(self, label, params):
        context = self.contexts_by_label.get(label)
        context.modify(params)


    def remove_context(self, label):
        context = self.contexts_by_label.pop(label)
        
        old_sid = (context.owner_addr, context.label)
        self.msgp.remove_stream(old_sid)
        
        
    def process_request(self, sid, seq, params, target):
        try:
            owner_addr, label = sid
            
            if target == "create":
                self.add_context(label, owner_addr, params["type"])
                self.msgp.add_stream(sid)
                self.modify_context(label, params)  # fake modification
            elif target == "modify":
                self.modify_context(label, params)
            elif target == "delete":
                self.remove_context(label)
            elif target == "take":
                context = self.contexts_by_label.get(label)
                old_sid = (context.owner_addr, context.label)
                self.msgp.remove_stream(old_sid)
                
                self.msgp.add_stream(sid)
                context.owner_addr = owner_addr
            else:
                raise Error("Invalid target %s!" % target)
        except Exception as e:
            print("Context error: %s" % e)
            self.msgp.send_message(sid, seq, "error")
        else:
            self.msgp.send_message(sid, seq, "ok")
        

    def detected(self, sid, li, remote_addr):
        params = { 'legs': { li: { 'remote_addr': remote_addr } } }
        self.msgp.send_message(sid, "detected", params)

