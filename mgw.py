import socket
import weakref
import struct
import datetime
#import logging
import wave

import g711
from msgp import MsgpServer
from async import WeakMethod
from util import Loggable, build_oid


class Error(Exception): pass


def get_payload_type(packet):
    return packet[1] & 0x7f
    
    
def set_payload_type(packet, pt):
    packet[1] = packet[1] & 0x80 | pt & 0x7f


def read_wav(filename):
    f = wave.open(filename, "rb")
    x = f.readframes(f.getnframes())
    f.close()
    
    return bytearray(x)  # Make it mutable


def write_wav(filename, data1, data2 = None):
    f = wave.open(filename, "wb")
    f.setsampwidth(2)
    f.setframerate(8000)
    
    if data2 is None:
        f.setnchannels(1)
        f.writeframes(data1)
    else:
        n = min(len(data1), len(data2)) // 2
        data = bytearray(2 * 2 * n)
        
        for i in range(n):
            data[2 * 2 * i + 0 : 2 * 2 * i + 2] = data1[2 * i : 2 * i + 2]
            data[2 * 2 * i + 2 : 2 * 2 * i + 4] = data2[2 * i : 2 * i + 2]
        
        f.setnchannels(2)
        f.writeframes(data)
        
    f.close()


def amplify_wav(samples, volume):
    BYTES_PER_SAMPLE = 2
    
    for i in range(int(len(samples) / BYTES_PER_SAMPLE)):
        offset = i * BYTES_PER_SAMPLE
        old = struct.unpack_from("<h", samples, offset)[0]
        struct.pack_into('<h', samples, offset, int(volume * old))


def encode_samples(encoding, samples):
    if encoding == "PCMA":
        payload = g711.encode_pcma(samples)
    elif encoding == "PCMU":
        payload = g711.encode_pcmu(samples)
    else:
        raise Error("WTF?")
        
    return payload


def decode_samples(encoding, payload):
    if encoding == "PCMA":
        samples = g711.decode_pcma(payload)
    elif encoding == "PCMU":
        samples = g711.decode_pcmu(payload)
    else:
        raise Error("WTF?")
        
    return samples


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
    payload_type = packet[1] & 0x7f
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
        
        samples_per_packet = int(clock * self.PTIME / 1000)
        timestamp = seq * samples_per_packet
        
        new_offset = self.offset + samples_per_packet * self.BYTES_PER_SAMPLE
        samples = self.data[self.offset:new_offset]
        self.offset = new_offset
        
        if self.fade_steps:
            self.fade_steps -= 1
            self.volume += self.fade_step
        
        amplify_wav(samples, self.volume)
        payload = encode_samples(encoding, samples)
            
        packet = build_rtp(self.ssrc, self.base_seq + seq, self.base_timestamp + timestamp, 127, payload)
        self.handler(self.format, packet)
        
        if self.offset >= len(self.data):
            self.metapoll.unregister_timeout(self.handle)
            self.handle = None


class Thing(Loggable):
    def __init__(self, oid, label, owner_sid):
        Loggable.__init__(self)

        self.label = label
        self.owner_sid = owner_sid
        self.set_oid(oid)
        
        
    def modify(self, params):
        pass

    
class Leg(Thing):
    def __init__(self, oid, label, owner_sid, type):
        super().__init__(oid, label, owner_sid)
        
        self.type = type
        self.forward_handler = None
        self.logger.debug("Created %s leg" % self.type)
        
        
    def __del__(self):
        self.logger.debug("Deleted %s leg" % self.type)


    def set_forward_handler(self, fh):
        self.forward_handler = fh
                    
        
    def recv_format(self, format, packet):
        if self.forward_handler:
            self.forward_handler(format, packet)
        else:
            self.logger.warning("No context to forward media to!")


    def send_format(self, format, packet):
        raise NotImplementedError()


class PassLeg(Leg):
    pass_legs_by_label = weakref.WeakValueDictionary()
    
    
    def __init__(self, oid, label, owner_sid):
        super().__init__(oid, label, owner_sid, "pass")
        
        self.other_label = None
        self.filename = None
        self.is_recording = False
        self.has_recorded = False
        self.samples_recved = bytearray()
        self.samples_sent = bytearray()
        
        # A file will be written only if recording was turned on, even if there
        # are no samples recorded.
        
        self.pass_legs_by_label[label] = self


    def __del__(self):
        if self.has_recorded:
            if self.filename:
                write_wav(self.filename, self.samples_recved, self.samples_sent)
            else:
                self.logger.error("Couldn't save recording without a filename!")

        Leg.__del__(self)                


    def modify(self, params):
        super().modify(params)
        
        if "other" in params:
            self.other_label = params["other"]
            
        if "filename" in params:
            self.filename = params["filename"]
            
        if "record" in params:
            self.is_recording = params["record"]
            self.has_recorded = self.has_recorded or self.is_recording


    def recv_format(self, format, packet):
        if self.is_recording:
            ssrc, seq, timestamp, payload_type, payload = parse_rtp(packet)
            encoding, clock = format
            self.samples_recved += decode_samples(encoding, payload)

        Leg.recv_format(self, format, packet)
        

    def send_format(self, format, packet):
        if self.is_recording:
            ssrc, seq, timestamp, payload_type, payload = parse_rtp(packet)
            encoding, clock = format
            self.samples_sent += decode_samples(encoding, payload)
            
        other_leg = self.pass_legs_by_label.get(self.other_label)
        if other_leg:
            other_leg.recv_format(format, packet)
        
        

class NetLeg(Leg):
    def __init__(self, oid, label, owner_sid, metapoll):
        super().__init__(oid, label, owner_sid, "net")
        self.local_addr = None
        self.remote_addr = None
        self.socket = None
        self.metapoll = metapoll
        self.send_pts_by_format = None
        self.recv_formats_by_pt = None
        
        
    def __del__(self):
        if self.socket:
            self.metapoll.register_reader(self.socket, None)
            
        super().__del__()
            
        
    def modify(self, params):
        super().modify(params)

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
            params = { 'id': self.label, 'remote_addr': addr }
            msgid = (self.owner_sid, "detected")
            self.context.manager.msgp.send(msgid, params)
            
            self.remote_addr = addr
            
        pt = get_payload_type(packet)
        
        try:
            format = self.recv_formats_by_pt[pt]
        except KeyError:
            self.logger.debug("Ignoring received unknown payload type %d" % pt)
        else:
            self.recv_format(format, packet)
    

    def send_format(self, format, packet):
        try:
            pt = self.send_pts_by_format[format]  # pt can be 0, which is false...
        except (TypeError, KeyError):
            self.logger.debug("Ignoring sent unknown payload format %s" % (format,))
            return
            
        set_payload_type(packet, pt)
        
        if not self.remote_addr or not self.remote_addr[1]:
            return
            
        #print("Sending to %s" % (self.remote_addr,))
        # packet should be a bytearray here
        self.socket.sendto(packet, self.remote_addr)


class EchoLeg(Leg):
    def __init__(self, oid, label, owner_sid):
        super().__init__(oid, label, owner_sid, "echo")
        
        self.buffer = []


    def send_format(self, format, packet):
        # We don't care about payload types
        #self.logger.debug("Echoing %s packet on %s" % (format, self.label))
        self.buffer.append((format, packet))
        
        if len(self.buffer) > 10:
            f, p = self.buffer.pop(0)
            self.recv_format(f, p)


class PlayerLeg(Leg):
    def __init__(self, oid, label, owner_sid, metapoll):
        super().__init__(oid, label, owner_sid, "player")
        
        self.metapoll = metapoll
        self.rtp_player = None
        self.format = None
        self.volume = 1


    def modify(self, params):
        self.logger.debug("Player leg modified: %s" % params)
        super().modify(params)

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


class Context(Thing):
    def __init__(self, oid, label, owner_sid, manager):
        super().__init__(oid, label, owner_sid)

        self.manager = manager
        self.legs = []
        self.logger.debug("Created context")
        
        
    def __del__(self):
        self.logger.debug("Deleted context")
        
        
    def modify(self, params):
        leg_labels = params["legs"]

        for i, leg in enumerate(self.legs):
            leg.set_forward_handler(None)

        self.legs = [ self.manager.get_leg(label) for label in leg_labels ]

        for i, leg in enumerate(self.legs):
            leg.set_forward_handler(WeakMethod(self.forward, i))


    def forward(self, format, packet, li):  # li is bound
        lj = (1 if li == 0 else 0)
        
        try:
            leg = self.legs[lj]
        except IndexError:
            raise Error("No outgoing leg!")

        #print("Forwarding in %s a %s from %d to %d" % (self.label, format, li, lj))
        
        leg.send_format(format, packet)


class MediaGateway(Loggable):
    def __init__(self, metapoll, mgw_addr):
        Loggable.__init__(self)

        self.metapoll = metapoll
        self.contexts_by_label = {}
        self.legs_by_label = {}
        self.msgp = MsgpServer(metapoll, WeakMethod(self.process_request), None, mgw_addr)


    def set_oid(self, oid):
        Loggable.set_oid(self, oid)
        self.msgp.set_oid(build_oid(oid, "msgp"))
        
    
    def get_leg(self, label):
        return self.legs_by_label[label]
        

    # Things
    
    def add_thing(self, things, thing):
        if thing.label in things:
            raise Error("Duplicate %s!" % thing.__class__)
        
        things[thing.label] = thing
        return thing
        
        
    def modify_thing(self, things, label, params):
        thing = things[label]
        thing.modify(params)
        
        
    def delete_thing(self, things, label):
        things.pop(label)


    def take_thing(self, things, label, owner_sid):
        thing = things[label]
        thing.owner_sid = owner_sid
    
    
    # Contexts

    def create_context(self, label, owner_sid, type):
        if type == "proxy":
            oid = build_oid(build_oid(self.oid, "context"), label)
            context = Context(oid, label, owner_sid, weakref.proxy(self))
        else:
            raise Error("Invalid context type %s!" % type)
            
        return self.add_thing(self.contexts_by_label, context)


    def modify_context(self, label, params):
        self.modify_thing(self.contexts_by_label, label, params)


    def delete_context(self, label):
        self.delete_thing(self.contexts_by_label, label)


    def take_context(self, label, owner_sid):
        self.take_thing(self.contexts_by_label, label, owner_sid)
        
        
    # Legs

    def create_leg(self, label, owner_sid, type):
        oid = build_oid(build_oid(self.oid, "leg"), label)
        
        if type == "pass":
            leg = PassLeg(oid, label, owner_sid)
        elif type == "net":
            leg = NetLeg(oid, label, owner_sid, self.metapoll)
        elif type == "echo":
            leg = EchoLeg(oid, label, owner_sid)
        elif type == "player":
            leg = PlayerLeg(oid, label, owner_sid, self.metapoll)
        else:
            raise Error("Invalid leg type '%s'!" % type)

        return self.add_thing(self.legs_by_label, leg)
        
        
    def modify_leg(self, label, params):
        self.modify_thing(self.legs_by_label, label, params)
        
        
    def delete_leg(self, label):
        # NOTE: a leg may stay alive here if linked into a context!
        self.delete_thing(self.legs_by_label, label)


    def take_leg(self, label, owner_sid):
        self.take_thing(self.legs_by_label, label, owner_sid)
        
        
    def process_request(self, target, msgid, params):
        try:
            owner_sid, seq = msgid
            label = params["id"]
            
            if target == "create_context":
                self.create_context(label, owner_sid, params["type"])
                self.modify_context(label, params)  # fake modification
            elif target == "modify_context":
                self.modify_context(label, params)
            elif target == "delete_context":
                self.delete_context(label)
            elif target == "take_context":
                self.take_context(label, owner_sid)
            elif target == "create_leg":
                self.create_leg(label, owner_sid, params["type"])
                self.modify_leg(label, params)  # fake modification
            elif target == "modify_leg":
                self.modify_leg(label, params)
            elif target == "delete_leg":
                self.delete_leg(label)
            elif target == "take_leg":
                self.take_leg(label, owner_sid)
            else:
                raise Error("Invalid target %s!" % target)
        except Exception as e:
            self.logger.debug("Processing error: %s" % e, exc_info=True)
            self.msgp.send(msgid, "error")
        else:
            self.msgp.send(msgid, "ok")
