import socket
import weakref

from rtp import read_wav, write_wav, RtpPlayer, RtpRecorder, DtmfExtractor, get_payload_type, set_payload_type
from msgp import MsgpServer
from async import WeakMethod, Weak
from util import Loggable, build_oid


class Error(Exception): pass


class Thing(Loggable):
    def __init__(self, oid, label, owner_sid):
        Loggable.__init__(self)

        self.label = label
        self.owner_sid = owner_sid
        self.mgw = None
        self.set_oid(oid)
        
        
    def set_mgw(self, mgw):
        self.mgw = mgw
        
    
    def report(self, tag, params, response_handler=None):
        msgid = (self.owner_sid, tag)
        self.mgw.report(msgid, dict(params, id=self.label), response_handler=response_handler)
        
        
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
        self.format = None
        self.recv_recorder = None
        self.send_recorder = None
        
        # A file will be written only if recording was turned on, even if there
        # are no samples recorded.
        
        self.pass_legs_by_label[label] = self


    def __del__(self):
        self.flush()
        Leg.__del__(self)


    def flush(self):
        if self.has_recorded:
            samples_recved = self.recv_recorder.get()
            samples_sent = self.send_recorder.get()
            write_wav(self.filename, samples_recved, samples_sent)


    def modify(self, params):
        super().modify(params)
        
        if "other" in params:
            self.other_label = params["other"]
            
        if "format" in params:
            self.format = tuple(params["format"])
            
            if self.recv_recorder:
                self.recv_recorder.set_format(self.format)
            if self.send_recorder:
                self.send_recorder.set_format(self.format)
            
        if "filename" in params:
            self.flush()
                
            self.filename = params["filename"]
            self.recv_recorder = RtpRecorder(self.format)
            self.recv_recorder.set_oid(build_oid(self.oid, "recv"))
            self.send_recorder = RtpRecorder(self.format)
            self.send_recorder.set_oid(build_oid(self.oid, "send"))
            
            self.is_recording = False
            self.has_recorded = False

        if "record" in params:
            if self.filename:
                self.is_recording = params["record"]
                self.has_recorded = self.has_recorded or self.is_recording
            else:
                self.logger.error("Can't save recording without a filename!")


    def recv_format(self, format, packet):
        if self.is_recording:
            self.recv_recorder.record(format, packet)

        Leg.recv_format(self, format, packet)
        

    def send_format(self, format, packet):
        if self.is_recording:
            self.send_recorder.record(format, packet)
            
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
        self.dtmf_extractor = DtmfExtractor(WeakMethod(self.dtmf_detected))
        
        
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
        packet = bytearray(packet)
        
        if self.remote_addr:
            remote_host, remote_port = self.remote_addr
            
            if remote_host and remote_host != addr[0]:
                return
                
            if remote_port and remote_port != addr[1]:
                return
        
        if addr != self.remote_addr:
            self.report("detected", { 'id': self.label, 'remote_addr': addr })
            self.remote_addr = addr
            
        pt = get_payload_type(packet)
        
        try:
            format = self.recv_formats_by_pt[pt]
        except KeyError:
            self.logger.debug("Ignoring received unknown payload type %d" % pt)
        else:
            if not self.dtmf_extractor.process(format, packet):
                self.recv_format(format, packet)
    
    
    def dtmf_detected(self, key):
        self.report("dtmf_detected", {'key': key}, response_handler=WeakMethod(self.dtmf_detected_response))
        
        
    def dtmf_detected_response(self, msgid, params):
        self.logger.debug("Seems like he MGC %sacknowledged our DTMF!" % ("" if msgid[1] else "NOT "))
        
    
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


    def report(self, msgid, params, response_handler=None):
        self.msgp.send(msgid, params, response_handler=response_handler)
        

    # Things
    
    def add_thing(self, things, thing):
        if thing.label in things:
            raise Error("Duplicate %s!" % thing.__class__)
        
        things[thing.label] = thing
        thing.set_mgw(Weak(self))
        
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
