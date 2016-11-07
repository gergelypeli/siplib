import socket
from weakref import ref, proxy, WeakValueDictionary

from rtp import read_wav, write_wav, RtpPlayer, RtpRecorder, RtpBuilder, RtpParser, DtmfExtractor, DtmfInjector, Format
from msgp import MsgpPeer  # MsgpServer
from util import Loggable, build_oid
import zap

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


    def report(self, target, params, response_tag=None):
        msgid = (self.owner_sid, target)
        self.mgw.report(msgid, dict(params, id=self.label), response_tag=response_tag)
        
        
    def modify(self, params):
        pass
        
        
    def notify(self, type, params):
        pass

    
class Leg(Thing):
    def __init__(self, oid, label, owner_sid, type):
        super().__init__(oid, label, owner_sid)
        
        self.type = type
        self.forward_slot = zap.EventSlot()
        self.logger.debug("Created %s leg" % self.type)
        
        
    def __del__(self):
        self.logger.debug("Deleted %s leg" % self.type)


    def recv_packet(self, packet):
        if self.forward_slot.plugs:  # FIXME: there should be a better way
            self.forward_slot.zap(packet)
        else:
            self.logger.warning("No context to forward media to!")


    def send_packet(self, packet):
        raise NotImplementedError()


class PassLeg(Leg):
    pass_legs_by_label = WeakValueDictionary()
    
    
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
            self.format = Format(*params["format"])
            
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


    def recv_packet(self, packet):
        if self.is_recording:
            self.recv_recorder.record_packet(packet)

        Leg.recv_packet(self, packet)
        

    def send_packet(self, packet):
        if self.is_recording:
            self.send_recorder.record_packet(packet)
            
        other_leg = self.pass_legs_by_label.get(self.other_label)
        if other_leg:
            other_leg.recv_packet(packet)
        
        

class NetLeg(Leg):
    def __init__(self, oid, label, owner_sid):
        super().__init__(oid, label, owner_sid, "net")
        
        self.local_addr = None
        self.remote_addr = None
        self.socket = None
        self.rtp_parser = RtpParser()
        self.rtp_builder = RtpBuilder()
        self.dtmf_extractor = DtmfExtractor()
        self.dtmf_detected_plug = self.dtmf_extractor.dtmf_detected_slot.plug(self.dtmf_detected)
        self.dtmf_extractor.set_oid(build_oid(self.oid, "dtmf-ex"))
        self.dtmf_injector = DtmfInjector()
        self.dtmf_injector.set_oid(build_oid(self.oid, "dtmf-in"))
        self.recved_plug = None
        
        
    def modify(self, params):
        super().modify(params)

        #print("Setting leg: %s" % (params,))
        
        if "send_formats" in params:
            payload_types_by_format = { Format(*v): int(k) for k, v in params["send_formats"].items() }
            self.rtp_builder.set_payload_types_by_format(payload_types_by_format)
            
            for format in payload_types_by_format:
                if format.encoding == "telephone-event":
                    self.dtmf_injector.set_format(format)
            
        #print("send_formats: %s" % (self.send_pts_by_format,))
            
        if "recv_formats" in params:
            formats_by_payload_type = { int(k): Format(*v) for k, v in params["recv_formats"].items() }
            self.rtp_parser.set_formats_by_payload_type(formats_by_payload_type)

        if "local_addr" in params:
            try:
                if self.socket:
                    self.recved_plug.unplug()
                    
                self.local_addr = tuple(params["local_addr"])
                self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                self.socket.setblocking(False)
                self.socket.bind(self.local_addr)
                self.recved_plug = zap.read_slot(self.socket).plug(self.recved)
            except Exception as e:
                raise Error("Couldn't set up net leg: %s" % e)
            
        if "remote_addr" in params:
            self.remote_addr = tuple(params["remote_addr"])
            
    
    def recved(self):
        #print("Receiving on %s" % self.name)
        udp, addr = self.socket.recvfrom(65535)
        udp = bytearray(udp)
        
        if self.remote_addr:
            remote_host, remote_port = self.remote_addr
            
            if remote_host and remote_host != addr[0]:
                return
                
            if remote_port and remote_port != addr[1]:
                return
        
        if addr != self.remote_addr:
            self.report("detected", { 'id': self.label, 'remote_addr': addr })
            self.remote_addr = addr
            
        packet = self.rtp_parser.parse(udp)

        if packet is None:
            self.logger.debug("Ignoring received unknown payload type!")
        elif not self.dtmf_extractor.process(packet):
            self.recv_packet(packet)
    
    
    def dtmf_detected(self, name):
        self.report("tone", dict(name=name))
        
        
    #def dtmf_detected_response(self, msgid, params):
    #    self.logger.debug("Seems like he MGC %sacknowledged our DTMF!" % ("" if msgid[1] else "NOT "))
        
        
    def send_checked(self, packet):
        udp = self.rtp_builder.build(packet)
        
        if udp is None:
            self.logger.debug("Ignoring sent unknown payload format %s" % (packet.format,))
            return

        if not self.remote_addr or not self.remote_addr[1]:
            return
            
        #print("Sending to %s" % (self.remote_addr,))
        # packet should be a bytearray here
        self.socket.sendto(udp, self.remote_addr)
    
    
    def send_packet(self, packet):
        if not self.dtmf_injector.process(packet):
            self.send_checked(packet)


    def notify(self, type, params):
        if type == "tone":
            name = params.get("name")
            packets = self.dtmf_injector.inject(name)
        
            for packet in packets:
                self.send_checked(packet)
        else:
            Leg.notify(self, type, params)
        

class EchoLeg(Leg):
    def __init__(self, oid, label, owner_sid):
        super().__init__(oid, label, owner_sid, "echo")
        
        self.buffer = []


    def send_packet(self, packet):
        # We don't care about payload types
        #self.logger.debug("Echoing %s packet on %s" % (format, self.label))
        self.buffer.append(packet)
        
        if len(self.buffer) > 10:
            p = self.buffer.pop(0)
            self.recv_packet(p)


class PlayerLeg(Leg):
    def __init__(self, oid, label, owner_sid):
        super().__init__(oid, label, owner_sid, "player")
        
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
            self.format = Format(*params["format"])
                
        if "filename" in params:
            samples = read_wav(params["filename"])
            
            self.rtp_player = RtpPlayer(self.format, samples, self.volume, fade)
            self.rtp_player.packet_slot.plug(self.recv_packet)


    def send_packet(self, packet):
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
            leg.forward_slot.unplug_all()  # FIXME: ugly!

        self.legs = [ self.manager.get_leg(label) for label in leg_labels ]

        for i, leg in enumerate(self.legs):
            leg.forward_slot.plug(self.fixme, li=i)
            #set_forward_handler(WeakMethod(self.forward, i).bind_front())


    def fixme(self, packet, li):
        return self.forward(li, packet)
        

    def forward(self, li, packet):
        lj = (1 if li == 0 else 0)
        
        try:
            leg = self.legs[lj]
        except IndexError:
            raise Error("No outgoing leg!")

        #print("Forwarding in %s a %s from %d to %d" % (self.label, format, li, lj))
        
        leg.send_packet(packet)


class MediaGateway(Loggable):
    def __init__(self, mgw_addr):
        Loggable.__init__(self)

        self.contexts_by_label = {}
        self.legs_by_label = {}
        self.msgp = MsgpPeer(mgw_addr)
        self.msgp.request_slot.plug(self.process_request)
        self.msgp.response_slot.plug(self.process_response)


    def set_oid(self, oid):
        Loggable.set_oid(self, oid)
        self.msgp.set_oid(build_oid(oid, "msgp"))
        

    def set_name(self, name):
        self.msgp.set_name(name)
        
    
    def get_leg(self, label):
        return self.legs_by_label[label]


    def report(self, msgid, params, response_tag=None):
        self.msgp.send(msgid, params, response_tag=response_tag)
        

    # Things
    
    def add_thing(self, things, thing):
        if thing.label in things:
            raise Error("Duplicate %s!" % thing.__class__)
        
        things[thing.label] = thing
        thing.set_mgw(proxy(self))
        
        return thing
        
        
    def modify_thing(self, things, label, params):
        thing = things[label]
        thing.modify(params)
        
        
    def delete_thing(self, things, label):
        wthing = ref(things.pop(label))
        
        if wthing():
            # This is to ensure contexts are removed before removing legs.
            self.logger.error("Deleted thing %s remained alive!" % label)


    def take_thing(self, things, label, owner_sid):
        thing = things[label]
        thing.owner_sid = owner_sid


    def notify_thing(self, things, label, type, params):
        thing = things[label]
        thing.notify(type, params)
    
    
    # Contexts

    def create_context(self, label, owner_sid, type):
        if type == "proxy":
            oid = build_oid(build_oid(self.oid, "context"), label)
            context = Context(oid, label, owner_sid, proxy(self))
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
            leg = NetLeg(oid, label, owner_sid)
        elif type == "echo":
            leg = EchoLeg(oid, label, owner_sid)
        elif type == "player":
            leg = PlayerLeg(oid, label, owner_sid)
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
        
        
    def notify_leg(self, label, type, params):
        self.notify_thing(self.legs_by_label, label, type, params)
        
        
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
            elif target == "tone":
                self.notify_leg(label, "tone", params)
            else:
                raise Error("Invalid target %s!" % target)
        except Exception as e:
            self.logger.debug("Processing error: %s" % e, exc_info=True)
            self.msgp.send(msgid, "error")
        else:
            self.msgp.send(msgid, "ok")
            
            if not self.legs_by_label and not self.contexts_by_label:
                self.logger.info("Back to clean state.")


    def process_response(self, response_tag, msgid, params):
        self.logger.debug("Got response for %s: %s" % (response_tag, params))
