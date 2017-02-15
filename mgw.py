import socket
from weakref import proxy

from rtp import read_wav, write_wav, RtpPlayer, RtpRecorder, RtpBuilder, RtpParser, DtmfExtractor, DtmfInjector, Format
from msgp import MsgpPeer
from log import Loggable
import zap

class Error(Exception): pass


class Thing(Loggable):
    def __init__(self, label, owner_sid, type):
        Loggable.__init__(self)

        self.label = label
        self.owner_sid = owner_sid
        self.type = type
        self.mgw = None


    def __del__(self):
        self.logger.debug("Deleted %s" % self.type)
        
        
    def set_mgw(self, mgw):  # TODO: move to init
        self.mgw = mgw


    def report(self, ttag, params, origin=None):
        target = (self.owner_sid, ttag)
        self.mgw.send_request(target, dict(params, label=self.label), origin=origin)
        
        
    def modify(self, params):
        pass
        
        
    def notify(self, type, params):
        pass


    def forward(self, li, packet):
        self.mgw.forward(self.label, li, packet)
        
    
class RecordThing(Thing):
    def __init__(self, label, owner_sid):
        Thing.__init__(self, label, owner_sid, "record")
        
        self.filename = None
        self.is_recording = False
        self.has_recorded = False
        self.format = None
        self.fore_recorder = None
        self.back_recorder = None
        
        # A file will be written only if recording was turned on, even if there
        # are no samples recorded.


    def __del__(self):
        self.flush()
        Thing.__del__(self)


    def flush(self):
        if self.has_recorded:
            samples_fore = self.fore_recorder.get()
            samples_back = self.back_recorder.get()
            write_wav(self.filename, samples_fore, samples_back)


    def modify(self, params):
        Thing.modify(self, params)
        
        if "format" in params:
            self.format = Format(*params["format"])
            
        if "filename" in params:
            self.flush()
                
            self.filename = params["filename"]
            self.fore_recorder = RtpRecorder(self.format)
            self.fore_recorder.set_oid(self.oid.add("fore"))
            self.back_recorder = RtpRecorder(self.format)
            self.back_recorder.set_oid(self.oid.add("back"))
            
            self.is_recording = False
            self.has_recorded = False

        if "record" in params:
            if self.filename:
                self.is_recording = params["record"]
                self.has_recorded = self.has_recorded or self.is_recording
            else:
                self.logger.error("Can't save recording without a filename!")


    def process(self, li, packet):
        if self.is_recording:
            if li == 0:
                self.fore_recorder.record_packet(packet)
            else:
                self.back_recorder.record_packet(packet)
                
        lj = 1 - li
        self.forward(lj, packet)
        

class RtpThing(Thing):
    def __init__(self, label, owner_sid):
        Thing.__init__(self, label, owner_sid, "rtp")
        
        self.local_addr = None
        self.remote_addr = None
        self.socket = None
        self.rtp_parser = RtpParser()
        self.rtp_builder = RtpBuilder()
        self.dtmf_extractor = DtmfExtractor()
        self.dtmf_detected_plug = self.dtmf_extractor.dtmf_detected_slot.plug(self.dtmf_detected)
        self.dtmf_injector = DtmfInjector()
        self.recved_plug = None
        
        
    def __del__(self):
        # TODO: it seems like sometimes the plug does not get uplugged, while the
        # socket object is destroyed, then the Kernel will poll POLLNVAL events from
        # this descriptor, and has to purge it from the poll set. So do this explicitly.
        
        if self.recved_plug:
            self.recved_plug.unplug()
            
        
    def set_oid(self, oid):
        Thing.set_oid(self, oid)
        
        self.dtmf_extractor.set_oid(self.oid.add("dtmf-ex"))
        self.dtmf_injector.set_oid(self.oid.add("dtmf-in"))
        
        
    def modify(self, params):
        Thing.modify(self, params)

        if "send_formats" in params:
            payload_types_by_format = { Format(*v): int(k) for k, v in params["send_formats"].items() }
            self.rtp_builder.set_payload_types_by_format(payload_types_by_format)
            
            for format in payload_types_by_format:
                if format.encoding == "telephone-event":
                    self.dtmf_injector.set_format(format)
            
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
            # FIXME: is this not working?
            self.report("detected", { 'id': self.label, 'remote_addr': addr })
            self.remote_addr = addr
            
        packet = self.rtp_parser.parse(udp)

        if packet is None:
            self.logger.debug("Ignoring received unknown payload type!")
        elif not self.dtmf_extractor.process(packet):
            self.forward(0, packet)
    
    
    def dtmf_detected(self, name):
        self.report("tone", dict(name=name), origin="FIXME")
        
        
    # FIXME: we should process the response for this
    #def dtmf_detected_response(self, msgid, params):
    #    self.logger.debug("Seems like he MGC %sacknowledged our DTMF!" % ("" if msgid[1] else "NOT "))
        
        
    def process(self, li, packet):
        for p in self.dtmf_injector.process(packet):
            udp = self.rtp_builder.build(p)
        
            if udp is None:
                self.logger.debug("Ignoring sent unknown payload format %s" % (p.format,))
                return

            if not self.remote_addr or not self.remote_addr[1]:
                return
            
            #self.logger.info("Sending RTP packet to %s" % (self.remote_addr,))
            # packet should be a bytearray here
            self.socket.sendto(udp, self.remote_addr)


    def notify(self, type, params):
        if type == "tone":
            name = params.get("name")
            self.dtmf_injector.inject(name)
        else:
            Thing.notify(self, type, params)
        

class EchoThing(Thing):
    def __init__(self, label, owner_sid):
        Thing.__init__(self, label, owner_sid, "echo")
        
        self.buffer = []


    def process(self, li, packet):
        # We don't care about payload types
        #self.logger.debug("Echoing %s packet on %s" % (format, self.label))
        self.buffer.append(packet)
        
        if len(self.buffer) > 10:
            p = self.buffer.pop(0)
            self.forward(0, p)


class PlayerThing(Thing):
    def __init__(self, label, owner_sid):
        Thing.__init__(self, label, owner_sid, "player")
        
        self.rtp_player = None
        self.format = None
        self.volume = 1
        self.filename = None


    def modify(self, params):
        self.logger.debug("Player thing modified: %s" % params)
        Thing.modify(self, params)

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
            self.filename = params["filename"]
            samples = read_wav(params["filename"])
            
            self.rtp_player = RtpPlayer(self.format, samples, self.volume, fade)
            self.rtp_player.packet_slot.plug(self.forward_packet)


    def forward_packet(self, packet):
        #self.logger.info("Generated packet from %s." % self.filename)
        self.forward(0, packet)
        

    def process(self, li, packet):
        pass


class MediaGateway(Loggable):
    def __init__(self, mgw_addr):
        Loggable.__init__(self)
        mgw_addr.assert_resolved()

        self.things_by_label = {}
        self.links = {}
        
        self.msgp = MsgpPeer(mgw_addr)
        self.msgp.request_slot.plug(self.process_request)
        self.msgp.response_slot.plug(self.process_response)


    def set_oid(self, oid):
        Loggable.set_oid(self, oid)
        self.msgp.set_oid(oid.add("msgp"))
        

    def set_name(self, name):
        self.msgp.set_name(name)
        
        
    def send_request(self, target, params, origin=None):
        self.msgp.send_request(target, params, origin=origin)
        
        
    def forward(self, label, li, packet):
        this = (label, li)
        that = self.links.get(this)
        
        if that:
            label, li = that
            self.things_by_label[label].process(li, packet)
            

    def create_thing(self, label, owner_sid, type):
        if type == "record":
            thing = RecordThing(label, owner_sid)
        elif type == "rtp":
            thing = RtpThing(label, owner_sid)
        elif type == "echo":
            thing = EchoThing(label, owner_sid)
        elif type == "player":
            thing = PlayerThing(label, owner_sid)
        else:
            raise Error("Invalid thing type '%s'!" % type)

        thing.set_oid(self.oid.add("thing", label))
        self.logger.info("Created thing %s" % label)

        if label in self.things_by_label:
            raise Error("Duplicate %s!" % type)
        
        self.things_by_label[label] = thing
        thing.set_mgw(proxy(self))
        
        return thing
        
        
    def modify_thing(self, label, params):
        self.things_by_label[label].modify(params)
        self.logger.info("Modified thing %s: %s" % (label, params))
        
        
    def delete_thing(self, label):
        self.things_by_label.pop(label)
        self.logger.info("Deleted thing %s" % label)


    def take_thing(self, label, owner_sid):
        self.things_by_label[label].owner_sid = owner_sid
        
        
    def notify_thing(self, label, type, params):
        self.things_by_label[label].notify(type, params)
        
        
    def link_slots(self, this, that):
        if this in self.links or that in self.links:
            raise Error("Things already linked!")
            
        if this[0] not in self.things_by_label:
            raise Error("Linked thing does not exist: %s!" % this[0])

        if that[0] not in self.things_by_label:
            raise Error("Linked thing does not exist: %s!" % that[0])
            
        self.links[this] = that
        self.links[that] = this
        
        
    def unlink_slots(self, this, that):
        if self.links[this] != that or self.links[that] != this:
            raise Error("Things not linked together!")
            
        self.links.pop(this)
        self.links.pop(that)
        
        
    def process_request(self, target, params, source):
        try:
            owner_sid, seq = source
            
            if target == "create_thing":
                label = params["label"]
                self.create_thing(label, owner_sid, params["type"])
                self.modify_thing(label, params)  # fake modification
            elif target == "modify_thing":
                label = params["label"]
                self.modify_thing(label, params)
            elif target == "delete_thing":
                label = params["label"]
                self.delete_thing(label)
            elif target == "take_thing":
                label = params["label"]
                self.take_thing(label, owner_sid)
            elif target == "tone":
                label = params["label"]
                self.notify_thing(label, "tone", params)
            elif target == "link_slots":
                slots = params["slots"]
                self.link_slots(tuple(slots[0]), tuple(slots[1]))
            elif target == "unlink_slots":
                slots = params["slots"]
                self.unlink_slots(tuple(slots[0]), tuple(slots[1]))
            else:
                raise Error("Invalid target %s!" % target)
        except Exception as e:
            self.logger.error("Processing error: %s" % e, exc_info=True)
            self.msgp.send_response(source, "error")
        else:
            self.msgp.send_response(source, "ok")
            
            if not self.things_by_label:
                self.logger.info("Back to clean state.")


    def process_response(self, origin, params, source):
        self.logger.debug("Got response for %s: %s" % (origin, params))
