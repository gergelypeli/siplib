from weakref import proxy, ref
from collections import namedtuple

from msgp import MsgpPeer
from log import Loggable
from zap import EventSlot, Plug


def label_from_oid(oid):
    #return "/".join(part.split("=")[1] if "=" in part else "" for part in oid.split(","))
    #return oid.replace("=", ":").replace(",", ";")
    return oid.replace("switch=", "").replace("call=", "").replace("media=", "").replace("=", ":").replace(",", "/")


MediaLeg = namedtuple("MediaLeg", [ "mgw_sid", "label", "li" ])


class MediaThing(Loggable):
    def __init__(self, type):
        Loggable.__init__(self)
        
        self.type = type

        self.mgc = None
        self.sid = None
        self.is_created = False
        self.label = None


    def __del__(self):
        self.delete()


    def set_mgw(self, sid):
        self.sid = sid


    def set_mgc(self, mgc):
        self.mgc = mgc


    def set_oid(self, oid):
        Loggable.set_oid(self, oid)
        
        self.label = label_from_oid(self.oid)
        self.mgc.register_thing(self.label, self)


    def get_leg(self, li):
        return MediaLeg(self.sid, self.label, li)
        

    def send_request(self, ttag, params, drop_response=False):
        params["label"] = self.label
        target = (self.sid, ttag)
        
        self.mgc.send_request(target, params, self.label, drop_response)


    def send_response(self, target, params):
        self.mgc.send_response(target, params)
        

    def process_request(self, target, params, source):
        self.logger.warning("Unknown request %s from MGW!" % target)


    def process_response(self, origin, params, source):
        if params == "ok":
            pass  #self.logger.debug("Huh, MGW message %s/%s was successful." % msgid)
        else:
            self.logger.error("Oops, MGW message %s/%s failed!" % source)


    def create(self):
        if not self.is_created:
            self.is_created = True
            params = dict(type=self.type)
            self.send_request("create_thing", params)


    def modify(self, params):
        if not self.is_created:
            raise Exception("Media thing not yet created!")

        self.send_request("modify_thing", params)


    def delete(self):
        if self.is_created:
            self.is_created = False
            params = {}
            self.send_request("delete_thing", params, drop_response=True)


    def notify(self, target, params):
        self.send_request(target, params)
            

class RecordMediaThing(MediaThing):
    def __init__(self):
        MediaThing.__init__(self, "record")
        

class EchoMediaThing(MediaThing):
    def __init__(self):
        MediaThing.__init__(self, "echo")


class PlayerMediaThing(MediaThing):
    def __init__(self):
        MediaThing.__init__(self, "player")
        

    def play(self, filename=None, format=None, volume=1, fade=0):  # TODO: rename to refresh?
        params = dict(
            filename=filename,
            format=format,
            volume=volume,
            fade=fade
        )
        
        params = { k: v for k, v in params.items() if v is not None }
        
        self.modify(params)


class RtpMediaThing(MediaThing):
    def __init__(self):
        MediaThing.__init__(self, "rtp")

        self.event_slot = EventSlot()

    
    def process_request(self, target, params, source):
        if target == "tone":
            self.logger.debug("Yay, just detected a tone %s!" % (params,))
            self.send_response(source, "OK")
            self.event_slot.zap("tone", params)
        else:
            MediaThing.process_request(self, target, params, source)
        
        
class Controller(Loggable):
    def __init__(self):
        Loggable.__init__(self)

        self.mgw_sid = None  # FIXME: Msgp can handle multiple connections already!
        
        # Store weak references, and only remove the nullified ones
        # after getting a drop_response response from the MGW.
        self.wthings_by_label = {}
        
        self.msgp = MsgpPeer(None)
        Plug(self.process_request).attach(self.msgp.request_slot)
        Plug(self.process_response).attach(self.msgp.response_slot)
        Plug(self.status_changed).attach(self.msgp.status_slot)
        
        
    def set_oid(self, oid):
        Loggable.set_oid(self, oid)
        
        self.msgp.set_oid(oid.add("msgp"))
        
        
    def set_name(self, name):
        self.msgp.set_name(name)


    def add_mgw_addr(self, addr):
        addr.assert_resolved()
        self.msgp.add_remote_addr(addr)
    
    
    def register_thing(self, label, thing):
        if label in self.wthings_by_label:
            raise Exception("Duplicate MGC thing label: %s!" % label)
            
        self.wthings_by_label[label] = ref(thing)
        
    
    def send_request(self, target, params, label=None, drop_response=False):
        # If we pass an origin, the MGW must respond, otherwise our side will time out!
        origin = (label, drop_response) if label else None
        self.msgp.send_request(target, params, origin=origin)


    def send_response(self, target, params, label=None, drop_response=False):
        # If we pass an origin, the MGW must respond, otherwise our side will time out!
        origin = (label, drop_response) if label else None
        self.msgp.send_response(target, params, origin=origin)
        
        
    def process_request(self, target, params, source):
        label = params.pop('label', None)
        
        if not label:
            self.logger.error("Request from MGW without label, can't process!")
        else:
            self.logger.debug("Request to thing %s" % label)
            wthing = self.wthings_by_label.get(label)
            
            if wthing:
                thing = wthing()
                
                if thing:
                    thing.process_request(target, params, source)
                else:
                    self.logger.debug("Ignoring request to just deleted thing %s!" % label)
            else:
                self.logger.warning("Request to unknown thing %s!" % label)


    def process_response(self, origin, params, source):
        label, drop_response = origin

        if not label:
            self.logger.info("Response from MGW without label, ignoring.")
        else:
            self.logger.debug("Response to thing %s" % label)
            wthing = self.wthings_by_label.get(label)
        
            if not wthing:
                self.logger.warning("Response to unknown thing %s!" % label)
            elif drop_response:
                self.logger.debug("Dropping last response to thing %s." % label)
                self.wthings_by_label.pop(label)
            else:
                thing = wthing()
            
                if thing:
                    thing.process_response(None, params, source)
                else:
                    self.logger.debug("Ignoring response to just deleted thing %s!" % label)
        
        
    def status_changed(self, sid, remote_addr):
        if remote_addr:
            self.logger.debug("MGW %s is reachable at %s" % (sid, remote_addr))
        else:
            self.logger.error("MGW %s is unreachable!" % sid)
            
        self.mgw_sid = sid if remote_addr else None


    def select_gateway_sid(self, ctype, mgw_affinity):
        if not self.mgw_sid:
            raise Exception("Sorry, no mgw_sid yet!")
        elif mgw_affinity and mgw_affinity != self.mgw_sid:
            raise Exception("WAT, we don't know this MGW!")
        else:
            return self.mgw_sid
    

    def allocate_media_address(self, mgw_sid):
        raise NotImplementedError()


    def deallocate_media_address(self, addr):
        raise NotImplementedError()

    
    def make_media_thing(self, type):
        # TODO: maybe this function shouldn't be in Controller at all.
        mt = None
        
        if type == "record":
            mt = RecordMediaThing()
        elif type == "echo":
            mt = EchoMediaThing()
        elif type == "player":
            mt = PlayerMediaThing()
        elif type == "rtp":
            mt = RtpMediaThing()
        else:
            raise Exception("No such media leg type: %s!" % type)

        mt.set_mgc(proxy(self))

        self.logger.info("Made media thing of type %s" % type)
        return mt


    def link_media_legs(self, ml0, ml1):
        if ml0.mgw_sid != ml1.mgw_sid:
            raise Exception("Can't link media legs on different MGWs!")  # TODO
            
        target = (ml0.mgw_sid, "link_slots")
        params = dict(slots=[ (ml0.label, ml0.li), (ml1.label, ml1.li) ])
        self.msgp.send_request(target, params, origin=(None, None))


    def unlink_media_legs(self, ml0, ml1):
        if ml0.mgw_sid != ml1.mgw_sid:
            raise Exception("Can't unlink media legs on different MGWs!")  # TODO
            
        target = (ml0.mgw_sid, "unlink_slots")
        params = dict(slots=[ (ml0.label, ml0.li), (ml1.label, ml1.li) ])
        self.msgp.send_request(target, params, origin=(None, None))
