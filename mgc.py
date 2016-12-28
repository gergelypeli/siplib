from weakref import proxy, WeakValueDictionary

from msgp import MsgpPeer  # MsgpClient
from util import vacuum, Loggable
import zap


def label_from_oid(oid):
    return "/".join(part.split("=")[1] if "=" in part else "" for part in oid.split(","))
    #return oid.replace("=", ":").replace(",", ";")


class MediaThing(Loggable):
    def __init__(self):
        Loggable.__init__(self)
        
        self.mgc = None
        self.sid = None
        self.is_created = False
        self.label = None


    def bind(self, mgc, sid):
        # Must be called after set_oid
        self.mgc = mgc
        self.sid = sid
        self.label = label_from_oid(self.oid)

        self.mgc.register_thing(self.label, self)
        

    def send_request(self, target, params, drop_response=False):
        response_tag = (self.label, drop_response)
        params["label"] = self.label
        self.mgc.send_message((self.sid, target), params, response_tag=response_tag)


    def send_response(self, msgid, params):
        self.mgc.send_message(msgid, params, response_tag=None)
        

    def process_request(self, target, msgid, params):
        self.logger.warning("Unknown request %s from MGW!" % target)


    def process_response(self, response_tag, msgid, params):
        if params == "ok":
            pass  #self.logger.debug("Huh, MGW message %s/%s was successful." % msgid)
        else:
            self.logger.debug("Oops, MGW message %s/%s failed!" % msgid)


class MediaLeg(MediaThing):
    def __init__(self, type):
        MediaThing.__init__(self)
        
        self.type = type


    def modify(self, params):
        if not self.is_created:
            self.is_created = True
            params = dict(params, type=self.type)
            self.send_request("create_leg", params)
        else:
            self.send_request("modify_leg", params)
        
        
    def create(self):
        if not self.is_created:
            self.modify({})


    def delete(self):
        if self.is_created:
            self.is_created = False  # Call uses this to ignore such MediaLeg-s
            params = {}
            self.send_request("delete_leg", params, drop_response=True)


    def notify(self, target, params):
        self.send_request(target, params)
            

class PassMediaLeg(MediaLeg):
    def __init__(self):
        MediaLeg.__init__(self, "pass")
        
        self.other = None
        
        
    def pair(self, other):
        self.other = other
        
        
    def modify(self, params):
        MediaLeg.modify(self, dict(params, other=self.other.label))
        

class EchoMediaLeg(MediaLeg):
    def __init__(self):
        MediaLeg.__init__(self, "echo")


class PlayerMediaLeg(MediaLeg):
    def __init__(self):
        MediaLeg.__init__(self, "player")
        

    def play(self, filename=None, format=None, volume=1, fade=0):  # TODO: rename to refresh?
        new = vacuum(dict(
            filename=filename,
            format=format,
            volume=volume,
            fade=fade
        ))
        
        self.modify(new)


class NetMediaLeg(MediaLeg):
    def __init__(self):
        MediaLeg.__init__(self, "net")

        self.event_slot = zap.EventSlot()
        #self.committed = {}


    #def update(self, **kwargs):  # TODO: rename to refresh?
    #    changes = { k: v for k, v in kwargs.items() if v != self.committed.get(k) }
    #    self.committed.update(changes)
    #    self.modify(changes)
        
    
    def process_request(self, target, msgid, params):
        if target == "tone":
            self.logger.debug("Yay, just detected a tone %s!" % (params,))
            self.send_response(msgid, "OK")
            self.event_slot.zap("tone", params)
        else:
            MediaLeg.process_request(self, target, msgid, params)
            
        
        
class MediaContext(MediaThing):
    def modify(self, params):
        if not self.is_created:
            self.is_created = True
            params = dict(params, type="proxy")
            self.send_request("create_context", params)
        else:
            self.send_request("modify_context", params)
    
    
    def create(self):
        if not self.is_created:
            self.modify({})
            

    def delete(self):
        if self.is_created:
            self.is_created = False
            self.logger.debug("Deleting context")
            self.send_request("delete_context", {}, drop_response=True)


class Controller(Loggable):
    def __init__(self):
        Loggable.__init__(self)

        self.mgw_sid = None  # FIXME: Msgp can handle multiple connections already!
        
        self.things_by_label = WeakValueDictionary()
        self.msgp = MsgpPeer(None)
        self.msgp.request_slot.plug(self.process_request)
        self.msgp.response_slot.plug(self.process_response)
        self.msgp.status_slot.plug(self.status_changed)
        
        
    def set_oid(self, oid):
        Loggable.set_oid(self, oid)
        
        self.msgp.set_oid(oid.add("msgp"))
        
        
    def set_name(self, name):
        self.msgp.set_name(name)


    def add_mgw_addr(self, addr):
        addr.assert_resolved()
        self.msgp.add_remote_addr(addr)
    
    
    def register_thing(self, label, thing):
        if label in self.things_by_label:
            raise Exception("Duplicate MGC thing label: %s!" % label)
            
        self.things_by_label[label] = thing
        
    
    def send_message(self, msgid, params, response_tag):
        #self.logger.info("XXX Sending message %s with %s tagged %s" % (msgid, params, response_tag))
        self.msgp.send(msgid, params, response_tag=response_tag)
        
        
    def process_request(self, target, msgid, params):
        label = params.pop('label', None)
        
        if not label:
            self.logger.error("Request from MGW without label, can't process!")
        else:
            self.logger.debug("Request %s from MGW to %s" % (target, label))
            thing = self.things_by_label.get(label)
            
            if thing:
                thing.process_request(target, msgid, params)
            else:
                self.logger.warning("No thing for this %s request!" % target)


    def process_response(self, response_tag, msgid, params):
        #self.logger.info("XXX Received response %s with %s tagged %s" % (msgid, params, response_tag))
        label, drop_response = response_tag
        thing = self.things_by_label.get(label)
        
        if not thing:
            if not drop_response:
                # TODO: Hm, this can still happen, if we modify and delete something
                # quickly, and the thing is gone when the modify response arrives.
                # Probably we shouldn't delete things with a pending modification,
                # but deleting should be an unrefusable step.
                self.logger.error("Response from MGW to unknown entity: %s" % label)
        else:
            self.logger.debug("Response from MGW to %s" % label)
            thing.process_response(None, msgid, params)
        
        
    def status_changed(self, sid, remote_addr):
        if remote_addr:
            self.logger.debug("MGW stream %s is now available at %s" % (sid, remote_addr))
        else:
            self.logger.error("MGW stream %s is now gone!" % sid)
            
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

    
    def make_media_leg(self, type):
        # TODO: maybe this function shouldn't be in Controller at all.
        ml = None
        
        if type == "pass":
            ml = PassMediaLeg()
        elif type == "echo":
            ml = EchoMediaLeg()
        elif type == "player":
            ml = PlayerMediaLeg()
        elif type == "net":
            ml = NetMediaLeg()
        elif type == "context":  # FIXME!
            ml = MediaContext()
        else:
            raise Exception("No such media leg type: %s!" % type)

        self.logger.info("Made media leg of type %s" % type)
        return ml


    def bind_media_leg(self, ml, mgw_sid):
        ml.bind(proxy(self), mgw_sid)
