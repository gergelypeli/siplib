from weakref import proxy, ref

from msgp import MsgpPeer
from log import Loggable
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


    def set_mgw(self, sid):
        self.sid = sid


    def set_mgc(self, mgc):
        self.mgc = mgc


    def set_oid(self, oid):
        Loggable.set_oid(self, oid)
        
        self.label = label_from_oid(self.oid)
        self.mgc.register_thing(self.label, self)
        

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
        params = dict(
            filename=filename,
            format=format,
            volume=volume,
            fade=fade
        )
        
        params = { k: v for k, v in params.items() if v is not None }
        
        self.modify(params)


class NetMediaLeg(MediaLeg):
    def __init__(self):
        MediaLeg.__init__(self, "net")

        self.event_slot = zap.EventSlot()
        #self.committed = {}


    #def update(self, **kwargs):  # TODO: rename to refresh?
    #    changes = { k: v for k, v in kwargs.items() if v != self.committed.get(k) }
    #    self.committed.update(changes)
    #    self.modify(changes)
        
    
    def process_request(self, target, params, source):
        if target == "tone":
            self.logger.debug("Yay, just detected a tone %s!" % (params,))
            self.send_response(source, "OK")
            self.event_slot.zap("tone", params)
        else:
            MediaLeg.process_request(self, target, params, source)
            
        
        
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
        
        # Store weak references, and only remove the nullified ones
        # after getting a drop_response response from the MGW.
        self.wthings_by_label = {}  # WeakValueDictionary()
        
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
            self.logger.error("Response from MGW without label, can't process!")
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

        ml.set_mgc(proxy(self))

        self.logger.info("Made media leg of type %s" % type)
        return ml
