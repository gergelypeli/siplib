from weakref import proxy, WeakValueDictionary

from msgp import MsgpPeer  # MsgpClient
from util import vacuum, build_oid, Loggable
import zap


class MediaThing(Loggable):
    def __init__(self):
        Loggable.__init__(self)
        
        self.mgc = None
        self.sid = None
        self.is_created = False


    def bind(self, mgc, sid):
        self.mgc = mgc
        self.sid = sid

        self.mgc.register_thing(self)
        

    def send_request(self, target, params, drop_response=False):
        response_tag = (self.oid, drop_response)
        self.mgc.send_message((self.sid, target), params, response_tag=response_tag)


    def send_response(self, msgid, params):
        self.mgc.send_message(msgid, params, response_tag=None)
        

    def process_request(self, target, msgid, params):
        self.logger.warning("Unknown request %s from MGW!" % target)


    def process_response(self, response_tag, msgid, params):
        if params == "ok":
            self.logger.debug("Huh, MGW message %s/%s was successful." % msgid)
        else:
            self.logger.debug("Oops, MGW message %s/%s failed!" % msgid)



        
class MediaLeg(MediaThing):
    def __init__(self, type):
        MediaThing.__init__(self)
        
        self.type = type
        self.dirty_slot = zap.Slot()


    def refresh(self, params):
        if not self.is_created:
            self.is_created = True
            params = dict(params, id=self.oid, type=self.type)
            self.send_request("create_leg", params)
            self.dirty_slot.zap()
        else:
            params = dict(params, id=self.oid)
            self.send_request("modify_leg", params)
        
        
    def delete(self):
        if self.is_created:
            self.is_created = False  # Call uses this to ignore such MediaLeg-s
            params = dict(id=self.oid)
            self.send_request("delete_leg", params, drop_response=True)
            self.dirty_slot.zap()


    def notify(self, target, params):
        params = dict(params, id=self.oid)
        self.send_request(target, params)
            

class PassMediaLeg(MediaLeg):
    def __init__(self):
        super().__init__("pass")
        
        self.other = None
        
        
    def pair(self, other):
        self.other = other
        
        
    def refresh(self, params):
        MediaLeg.refresh(self, dict(params, other=self.other.oid))
        

class EchoMediaLeg(MediaLeg):
    def __init__(self):
        super().__init__("echo")


class PlayerMediaLeg(MediaLeg):
    def __init__(self):
        super().__init__("player")
        

    def play(self, filename=None, format=None, volume=1, fade=0):  # TODO: rename to refresh?
        new = vacuum(dict(
            filename=filename,
            format=format,
            volume=volume,
            fade=fade
        ))
        
        self.refresh(new)


class NetMediaLeg(MediaLeg):
    def __init__(self):
        super().__init__("net")

        self.event_slot = zap.EventSlot()
        self.committed = {}


    def update(self, **kwargs):  # TODO: rename to refresh?
        changes = { k: v for k, v in kwargs.items() if v != self.committed.get(k) }
        self.committed.update(changes)
        self.refresh(changes)
        
    
    def process_request(self, target, msgid, params):
        if target == "tone":
            self.logger.debug("Yay, just got a tone %s!" % (params,))
            self.send_response(msgid, "OK")
            self.event_slot.zap("tone", params)
        else:
            MediaLeg.process_request(self, target, msgid, params)
            
        
        
class MediaContext(MediaThing):
    def __init__(self):
        MediaThing.__init__(self)
        
        self.leg_oids = []
        

    def set_leg_oids(self, leg_oids):
        if leg_oids != self.leg_oids:
            self.leg_oids = leg_oids
            self.refresh()
        else:
            self.logger.debug("Context legs not changed.")

        
    def refresh(self):
        # TODO: implement leg deletion
        params = {
            'id': self.oid,
            'type': 'proxy',
            'legs': self.leg_oids
        }
        
        if not self.is_created:
            self.is_created = True
            self.logger.debug("Creating context")
            self.send_request("create_context", params)
        else:
            self.logger.debug("Modifying context")
            self.send_request("modify_context", params)
    

    def delete(self):
        if self.is_created:
            self.is_created = False
            self.logger.debug("Deleting context")
            params = dict(id=self.oid)
            self.send_request("delete_context", params, drop_response=True)


class Controller(Loggable):
    def __init__(self):
        Loggable.__init__(self)

        self.mgw_sid = None  # FIXME: Msgp can handle multiple connections already!
        
        self.things_by_oid = WeakValueDictionary()
        self.msgp = MsgpPeer(None)
        self.msgp.request_slot.plug(self.process_request)
        self.msgp.response_slot.plug(self.process_response)
        self.msgp.status_slot.plug(self.status_changed)
        
        
    def set_oid(self, oid):
        Loggable.set_oid(self, oid)
        
        self.msgp.set_oid(build_oid(oid, "msgp"))
        
        
    def set_name(self, name):
        self.msgp.set_name(name)


    def add_mgw_addr(self, addr):
        self.msgp.add_remote_addr(addr)
    
    
    def register_thing(self, thing):
        self.things_by_oid[thing.oid] = thing
        
    
    def send_message(self, msgid, params, response_tag):
        self.msgp.send(msgid, params, response_tag=response_tag)
        
        
    def process_request(self, target, msgid, params):
        oid = params.pop('id', None)
        
        if not oid:
            self.logger.error("Request from MGW without id, can't process!")
        else:
            self.logger.debug("Request %s from MGW to %s" % (target, oid))
            thing = self.things_by_oid.get(oid)
            
            if thing:
                thing.process_request(target, msgid, params)
            else:
                self.logger.warning("No thing for this request!")


    def process_response(self, response_tag, msgid, params):
        oid, drop_response = response_tag
        thing = self.things_by_oid.get(oid)
        
        if not thing:
            if not drop_response:
                self.logger.error("Response from MGW to unknown entity!")
        else:
            self.logger.debug("Response from MGW to %s" % oid)
            thing.process_response(None, msgid, params)
        
        
    def status_changed(self, sid, remote_addr):
        if remote_addr:
            self.logger.debug("MGW stream %s is now available at %s" % (sid, remote_addr))
        else:
            self.logger.error("MGW stream %s is now gone!" % sid)
            
        self.mgw_sid = sid if remote_addr else None


    def select_gateway_sid(self):
        if not self.mgw_sid:
            raise Exception("Sorry, no mgw_sid yet!")
        else:
            return self.mgw_sid
    

    def allocate_media_address(self, sid):
        raise NotImplementedError()


    def deallocate_media_address(self, addr):
        raise NotImplementedError()

    
    def make_media_leg(self, type):
        # TODO: maybe this function shouldn't be in Controller at all.
        
        if type == "pass":
            return PassMediaLeg()
        elif type == "echo":
            return EchoMediaLeg()
        elif type == "player":
            return PlayerMediaLeg()
        elif type == "net":
            return NetMediaLeg()
        else:
            raise Exception("No such media leg type: %s!" % type)


    def bind_thing(self, ml, sid_affinity):
        sid = sid_affinity or self.select_gateway_sid()
        ml.bind(proxy(self), sid)
