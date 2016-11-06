from weakref import proxy, WeakValueDictionary

from msgp import MsgpPeer  # MsgpClient
from util import vacuum, build_oid, Loggable
import zap


class MediaThing(Loggable):
    def __init__(self, mgc, sid):
        Loggable.__init__(self)
        
        self.mgc = mgc
        self.sid = sid
        self.is_created = False


    def set_oid(self, oid):
        Loggable.set_oid(self, oid)
        
        self.mgc.register_thing(self)


    def send_request(self, target, params, response_tag='dummy'):
        # The dummy response tag is to tell the Msgp to wait for an answer,
        # because the MGW will send it anyway, even in we later choose to ignore it here.
        self.mgc.send_message((self.sid, target), params, response_tag)


    def send_response(self, msgid, params, response_tag=None):
        self.mgc.send_message(msgid, params, response_tag)
        

    def process_request(self, target, msgid, params):
        self.logger.warning("Unknown request %s from MGW!" % target)


    def process_response(self, response_tag, msgid, params):
        if params == "ok":
            self.logger.debug("Huh, MGW message %s/%s was successful." % msgid)
        else:
            self.logger.debug("Oops, MGW message %s/%s failed!" % msgid)



        
class MediaLeg(MediaThing):
    def __init__(self, mgc, sid, type):
        MediaThing.__init__(self, mgc, sid)
        
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
            self.send_request("delete_leg", params, response_tag='delete')
            self.dirty_slot.zap()


    def notify(self, type, params):
        params = dict(params, id=self.oid)
        self.mgc.send_message((self.sid, type), params)
            

class PassMediaLeg(MediaLeg):
    def __init__(self, mgc, sid):
        super().__init__(mgc, sid, "pass")
        
        self.other = None
        
        
    def pair(self, other):
        self.other = other
        
        
    def refresh(self, params):
        MediaLeg.refresh(self, dict(params, other=self.other.oid))
        

class EchoMediaLeg(MediaLeg):
    def __init__(self, mgc, sid):
        super().__init__(mgc, sid, "echo")


class PlayerMediaLeg(MediaLeg):
    def __init__(self, mgc, sid):
        super().__init__(mgc, sid, "player")
        

    def play(self, filename=None, format=None, volume=1, fade=0):  # TODO: rename to refresh?
        new = vacuum(dict(
            filename=filename,
            format=format,
            volume=volume,
            fade=fade
        ))
        
        self.refresh(new)


class ProxiedMediaLeg(MediaLeg):
    def __init__(self, mgc, sid):
        super().__init__(mgc, sid, "net")

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
    def __init__(self, mgc, sid):
        MediaThing.__init__(self, mgc, sid)
        
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
            self.send_request("delete_context", params, response_tag='delete')


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
        
    
    def send_message(self, msgid, params, response_tag=None):
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
        if response_tag == 'delete':
            return  # The thing may be gone already
            
        oid = params.pop('id', None)
        
        if not oid:
            self.logger.error("Response from MGW without id, can't process!")
        else:
            self.logger.debug("Response %s from MGW to %s" % (response_tag, oid))
            thing = self.things_by_oid.get(oid)
            
            if thing:
                thing.process_response(response_tag, msgid, params)
            else:
                self.logger.warning("No thing for this response!")
        
        
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

    
    def make_media_leg(self, sid_affinity, type, **kwargs):
        sid = sid_affinity or self.select_gateway_sid()

        if type == "pass":
            return PassMediaLeg(proxy(self), sid, **kwargs)
        elif type == "echo":
            return EchoMediaLeg(proxy(self), sid, **kwargs)
        elif type == "player":
            return PlayerMediaLeg(proxy(self), sid, **kwargs)
        elif type == "net":
            return ProxiedMediaLeg(proxy(self), sid, **kwargs)
        else:
            raise Exception("No such media leg type: %s!" % type)
