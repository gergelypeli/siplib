from __future__ import unicode_literals, print_function

from async import WeakMethod, Weak
#from msgp import JsonMsgp, Sid
from msgp import MsgpClient
from util import vacuum, build_oid, Loggable

        
class MediaLeg(Loggable):
    def __init__(self, mgc, sid, type):
        Loggable.__init__(self)
        
        self.mgc = mgc
        self.sid = sid
        self.type = type
        self.is_created = False


    def realize(self):
        if not self.is_created:
            # This shouldn't happen, unless one Leg was lazy
            self.logger.warning("Leg was forcedly realized!")
            self.refresh({})

        return self.oid
        

    def refresh(self, params):
        if not self.is_created:
            self.is_created = True
            #self.sid = self.mgc.generate_leg_sid(self.affinity)
            #self.sid = self.mgc.select_gateway_sid(self.affinity)
            params = dict(params, id=self.oid, type=self.type)
            self.mgc.create_leg(self.sid, params, response_handler=lambda x, y: None)  # TODO
        else:
            params = dict(params, id=self.oid)
            self.mgc.modify_leg(self.sid, params, response_handler=lambda x, y: None)  # TODO
        
        
    def delete(self, handler=None):
        if self.is_created:
            params = dict(id=self.oid)
            response_handler = lambda msgid, params: handler()  # TODO
            self.mgc.delete_leg(self.sid, params, response_handler=response_handler)
        else:
            handler()


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
    def __init__(self, mgc, sid, local_addr):
        super().__init__(mgc, sid, "net")

        self.local_addr = local_addr
        self.committed = {}


    def update(self, **kwargs):  # TODO: rename to refresh?
        new = dict(kwargs, local_addr=self.local_addr)
        changes = { k: v for k, v in new.items() if v != self.committed.get(k) }
        self.committed.update(changes)
        self.refresh(changes)
        
        
class MediaContext(Loggable):
    def __init__(self, mgc):
        Loggable.__init__(self)
        
        self.mgc = mgc
        self.sid = None
        self.legs = []
        self.is_created = False
        

    def set_legs(self, legs):
        if legs != self.legs:
            self.legs = legs
            
            leg_sids = set(leg.sid for leg in self.legs)
            if len(leg_sids) != 1:
                raise Exception("Multiple sids among media legs!")

            sid = leg_sids.pop()
                
            if self.sid and self.sid != sid:
                raise Exception("Context sid changed!")
                
            self.sid = sid
            self.refresh()
        else:
            self.logger.debug("Context not changed.")

        
    def process_mgw_response(self, msgid, params):
        if params == "ok":
            self.logger.debug("Huh, MGW message %s/%s was successful." % msgid)
        else:
            self.logger.debug("Oops, MGW message %s/%s failed!" % msgid)
        
        
    def refresh(self):
        # TODO: implement leg deletion
        leg_oids = [ leg.realize() for leg in self.legs ]
        
        params = {
            'id': self.oid,
            'type': 'proxy',
            'legs': leg_oids
        }
        
        if not self.is_created:
            self.is_created = True
            self.logger.debug("Creating context %s" % (self.oid,))
            
            #request_handler = WeakMethod(self.process_mgw_request)
            response_handler = WeakMethod(self.process_mgw_response)
            self.mgc.create_context(self.sid, params, response_handler=response_handler)
        else:
            self.logger.debug("Modifying context %s" % (self.oid,))
            response_handler = WeakMethod(self.process_mgw_response)
            self.mgc.modify_context(self.sid, params, response_handler=response_handler)
    

    def delete(self, handler=None):
        if self.is_created:
            params = dict(id=self.oid)
            response_handler = lambda msgid, params: handler()  # TODO
            self.mgc.delete_context(self.sid, params, response_handler=response_handler)
        else:
            handler()


class Controller(Loggable):
    def __init__(self, metapoll):
        Loggable.__init__(self)

        self.metapoll = metapoll
        self.mgw_sid = None  # FIXME: Msgp can handle multiple connections already!
        
        self.msgp = MsgpClient(metapoll, WeakMethod(self.process_request), WeakMethod(self.status_changed))
        
        
    def set_oid(self, oid):
        Loggable.set_oid(self, oid)
        self.msgp.set_oid(build_oid(oid, "msgp"))


    def add_mgw_addr(self, addr):
        self.msgp.add_mgw_addr(addr)
                
        
    def send_message(self, sid, target, params, response_handler):
        if not params.get('id'):
            raise Exception("An id is missing here...")
            
        msgid = (sid, target)
        self.msgp.send(msgid, params, response_handler=response_handler)
        
        
    def process_request(self, target, msgid, params):
        self.logger.debug("Unsolicited request from the MGW!")
        
        
    def create_context(self, sid, params, response_handler=None):
        self.send_message(sid, "create_context", params, response_handler)


    def modify_context(self, sid, params, response_handler=None):
        self.send_message(sid, "modify_context", params, response_handler)


    def delete_context(self, sid, params, response_handler=None):
        self.send_message(sid, "delete_context", params, response_handler)
        
        
    def create_leg(self, sid, params, response_handler=None):
        self.send_message(sid, "create_leg", params, response_handler)


    def modify_leg(self, sid, params, response_handler=None):
        self.send_message(sid, "modify_leg", params, response_handler)


    def delete_leg(self, sid, params, response_handler=None):
        self.send_message(sid, "delete_leg", params, response_handler)


    def status_changed(self, sid, remote_addr):
        self.mgw_sid = sid if remote_addr else None


    def select_gateway_sid(self):
        if not self.mgw_sid:
            raise Exception("Sorry, no mgw_sid yet!")
        else:
            return self.mgw_sid
    

    def allocate_media_address(self, sid):
        raise NotImplementedError()


    def deallocate_media_address(self, sid, addr):
        raise NotImplementedError()

    
    def make_media_leg(self, sid_affinity, type):
        sid = sid_affinity or self.select_gateway_sid()

        if type == "pass":
            return PassMediaLeg(Weak(self), sid)
        elif type == "echo":
            return EchoMediaLeg(Weak(self), sid)
        elif type == "player":
            return PlayerMediaLeg(Weak(self), sid)
        elif type == "net":
            local_addr = self.allocate_media_address(sid)
            return ProxiedMediaLeg(Weak(self), sid, local_addr)
        else:
            raise Exception("No such media leg type: %s!" % type)
