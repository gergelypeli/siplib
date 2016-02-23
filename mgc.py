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
        self.report_dirty = None


    def set_report_dirty(self, report_dirty):
        self.report_dirty = report_dirty
        

    def realize(self):
        if not self.is_created:
            raise Exception("Leg was not realized but needed for a context!")
            # This shouldn't happen, unless one Leg was lazy
            #self.logger.warning("Leg was forcedly realized!")
            #self.refresh({})

        return self.oid
        

    def refresh(self, params):
        if not self.is_created:
            self.is_created = True
            #self.sid = self.mgc.generate_leg_sid(self.affinity)
            #self.sid = self.mgc.select_gateway_sid(self.affinity)
            params = dict(params, id=self.oid, type=self.type)
            self.mgc.create_leg(self.sid, params, response_handler=lambda x, y: None, request_handler=WeakMethod(self.process_request))  # TODO
            self.report_dirty()
        else:
            params = dict(params, id=self.oid)
            self.mgc.modify_leg(self.sid, params, response_handler=lambda x, y: None)  # TODO
        
        
    def delete(self, handler=None):
        if self.is_created:
            params = dict(id=self.oid)
            response_handler = lambda msgid, params: handler()  # TODO
            self.mgc.delete_leg(self.sid, params, response_handler=response_handler)
            self.report_dirty()
        else:
            handler()


    def notify(self, type, params):
        params = dict(params, id=self.oid)
        self.mgc.send_message((self.sid, type), params)
            
        
    def process_request(self, target, msgid, params):
        self.logger.warning("Unknown request %s from MGW!" % target)


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
    def __init__(self, mgc, sid, report=None):
        super().__init__(mgc, sid, "net")

        self.report = report
        self.committed = {}


    def update(self, **kwargs):  # TODO: rename to refresh?
        changes = { k: v for k, v in kwargs.items() if v != self.committed.get(k) }
        self.committed.update(changes)
        self.refresh(changes)
        
    
    def process_request(self, target, msgid, params):
        if target == "tone":
            self.logger.debug("Yay, just got a tone %s!" % (params,))
            self.mgc.send_message(msgid, "OK")
            self.report("tone", params)
        else:
            MediaLeg.process_request(self, target, msgid, params)
            
        
        
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
        
        self.request_handlers_by_id = {}
        self.msgp = MsgpClient(metapoll, WeakMethod(self.process_request), WeakMethod(self.status_changed))
        
        
    def set_oid(self, oid):
        Loggable.set_oid(self, oid)
        self.msgp.set_oid(build_oid(oid, "msgp"))


    def add_mgw_addr(self, addr):
        self.msgp.add_mgw_addr(addr)
    
    
    def manage_request_handler(self, id, request_handler):
        if request_handler:
            self.logger.debug("Added request handler %s." % id)
            self.request_handlers_by_id[id] = request_handler
        else:
            if self.request_handlers_by_id.pop(id):
                self.logger.debug("Removed request handler %s." % id)


    def send_message(self, msgid, params, response_handler=None, request_handler=None):
        if not msgid[1].isdigit() and not params.get('id'):
            raise Exception("A request id is missing here...")
            
        self.msgp.send(msgid, params, response_handler=response_handler)
        
        
    def process_request(self, target, msgid, params):
        oid = params.pop('id', None)
        
        if not oid:
            self.logger.error("Request from MGW without id, can't process!")
        else:
            self.logger.debug("Request %s from MGW to %s" % (target, oid))
            request_handler = self.request_handlers_by_id.get(oid)
            
            if request_handler:
                request_handler(target, msgid, params)
            else:
                self.logger.warning("No handler for this request!")
        
        
    # TODO: remove these methods
    def create_context(self, sid, params, response_handler=None):
        self.send_message((sid, "create_context"), params, response_handler)


    def modify_context(self, sid, params, response_handler=None):
        self.send_message((sid, "modify_context"), params, response_handler)


    def delete_context(self, sid, params, response_handler=None):
        self.send_message((sid, "delete_context"), params, response_handler)
        
        
    def create_leg(self, sid, params, response_handler=None, request_handler=None):
        self.manage_request_handler(params["id"], request_handler)
        self.send_message((sid, "create_leg"), params, response_handler)


    def modify_leg(self, sid, params, response_handler=None):
        self.send_message((sid, "modify_leg"), params, response_handler)


    def delete_leg(self, sid, params, response_handler=None):
        self.send_message((sid, "delete_leg"), params, response_handler)
        self.manage_request_handler(params["id"], None)


    def status_changed(self, sid, remote_addr):
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
            return PassMediaLeg(Weak(self), sid, **kwargs)
        elif type == "echo":
            return EchoMediaLeg(Weak(self), sid, **kwargs)
        elif type == "player":
            return PlayerMediaLeg(Weak(self), sid, **kwargs)
        elif type == "net":
            return ProxiedMediaLeg(Weak(self), sid, **kwargs)
        else:
            raise Exception("No such media leg type: %s!" % type)
