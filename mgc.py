from __future__ import unicode_literals, print_function

from async import WeakMethod
#from msgp import JsonMsgp, Sid
from msgp2b import MsgpClient
from util import vacuum, Logger, build_oid, Loggable

        
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
        

#class EchoedMediaLeg(MediaLeg):
#    def __init__(self):
#        super(EchoedMediaLeg, self).__init__(type="echo")


class PlayerMediaLeg(MediaLeg):
    def __init__(self, mgc, sid):
        super(PlayerMediaLeg, self).__init__(mgc, sid, "player")
        

    def play(self, filename=None, format=None, volume=1, fade=0):
        new = vacuum(dict(
            filename=filename,
            format=format,
            volume=volume,
            fade=fade
        ))
        
        self.refresh(new)


class ProxiedMediaLeg(MediaLeg):
    def __init__(self, mgc, sid, local_addr):
        super(ProxiedMediaLeg, self).__init__(mgc, sid, "net")

        self.local_addr = local_addr
        self.committed = {}


    def update(self, **kwargs):
        new = dict(kwargs, local_addr=self.local_addr)
        changes = { k: v for k, v in new.items() if v != self.committed.get(k) }
        self.committed.update(changes)
        self.refresh(changes)
        
        
class MediaChannel(Loggable):
    def __init__(self, mgc):
        Loggable.__init__(self)
        
        self.mgc = mgc
        self.sid = None
        self.legs = [ None, None ]
        self.is_created = False
        

    def set_legs(self, legs):
        changed = legs[0] and legs[1] and (legs[0] != self.legs[0] or legs[1] != self.legs[1])
        self.legs = legs
        
        if changed:
            self.refresh()
        else:
            self.logger.debug("Context not changed.")

        
    #def process_mgw_request(self, sid, seq, params, target):
    #    self.logger.debug("Huh, MGW %s sent a %s message!" % (sid, target))
        
        
    def process_mgw_response(self, msgid, params):
        if params == "ok":
            self.logger.debug("Huh, MGW %s is OK for %s!" % msgid)
        else:
            self.logger.debug("Oops, MGW %s error for %s!" % msgid)
        
        
    def refresh(self):
        # TODO: implement leg deletion
        #leg_labels = []
        
        #for leg in self.legs:
        #    if not leg.sid:
        #        raise Exception("Bridged media leg is not created yet!")
                
        #    leg_labels.append(leg.sid.label)
        
        leg_oids = [ leg.realize() for leg in self.legs ]
        
        params = {
            'id': self.oid,
            'type': 'proxy',
            'legs': leg_oids
        }
        
        if not self.is_created:
            self.is_created = True
            leg_sids = set(leg.sid for leg in self.legs)
            if len(leg_sids) != 1:
                raise Exception("Multiple sids among media legs!")
                
            self.sid = leg_sids.pop()
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


class Controller(object):
    last_mgc_number = 0
    
    def __init__(self, metapoll, mgw_addr):
        self.metapoll = metapoll

        Controller.last_mgc_number += 1
        self.mgw_sid = None
        
        self.mgc_number = Controller.last_mgc_number
        self.last_leg_number = 0
        self.last_context_number = 0  # keep this unique even across MGW-s for logging!
        #self.msgp = JsonMsgp(metapoll, mgc_addr, WeakMethod(self.process_request))
        self.msgp = MsgpClient(metapoll, WeakMethod(self.process_request), WeakMethod(self.status_changed), mgw_addr)
        
        self.logger = Logger()
        
        
    def set_oid(self, oid):
        self.logger.set_oid(oid)
        self.msgp.set_oid(build_oid(oid, "msgp"))
        
        
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
