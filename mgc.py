from __future__ import unicode_literals, print_function
import logging

from async import WeakMethod
from msgp import JsonMsgp, Sid
from util import vacuum

logger = logging.getLogger(__name__)


class Controller(object):
    last_mgc_number = 0
    
    def __init__(self, metapoll, mgc_addr):
        self.mgc_addr = mgc_addr
        self.metapoll = metapoll

        Controller.last_mgc_number += 1
        self.mgc_number = Controller.last_mgc_number
        self.last_leg_number = 0
        self.last_context_number = 0
        self.msgp = JsonMsgp(metapoll, mgc_addr, WeakMethod(self.process_request))
        
        
    def send_message(self, sid, target, params, response_handler):
        self.msgp.send_message(sid, target, params, response_handler=response_handler)
        
        
    def process_request(self, sid, seq, params, target):
        logger.debug("Unsolicited request from the MGW!")
        
        
    def create_context(self, sid, params, response_handler=None, request_handler=None):
        self.msgp.add_stream(sid, request_handler)
        self.send_message(sid, "create_context", params, response_handler)


    def modify_context(self, sid, params, response_handler=None):
        self.send_message(sid, "modify_context", params, response_handler)


    def delete_context(self, sid, response_handler=None):
        self.send_message(sid, "delete_context", None, response_handler)
        self.msgp.remove_stream(sid)
        
        
    def create_leg(self, sid, params, response_handler=None, request_handler=None):
        self.msgp.add_stream(sid, request_handler)
        self.send_message(sid, "create_leg", params, response_handler)


    def modify_leg(self, sid, params, response_handler=None):
        self.send_message(sid, "modify_leg", params, response_handler)


    def delete_leg(self, sid, response_handler=None):
        self.send_message(sid, "delete_leg", None, response_handler)
        self.msgp.remove_stream(sid)

        
    def generate_leg_sid(self, affinity=None):
        addr = self.select_gateway_address(affinity)
        self.last_leg_number += 1
        label = "leg-%s-%s" % (chr(96 + self.mgc_number), self.last_leg_number)
        sid = Sid(addr, label)
        return sid
        

    def generate_context_sid(self, affinity=None):
        addr = self.select_gateway_address(affinity)
        self.last_context_number += 1
        label = "ctx-%s-%s" % (chr(96 + self.mgc_number), self.last_context_number)
        sid = Sid(addr, label)
        return sid
    

    def allocate_media_address(self, channel_index):
        raise NotImplementedError()


    def deallocate_media_address(self, addr):
        raise NotImplementedError()
        
        
    def select_gateway_address(self, affinity=None):
        raise NotImplementedError()
        
        
class MediaLeg(object):
    def __init__(self, mgc, type, affinity=None):
        self.mgc = mgc
        self.type = type
        self.affinity = affinity
        self.sid = None


    def refresh(self, params):
        if not self.sid:
            self.sid = self.mgc.generate_leg_sid(self.affinity)
            params = dict(params, type=self.type)
            self.mgc.create_leg(self.sid, params, response_handler=lambda x, y, z: None)  # TODO
        else:
            self.mgc.modify_leg(self.sid, params, response_handler=lambda x, y, z: None)  # TODO
        
        
    def delete(self, handler=None):
        if self.sid:
            response_handler = lambda sid, source, body: handler()  # TODO
            self.mgc.delete_leg(self.sid, response_handler=response_handler)
        else:
            handler()
        

#class EchoedMediaLeg(MediaLeg):
#    def __init__(self):
#        super(EchoedMediaLeg, self).__init__(type="echo")


class PlayerMediaLeg(MediaLeg):
    def __init__(self, mgc):
        super(PlayerMediaLeg, self).__init__(mgc, "player")
        

    def play(self, filename=None, format=None, volume=1, fade=0):
        new = vacuum(dict(
            filename=filename,
            format=format,
            volume=volume,
            fade=fade
        ))
        
        self.refresh(new)


class ProxiedMediaLeg(MediaLeg):
    def __init__(self, mgc, local_addr):
        super(ProxiedMediaLeg, self).__init__(mgc, "net", affinity=local_addr)

        self.local_addr = local_addr
        self.committed = {}


    def update(self, **kwargs):
        new = dict(kwargs, local_addr=self.local_addr)
        changes = { k: v for k, v in new.items() if v != self.committed.get(k) }
        self.committed.update(changes)
        self.refresh(changes)
        
        
class MediaChannel(object):
    def __init__(self, mgc):
        self.mgc = mgc
        self.sid = None
        self.legs = [ None, None ]
        
        
    def set_legs(self, legs):
        changed = legs[0] and legs[1] and (legs[0] != self.legs[0] or legs[1] != self.legs[1])
        self.legs = legs
        
        if changed:
            self.refresh()
        else:
            logger.debug("Context not changed.")

        
    def process_mgw_request(self, sid, seq, params, target):
        logger.debug("Huh, MGW %s sent a %s message!" % (sid, target))
        
        
    def process_mgw_response(self, sid, seq, params, purpose):
        if params == "ok":
            logger.debug("Huh, MGW %s is OK for %s!" % (sid, purpose))
        else:
            logger.debug("Oops, MGW %s error for %s!" % (sid, purpose))
        
        
    def refresh(self):
        # TODO: implement leg deletion
        params = {
            'type': 'proxy',
            'legs': [ leg.sid.label for leg in self.legs ]
        }
        
        if not self.sid:
            leg_addrs = set(leg.sid.addr for leg in self.legs)
            if len(leg_addrs) != 1:
                raise Exception("Multiple addresses among media legs!")
                
            addr = leg_addrs.pop()
            self.sid = self.mgc.generate_context_sid(addr)
            logger.debug("Creating context %s" % (self.sid,))
            
            request_handler = WeakMethod(self.process_mgw_request)
            response_handler = WeakMethod(self.process_mgw_response, "cctx")
            self.mgc.create_context(self.sid, params, response_handler=response_handler, request_handler=request_handler)
        else:
            logger.debug("Modifying context %s" % (self.sid,))
            response_handler = WeakMethod(self.process_mgw_response, "mctx")
            self.mgc.modify_context(self.context_sid, params, response_handler=response_handler)
    

    def delete(self, handler=None):
        if self.sid:
            response_handler = lambda sid, source, body: handler()  # TODO
            self.mgc.delete_context(self.sid, response_handler=response_handler)
        else:
            handler()
