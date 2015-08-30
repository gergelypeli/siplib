from __future__ import unicode_literals, print_function
from async import WeakMethod
import msgp


class Controller(object):
    def __init__(self, metapoll, mgc_addr):
        self.mgc_addr = mgc_addr
        
        self.metapoll = metapoll
        self.msgp = msgp.JsonMsgp(metapoll, mgc_addr, WeakMethod(self.process_request))
        
        
    def send_message(self, sid, target, params, response_handler):
        self.msgp.send_message(sid, target, params, response_handler=response_handler)
        
        
    def process_request(self, sid, seq, params, target):
        print("Unsolicited request from the MGW!")
        
        
    def create_context(self, sid, params, response_handler=None, request_handler=None):
        self.msgp.add_stream(sid, request_handler)
        self.send_message(sid, "create", params, response_handler)


    def modify_context(self, sid, params, response_handler=None):
        self.send_message(sid, "modify", params, response_handler)


    def delete_context(self, sid, response_handler=None):
        self.send_message(sid, "delete", None, response_handler)
        self.msgp.remove_stream(sid)
        
        
    def make_media_channel(self, left_info, right_info):
        raise NotImplementedError()


class MediaLeg(object):
    # TODO: add MGW affinity field!
    
    def __init__(self, **kwargs):
        self.committed = {}
        self.current = kwargs


    def get(self, k, v=None):
        return self.current.get(k, v)
        

    def update(self, **kwargs):
        self.current.update(kwargs)
        
        
    def commit(self):
        changes = { k: v for k, v in self.current.items() if v != self.committed.get(k) }
        self.committed = self.current.copy()
        return changes


#class EchoedMediaLeg(MediaLeg):
#    def __init__(self):
#        super(EchoedMediaLeg, self).__init__(type="echo")


#class PlayerMediaLeg(MediaLeg):
#    def __init__(self, format, filename, volume=1, fade=0):
#        super(PlayerMediaLeg, self).__init__(type="player", format=format, filename=filename, volume=volume, fade=fade)


#class ProxiedMediaLeg(MediaLeg):
#    def __init__(self, local_addr):
#        super(ProxiedMediaLeg, self).__init__(type="net", local_addr=local_addr)


class ProxiedMediaChannel(object):
    def __init__(self, mgc, sid):
        self.mgc = mgc
        self.context_sid = sid
        self.legs = { 0: MediaLeg(), 1: MediaLeg() }
        self.is_created = False

        
    def process_mgw_request(self, sid, seq, params, target):
        print("Huh, MGW %s sent a %s message!" % (sid, target))
        
        
    def process_mgw_response(self, sid, seq, params, purpose):
        if params == "ok":
            print("Huh, MGW %s is OK for %s!" % (sid, purpose))
        else:
            print("Oops, MGW %s error for %s!" % (sid, purpose))
        
        
    def refresh_context(self, li, ri):
        print("Refreshing context %s" % (self.context_sid,))
        self.legs[0].update(**li)
        self.legs[1].update(**ri)
        
        # TODO: implement leg deletion
        params = {
            'type': 'proxy',
            'legs': { li: leg.commit() for li, leg in self.legs.items() }
        }
        
        if not self.is_created:
            request_handler = WeakMethod(self.process_mgw_request)
            response_handler = WeakMethod(self.process_mgw_response, "cctx")
            self.mgc.create_context(self.context_sid, params, response_handler=response_handler, request_handler=request_handler)
            self.is_created = True
        else:
            response_handler = WeakMethod(self.process_mgw_response, "mctx")
            self.mgc.modify_context(self.context_sid, params, response_handler=response_handler)
    

    def finish(self):  # TODO: get a callback?
        if self.is_created:
            self.mgc.delete_context(self.context_sid)  # TODO: wait for response!

