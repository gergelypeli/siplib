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
        
        
    def make_media_channel(self, media_legs):
        raise NotImplementedError()


def extract_formats(c):
    return { r.payload_type: (r.encoding, r.clock) for r in c.formats }


class MediaLeg(object):
    def __init__(self, type):
        self.changes = { "type": type }


    def change(self, **kwargs):
        self.changes.update(kwargs)
        
        
    def get_changes(self):
        changes = self.changes
        self.changes = {}
        return changes
        
        
    def get_local_addr(self):
        return None


class EchoedMediaLeg(MediaLeg):
    def __init__(self):
        super(EchoedMediaLeg, self).__init__("echo")
        

class PlayerMediaLeg(MediaLeg):
    def __init__(self, format, filename, volume=1, fade=0):
        super(PlayerMediaLeg, self).__init__("player")

        self.change(format=format, filename=filename, volume=volume, fade=fade)


class ProxiedMediaLeg(MediaLeg):
    def __init__(self, local_addr):
        super(ProxiedMediaLeg, self).__init__("net")
        
        self.local_addr = local_addr
        self.change(local_addr=local_addr)
        
        
    def get_local_addr(self):
        return self.local_addr


class ProxiedMediaChannel(object):
    def __init__(self, mgc, sid, media_legs):
        self.mgc = mgc
        self.context_sid = sid
        self.legs = media_legs
        self.is_created = False
        self.pending_addr = None
        self.pending_formats = None

        
    def process_mgw_request(self, sid, seq, params, target):
        print("Huh, MGW %s sent a %s message!" % (sid, target))
        
        
    def process_mgw_response(self, sid, seq, params, purpose):
        if params == "ok":
            print("Huh, MGW %s is OK for %s!" % (sid, purpose))
        else:
            print("Oops, MGW %s error for %s!" % (sid, purpose))
        
        
    def refresh_context(self):
        # TODO: implement leg deletion
        params = {
            'type': 'proxy',
            'legs': { li: leg.get_changes() for li, leg in self.legs.items() }
        }
        
        if not self.is_created:
            request_handler = WeakMethod(self.process_mgw_request)
            response_handler = WeakMethod(self.process_mgw_response, "cctx")
            self.mgc.create_context(self.context_sid, params, response_handler=response_handler, request_handler=request_handler)
            self.is_created = True
        else:
            response_handler = WeakMethod(self.process_mgw_response, "mctx")
            self.mgc.modify_context(self.context_sid, params, response_handler=response_handler)
    

    def process_offer(self, li, oc):
        self.pending_addr = oc.addr
        self.pending_formats = extract_formats(oc)


    def process_answer(self, li, ac):
        lj = 1 - li
        
        offering_leg = self.legs[lj]
        answering_leg = self.legs[li]

        answer_addr = ac.addr
        answer_formats = extract_formats(ac)
        
        offer_addr = self.pending_addr
        offer_formats = self.pending_formats

        self.pending_addr = None
        self.pending_formats = None
        
        answering_leg.change(
            remote_addr=answer_addr,
            send_formats=answer_formats,
            recv_formats=offer_formats
        )
        offering_leg.change(
            remote_addr=offer_addr,
            send_formats=offer_formats,
            recv_formats=answer_formats
        )

        self.refresh_context()


    def finish(self):  # TODO: get a callback?
        if self.is_created:
            self.mgc.delete_context(self.context_sid)  # TODO: wait for response!

