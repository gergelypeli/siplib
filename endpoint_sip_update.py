from log import Loggable
from format import Status, Sip
from sdp import add_sdp, get_sdp
import zap


class UpdateState(Loggable):
    def __init__(self):
        Loggable.__init__(self)
        
        self.request = None
        self.message_slot = zap.EventSlot()
        
        
    def is_finished(self):
        return self.request is None
        
        
class UpdateClientState(UpdateState):
    def process_outgoing(self, msg, sdp, is_answer):
        if self.request:
            raise Exception("Update was already sent!")
            
        assert not is_answer
        add_sdp(msg, sdp)

        self.logger.info("Sending message: UPDATE with offer")
        self.message_slot.zap(msg, None)
        self.request = msg


    def process_incoming(self, msg):
        if not self.request:
            raise Exception("Update request was not sent, but now receiving one!")
            
        if not msg.is_response:
            self.logger.warning("Rejecting incoming UPDATE because one was already sent!")
            res = Sip.response(status=Status(419))
            self.message_slot.zap(res, msg)
            return None, None, None
    
        self.request = None
        
        if msg.status.code == 200:
            self.logger.info("Processing message: UPDATE response with accept")
            sdp = get_sdp(msg)
            return msg, sdp, True
        else:
            self.logger.info("Processing message: UPDATE response with reject")
            return msg, None, True
        

class UpdateServerState(UpdateState):        
    def process_outgoing(self, msg, sdp, is_answer):
        if not self.request:
            raise Exception("Update was not yet received!")
        
        assert is_answer
        if sdp:
            add_sdp(msg, sdp)
            self.logger.info("Sending message: UPDATE response with accept")
        else:
            self.logger.info("Sending message: UPDATE response with reject")
            
        self.message_slot.zap(msg, self.request)
        self.request = None
            
            
    def process_incoming(self, msg):
        if self.request:
            self.logger.warning("Update request was already received!")
            res = Sip.response(status=Status(419))
            self.message_slot.zap(res, msg)
            return None, None, None
            
        if msg.is_response:
            self.logger.warning("Got an update response without sending a request!")
            return None, None, None
            
        self.request = msg
            
        self.logger.info("Processing message: UPDATE request with offer")
        sdp = get_sdp(msg)
        return msg, sdp, False
