from format import Status
from party_sip_invite import InviteClientState, InviteServerState
from util import Loggable
from sdp import add_sdp, get_sdp
import zap


class InviteHelper(Loggable):
    def __init__(self, use_rpr):
        Loggable.__init__(self)
    
        self.use_rpr = use_rpr
        
        self.state = None
        self.is_clogged = False
        
        self.request_slot = zap.EventSlot()    # these must be sync
        self.response_slot = zap.EventSlot()
        self.unclogged_slot = zap.EventSlot()  # this can be async
        
        
    def new(self, is_outgoing):
        if self.state:
            raise Exception("Invite already in progress!")
            
        # Must use instaplugs, because the ordering of the messages mustn't change,
        # and not INVITE related messages are processed synchronously.
            
        if is_outgoing:
            self.state = InviteClientState(self.use_rpr)
            self.state.message_slot.instaplug(self.request_slot.zap)
            self.state.set_oid(self.oid.add("client"))
        else:
            self.state = InviteServerState(self.use_rpr)
            self.state.message_slot.instaplug(self.response_slot.zap)
            self.state.set_oid(self.oid.add("server"))
        
        
    def outgoing(self, msg, sdp, is_answer):
        if not self.state:
            raise Exception("Invite not in progress!")

        if self.is_clogged:
            raise Exception("Mustn't try sending while invite is clogged!")

        self.state.process_outgoing(msg, sdp, is_answer)
        
        if self.state.is_finished():
            self.state = None
        elif self.state.is_clogged():
            self.logger.debug("Invite is clogged, may postpone future actions.")
            self.is_clogged = True


    def incoming(self, msg):
        if not self.state:
            raise Exception("Invite not in progress!")
            
        msg, sdp, is_answer = self.state.process_incoming(msg)
        
        if self.state.is_finished():
            self.state = None
        elif self.is_clogged and not self.state.is_clogged():
            self.is_clogged = False
            
            # An InviteClient only gets clogged after unreliable offers, with
            # RPR-s it won't happen. An InviteServer only gets clogged after
            # sending an RPR. When it gets unclogged, it received a PRACK, and
            # if there's an offer in it, that's the fucked up PRACK offer case.
            # Thanks to them being unrejectable, we may need to reject pending
            # outgoing offers before we send the received one up.
            reject_pending_offers = self.use_rpr and sdp and not is_answer

            self.unclogged_slot.zap(reject_pending_offers)
            
        return msg, sdp, is_answer


    def is_active(self):
        return self.state is not None
        
        
    def is_session_established(self):
        return self.state.is_session_established()
        

    def is_session_pracking(self):
        return self.state.is_session_pracking()


class UpdateHelper(Loggable):
    def __init__(self):
        Loggable.__init__(self)

        self.state = None
        self.request_slot = zap.EventSlot()    # these must be sync
        self.response_slot = zap.EventSlot()
        
                
    def new(self, is_outgoing):
        if self.state:
            raise Exception("Update already in progress!")
            
        self.state = dict(is_outgoing=is_outgoing, request=None)
        
        
    def outgoing(self, msg, sdp, is_answer):
        if not self.state:
            raise Exception("Update not in progress!")
            
        if self.state["is_outgoing"]:
            if self.state["request"]:
                raise Exception("Update was already sent!")
                
            assert not is_answer
            add_sdp(msg, sdp)

            self.logger.info("Sending message: UPDATE with offer")
            self.request_slot.zap(msg, None)
            self.state["request"] = msg
        else:
            if not self.state["request"]:
                raise Exception("Update was not yet received!")
            
            assert is_answer
            if sdp:
                add_sdp(msg, sdp)
                self.logger.info("Sending message: UPDATE response with answer")
            else:
                self.logger.info("Sending message: UPDATE response with rejection")
                
            self.response_slot.zap(msg, self.state["request"])
            self.state = None
            
            
    def incoming(self, msg):
        if not self.state:
            raise Exception("Update not in progress!")
            
        if self.state["is_outgoing"]:
            if not self.state["request"]:
                raise Exception("Update request was not sent, but now receiving one!")
                
            if not msg["is_response"]:
                self.logger.warning("Rejecting incoming UPDATE because one was already sent!")
                res = dict(status=Status(419))
                self.response_slot.zap(res, msg)
                return None, None, None
        
            self.state = None
        
            if msg["status"].code == 200:
                self.logger.info("Processing message: UPDATE response with answer")
                sdp = get_sdp(msg)
                return msg, sdp, True
            else:
                self.logger.info("Processing message: UPDATE response with rejection")
                return msg, None, True
        else:
            if self.state["request"]:
                self.logger.warning("Update request was already received!")
                res = dict(status=Status(419))
                self.response_slot.zap(res, msg)
                return None, None, None
                
            if msg["is_response"]:
                self.logger.warning("Got an update response without sending a request!")
                return None, None, None
                
            self.state["request"] = msg
                
            self.logger.info("Processing message: UPDATE request with offer")
            sdp = get_sdp(msg)
            return msg, sdp, False


    def outgoing_auto(self, sdp, is_answer):
        if not is_answer:
            self.new(is_outgoing=True)
            self.outgoing(dict(method="UPDATE"), sdp, is_answer)
        else:
            code = 200 if sdp else 488
            self.outgoing(dict(status=Status(code)), sdp, is_answer)


    def incoming_auto(self, msg):
        if not msg["is_response"]:
            self.new(is_outgoing=False)
            msg, sdp, is_answer = self.incoming(msg)
        else:
            msg, sdp, is_answer = self.incoming(msg)
            
        return msg, sdp, is_answer
