
from async import WeakMethod
from format import Status
from transactions import make_virtual_response
from dialog import Dialog

class Error(Exception): pass

class Leg(object):
    def __init__(self):
        self.report = None
        self.ctx = {}
    
    
    def make_media_leg(self, sdp_channel):
        return None  # The Call may decide what to use
        
    
    def set_report(self, report):
        self.report = report
        
    
    def do(self, action):
        raise NotImplementedError()


class Session(object):  # TODO: this is a reimplementation from call.py!
    def __init__(self):
        self.local_sdp = None
        self.remote_sdp = None
        self.pending_local_sdp = None
        self.pending_remote_sdp = None
        
        
    def set_local_offer(self, sdp):
        if sdp is None:
            raise Error("No SDP specified!")
        elif self.pending_local_sdp:
            raise Error("Outgoing offer already pending!")
        elif self.pending_remote_sdp:
            raise Error("Incoming offer also pending!")
        else:
            self.pending_local_sdp = sdp


    def set_remote_offer(self, sdp):
        if sdp is None:
            raise Error("No SDP specified!")
        elif self.pending_local_sdp:
            raise Error("Outgoing offer also pending!")
        elif self.pending_remote_sdp:
            raise Error("Incoming offer already pending!")
        else:
            self.pending_remote_sdp = sdp
            
            
    def set_local_answer(self, sdp):
        if sdp is None:
            raise Error("No SDP specified!")
        elif not self.pending_remote_sdp:
            raise Error("Incoming offer not pending!")
        else:
            # empty is rejection
            if not sdp.is_empty():
                self.remote_sdp = self.pending_remote_sdp
                self.local_sdp = sdp
                
            self.pending_remote_sdp = None


    def set_remote_answer(self, sdp):
        if sdp is None:
            raise Error("No SDP specified!")
        elif not self.pending_local_sdp:
            raise Error("Outgoing offer not pending!")
        else:
            # empty is rejection
            if not sdp.is_empty():
                self.local_sdp = self.pending_local_sdp
                self.remote_sdp = sdp
                
            self.pending_local_sdp = None


    def get_local_offer(self):
        if self.pending_local_sdp:
            return self.pending_local_sdp
        else:
            raise Error("Outgoing offer not pending!")
        
        
    def get_remote_offer(self):
        if self.pending_remote_sdp:
            return self.pending_remote_sdp
        else:
            raise Error("Incoming offer not pending!")
            
            
    def get_local_answer(self):
        if self.pending_local_sdp:
            raise Error("Outgoing offer is pending!")
        elif self.pending_remote_sdp:
            raise Error("Incoming offer still pending!")
        elif not self.local_sdp:
            raise Error("No outgoing answer yet!")
        else:
            return self.local_sdp


    def get_remote_answer(self):
        if self.pending_local_sdp:
            raise Error("Outgoing offer still pending!")
        elif self.pending_remote_sdp:
            raise Error("Incoming offer is pending!")
        elif not self.remote_sdp:
            raise Error("No incoming answer yet!")
        else:
            return self.remote_sdp


class InviteState(object):
    def __init__(self, request):
        self.request = request
        sdp = request.get("sdp")
        self.has_offer_in_request = (sdp and sdp.is_session())
        self.responded_session = None
        self.rpr_session_done = False
        self.final_response = None


class SipLeg(Leg):
    DOWN = "DOWN"
    DIALING_IN = "DIALING_IN"
    DIALING_OUT = "DIALING_OUT"
    DIALING_IN_RINGING = "DIALING_IN_RINGING"
    DIALING_OUT_RINGING = "DIALING_OUT_RINGING"
    DIALING_IN_ANSWERED = "DIALING_IN_ANSWERED"
    DIALING_OUT_ANSWERED = "DIALING_OUT_ANSWERED"
    UP = "UP"
    DISCONNECTING_OUT = "DISCONNECTING_OUT"
    

    def __init__(self, dialog):
        super(SipLeg, self).__init__()
        self.state = self.DOWN
        self.dialog = dialog
        self.invite_state = None
        self.session = Session()
        
        self.dialog.set_report(WeakMethod(self.process))


    def send_request(self, request, related=None):
        self.dialog.send_request(request, related)


    def send_response(self, response, related):
        if related and not related["is_response"]:
            self.dialog.send_response(response, related)
        else:
            raise Error("Respond to what?")


    def do(self, action):
        type = action["type"]
        offer = action.get("offer")
        answer = action.get("answer")
        
        if offer and answer:
            raise Error("WTF?")
        elif offer:
            self.session.set_local_offer(offer)
        elif answer:
            self.session.set_local_answer(answer)
        
        if self.state == self.DOWN:
            if type == "dial":
                self.ctx.update(action["ctx"])
                self.dialog.setup_outgoing(
                    self.ctx["from"].uri, self.ctx["from"].name, self.ctx["to"].uri,
                    self.ctx.get("route"), self.ctx.get("hop")
                )
                
                invite_request = dict(method="INVITE", sdp=offer)
                self.send_request(invite_request)  # Will be extended!
                self.invite_state = InviteState(invite_request)
                self.state = self.DIALING_OUT
                return
        elif self.state in (self.DIALING_OUT, self.DIALING_OUT_RINGING):
            if type == "hangup":
                self.send_request(dict(method="CANCEL"), self.invite_state.request)
                self.state = self.DISCONNECTING_OUT
                return
        elif self.state in (self.DIALING_OUT_ANSWERED,):  # TODO: into InviteState?
            if type == "session":
                # send ACK with SDP
                if not answer:
                    raise Error("Answer expected in ACK!")
                    
                self.send_request(dict(method="ACK", sdp=answer), self.invite_state.final_response)
                self.state = self.UP
                return
        elif self.state in (self.DIALING_IN, self.DIALING_IN_RINGING):
            if self.invite_state.responded_session:
                sdp = self.invite_state.responded_session
            else:
                sdp = answer if self.invite_state.has_offer_in_request else offer
                if sdp:
                    self.invite_state.responded_session = sdp
                
            if type == "ring":
                invite_response = dict(status=Status(180, "Ringing"), sdp=sdp)
                self.send_response(invite_response, self.invite_state.request)
                self.state = self.DIALING_IN_RINGING
                return
            elif type == "session":
                status = Status(180, "Ringing") if self.state == self.DIALING_IN_RINGING else Status(183, "Session Progress")
                invite_response = dict(status=status, sdp=sdp)
                self.send_response(invite_response, self.invite_state.request)
                return
            elif type == "accept":
                invite_response = dict(status=Status(200, "OK"), sdp=sdp)
                self.send_response(invite_response, self.invite_state.request)
                self.state = self.DIALING_IN_ANSWERED  # TODO: into InviteState?
                # TODO: Do we need to block requests here, or can we already send new ones?
                # Just wait for the ACK for now.
                return
        elif self.state == self.UP:
            if type == "hangup":
                self.send_request(dict(method="BYE"))
                self.state = self.DISCONNECTING_OUT
                return
                
        raise Error("Weirdness!")


    def process(self, msg):
        is_response = msg["is_response"]
        method = msg["method"]
        status = msg.get("status")
        sdp = msg.get("sdp")
        
        if self.state == self.DOWN:
            if not is_response and method == "INVITE":
                self.ctx.update({
                    "from": msg["from"],
                    "to": msg["to"]
                })
                
                offer = sdp if sdp and sdp.is_session() else None
                if offer:
                    self.session.set_remote_offer(offer)  # TODO: session query?
                    
                self.invite_state = InviteState(msg)
                self.report(dict(type="dial", ctx=self.ctx, offer=offer))
                self.state = self.DIALING_IN
                return
        elif self.state in (self.DIALING_IN, self.DIALING_IN_RINGING):
            if not is_response and method == "CANCEL":
                self.report(dict(type="hangup"))
                self.send_response(dict(status=Status(487, "Request Terminated")), self.invite_state.request)
                self.send_response(dict(status=Status(200, "OK")), msg)
                self.invite_state = None
                self.state = self.DOWN
                return
        elif self.state in (self.DIALING_OUT, self.DIALING_OUT_RINGING):
            if is_response and method == "INVITE":
                offer, answer = None, None
                
                if sdp and sdp.is_session() and not self.invite_state.responded_session:
                    if self.invite_state.has_offer_in_request:
                        self.session.set_remote_answer(sdp)
                        answer = sdp
                    else:
                        self.session.set_remote_offer(sdp)
                        offer = sdp

                    self.invite_state.responded_session = sdp  # just to ignore any further
                    
                if status.code == 180:
                    if self.state == self.DIALING_OUT:
                        self.report(dict(type="ring", offer=offer, answer=answer))
                    elif offer or answer:
                        self.report(dict(type="session", offer=offer, answer=answer))
                    self.state = self.DIALING_OUT_RINGING
                    return
                elif status.code == 183:
                    if offer or answer:
                        self.report(dict(type="session", offer=offer, answer=answer))
                    return
                elif status.code >= 300:
                    self.report(dict(type="reject", status=status))
                    self.state = self.DOWN
                    # ACKed by tr
                    return
                elif status.code >= 200:
                    if self.invite_state.has_offer_in_request:
                        # send ACK without SDP now
                        self.send_request(dict(method="ACK"), msg)
                        self.state = self.UP
                    else:
                        # wait for outgoing session for the ACK
                        # beware, the report() below may send it!
                        self.invite_state.final_response = msg
                        self.state = self.DIALING_OUT_ANSWERED
                        
                    self.report(dict(type="accept", offer=offer, answer=answer))
                    return
        elif self.state == self.DIALING_IN_ANSWERED:
            if not is_response and method == "ACK":
                answer = None
                
                if self.invite_state.has_offer_in_request:
                    if sdp and sdp.is_session():
                        print("Unexpected session in ACK!")
                else:
                    if not (sdp and sdp.is_session()):
                        print("Unexpected sessionless ACK!")
                    else:
                        self.session.set_remote_answer(sdp)
                        answer = sdp
                        self.report(dict(type="session", answer=answer))
                    
                # Stop the retransmission of the final answer
                self.send_response(make_virtual_response(), self.invite_state.request)
                # Let the ACK server transaction expire
                self.send_response(make_virtual_response(), msg)
                self.invite_state = None
                self.state = self.UP
                return
            elif not is_response and method == "NAK":  # virtual request, no ACK received
                self.send_request(dict(method="BYE"))  # required behavior
                self.state = self.DISCONNECTING_OUT
                return
        elif self.state == self.UP:
            if not is_response and method == "BYE":
                self.report(dict(type="hangup"))
                self.send_response(dict(status=Status(200, "OK")), msg)
                self.state = self.DOWN
                return
        elif self.state == self.DISCONNECTING_OUT:
            if is_response and method == "BYE":
                self.state = self.DOWN
                return
            elif is_response and method == "INVITE":
                print("Got cancelled invite response: %s" % (status,))
                # This was ACKed by the transaction layer
                self.state = self.DOWN
                return
            elif is_response and method == "CANCEL":
                print("Got cancel response: %s" % (status,))
                return
                
        raise Error("Weirdness!")


def create_uninvited_leg(dialog_manager, invite_params):
    # TODO: real UninvitedLeg class
    leg = Leg(dialog_manager, None, None, None)
    leg.dialog.send_request(dict(method="UNINVITE"), invite_params, leg.process)  # strong ref!
    leg.state = leg.DIALING_OUT
