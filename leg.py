
from async import WeakMethod
from format import Status
from transactions import make_virtual_response
from dialog import Dialog

class Error(Exception): pass

class Leg(object):
    def __init__(self):
        self.report = None
        self.ctx = {}
    
    
    def set_report(self, report):
        self.report = report
        
    
    def do(self, action):
        raise NotImplementedError()


class InviteState(object):
    PROVISIONAL_SESSION_NONE = "PROVISIONAL_SESSION_NONE"
    PROVISIONAL_SESSION_UNRELIABLE = "PROVISIONAL_SESSION_UNRELIABLE"
    PROVISIONAL_SESSION_RELIABLE = "PROVISIONAL_SESSION_RELIABLE"

    def __init__(self, request):
        self.request = request
        self.provisional_session = self.PROVISIONAL_SESSION_NONE


class SipLeg(Leg):
    DOWN = "DOWN"
    DIALING_IN = "DIALING_IN"
    DIALING_OUT = "DIALING_OUT"
    DIALING_IN_RINGING = "DIALING_IN_RINGING"
    DIALING_OUT_RINGING = "DIALING_OUT_RINGING"
    DIALING_IN_ANSWERED = "DIALING_IN_ANSWERED"
    UP = "UP"
    DISCONNECTING_OUT = "DISCONNECTING_OUT"
    

    def __init__(self, dialog):
        super(SipLeg, self).__init__()
        self.state = self.DOWN
        self.dialog = dialog
        self.invite_state = None
        #self.received_invite = None  # TODO: rethink these!
        #self.sent_invite = None
        #self.provisional_session = self.PROVISIONAL_SESSION_NONE
        
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
        sdp = action.get("sdp")
        
        if self.state == self.DOWN:
            if type == "dial":
                self.ctx.update(action["ctx"])
                self.dialog.setup_outgoing(
                    self.ctx["from"].uri, self.ctx["from"].name, self.ctx["to"].uri,
                    self.ctx.get("route"), self.ctx.get("hop")
                )
                
                invite = dict(method="INVITE", sdp=sdp)
                self.send_request(invite)  # Will be extended!
                self.invite_state = InviteState(invite)
                self.state = self.DIALING_OUT
                return
        elif self.state in (self.DIALING_OUT, self.DIALING_OUT_RINGING):
            if type == "hangup":
                self.send_request(dict(method="CANCEL"), self.invite_state.request)
                self.state = self.DISCONNECTING_OUT
                return
        elif self.state in (self.DIALING_IN, self.DIALING_IN_RINGING):
            if type == "ring":
                if self.state == self.DIALING_IN:
                    self.send_response(dict(status=Status(180, "Ringing")), self.invite_state.request)
                    self.state = self.DIALING_IN_RINGING
                return
            elif type == "answer":
                self.send_response(dict(status=Status(200, "OK"), sdp=sdp), self.invite_state.request)
                self.state = self.DIALING_IN_ANSWERED  # TODO: into InviteState?
                # Must wait for the ACK
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
                
                self.invite_state = InviteState(msg)
                self.report(dict(type="dial", ctx=self.ctx, sdp=sdp))
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
                if status.code == 180:
                    if self.state == self.DIALING_OUT:
                        self.report(dict(type="ring"))
                        self.state = self.DIALING_OUT_RINGING
                    return
                elif status.code >= 300:
                    self.report(dict(type="reject", status=status))
                    self.state = self.DOWN
                    # ACKed by tr
                    return
                elif status.code >= 200:
                    self.report(dict(type="answer", sdp=sdp))
                    self.state = self.UP
                    self.send_request(dict(method="ACK"), msg)
                    return
        elif self.state == self.DIALING_IN_ANSWERED:
            if not is_response and method == "ACK":
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
