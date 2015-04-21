
from async import WeakMethod
from format import Status
from transactions import make_virtual_response
from dialog import Dialog

class Error(Exception): pass

class Leg(object):
    DOWN = "DOWN"
    DIALING_IN = "DIALING_IN"
    DIALING_OUT = "DIALING_OUT"
    DIALING_IN_RINGING = "DIALING_IN_RINGING"
    DIALING_OUT_RINGING = "DIALING_OUT_RINGING"
    DIALING_IN_ANSWERED = "DIALING_IN_ANSWERED"
    UP = "UP"
    DISCONNECTING_OUT = "DISCONNECTING_OUT"

    def __init__(self, report, dialog_manager, local_uri, local_name, remote_uri):
        self.report = report
        self.state = self.DOWN
        self.dialog = Dialog(dialog_manager, WeakMethod(self.recved), local_uri, local_name, remote_uri)
        self.pending_received_message = None
        
        
    def send_request(self, request, related=None):
        self.dialog.send_request(request, related, WeakMethod(self.recved))


    def send_response(self, response):
        if self.pending_received_message and not self.pending_received_message["is_response"]:
            self.dialog.send_response(response, self.pending_received_message)
        else:
            raise Error("Respond to what?")

        
    def do(self, action):
        type = action["type"]
        
        if self.state == self.DOWN:
            if type == "dial":
                self.send_request(dict(method="INVITE", sdp=action.get("sdp")))
                self.state = self.DIALING_OUT
                return
        elif self.state in (self.DIALING_IN, self.DIALING_IN_RINGING):
            if type == "ring":
                if self.state == self.DIALING_IN:
                    self.send_response(dict(status=Status(180, "Ringing")))
                    self.state = self.DIALING_IN_RINGING
                return
            elif type == "answer":
                self.send_response(dict(status=Status(200, "OK")))
                self.state = self.DIALING_IN_ANSWERED
                # Must wait for the ACK
                return
        elif self.state == self.UP:
            if type == "hangup":
                self.send_request(dict(method="BYE"))
                self.state = self.DISCONNECTING_OUT
                return
                
        raise Error("Weirdness!")


    def recved(self, msg):
        is_response = msg["is_response"]
        method = msg["method"]
        status = msg.get("status")
        
        if self.state == self.DOWN:
            if not is_response and method == "INVITE":
                self.pending_received_message = msg
                self.report(dict(type="dial"))
                self.state = self.DIALING_IN
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
                    self.report(dict(type="answer"))
                    self.state = self.UP
                    self.send_request(dict(method="ACK"), msg)
                    return
        elif self.state == self.DIALING_IN_ANSWERED:
            if not is_response and method == "ACK":
                virtual_response = make_virtual_response()
                self.send_response(virtual_response)  # for the INVITE
                self.pending_received_message = msg
                self.send_response(virtual_response)  # for the ACK
                self.pending_received_message = None
                self.state = self.UP
                return
        elif self.state == self.UP:
            if not is_response and method == "BYE":
                self.pending_received_message = msg
                self.report(dict(type="hangup"))
                self.send_response(dict(status=Status(200, "OK")))
                self.state = self.DOWN
                return
        elif self.state == self.DISCONNECTING_OUT:
            if is_response and method == "BYE":
                self.state = self.DOWN
                return
                
        raise Error("Weirdness!")
