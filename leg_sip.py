from copy import deepcopy

from async import WeakMethod
from format import Status, make_virtual_response
from util import build_oid
from leg import Leg, SessionState, Error
from sdp import Session


class InviteState(object):
    def __init__(self, request, is_outgoing):
        self.request = request
        self.is_outgoing = is_outgoing
        
        #self.had_offer_in_request = bool(request.get("sdp"))  # FIXME: unnecessary!
        
        self.responded_sdp = None
        self.rpr_session_done = False
        self.final_response = None


class SipLeg(Leg):
    DOWN = "DOWN"
    DIALING_IN = "DIALING_IN"
    DIALING_OUT = "DIALING_OUT"
    DIALING_IN_RINGING = "DIALING_IN_RINGING"
    DIALING_OUT_RINGING = "DIALING_OUT_RINGING"
    UP = "UP"
    DISCONNECTING_OUT = "DISCONNECTING_OUT"
    

    def __init__(self, dialog):
        super().__init__()

        self.dialog = dialog
        self.state = self.DOWN
        self.invite = None
        self.session = SessionState()
        
        self.dialog.set_report(WeakMethod(self.process))


    def set_oid(self, oid):
        Leg.set_oid(self, oid)
        self.dialog.set_oid(build_oid(oid, "dialog"))


    def change_state(self, new_state):
        self.logger.debug("Changing state %s => %s" % (self.state, new_state))
        self.state = new_state
        

    def send_request(self, request, related=None):
        self.dialog.send_request(request, related)


    def send_response(self, response, related):
        if related and not related["is_response"]:
            self.dialog.send_response(response, related)
        else:
            raise Error("Respond to what?")


    def refresh_media(self):
        def extract_formats(c):
            return { r.payload_type: (r.encoding, r.clock) for r in c.formats }

        # Negotiated session parameters, must have the same length
        lsdp = self.session.local_sdp
        rsdp = self.session.remote_sdp
        channel_count = min(len(lsdp.channels), len(rsdp.channels)) if lsdp and rsdp else 0

        for i in range(channel_count):
            lc = lsdp.channels[i]
            rc = rsdp.channels[i]
            ml = self.media_legs[i]  # must also have the same number of media legs
            #print("XXX local: %s (%s), remote: %s (%s)" % (lc.addr, id(lc.addr), rc.addr, id(rc.addr)))
            
            ml.update(
                remote_addr=rc.addr,
                send_formats=extract_formats(rc),
                recv_formats=extract_formats(lc)
            )
            
        Leg.refresh_media(self)
            

    def preprocess_outgoing_sdp(self, sdp):
        for i in range(len(self.media_legs), len(sdp.channels)):
            self.make_media_leg(i, "net", report=WeakMethod(self.notified))
        
        for i in range(len(sdp.channels)):
            # No need to make a copy again here
            if sdp.channels[i].addr:
                raise Exception("Outgoing session has channel address set! %s" % id(sdp.channels[i]))
                
            sdp.channels[i].addr = self.media_legs[i].local_addr
            
        return sdp  # For the sake of consistency


    def postprocess_incoming_sdp(self, sdp):
        sdp = deepcopy(sdp)

        for i in range(len(sdp.channels)):
            sdp.channels[i].addr = None  # Just to be sure
            
        return sdp

        
    def process_incoming_offer(self, sdp):
        self.session.set_remote_offer(sdp)
        sdp = self.postprocess_incoming_sdp(sdp)
        return sdp

    
    def process_incoming_answer(self, sdp):
        self.session.set_remote_answer(sdp)
        self.refresh_media()
        sdp = self.postprocess_incoming_sdp(sdp)
        return sdp

    
    def process_outgoing_offer(self, sdp):
        sdp = self.preprocess_outgoing_sdp(sdp)
        self.session.set_local_offer(sdp)
        return sdp

    
    def process_outgoing_answer(self, sdp):
        sdp = self.preprocess_outgoing_sdp(sdp)
        self.session.set_local_answer(sdp)
        self.refresh_media()
        return sdp


    def invite_outgoing_request(self, msg, session):
        if self.invite:
            raise Exception("Invalid outgoing INVITE request!")
        
        if not session:
            raise Exception("No session for INVITE request!")
        elif session.is_answer:
            raise Exception("Session answer for INVITE request!")
        elif session.sdp:
            msg["sdp"] = self.process_outgoing_offer(session.sdp)
            
        self.invite = InviteState(msg, True)
        self.send_request(msg)  # Will be extended!


    def invite_incoming_response(self, msg):
        # TODO: error handling!
        if not self.invite or not self.invite.is_outgoing:
            raise Exception("Unexpected incoming INVITE response!")
        
        status = msg.get("status")
        sdp = msg.get("sdp")
        session = None
        
        if sdp and not self.invite.responded_sdp:
            # No session yet in INVITE responses, take this one
            
            if self.invite.request.get("sdp"):
                session = Session(is_answer=True, sdp=self.process_incoming_answer(sdp))
            else:
                session = Session(is_answer=False, sdp=self.process_incoming_offer(sdp))

            self.invite.responded_sdp = session.sdp  # just to ignore any further

        if status.code >= 300:
            # Transactions sent the ACK
            self.invite = None
            return Session(is_answer=True, sdp=None)
        elif status.code >= 200:
            self.invite.final_response = msg
                
        return session


    def invite_outgoing_ack(self, session):
        if not self.invite or not self.invite.is_outgoing or not self.invite.final_response:
            raise Exception("Invalid outgoing ACK request!")
            
        if self.invite.request.get("sdp"):
            if session:
                raise Error("Unnecessary session for ACK!")
            else:
                sdp = None
        else:
            # send ACK with SDP
            if not session:
                # This function is only called for the no-SDP-in-request case
                raise Error("Missing session for ACK!")
            elif not session.is_answer:
                raise Error("Answer expected for ACK!")
            else:
                sdp = self.process_outgoing_answer(session.sdp)
        
        self.send_request(dict(method="ACK", sdp=sdp), self.invite.final_response)
        self.invite = None


    def invite_incoming_request(self, msg):
        if self.invite:
            raise Exception("Unexpected incoming INVITE request!")

        sdp = msg.get("sdp")
        session = None
        
        if sdp:
            session = Session(is_answer=False, sdp=self.process_incoming_offer(sdp))
        else:
            session = Session(is_answer=False, sdp=None)
            
        self.invite = InviteState(msg, False)
        
        return session


    def invite_outgoing_response_session(self, session):
        if not self.invite or self.invite.is_outgoing:
            raise Exception("Invalid outgoing INVITE response session!")
    
        if not session:
            return False
    
        if self.invite.responded_sdp:
            if session:
                raise Exception("Ignoring outgoing session, because one already sent!")
                
            return False
        elif self.invite.request.get("sdp"):
            if not session.is_answer:
                raise Exception("Ignoring outgoing session, because answer is expected!")
            
            self.invite.responded_sdp = self.process_outgoing_answer(session.sdp)
            return True
        else:
            if session.is_answer or not session.sdp:
                raise Exception("Ignoring outgoing session, because offer is expected!")
            
            self.invite.responded_sdp = self.process_outgoing_offer(session.sdp)
            return True


    def invite_outgoing_response(self, msg):
        if not self.invite or self.invite.is_outgoing:
            raise Exception("Invalid outgoing INVITE response!")
        
        if self.invite.responded_sdp:
            msg["sdp"] = self.invite.responded_sdp
            
        self.invite.final_response = msg  # Note: this is an incomplete message, but OK here
        self.send_response(self.invite.final_response, self.invite.request)
        
        if msg["status"].code >= 300:
            self.invite = None
    
    
    def invite_incoming_ack(self, msg):
        if not self.invite or self.invite.is_outgoing or not self.invite.final_response:
            raise Exception("Unexpected incoming ACK request!")

        sdp = msg.get("sdp")
        session = None
        
        if self.invite.request.get("sdp"):
            if sdp:
                self.logger.debug("Unexpected session in ACK!")
        else:
            if not sdp:
                self.logger.debug("Unexpected sessionless ACK!")
            else:
                session = Session(is_answer=True, sdp=self.process_incoming_answer(sdp))
            
        # Stop the retransmission of the final answer
        self.send_response(make_virtual_response(), self.invite.request)
        # Let the ACK server transaction expire
        self.send_response(make_virtual_response(), msg)
        
        self.invite = None
        return session


    def invite_incoming_nak(self):
        if not self.invite or self.invite.is_outgoing or not self.invite.final_response:
            raise Exception("Unexpected incoming NAK request!")
                
        self.invite = None
    

    def do(self, action):
        self.logger.debug("Doing %s" % action)
        
        type = action["type"]
        session = action.get("session")
        
        if self.state == self.DOWN:
            if type == "dial":
                self.ctx.update(action["ctx"])
                
                # TODO: uri and hop should be set in the constructor, route is
                # empty, others may come from the ctx (currently from and to only).
                
                self.dialog.setup_outgoing(
                    self.ctx["uri"],
                    self.ctx["from"], self.ctx["to"],
                    self.ctx.get("route"), self.ctx.get("hop")
                )
                
                self.invite_outgoing_request(dict(method="INVITE"), session)
                self.change_state(self.DIALING_OUT)
                
                self.anchor()
                return

        elif self.state in (self.DIALING_OUT, self.DIALING_OUT_RINGING):
            if type == "hangup":
                self.send_request(dict(method="CANCEL"), self.invite.request)
                self.change_state(self.DISCONNECTING_OUT)
                return
                
            elif type == "session":
                self.invite_outgoing_ack(session)
                self.change_state(self.UP)
                return

        elif self.state in (self.DIALING_IN, self.DIALING_IN_RINGING):
            session_changed = self.invite_outgoing_response_session(session)

            if type == "session":
                if not session_changed:
                    self.logger.debug("Session unchanged, skipping 180/183.")
                    return
                    
                already_ringing = (self.state == self.DIALING_IN_RINGING)
                status = Status(180, "Ringing") if already_ringing else Status(183, "Session Progress")
                self.invite_outgoing_response(dict(status=status))
                return
                
            elif type == "ring":
                already_ringing = (self.state == self.DIALING_IN_RINGING)
                
                if not session_changed and already_ringing:
                    self.logger.debug("Already ringing and session unchanged, skipping 180.")
                    return
                
                self.invite_outgoing_response(dict(status=Status(180, "Ringing")))
                
                if not already_ringing:
                    self.change_state(self.DIALING_IN_RINGING)
                    
                return
                    
            elif type == "accept":
                self.invite_outgoing_response(dict(status=Status(200, "OK")))
                # Wait for the ACK before changing state
                return

            elif type == "reject":
                self.invite_outgoing_response(dict(status=action["status"]))
                # The transactions will catch the ACK
                self.change_state(self.DOWN)
                self.finish_media()
                return

        elif self.state == self.UP:
            # Re-INVITE stuff
            
            if type == "session":
                if not self.invite:
                    self.invite_outgoing_request(dict(method="INVITE"), session)
                elif not self.invite.final_response:
                    # TODO: handle rejection!
                    self.invite_outgoing_response_session(session)
                    self.invite_outgoing_response(dict(status=Status(200)))
                else:
                    self.invite_outgoing_ack(session)
                        
                return
        
            elif type == "tone":
                if self.media_legs and action.get("name"):
                    self.media_legs[0].notify("tone", dict(name=action["name"]))
                    
                return

            elif type == "hangup":
                self.send_request(dict(method="BYE"))
                self.change_state(self.DISCONNECTING_OUT)
                return
            
        raise Error("Weird thing to do %s in state %s!" % (type, self.state))


    def process(self, msg):
        is_response = msg["is_response"]
        method = msg["method"]
        status = msg.get("status")
        #sdp = msg.get("sdp")
        
        # Note: must change state before report, because that can generate
        # a reverse action which should only be processed with the new state!
        
        if self.state == self.DOWN:
            if not is_response and method == "INVITE":
                self.ctx.update({
                    "uri": msg["uri"],
                    "from": msg["from"],
                    "to": msg["to"]
                })
                
                session = self.invite_incoming_request(msg)
                
                self.change_state(self.DIALING_IN)
                self.report(dict(type="dial", ctx=self.ctx, session=session))
                return
                
        elif self.state in (self.DIALING_IN, self.DIALING_IN_RINGING):
            if not is_response and method == "CANCEL":
                self.send_response(dict(status=Status(200, "OK")), msg)
                self.send_response(dict(status=Status(487, "Request Terminated")), self.invite.request)
                
                self.change_state(self.DOWN)
                self.report(dict(type="hangup"))
                self.finish_media()
                return
                
            elif not is_response and method == "ACK":
                session = self.invite_incoming_ack(msg)
                
                self.change_state(self.UP)
                if session:
                    self.report(dict(type="session", session=session))
                return
                
            elif not is_response and method == "NAK":  # virtual request, no ACK received
                self.invite_incoming_nak()

                self.send_request(dict(method="BYE"))  # required behavior
                self.change_state(self.DISCONNECTING_OUT)
                return
                
        elif self.state in (self.DIALING_OUT, self.DIALING_OUT_RINGING):
            if is_response and method == "INVITE":
                session = self.invite_incoming_response(msg)
                    
                if status.code == 180:
                    already_ringing = (self.state == self.DIALING_OUT_RINGING)
                    self.change_state(self.DIALING_OUT_RINGING)
                    
                    if not already_ringing:
                        self.report(dict(type="ring", session=session))
                    elif session:
                        self.report(dict(type="session", session=session))
                    return
                    
                elif status.code == 183:
                    if session:
                        self.report(dict(type="session", session=session))
                    return
                    
                elif status.code >= 300:
                    self.change_state(self.DOWN)
                    self.report(dict(type="reject", status=status))
                    self.finish_media()
                    return
                    
                elif status.code >= 200:
                    if self.invite.request.get("sdp"):
                        self.invite_outgoing_ack(None)
                        self.change_state(self.UP)
                        
                    self.report(dict(type="accept", session=session))
                    return
                    
        elif self.state == self.UP:
            # Re-INVITE stuff
            
            if not is_response and method == "INVITE":
                session = self.invite_incoming_request(msg)
                if session:
                    self.report(dict(type="session", session=session))
                return
                
            elif not is_response and method == "ACK":
                session = self.invite_incoming_ack(msg)
                if session:
                    self.report(dict(type="session", session=session))
                return
                
            elif not is_response and method == "NAK":  # virtual request, no ACK received
                self.invite_incoming_nak()
                # TODO: now what?
                return
                
            elif is_response and method == "INVITE":
                # FIXME: check at least the status code!
                session = self.invite_incoming_response(msg)
                
                if self.invite.request.get("sdp"):
                    self.invite_outgoing_ack(None)
                    
                if session:
                    self.report(dict(type="session", session=session))
                    
                return

            elif not is_response and method == "BYE":
                self.send_response(dict(status=Status(200, "OK")), msg)
                self.change_state(self.DOWN)
                self.report(dict(type="hangup"))
                self.finish_media()
                return
                
        elif self.state == self.DISCONNECTING_OUT:
            if is_response and method == "BYE":
                self.change_state(self.DOWN)
                self.finish_media()
                return
                
            elif is_response and method == "INVITE":
                self.logger.debug("Got cancelled invite response: %s" % (status,))
                # This was ACKed by the transaction layer
                self.change_state(self.DOWN)
                self.finish_media()
                return
                
            elif is_response and method == "CANCEL":
                self.logger.debug("Got cancel response: %s" % (status,))
                return
                
        raise Error("Weird message %s %s in state %s!" % (method, "response" if is_response else "request", self.state))


    def notified(self, type, params):
        self.report(dict(params, type=type))
