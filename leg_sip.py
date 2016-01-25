from copy import deepcopy

from async import WeakMethod
from format import Status, make_virtual_response
from util import build_oid
from leg import Leg, Session, Error


class Invite(object):
    def __init__(self, request, is_outgoing):
        self.request = request
        self.is_outgoing = is_outgoing
        
        sdp = request.get("sdp")
        self.had_offer_in_request = (sdp and sdp.is_session())
        
        self.responded_session = None
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
        self.session = Session()
        
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
            

    def preprocess_outgoing_session(self, sdp):
        for i in range(len(self.media_legs), len(sdp.channels)):
            self.make_media_leg(i, "net", report=WeakMethod(self.notified))
        
        for i in range(len(sdp.channels)):
            # No need to make a copy again here
            if sdp.channels[i].addr:
                raise Exception("Outgoing session has channel address set! %s" % id(sdp.channels[i]))
                
            sdp.channels[i].addr = self.media_legs[i].local_addr
            
        return sdp  # For the sake of consistency


    def postprocess_incoming_session(self, sdp):
        sdp = deepcopy(sdp)

        for i in range(len(sdp.channels)):
            sdp.channels[i].addr = None  # Just to be sure
            
        return sdp

        
    def process_incoming_offer(self, sdp):
        self.session.set_remote_offer(sdp)
        sdp = self.postprocess_incoming_session(sdp)
        return sdp

    
    def process_incoming_answer(self, sdp):
        self.session.set_remote_answer(sdp)
        self.refresh_media()
        sdp = self.postprocess_incoming_session(sdp)
        return sdp

    
    def process_outgoing_offer(self, sdp):
        sdp = self.preprocess_outgoing_session(sdp)
        self.session.set_local_offer(sdp)
        return sdp

    
    def process_outgoing_answer(self, sdp):
        sdp = self.preprocess_outgoing_session(sdp)
        self.session.set_local_answer(sdp)
        self.refresh_media()
        return sdp


    def invite_outgoing_request(self, msg):
        if self.invite:
            raise Exception("Invalid outgoing INVITE request!")
        
        if msg.get("sdp"):
            msg["sdp"] = self.process_outgoing_offer(msg["sdp"])
            
        self.invite = Invite(msg, True)
        self.send_request(msg)  # Will be extended!


    def invite_incoming_response(self, msg):
        # TODO: error handling!
        if not self.invite or not self.invite.is_outgoing:
            raise Exception("Unexpected incoming INVITE response!")
        
        status = msg.get("status")
        sdp = msg.get("sdp")
        session = {}
        
        if sdp and sdp.is_session() and not self.invite.responded_session:
            # No session yet in INVITE responses, take this one
            
            if self.invite.had_offer_in_request:
                session["answer"] = self.process_incoming_answer(sdp)
            else:
                session["offer"] = self.process_incoming_offer(sdp)

            self.invite.responded_session = sdp  # just to ignore any further

        if status.code >= 300:
            pass  # TODO: some kind of rejection? At least ignore the session in it.
        elif status.code >= 200:
            if self.invite.had_offer_in_request:
                # send ACK without SDP now
                self.send_request(dict(method="ACK"), msg)
                self.invite = None
            else:
                # wait for outgoing session for the ACK
                # beware, the report() below may send it!
                self.invite.final_response = msg
                self.invite.state = Invite.OUT_RESPONDED
                
        return session


    def invite_outgoing_ack(self, answer):
        if not self.invite or not self.invite.is_outgoing or not self.invite.final_response:
            raise Exception("Invalid outgoing ACK request!")
            
        # send ACK with SDP
        if not answer:
            # This function is only called for the no-SDP-in-request case
            raise Error("Answer expected for explicit ACK!")
            
        sdp = self.process_outgoing_answer(answer)
        self.send_request(dict(method="ACK", sdp=sdp), self.invite.final_response)
        self.invite = None


    def invite_incoming_request(self, msg):
        if self.invite:
            raise Exception("Unexpected incoming INVITE request!")

        sdp = msg.get("sdp")
        session = {}
        
        if sdp and sdp.is_session():
            session["offer"] = self.process_incoming_offer(sdp)  # TODO: session query?
            
        self.invite = Invite(msg, False)
        
        return session


    def invite_outgoing_response_session(self, offer, answer):
        if not self.invite or self.invite.is_outgoing:
            raise Exception("Invalid outgoing INVITE response session!")
    
        session_changed = False
        
        if self.invite.responded_session:
            if offer or answer:
                self.logger.warning("Ignoring outgoing session, because one already sent!")
        elif self.invite.had_offer_in_request:
            if answer:
                self.invite.responded_session = self.process_outgoing_answer(answer)
                session_changed = True
            
            if offer:
                self.logger.warning("Ignoring outgoing offer, because answer is expected!")
        else:
            if offer:
                self.invite.responded_session = self.process_outgoing_offer(offer)
                session_changed = True
            
            if answer:
                self.logger.warning("Ignoring outgoing answer, because offer is expected!")

        return session_changed


    def invite_outgoing_response(self, msg):
        if not self.invite or self.invite.is_outgoing:
            raise Exception("Invalid outgoing INVITE response!")
        
        msg["sdp"] = self.invite.responded_session
        self.invite.final_response = msg  # Note: this is an incomplete message, but OK here
        self.send_response(self.invite.final_response, self.invite.request)
        
        if msg["status"].code >= 300:
            self.invite = None
    
    
    def invite_incoming_ack(self, msg):
        if not self.invite or self.invite.is_outgoing or not self.invite.final_response:
            raise Exception("Unexpected incoming ACK request!")

        sdp = msg.get("sdp")
        session = {}
        
        if self.invite.had_offer_in_request:
            if sdp and sdp.is_session():
                self.logger.debug("Unexpected session in ACK!")
        else:
            if not (sdp and sdp.is_session()):
                self.logger.debug("Unexpected sessionless ACK!")
            else:
                session["answer"] = self.process_incoming_answer(sdp)
            
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
        offer = action.get("offer")
        answer = action.get("answer")
        
        if offer and answer:
            raise Error("WTF?")
        
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
                
                self.invite_outgoing_request(dict(method="INVITE", sdp=offer))
                self.change_state(self.DIALING_OUT)
                
                self.anchor()
                return

        elif self.state in (self.DIALING_OUT, self.DIALING_OUT_RINGING):
            if type == "hangup":
                self.send_request(dict(method="CANCEL"), self.invite.request)
                self.change_state(self.DISCONNECTING_OUT)
                return
                
            elif type == "session":
                self.invite_outgoing_ack(answer)
                self.change_state(self.UP)
                return

        elif self.state in (self.DIALING_IN, self.DIALING_IN_RINGING):
            session_changed = self.invite_outgoing_response_session(offer, answer)

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
                    if not offer:
                        pass
                    else:
                        self.invite_outgoing_request(dict(method="INVITE", sdp=offer))
                elif not self.invite.final_response:
                    # TODO: handle rejection!
                    self.invite_outgoing_response_session(offer, answer)
                    self.invite_outgoing_response(dict(status=Status(200)))
                else:
                    if not answer:
                        pass
                    else:
                        self.invite_outgoing_ack(answer)
                        
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
                self.report(dict(type="dial", ctx=self.ctx, **session))
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
                    self.report(dict(type="session", **session))
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
                        self.report(dict(type="ring", **session))
                    elif session:
                        self.report(dict(type="session", **session))
                    return
                    
                elif status.code == 183:
                    if session:
                        self.report(dict(type="session", **session))
                    return
                    
                elif status.code >= 300:
                    self.change_state(self.DOWN)
                    self.report(dict(type="reject", status=status))
                    self.finish_media()
                    return
                    
                elif status.code >= 200:
                    if not self.invite:
                        # If the INVITE is completed, then go up
                        self.change_state(self.UP)
                        
                    self.report(dict(type="accept", **session))
                    return
                    
        elif self.state == self.UP:
            # Re-INVITE stuff
            
            if not is_response and method == "INVITE":
                session = self.invite_incoming_request(msg)
                if session:
                    self.report(dict(type="session", **session))
                return
                
            elif not is_response and method == "ACK":
                session = self.invite_incoming_ack(msg)
                if session:
                    self.report(dict(type="session", **session))
                return
                
            elif not is_response and method == "NAK":  # virtual request, no ACK received
                self.invite_incoming_nak()
                # TODO: now what?
                return
                
            elif is_response and method == "INVITE":
                session = self.invite_incoming_response(msg)
                if session:
                    self.report(dict(type="session", **session))
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
