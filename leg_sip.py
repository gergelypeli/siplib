from async_base import WeakMethod
from format import Status, Rack, make_virtual_response
from util import build_oid
from leg import Leg, SessionState, Error
from sdp import SdpBuilder, SdpParser, STATIC_PAYLOAD_TYPES


class InviteState(object):
    REQUEST_OFFER = "REQUEST_OFFER"
    PROVISIONAL_ANSWER = "PROVISIONAL_ANSWER"
    RELIABLE_ANSWER = "RELIABLE_ANSWER"
    PRACK_OFFER = "PRACK_OFFER"
    FINAL_ANSWER = "FINAL_ANSWER"
    
    REQUEST_EMPTY = "REQUEST_EMPTY"
    PROVISIONAL_OFFER = "PROVISIONAL_OFFER"
    RELIABLE_OFFER = "RELIABLE_OFFER"
    PRACK_ANSWER = "PRACK_ANSWER"
    FINAL_OFFER = "FINAL_OFFER"
    
    SESSION_ESTABLISHED = "SESSION_ESTABLISHED"
    FINISHED = "FINISHED"
    
    def __init__(self, request, is_outgoing):
        has_sdp = request.get("sdp")
        self.state = self.REQUEST_OFFER if has_sdp else self.REQUEST_EMPTY

        self.request = request
        
        #self.is_outgoing = is_outgoing  # Do we need this?
        self.responded_sdp = None
        #self.final_response = None
        
        self.rpr_supported_locally = True  # FIXME
        self.rpr_supported_remotely = True  # FIXME
        #self.rpr_state = self.RPR_NONE
        self.rpr_unpracked = False
        self.rpr_rseq = None
        #self.rpr_sdp_responded = False
        self.rpr_last_message = None  # rpr or prack request
        #self.rpr_queue = []


    def change_state(self, new_state):
        self.state = new_state
        
        
    def is_finished(self):
        return self.state == self.FINISHED
        

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
        self.pending_actions = []
        
        host = dialog.dialog_manager.get_local_addr().resolve()[0]  # TODO: not nice
        self.sdp_builder = SdpBuilder(host)
        self.sdp_parser = SdpParser()
        
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


    def flatten_formats(self, formats, pt_key):
        return { f[pt_key]: (f["encoding"], f["clock"], f["encp"], f["fmtp"]) for f in formats }


    def allocate_local_media(self, old_session, new_session):
        new_channels = new_session["channels"] if new_session else []
        old_channels = old_session["channels"] if old_session else []
        
        for i, new_channel in enumerate(new_channels):
            if i >= len(old_channels):
                self.logger.debug("Allocating local media address for channel %d" % i)
                local_addr = self.call.allocate_media_address(i)  # TODO: deallocate!
            else:
                local_addr = old_channels[i]["rtp_local_addr"]
        
            new_channel["rtp_local_addr"] = local_addr
            next_pt = 96
            
            for f in new_channel["formats"]:
                x = (f["encoding"], f["clock"], f["encp"], f["fmtp"])
                
                for pt, info in STATIC_PAYLOAD_TYPES.items():
                    if info == x:
                        break
                else:
                    pt = next_pt
                    next_pt += 1
                    #raise Exception("Couldn't find payload type for %s!" % (encoding, clock))

                f["rtp_local_payload_type"] = pt
                
                
    def deallocate_local_media(self, old_session, new_session):
        new_channels = new_session["channels"] if new_session else []
        old_channels = old_session["channels"] if old_session else []
        
        for i in range(len(old_channels), len(new_channels)):
            self.logger.debug("Deallocating local media address for channel %d" % i)
            self.call.deallocate_media_address(new_channels[i]["rtp_local_addr"])


    def realize_local_media(self):
        # This must only be called after an answer is accepted
        self.logger.debug("realize_local_media")
        local_channels = self.session.local_session["channels"]
        remote_channels = self.session.remote_session["channels"]
        
        if len(local_channels) != len(remote_channels):
            raise Exception("Channel count mismatch!")
        
        for i in range(len(local_channels)):
            if i >= len(self.media_legs):
                self.logger.debug("Making media leg for channel %d" % i)
                self.make_media_leg(i, "net", report=WeakMethod(self.notified))
                
            lc = local_channels[i]
            rc = remote_channels[i]
            
            params = {
                'local_addr': lc["rtp_local_addr"],
                'remote_addr': rc["rtp_remote_addr"],
                'send_formats': self.flatten_formats(rc["formats"], "rtp_remote_payload_type"),
                'recv_formats': self.flatten_formats(lc["formats"], "rtp_local_payload_type")
            }
            
            self.logger.debug("Refreshing media leg %d: %s" % (i, params))
            self.media_legs[i].update(**params)
            

    def process_incoming_offer(self, sdp):
        session = self.sdp_parser.parse(sdp, is_answer=False)
        self.session.set_remote_offer(session)
        return session

    
    def process_incoming_answer(self, sdp):
        session = self.sdp_parser.parse(sdp, is_answer=True)
        rejected_local_offer = self.session.set_remote_answer(session)
        
        if rejected_local_offer:
            self.deallocate_local_media(self.session.local_session, rejected_local_offer)
        else:
            self.realize_local_media()
            
        return session

    
    def process_outgoing_offer(self, session):
        if session["is_answer"]:
            raise Exception("Offer expected!")
            
        self.allocate_local_media(self.session.local_session, session)
        self.session.set_local_offer(session)
        sdp = self.sdp_builder.build(session)
        return sdp

    
    def process_outgoing_answer(self, session):
        if not session["is_answer"]:
            raise Exception("Answer expected!")
            
        self.allocate_local_media(self.session.local_session, session)
        self.session.set_local_answer(session)  # don't care about rejected remote offers
        self.realize_local_media()
        sdp = self.sdp_builder.build(session)
        return sdp


    # Invite client

    def invite_outgoing_request(self, msg, session, rpr):
        if self.invite:
            raise Exception("Invalid outgoing INVITE request!")
        
        if session:
            msg["sdp"] = self.process_outgoing_offer(session)
            
        self.invite = InviteState(msg, True)
        
        if rpr:
            self.invite.rpr_supported_locally = True
            msg.setdefault("supported", set()).add("100rel")
        
        self.send_request(msg)  # Will be extended!


    def invite_incoming_response(self, msg):
        # TODO: error handling!
        if not self.invite or not self.invite.state in (InviteState.REQUEST_OFFER, InviteState.REQUEST_EMPTY):
            raise Exception("Unexpected incoming INVITE response!")
        
        status = msg.get("status")
        sdp = msg.get("sdp")
        is_rpr = "100rel" in msg.get("require", set())
        session = None

        if self.invite.state == InviteState.REQUEST_OFFER:
            if status.code >= 300:
                self.invite = None
            elif status.code >= 200:
                if sdp:
                    session = self.process_incoming_answer(sdp)
                    self.invite.change_state(InviteState.FINAL_ANSWER)
            elif is_rpr:
                if sdp:
                    session = self.process_incoming_answer(sdp)
                    self.invite.change_state(InviteState.RELIABLE_ANSWER)
                    
                self.invite_outgoing_prack(msg)
            else:
                if sdp:
                    session = self.process_incoming_answer(sdp)
                    self.invite.change_state(InviteState.PROVISIONAL_ANSWER)
                    
        elif self.invite.state == InviteState.REQUEST_EMPTY:
            if status.code >= 300:
                self.invite = None
            elif status.code >= 200:
                if sdp:
                    session = self.process_incoming_offer(sdp)
                    self.invite.change_state(InviteState.FINAL_OFFER)
            elif is_rpr:
                if sdp:
                    session = self.process_incoming_offer(sdp)
                    self.invite.change_state(InviteState.RELIABLE_OFFER)
                else:
                    self.invite_outgoing_prack(msg)
            else:
                if sdp:
                    session = self.process_incoming_offer(sdp)
                    self.invite.change_state(InviteState.PROVISIONAL_OFFER)
        
        elif self.invite.state == InviteState.PROVISIONAL_ANSWER:
            if status.code >= 300:
                self.invite = None
            elif status.code >= 200:  # Must get answer!
                if sdp:
                    self.invite.state = InviteState.FINAL_ANSWER  # Just FINAL?
                else:
                    pass  # Complain
            elif is_rpr:
                if sdp:
                    self.invite.change_state(InviteState.RELIABLE_ANSWER)
                    
                self.invite_outgoing_prack(msg)  # PRACK always
            else:
                pass

        elif self.invite.state == InviteState.PROVISIONAL_OFFER:
            if status.code >= 300:
                self.invite = None
            elif status.code >= 200:
                self.invite.change_state(InviteState.FINAL_OFFER)
            elif is_rpr:
                if sdp:
                    self.invite.state = InviteState.RELIABLE_OFFER  # Delay PRACK
                else:
                    self.invite_outgoing_prack(msg)
            else:
                pass

        elif self.invite.state == InviteState.RELIABLE_ANSWER:
            if status.code >= 300:
                self.invite = None
            elif status.code >= 200:
                pass  # Is it illegal?
            elif is_rpr:
                if sdp:
                    pass  # Illegal
                    
                self.invite_outgoing_prack(msg)  # PRACK always
            else:
                pass

        elif self.invite.state == InviteState.RELIABLE_OFFER:
            if status.code >= 300:
                self.invite = None
            elif status.code >= 200:
                pass  # Is it illegal?
            elif is_rpr:
                if sdp:
                    pass  # Illegal
                else:
                    self.invite_outgoing_prack(msg)
            else:
                pass

        elif self.invite.state == InviteState.SESSION_ESTABLISHED:
            if status.code >= 300:
                self.invite = None
            elif status.code >= 200:
                if sdp:
                    pass  # Complain
                
                self.invite.change_state(InviteState.FINAL_EMPTY)
            elif is_rpr:
                if sdp:
                    pass  # Illegal
                    
                self.invite_outgoing_prack(msg)
            else:
                pass
            
        else:
            raise Exception("Unexpected invite response in state %s!" % self.invite.state)
                
        return session


    def invite_outgoing_ack(self, session):
        if not self.invite:
            raise Exception("Invalid outgoing ACK request!")

        sdp = None
            
        if self.invite.state in (InviteState.FINAL_EMPTY, InviteState.FINAL_ANSWER):
            if session:
                raise Error("Unexpected session for outgoing ACK!")
        elif self.invite.state == InviteState.FINAL_OFFER:
            if not session:
                raise Error("Missing session for outgoing ACK!")
                
            sdp = self.process_outgoing_answer(session)
        else:
            raise Error("Invalid outgoing ACK in state %s!" % self.invite.state)
            
        self.send_request(dict(method="ACK", sdp=sdp), self.invite.request)  # Hope it works
        self.invite = None


    def invite_outgoing_prack(self, rpr, session):
        if not self.invite:
            raise Exception("Invalid outgoing PACK request!")
        
        rseq = rpr.get("rseq")
        cseq = rpr["cseq"]
        method = rpr["method"]
        sdp = None

        if self.invite.state == InviteState.RELIABLE_OFFER:
            if not session:
                raise Error("Missing session for outgoing PRACK!")

            sdp = self.process_outgoing_answer(session)
            self.invite.change_state(InviteState.PRACK_ANSWER)
        else:
            if session:
                raise Error("Unexpected session for outgoing PRACK!")
                
            # A plain PRACK is legal in many states

        self.send_request(dict(method="PRACK", rack=Rack(rseq, cseq, method), sdp=sdp))


    def invite_incoming_prack_ok(self, msg):
        if self.invite.state == InviteState.PRACK_ANSWER:
            self.invite.change_state(InviteState.SESSION_ESTABLISHED)


    # Invite server

    def invite_incoming_request(self, msg):
        if self.invite:
            raise Exception("Unexpected incoming INVITE request!")

        sdp = msg.get("sdp")
        session = None
        
        if sdp:
            session = self.process_incoming_offer(sdp)
            
        self.invite = InviteState(msg, False)
        
        self.logger.debug("XXX supported: %s" % msg.get("supported"))
        if "100rel" in msg.get("supported", set()):
            self.invite.rpr_supported_remotely = True
        
        return session


    def invite_outgoing_response_session_sendable(self):
        return self.invite and self.invite.state in (
            InviteState.REQUEST_OFFER, InviteState.REQUEST_EMPTY,
            InviteState.SESSION_ESTABLISHED
        )


    def invite_outgoing_response_sendable(self):
        return self.invite and self.invite.state in (
            InviteState.REQUEST_OFFER, InviteState.REQUEST_EMPTY,
            InviteState.PROVISIONAL_ANSWER, InviteState.PROVISIONAL_OFFER,
            InviteState.SESSION_ESTABLISHED
        )
        

    def invite_outgoing_response(self, msg, session, is_rpr):
        if not self.invite:
            raise Exception("Invalid outgoing INVITE response!")

        if is_rpr:
            self.invite.rpr_supported_locally = True

        is_rpr = self.invite.rpr_supported_locally and self.invite.rpr_supported_remotely

        # FIXME: responded_sdp should be provisional_sdp, and be cleared after
        # a reliable one is sent
        status = msg["status"]
        sdp = None

        if self.invite.state == InviteState.REQUEST_OFFER:
            if status.code >= 300:
                self.invite = None
            elif status.code >= 200:
                if session:
                    sdp = self.process_outgoing_answer(session)
                    self.invite.change_state(InviteState.FINAL_ANSWER)
                else:
                    raise Error("Missing session in final response!")
            elif is_rpr:
                if session:
                    sdp = self.process_outgoing_answer(session)
                    self.invite.change_state(InviteState.RELIABLE_ANSWER)
            else:
                if session:
                    sdp = self.process_outgoing_answer(session)
                    self.invite.provisional_sdp = sdp
                    self.invite.change_state(InviteState.PROVISIONAL_ANSWER)
                
        elif self.invite.state == InviteState.REQUEST_EMPTY:
            if status.code >= 300:
                self.invite = None
            elif status.code >= 200:
                if session:
                    sdp = self.process_outgoing_offer(session)
                    self.invite.change_state(InviteState.FINAL_OFFER)
                else:
                    raise Error("Missing session in final response!")
            elif is_rpr:
                if session:
                    sdp = self.process_outgoing_offer(session)
                    self.invite.change_state(InviteState.RELIABLE_OFFER)
                else:
                    raise Error("Missing offer in first reliable response!")
            else:
                if session:
                    sdp = self.process_outgoing_offer(session)
                    self.invite.provisional_sdp = sdp
                    self.invite.change_state(InviteState.PROVISIONAL_OFFER)

        elif self.invite.state == InviteState.PROVISIONAL_ANSWER:
            if status.code >= 300:
                self.invite = None
            elif status.code >= 200:
                if session:
                    raise Error("Unexpected session in final response after provisional!")
                else:
                    sdp = self.invite.provisional_sdp
                    self.invite.change_state(InviteState.FINAL_ANSWER)
            elif is_rpr:
                if session:
                    raise Error("Unexpected session in reliable response after provisional!")
                else:
                    sdp = self.invite.provisional_sdp
                    self.invite.change_state(InviteState.RELIABLE_ANSWER)
            else:
                if session:
                    raise Error("Unexpected session in provisional response after provisional!")
                else:
                    sdp = self.invite.provisional_sdp
                
        elif self.invite.state == InviteState.PROVISIONAL_OFFER:
            if status.code >= 300:
                self.invite = None
            elif status.code >= 200:
                if session:
                    raise Error("Unexpected session in final response after provisional!")
                else:
                    sdp = self.invite.provisional_sdp
                    self.invite.change_state(InviteState.FINAL_OFFER)
            elif is_rpr:
                if session:
                    raise Error("Unexpected session in reliable response after provisional!")
                else:
                    sdp = self.invite.provisional_sdp
                    self.invite.change_state(InviteState.RELIABLE_OFFER)
            else:
                if session:
                    raise Error("Unexpected session in provisional response after provisional!")
                else:
                    sdp = self.invite.provisional_sdp

        elif self.invite.state == InviteState.RELIABLE_ANSWER:
            raise Error("Can't send response while a reliable is unacknowledged!")
                
        elif self.invite.state == InviteState.RELIABLE_OFFER:
            raise Error("Can't send response while a reliable is unacknowledged!")

        elif self.invite.state == InviteState.SESSION_ESTABLISHED:
            if status.code >= 300:
                self.invite = None
            elif status.code >= 200:
                if session:
                    raise Error("Unexpected session in final response after established!")
                else:
                    self.invite.change_state(InviteState.FINAL_EMPTY)
            elif is_rpr:
                if session:
                    raise Error("Unexpected session in reliable response after established!")
            else:
                if session:
                    raise Error("Unexpected session in provisional response after established!")
                
        else:
            raise Error("Invalid outgoing response in state %s!" % self.invite.state)
        
        msg["sdp"] = sdp

        if is_rpr:
            # Provisional, reliable
            msg.setdefault("require", set()).add("100rel")

            self.invite.rpr_rseq = self.invite.rpr_rseq + 1 if self.invite.rpr_rseq is not None else 1
            msg["rseq"] = self.invite.rpr_rseq
            
        self.send_response(msg, self.invite.request)
    
    
    def invite_incoming_ack(self, msg):
        if not self.invite:
            raise Exception("Unexpected incoming ACK request!")

        sdp = msg.get("sdp")
        session = None
        
        if self.invite.state in (InviteState.FINAL_ANSWER, InviteState.FINAL_EMPTY):
            if sdp:
                self.logger.debug("Unexpected session in ACK!")
        elif self.invite.state == InviteState.FINAL_OFFER:
            if not sdp:
                self.logger.debug("Unexpected sessionless ACK!")
            else:
                session = self.process_incoming_answer(sdp)
            
        # Stop the retransmission of the final answer
        self.send_response(make_virtual_response(), self.invite.request)
        # Let the ACK server transaction expire
        self.send_response(make_virtual_response(), msg)
        
        self.invite = None
        return session


    def invite_incoming_nak(self):
        if not self.invite:
            raise Exception("Unexpected incoming NAK request!")
            
        if self.invite.state not in (InviteState.FINAL_ANSWER, InviteState.FINAL_EMPTY, InviteState.FINAL_OFFER):
            self.logger.debug("Unexpected NAK in state %s!" % self.invite.state)
                
        self.invite = None


    def invite_incoming_prack(self, msg):
        if not self.invite:
            raise Exception("Unexpected incoming PRACK request!")
        
        sdp = msg.get("sdp")
        session = None
        rseq, cseq, method = msg["rack"]
        
        if method != "INVITE" or cseq != self.invite.request["cseq"] or rseq != self.invite.rpr_rseq or not self.invite.rpr_unpracked:
            return False
        
        self.invite.rpr_unpracked = False
        
        # Stop the retransmission of the provisional answer
        self.send_response(make_virtual_response(), self.invite.request)

        if self.invite.state == InviteState.RELIABLE_OFFER:
            if not sdp:
                self.logger.error("Missing answer in PRACK request!")
            else:
                session = self.process_incoming_answer(sdp)
                self.invite.change_state(InviteState.SESSION_ESTABLISHED)
                self.invite_outgoing_prack_ok(msg)
                
        elif self.invite.state == InviteState.RELIABLE_ANSWER:
            if not sdp:
                # Yay, no PRACK abuse!
                self.invite.change_state(InviteState.SESSION_ESTABLISHED)
                self.invite_outgoing_prack_ok(msg)
            else:
                session = self.process_incoming_offer(sdp)
                self.invite.change_state(InviteState.PRACK_OFFER)
                # Don't respond yet
        else:
            if sdp:
                self.logger.error("Unexpected SDP in PRACK request!")
                
        
        return session


    def invite_outgoing_prack_ok(self, prack, session):
        sdp = None
        
        if self.invite.state == InviteState.PRACK_OFFER:
            if not session:
                raise Error("Missing answer for PRACK response!")
            else:
                sdp = self.process_outgoing_answer(session)
                self.invite.change_state(InviteState.SESSION_ESTABLISHED)
        else:
            if session:
                raise Error("Unexpected session for PRACK response!")
        
        self.send_response(dict(status=Status(200), sdp=sdp), prack)
        # TODO: prod here!
        

    # Others

    def finish_media(self, error=None):
        self.deallocate_local_media(None, self.session.local_session)
        Leg.finish_media(self, error)
        

    def do(self, action):
        # TODO: we probably need an inner do method, to retry pending actions,
        # because we should update self.session once, not on retries.
        # Or we should update it in each case, below. Hm. In the helper methods?
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
                
                rpr = "100rel" in action.get("options", set())
                self.invite_outgoing_request(dict(method="INVITE"), session, rpr)
                self.change_state(self.DIALING_OUT)
                
                #self.anchor()
                return

        elif self.state in (self.DIALING_OUT, self.DIALING_OUT_RINGING):
            if type == "hangup":
                self.send_request(dict(method="CANCEL"), self.invite.request)
                self.change_state(self.DISCONNECTING_OUT)
                return
                
            elif type == "session":
                if self.invite.request.get("sdp"):
                    # We sent the offer, but got no final response yet.
                
                    if self.invite.rpr_sdp_responded:
                        # Got answer in rpr, PRACK-ed automatically, send UPDATE with a new offer
                        pass
                    else:
                        # Session exchange is not officially over
                        self.pending_actions.append(action)
                else:
                    # We requested an offer
                
                    if self.invite.final_response:
                        # Got offer in final response, send answer in ACK
                        self.invite_outgoing_ack(session)
                        self.change_state(self.UP)
                    elif self.invite.rpr_sdp_responded:
                        # Got offer in rpr, have we sent an answer in a PRACK?
                        
                        # rpr_state is a wrong idea, the caller may get multiple
                        # rpr-s at once! That is only used for session-rpr-s, so
                        # should be called as such!
                        
                        if self.invite.rpr_state == InviteState.RPR_SENT:
                        #if self.invite.rpr_offer_message:
                            # No, we still have the rpr here
                            self.invite_outgoing_prack(self.invite.rpr_offer_message, session)
                            self.invite.rpr_offer_message = None
                        elif self.invite.rpr_state == InviteState.RPR_PRACKED:
                            # Already answered, but still waiting for the PRACK response
                            self.pending_actions.append(action)
                        else:
                            # Answered, and accepted, now we can send an UPDATE
                            pass  
                    else:
                        # Session exchange not complete yet
                        self.pending_actions.append(action)
                            
                return

        elif self.state in (self.DIALING_IN, self.DIALING_IN_RINGING):
            already_ringing = (self.state == self.DIALING_IN_RINGING)
            rpr = "100rel" in action.get("options", set())

            if session and not self.invite_outgoing_response_session_sendable():
                raise Exception("FIXME!")

            if type == "session":
                status = Status(180) if already_ringing else Status(183)
                self.invite_outgoing_response(dict(status=status), session, rpr)
                return
                
            elif type == "ring":
                if not session and already_ringing:
                    self.logger.debug("Already ringing and session unchanged, skipping 180.")
                    return
                
                self.invite_outgoing_response(dict(status=Status(180)), session, rpr)
                
                if not already_ringing:
                    self.change_state(self.DIALING_IN_RINGING)
                    
                return
                    
            elif type == "accept":
                self.invite_outgoing_response(dict(status=Status(200)), session, rpr)
                # Wait for the ACK before changing state
                return

            elif type == "reject":
                self.invite_outgoing_response(dict(status=action["status"]), None, False)
                # The transactions will catch the ACK
                self.change_state(self.DOWN)
                self.finish_media()
                return

        elif self.state == self.UP:
            # Re-INVITE stuff
            
            if type == "session":
                if not self.invite:
                    self.invite_outgoing_request(dict(method="INVITE"), session, False)
                elif not self.invite.final_response:
                    # TODO: handle rejection!
                    self.invite_outgoing_response(dict(status=Status(200)), session, False)
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
        
        if is_response:
            self.logger.debug("Processing response %d %s" % (status.code, method))
        else:
            self.logger.debug("Processing request %s" % method)

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
                options = set()
                if self.invite.rpr_supported_remotely:
                    options.add("100rel")
                
                self.change_state(self.DIALING_IN)
                self.report(dict(type="dial", ctx=self.ctx, session=session, options=options))
                return
                
        elif self.state in (self.DIALING_IN, self.DIALING_IN_RINGING):
            if not is_response and method == "CANCEL":
                self.send_response(dict(status=Status(200, "OK")), msg)
                self.invite_outgoing_response(dict(status=Status(487, "Request Terminated")))
                
                self.change_state(self.DOWN)
                self.report(dict(type="hangup"))
                self.finish_media()
                return
                
                
            elif not is_response and method == "PRACK":
                ok = self.invite_incoming_prack(msg)
                
                if ok:
                    self.invite_outgoing_prack_ok(msg)
                else:
                    self.send_response(dict(status=Status(481)), msg)
                    
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
        
            elif is_response and method == "PRACK":
                self.invite_incoming_prack_ok(msg)
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
