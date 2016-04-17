from async_base import WeakMethod
from format import Status, Rack, make_virtual_response
from util import build_oid
from leg import Leg, SessionState, Error
from sdp import SdpBuilder, SdpParser, STATIC_PAYLOAD_TYPES


class InviteState(object):
    RPR_NONE = "RPR_NONE"
    RPR_SENT = "RPR_SENT"
    RPR_PRACKED = "RPR_PRACKED"
    
    def __init__(self, request, is_outgoing):
        self.request = request
        self.is_outgoing = is_outgoing
        self.responded_sdp = None
        self.final_response = None
        
        self.rpr_supported_locally = True  # FIXME
        self.rpr_supported_remotely = True  # FIXME
        self.rpr_state = self.RPR_NONE
        self.rpr_rseq = None
        self.rpr_sdp_responded = False
        self.rpr_offer_message = None  # rpr or prack request
        self.rpr_queue = []


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


    def can_send_session(self):
        

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
        self.allocate_local_media(self.session.local_session, session)
        self.session.set_local_offer(session)
        sdp = self.sdp_builder.build(session)
        return sdp

    
    def process_outgoing_answer(self, session):
        self.allocate_local_media(self.session.local_session, session)
        self.session.set_local_answer(session)  # don't care about rejected remote offers
        self.realize_local_media()
        sdp = self.sdp_builder.build(session)
        return sdp


    # Invite client

    def invite_outgoing_request(self, msg, session, rpr):
        if self.invite:
            raise Exception("Invalid outgoing INVITE request!")
        
        if not session:
            msg["sdp"] = None  #raise Exception("No session for INVITE request!")
        elif session["is_answer"]:
            raise Exception("Session answer for INVITE request!")
        else:
            msg["sdp"] = self.process_outgoing_offer(session)
            
        self.invite = InviteState(msg, True)
        
        if rpr:
            self.invite.rpr_supported_locally = True
            msg.setdefault("supported", set()).add("100rel")
        
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
                session = self.process_incoming_answer(sdp)
            else:
                session = self.process_incoming_offer(sdp)

            self.invite.responded_sdp = sdp  # just to ignore any further

        if status.code >= 300:
            # Transactions already sent the ACK
            self.invite = None
            return dict(is_answer=True)  # rejection  FIXME: not necessarily an answer!
        elif status.code >= 200:
            self.invite.final_response = msg
        elif "100rel" in msg.get("require", set()):
            self.invite.rpr_state = self.invite.RPR_SENT
            
            if self.invite.request.get("sdp") or not sdp:
                self.invite_outgoing_prack(msg)
            else:
                raise Exception("Can't PRACK with session yet!")  # TODO
                
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
            elif not session["is_answer"]:
                raise Error("Answer expected for ACK!")
            else:
                #self.logger.debug("invite_outgoing_ack with answer")
                sdp = self.process_outgoing_answer(session)
        
        self.send_request(dict(method="ACK", sdp=sdp), self.invite.final_response)
        self.invite = None


    def invite_outgoing_prack(self, rpr, session):
        rseq = rpr.get("rseq")
        cseq = rpr["cseq"]
        method = rpr["method"]
        sdp = None

        if session:
            if self.invite.request.get("sdp"):
                if session["is_answer"]:
                    raise Exception("Ignoring outgoing session, because offer is expected!")
        
                sdp = self.process_outgoing_offer(session)
            else:
                if not session["is_answer"]:
                    raise Exception("Ignoring outgoing session, because answer is expected!")
        
                sdp = self.process_outgoing_answer(session)
        
        self.send_request(dict(method="PRACK", rack=Rack(rseq, cseq, method), sdp=sdp))
        self.invite.rpr_state = self.invite.RPR_PRACKED


    def invite_incoming_prack_ok(self, msg):
        self.invite.state = self.invite.RPR_NONE


    # Invite server

    def invite_incoming_request(self, msg):
        if self.invite:
            raise Exception("Unexpected incoming INVITE request!")

        sdp = msg.get("sdp")
        session = None
        
        if sdp:
            session = self.process_incoming_offer(sdp)
        else:
            session = None
            
        self.invite = InviteState(msg, False)
        
        self.logger.debug("XXX supported: %s" % msg.get("supported"))
        if "100rel" in msg.get("supported", set()):
            self.invite.rpr_supported_remotely = True
        
        return session


    def invite_outgoing_response_session_sendable(self):
        return self.invite and not self.invite.is_outgoing and not self.invite.responded_sdp
        

    def invite_outgoing_response(self, msg, session, rpr):
        if not self.invite or self.invite.is_outgoing:
            raise Exception("Invalid outgoing INVITE response!")

        if session:
            if self.invite.responded_sdp:
                raise Exception("Ignoring outgoing session, because one already sent!")
            elif self.invite.request.get("sdp"):
                if not session["is_answer"]:
                    raise Exception("Ignoring outgoing session, because answer is expected!")
            
                self.invite.responded_sdp = self.process_outgoing_answer(session)
            else:
                if session["is_answer"]:
                    raise Exception("Ignoring outgoing session, because offer is expected!")
            
                self.invite.responded_sdp = self.process_outgoing_offer(session)
            
        if rpr:
            self.invite.rpr_supported_locally = True

        status = msg["status"]
        
        if self.invite.rpr_state == self.invite.RPR_SENT:
            self.logger.debug("Oops, can't send this %d response yet, queueing it" % status.code)
            self.invite.rpr_queue.append(msg)
            return

        if self.invite.responded_sdp and status.code < 300 and not self.invite.rpr_sdp_responded:
            msg["sdp"] = self.invite.responded_sdp

        if status.code >= 200:
            # Final
            self.invite.final_response = msg  # Note: this is an incomplete message, but OK here
        elif self.invite.rpr_supported_remotely and self.invite.rpr_supported_locally:
            # Provisional, reliable
            msg.setdefault("require", set()).add("100rel")

            self.invite.rpr_rseq = self.invite.rpr_rseq + 1 if self.invite.rpr_rseq is not None else 1
            msg["rseq"] = self.invite.rpr_rseq
            
            self.invite.rpr_state = self.invite.RPR_SENT

            if self.invite.responded_sdp:
                self.invite.rpr_sdp_responded = True
            
        self.send_response(msg, self.invite.request)
        
        if status.code >= 300:
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
                session = self.process_incoming_answer(sdp)
            
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


    def invite_incoming_prack(self, msg):
        rseq, cseq, method = msg["rack"]
        
        if method != "INVITE" or cseq != self.invite.request["cseq"] or rseq != self.invite.rpr_rseq:
            return False
        
        # Stop the retransmission of the provisional answer
        self.send_response(make_virtual_response(), self.invite.request)

        self.invite.rpr_state = self.invite.RPR_PRACKED  # TODO: session...
        return True


    def invite_outgoing_prack_ok(self, prack):
        self.send_response(dict(status=Status(200)), prack)
        self.invite.rpr_state = self.invite.RPR_NONE
        
        if self.invite.rpr_queue:
            msg = self.invite.rpr_queue.pop(0)
            status = msg["status"]
            self.logger.debug("Now sending the queued %d response" % status.code)
            
            self.invite_outgoing_response(msg)


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
