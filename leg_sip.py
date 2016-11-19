from format import Status
from util import build_oid
from leg import Party, Error
from session import SessionState
from sdp import SdpBuilder, SdpParser, STATIC_PAYLOAD_TYPES
from leg_sip_invite import InviteClientState, InviteServerState


class SipParty(Party):
    DOWN = "DOWN"
    SELECTING_HOP = "SELECTING_HOP"
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
        
        self.sdp_builder = SdpBuilder()
        self.sdp_parser = SdpParser()
        
        self.dialog.report_slot.plug(self.process)


    def set_oid(self, oid):
        Party.set_oid(self, oid)
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


    def forward(self, action):
        self.legs[0].forward(action)
        

    def make_invite(self, is_outgoing):
        if self.invite:
            raise Error("Already has an invite!")
            
        if is_outgoing:
            self.invite = InviteClientState()
            self.invite.message_slot.plug(self.send_request)
        else:
            self.invite = InviteServerState()
            self.invite.message_slot.plug(self.send_response)
            
        self.invite.set_oid(build_oid(self.oid, "invite"))
        

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

        leg = self.legs[0]
        
        for i in range(len(local_channels)):
            ml = leg.get_media_leg(i)
            
            if not ml:
                self.logger.debug("Making media leg for channel %d" % i)
                ml = leg.make_media_leg("net")
                leg.set_media_leg(i, ml)
                ml.event_slot.plug(self.notified)
                
            lc = local_channels[i]
            rc = remote_channels[i]
            
            params = {
                'local_addr': lc["rtp_local_addr"],
                'remote_addr': rc["rtp_remote_addr"],
                'send_formats': self.flatten_formats(rc["formats"], "rtp_remote_payload_type"),
                'recv_formats': self.flatten_formats(lc["formats"], "rtp_local_payload_type")
            }
            
            self.logger.debug("Refreshing media leg %d: %s" % (i, params))
            ml.update(**params)
            

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


    def process_incoming_sdp(self, sdp, is_answer):
        if not sdp:
            return None
        elif is_answer:
            return self.process_incoming_answer(sdp)
        else:
            return self.process_incoming_offer(sdp)
            
    
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

            
    def process_outgoing_session(self, session):
        # Returns the sdp
        
        if not session:
            return None
        elif session["is_answer"]:
            return self.process_outgoing_answer(session)
        else:
            return self.process_outgoing_offer(session)
        

    def may_finish(self):
        self.deallocate_local_media(None, self.session.local_session)
        Party.may_finish(self)


    def hop_selected(self, hop, action):
        action["auto_hop"] = hop  # don't alter the ctx, just to be sure
        self.logger.debug("Retrying dial with resolved hop")
        self.do_slot(0, action)
        

    def do_slot(self, li, action):
        # TODO: we probably need an inner do method, to retry pending actions,
        # because we should update self.session once, not on retries.
        # Or we should update it in each case, below. Hm. In the helper methods?
        self.logger.debug("Doing %s" % action)
        
        type = action["type"]
        session = action.get("session")
        
        if self.state in (self.DOWN, self.SELECTING_HOP):
            if type == "dial":
                ctx = action["ctx"]
                fr = ctx.get("from")
                to = ctx.get("to")
                
                # These are mandatory
                if not fr:
                    self.logger.error("No From field for outgoing SIP leg!")
                elif not to:
                    self.logger.error("No To field for outgoing SIP leg!")
                    
                # Explicit URI is not needed if the To is fine
                uri = ctx.get("uri") or to.uri
                route = ctx.get("route")
                hop = ctx.get("hop") or action.get("auto_hop")
                
                # Hop may be calculated here, but it takes another round
                if not hop:
                    next_uri = route[0].uri if route else uri
                    self.call.select_hop_slot(next_uri).plug(self.hop_selected, action=action)
                    self.change_state(self.SELECTING_HOP)
                    return
            
                # These parameters can't go into the user_params, because it
                # won't be there for future requests, and we should be consistent.
                self.dialog.setup_outgoing(uri, fr, to, route, hop)
                self.make_invite(True)
                
                if "100rel" in action.get("options", set()):
                    self.invite.use_rpr_locally()
                    
                sdp = self.process_outgoing_session(session)
                self.invite.outgoing(dict(method="INVITE"), sdp)
                self.change_state(self.DIALING_OUT)
                
                return

        elif self.state in (self.DIALING_OUT, self.DIALING_OUT_RINGING):
            if type == "hangup":
                self.invite.outgoing(dict(method="CANCEL"))
                self.change_state(self.DISCONNECTING_OUT)
                return
                
            elif type == "session":
                if self.invite.is_session_finished():
                    raise Error("Can't send UPDATE yet!")
                else:
                    sdp = self.process_outgoing_session(session)
                    self.invite.outgoing(None, sdp)
                    
                    if self.invite.is_finished():  # may have sent an ACK
                        self.invite = None
                        self.change_state(self.UP)
                        
                return

        elif self.state in (self.DIALING_IN, self.DIALING_IN_RINGING):
            already_ringing = (self.state == self.DIALING_IN_RINGING)
            #rpr = "100rel" in action.get("options", set())

            if self.invite.is_session_finished():
                raise Error("Can't send UPDATE yet!")

            if type == "session":
                # TODO: use an empty msg, to have opportunity for extra fields!
                msg = dict(status=Status(180)) if already_ringing else None
                sdp = self.process_outgoing_session(session)
                self.invite.outgoing(msg, sdp)
                return
                
            elif type == "ring":
                msg = dict(status=Status(180))
                sdp = self.process_outgoing_session(session)
                self.invite.outgoing(msg, sdp)

                if not already_ringing:
                    self.change_state(self.DIALING_IN_RINGING)
                    
                return
                    
            elif type == "accept":
                msg = dict(status=Status(200))
                sdp = self.process_outgoing_session(session)
                self.invite.outgoing(msg, sdp)
                # Wait for the ACK before changing state
                return

            elif type == "reject":
                msg = dict(status=action["status"])
                self.invite.outgoing(msg, None)
                # The transactions will catch the ACK
                self.change_state(self.DOWN)
                self.may_finish()
                return

        elif self.state == self.UP:
            # Re-INVITE stuff
            
            if type == "session":
                if not self.invite:
                    self.make_invite(True)
                    msg = dict(method="INVITE")
                    sdp = self.process_outgoing_session(session)
                    self.invite.outgoing(msg, sdp)
                else:
                    msg = dict(status=Status(200))  # TODO: handle rejection!
                    sdp = self.process_outgoing_session(session)
                    self.invite.outgoing(msg, sdp)
                return
        
            elif type == "tone":
                if self.legs[0].media_legs and action.get("name"):
                    self.legs[0].media_legs[0].notify("tone", dict(name=action["name"]))
                    
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
        
        # Note: must change state before forward, because that can generate
        # a reverse action which should only be processed with the new state!
        
        if self.state == self.DOWN:
            if not is_response and method == "INVITE":
                ctx = {
                    "uri": msg["uri"],
                    "from": msg["from"],
                    "to": msg["to"]
                }
                
                self.make_invite(False)
                msg, sdp, is_answer = self.invite.incoming(msg)
                session = self.process_incoming_sdp(sdp, is_answer)
                options = set("100rel") if self.invite.is_rpr_supported() else set()
                
                self.change_state(self.DIALING_IN)
                self.forward(dict(type="dial", ctx=ctx, session=session, options=options))
                return
                
        elif self.state in (self.DIALING_IN, self.DIALING_IN_RINGING):
            msg, sdp, is_answer = self.invite.incoming(msg)
            
            if self.invite.is_finished():
                if method == "CANCEL":
                    self.change_state(self.DOWN)
                    self.forward(dict(type="hangup"))
                    self.may_finish()
                elif method == "ACK":
                    session = self.process_incoming_sdp(sdp, is_answer)
                    self.change_state(self.UP)
                    
                    if session:
                        self.forward(dict(type="session", session=session))
                elif method == "NAK":
                    self.send_request(dict(method="BYE"))  # required behavior
                    self.change_state(self.DISCONNECTING_OUT)
                    
                return
                
        elif self.state in (self.DIALING_OUT, self.DIALING_OUT_RINGING):
            msg, sdp, is_answer = self.invite.incoming(msg)
            session = self.process_incoming_sdp(sdp, is_answer)

            if msg and msg["method"] == "INVITE":
                status = msg["status"]
                
                if status.code == 180:
                    if self.state == self.DIALING_OUT_RINGING:
                        if session:
                            self.forward(dict(type="session", session=session))
                        
                    else:
                        self.change_state(self.DIALING_OUT_RINGING)
                        self.forward(dict(type="ring", session=session))
                        
                    return

                elif status.code == 183:
                    if session:
                        self.forward(dict(type="session", session=session))
                        
                    return
                    
                elif status.code >= 300:
                    if self.invite.is_finished():
                        # Transaction now acked, invite should be finished now
                        self.invite = None
                        self.change_state(self.DOWN)
                        self.forward(dict(type="reject", status=status))
                        self.may_finish()
                        
                    return

                elif status.code >= 200:
                    if self.invite.is_finished():
                        self.invite = None
                        self.change_state(self.UP)
                        
                    self.forward(dict(type="accept", session=session))
                    return

        elif self.state == self.UP:
            # Re-INVITE stuff
            
            if not is_response:
                if method in ("INVITE", "ACK", "NAK"):
                    if method == "INVITE":
                        self.make_invite(False)
                    
                    msg, sdp, is_answer = self.invite.incoming(msg)
                    session = self.process_incoming_sdp(sdp, is_answer)
                    
                    if self.invite.is_finished():
                        self.invite = None
                
                    if session:
                        self.forward(dict(type="session", session=session))
                        
                    return
                    
                elif method == "BYE":
                    self.send_response(dict(status=Status(200, "OK")), msg)
                    self.change_state(self.DOWN)
                    self.forward(dict(type="hangup"))
                    self.may_finish()
                    return
                    
            else:
                if method == "INVITE" and self.invite:
                    # FIXME: copy-paste!
                    msg, sdp, is_answer = self.invite.incoming(msg)
                    session = self.process_incoming_sdp(sdp, is_answer)
                    
                    if self.invite.is_finished():
                        self.invite = None
                
                    if session:
                        self.forward(dict(type="session", session=session))
                        
                    return

        elif self.state == self.DISCONNECTING_OUT:
            if is_response and method == "BYE":
                self.change_state(self.DOWN)
                self.may_finish()
                return
                
            elif is_response and method == "INVITE":
                self.logger.debug("Got cancelled invite response: %s" % (status,))
                # This was ACKed by the transaction layer
                self.change_state(self.DOWN)
                self.may_finish()
                return
                
            elif is_response and method == "CANCEL":
                self.logger.debug("Got cancel response: %s" % (status,))
                return
                
        if msg or sdp:
            raise Error("Weird message %s %s in state %s!" % (method, "response" if is_response else "request", self.state))


    def notified(self, type, params):
        self.forward(dict(params, type=type))
