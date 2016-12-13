from collections import namedtuple

from format import Status
from party import Endpoint
from sdp import SdpBuilder, SdpParser, STATIC_PAYLOAD_TYPES
from party_sip_invite import InviteClientState, InviteServerState


AllocatedMedia = namedtuple("AllocatedMedia", "mgw_sid local_addr")


class Error(Exception):
    pass
    

class SipEndpoint(Endpoint):
    DOWN = "DOWN"
    SELECTING_HOP = "SELECTING_HOP"
    DIALING_IN = "DIALING_IN"
    DIALING_OUT = "DIALING_OUT"
    DIALING_IN_RINGING = "DIALING_IN_RINGING"
    DIALING_OUT_RINGING = "DIALING_OUT_RINGING"
    UP = "UP"
    DISCONNECTING_OUT = "DISCONNECTING_OUT"
    

    def __init__(self, dialog):
        Endpoint.__init__(self)

        self.dialog = dialog
        self.state = self.DOWN
        self.invite = None
        self.allocated_media = []
        self.pending_actions = []
        
        self.sdp_builder = SdpBuilder()
        self.sdp_parser = SdpParser()
        
        self.dialog.report_slot.plug(self.process)


    def set_oid(self, oid):
        Endpoint.set_oid(self, oid)
        self.dialog.set_oid(oid.add("dialog"))


    def get_dialog(self):
        return self.dialog
        

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
        self.leg.forward(action)
        

    def make_invite(self, is_outgoing):
        if self.invite:
            raise Error("Already has an invite!")
            
        if is_outgoing:
            self.invite = InviteClientState()
            self.invite.message_slot.plug(self.send_request)
        else:
            self.invite = InviteServerState()
            self.invite.message_slot.plug(self.send_response)
            
        self.invite.set_oid(self.oid.add("invite"))
        

    def flatten_formats(self, formats, pt_key):
        return { f[pt_key]: (f["encoding"], f["clock"], f["encp"], f["fmtp"]) for f in formats }


    def update_local_media(self):
        ss = self.leg.session_state
        gs = ss.pending_ground_session or ss.ground_session
        local_channels = gs["channels"] if gs else []
        
        for i, local_channel in enumerate(local_channels):
            if i < len(self.allocated_media):
                mgw_sid, local_addr = self.allocated_media[i]
            else:
                ctype = local_channel["type"]
                mgw_affinity = local_channel.get("mgw_affinity")
                self.logger.debug("Allocating local media address for channel %d (%s@%s)" % (i, ctype, mgw_affinity))
                
                mgw_sid = self.ground.select_gateway_sid(ctype, mgw_affinity)
                local_addr = self.ground.allocate_media_address(mgw_sid)
                
                allocated_media = AllocatedMedia(mgw_sid, local_addr)
                self.allocated_media.append(allocated_media)
                
            local_channel["rtp_local_addr"] = local_addr
            
            next_payload_type = 96
            used_payload_types = set()
            
            for f in local_channel["formats"]:
                format_info = (f["encoding"], f["clock"], f["encp"], f["fmtp"])
                rpt = f.get("rtp_remote_payload_type")  # the one from the other endpoint
                
                for spt, info in STATIC_PAYLOAD_TYPES.items():
                    if info == format_info:
                        pt = spt
                        break
                else:
                    if rpt and rpt not in used_payload_types:
                        pt = rpt
                    else:
                        while next_payload_type in used_payload_types:
                            next_payload_type += 1
                        
                        pt = next_payload_type
                        next_payload_type += 1
                        #raise Exception("Couldn't find payload type for %s!" % (encoding, clock))

                f["rtp_local_payload_type"] = pt
                used_payload_types.add(pt)
        
        for i, allocated_media in reversed(list(enumerate(self.allocated_media))):
            if i >= len(local_channels):
                mgw_sid, local_addr = allocated_media
                self.logger.debug("Deallocating local media address for channel %d" % i)
                self.ground.deallocate_media_address(local_addr)
                self.allocated_media.pop()


    def realize_local_media(self):
        # This must only be called after an answer is accepted
        self.logger.debug("realize_local_media")
        ss = self.leg.session_state
        local_channels = ss.ground_session["channels"] if ss.ground_session else []
        remote_channels = ss.party_session["channels"] if ss.party_session else []
        
        if len(local_channels) != len(remote_channels):
            raise Exception("Channel count mismatch!")

        for i in range(len(local_channels)):
            lc = local_channels[i]
            rc = remote_channels[i]
            am = self.allocated_media[i]
            ml = self.leg.get_media_leg(i)
            
            if not ml:
                self.logger.debug("Making media leg for channel %d" % i)
                mgw_sid = am.mgw_sid
                ml = self.make_media_leg("net")
                self.leg.set_media_leg(i, ml, mgw_sid)
                ml.event_slot.plug(self.notified)
            
            params = {
                'local_addr': lc["rtp_local_addr"],
                'remote_addr': rc["rtp_remote_addr"],
                'send_formats': self.flatten_formats(rc["formats"], "rtp_remote_payload_type"),
                'recv_formats': self.flatten_formats(lc["formats"], "rtp_local_payload_type")
            }
            
            self.logger.debug("Refreshing media leg %d: %s" % (i, params))
            ml.update(**params)
            
            
    def add_mgw_affinities(self, session):
        remote_channels = session["channels"]
        
        for i in range(len(self.allocated_media)):
            remote_channels[i]["mgw_affinity"] = self.allocated_media[i].mgw_sid


    def process_incoming_sdp(self, sdp, is_answer):
        if not sdp:
            return None
            
        session = self.sdp_parser.parse(sdp, is_answer)
        self.leg.session_state.set_party_session(session)

        if is_answer:
            if "channels" not in session:  # reject
                self.update_local_media()
            else:
                self.add_mgw_affinities(session)
                self.realize_local_media()
        else:
            self.add_mgw_affinities(session)
        
        return session
            
    
    def process_outgoing_session(self, session):
        # Returns the sdp
        
        if not session:
            return None
            
        self.leg.session_state.set_ground_session(session)
        self.update_local_media()

        if session["is_answer"]:
            self.realize_local_media()

        sdp = self.sdp_builder.build(session)
        return sdp
        

    def may_finish(self):
        # TODO: nicer!
        self.leg.session_state.ground_session = None
        self.update_local_media()
        
        Endpoint.may_finish(self)


    def hop_selected(self, hop, action):
        action["auto_hop"] = hop  # don't alter the ctx, just to be sure
        self.logger.debug("Retrying dial with resolved hop")
        self.do(action)
        

    def do(self, action):
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
                    self.ground.select_hop_slot(next_uri).plug(self.hop_selected, action=action)
                    self.change_state(self.SELECTING_HOP)
                    return
            
                # These parameters can't go into the user_params, because it
                # won't be there for future requests, and we should be consistent.
                self.logger.info("Using hop %s to reach %s." % (hop, uri))
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
                if self.leg.media_legs and action.get("name"):
                    self.leg.media_legs[0].notify("tone", dict(name=action["name"]))
                    
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
