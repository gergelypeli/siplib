from collections import namedtuple

from format import Status
from party import Endpoint
from sdp import SdpBuilder, SdpParser, STATIC_PAYLOAD_TYPES, add_sdp, get_sdp
from party_sip_invite import InviteClientState, InviteServerState
import zap


AllocatedMedia = namedtuple("AllocatedMedia", "mgw_sid local_addr")


class Error(Exception):
    pass
    

class SipEndpoint(Endpoint):
    DOWN = "DOWN"
    DIALING_IN = "DIALING_IN"
    DIALING_OUT = "DIALING_OUT"
    DIALING_IN_RINGING = "DIALING_IN_RINGING"
    DIALING_OUT_RINGING = "DIALING_OUT_RINGING"
    UP = "UP"
    DISCONNECTING_OUT = "DISCONNECTING_OUT"

    DEFAULT_ALLOWED_METHODS = ["INVITE", "CANCEL", "ACK", "PRACK", "BYE", "UPDATE"]

    USE_RPR = True

    def __init__(self, dialog):
        Endpoint.__init__(self)

        self.dialog = dialog
        self.state = self.DOWN
        self.invite = None
        self.update = None
        self.allocated_media = []
        
        self.has_clogged_invite = False
        self.pending_actions = []
        
        self.sdp_builder = SdpBuilder()
        self.sdp_parser = SdpParser()
        
        self.dialog.report_slot.plug(self.process)


    def set_oid(self, oid):
        Endpoint.set_oid(self, oid)
        self.dialog.set_oid(oid.add("dialog"))


    def get_dialog(self):
        return self.dialog
        
        
    def identify(self, params):
        self.dst = params
        
        return self.dialog.get_local_tag()
        

    def change_state(self, new_state):
        self.logger.debug("Changing state %s => %s" % (self.state, new_state))
        self.state = new_state
        

    def send_request(self, request, related=None):
        request["allow"] = self.DEFAULT_ALLOWED_METHODS
        self.dialog.send_request(request, related)


    def send_response(self, response, related):
        if related and not related["is_response"]:
            response["allow"] = self.DEFAULT_ALLOWED_METHODS
            self.dialog.send_response(response, related)
        else:
            raise Error("Respond to what?")


    def forward(self, action):
        self.leg.forward(action)
        

    # INVITE helpers
    def invite_new(self, is_outgoing):
        if self.invite:
            raise Error("Invite already in progress!")
            
        if is_outgoing:
            self.invite = InviteClientState(self.USE_RPR)
            self.invite.message_slot.plug(self.send_request)
        else:
            self.invite = InviteServerState(self.USE_RPR)
            self.invite.message_slot.plug(self.send_response)
            
        self.invite.set_oid(self.oid.add("invite"))
        
        
    def invite_outgoing(self, msg, sdp):
        if not self.invite:
            raise Error("Invite not in progress!")

        if self.has_clogged_invite:
            raise Error("Mustn't try sending while invite is clogged!")

        self.invite.process_outgoing(msg, sdp)
        
        if self.invite.is_finished():
            self.invite = None
        elif self.invite.is_clogged():
            self.logger.debug("Invite is clogged, may postpone future actions.")
            self.has_clogged_invite = True


    def invite_incoming(self, msg):
        if not self.invite:
            raise Error("Invite not in progress!")
            
        msg, sdp, is_answer = self.invite.process_incoming(msg)
        
        if self.invite.is_finished():
            self.invite = None
        elif self.has_clogged_invite and not self.invite.is_clogged():
            self.has_clogged_invite = False
            
            if sdp and not is_answer:
                # Thanks to the PRACK offers, we may need to reject pending outgoing offers
                # before we send the received one up.
                
                pas = []
                
                for action in self.pending_actions:
                    session = action.get("session")
                    
                    if session:
                        self.forward(dict(type="session", session=dict(is_answer=True)))  # TODO: proper rejection!
                        
                        if action["type"] == "session":
                            continue
                        
                        action["session"] = None
                        
                    pas.append(action)
                    
                self.pending_actions = pas
            
            if self.pending_actions:
                self.logger.info("Invite unclogged, will retry postponed actions.")
                zap.time_slot(0).plug(self.do, action=None)
            
        return msg, sdp, is_answer
        
        
    # UPDATE helpers
    def update_new(self, is_outgoing):
        if self.update:
            raise Error("Update already in progress!")
            
        self.update = dict(is_outgoing=is_outgoing, request=None)
        
        
    def update_outgoing(self, msg, sdp):
        if not self.update:
            raise Error("Update not in progress!")
            
        if self.update["is_outgoing"]:
            if self.update["request"]:
                raise Error("Update was already sent!")
                
            req = add_sdp(dict(method="UPDATE"), sdp)
            self.send_request(req)
            self.update["request"] = req
        else:
            if not self.update["request"]:
                raise Error("Update was not yet received!")
                
            res = add_sdp(dict(status=Status(200)), sdp)  # TODO: handle rejection
            self.send_response(res, self.update["request"])
            self.update = None
            
            
    def update_incoming(self, msg):
        if not self.update:
            raise Error("Update not in progress!")
            
        if self.update["is_outgoing"]:
            if not self.update["request"]:
                raise Error("Update request was not sent, but now receiving one!")
                
            if not msg["is_response"]:
                self.logger.warning("Rejecting incoming UPDATE because one was already sent!")
                res = dict(status=Status(419))
                self.send_response(res, msg)
                return None, None, None
        
            if msg["status"].code == 200:
                sdp = get_sdp(msg)
                return None, sdp, True
            else:
                return None, dict(is_answer=True), True  # TODO: proper session rejection!
        else:
            if self.update["request"]:
                self.logger.warning("Update request was already received!")
                res = dict(status=Status(419))
                self.send_response(res, msg)
                return None, None, None
                
            if msg["is_response"]:
                self.logger.warning("Got an update response without sending a request!")
                return None, None, None
                
            sdp = get_sdp(msg)
            return None, sdp, False


    def update_outgoing_sdp(self, sdp, is_answer):
        if is_answer:
            self.update_outgoing(dict(status=Status(200)), sdp)  # TODO: handle rejection
        else:
            self.update_new(is_outgoing=True)
            self.update_outgoing(dict(method="UPDATE"), sdp)

        
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
            
            self.logger.debug("Modifying media leg %d: %s" % (i, params))
            ml.modify(params)
            
            
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


    def hop_selected(self, hop):
        self.dst["hop"] = hop
        self.logger.debug("Retrying dial with resolved hop")
        self.do(None)


    def do(self, action):
        type = action["type"] if action else None
        
        if not type:
            action = self.pending_actions.pop(0)
            type = action["type"]
            self.logger.info("Retrying pending %s action." % type)
        elif self.has_clogged_invite or self.pending_actions:
            if type != "hangup":
                self.logger.info("Invite clogged, postponing %s action." % type)
                self.pending_actions.append(action)
                return
            else:
                self.logger.info("Invite clogged, but not postponing hangup.")
        elif type == "dial" and not self.dst.get("hop"):
            self.logger.info("Dialing out without hop, postponing until resolved.")
            self.pending_actions.append(action)
            to = self.dst.get("to")
            uri = self.dst.get("uri") or to.uri
            route = self.dst.get("route")
            next_uri = route[0].uri if route else uri
            self.ground.select_hop_slot(next_uri).plug(self.hop_selected)
            return
        else:
            self.logger.info("Doing %s action." % type)
        
        session = action.get("session")
        # FIXME: now we can process sessions here again, because hop resolution is now
        # above
        
        if self.state in (self.DOWN,):
            if type == "dial":
                fr = self.dst.get("from")
                to = self.dst.get("to")
                
                # These are mandatory
                if not fr:
                    self.logger.error("No From field for outgoing SIP leg!")
                elif not to:
                    self.logger.error("No To field for outgoing SIP leg!")
                    
                # Explicit URI is not needed if the To is fine
                uri = self.dst.get("uri") or to.uri
                route = self.dst.get("route")
                hop = self.dst.get("hop")
                
                # These parameters can't go into the user_params, because it
                # won't be there for future requests, and we should be consistent.
                self.logger.info("Using hop %s to reach %s." % (hop, uri))
                self.dialog.setup_outgoing(uri, fr, to, route, hop)
                self.invite_new(is_outgoing=True)
                
                sdp = self.process_outgoing_session(session)
                self.invite_outgoing(dict(method="INVITE"), sdp)
                self.change_state(self.DIALING_OUT)
                
                return

        elif self.state in (self.DIALING_OUT, self.DIALING_OUT_RINGING):
            if type == "hangup":
                self.invite_outgoing(dict(method="CANCEL"))
                self.change_state(self.DISCONNECTING_OUT)
                return
                
            elif type == "session":
                if not self.invite.is_session_established():
                    sdp = self.process_outgoing_session(session)
                    self.invite_outgoing(None, sdp)
                    
                    if not self.invite:  # may have sent an ACK
                        self.change_state(self.UP)
                else:
                    sdp = self.process_outgoing_session(session)
                    self.update_outgoing_sdp(sdp, session["is_answer"])
                        
                return

        elif self.state in (self.DIALING_IN, self.DIALING_IN_RINGING):
            already_ringing = (self.state == self.DIALING_IN_RINGING)

            if session and self.invite.is_session_established():
                # Rip the sdp from the response, and use an UPDATE instead
                sdp = self.process_outgoing_session(session)
                self.update_outgoing_sdp(sdp, session["is_answer"])
                
                session = None
                if type == "session":
                    return
                
            if type == "session":
                # TODO: use an empty msg, to have opportunity for extra fields!
                msg = dict(status=Status(180)) if already_ringing else None
                sdp = self.process_outgoing_session(session)
                self.invite_outgoing(msg, sdp)
                return
                
            elif type == "ring":
                msg = dict(status=Status(180))
                sdp = self.process_outgoing_session(session)
                self.invite_outgoing(msg, sdp)

                if not already_ringing:
                    self.change_state(self.DIALING_IN_RINGING)
                    
                return
                    
            elif type == "accept":
                msg = dict(status=Status(200))
                sdp = self.process_outgoing_session(session)
                self.invite_outgoing(msg, sdp)
                # Wait for the ACK before changing state
                return

            elif type == "reject":
                msg = dict(status=action["status"])
                self.invite_outgoing(msg, None)
                # The transactions will catch the ACK
                self.change_state(self.DOWN)
                self.may_finish()
                return

        elif self.state == self.UP:
            # Re-INVITE stuff
            
            if type == "session":
                sdp = self.process_outgoing_session(session)
                
                if not self.invite:
                    self.invite_new(is_outgoing=True)
                    msg = dict(method="INVITE")
                    self.invite_outgoing(msg, sdp)
                elif not self.invite.is_session_established():
                    msg = dict(status=Status(200))  # TODO: handle rejection!
                    self.invite_outgoing(msg, sdp)
                else:
                    self.update_outgoing_sdp(sdp, session["is_answer"])
                    
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
                if self.USE_RPR and "100rel" not in msg.get("supported", set()):
                    self.logger.error("Obsolete peer does not support 100rel!")
                    self.send_response(dict(status=Status(421, "100rel support required")), msg)
                    self.may_finish()
                    return
                
                src = dict(msg, type="sip")
                ctx = {}
                
                self.invite_new(is_outgoing=False)
                msg, sdp, is_answer = self.invite_incoming(msg)
                session = self.process_incoming_sdp(sdp, is_answer)
                
                self.change_state(self.DIALING_IN)
                
                action=dict(
                    type="dial",
                    call_info=self.get_call_info(),
                    src=src,
                    ctx=ctx,
                    session=session
                )
                self.forward(action)
                return
                
        elif self.state in (self.DIALING_IN, self.DIALING_IN_RINGING):
            if method == "UPDATE":
                msg, sdp, is_answer = self.update_incoming(msg)
                session = self.process_incoming_sdp(sdp, is_answer)
                self.forward(dict(type="session", session=session))
                return
            elif method in ("CANCEL", "ACK", "NAK", "PRACK"):
                if is_response:
                    self.logger.warning("A %s response, WTF?" % method)
                    return
                
                msg, sdp, is_answer = self.invite_incoming(msg)
                
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
                elif method == "PRACK":
                    session = self.process_incoming_sdp(sdp, is_answer)
                    if session:
                        self.forward(dict(type="session", session=session))
                    
                return
                
        elif self.state in (self.DIALING_OUT, self.DIALING_OUT_RINGING):
            if method == "UPDATE":
                msg, sdp, is_answer = self.update_incoming(msg)
                session = self.process_incoming_sdp(sdp, is_answer)
                self.forward(dict(type="session", session=session))
                return
            elif method in ("INVITE", "PRACK"):
                if not is_response:
                    self.logger.warning("A %s request, WTF?" % method)
                    return

                status = msg["status"]
                    
                msg, sdp, is_answer = self.invite_incoming(msg)
                if not msg:
                    return
                
                session = self.process_incoming_sdp(sdp, is_answer)

                if method == "INVITE":
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
                        if not self.invite:
                            # Transaction now acked, invite should be finished now
                            self.change_state(self.DOWN)
                            self.forward(dict(type="reject", status=status))
                            self.may_finish()
                        
                        return

                    elif status.code >= 200:
                        if not self.invite:
                            self.change_state(self.UP)
                        
                        self.forward(dict(type="accept", session=session))
                        return
                elif method == "PRACK":
                    return # Nothing meaningful should arrive in PRACK responses

        elif self.state == self.UP:
            if method == "UPDATE":
                msg, sdp, is_answer = self.update_incoming(msg)
                session = self.process_incoming_sdp(sdp, is_answer)
                self.forward(dict(type="session", session=session))
                return
            elif method in ("INVITE", "ACK", "NAK", "PRACK"):
                # Re-INVITE stuff
            
                if not is_response:
                    if method == "INVITE":
                        self.invite_new(is_outgoing=False)
                
                    msg, sdp, is_answer = self.invite_incoming(msg)
                    session = self.process_incoming_sdp(sdp, is_answer)
                
                    if session:
                        self.forward(dict(type="session", session=session))
                    
                    return
                else:
                    # FIXME: copy-paste!
                    msg, sdp, is_answer = self.invite_incoming(msg)
                    session = self.process_incoming_sdp(sdp, is_answer)
                
                    if session:
                        self.forward(dict(type="session", session=session))
                    
                    return
            elif method == "BYE":
                if not is_response:
                    self.send_response(dict(status=Status(200, "OK")), msg)
                    self.change_state(self.DOWN)
                    self.forward(dict(type="hangup"))
                    self.may_finish()
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
