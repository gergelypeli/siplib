from collections import namedtuple

from format import Status
from party_sip_invite import InviteClientState, InviteServerState
from sdp import add_sdp, get_sdp, SdpBuilder, SdpParser, STATIC_PAYLOAD_TYPES


class InviteHelper:
    def __init__(self, use_rpr):
        self.invite_use_rpr = use_rpr

        self.invite_state = None
        self.invite_was_clogged = False


    def send_request(self, request, related=None):
        raise NotImplementedError()


    def send_response(self, response, related):
        raise NotImplementedError()
        

    def invite_unclogged(self, reject_pending_offers):
        raise NotImplementedError()
                
        
    def invite_new(self, is_outgoing):
        if self.invite_state:
            raise Exception("Invite already in progress!")
            
        # Must use instaplugs, because the ordering of the messages mustn't change,
        # and not INVITE related messages are processed synchronously.
            
        if is_outgoing:
            self.invite_state = InviteClientState(self.invite_use_rpr)
            self.invite_state.message_slot.instaplug(self.send_request)
            self.invite_state.set_oid(self.oid.add("invite-client"))
        else:
            self.invite_state = InviteServerState(self.invite_use_rpr)
            self.invite_state.message_slot.instaplug(self.send_response)
            self.invite_state.set_oid(self.oid.add("invite-server"))
        
        
    def invite_outgoing(self, msg, sdp, is_answer):
        if not self.invite_state:
            raise Exception("Invite not in progress!")

        if self.invite_was_clogged:
            raise Exception("Mustn't try sending while invite is clogged!")

        self.invite_state.process_outgoing(msg, sdp, is_answer)
        
        if self.invite_state.is_finished():
            self.invite_state = None
        elif self.invite_state.is_clogged():
            self.logger.debug("Invite is clogged, may postpone future actions.")
            self.invite_was_clogged = True


    def invite_incoming(self, msg):
        if not self.invite_state:
            raise Exception("Invite not in progress!")
            
        msg, sdp, is_answer = self.invite_state.process_incoming(msg)
        
        if self.invite_state.is_finished():
            self.invite_state = None
        elif self.invite_was_clogged and not self.invite_state.is_clogged():
            self.invite_was_clogged = False
            
            # An InviteClient only gets clogged after unreliable offers, with
            # RPR-s it won't happen. An InviteServer only gets clogged after
            # sending an RPR. When it gets unclogged, it received a PRACK, and
            # if there's an offer in it, that's the fucked up PRACK offer case.
            # Thanks to them being unrejectable, we may need to reject pending
            # outgoing offers before we send the received one up.
            reject_pending_offers = self.invite_use_rpr and sdp and not is_answer

            self.invite_unclogged(reject_pending_offers)
            
        return msg, sdp, is_answer


    def invite_is_active(self):
        return self.invite_state is not None
        
        
    def invite_is_clogged(self):
        return self.invite_was_clogged
        
        
    def invite_is_session_established(self):
        return self.invite_state.is_session_established()
        

    def invite_is_session_pracking(self):
        return self.invite_state.is_session_pracking()


class UpdateHelper:
    def __init__(self):
        self.update_state = None
        
                
    def update_new(self, is_outgoing):
        if self.update_state:
            raise Exception("Update already in progress!")
            
        self.update_state = dict(is_outgoing=is_outgoing, request=None)
        
        
    def update_outgoing(self, msg, sdp, is_answer):
        if not self.update_state:
            raise Exception("Update not in progress!")
            
        if self.update_state["is_outgoing"]:
            if self.update_state["request"]:
                raise Exception("Update was already sent!")
                
            assert not is_answer
            add_sdp(msg, sdp)

            self.logger.info("Sending message: UPDATE with offer")
            self.send_request(msg, None)
            self.update_state["request"] = msg
        else:
            if not self.update_state["request"]:
                raise Exception("Update was not yet received!")
            
            assert is_answer
            if sdp:
                add_sdp(msg, sdp)
                self.logger.info("Sending message: UPDATE response with answer")
            else:
                self.logger.info("Sending message: UPDATE response with rejection")
                
            self.send_response(msg, self.update_state["request"])
            self.update_state = None
            
            
    def update_incoming(self, msg):
        if not self.update_state:
            raise Exception("Update not in progress!")
            
        if self.update_state["is_outgoing"]:
            if not self.update_state["request"]:
                raise Exception("Update request was not sent, but now receiving one!")
                
            if not msg["is_response"]:
                self.logger.warning("Rejecting incoming UPDATE because one was already sent!")
                res = dict(status=Status(419))
                self.send_response(res, msg)
                return None, None, None
        
            self.update_state = None
        
            if msg["status"].code == 200:
                self.logger.info("Processing message: UPDATE response with answer")
                sdp = get_sdp(msg)
                return msg, sdp, True
            else:
                self.logger.info("Processing message: UPDATE response with rejection")
                return msg, None, True
        else:
            if self.update_state["request"]:
                self.logger.warning("Update request was already received!")
                res = dict(status=Status(419))
                self.send_response(res, msg)
                return None, None, None
                
            if msg["is_response"]:
                self.logger.warning("Got an update response without sending a request!")
                return None, None, None
                
            self.update_state["request"] = msg
                
            self.logger.info("Processing message: UPDATE request with offer")
            sdp = get_sdp(msg)
            return msg, sdp, False


    def update_outgoing_auto(self, sdp, is_answer):
        if not is_answer:
            self.update_new(is_outgoing=True)
            self.update_outgoing(dict(method="UPDATE"), sdp, is_answer)
        else:
            code = 200 if sdp else 488
            self.update_outgoing(dict(status=Status(code)), sdp, is_answer)


    def update_incoming_auto(self, msg):
        if not msg["is_response"]:
            self.update_new(is_outgoing=False)
            msg, sdp, is_answer = self.update_incoming(msg)
        else:
            msg, sdp, is_answer = self.update_incoming(msg)
            
        return msg, sdp, is_answer


AllocatedMedia = namedtuple("AllocatedMedia", "mgw_sid local_addr")

# Supposed to be a side class of an Endpoint
class SessionHelper:
    def __init__(self):
        self.allocated_media = []
        
        # These are not Loggable (yet)
        self.sdp_builder = SdpBuilder()
        self.sdp_parser = SdpParser()


    def media_leg_notified(self, type, params):
        raise NotImplementedError()
                
        
    def flatten_formats(self, formats, pt_key):
        return { f[pt_key]: (f["encoding"], f["clock"], f["encp"], f["fmtp"]) for f in formats }


    def refresh_local_media(self):
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
                ml.event_slot.plug(self.media_leg_notified)
            
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
            if session.is_reject():
                self.refresh_local_media()  # deallocate media addresses
            else:
                self.add_mgw_affinities(session)
                self.realize_local_media()
        else:
            self.add_mgw_affinities(session)
        
        return session
            
    
    def process_outgoing_session(self, session):
        if not session:
            return None, None
            
        is_answer = session.is_answer()
            
        self.leg.session_state.set_ground_session(session)
        self.refresh_local_media()

        if session.is_accept():
            self.realize_local_media()

        sdp = self.sdp_builder.build(session) if not session.is_reject() else None
        
        return sdp, is_answer
        
