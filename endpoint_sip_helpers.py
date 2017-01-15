from collections import namedtuple

from format import Status, Sip
from endpoint_sip_invite import InviteClientState, InviteServerState
from endpoint_sip_update import UpdateClientState, UpdateServerState
from sdp import SdpBuilder, SdpParser, STATIC_PAYLOAD_TYPES, Session
from ground import SessionState


class InviteUpdateHelper:
    def __init__(self, use_rpr):
        self.invite_use_rpr = use_rpr

        self.invite_state = None
        self.invite_is_clogged = False

        self.update_state = None

        self.iu_pending_actions = []


    def invite_outgoing(self, msg, sdp, is_answer):
        method = msg.method  # Should only be set for requests
        
        if method == "INVITE":
            if self.invite_state:
                raise Exception("INVITE request while another INVITE is in progress!")
                
            self.invite_state = InviteClientState(self.invite_use_rpr)
            self.invite_state.message_slot.instaplug(self.send)
            self.invite_state.set_oid(self.oid.add("invite-client"))
        else:
            if not self.invite_state:
                raise Exception("INVITE message while INVITE not in progress!")
                
        self.invite_state.process_outgoing(msg, sdp, is_answer)
        
        if self.invite_state.is_finished():
            self.invite_state = None
            self.invite_is_clogged = False
            self.invite_unclogged()
        elif not self.invite_is_clogged and self.invite_state.is_clogged():
            self.logger.debug("Invite is clogged, may postpone future actions.")
            self.invite_is_clogged = True


    def invite_incoming(self, msg):
        is_response = msg.is_response
        
        if not is_response and msg.method == "INVITE":
            if self.invite_state:
                self.logger.warning("INVITE request while INVITE is in progress!")
                self.send(Sip.response(status=Status(400)), msg)
                return None, None, None
                
            self.invite_state = InviteServerState(self.invite_use_rpr)
            self.invite_state.message_slot.instaplug(self.send)
            self.invite_state.set_oid(self.oid.add("invite-server"))
        else:
            if not self.invite_state:
                if not is_response:
                    self.logger.warning("INVITE-like request while INVITE not in progress!")
                    self.send(Sip.response(status=Status(400)), msg)
                    return None, None, None
                else:
                    self.logger.warning("INVITE-like response while INVITE not in progress!")
                    return None, None, None
                    
        msg, sdp, is_answer = self.invite_state.process_incoming(msg)
        
        if self.invite_state.is_finished():
            self.invite_state = None
            self.invite_is_clogged = False
            self.invite_unclogged()
        elif self.invite_is_clogged and not self.invite_state.is_clogged():
            self.invite_is_clogged = False
            if self.invite_use_rpr and sdp and not is_answer:
                self.reject_pending_offers()
            self.invite_unclogged()
            
        return msg, sdp, is_answer


    def invite_is_active(self):
        return self.invite_state is not None
        
        
    def update_outgoing(self, msg, sdp, is_answer):
        method = msg.method  # Should only be set for requests

        if self.invite_state and not self.invite_state.is_session_established():
            raise Exception("UPDATE while INVITE session not established!")
            
        if method:
            if self.update_state:
                raise Exception("UPDATE request while another UPDATE is in progress!")
            
            self.update_state = UpdateClientState()
            self.update_state.set_oid(self.oid.add("update-client"))
            self.update_state.message_slot.instaplug(self.send)
        else:
            if not self.update_state:
                raise Exception("UPDATE response while no UPDATE is in progress!")
        
        self.update_state.process_outgoing(msg, sdp, is_answer)
        
        if self.update_state.is_finished():
            self.update_state = None
            
            
    def update_incoming(self, msg):
        is_response = msg.is_response
        
        if self.invite_state and not self.invite_state.is_session_established():
            if not is_response:
                self.logger.warning("UPDATE request while INVITE session not established!")
                self.send(Sip.response(status=Status(400)), msg)
                return None, None, None
            else:
                self.logger.warning("UPDATE response while INVITE session not established, WTF?")
                return None, None, None

        if not is_response:
            if self.update_state:
                self.logger.warning("UPDATE request while another UPDATE is in progress!")
                self.send(Sip.response(status=Status(400)), msg)
                return None, None, None

            # Create update server state
            self.update_state = UpdateServerState()
            self.update_state.set_oid(self.oid.add("update-server"))
            self.update_state.message_slot.instaplug(self.send)
        else:
            if not self.update_state:
                self.logger.warning("UPDATE response while no UPDATE is in progress!")
                return None, None, None
                
        msg, sdp, is_answer = self.update_state.process_incoming(msg)
    
        if self.update_state.is_finished():
            self.update_state = None
        
        return msg, sdp, is_answer
        

    def try_sending_session_by_anything_but_invite_response(self, action, sdp, is_answer):
        # Session queries can't be sent with UPDATE.
        if sdp or is_answer:
            if self.update_state or (self.invite_state and self.invite_state.is_session_established()):
                if not is_answer:
                    #self.update_new(is_outgoing=True)
                    msg = self.make_message(action, method="UPDATE")
                else:
                    msg = self.make_message(action, status=Status(200 if sdp else 488))

                self.update_outgoing(msg, sdp, is_answer)
                
                return True

        # OK, it must be an INVITE then
        if not self.invite_is_active():
            #self.invite_new(is_outgoing=True)
            msg = self.make_message(action, method="INVITE")
            self.invite_outgoing(msg, sdp, is_answer)
            return True
        elif not sdp and not is_answer:
            # FIXME: A session query with and established, but unfinished INVITE
            # cannot be sent. We're fucked here. This thing should have stayed in
            # the queue until this state lasts.
            return False
        elif not self.invite_state.is_outgoing:
            # Can we respond something?
            
            if self.invite_state.is_session_pracking():
                # FIXME: The funny thing is we may send a reject in this 200...
                msg = self.make_message(action, status=Status(200, "Not Happy"))
                self.invite_outgoing(msg, sdp, is_answer)
                return True
            else:
                # We're explicitly asked not to send INVITE responses here
                return False
        else:
            # Can we request something?
            if self.invite_state.is_session_pracking():
                msg = self.make_message(action, method="PRACK")
                self.invite_outgoing(msg, sdp, is_answer)
                return True
            else:
                # The invite was clogged until we got a final answer, so it must be OK
                msg = self.make_message(action, method="ACK")
                self.invite_outgoing(msg, sdp, is_answer)
                return True


    def reject_pending_offers(self):
        # An InviteClient only gets clogged after unreliable offers, with
        # RPR-s it won't happen. An InviteServer only gets clogged after
        # sending an RPR. When it gets unclogged, it received a PRACK, and
        # if there's an offer in it, that's the fucked up PRACK offer case.
        # Thanks to them being unrejectable, we may need to reject pending
        # outgoing offers before we send the received one up.
        pas = []
    
        for action in self.iu_pending_actions:
            session = action.get("session")
        
            if session:
                self.forward(dict(type="session", session=Session.make_reject()))
            
                if action["type"] == "session":
                    continue
            
                action["session"] = None
            
            pas.append(action)
        
        self.iu_pending_actions = pas


    def invite_unclogged(self):
        if self.iu_pending_actions:
            self.logger.info("Invite unclogged, will retry postponed actions.")
            actions = self.iu_pending_actions
            self.iu_pending_actions = []
            
            for action in actions:
                self.do(action)
                
            #zap.time_slot(0).plug(self.do, action=None)


    def iu_check_action(self, action):
        type = action["type"]
        session = action.get("session")
        
        # Damn, we can't handle this
        if session and session.is_query() and self.invite_is_active():
            self.iu_pending_actions.append(dict(type="session", session=session))
            
            if type == "session":
                return None
            else:
                action["session"] = None
            
        if self.invite_is_clogged:
            self.logger.info("Invite clogged, postponing %s action." % type)
            self.iu_pending_actions.append(action)
            return None

        return action


AllocatedMedia = namedtuple("AllocatedMedia", "mgw_sid local_addr cached_params")

# Supposed to be a side class of an Endpoint
class SessionHelper:
    def __init__(self):
        self.allocated_media = []
        
        # These are not Loggable (yet)
        self.sdp_builder = SdpBuilder()
        self.sdp_parser = SdpParser()


    def media_leg_notified(self, type, params, mli):
        raise NotImplementedError()
                

    def allocate_local_media(self, local_session):
        # Must be called for outgoing offers and accepts (before realizing media)
        local_channels = local_session["channels"] if local_session else []
        
        channel_count = len(local_channels)
        allocated_count = len(self.allocated_media)
        
        # Allocate
        for i in range(allocated_count, channel_count):
            local_channel = local_channels[i]
            ctype = local_channel["type"]
            mgw_affinity = local_channel.get("mgw_affinity")
            self.logger.debug("Allocating local media address for channel %d (%s@%s)" % (i, ctype, mgw_affinity))
            
            mgw_sid = self.ground.select_gateway_sid(ctype, mgw_affinity)
            local_addr = self.ground.allocate_media_address(mgw_sid)
            
            allocated_media = AllocatedMedia(mgw_sid, local_addr, {})
            self.allocated_media.append(allocated_media)


    def deallocate_local_media(self, local_session):
        # Must be called for incoming rejects, and after realizing media
        local_channels = local_session["channels"] if local_session else []
        
        channel_count = len(local_channels)
        allocated_count = len(self.allocated_media)
                
        # Deallocate
        for i in reversed(range(channel_count, allocated_count)):
            am = self.allocated_media.pop()
            self.logger.debug("Deallocating local media address for channel %d" % i)
            self.ground.deallocate_media_address(am.local_addr)


    def add_local_info(self, local_session):
        # Must be called for outgoing offers and accepts
        local_channels = local_session["channels"] if local_session else []
        
        for i, local_channel in enumerate(local_channels):
            am = self.allocated_media[i]
            local_channel["rtp_local_addr"] = am.local_addr
            
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

                    used_payload_types.add(pt)

                f["rtp_local_payload_type"] = pt
            
        
    def flatten_formats(self, formats, pt_key):
        return { f[pt_key]: (f["encoding"], f["clock"], f["encp"], f["fmtp"]) for f in formats }


    def realize_local_media(self, local_session, remote_session):
        # Must be called for all accepts
        # Allocating resources must precede it, and deallocating them must follow
        local_channels = local_session["channels"] if local_session else []
        remote_channels = remote_session["channels"] if remote_session else []
        
        if len(local_channels) != len(remote_channels):
            raise Exception("Channel count mismatch!")
            
        channel_count = len(local_channels)
        media_leg_count = self.leg.get_media_leg_count()

        # Create
        for i in range(media_leg_count, channel_count):
            self.logger.debug("Making media leg for channel %d" % i)
            am = self.allocated_media[i]
            
            ml = self.make_media_leg("net")
            ml.set_mgw(am.mgw_sid)
            self.leg.add_media_leg(ml)
            ml.event_slot.plug(self.media_leg_notified, mli=i)
            
        # Delete (currently can only happen on shutdown, not on session exchange)
        for i in reversed(range(channel_count, media_leg_count)):
            self.leg.remove_media_leg()
            
        # Modify
        for i in range(channel_count):
            lc = local_channels[i]
            rc = remote_channels[i]
            ml = self.leg.get_media_leg(i)
            am = self.allocated_media[i]
            
            params = {
                'local_addr': lc["rtp_local_addr"],
                'remote_addr': rc["rtp_remote_addr"],
                'send_formats': self.flatten_formats(rc["formats"], "rtp_remote_payload_type"),
                'recv_formats': self.flatten_formats(lc["formats"], "rtp_local_payload_type")
            }
            
            diff = {}
            for key, value in params.items():
                if value != am.cached_params.get(key):
                    diff[key] = value
            
            if not diff:
                self.logger.debug("No need to modify media leg %d." % i)
            else:
                self.logger.debug("Modifying media leg %d: %s" % (i, diff))
                am.cached_params.update(diff)
                ml.modify(diff)
            
            
    def add_remote_info(self, remote_session):
        remote_channels = remote_session["channels"]
        
        for i in range(len(self.allocated_media)):
            remote_channels[i]["mgw_affinity"] = self.allocated_media[i].mgw_sid


    def process_incoming_sdp(self, sdp, is_answer):
        if sdp is None and is_answer is None:
            return None  # no session to process
            
        remote_session = self.sdp_parser.parse(sdp, is_answer)
        result = self.leg.session_state.set_party_session(remote_session)
        
        if result in (SessionState.IGNORE_UNEXPECTED, SessionState.IGNORE_RESOLVED, SessionState.IGNORE_STALE):
            self.logger.info("Won't process incoming session: %s." % result)
            return #None
        elif result in (SessionState.REJECT_DUPLICATE, SessionState.REJECT_COLLIDING):
            self.logger.error("Can't process incoming session: %s!" % result)
            # TODO: let the offerer know if it was just a collision
            # FIXME: and now what?
            return #None
        
        local_session = self.leg.session_state.get_ground_session()
        
        if remote_session.is_offer():
            self.add_remote_info(remote_session)
        elif remote_session.is_accept():
            self.add_remote_info(remote_session)
            self.realize_local_media(local_session, remote_session)
            self.deallocate_local_media(local_session)
        elif remote_session.is_reject():
            self.deallocate_local_media(local_session)

        return remote_session
            
    
    def process_outgoing_session(self, local_session):
        # Results:
        #   None, None  - nothing to do
        #   sdp, False  - offer
        #   sdp, True   - accept
        #   None, False - query
        #   None, True  - reject
        if not local_session:
            return None, None
            
        result = self.leg.session_state.set_ground_session(local_session)
        
        if result in (SessionState.IGNORE_UNEXPECTED, SessionState.IGNORE_RESOLVED, SessionState.IGNORE_STALE):
            self.logger.info("Won't send outgoing session: %s." % result)
            return# None, None
        elif result in (SessionState.REJECT_DUPLICATE, SessionState.REJECT_COLLIDING):
            self.logger.warning("Can't send outgoing session: %s!" % result)
            # TODO: let the offerer know if it was just a collision
            action = dict(type="session", session=Session.make_reject())
            self.forward(action)
            return# None, None
        
        remote_session = self.leg.session_state.get_party_session()
        
        if local_session.is_offer():
            self.allocate_local_media(local_session)
            self.add_local_info(local_session)
        elif local_session.is_accept():
            self.allocate_local_media(local_session)
            self.add_local_info(local_session)
            self.realize_local_media(local_session, remote_session)
            self.deallocate_local_media(local_session)
        elif local_session.is_reject():
            pass

        sdp = self.sdp_builder.build(local_session)
        is_answer = local_session.is_accept() or local_session.is_reject()
        
        return sdp, is_answer


    def clear_local_media(self):
        self.logger.info("Clearing local media")
        
        fake_offer = Session.make_offer(channels=[])
        fake_accept = Session.make_accept(channels=[])
        
        self.leg.session_state.set_party_session(fake_offer)
        self.leg.session_state.set_ground_session(fake_accept)
        
        self.realize_local_media(fake_accept, fake_offer)
        self.deallocate_local_media(fake_accept)
