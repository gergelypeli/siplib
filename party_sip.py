from weakref import WeakValueDictionary, proxy

from format import Status, TargetDialog, Parser
from party import Endpoint
from party_sip_helpers import InviteHelper, UpdateHelper, SessionHelper
from sdp import Session
from log import Loggable

import zap


class SipEndpoint(Endpoint, InviteHelper, UpdateHelper, SessionHelper):
    DOWN = "DOWN"
    DIALING_IN = "DIALING_IN"
    DIALING_OUT = "DIALING_OUT"
    DIALING_IN_RINGING = "DIALING_IN_RINGING"
    DIALING_OUT_RINGING = "DIALING_OUT_RINGING"
    UP = "UP"
    DISCONNECTING_OUT = "DISCONNECTING_OUT"

    DEFAULT_ALLOWED_METHODS = { "INVITE", "CANCEL", "ACK", "PRACK", "BYE", "UPDATE", "REFER" }

    USE_RPR = True


    def __init__(self, manager, dialog):
        Endpoint.__init__(self)
        InviteHelper.__init__(self, use_rpr=True)
        UpdateHelper.__init__(self)
        SessionHelper.__init__(self)

        self.manager = manager
        self.state = self.DOWN
        self.pending_actions = []

        self.dialog = dialog
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
        
        
    def add_abilities(self, msg):
        msg.setdefault("allow", set()).update(self.DEFAULT_ALLOWED_METHODS)
        msg.setdefault("supported", set()).add("norefersub")
        return msg
        

    def send_request(self, request, related=None):
        self.dialog.send_request(self.add_abilities(request), related)


    def send_response(self, response, related):
        self.dialog.send_response(self.add_abilities(response), related)


    def may_finish(self):
        self.clear_local_media()
        
        Endpoint.may_finish(self)


    def media_leg_notified(self, type, params, mli):
        self.forward(dict(params, type=type))
        
        
    def invite_unclogged(self, reject_pending_offer_actions):
        if reject_pending_offer_actions:
            pas = []
        
            for action in self.pending_actions:
                session = action.get("session")
            
                if session:
                    self.forward(dict(type="session", session=Session.make_reject()))
                
                    if action["type"] == "session":
                        continue
                
                    action["session"] = None
                
                pas.append(action)
            
            self.pending_actions = pas
        
        if self.pending_actions:
            self.logger.info("Invite unclogged, will retry postponed actions.")
            zap.time_slot(0).plug(self.do, action=None)
        
        
    def make_message(self, action, **kwargs):
        return kwargs
        
        
    def make_action(self, msg, **kwargs):
        return kwargs
        

    def hop_selected(self, hop):
        self.dst["hop"] = hop
        self.logger.debug("Retrying dial with resolved hop")
        self.do(None)


    def try_update(self, action, sdp, is_answer):
        # Decide if an UPDATE would be appropriate to send a session
        
        # Session queries can't be sent with UPDATE.
        if not sdp and not is_answer:
            return False
        
        if self.update_is_active() or (self.invite_is_active() and self.invite_is_session_established()):
            if not is_answer:
                self.update_new(is_outgoing=True)
                msg = self.make_message(action, method="UPDATE")
            else:
                msg = self.make_message(action, status=Status(200 if sdp else 488))

            self.update_outgoing(msg, sdp, is_answer)
                
            return True
        else:
            return False
            

    def do(self, action):
        type = action["type"] if action else None
        
        if not type:
            action = self.pending_actions.pop(0)
            type = action["type"]
            self.logger.info("Retrying pending %s action." % type)
        elif self.invite_is_clogged() or self.pending_actions:
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
        sdp, is_answer = self.process_outgoing_session(session)
        
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
                
                msg = self.make_message(action, method="INVITE")
                #if "alert_info" in self.dst:
                #    req["alert_info"] = self.dst["alert_info"]
                
                self.invite_outgoing(msg, sdp, is_answer)
                self.change_state(self.DIALING_OUT)
                
                return

        elif self.state in (self.DIALING_OUT, self.DIALING_OUT_RINGING):
            if type == "hangup":
                # TODO: technically we must first wait for at least a provisional
                # answer before cancelling, because otherwise the CANCEL may
                # overtake the invite. This is actually in the RFC. But that
                # would be too complex for now.
                
                msg = self.make_message(action, method="CANCEL")
                self.invite_outgoing(msg)
                self.change_state(self.DISCONNECTING_OUT)
                return
                
            elif type == "session":
                used_update = self.try_update(action, sdp, is_answer)
                if used_update:
                    return
                    
                if self.invite_is_session_pracking():
                    msg = self.make_message(action, method="PRACK")
                else:
                    # The invite was clogged until we got a final answer, so it must be OK
                    msg = self.make_message(action, method="ACK")
                    
                self.invite_outgoing(msg, sdp, is_answer)
                
                if not self.invite_is_active():
                    self.change_state(self.UP)
                        
                return

        elif self.state in (self.DIALING_IN, self.DIALING_IN_RINGING):
            # NOTE: it may happen that we got a PRACK offer, but before we
            # could generate the answer, the call is accepted, and we generate the INVITE
            # response. Then the INVITE will complete before the PRACK. This does
            # not seem to be illegal, only as fucked up as PRACK offers in general.
            
            already_ringing = (self.state == self.DIALING_IN_RINGING)

            if session:
                if self.try_update(action, sdp, is_answer):
                    # Rip the SDP from the response, and use an UPDATE instead
                    session = None
                    if type == "session":
                        return
                elif self.invite_is_session_pracking():
                    # Rip the SDP from the response, and use a PRACK response instead
                    # FIXME: The funny thing is we may send a reject in this 200...
                    msg = self.make_message(action, status=Status(200, "Not Happy"))
                    self.invite_outgoing(msg, sdp, is_answer)
                    
                    session = None
                    if type == "session":
                        return
                else:
                    # Just send the session in the INVITE response
                    if not sdp and is_answer:
                        # We must reject the offer, so reject the call, too
                        msg = self.make_message(action, status=Status(488))
                        self.invite_outgoing(msg, None, None)
                        # The transactions will catch the ACK
                        self.change_state(self.DOWN)
                        self.may_finish()
                        return
                
            if type == "session":
                msg = self.make_message(action, status=Status(180 if already_ringing else 183))
                self.invite_outgoing(msg, sdp, is_answer)
                return
                
            elif type == "ring":
                msg = self.make_message(action, status=Status(180))
                self.invite_outgoing(msg, sdp, is_answer)

                if not already_ringing:
                    self.change_state(self.DIALING_IN_RINGING)
                    
                return
                    
            elif type == "accept":
                msg = self.make_message(action, status=Status(200))
                self.invite_outgoing(msg, sdp, is_answer)
                # Wait for the ACK before changing state
                return

            elif type == "reject":
                msg = self.make_message(action, status=action["status"])
                self.invite_outgoing(msg, None, None)
                # The transactions will catch the ACK
                self.change_state(self.DOWN)
                self.may_finish()
                return

        elif self.state == self.UP:
            # Re-INVITE stuff
            
            if type == "session":
                # TODO: to be precise, we may need to do all the RPR related stuff
                # here, too, including sending UPDATE-s if the reinvite completed
                # and "early" session, or a PRACK offer was received. All of these
                # are fucked up for re-INVITE-s, but who knows?

                if self.try_update(action, sdp, is_answer):
                    return
                else:
                    if not self.invite_is_active():
                        self.invite_new(is_outgoing=True)
                        msg = self.make_message(action, method="INVITE")
                    elif not sdp and not is_answer:
                        # FIXME: A session query with and established, but unfinished INVITE
                        # cannot be sent. We're fucked here.
                        pass
                    elif not self.invite_is_outgoing():
                        code = 488 if is_answer and not sdp else 200
                        msg = self.make_message(action, status=Status(code))
                    else:
                        msg = self.make_message(action, method="ACK")
                        
                    self.invite_outgoing(msg, sdp, is_answer)
                    return
        
            elif type == "tone":
                if self.leg.media_legs and action.get("name"):
                    self.leg.media_legs[0].notify("tone", dict(name=action["name"]))
                    
                return

            elif type == "hangup":
                msg = self.make_message(action, method="BYE")
                self.send_request(msg)
                self.change_state(self.DISCONNECTING_OUT)
                return
                
            elif type == "transfer":
                self.process_transfer(action)
                return
            
        raise Exception("Weird thing to do %s in state %s!" % (type, self.state))


    def process_update(self, msg):
        # Process or reject incoming UPDATE-s
        is_response = msg["is_response"]
        
        if self.invite_is_active() and not self.invite_is_session_established():
            if not is_response:
                self.logger.warning("UPDATE request while INVITE session not established!")
                self.send_response(dict(status=Status(400)), msg)
            else:
                self.logger.warning("UPDATE response while INVITE session not established, WTF?")
        else:
            if not is_response:
                self.update_new(is_outgoing=False)

            msg, sdp, is_answer = self.update_incoming(msg)
            session = self.process_incoming_sdp(sdp, is_answer)
    
            if session:
                action = self.make_action(msg, type="session", session=session)
                self.forward(action)


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
            if method == "INVITE":
                src = dict(msg, type="sip")
                ctx = {}
                
                self.invite_new(is_outgoing=False)
                msg, sdp, is_answer = self.invite_incoming(msg)
                
                if not self.invite_is_active():
                    # May happen with 100rel support conflict
                    self.logger.error("Couldn't receive INVITE, finishing.")
                    self.may_finish()
                    return
                
                session = self.process_incoming_sdp(sdp, is_answer)
                
                self.change_state(self.DIALING_IN)
                
                action = self.make_action(msg,
                    type="dial",
                    call_info=self.get_call_info(),
                    src=src,
                    ctx=ctx,
                    session=session
                )
                
                self.forward(action)
                return
                
        elif self.state in (self.DIALING_IN, self.DIALING_IN_RINGING):
            if method in ("CANCEL", "ACK", "NAK", "PRACK"):
                if is_response:
                    self.logger.warning("A %s response, WTF?" % method)
                    return
                
                msg, sdp, is_answer = self.invite_incoming(msg)
                
                if method == "CANCEL":
                    self.change_state(self.DOWN)
                    action = self.make_action(msg, type="hangup")
                    self.forward(action)
                    self.may_finish()
                elif method == "ACK":
                    session = self.process_incoming_sdp(sdp, is_answer)
                    self.change_state(self.UP)
                    
                    if session:
                        action = self.make_action(msg, type="session", session=session)
                        self.forward(action)
                elif method == "NAK":
                    self.send_request(dict(method="BYE"))  # required behavior
                    self.change_state(self.DISCONNECTING_OUT)
                elif method == "PRACK":
                    session = self.process_incoming_sdp(sdp, is_answer)

                    if session:
                        action = self.make_action(msg, type="session", session=session)
                        self.forward(action)
                    
                return
            elif method == "UPDATE":
                self.process_update(msg)
                return
                
        elif self.state in (self.DIALING_OUT, self.DIALING_OUT_RINGING):
            if method in ("INVITE", "ACK", "NAK", "PRACK"):
                # Of course, there will be no ACK or NAK responses, just for completeness
                
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
                                action = self.make_action(msg, type="session", session=session)
                                self.forward(action)
                        
                        else:
                            self.change_state(self.DIALING_OUT_RINGING)
                            action = self.make_action(msg, type="ring", session=session)
                            self.forward(action)
                        
                        return

                    elif status.code == 183:
                        if session:
                            action = self.make_action(msg, type="session", session=session)
                            self.forward(action)
                        
                        return
                    
                    elif status.code >= 300:
                        if not self.invite_is_active():
                            # Transaction now acked, invite should be finished now
                            self.change_state(self.DOWN)
                            action = self.make_action(msg, type="reject", status=status)
                            self.forward(action)
                            self.may_finish()
                        
                        return

                    elif status.code >= 200:
                        if not self.invite_is_active():
                            self.change_state(self.UP)
                        
                        action = self.make_action(msg, type="accept", session=session)
                        self.forward(action)
                        return
                elif method == "PRACK":
                    return # Nothing meaningful should arrive in PRACK responses
            elif method == "UPDATE":
                self.process_update(msg)
                return

        elif self.state == self.UP:
            if method in ("INVITE", "ACK", "NAK", "PRACK"):
                # Re-INVITE stuff
            
                if method == "INVITE" and not is_response:
                    self.invite_new(is_outgoing=False)
            
                msg, sdp, is_answer = self.invite_incoming(msg)
                session = self.process_incoming_sdp(sdp, is_answer)
            
                if session:
                    action = self.make_action(msg, type="session", session=session)
                    self.forward(action)
                
                return
            elif method == "UPDATE":
                self.process_update(msg)
                return
            elif method == "REFER":
                if is_response:
                    self.logger.warning("Ignoring REFER response!")
                    return
                
                refer_to = msg.get("refer_to")
                if not refer_to:
                    self.logger.warning("No Refer-To header!")
                    self.send_response(dict(status=Status(400)), msg)
                    return
                    
                replaces = refer_to.uri.headers.get("replaces")
                if not replaces:
                    self.logger.info("Blind transfer to %s." % (refer_to,))
                    other = None
                    tid = self.ground.make_transfer("blind")
                    src = {
                        'type': "sip",
                        'from': msg["from"],
                        'to': refer_to
                    }
                else:
                    td = TargetDialog.parse(Parser(replaces))
                    
                    # Moronic RFC 3891:
                    # In other words, the to-tag parameter is compared to the local tag,
                    # and the from-tag parameter is compared to the remote tag.
                    local_tag = td.params["to-tag"]
                    remote_tag = td.params["from-tag"]
                    call_id = td.call_id
                
                    other = self.manager.get_endpoint(local_tag, remote_tag, call_id)
                
                    if not other:
                        self.logger.warning("No target dialog found, l=%s, r=%s, c=%s" % (local_tag, remote_tag, call_id))
                        self.send_response(dict(status=Status(404)), msg)
                        return
                        
                    self.logger.info("Attended transfer to SIP endpoint %s." % local_tag)
                    tid = self.ground.make_transfer("attended")
                    src = None
                    
                refer_sub = "false" if msg.get("refer_sub") == "false" else "true"
                self.send_response(dict(status=Status(200), refer_sub=refer_sub), msg)
                    
                if refer_sub != "false":
                    notify = dict(
                        method="NOTIFY",
                        event="refer",
                        subscription_state="terminated;reason=noresource",
                        content_type="message/sipfrag",
                        body="SIP/2.0 200 OK".encode("utf8")
                    )
                
                    self.send_request(notify)

                if not other:
                    # blind
                    action = self.make_action(msg, type="transfer", transfer_id=tid, call_info=self.call_info, ctx={}, src=src)
                    self.forward(action)
                else:
                    # attended
                    action = other.make_action(msg, type="transfer", transfer_id=tid)
                    other.forward(action)

                    action = self.make_action(msg, type="transfer", transfer_id=tid)
                    self.forward(action)

                return
            elif method == "NOTIFY":
                if is_response:
                    # Ahhh, this is the response for a REFER-initiated NOTIFY
                    return
            elif method == "BYE":
                if not is_response:
                    self.send_response(dict(status=Status(200, "OK")), msg)
                    self.change_state(self.DOWN)
                    action = self.make_action(msg, type="hangup")
                    self.forward(action)
                    self.may_finish()
                    return
        elif self.state == self.DISCONNECTING_OUT:
            if method == "BYE":
                if is_response:
                    self.change_state(self.DOWN)
                    self.may_finish()
                    return
                else:
                    self.logger.debug("Mutual BYE, finishing immediately.")
                    self.send_response(dict(status=Status(200)), msg)
                    self.change_state(self.DOWN)
                    self.may_finish()
                    return
                
            elif method == "INVITE":
                if is_response:
                    self.logger.debug("Got late INVITE response: %s" % (status,))
                    # This was ACKed by the transaction layer
                    self.change_state(self.DOWN)
                    self.may_finish()
                    return
                
            elif method == "CANCEL":
                if is_response:
                    self.logger.debug("Got late CANCEL response: %s" % (status,))
                    return

            elif method == "NOTIFY":
                if is_response:
                    self.logger.debug("Got late NOTIFY response: %s" % (status,))
                    return
                
        if msg or sdp:
            raise Exception("Weird message %s %s in state %s!" % (method, "response" if is_response else "request", self.state))


class SipManager(Loggable):
    def __init__(self):
        Loggable.__init__(self)
        
        self.endpoints_by_local_tag = WeakValueDictionary()
        
        
    def make_endpoint(self, dialog):
        endpoint = SipEndpoint(proxy(self), dialog)
        self.endpoints_by_local_tag[dialog.get_local_tag()] = endpoint
        
        return endpoint

        
    def get_endpoint(self, local_tag, remote_tag, call_id):
        endpoint = self.endpoints_by_local_tag.get(local_tag)
        if endpoint:
            dialog = endpoint.get_dialog()
            
            if dialog.get_remote_tag() == remote_tag and dialog.get_call_id() == call_id:
                return endpoint
            
        return None
