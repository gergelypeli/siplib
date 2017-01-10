from weakref import WeakValueDictionary, proxy

from format import Status, TargetDialog, Parser
from party import Endpoint
from endpoint_sip_helpers import InviteUpdateHelper, SessionHelper
from log import Loggable


class SipEndpoint(Endpoint, InviteUpdateHelper, SessionHelper):
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
        InviteUpdateHelper.__init__(self, use_rpr=True)
        SessionHelper.__init__(self)

        self.manager = manager
        self.state = self.DOWN

        self.dialog = dialog
        self.dialog.request_slot.plug(self.process_request)
        self.dialog.response_slot.plug(self.process_response)
        

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
        
        
    def make_message(self, action, **kwargs):
        return kwargs
        
        
    def make_action(self, msg, **kwargs):
        return kwargs
        

    def hop_selected(self, hop, action):
        self.dst["hop"] = hop
        self.logger.debug("Retrying dial with resolved hop")
        self.do(action)


    def do(self, action):
        if action["type"] in ("session", "ring", "accept", "reject"):
            action = self.iu_check_action(action)
            
        if not action:
            return
            
        type = action["type"]
        
        if type == "dial" and not self.dst.get("hop"):
            self.logger.info("Dialing out without hop, postponing until resolved.")
            to = self.dst.get("to")
            uri = self.dst.get("uri") or to.uri
            route = self.dst.get("route")
            next_uri = route[0].uri if route else uri
            self.ground.select_hop_slot(next_uri).plug(self.hop_selected, action=action)
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
                #self.invite_new(is_outgoing=True)
                
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
                self.try_sending_session_by_anything_but_invite_response(action, sdp, is_answer)
                
                # This might have sent an ACK
                if not self.invite_is_active():
                    self.change_state(self.UP)
                        
                return

        elif self.state in (self.DIALING_IN, self.DIALING_IN_RINGING):
            # NOTE: it may happen that we got a PRACK offer, but before we
            # could generate the answer, the call is accepted, and we generate the INVITE
            # response. Then the INVITE will complete before the PRACK. This does
            # not seem to be illegal, only as fucked up as PRACK offers in general.
            
            if session:
                ok = self.try_sending_session_by_anything_but_invite_response(action, sdp, is_answer)
                
                if ok:
                    # Rip the SDP from the response
                    if type == "session":
                        return
                    else:
                        sdp = None
                        is_answer = None

                # Otherwise we'll use an INVITE response for the SDP

            status = None
            already_ringing = (self.state == self.DIALING_IN_RINGING)

            # Session rejects automatically screw the whole call, regardless of the action
            if not sdp and is_answer:
                status = Status(488)
            elif type == "session":
                status = Status(180 if already_ringing else 183)
            elif type == "ring":
                status = Status(180)
                if not already_ringing:
                    self.change_state(self.DIALING_IN_RINGING)
            elif type == "accept":
                status = Status(200)
                # Wait for the ACK before changing state
            elif type == "reject":
                status = action["status"] or Status(500)
            else:
                raise Exception("Unknown action type: %s!" % type)
                
            msg = self.make_message(action, status=status)
            self.invite_outgoing(msg, sdp, is_answer)
            
            if status.code >= 300:
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

                ok = self.try_sending_session_by_anything_but_invite_response(action, sdp, is_answer)
                if ok:
                    return
                    
                status = Status(488 if not sdp else 200)
                msg = self.make_message(action, status=status)
                self.invite_outgoing(msg, sdp, is_answer)
                return
        
            elif type == "tone":
                if self.leg.media_legs and action.get("name"):
                    self.leg.media_legs[0].notify("tone", dict(name=action["name"]))
                    
                return

            elif type == "hangup":
                reason = action.get("reason")
                msg = self.make_message(action, method="BYE", reason=[reason] if reason else None)
                self.send_request(msg)
                self.change_state(self.DISCONNECTING_OUT)
                return
                
            elif type == "transfer":
                self.process_transfer(action)
                return
            
        raise Exception("Weird thing to do %s in state %s!" % (type, self.state))


    def process_session(self, msg, sdp, is_answer):
        session = self.process_incoming_sdp(sdp, is_answer)

        if session:
            action = self.make_action(msg, type="session", session=session)
            self.forward(action)


    def process_request(self, request):
        method = request["method"]
        self.logger.debug("Processing request %s" % method)

        # Note: must change state before forward, because that can generate
        # a reverse action which should only be processed with the new state!
        
        if self.state == self.DOWN:
            if method == "INVITE":
                src = dict(request, type="sip")
                ctx = {}
                
                #self.invite_new(is_outgoing=False)
                request, sdp, is_answer = self.invite_incoming(request)
                
                if not self.invite_is_active():
                    # May happen with 100rel support conflict
                    self.logger.error("Couldn't receive INVITE, finishing.")
                    self.may_finish()
                    return
                
                session = self.process_incoming_sdp(sdp, is_answer)
                
                self.change_state(self.DIALING_IN)
                
                action = self.make_action(request,
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
                request, sdp, is_answer = self.invite_incoming(request)
                
                if not request:
                    return
                elif method == "CANCEL":
                    self.change_state(self.DOWN)
                    action = self.make_action(request, type="hangup")
                    self.forward(action)
                    self.may_finish()
                elif method == "ACK":
                    session = self.process_incoming_sdp(sdp, is_answer)
                    self.change_state(self.UP)
                    
                    if session:
                        action = self.make_action(request, type="session", session=session)
                        self.forward(action)
                elif method == "NAK":
                    self.send_request(dict(method="BYE"))  # required behavior
                    self.change_state(self.DISCONNECTING_OUT)
                elif method == "PRACK":
                    session = self.process_incoming_sdp(sdp, is_answer)

                    if session:
                        action = self.make_action(request, type="session", session=session)
                        self.forward(action)
                    
                return
            elif method == "UPDATE":
                request, sdp, is_answer = self.update_incoming(request)
                self.process_session(request, sdp, is_answer)
                return
                
        elif self.state in (self.DIALING_OUT, self.DIALING_OUT_RINGING):
            if method in ("INVITE", "ACK", "NAK", "PRACK"):
                self.logger.warning("A %s request, WTF?" % method)
                return
            elif method == "UPDATE":
                request, sdp, is_answer = self.update_incoming(request)
                self.process_session(request, sdp, is_answer)
                return

        elif self.state == self.UP:
            if method in ("INVITE", "ACK", "NAK", "PRACK"):
                request, sdp, is_answer = self.invite_incoming(request)
                self.process_session(request, sdp, is_answer)
                return
            elif method == "UPDATE":
                request, sdp, is_answer = self.update_incoming(request)
                self.process_session(request, sdp, is_answer)
                return
            elif method == "REFER":
                refer_to = request.get("refer_to")
                if not refer_to:
                    self.logger.warning("No Refer-To header!")
                    self.send_response(dict(status=Status(400)), request)
                    return
                    
                replaces = refer_to.uri.headers.get("replaces")
                if not replaces:
                    self.logger.info("Blind transfer to %s." % (refer_to,))
                    other = None
                    tid = self.ground.make_transfer("blind")
                    src = {
                        'type': "sip",
                        'from': request["from"],
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
                        self.send_response(dict(status=Status(404)), request)
                        return
                        
                    self.logger.info("Attended transfer to SIP endpoint %s." % local_tag)
                    tid = self.ground.make_transfer("attended")
                    src = None
                    
                refer_sub = "false" if request.get("refer_sub") == "false" else "true"
                self.send_response(dict(status=Status(200), refer_sub=refer_sub), request)
                    
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
                    action = self.make_action(request, type="transfer", transfer_id=tid, call_info=self.call_info, ctx={}, src=src)
                    self.forward(action)
                else:
                    # attended
                    action = other.make_action(request, type="transfer", transfer_id=tid)
                    other.forward(action)

                    action = self.make_action(request, type="transfer", transfer_id=tid)
                    self.forward(action)

                return
            elif method == "BYE":
                self.send_response(dict(status=Status(200, "OK")), request)
                self.change_state(self.DOWN)
                action = self.make_action(request, type="hangup")
                self.forward(action)
                self.may_finish()
                return
        elif self.state == self.DISCONNECTING_OUT:
            if method == "BYE":
                self.logger.debug("Mutual BYE, finishing immediately.")
                self.send_response(dict(status=Status(200)), request)
                self.change_state(self.DOWN)
                self.may_finish()
                return

        if request or sdp:
            raise Exception("Weird %s request in state %s!" % (method, self.state))


    def process_response(self, response):
        method = response["method"]
        status = response.get("status")
        self.logger.debug("Processing response %d %s" % (status.code, method))

        # Note: must change state before forward, because that can generate
        # a reverse action which should only be processed with the new state!
        
        if self.state == self.DOWN:
            pass

        elif self.state in (self.DIALING_IN, self.DIALING_IN_RINGING):
            if method in ("CANCEL", "ACK", "NAK", "PRACK"):
                self.logger.warning("A %s response, WTF?" % method)
                return
            elif method == "UPDATE":
                response, sdp, is_answer = self.update_incoming(response)
                self.process_session(response, sdp, is_answer)
                return
                
        elif self.state in (self.DIALING_OUT, self.DIALING_OUT_RINGING):
            if method in ("INVITE", "ACK", "NAK", "PRACK"):
                # Of course, there will be no ACK or NAK responses, just for completeness
                    
                response, sdp, is_answer = self.invite_incoming(response)
                if not response:
                    return
                
                session = self.process_incoming_sdp(sdp, is_answer)

                if method == "INVITE":
                    if status.code == 180:
                        if self.state == self.DIALING_OUT_RINGING:
                            if session:
                                action = self.make_action(response, type="session", session=session)
                                self.forward(action)
                        
                        else:
                            self.change_state(self.DIALING_OUT_RINGING)
                            action = self.make_action(response, type="ring", session=session)
                            self.forward(action)
                        
                        return

                    elif status.code == 183:
                        if session:
                            action = self.make_action(response, type="session", session=session)
                            self.forward(action)
                        
                        return
                    
                    elif status.code >= 300:
                        if not self.invite_is_active():
                            # Transaction now acked, invite should be finished now
                            self.change_state(self.DOWN)
                            action = self.make_action(response, type="reject", status=status)
                            self.forward(action)
                            self.may_finish()
                        
                        return

                    elif status.code >= 200:
                        if not self.invite_is_active():
                            self.change_state(self.UP)
                        
                        action = self.make_action(response, type="accept", session=session)
                        self.forward(action)
                        return
                elif method == "PRACK":
                    return # Nothing meaningful should arrive in PRACK responses
            elif method == "UPDATE":
                response, sdp, is_answer = self.update_incoming(response)
                self.process_session(response, sdp, is_answer)
                return

        elif self.state == self.UP:
            if method in ("INVITE", "ACK", "NAK", "PRACK"):
                response, sdp, is_answer = self.invite_incoming(response)
                self.process_session(response, sdp, is_answer)
                return
            elif method == "UPDATE":
                response, sdp, is_answer = self.update_incoming(response)
                self.process_session(response, sdp, is_answer)
                return
            elif method == "REFER":
                self.logger.warning("Ignoring REFER response!")
                return
            elif method == "NOTIFY":
                self.logger.warning("Ignoring NOTIFY response!")
                return
        elif self.state == self.DISCONNECTING_OUT:
            if method == "BYE":
                self.change_state(self.DOWN)
                self.may_finish()
                return
                
            elif method == "INVITE":
                self.logger.debug("Got late INVITE response: %s" % (status,))
                # This was ACKed by the transaction layer
                self.change_state(self.DOWN)
                self.may_finish()
                return
                
            elif method == "CANCEL":
                self.logger.debug("Got late CANCEL response: %s" % (status,))
                return

            elif method == "NOTIFY":
                self.logger.debug("Got late NOTIFY response: %s" % (status,))
                return
                
        if response or sdp:
            raise Exception("Weird %s response in state %s!" % (method, self.state))


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
