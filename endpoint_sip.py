from weakref import WeakValueDictionary, proxy

from format import Status, TargetDialog, Parser, Sip, Reason
from party import Endpoint
from sdp import Session
from endpoint_sip_helpers import SessionHelper
from endpoint_sip_iuc import InviteUpdateComplex
from log import Loggable


class SipEndpoint(Endpoint, SessionHelper):
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
        SessionHelper.__init__(self)

        self.manager = manager
        self.state = self.DOWN

        self.dialog = dialog
        self.dialog.message_slot.plug(self.process)
        
        self.iuc = InviteUpdateComplex(proxy(self), self.USE_RPR)
        

    def set_oid(self, oid):
        Endpoint.set_oid(self, oid)

        self.dialog.set_oid(oid.add("dialog"))
        self.iuc.set_oid(oid.add("iuc"))


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
        

    def send(self, msg):
        self.dialog.send(self.add_abilities(msg))


    def may_finish(self):
        self.clear_local_media()
        
        Endpoint.may_finish(self)


    def media_leg_notified(self, type, params, mli):
        self.forward(dict(params, type=type))
        
        
    def hop_selected(self, hop, action):
        self.dst["hop"] = hop
        self.logger.debug("Retrying dial with resolved hop")
        self.do(action)


    def do(self, action):
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
        if session:
            self.process_local_session(session)
        
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
                
                msg = Sip.request(method="INVITE")
                self.iuc.queue_session(session or Session.make_query())
                self.iuc.out_client(msg)
                self.change_state(self.DIALING_OUT)
                
                return
            elif type == "hangup":
                # This may happen after reporting a divert.
                self.logger.info("Hangup in DOWN state, ignoring.")
                return

        elif self.state in (self.DIALING_OUT, self.DIALING_OUT_RINGING):
            if type == "hangup":
                # TODO: technically we must first wait for at least a provisional
                # answer before cancelling, because otherwise the CANCEL may
                # overtake the invite. This is actually in the RFC. But that
                # would be too complex for now.
                
                msg = Sip.request(method="CANCEL")
                msg["reason"] = action.get("reason")
                self.iuc.out_client(msg)
                self.change_state(self.DISCONNECTING_OUT)
                return
                
            elif type == "session":
                self.iuc.out_session(session)
                
                # This might have sent an ACK
                if self.iuc.is_finished():
                    self.change_state(self.UP)
                        
                return

        elif self.state in (self.DIALING_IN, self.DIALING_IN_RINGING):
            # NOTE: it may happen that we got a PRACK offer, but before we
            # could generate the answer, the call is accepted, and we generate the INVITE
            # response. Then the INVITE will complete before the PRACK. This does
            # not seem to be illegal, only as fucked up as PRACK offers in general.
            
            if session:
                ok = self.iuc.out_session(session)
                
                if ok:
                    # Rip the SDP from the response
                    if type == "session":
                        return

                # Otherwise it was just queued

            status = None
            already_ringing = (self.state == self.DIALING_IN_RINGING)

            # Session rejects automatically screw the whole call, regardless of the action
            if session and session.is_reject():
                status = Status.NOT_ACCEPTABLE_HERE
            elif type == "session":
                status = Status.RINGING if already_ringing else Status.SESSION_PROGRESS
            elif type == "ring":
                status = Status.RINGING
                if not already_ringing:
                    self.change_state(self.DIALING_IN_RINGING)
            elif type == "accept":
                status = Status.OK
                # Wait for the ACK before changing state
            elif type == "reject":
                status = action["status"] or Status.SERVER_INTERNAL_ERROR
            else:
                raise Exception("Unknown action type: %s!" % type)
                
            msg = Sip.response(status=status)
            self.iuc.out_server(msg)
            
            if status.code >= 300:
                # The transactions will catch the ACK
                self.change_state(self.DOWN)
                self.may_finish()
            
            return
            
        elif self.state == self.UP:
            # Re-INVITE stuff
            
            if type == "session":
                ok = self.iuc.out_session(session)
                if ok:
                    return
                    
                status = Status.NOT_ACCEPTABLE_HERE if session.is_reject() else Status.OK
                msg = Sip.response(status=status)
                self.iuc.out_server(msg)
                return
        
            elif type == "tone":
                if self.leg.media_legs and action.get("name"):
                    self.leg.media_legs[0].notify("tone", dict(name=action["name"]))
                    
                return

            elif type == "hangup":
                msg = Sip.request(method="BYE")
                msg["reason"] = action.get("reason")
                self.send(msg)
                self.change_state(self.DISCONNECTING_OUT)
                return
                
            elif type == "transfer":
                self.process_transfer(action)
                return
            
        raise Exception("Weird thing to do %s in state %s!" % (type, self.state))


    def process_session(self, msg, session):
        if session:
            self.process_remote_session(session)
            action = dict(type="session", session=session)
            self.forward(action)


    def process_refer_request(self, request):
        refer_to = request.get("refer_to")
        if not refer_to:
            self.logger.warning("No Refer-To header!")
            self.send(Sip.response(status=Status.BAD_REQUEST), request)
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
                self.send(Sip.response(status=Status.NOT_FOUND), request)
                return
                
            self.logger.info("Attended transfer to SIP endpoint %s." % local_tag)
            tid = self.ground.make_transfer("attended")
            src = None
            
        refer_sub = "false" if request.get("refer_sub") == "false" else "true"
        response = Sip.response(status=Status.OK, related=request)
        response["refer_sub"] = refer_sub
        self.send(response)
            
        if refer_sub != "false":
            notify = Sip.request(method="NOTIFY")
            notify["event"] = "refer"
            notify["subscription_state"] = "terminated;reason=noresource"
            notify["content_type"] = "message/sipfrag"
            notify.body = "SIP/2.0 200 OK".encode("utf8")
        
            self.send(notify)

        if not other:
            # blind
            action = dict(type="transfer", transfer_id=tid, src=src)
            self.forward(action)
        else:
            # attended
            action = dict(type="transfer", transfer_id=tid)
            other.forward(action)

            action = dict(type="transfer", transfer_id=tid)
            self.forward(action)


    def process(self, msg):
        method = msg.method
        
        if msg.is_response:
            self.logger.debug("Processing response %d %s" % (msg.status.code, method))
        else:
            self.logger.debug("Processing request %s" % method)

        # Note: must change state before forward, because that can generate
        # a reverse action which should only be processed with the new state!
        
        if self.state == self.DOWN:
            if method == "INVITE":
                if msg.is_response:
                    return

                request = msg
                src = dict(request, type="sip")
                
                request, session = self.iuc.in_server(request)
                
                if self.iuc.is_finished():
                    # May happen with 100rel support conflict
                    self.logger.error("Couldn't receive INVITE, finishing.")
                    self.may_finish()
                    return
                
                if session:
                    self.process_remote_session(session)
                
                self.change_state(self.DIALING_IN)

                self.dial(src, session)
                return

        elif self.state in (self.DIALING_IN, self.DIALING_IN_RINGING):
            if method in ("CANCEL", "ACK", "NAK", "PRACK"):
                if msg.is_response:
                    self.logger.warning("A %s response, WTF?" % method)
                    return
                    
                request = msg
                request, session = self.iuc.in_server(request)
                
                if not request:
                    return
                    
                elif method == "CANCEL":
                    self.change_state(self.DOWN)
                    action = dict(type="hangup")
                    self.forward(action)
                    self.may_finish()
                    
                elif method == "ACK":
                    self.change_state(self.UP)
                    self.process_session(request, session)
                        
                elif method == "NAK":
                    self.send(Sip.request(method="BYE"))  # required behavior
                    self.change_state(self.DISCONNECTING_OUT)
                    
                elif method == "PRACK":
                    self.process_session(request, session)
                    
                return
                
            elif method == "UPDATE":
                msg, session = self.iuc.in_update(msg)
                self.process_session(msg, session)
                return
                
        elif self.state in (self.DIALING_OUT, self.DIALING_OUT_RINGING):
            if method in ("INVITE", "ACK", "NAK", "PRACK"):
                # Of course, there will be no ACK or NAK responses, just for completeness
                
                if not msg.is_response:
                    self.logger.warning("A %s request, WTF?" % method)
                    return

                response = msg
                status = response.status
                
                response, session = self.iuc.in_client(response)
                if not response:
                    return
                
                if session:
                    self.process_remote_session(session)

                if method == "INVITE":
                    if status.code == 180:
                        if self.state == self.DIALING_OUT_RINGING:
                            if session:
                                action = dict(type="session", session=session)
                                self.forward(action)
                        
                        else:
                            self.change_state(self.DIALING_OUT_RINGING)
                            action = dict(type="ring", session=session)
                            self.forward(action)

                        return

                    elif status.code == 183:
                        if session:
                            action = dict(type="session", session=session)
                            self.forward(action)

                        return
                    
                    elif status.code >= 300:
                        # Don't wait for any outgoing message, because after reporting a reject,
                        # nothing will come.
                        
                        # Do this before reporting transfers, so we know we're already down.
                        self.change_state(self.DOWN)

                        if status.code in (301, 302):
                            contact = response["contact"][0]
                            self.logger.info("Deflect to %s." % (contact,))
                            tid = self.ground.make_transfer("deflect")

                            diversion = response.get("diversion")
                            reason_string = diversion.params.get("reason") if diversion else status.reason
                            reason = Reason("SIP", dict(cause=status.code, text=reason_string))

                            src = {
                                'type': "sip",
                                'from': response["to"],
                                'to': contact,
                                'reason': [ reason ]  # TODO: use this for all transfers!
                            }

                            action = dict(type="transfer", transfer_id=tid, src=src)
                            self.forward(action)

                        action = dict(type="reject", status=status)
                        self.forward(action)
                        self.may_finish()
                        
                        return

                    elif status.code >= 200:
                        if self.iuc.is_finished():
                            self.change_state(self.UP)
                        
                        action = dict(type="accept", session=session)
                        self.forward(action)
                        return
                        
                elif method == "PRACK":
                    return # Nothing meaningful should arrive in PRACK responses
                    
            elif method == "UPDATE":
                msg, session = self.iuc.in_update(msg)
                self.process_session(msg, session)
                return

        elif self.state == self.UP:
            if method in ("INVITE", "ACK", "NAK", "PRACK", "UPDATE"):
                msg, session = self.iuc.in_generic(msg)
                self.process_session(msg, session)
                return
                
            elif method == "REFER":
                if msg.is_response:
                    self.logger.warning("Ignoring REFER response!")
                    return
                    
                self.process_refer_request(msg)
                return
                
            elif method == "NOTIFY":
                if msg.is_response:
                    self.logger.warning("Ignoring NOTIFY response!")
                    return

            elif method == "BYE":
                if msg.is_response:
                    self.logger.warning("Ignoring BYE response!")
                    return
                
                request = msg
                self.send(Sip.response(status=Status.OK, related=request))
                self.change_state(self.DOWN)
                action = dict(type="hangup")
                self.forward(action)
                self.may_finish()
                return
                    
        elif self.state == self.DISCONNECTING_OUT:
            if method == "BYE":
                if msg.is_response:
                    self.change_state(self.DOWN)
                    self.may_finish()
                    return

                request = msg
                self.logger.debug("Mutual BYE, finishing immediately.")
                self.send(Sip.response(status=Status.OK, related=request))
                self.change_state(self.DOWN)
                self.may_finish()
                return
                
            elif method == "INVITE":
                if msg.is_response:
                    self.logger.debug("Got late INVITE response: %s" % (msg.status,))
                    # This was ACKed by the transaction layer
                    self.change_state(self.DOWN)
                    self.may_finish()
                    return
                
            elif method == "CANCEL":
                if msg.is_response:
                    self.logger.debug("Got late CANCEL response: %s" % (msg.status,))
                    return

            elif method == "NOTIFY":
                if msg.is_response:
                    self.logger.debug("Got late NOTIFY response: %s" % (msg.status,))
                    return
                    
            elif method == "ACK":
                # This may happen if we got a REFER before the ACK, Snom does that
                self.logger.debug("Got late ACK request.")
                return
                
        if msg.is_response:
            self.logger.warning("Weird %s response in state %s!" % (method, self.state))
        else:
            self.logger.warning("Weird %s request in state %s!" % (method, self.state))


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
