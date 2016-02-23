from async import WeakMethod
from format import Status, make_virtual_response
from util import build_oid, resolve
from leg import Leg, SessionState, Error
from sdp import SdpBuilder, SdpParser, STATIC_PAYLOAD_TYPES


class InviteState(object):
    def __init__(self, request, is_outgoing):
        self.request = request
        self.is_outgoing = is_outgoing
        self.responded_sdp = None
        self.rpr_session_done = False
        self.final_response = None


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
        host = resolve(dialog.dialog_manager.get_local_addr())[0]  # TODO: not nice
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


    #def flatten_formats(self, formats_by_pt):
    #    return { pt: (f["encoding"], f["clock"], f["encp"], f["fmtp"]) for pt, f in formats_by_pt.items() }


    def flatten_formats(self, formats, pt_key):
        return { f[pt_key]: (f["encoding"], f["clock"], f["encp"], f["fmtp"]) for f in formats }


    def realize_media_legs(self):
        # This must only be called after an answer is accepted
        self.logger.debug("realize_media_legs")
        local_channels = self.session.local_session["channels"]
        remote_channels = self.session.remote_session["channels"]
        
        if len(local_channels) != len(remote_channels):
            raise Exception("Channel count mismatch!")
        
        for i in range(len(local_channels)):
            if i >= len(self.media_legs):
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
            
        #Leg.refresh_media(self)
            

    def preprocess_outgoing_session(self, session):
        # TODO: handle rejection, too!
        self.logger.debug("preprocess_outgoing_session")
        channels = session["channels"]
        old_channels = self.session.local_session["channels"] if self.session.local_session else []
        
        for i, c in enumerate(channels):
            if i >= len(old_channels):
                local_addr = self.call.allocate_media_address(i)  # TODO: deallocate!
            else:
                local_addr = old_channels[i]["rtp_local_addr"]
        
            c["rtp_local_addr"] = local_addr
            next_pt = 96
            
            for f in c["formats"]:
                x = (f["encoding"], f["clock"], f["encp"], f["fmtp"])
                
                for pt, info in STATIC_PAYLOAD_TYPES.items():
                    if info == x:
                        break
                else:
                    pt = next_pt
                    next_pt += 1
                    #raise Exception("Couldn't find payload type for %s!" % (encoding, clock))

                f["rtp_local_payload_type"] = pt
                

    def postprocess_incoming_session(self, session):
        # TODO: this should extract the remote addr and encodings_by_pt?
        pass

        
    def process_incoming_offer(self, sdp):
        session = self.sdp_parser.parse(sdp, is_answer=False)
        self.session.set_remote_offer(session)
        self.postprocess_incoming_session(session)
        return session

    
    def process_incoming_answer(self, sdp):
        session = self.sdp_parser.parse(sdp, is_answer=True)
        self.session.set_remote_answer(session)
        self.realize_media_legs()  # necessary to realize leg with the cached params
        self.postprocess_incoming_session(session)
        return session

    
    def process_outgoing_offer(self, session):
        self.preprocess_outgoing_session(session)
        self.session.set_local_offer(session)
        sdp = self.sdp_builder.build(session)
        return sdp

    
    def process_outgoing_answer(self, session):
        self.preprocess_outgoing_session(session)
        self.session.set_local_answer(session)
        self.realize_media_legs()
        sdp = self.sdp_builder.build(session)
        return sdp


    def invite_outgoing_request(self, msg, session):
        if self.invite:
            raise Exception("Invalid outgoing INVITE request!")
        
        if not session:
            msg["sdp"] = None  #raise Exception("No session for INVITE request!")
        elif session["is_answer"]:
            raise Exception("Session answer for INVITE request!")
        else:
            msg["sdp"] = self.process_outgoing_offer(session)
            
        self.invite = InviteState(msg, True)
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
            # Transactions sent the ACK
            self.invite = None
            return dict(is_answer=True)  # rejection
        elif status.code >= 200:
            self.invite.final_response = msg
                
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
        
        return session


    def invite_outgoing_response_session(self, session):
        if not self.invite or self.invite.is_outgoing:
            raise Exception("Invalid outgoing INVITE response session!")
    
        if not session:
            return False
    
        if self.invite.responded_sdp:
            if session:
                raise Exception("Ignoring outgoing session, because one already sent!")
                
            return False
        elif self.invite.request.get("sdp"):
            if not session["is_answer"]:
                raise Exception("Ignoring outgoing session, because answer is expected!")
            
            self.invite.responded_sdp = self.process_outgoing_answer(session)
            return True
        else:
            if session["is_answer"]:
                raise Exception("Ignoring outgoing session, because offer is expected!")
            
            self.invite.responded_sdp = self.process_outgoing_offer(session)
            return True


    def invite_outgoing_response(self, msg):
        if not self.invite or self.invite.is_outgoing:
            raise Exception("Invalid outgoing INVITE response!")
        
        is_reject = (msg["status"].code >= 300)
        
        if self.invite.responded_sdp and not is_reject:
            msg["sdp"] = self.invite.responded_sdp
            
        self.invite.final_response = msg  # Note: this is an incomplete message, but OK here
        self.send_response(self.invite.final_response, self.invite.request)
        
        if is_reject:
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
    

    def do(self, action):
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
                
                self.invite_outgoing_request(dict(method="INVITE"), session)
                self.change_state(self.DIALING_OUT)
                
                self.anchor()
                return

        elif self.state in (self.DIALING_OUT, self.DIALING_OUT_RINGING):
            if type == "hangup":
                self.send_request(dict(method="CANCEL"), self.invite.request)
                self.change_state(self.DISCONNECTING_OUT)
                return
                
            elif type == "session":
                self.invite_outgoing_ack(session)
                self.change_state(self.UP)
                return

        elif self.state in (self.DIALING_IN, self.DIALING_IN_RINGING):
            session_changed = self.invite_outgoing_response_session(session)

            if type == "session":
                if not session_changed:
                    self.logger.debug("Session unchanged, skipping 180/183.")
                    return
                    
                already_ringing = (self.state == self.DIALING_IN_RINGING)
                status = Status(180, "Ringing") if already_ringing else Status(183, "Session Progress")
                self.invite_outgoing_response(dict(status=status))
                return
                
            elif type == "ring":
                already_ringing = (self.state == self.DIALING_IN_RINGING)
                
                if not session_changed and already_ringing:
                    self.logger.debug("Already ringing and session unchanged, skipping 180.")
                    return
                
                self.invite_outgoing_response(dict(status=Status(180, "Ringing")))
                
                if not already_ringing:
                    self.change_state(self.DIALING_IN_RINGING)
                    
                return
                    
            elif type == "accept":
                self.invite_outgoing_response(dict(status=Status(200, "OK")))
                # Wait for the ACK before changing state
                return

            elif type == "reject":
                self.invite_outgoing_response(dict(status=action["status"]))
                # The transactions will catch the ACK
                self.change_state(self.DOWN)
                self.finish_media()
                return

        elif self.state == self.UP:
            # Re-INVITE stuff
            
            if type == "session":
                if not self.invite:
                    self.invite_outgoing_request(dict(method="INVITE"), session)
                elif not self.invite.final_response:
                    # TODO: handle rejection!
                    self.invite_outgoing_response_session(session)
                    self.invite_outgoing_response(dict(status=Status(200)))
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
                
                self.change_state(self.DIALING_IN)
                self.report(dict(type="dial", ctx=self.ctx, session=session))
                return
                
        elif self.state in (self.DIALING_IN, self.DIALING_IN_RINGING):
            if not is_response and method == "CANCEL":
                self.send_response(dict(status=Status(200, "OK")), msg)
                self.send_response(dict(status=Status(487, "Request Terminated")), self.invite.request)
                
                self.change_state(self.DOWN)
                self.report(dict(type="hangup"))
                self.finish_media()
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
