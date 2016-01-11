from copy import deepcopy
#import logging

from async import WeakMethod
from format import Status, make_virtual_response
#from mgc import ProxiedMediaLeg
from util import build_oid
from leg import Leg, Session, Error


class InviteState(object):
    def __init__(self, request):
        self.request = request
        sdp = request.get("sdp")
        self.had_offer_in_request = (sdp and sdp.is_session())
        self.responded_session = None
        self.rpr_session_done = False
        self.final_response = None


class SipLeg(Leg):
    DOWN = "DOWN"
    DIALING_IN = "DIALING_IN"
    DIALING_OUT = "DIALING_OUT"
    DIALING_IN_RINGING = "DIALING_IN_RINGING"
    DIALING_OUT_RINGING = "DIALING_OUT_RINGING"
    DIALING_IN_ANSWERED = "DIALING_IN_ANSWERED"
    DIALING_OUT_ANSWERED = "DIALING_OUT_ANSWERED"
    UP = "UP"
    DISCONNECTING_OUT = "DISCONNECTING_OUT"
    

    def __init__(self, dialog):
        super().__init__()

        self.dialog = dialog
        self.state = self.DOWN
        self.invite_state = None
        self.session = Session()
        
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


    def refresh_media(self):
        def extract_formats(c):
            return { r.payload_type: (r.encoding, r.clock) for r in c.formats }

        # Negotiated session parameters, must have the same length
        lsdp = self.session.local_sdp
        rsdp = self.session.remote_sdp
        channel_count = min(len(lsdp.channels), len(rsdp.channels)) if lsdp and rsdp else 0

        for i in range(channel_count):
            lc = lsdp.channels[i]
            rc = rsdp.channels[i]
            ml = self.media_legs[i]  # must also have the same number of media legs
            #print("XXX local: %s (%s), remote: %s (%s)" % (lc.addr, id(lc.addr), rc.addr, id(rc.addr)))
            
            ml.update(
                remote_addr=rc.addr,
                send_formats=extract_formats(rc),
                recv_formats=extract_formats(lc)
            )
            
        Leg.refresh_media(self)
            

    def preprocess_outgoing_session(self, sdp):
        for i in range(len(self.media_legs), len(sdp.channels)):
            self.make_media_leg(i, "net")
        
        for i in range(len(sdp.channels)):
            # No need to make a copy again here
            if sdp.channels[i].addr:
                raise Exception("Outgoing session has channel address set! %s" % id(sdp.channels[i]))
                
            sdp.channels[i].addr = self.media_legs[i].local_addr
            
        return sdp  # For the sake of consistency


    def postprocess_incoming_session(self, sdp):
        sdp = deepcopy(sdp)

        for i in range(len(sdp.channels)):
            sdp.channels[i].addr = None  # Just to be sure
            
        return sdp

        
    def process_incoming_offer(self, sdp):
        self.session.set_remote_offer(sdp)
        sdp = self.postprocess_incoming_session(sdp)
        return sdp

    
    def process_incoming_answer(self, sdp):
        self.session.set_remote_answer(sdp)
        self.refresh_media()
        sdp = self.postprocess_incoming_session(sdp)
        return sdp

    
    def process_outgoing_offer(self, sdp):
        sdp = self.preprocess_outgoing_session(sdp)
        self.session.set_local_offer(sdp)
        return sdp

    
    def process_outgoing_answer(self, sdp):
        sdp = self.preprocess_outgoing_session(sdp)
        self.session.set_local_answer(sdp)
        self.refresh_media()
        return sdp
        

    def do(self, action):
        self.logger.debug("Doing %s" % action)
        
        type = action["type"]
        offer = action.get("offer")
        answer = action.get("answer")
        
        if offer and answer:
            raise Error("WTF?")
        elif offer:
            offer = self.process_outgoing_offer(offer)
        elif answer:
            answer = self.process_outgoing_answer(answer)
        
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
                
                invite_request = dict(method="INVITE", sdp=offer)
                self.send_request(invite_request)  # Will be extended!
                self.invite_state = InviteState(invite_request)
                self.change_state(self.DIALING_OUT)
                
                self.anchor()
                return
            else:
                self.logger.debug("Ignoring %s, already down." % type)
                return
        elif self.state in (self.DIALING_OUT, self.DIALING_OUT_RINGING):
            if type == "hangup":
                self.send_request(dict(method="CANCEL"), self.invite_state.request)
                self.change_state(self.DISCONNECTING_OUT)
                return
        elif self.state in (self.DIALING_OUT_ANSWERED,):  # TODO: into InviteState?
            if type == "session":
                # send ACK with SDP
                if not answer:
                    raise Error("Answer expected in ACK!")
                    
                self.send_request(dict(method="ACK", sdp=answer), self.invite_state.final_response)
                self.change_state(self.UP)
                return
        elif self.state in (self.DIALING_IN, self.DIALING_IN_RINGING):
            session_changed = False
            
            if self.invite_state.responded_session:
                sdp = self.invite_state.responded_session
            else:
                sdp = answer if self.invite_state.had_offer_in_request else offer
                if sdp:
                    session_changed = True
                    self.invite_state.responded_session = sdp
                
            if type == "ring":
                already_ringing = (self.state == self.DIALING_IN_RINGING)
                if not session_changed and already_ringing:
                    self.logger.debug("Already ringing and session unchanged, skipping 180.")
                    return
                
                invite_response = dict(status=Status(180, "Ringing"), sdp=sdp)
                self.send_response(invite_response, self.invite_state.request)
                self.change_state(self.DIALING_IN_RINGING)
                return
            elif type == "session":
                if not session_changed:
                    self.logger.debug("Session unchanged, skipping 180/183.")
                    return
                    
                already_ringing = (self.state == self.DIALING_IN_RINGING)
                status = Status(180, "Ringing") if already_ringing else Status(183, "Session Progress")
                invite_response = dict(status=status, sdp=sdp)
                self.send_response(invite_response, self.invite_state.request)
                return
            elif type == "accept":
                invite_response = dict(status=Status(200, "OK"), sdp=sdp)
                self.send_response(invite_response, self.invite_state.request)
                self.change_state(self.DIALING_IN_ANSWERED)  # TODO: into InviteState?
                # TODO: Do we need to block requests here, or can we already send new ones?
                # Just wait for the ACK for now.
                return
            elif type == "reject":
                status = action["status"]
                invite_response = dict(status=status)
                self.send_response(invite_response, self.invite_state.request)
                self.change_state(self.DOWN)  # The transactions will catch the ACK
                self.finish_media()
                return
        elif self.state == self.UP:
            if type == "hangup":
                self.send_request(dict(method="BYE"))
                self.change_state(self.DISCONNECTING_OUT)
                return
                
        raise Error("Weirdness!")


    def process(self, msg):
        is_response = msg["is_response"]
        method = msg["method"]
        status = msg.get("status")
        sdp = msg.get("sdp")
        
        # Note: must change state before report, because that can generate
        # a reverse action which should only be processed with the new state!
        
        if self.state == self.DOWN:
            if not is_response and method == "INVITE":
                self.ctx.update({
                    "uri": msg["uri"],
                    "from": msg["from"],
                    "to": msg["to"]
                })
                
                offer = sdp if sdp and sdp.is_session() else None
                if offer:
                    offer = self.process_incoming_offer(offer)  # TODO: session query?
                    
                self.invite_state = InviteState(msg)
                self.change_state(self.DIALING_IN)
                self.report(dict(type="dial", ctx=self.ctx, offer=offer))
                return
        elif self.state in (self.DIALING_IN, self.DIALING_IN_RINGING):
            if not is_response and method == "CANCEL":
                self.send_response(dict(status=Status(200, "OK")), msg)
                self.send_response(dict(status=Status(487, "Request Terminated")), self.invite_state.request)
                
                self.change_state(self.DOWN)
                self.report(dict(type="hangup"))
                self.finish_media()
                return
        elif self.state in (self.DIALING_OUT, self.DIALING_OUT_RINGING):
            if is_response and method == "INVITE":
                offer, answer = None, None
                
                if sdp and sdp.is_session() and not self.invite_state.responded_session:
                    # No session yet in INVITE responses, take this one
                    
                    if self.invite_state.had_offer_in_request:
                        answer = self.process_incoming_answer(sdp)
                    else:
                        offer = self.process_incoming_offer(sdp)

                    self.invite_state.responded_session = sdp  # just to ignore any further
                    
                if status.code == 180:
                    already_ringing = (self.state == self.DIALING_OUT_RINGING)
                    self.change_state(self.DIALING_OUT_RINGING)
                    
                    if not already_ringing:
                        self.report(dict(type="ring", offer=offer, answer=answer))
                    elif offer or answer:
                        self.report(dict(type="session", offer=offer, answer=answer))
                    return
                elif status.code == 183:
                    if offer or answer:
                        self.report(dict(type="session", offer=offer, answer=answer))
                    return
                elif status.code >= 300:
                    self.change_state(self.DOWN)
                    self.report(dict(type="reject", status=status))
                    # ACKed by tr
                    self.finish_media()
                    return
                elif status.code >= 200:
                    if self.invite_state.had_offer_in_request:
                        # send ACK without SDP now
                        self.send_request(dict(method="ACK"), msg)
                        self.change_state(self.UP)
                    else:
                        # wait for outgoing session for the ACK
                        # beware, the report() below may send it!
                        self.invite_state.final_response = msg
                        self.change_state(self.DIALING_OUT_ANSWERED)
                        
                    self.report(dict(type="accept", offer=offer, answer=answer))
                    return
        elif self.state == self.DIALING_IN_ANSWERED:
            if not is_response and method == "ACK":
                answer = None
                
                if self.invite_state.had_offer_in_request:
                    if sdp and sdp.is_session():
                        self.logger.debug("Unexpected session in ACK!")
                else:
                    if not (sdp and sdp.is_session()):
                        self.logger.debug("Unexpected sessionless ACK!")
                    else:
                        answer = self.process_incoming_answer(sdp)
                    
                # Stop the retransmission of the final answer
                self.send_response(make_virtual_response(), self.invite_state.request)
                # Let the ACK server transaction expire
                self.send_response(make_virtual_response(), msg)
                
                self.invite_state = None
                self.change_state(self.UP)
                if answer:
                    self.report(dict(type="session", answer=answer))
                return
            elif not is_response and method == "NAK":  # virtual request, no ACK received
                self.send_request(dict(method="BYE"))  # required behavior
                self.change_state(self.DISCONNECTING_OUT)
                return
        elif self.state == self.UP:
            if not is_response and method == "BYE":
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
                
        raise Error("Weirdness!")
