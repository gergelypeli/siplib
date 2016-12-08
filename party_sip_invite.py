from format import Status, Rack, make_virtual_response
from util import Loggable
import zap


START = "START"

REQUEST_OFFER = "REQUEST_OFFER"
# no need for PROVISIONAL_EMPTY
PROVISIONAL_ANSWER = "PROVISIONAL_ANSWER"
RELIABLE_NOANSWER = "RELIABLE_NOANSWER"  # wait for PRACK only, then RO
RELIABLE_ANSWER = "RELIABLE_ANSWER"  # wait for PRACK
PRACK_OFFER = "PRACK_OFFER"

REQUEST_EMPTY = "REQUEST_EMPTY"
PROVISIONAL_OFFER = "PROVISIONAL_OFFER"
RELIABLE_OFFER = "RELIABLE_OFFER"  # wait for PRACK

EARLY_SESSION = "EARLY_SESSION"
RELIABLE_EMPTY = "RELIABLE_EMPTY"  # wait for PRACK only, then ES

FINAL_OFFER = "FINAL_OFFER"
FINAL_ANSWER = "FINAL_ANSWER"
FINAL_EMPTY = "FINAL_EMPTY"

FINISH = "FINISH"


class Error(Exception):
    pass


class InviteState(Loggable):
    def __init__(self):
        self.message_slot = zap.EventSlot()
        
        self.state = START
        self.request = None
        
        self.rpr_supported = True  # FIXME
        self.rpr_last_rseq = 0
        
        self.pending_sdp = None
        self.pending_messages = []


    def send_message(self, msg, related_msg=None):
        self.message_slot.zap(msg, related_msg)
        

    def outgoing(self, message, sdp):
        if message:
            self.pending_messages.append(message)
        
        if sdp:
            if self.pending_sdp:
                raise Error("SDP already pending!")
                
            self.pending_sdp = sdp
            
            
    def incoming(self, msg):
        raise NotImplementedError()
        
        
    def set_rpr_supported(self):
        self.rpr_supported = True
        
        
    def is_rpr_supported(self):
        return self.rpr_supported
        
        
    def is_finished(self):
        return self.state == FINISH


    def is_session_finished(self):
        return self.state in (EARLY_SESSION, RELIABLE_EMPTY, FINAL_EMPTY, FINISH)

    # FIXME: use a separate send_request and send_response in each
    # subclasses! And let the user pimp messages there, pass only a simple
    # dict to them from here!
    
    #def send_message(self, message, related):
    #    raise NotImplementedError()

    # FIXME: no need for pending queue, just use a single variable!
    
    # FIXME: maybe even a single status or method string? Only the initial
    # invite has meaningful fields. But those are set in SipLeg.

    def send(self, log, state=None, msg=None, rel=None):
        if state:
            self.logger.debug("Changing state %s => %s" % (self.state, state))
            self.state = state
        
        if self.pending_messages and self.pending_messages[0] is msg:
            self.pending_messages.pop(0)

        if msg:
            self.logger.debug("Sending message: %s" % log)
            self.send_message(msg, rel)
        else:
            self.logger.debug("Not sending message: %s" % log)


    def recv(self, log, state, msg, sdp=None, is_answer=None):
        self.logger.debug("Received message: %s" % log)
    
        if sdp is not None and is_answer is None:
            raise Exception("Please tell explicitly if the SDP is an answer or not!")
    
        if state:
            self.logger.debug("Changing state %s => %s" % (self.state, state))
            self.state = state
        
        return msg, sdp, is_answer

        
    def abort(self, message):
        self.logger.error(message)
        self.state = FINISH
        return None, None, None


    def ignore(self, message):
        self.logger.warning(message)
        return None, None, None


class InviteClientState(InviteState):
    def __init__(self):
        InviteState.__init__(self)
        
        self.unanswered_rpr = None


    def make_prack(self, rpr, sdp=None):
        rack = Rack(rpr["rseq"], rpr["cseq"], rpr["method"])
        return dict(method="PRACK", rack=rack, sdp=sdp)


    def outgoing(self, message, sdp=None):
        InviteState.outgoing(self, message, sdp)
        
        s = self.state
        req = self.request

        msg = self.pending_messages[0] if self.pending_messages else None
        method = msg["method"] if msg else None
        sdp = self.pending_sdp

        # CANCEL is possible in all states
        if method == "CANCEL":
            if s == START:
                raise Error("Bad CANCEL!")
                
            return self.send("cancel", None, msg, req)
        elif method == "INVITE":
            if s != START:
                raise Error("Bad INVITE!")
            
            self.request = msg
                
            if sdp:
                msg["sdp"] = sdp
                return self.send("request with offer", REQUEST_OFFER, msg, None)
            else:
                return self.send("request without offer", REQUEST_EMPTY, msg, None)
        elif method:
            # PRACK request is always implicit
            raise Error("Bad outgoing request!")
        elif sdp:
            if s == START:
                return self.send("delaying offer until INVITE")
            elif s == PROVISIONAL_OFFER:
                return self.send("delaying answer until reliable offer")
            elif s == RELIABLE_OFFER:
                rpr = self.unanswered_rpr
                self.unanswered_rpr = None
                
                pra = self.make_prack(rpr, sdp)
                return self.send("PRACK with answer", EARLY_SESSION, pra, None)
            elif s == FINAL_OFFER:
                ack = dict(method="ACK", sdp=sdp)
                
                return self.send("ACK with answer", FINISH, ack, req)
            else:
                raise Error("Invalid outgoing SDP in state %s!" % s)

                
    def incoming(self, msg):
        s = self.state
        req = self.request

        method = msg["method"]
        is_response = msg["is_response"]
        sdp = msg.pop("sdp")
        status = msg["status"] if is_response else None
        has_reject = status and status.code >= 300
        has_final = status and status.code < 300 and status.code >= 200
        has_rpr = "100rel" in msg.get("require", set())

        if method == "CANCEL":
            if has_reject:
                # Finish immediately
                return self.recv("cancel rejected", FINISH, msg)
            else:
                # Wait for rejection of the INVITE
                return self.recv("cancel accepted", None, msg)
        
        elif method == "INVITE":
            if has_reject:
                # Transaction layer already sends the ACK for this
                return self.recv("reject response", FINISH, msg)
            elif has_final:
                if s in (REQUEST_OFFER, PROVISIONAL_ANSWER, RELIABLE_NOANSWER):
                    ack = dict(method="ACK")
                    self.send("ACK", None, ack, req)
                    
                    if sdp:
                        return self.recv("final response with answer", FINISH, msg, sdp, True)
                    else:
                        return self.abort("final response missing answer")
                elif s in (REQUEST_EMPTY, PROVISIONAL_OFFER):
                    if sdp:
                        # Wait for answer to ACK
                        return self.recv("final response with offer", FINAL_OFFER, msg, sdp, False)
                    else:
                        ack = dict(method="ACK")
                        self.send("ACK for bad response", None, ack, req)
                        return self.abort("final response missing offer")
                elif s in (EARLY_SESSION, RELIABLE_EMPTY):
                    if sdp:
                        return self.abort("final response with unexpected SDP!")
                    else:
                        ack = dict(method="ACK")
                        self.send("ACK", None, ack, req)
                        return self.recv("final response", FINISH, msg)
                else:
                    # This can be a duplicate final
                    return self.ignore("Ignoring unexpected final response!")
            elif has_rpr:
                rseq = msg["rseq"]
                
                if rseq <= self.rpr_last_rseq:
                    return self.recv("duplicate rpr", None, None)
                elif rseq > self.rpr_last_rseq + 1:
                    return self.recv("out of order rpr", None, None)

                self.rpr_last_rseq += 1
                
                if s in (REQUEST_OFFER,):
                    self.send("PRACK", None, self.make_prack(msg))
                    
                    if sdp:
                        return self.recv("reliable response with answer", EARLY_SESSION, msg, sdp, True)
                    else:
                        return self.recv("reliable response", None, msg)
                elif s in (PROVISIONAL_ANSWER,):
                    self.send("PRACK", None, self.make_prack(msg))
                    
                    if sdp:
                        return self.recv("reliable response with ignored answer", EARLY_SESSION, msg)
                    else:
                        return self.recv("reliable response", None, msg)
                elif s in (REQUEST_EMPTY,):
                    if sdp:
                        self.unanswered_rpr = msg
                        return self.recv("reliable response with offer", RELIABLE_OFFER, msg, sdp, False)
                    else:
                        return self.abort("reliable response missing offer")
                elif s in (PROVISIONAL_OFFER,):
                    self.send("PRACK", None, self.make_prack(msg))
                    
                    if sdp:
                        return self.recv("reliable response with ignored offer", EARLY_SESSION, msg)
                    else:
                        return self.recv("reliable response", None, msg)
                else:
                    return self.abort("Unexpected reliable response!")
            else:
                if s in (REQUEST_OFFER,):
                    if sdp:
                        return self.recv("provisional response with answer", PROVISIONAL_ANSWER, msg, sdp, True)
                    else:
                        return self.recv("provisional response", None, msg)
                elif s in (PROVISIONAL_ANSWER,):
                    if sdp:
                        return self.recv("provisional response with ignored answer", None, msg)
                    else:
                        return self.recv("provisional response", None, msg)
                elif s in (REQUEST_EMPTY,):
                    if sdp:
                        return self.recv("provisional response with offer", PROVISIONAL_OFFER, msg, sdp, False)
                    else:
                        return self.recv("provisional response", None, msg)
                elif s in (PROVISIONAL_OFFER,):
                    if sdp:
                        return self.recv("provisional response with ignored offer", None, msg)
                    else:
                        return self.recv("provisional response", None, msg)
                else:
                    return self.abort("Unexpected provisional response!")

        elif method == "PRACK":
            if has_reject:
                return self.abort("LOL, rejected PRACK!")
            elif has_final:
                # We never send offers in PRACK requests, so the response is irrelevant
                return self.recv("PRACK response", None, None)
            else:
                return self.abort("LOL, provisional PRACK!")
        
        else:
            return self.abort("Unexpected response!")


# Invite server

class InviteServerState(InviteState):
    def __init__(self):
        InviteState.__init__(self)

        self.unanswered_prack = None


    def require_100rel(self, msg):
        msg.setdefault("require", set()).add("100rel")

        self.rpr_last_rseq += 1
        msg["rseq"] = self.rpr_last_rseq
        

    def outgoing(self, message, sdp=None):
        InviteState.outgoing(self, message, sdp)

        s = self.state
        req = self.request
        sdp = self.pending_sdp
        can_rpr = self.is_rpr_supported()
        
        msg = self.pending_messages[0] if self.pending_messages else None
        status = msg["status"] if msg else None
        has_reject = status and status.code >= 300
        has_final = status and status.code < 300 and status.code >= 200
        has_prov = status and status.code < 200

        if has_reject:
            # regardless of the session state, send this out
            return self.send("final non-2xx", FINISH, msg, req)
        elif has_final:
            if s in (REQUEST_EMPTY, PROVISIONAL_OFFER):
                if sdp:
                    msg["sdp"] = sdp
                    return self.send("final 2xx with offer", FINAL_OFFER, msg, req)
                else:
                    return self.abort("final 2xx needs offer")
            elif s in (REQUEST_OFFER, PROVISIONAL_ANSWER):
                if sdp:
                    msg["sdp"] = sdp
                    return self.send("final 2xx with answer", FINAL_ANSWER, msg, req)
                else:
                    return self.abort("final 2xx needs answer")
            elif s in (EARLY_SESSION,):
                return self.send("final 2xx after session", FINAL_EMPTY, msg, req)
        elif has_prov:  # including rpr
            if s in (REQUEST_EMPTY, PROVISIONAL_OFFER):
                if sdp:
                    msg["sdp"] = sdp
                    
                    if can_rpr:
                        self.require_100rel(msg)
                        return self.send("rpr with offer", RELIABLE_OFFER, msg, req)
                    else:
                        return self.send("prov with offer", PROVISIONAL_OFFER, msg, req)
                else:
                    # without offer we can't send an rpr
                    return self.send("prov without offer", REQUEST_EMPTY, msg, req)
            elif s in (REQUEST_OFFER, PROVISIONAL_ANSWER):
                if sdp:
                    msg["sdp"] = sdp

                    if can_rpr:
                        self.require_100rel(msg)
                        return self.send("rpr with answer", RELIABLE_ANSWER, msg, req)
                    else:
                        return self.send("prov with answer", PROVISIONAL_ANSWER, msg, req)
                else:
                    if can_rpr:
                        self.require_100rel(msg)
                        return self.send("rpr without answer", RELIABLE_NOANSWER, msg, req)
                    else:
                        return self.send("prov without answer", REQUEST_OFFER, msg, req)
            elif s in (EARLY_SESSION,):
                # seems like we can do rpr, but can't send further sessions in rpr
                return self.send("rpr after session", RELIABLE_EMPTY, msg, req)
        else:
            if s in (REQUEST_EMPTY, PROVISIONAL_OFFER):
                if sdp:
                    msg = dict(status=Status(183), sdp=sdp)
                    
                    if can_rpr:
                        return self.send("session progress rpr with offer", RELIABLE_OFFER, msg, req)
                    else:
                        return self.send("session progress with offer", PROVISIONAL_OFFER, msg, req)
            elif s in (REQUEST_OFFER, PROVISIONAL_ANSWER):
                if sdp:
                    msg = dict(status=Status(183), sdp=sdp)
                    
                    if can_rpr:
                        return self.send("session progress rpr with answer", RELIABLE_ANSWER, msg, req)
                    else:
                        return self.send("session progress with answer", PROVISIONAL_ANSWER, msg, req)
            elif s in (PRACK_OFFER,):
                if sdp:
                    msg = dict(status=Status(200), sdp=sdp)
                    pra = self.unanswered_prack
                    self.unanswered_prack = None
                    
                    return self.send("prack response with answer", EARLY_SESSION, msg, pra)
            else:
                return self.abort("Huh?")


    def incoming(self, msg):
        s = self.state
        req = self.request
        
        method = msg["method"]
        sdp = msg.pop("sdp")

        if method == "INVITE":
            if s == START:
                self.request = msg
                
                if "100rel" in msg.get("supported", set()):
                    self.set_rpr_supported()
        
                if sdp:
                    return self.recv("request with offer", REQUEST_OFFER, msg, sdp, False)
                else:
                    return self.recv("request without offer", REQUEST_EMPTY, msg, None)
            else:
                return self.abort("Not an INVITE request is received!")

        elif method == "CANCEL":
            res = dict(status=Status(200))
            self.send_message(res, msg)
        
            res = dict(status=Status(487))  # ?
            self.send_message(res, self.request)
        
            return self.recv("cancelled", FINISH, msg, None)

        elif method == "PRACK":
            # Prack is swallowed, not returned
            rseq, rcseq, rmethod = msg["rack"]
    
            if rmethod != req["method"] or rcseq != req["cseq"] or rseq != self.rpr_last_rseq:
                prr = dict(status=Status(481))
                self.send_message(prr, msg)
                return self.abort("Wrong rpr to PRACK!")
            
            # Stop the retransmission of the rpr
            self.send_message(make_virtual_response(), req)
                
            if s in (RELIABLE_OFFER,):
                if sdp:
                    prr = dict(status=Status(200))
                    self.send_message(prr, msg)
                    return self.recv("PRACK with answer", EARLY_SESSION, None, sdp, True)
                else:
                    return self.abort("Missing answer in PRACK request!")
            elif s in (RELIABLE_ANSWER,):
                if sdp:
                    # Bleh
                    self.unanswered_prack = msg
                    self.pending_sdp = None  # This is needed to let us answer again
                    return self.recv("PRACK with offer", PRACK_OFFER, None, sdp, False)
                else:
                    prr = dict(status=Status(200))
                    self.send_message(prr, msg)
                    return self.recv("PRACK", EARLY_SESSION, None, None)
            elif s in (RELIABLE_NOANSWER, RELIABLE_EMPTY):
                if sdp:
                    return self.abort("PRACK with unexpected SDP!")
                else:
                    prr = dict(status=Status(200))
                    self.send_message(prr, msg)
                    state = (EARLY_SESSION if s == RELIABLE_EMPTY else REQUEST_OFFER)
                    return self.recv("PRACK", state, None, None)
            else:
                raise Error("PRACK in a weird state: %s!" % s)
                    
        elif method == "ACK":
            # Stop the retransmission of the final answer
            self.send_message(make_virtual_response(), req)
            
            # Let the ACK server transaction expire
            self.send_message(make_virtual_response(), msg)

            if s in (FINAL_ANSWER, FINAL_EMPTY):
                if sdp:
                    return self.recv("ACK with ignored SDP", FINISH, msg, None)
                else:
                    return self.recv("ACK", FINISH, msg, None)
            elif s in (FINAL_OFFER,):
                if sdp:
                    return self.recv("ACK with answer", FINISH, msg, sdp, True)
                else:
                    return self.recv("ACK without SDP", FINISH, msg, None)

        elif method == "NAK":
            return self.recv("NAK", FINISH, None, None)
