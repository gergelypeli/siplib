from format import Status, Rack, make_virtual_response
from sdp import add_sdp, get_sdp
from util import Loggable
import zap


KEEP = "KEEP"

START = "START"

REQUEST_OFFER = "REQUEST_OFFER"
# no need for PROVISIONAL_EMPTY
PROVISIONAL_ANSWER = "PROVISIONAL_ANSWER"
RELIABLE_PREANSWER = "RELIABLE_PREANSWER"  # wait for PRACK only, then RO
RELIABLE_ANSWER = "RELIABLE_ANSWER"  # wait for PRACK
PRACK_OFFER = "PRACK_OFFER"  # wait for answer

REQUEST_EMPTY = "REQUEST_EMPTY"
PROVISIONAL_OFFER = "PROVISIONAL_OFFER"
RELIABLE_OFFER = "RELIABLE_OFFER"  # wait for PRACK

EARLY_SESSION = "EARLY_SESSION"
RELIABLE_POSTANSWER = "RELIABLE_POSTANSWER"  # wait for PRACK only, then ES

FINAL_OFFER = "FINAL_OFFER"
FINAL_ANSWER = "FINAL_ANSWER"
FINAL_EMPTY = "FINAL_EMPTY"

FINISH = "FINISH"
ABORT = "ABORT"


class Error(Exception):
    pass


class InviteState(Loggable):
    def __init__(self, use_rpr):
        Loggable.__init__(self)
        
        self.message_slot = zap.EventSlot()
        
        self.state = START
        self.request = None
        
        self.use_rpr = use_rpr
        self.rpr_last_rseq = 0


    def send_message(self, msg, related_msg=None):
        self.message_slot.zap(msg, related_msg)
        

    def process_outgoing(self, message, sdp, is_answer):
        raise NotImplementedError()
            
            
    def process_incoming(self, msg):
        raise NotImplementedError()
        
        
    def is_finished(self):
        return self.state in (FINISH, ABORT)


    def is_session_established(self):
        return self.state in (EARLY_SESSION, RELIABLE_POSTANSWER, FINAL_EMPTY, FINISH)


    def is_clogged(self):
        raise NotImplementedError()


    def is_session_pracking(self):
        raise NotImplementedError()
        

    # FIXME: use a separate send_request and send_response in each
    # subclasses! And let the user pimp messages there, pass only a simple
    # dict to them from here!
    
    #def send_message(self, message, related):
    #    raise NotImplementedError()

    # FIXME: maybe even a single status or method string? Only the initial
    # invite has meaningful fields. But those are set in SipLeg.

    def send(self, log, state, msg=None, rel=None):
        if state != KEEP:
            self.logger.debug("Changing state %s => %s" % (self.state, state))
            self.state = state
        
        if msg:
            self.logger.debug("Sending message: %s" % log)
            self.send_message(msg, rel)
        else:
            self.logger.debug("Not sending message: %s" % log)


    def recv(self, log, state, msg=None, sdp=None, is_answer=None):
        if msg:
            self.logger.debug("Processing message: %s" % log)
        else:
            self.logger.warning("Not processing message: %s" % log)
    
        if sdp is not None and is_answer is None:
            raise Exception("Please tell explicitly if the SDP is an answer or not!")
    
        if state != KEEP:
            self.logger.debug("Changing state %s => %s" % (self.state, state))
            self.state = state
        
        return msg, sdp, is_answer

        
class InviteClientState(InviteState):
    def __init__(self, use_rpr):
        InviteState.__init__(self, use_rpr)
        
        self.unanswered_rpr = None


    def is_clogged(self):
        return self.state in (PROVISIONAL_OFFER,)
        
        
    def is_session_pracking(self):
        return self.use_rpr and self.state == RELIABLE_OFFER
        
        
    def make_rack(self, rpr):
        return Rack(rpr["rseq"], rpr["cseq"], rpr["method"])
        

    def make_prack(self, rpr, sdp=None):
        rack = self.make_rack(rpr)
        req = dict(method="PRACK", rack=rack)
        
        if sdp:
            add_sdp(req, sdp)
            
        return req


    def process_outgoing(self, msg, sdp=None, is_answer=None):
        if sdp is not None and is_answer is None:
            raise Error("SDP without direction!")
    
        s = self.state
        req = self.request
        method = msg["method"]

        # CANCEL is possible in all states
        if method == "CANCEL":
            if s == START:
                return self.send("premature CANCEL", ABORT)
            else:
                return self.send("cancel", None, msg, req)
        elif method == "INVITE":
            if s != START:
                return self.send("late INVITE", ABORT)
            
            if self.use_rpr:
                msg.setdefault("supported", set()).add("100rel")
                msg.setdefault("require", set()).add("100rel")
            
            self.request = msg
            assert not is_answer  # Must be offer or query
                
            if sdp:
                add_sdp(msg, sdp)
                return self.send("request with offer", REQUEST_OFFER, msg)
            else:
                return self.send("request without offer", REQUEST_EMPTY, msg)
        elif method == "ACK":
            if s == FINAL_OFFER:
                if sdp:
                    assert is_answer
                
                    add_sdp(msg, sdp)
                    return self.send("ACK with answer", FINISH, msg, req)
                else:
                    return self.send("ACK needs answer", ABORT)
            else:
                return self.send("unexpected ACK", ABORT)
        elif method == "PRACK":
            if s == RELIABLE_OFFER:
                if sdp:
                    assert is_answer
                    rpr = self.unanswered_rpr
                    self.unanswered_rpr = None
                
                    msg["rack"] = self.make_rack(rpr)
                    add_sdp(msg, sdp)
                    return self.send("PRACK with answer", EARLY_SESSION, msg)
                else:
                    return self.send("PRACK needs answer", ABORT)
            else:
                return self.send("unexpected PRACK", ABORT)
        else:
            return self.send("unexpected message", ABORT)

                
    def process_incoming(self, msg):
        s = self.state
        req = self.request

        method = msg["method"]
        is_response = msg["is_response"]
        sdp = get_sdp(msg)
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
                return self.recv("cancel accepted", KEEP, msg)
        
        elif method == "INVITE":
            if has_reject:
                # Transaction layer already sends the ACK for this
                return self.recv("reject response", FINISH, msg)
            elif has_final:
                if s in (REQUEST_OFFER, PROVISIONAL_ANSWER, RELIABLE_PREANSWER):
                    ack = dict(method="ACK")
                    self.send("ACK", KEEP, ack, req)
                    
                    if sdp:
                        return self.recv("final response with answer", FINISH, msg, sdp, True)
                    else:
                        return self.recv("final response missing answer", ABORT)
                elif s in (REQUEST_EMPTY, PROVISIONAL_OFFER):
                    if sdp:
                        # Wait for answer to ACK
                        return self.recv("final response with offer", FINAL_OFFER, msg, sdp, False)
                    else:
                        ack = dict(method="ACK")
                        self.send("ACK for bad response", KEEP, ack, req)
                        return self.recv("final response missing offer", ABORT)
                elif s in (EARLY_SESSION, RELIABLE_POSTANSWER):
                    if sdp:
                        # The Snom sends such SDP
                        ack = dict(method="ACK")
                        self.send("ACK", KEEP, ack, req)
                        return self.recv("final response with ignored SDP", FINISH, msg)
                    else:
                        ack = dict(method="ACK")
                        self.send("ACK", KEEP, ack, req)
                        return self.recv("final response", FINISH, msg)
                else:
                    return self.recv("duplicate final response", KEEP)
            elif has_rpr:
                rseq = msg["rseq"]
                
                if rseq <= self.rpr_last_rseq:
                    return self.recv("ignoring duplicate rpr", KEEP)
                elif rseq > self.rpr_last_rseq + 1:
                    return self.recv("ignoring out of order rpr", KEEP)

                self.rpr_last_rseq += 1
                
                if s in (REQUEST_OFFER,):
                    pra = dict(method="PRACK", rack=self.make_rack(msg))
                    self.send("PRACK", KEEP, pra)
                    
                    if sdp:
                        return self.recv("reliable response with answer", EARLY_SESSION, msg, sdp, True)
                    else:
                        return self.recv("reliable response", KEEP, msg)
                elif s in (PROVISIONAL_ANSWER,):
                    pra = dict(method="PRACK", rack=self.make_rack(msg))
                    self.send("PRACK", KEEP, pra)
                    
                    if sdp:
                        return self.recv("reliable response with ignored answer", EARLY_SESSION, msg)
                    else:
                        return self.recv("reliable response", KEEP, msg)
                elif s in (REQUEST_EMPTY,):
                    # This will be PRACK-ed explicitly when the answer is ready.
                    # Also, the first reliable response must have an offer.
                    
                    if sdp:
                        self.unanswered_rpr = msg
                        return self.recv("reliable response with offer", RELIABLE_OFFER, msg, sdp, False)
                    else:
                        return self.recv("reliable response missing offer", ABORT)
                elif s in (PROVISIONAL_OFFER,):
                    pra = dict(method="PRACK", rack=self.make_rack(msg))
                    self.send("PRACK", KEEP, pra)
                    
                    if sdp:
                        return self.recv("reliable response with ignored offer", EARLY_SESSION, msg)
                    else:
                        return self.recv("reliable response", KEEP, msg)
                else:
                    return self.recv("unexpected reliable response", ABORT)
            else:
                if self.use_rpr:
                    return self.recv("disallowed unreliable provisional response", ABORT)
                elif s in (REQUEST_OFFER,):
                    if sdp:
                        return self.recv("provisional response with answer", PROVISIONAL_ANSWER, msg, sdp, True)
                    else:
                        return self.recv("provisional response", KEEP, msg)
                elif s in (PROVISIONAL_ANSWER,):
                    if sdp:
                        return self.recv("provisional response with ignored answer", KEEP, msg)
                    else:
                        return self.recv("provisional response", KEEP, msg)
                elif s in (REQUEST_EMPTY,):
                    if sdp:
                        return self.recv("provisional response with offer", PROVISIONAL_OFFER, msg, sdp, False)
                    else:
                        return self.recv("provisional response", KEEP, msg)
                elif s in (PROVISIONAL_OFFER,):
                    if sdp:
                        return self.recv("provisional response with ignored offer", KEEP, msg)
                    else:
                        return self.recv("provisional response", KEEP, msg)
                else:
                    return self.recv("unexpected provisional response", ABORT)

        elif method == "PRACK":
            if has_reject:
                return self.recv("rejected PRACK", ABORT)
            elif has_final:
                return self.recv("PRACK response", KEEP, msg)
            else:
                return self.recv("provisional PRACK response", KEEP)
        
        else:
            return self.recv("unexpected response", ABORT)


# Invite server

class InviteServerState(InviteState):
    def __init__(self, use_rpr):
        InviteState.__init__(self, use_rpr)

        self.unanswered_prack = None  # only used for PRACK offers
        self.provisional_sdp = None  # only used without rpr


    def send(self, log, state=None, msg=None, rel=None):
        if state in (RELIABLE_PREANSWER, RELIABLE_OFFER, RELIABLE_ANSWER, RELIABLE_POSTANSWER):
            self.provisional_sdp = None
            msg.setdefault("require", set()).add("100rel")
            
            self.rpr_last_rseq += 1
            msg["rseq"] = self.rpr_last_rseq
            
        InviteState.send(self, log, state, msg, rel)


    def is_clogged(self):
        # After sending an rpr we must wait until the corresponding PRACK before we
        # can handle the next outgoing message. Better tell our owner beforehand.
        return self.state in (RELIABLE_PREANSWER, RELIABLE_ANSWER, RELIABLE_OFFER, RELIABLE_POSTANSWER)
        
        
    def is_session_pracking(self):
        return self.use_rpr and self.state == PRACK_OFFER
        

    def process_outgoing(self, msg, sdp=None, is_answer=None):
        if sdp is not None and is_answer is None:
            raise Error("SDP without direction!")
            
        s = self.state
        req = self.request

        status = msg["status"]
        has_reject = status and status.code >= 300
        has_final = status and status.code < 300 and status.code >= 200
        has_prov = status and status.code < 200

        if has_reject:
            # regardless of the session state, send this out
            return self.send("final non-2xx", FINISH, msg, req)
        elif has_final:
            if s in (REQUEST_EMPTY,):
                if sdp:
                    assert not is_answer
                    add_sdp(msg, sdp)
                    return self.send("final 2xx with offer", FINAL_OFFER, msg, req)
                else:
                    return self.send("final 2xx needs offer", ABORT)
            elif s in (PROVISIONAL_OFFER,):
                if sdp:
                    return self.send("final 2xx with unexpected SDP", ABORT)
                else:
                    add_sdp(msg, self.provisional_sdp)
                    return self.send("final 2xx with repeated offer", FINAL_OFFER, msg, req)
            elif s in (REQUEST_OFFER,):
                if sdp:
                    assert is_answer
                    add_sdp(msg, sdp)
                    return self.send("final 2xx with answer", FINAL_ANSWER, msg, req)
                else:
                    return self.send("final 2xx needs answer", ABORT)
            elif s in (PROVISIONAL_ANSWER,):
                if sdp:
                    return self.send("final 2xx with unexpected SDP", ABORT)
                else:
                    add_sdp(msg, self.provisional_sdp)
                    return self.send("final 2xx with repeated answer", FINAL_ANSWER, msg, req)
            elif s in (EARLY_SESSION,):
                if sdp:
                    return self.send("final 2xx with unexpected SDP", ABORT)
                else:
                    return self.send("final 2xx after session", FINAL_EMPTY, msg, req)
            elif s in (PRACK_OFFER,):
                if sdp:
                    assert is_answer
                    add_sdp(msg, sdp)
                    pra = self.unanswered_prack
                    self.unanswered_prack = None
                    
                    return self.send("prack response with answer", EARLY_SESSION, msg, pra)
                else:
                    return self.send("prack response needs answer", ABORT)
            else:
                return self.send("unexpected final 2xx", ABORT)
        elif has_prov:
            if s in (REQUEST_EMPTY,):
                if sdp:
                    assert not is_answer
                    add_sdp(msg, sdp)
                    
                    if self.use_rpr:
                        return self.send("rpr with offer", RELIABLE_OFFER, msg, req)
                    else:
                        self.provisional_sdp = sdp
                        return self.send("prov with offer", PROVISIONAL_OFFER, msg, req)
                else:
                    if self.use_rpr:
                        # The first reliable response must contain the offer.
                        # While it may be an option to send an unreliable response
                        # without an offer if the caller didn't require 100rel, we
                        # don't want to mix the two modes, so just don't send anything.
                        return self.send("omitted without offer", KEEP)
                    else:
                        return self.send("prov without offer", KEEP, msg, req)
            elif s in (PROVISIONAL_OFFER,):
                if sdp:
                    return self.send("prov with unexpected SDP", ABORT)
                else:
                    add_sdp(msg, self.provisional_sdp)

                    if self.use_rpr:
                        return self.send("rpr with repeated offer", RELIABLE_OFFER, msg, req)
                    else:
                        return self.send("prov with repeated offer", KEEP, msg, req)
            elif s in (REQUEST_OFFER,):
                if sdp:
                    assert is_answer
                    add_sdp(msg, sdp)
                    
                    if self.use_rpr:
                        return self.send("rpr with answer", RELIABLE_ANSWER, msg, req)
                    else:
                        self.provisional_sdp = sdp
                        return self.send("prov with answer", PROVISIONAL_ANSWER, msg, req)
                else:
                    if self.use_rpr:
                        return self.send("rpr without answer", RELIABLE_PREANSWER, msg, req)
                    else:
                        return self.send("prov without answer", KEEP, msg, req)
            elif s in (PROVISIONAL_ANSWER,):
                if sdp:
                    return self.send("prov with unexpected SDP", ABORT)
                else:
                    add_sdp(msg, self.provisional_sdp)

                    if self.use_rpr:
                        return self.send("rpr with repeated answer", RELIABLE_ANSWER, msg, req)
                    else:
                        return self.send("prov with repeated answer", KEEP, msg, req)
            elif s in (EARLY_SESSION,):
                if sdp:
                    return self.send("prov with unexpected SDP", ABORT)
                else:
                    return self.send("rpr after session", RELIABLE_POSTANSWER, msg, req)
            else:
                return self.send("unexpected prov", ABORT)
        else:
            return self.send("unexpected response", ABORT)


    def process_incoming(self, msg):
        s = self.state
        req = self.request
        
        method = msg["method"]
        sdp = get_sdp(msg)

        if method == "INVITE":
            if s == START:
                if self.use_rpr and "100rel" not in msg.get("supported", set()):
                    self.send_message(dict(status=Status(421, "100rel required")), msg)
                    return self.recv("peer does not support 100rel", ABORT)
                elif not self.use_rpr and "100rel" in msg.get("require", set()):
                    self.send_message(dict(status=Status(420, "100rel unsupported")), msg)
                    return self.recv("peer requires 100rel", ABORT)
            
                self.request = msg
                
                if sdp:
                    return self.recv("request with offer", REQUEST_OFFER, msg, sdp, False)
                else:
                    # This is the only known place to generate a session query
                    return self.recv("request without offer", REQUEST_EMPTY, msg, None, False)
            else:
                return self.recv("INVITE request after started", ABORT)

        elif method == "CANCEL":
            # A gem from 9.2:
            #   the To tag of the response to the CANCEL and the To tag
            #   in the response to the original request SHOULD be the same.
            # Which response, darling? The 100 Trying didn't have To tag, the others did.
            res = dict(status=Status(200))
            self.send_message(res, msg)
        
            res = dict(status=Status(487))  # ?
            self.send_message(res, self.request)
        
            return self.recv("cancelled", FINISH, msg, None)

        elif method == "PRACK":
            rseq, rcseq, rmethod = msg["rack"]
    
            if rmethod != req["method"] or rcseq != req["cseq"] or rseq != self.rpr_last_rseq:
                prr = dict(status=Status(481))
                self.send_message(prr, msg)
                return self.recv("PRACK for wrong rpr", KEEP)
            
            # Stop the retransmission of the rpr
            self.send_message(make_virtual_response(), req)
                
            if s in (RELIABLE_OFFER,):
                if sdp:
                    prr = dict(status=Status(200))
                    self.send_message(prr, msg)
                    return self.recv("PRACK with answer", EARLY_SESSION, msg, sdp, True)
                else:
                    return self.recv("PRACK needs answer", ABORT)
            elif s in (RELIABLE_ANSWER,):
                if sdp:
                    # Bleh
                    self.unanswered_prack = msg
                    return self.recv("PRACK with offer", PRACK_OFFER, msg, sdp, False)
                else:
                    prr = dict(status=Status(200))
                    self.send_message(prr, msg)
                    return self.recv("plain PRACK", EARLY_SESSION, msg)
            elif s in (RELIABLE_PREANSWER,):
                if sdp:
                    return self.recv("preanswer PRACK with unexpected SDP", ABORT)
                else:
                    prr = dict(status=Status(200))
                    self.send_message(prr, msg)
                    return self.recv("preanswer PRACK", REQUEST_OFFER, msg)
            elif s in (RELIABLE_POSTANSWER,):
                if sdp:
                    return self.recv("postanswer PRACK with unexpected SDP", ABORT)
                else:
                    prr = dict(status=Status(200))
                    self.send_message(prr, msg)
                    return self.recv("postanswer PRACK", EARLY_SESSION, msg)
            else:
                return self.recv("unexpected PRACK", ABORT)
                    
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
            return self.recv("NAK", ABORT)
