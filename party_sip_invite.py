from format import Status, Rack, make_virtual_response
from sdp import add_sdp, get_sdp
from util import Loggable
import zap


START = "START"

REQUEST_OFFER = "REQUEST_OFFER"
# no need for PROVISIONAL_EMPTY
PROVISIONAL_ANSWER = "PROVISIONAL_ANSWER"
RELIABLE_NOANSWER = "RELIABLE_NOANSWER"  # wait for PRACK only, then RO
RELIABLE_ANSWER = "RELIABLE_ANSWER"  # wait for PRACK
PRACK_OFFER = "PRACK_OFFER"  # wait for answer

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
    def __init__(self, use_rpr):
        Loggable.__init__(self)
        
        self.message_slot = zap.EventSlot()
        
        self.state = START
        self.request = None
        
        self.use_rpr = use_rpr
        self.rpr_last_rseq = 0


    def send_message(self, msg, related_msg=None):
        self.message_slot.zap(msg, related_msg)
        

    def process_outgoing(self, message, sdp):
        raise NotImplementedError()
            
            
    def process_incoming(self, msg):
        raise NotImplementedError()
        
        
    def is_finished(self):
        return self.state == FINISH


    def is_session_established(self):
        return self.state in (EARLY_SESSION, RELIABLE_EMPTY, FINAL_EMPTY, FINISH)


    def is_clogged(self):
        raise NotImplementedError()


    # FIXME: use a separate send_request and send_response in each
    # subclasses! And let the user pimp messages there, pass only a simple
    # dict to them from here!
    
    #def send_message(self, message, related):
    #    raise NotImplementedError()

    # FIXME: maybe even a single status or method string? Only the initial
    # invite has meaningful fields. But those are set in SipLeg.

    def send(self, log, state=None, msg=None, rel=None):
        if state:
            self.logger.debug("Changing state %s => %s" % (self.state, state))
            self.state = state
        
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
        self.logger.info(message)
        return None, None, None


class InviteClientState(InviteState):
    def __init__(self, use_rpr):
        InviteState.__init__(self, use_rpr)
        
        self.unanswered_rpr = None


    def is_clogged(self):
        return self.state in (PROVISIONAL_OFFER,)
        

    def make_prack(self, rpr, sdp=None):
        rack = Rack(rpr["rseq"], rpr["cseq"], rpr["method"])
        req = dict(method="PRACK", rack=rack)
        
        if sdp:
            add_sdp(req, sdp)
            
        return req


    def process_outgoing(self, msg, sdp=None):
        s = self.state
        req = self.request
        method = msg["method"] if msg else None

        # CANCEL is possible in all states
        if method == "CANCEL":
            if s == START:
                raise Error("Bad CANCEL!")
                
            return self.send("cancel", None, msg, req)
        elif method == "INVITE":
            if s != START:
                raise Error("Bad INVITE!")
            
            if self.use_rpr:
                msg.setdefault("supported", set()).add("100rel")
                msg.setdefault("require", set()).add("100rel")
            
            self.request = msg
                
            if sdp:
                add_sdp(msg, sdp)
                return self.send("request with offer", REQUEST_OFFER, msg, None)
            else:
                return self.send("request without offer", REQUEST_EMPTY, msg, None)
        elif method:
            # PRACK request is always implicit
            raise Error("Bad outgoing request!")
        elif sdp:
            if s == START:
                return self.abort("offer before INVITE")
            elif s == PROVISIONAL_OFFER:
                return self.abort("clogged answer for provisional offer")
            elif s == RELIABLE_OFFER:
                rpr = self.unanswered_rpr
                self.unanswered_rpr = None
                
                pra = self.make_prack(rpr, sdp)
                return self.send("PRACK with answer", EARLY_SESSION, pra, None)
            elif s == FINAL_OFFER:
                ack = add_sdp(dict(method="ACK"), sdp)
                
                return self.send("ACK with answer", FINISH, ack, req)
            else:
                raise Error("Invalid outgoing SDP in state %s!" % s)

                
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
                        # The Snom sends such SDP
                        ack = dict(method="ACK")
                        self.send("ACK", None, ack, req)
                        return self.recv("final response with ignored SDP", FINISH, msg)
                    else:
                        ack = dict(method="ACK")
                        self.send("ACK", None, ack, req)
                        return self.recv("final response", FINISH, msg)
                else:
                    # This can be a duplicate final
                    return self.ignore("ignoring unexpected final response!")
            elif has_rpr:
                rseq = msg["rseq"]
                
                if rseq <= self.rpr_last_rseq:
                    return self.recv("ignoring duplicate rpr", None, None)
                elif rseq > self.rpr_last_rseq + 1:
                    return self.recv("ignoring out of order rpr", None, None)

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
                    return self.abort("unexpected reliable response")
            else:
                if self.use_rpr:
                    return self.abort("disallowed unreliable provisional response")
                elif s in (REQUEST_OFFER,):
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
                    return self.abort("unexpected provisional response")

        elif method == "PRACK":
            if has_reject:
                return self.abort("rejected PRACK")
            elif has_final:
                # We never send offers in PRACK requests, so the response is irrelevant
                return self.recv("PRACK response", None, msg)
            else:
                return self.ignore("provisional PRACK response")
        
        else:
            return self.abort("unexpected response")


# Invite server

class InviteServerState(InviteState):
    def __init__(self, use_rpr):
        InviteState.__init__(self, use_rpr)

        self.unanswered_prack = None  # only used for PRACK offers
        self.provisional_sdp = None  # only used without rpr


    def send(self, log, state=None, msg=None, rel=None):
        if state in (RELIABLE_NOANSWER, RELIABLE_OFFER, RELIABLE_ANSWER, RELIABLE_EMPTY):
            self.provisional_sdp = None
            msg.setdefault("require", set()).add("100rel")
            
            self.rpr_last_rseq += 1
            msg["rseq"] = self.rpr_last_rseq
            
        InviteState.send(self, log, state, msg, rel)


    def is_clogged(self):
        # After sending an rpr we must wait until the corresponding PRACK before we
        # can handle the next outgoing message. Better tell our owner beforehand.
        return self.state in (RELIABLE_NOANSWER, RELIABLE_ANSWER, RELIABLE_OFFER, RELIABLE_EMPTY)
        

    def process_outgoing(self, msg, sdp=None):
        s = self.state
        req = self.request
        
        status = msg["status"] if msg else None
        has_reject = status and status.code >= 300
        has_final = status and status.code < 300 and status.code >= 200
        has_prov = status and status.code < 200

        if not sdp:
            # This should be repeated in provisional and final responses, once sent.
            # If rpr is supported, we never set it.
            sdp = self.provisional_sdp

        if has_reject:
            # regardless of the session state, send this out
            return self.send("final non-2xx", FINISH, msg, req)
        elif has_final:
            if s in (REQUEST_EMPTY, PROVISIONAL_OFFER):
                if sdp:
                    add_sdp(msg, sdp)
                    return self.send("final 2xx with offer", FINAL_OFFER, msg, req)
                else:
                    return self.abort("final 2xx needs offer")
            elif s in (REQUEST_OFFER, PROVISIONAL_ANSWER):
                if sdp:
                    add_sdp(msg, sdp)
                    return self.send("final 2xx with answer", FINAL_ANSWER, msg, req)
                else:
                    return self.abort("final 2xx needs answer")
            elif s in (EARLY_SESSION,):
                return self.send("final 2xx after session", FINAL_EMPTY, msg, req)
        elif has_prov:  # including rpr
            if s in (REQUEST_EMPTY, PROVISIONAL_OFFER):
                if sdp:
                    add_sdp(msg, sdp)
                    
                    if self.use_rpr:
                        return self.send("rpr with offer", RELIABLE_OFFER, msg, req)
                    else:
                        # FIXME: check first!
                        self.provisional_sdp = sdp
                        return self.send("prov with offer", PROVISIONAL_OFFER, msg, req)
                else:
                    if self.use_rpr:
                        # must send an offer in the first provisional response
                        return self.send("waiting for offer", None, None, None)
                    else:
                        return self.send("prov without offer", None, msg, req)
            elif s in (REQUEST_OFFER, PROVISIONAL_ANSWER):
                if sdp:
                    add_sdp(msg, sdp)
                    
                    if self.use_rpr:
                        return self.send("rpr with answer", RELIABLE_ANSWER, msg, req)
                    else:
                        # FIXME: check first!
                        self.provisional_sdp = sdp
                        return self.send("prov with answer", PROVISIONAL_ANSWER, msg, req)
                else:
                    if self.use_rpr:
                        return self.send("rpr without answer", RELIABLE_NOANSWER, msg, req)
                    else:
                        return self.send("prov without answer", None, msg, req)
            elif s in (EARLY_SESSION,):
                # seems like we can do rpr, but can't send further sessions in rpr
                return self.send("rpr after session", RELIABLE_EMPTY, msg, req)
        else:
            if s in (REQUEST_EMPTY, PROVISIONAL_OFFER):
                if sdp:
                    msg = add_sdp(dict(status=Status(183)), sdp)
                    
                    if self.use_rpr:
                        return self.send("session progress rpr with offer", RELIABLE_OFFER, msg, req)
                    else:
                        return self.send("session progress with offer", PROVISIONAL_OFFER, msg, req)
                else:
                    raise Exception("Nothing to do!")
            elif s in (REQUEST_OFFER, PROVISIONAL_ANSWER):
                if sdp:
                    msg = add_sdp(dict(status=Status(183)), sdp)
                    
                    if self.use_rpr:
                        return self.send("session progress rpr with answer", RELIABLE_ANSWER, msg, req)
                    else:
                        return self.send("session progress with answer", PROVISIONAL_ANSWER, msg, req)
                else:
                    raise Exception("Nothing to do!")
            elif s in (PRACK_OFFER,):
                if sdp:
                    msg = add_sdp(dict(status=Status(200, "Not Happy")), sdp)
                    pra = self.unanswered_prack
                    self.unanswered_prack = None
                    
                    return self.send("prack response with answer", EARLY_SESSION, msg, pra)
                else:
                    raise Exception("Nothing to do!")
            else:
                return self.abort("Huh?")


    def process_incoming(self, msg):
        s = self.state
        req = self.request
        
        method = msg["method"]
        sdp = get_sdp(msg)

        if method == "INVITE":
            if s == START:
                self.request = msg
                
                if sdp:
                    return self.recv("request with offer", REQUEST_OFFER, msg, sdp, False)
                else:
                    return self.recv("request without offer", REQUEST_EMPTY, msg, None)
            else:
                return self.abort("Not an INVITE request is received!")

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
