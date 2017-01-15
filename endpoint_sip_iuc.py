from collections import namedtuple

from format import Status, Rack, Sip
from sdp import add_sdp, get_sdp, has_sdp
from log import Loggable


ESdp = namedtuple("ESdp", [ "sdp", "is_answer" ])


class InviteUpdateComplex(Loggable):
    def __init__(self, sip_endpoint, use_rpr):
        Loggable.__init__(self)
        
        self.sip_endpoint = sip_endpoint
        self.use_rpr = use_rpr
        self.is_ever_finished = False
        
        self.outgoing_response_queue = []
        self.outgoing_sdp_queue = []

        self.unresponded_invite = None
        self.unacked_final = None
        self.unpracked_rpr = None
        self.unresponded_prack = None
        self.unresponded_update = None
        self.unresponded_cancel = None
        
        self.is_client = None
        self.is_queried = None
        self.is_established = None
        self.invite_response_sdp = None
        self.rpr_last_rseq = None


    def send_message(self, msg):
        self.sip_endpoint.send(msg)
        
        
    def make_rack(self, rpr):
        return Rack(rpr["rseq"], rpr["cseq"], rpr.method)


    def is_finished(self):
        return self.is_ever_finished
        
        
    def is_busy(self):
        return (
            self.unresponded_invite or
            self.unacked_final or
            self.unpracked_rpr or
            self.unresponded_prack or
            self.unresponded_update or
            self.unresponded_cancel
        )

    
    def reset(self, is_client, is_queried):
        self.logger.info("Reseting state: %s/%s." % ("client" if is_client else "server", "queried" if is_queried else "offered"))
        
        self.is_client = is_client
        self.is_queried = is_queried
        self.is_established = False
        self.invite_response_sdp = None
        self.rpr_last_rseq = 0
        

    def retry(self):
        # Events that can trigger further processing:
        # Client side:
        #   * Receiving a final INVITE response can trigger an outgoing ACK with answer
        #   * Receiving a reliable INVITE response can trigger an outgoing PRACK with answer
        #   * Receiving a final INVITE response or UPDATE response can trigger an
        #     outgoing re-INVITE with a query
        #   * Receiving a PRACK response can trigger an outgoing UPDATE with an offer
        # Server side:
        #   * Receiving a PRACK request can trigger an outgoing INVITE response or UPDATE request
        #   * Receiving an ACK request or UPDATE response can trigger an outgoing re-INVITE
        
        if self.outgoing_response_queue:
            self.logger.info("Retrying %d outgoing responses." % len(self.outgoing_response_queue))
            
            responses = self.outgoing_response_queue
            self.outgoing_response_queue = []
            
            for response in responses:
                self.out_server(response)
                
        if self.outgoing_sdp_queue:
            self.logger.info("Retrying %d outgoing sessions." % len(self.outgoing_sdp_queue))
            
            items = self.outgoing_sdp_queue
            self.outgoing_sdp_queue = []
            
            for sdp, is_answer in items:
                self.out_sdp(sdp, is_answer)


    def may_finish(self):
        if not self.is_busy():
            if self.outgoing_response_queue:
                raise Exception("Still have queued responses, bad reset!")
                
            self.is_ever_finished = True
            self.retry()
        else:
            self.logger.info("Can't finish yet.")

            
    # Client
    # Outgoing INVITE, CANCEL are never queued.
    # Outgoing sdp may be queued, and retried after an incoming message.
    
    def out_client(self, request):
        if request.method == "INVITE":
            self.logger.info("Sending INVITE request.")
            
            if self.is_busy():
                raise Exception("INVITE already in progress!")
            
            if not self.outgoing_sdp_queue:
                raise Exception("INVITE request has no queued session!")
                
            sdp, is_answer = self.outgoing_sdp_queue.pop(0)
            
            if is_answer:
                self.logger.error("INVITE request can't take a session answer!")
                return
            elif sdp:
                self.logger.info("INVITE request taking a session offer.")
                add_sdp(request, sdp)
            else:
                self.logger.info("INVITE request taking a session query.")

            if self.use_rpr:
                request.setdefault("require", set()).add("100rel")
                request.setdefault("supported", set()).add("100rel")
            
            self.send_message(request)
            
            self.reset(is_client=True, is_queried=not sdp)
            self.unresponded_invite = request
            return
        elif request.method == "CANCEL":
            self.logger.info("Sending CANCEL request.")
            self.unresponded_cancel = request
            self.send_message(request)
            return
        else:
            raise Exception("Invalid explicit client request: %s!" % request.method)
        
        
    def in_client(self, response):
        if response.method == "INVITE":
            self.logger.info("Processing INVITE response.")
            
            if not self.unresponded_invite:
                self.logger.warning("No INVITE in progress, ignoring response!")
                return None, None, None
            
            sdp = get_sdp(response)
            is_answer = None
        
            if sdp:
                if not self.invite_response_sdp:
                    self.invite_response_sdp = sdp
                    is_answer = not self.is_queried
                    self.logger.info("Using SDP from INVITE response as %s." % ("answer" if is_answer else "offer"))
                else:
                    self.logger.info("Ignoring duplicate SDP from INVITE response.")
                    sdp = None
            else:
                self.logger.info("No SDP in INVITE response.")
        
            if response.status.code < 200:
                if "100rel" in response.get("require", set()):
                    self.unpracked_rpr = response
                
                    if sdp:
                        if not is_answer:
                            self.logger.info("Got reliable offer, waiting for answer to PRACK.")
                            self.retry()  # may be queued already
                            return response, sdp, is_answer
                        else:
                            # Got answer, send PRACK
                            self.logger.info("Got reliable answer, sending empty PRACK.")
                            request = Sip.request(method="PRACK")
                            request["rack"] = self.make_rack(self.unpracked_rpr)
                            self.send_message(request)
                        
                            self.is_established = True
                            self.unpracked_rpr = None
                            self.unresponded_prack = request
                            return response, sdp, is_answer
                    else:
                        if self.is_queried and not self.is_established:
                            self.logger.warning("Nasty peer sent an empty reliable response before the offer, welp!")
                        else:
                            self.logger.info("Got empty reliable response, sending empty PRACK.")
                            
                        request = Sip.request(method="PRACK")
                        request["rack"] = self.make_rack(self.unpracked_rpr)
                        self.send_message(request)
                    
                        self.unpracked_rpr = None
                        self.unresponded_prack = request
                        return response, None, None
                else:
                    if sdp:
                        if not is_answer:
                            self.logger.info("Got unreliable response with offer.")
                        else:
                            self.logger.info("Got unreliable response with answer.")
                    else:
                        self.logger.info("Got empty unreliable response.")
                        
                    return response, sdp, is_answer
            elif response.status.code < 300:
                self.unresponded_invite = None
                self.unacked_final = response
            
                if sdp:
                    if not is_answer:
                        self.logger.info("Got final offer, waiting for answer to ACK.")
                        self.retry()  # may be queued already
                        return response, sdp, is_answer
                    else:
                        self.logger.info("Got final answer, sending empty ACK.")
                        request = Sip.request(method="ACK", related=response)
                        self.send_message(request)
                
                        self.unacked_final = None
                        self.may_finish()
                        return response, sdp, is_answer
                else:
                    self.logger.info("Got empty final response, sending empty ACK.")
                    request = Sip.request(method="ACK", related=response)
                    self.send_message(request)
                
                    self.unacked_final = None
                    self.may_finish()
                    return response, None, None
            else:
                self.logger.info("Got final rejection, no need to ACK here.")
                self.unresponded_invite = None
                self.may_finish()
                return response, None, None
                                
        elif response.method == "PRACK":
            if has_sdp(response):
                self.logger.warning("Got PRACK response with unexpected SDP, ignoring!")
            else:
                self.logger.info("Got empty PRACK response.")
                
            self.unresponded_prack = None
            self.retry()
            
            return response, None, None
        elif response.method == "CANCEL":
            self.logger.info("Got CANCEL response.")
            self.unresponded_cancel = None
            self.may_finish()
            return response, None, None


    def queue_sdp(self, sdp, is_answer):
        if sdp is None and is_answer is None:
            raise Exception("Wanted to queue an empty session!")
            
        what = ("accept" if is_answer else "offer") if sdp else ("reject" if is_answer else "query")
        self.logger.info("Queuing outgoing session %s." % what)
        self.outgoing_sdp_queue.append((sdp, is_answer))
        

    def out_sdp(self, sdp, is_answer):
        if sdp is None and is_answer is None:
            raise Exception("Wanted to send an empty session!")

        if self.unresponded_update and is_answer:
            self.logger.info("Sending UPDATE response with answer.")
            status = Status(200 if sdp else 488)
            response = Sip.response(status=status, related=self.unresponded_update)
            add_sdp(response, sdp)
            self.send_message(response)
            
            self.unresponded_update = None
            return True
        elif not self.unresponded_update and self.is_established and sdp and not is_answer:
            self.logger.info("Sending UPDATE request with offer.")
            request = Sip.request(method="UPDATE")
            add_sdp(request, sdp)
            self.send_message(request)
            
            self.unresponded_update = request
            return True
        elif not self.is_busy():
            if is_answer:
                self.logger.error("Can't start a re-INVITE with a session answer, ignoring!")
                return
                
            self.logger.info("Sending INVITE request with %s." % ("query" if not sdp else "offer"))
            self.queue_sdp(sdp, is_answer)
            self.out_client(Sip.request(method="INVITE"))
            return True
        elif not sdp and not is_answer:
            self.logger.info("Can't send query in current INVITE, must queue.")
            self.queue_sdp(sdp, is_answer)
            return False
        elif self.is_client:
            if self.unpracked_rpr and is_answer:
                self.logger.info("Sending PRACK request with answer.")
                request = Sip.request(method="PRACK")
                request["rack"] = self.make_rack(self.unpracked_rpr)
                add_sdp(request, sdp)
                self.send_message(request)
                
                self.is_established = True
                self.unpracked_rpr = None
                self.unresponded_prack = request
                return True
            elif self.unacked_final and is_answer:
                self.logger.info("Sending ACK request with answer.")
                # Can't reject in ACK
                request = Sip.request(method="ACK", related=self.unacked_final)
                add_sdp(request, sdp)
                self.send_message(request)

                self.unacked_final = None
                return True
            else:
                self.logger.info("Can't send session in current INVITE, must queue.")
                self.queue_sdp(sdp, is_answer)
                return False
        else:
            if self.unresponded_prack and is_answer:
                self.logger.info("Sending PRACK response with answer.")
                # If the client dared to send a PRACK offer, then we dare to reject it.
                status = Status(200 if sdp else 488)
                response = Sip.response(status=status, related=self.unresponded_prack)
                add_sdp(response, sdp)
                self.send_message(response)
                
                self.unresponded_prack = None
                return True
            elif self.outgoing_response_queue:
                self.logger.info("Sending queued INVITE response with session.")
                self.queue_sdp(sdp, is_answer)
                self.retry()
                return True
            # We never send an INVITE response by ourselves, because it may be included
            # in an explicit response.
            else:
                self.logger.info("Can't send session in current INVITE, must queue.")
                self.queue_sdp(sdp, is_answer)
                return False
                
    
    # Server
    # Outgoing INVITE response may be queued.
    # Outgoing session may be queued.
    
    def in_server(self, request):
        if request.method == "INVITE":
            if self.is_busy():
                self.logger.warning("Got conflicting INVITE request, rejecting!")
                response = Sip.response(status=Status(400), related=request)
                self.send_message(response)
                return None, None, None

            sdp = get_sdp(request)
            is_answer = False
            self.logger.info("Got INVITE request with %s." % ("offer" if sdp else "query"))
            
            self.reset(is_client=False, is_queried=not sdp)
            self.unresponded_invite = request
            return request, sdp, is_answer
        elif request.method == "CANCEL":
            if not self.unresponded_invite:
                self.logger.warning("Got unexpected CANCEL request, rejecting!")
                response = Sip.response(status=Status(481), related=request)
                self.send_message(response)
                return None, None, None

            self.logger.info("Got CANCEL request.")
            
            response = Sip.response(status=Status(200), related=request)
            self.send_message(response)
            
            response = Sip.response(status=Status(487), related=self.unresponded_invite)
            self.send_message(response)
            
            self.unresponded_invite = None
            self.unacked_final = response
            return request, None, None
        elif request.method == "PRACK":
            if not self.unpracked_rpr:
                self.logger.warning("Got unexpected PRACK request, rejecting!")
                response = Sip.response(status=Status(400), related=request)
                self.send_message(response)
                return None, None, None

            rseq, rcseq, rmethod = request["rack"]
            rpr = self.unpracked_rpr
            
            if rmethod != rpr.related.method or rcseq != rpr.related["cseq"] or rseq != rpr["rseq"]:
                self.logger.warning("Got PRACK request with bad parameters, rejecting!")
                response = Sip.response(status=Status(400), related=request)
                self.send_message(response)
                return None, None, None

            sdp = get_sdp(request)
            is_answer = self.is_queried if sdp else None
            
            if not sdp:
                if has_sdp(self.unpracked_rpr) and self.is_queried:
                    self.logger.warning("Got PRACK request with missing answer!")
                    response = Sip.response(status=Status(400), related=request)
                    self.send_message(response)
                    
                    self.unpracked_rpr = None
                    self.retry()
                    return request, None, None
                else:
                    self.logger.info("Got empty PRACK request.")
                    response = Sip.response(status=Status(200), related=request)
                    self.send_message(response)
                    
                    self.unpracked_rpr = None
                    self.retry()
                    return request, None, None
            else:
                if has_sdp(self.unpracked_rpr):
                    if self.is_queried:
                        self.logger.info("Got PRACK request with answer.")
                        response = Sip.response(status=Status(200), related=request)
                        self.send_message(response)
                    
                        self.is_established = True
                        self.unpracked_rpr = None
                        self.retry()
                        return request, sdp, is_answer
                    else:
                        self.logger.info("Got PRACK request with offer.")
                    
                        self.unpracked_rpr = None
                        self.unresponded_prack = request
                        self.retry()
                        return request, sdp, is_answer
                else:
                    self.logger.warning("Got PRACK request with unexpected SDP, rejecting!")
                    response = Sip.response(status=Status(400), related=request)
                    self.send_message(response)
                    
                    self.unpracked_rpr = None
                    self.retry()
                    return None, None, None
        elif request.method == "ACK":
            if not self.unacked_final:
                self.logger.warning("Got unexpected ACK, ignoring!")
                return None, None, None
                
            sdp = get_sdp(request)
            is_answer = self.is_queried if sdp else None
            
            if not sdp:
                if self.is_queried and not self.is_established:
                    self.logger.warning("Got ACK with missing SDP, ignoring!")
                    return None, None, None
                else:
                    self.logger.info("Got empty ACK.")
            
                self.unacked_final = None
                self.may_finish()
                return request, sdp, is_answer
            else:
                if self.is_queried:
                    self.logger.info("Got ACK with answer.")
                    self.unacked_final = None
                    self.may_finish()
                    return request, sdp, is_answer
                else:
                    self.logger.warning("Got ACK with unexpected SDP!")
                    self.unacked_final = None
                    self.may_finish()
                    return request, sdp, is_answer
        else:
            self.logger.warning("Got unexpected request!")
        
        
    def out_server(self, response):
        if not self.unresponded_invite:
            raise Exception("No INVITE in progress!")

        self.logger.info("Sending INVITE response %d." % response.status.code)
            
        if self.unpracked_rpr or self.unresponded_prack:
            self.logger.info("A PRACK is in progress, queueing response.")
            self.outgoing_response_queue.append(response)
            return
            
        response.related = self.unresponded_invite

        # Take an outgoing session if available
        sdp, is_answer = self.outgoing_sdp_queue[0] if self.outgoing_sdp_queue else (None, None)
        
        if response.status.code >= 300:
            self.logger.info("No session needed, because it's a non-2xx response.")
        elif self.is_established:
            self.logger.info("No session needed, because it's already established.")
        elif self.invite_response_sdp:
            self.logger.info("No session needed, because already responded one.")
            sdp = self.invite_response_sdp
            is_answer = not self.is_queried
        elif is_answer is None:
            self.logger.info("No queued session.")
        elif not self.is_queried and is_answer:
            self.logger.info("Unqueueing answer.")
            self.outgoing_sdp_queue.pop(0)
            if sdp:
                add_sdp(response, sdp)
        elif self.is_queried and not is_answer and sdp:
            self.logger.info("Unqueueing offer.")
            self.outgoing_sdp_queue.pop(0)
            add_sdp(response, sdp)
        else:
            self.logger.error("Wrong type of session queued, dropping!")
            self.outgoing_sdp_queue.pop(0)
            sdp = None
            is_answer = None
            
        if sdp:
            add_sdp(response, sdp)
            
        if response.status.code < 200:
            if sdp:
                self.invite_response_sdp = sdp
                
            if self.use_rpr:
                if not sdp:
                    if self.is_queried and not self.is_established:
                        self.logger.info("Can't send reliable INVITE response before offer, must queue.")
                        self.outgoing_response_queue.append(response)
                        return
                    else:
                        self.logger.info("Sending empty reliable INVITE response.")
                else:
                    if not is_answer:
                        self.logger.info("Sending reliable INVITE response with offer.")
                    else:
                        self.logger.info("Sending reliable INVITE response with answer.")
                        self.is_established = True
                
                response.setdefault("require", set()).add("100rel")
                self.rpr_last_rseq += 1
                response["rseq"] = self.rpr_last_rseq
                self.send_message(response)
                
                self.unpracked_rpr = response
                return
            else:
                if not sdp:
                    self.logger.info("Sending empty unreliable INVITE response.")
                else:
                    if not is_answer:
                        self.logger.info("Sending unreliable INVITE response with offer.")
                    else:
                        self.logger.info("Sending unreliable INVITE response with answer.")
                        
                self.send_message(response)
                return
        elif response.status.code < 300:
            if not sdp:
                if is_answer:
                    self.logger.warning("Can't keep status of final INVITE response with reject!")
                    response.status = Status(488)
                else:
                    # queries were already checked
                    self.logger.info("Sending empty final INVITE response.")
            else:
                if not is_answer:
                    self.logger.info("Sending final INVITE response with offer.")
                else:
                    self.logger.info("Sending final INVITE response with answer.")
                    
            self.send_message(response)
            
            self.unacked_final = response
            self.invite_response_sdp = None
            return
        else:
            self.logger.info("Sending rejecting INVITE response.")
            self.send_message(response)
            
            self.unacked_final = response
            self.invite_response_sdp = None
            return
            
        
    # UPDATE

    def in_update(self, message):
        if not message.is_response:
            request = message
            
            if self.unresponded_invite and not self.is_established:
                self.logger.warning("Got UPDATE request while INVITE unestablished, rejecting!")
                response = Sip.response(status=Status(491), related=request)
                self.send_message(response)
                return None, None, None
                
            if self.unresponded_update:
                self.logger.warning("Got conflicting UPDATE request, rejecting!")
                response = Sip.response(status=Status(491), related=request)
                self.send_message(response)
                return None, None, None
                
            self.logger.info("Got UPDATE request with offer.")
            sdp = get_sdp(request)
            is_answer = False
            
            self.unresponded_update = request
            return request, sdp, is_answer
        else:
            response = message
            
            if not self.unresponded_update:
                self.logger.warning("Got unexpected UPDATE response, ignoring!")
                return None, None, None
            
            self.logger.info("Got UPDATE response with answer.")
            sdp = get_sdp(response)
            is_answer = True
            
            self.unresponded_update = None
            return response, sdp, is_answer


    def in_generic(self, message):
        if message.method == "UPDATE":
            return self.in_update(message)
        elif message.method in ("INVITE", "ACK", "NAK", "PRACK", "CANCEL"):
            if not message.is_response:
                return self.in_server(message)
            else:
                return self.in_client(message)
        else:
            self.logger.warning("Unexpected message: %s" % message.method)
