from format import Status, Rack, Sip
from sdp import add_sdp, get_sdp, has_sdp, SdpBuilder, SdpParser, Session
from log import Loggable


class InviteUpdateComplex(Loggable):
    def __init__(self, sip_endpoint, use_rpr):
        Loggable.__init__(self)
        
        self.sip_endpoint = sip_endpoint
        self.use_rpr = use_rpr
        self.is_ever_finished = False

        # These are not Loggable (yet)
        self.sdp_builder = SdpBuilder()
        self.sdp_parser = SdpParser()
        
        self.outgoing_response_queue = []
        self.outgoing_session_queue = []

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
        
        
    def build_sdp(self, session):
        return self.sdp_builder.build(session)
        
        
    def parse_sdp(self, sdp, is_answer):
        return self.sdp_parser.parse(sdp, is_answer)
        
        
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
                
        if self.outgoing_session_queue:
            self.logger.info("Retrying %d outgoing sessions." % len(self.outgoing_session_queue))
            
            sessions = self.outgoing_session_queue
            self.outgoing_session_queue = []
            
            for session in sessions:
                self.out_session(session)


    def may_finish(self):
        if not self.is_busy():
            if self.outgoing_response_queue:
                raise Exception("Still have queued responses, bad reset!")
                
            self.is_ever_finished = True
            self.retry()
        else:
            what = [
                "unresponded INVITE" if self.unresponded_invite else None,
                "un-ACK-ed final" if self.unacked_final else None,
                "un-PRACK-ed rpr" if self.unpracked_rpr else None,
                "unresponded PRACK" if self.unresponded_prack else None,
                "unresponded UPDATE" if self.unresponded_update else None,
                "unresponded CANCEL" if self.unresponded_cancel else None
            ]
            self.logger.info("Can't finish yet, has %s." % ", ".join(w for w in what if w))

            
    # Client
    # Outgoing INVITE, CANCEL are never queued.
    # Outgoing sdp may be queued, and retried after an incoming message.
    
    def out_client(self, request):
        if request.method == "INVITE":
            self.logger.info("Sending INVITE request.")
            
            if self.is_busy():
                raise Exception("INVITE already in progress!")
            
            if not self.outgoing_session_queue:
                raise Exception("INVITE request has no queued session!")
                
            session = self.outgoing_session_queue.pop(0)
            
            if session.is_offer():
                self.logger.info("INVITE request taking a session offer.")
                sdp = self.build_sdp(session)
                add_sdp(request, sdp)
            elif session.is_query():
                self.logger.info("INVITE request taking a session query.")
            else:
                self.logger.error("INVITE request can't take a session answer!")
                return

            if self.use_rpr:
                request.setdefault("require", set()).add("100rel")
                request.setdefault("supported", set()).add("100rel")
            
            self.send_message(request)
            
            self.reset(is_client=True, is_queried=session.is_query())
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
                return None, None
            
            sdp = get_sdp(response)
            session = None
        
            if sdp:
                if self.invite_response_sdp:
                    self.logger.info("Ignoring duplicate SDP from INVITE response.")
                elif not self.is_queried:
                    session = self.parse_sdp(sdp, True)
                    self.invite_response_sdp = sdp
                    self.logger.info("Using SDP from INVITE response as answer.")
                else:
                    session = self.parse_sdp(sdp, False)
                    self.invite_response_sdp = sdp
                    self.logger.info("Using SDP from INVITE response as offer.")
            else:
                self.logger.info("No SDP in INVITE response.")
        
            if response.status.code < 200:
                if "100rel" in response.get("require", set()):
                    self.unpracked_rpr = response
                
                    if session:
                        if session.is_offer():
                            self.logger.info("Got reliable offer, waiting for answer to PRACK.")
                            self.retry()  # may be queued already
                            return response, session
                        else:
                            # Got answer, send PRACK
                            self.logger.info("Got reliable answer, sending empty PRACK.")
                            request = Sip.request(method="PRACK")
                            request["rack"] = self.make_rack(self.unpracked_rpr)
                            self.send_message(request)
                        
                            self.is_established = True
                            self.unpracked_rpr = None
                            self.unresponded_prack = request
                            return response, session
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
                        return response, session
                else:
                    if session:
                        if session.is_offer():
                            self.logger.info("Got unreliable response with offer.")
                        else:
                            self.logger.info("Got unreliable response with answer.")
                    else:
                        self.logger.info("Got empty unreliable response.")
                        
                    return response, session
            elif response.status.code < 300:
                self.unresponded_invite = None
                self.unacked_final = response
            
                if session:
                    if session.is_offer():
                        self.logger.info("Got final offer, waiting for answer to ACK.")
                        self.retry()  # may be queued already
                        return response, session
                    else:
                        self.logger.info("Got final answer, sending empty ACK.")
                        request = Sip.request(method="ACK", related=response)
                        self.send_message(request)
                
                        self.unacked_final = None
                        self.may_finish()
                        return response, session
                else:
                    self.logger.info("Got empty final response, sending empty ACK.")
                    request = Sip.request(method="ACK", related=response)
                    self.send_message(request)
                
                    self.unacked_final = None
                    self.may_finish()
                    return response, session
            else:
                self.logger.info("Got final rejection, no need to ACK here.")
                self.unresponded_invite = None
                self.may_finish()
                return response, session
                                
        elif response.method == "PRACK":
            if has_sdp(response):
                self.logger.warning("Got PRACK response with unexpected SDP, ignoring!")
            else:
                self.logger.info("Got empty PRACK response.")
                
            self.unresponded_prack = None
            self.retry()
            
            return response, None
        elif response.method == "CANCEL":
            self.logger.info("Got CANCEL response.")
            self.unresponded_cancel = None
            self.may_finish()
            return response, None


    def queue_session(self, session):
        if not session:
            raise Exception("Wanted to queue an empty session!")
            
        what = "query" if session.is_query() else "offer" if session.is_offer() else "accept" if session.is_accept() else "reject"
        self.logger.info("Queuing outgoing session %s." % what)
        self.outgoing_session_queue.append(session)
        

    def out_session(self, session):
        if not session:
            raise Exception("Wanted to send an empty session!")

        if self.unresponded_update and (session.is_accept() or session.is_reject()):
            self.logger.info("Sending UPDATE response with answer.")
            status = Status.OK if session.is_accept() else Status.NOT_ACCEPTABLE_HERE
            response = Sip.response(status=status, related=self.unresponded_update)
            sdp = self.build_sdp(session)
            add_sdp(response, sdp)
            self.send_message(response)
            
            self.unresponded_update = None
            return True
        elif not self.unresponded_update and self.is_established and session.is_offer():
            self.logger.info("Sending UPDATE request with offer.")
            request = Sip.request(method="UPDATE")
            sdp = self.build_sdp(session)
            add_sdp(request, sdp)
            self.send_message(request)
            
            self.unresponded_update = request
            return True
        elif not self.is_busy():
            if session.is_accept() or session.is_reject():
                self.logger.error("Can't start a re-INVITE with a session answer, ignoring!")
                return
                
            self.logger.info("Sending INVITE request with %s." % ("query" if session.is_query() else "offer"))
            self.queue_session(session)
            self.out_client(Sip.request(method="INVITE"))
            return True
        elif session.is_query():
            self.logger.info("Can't send query in current INVITE, must queue.")
            self.queue_session(session)
            return False
        elif self.is_client:
            if self.unpracked_rpr and (session.is_accept() or session.is_reject()):
                self.logger.info("Sending PRACK request with answer.")
                request = Sip.request(method="PRACK")
                request["rack"] = self.make_rack(self.unpracked_rpr)
                
                if session.is_accept():
                    sdp = self.build_sdp(session)
                    add_sdp(request, sdp)
                else:
                    self.logger.warning("Oh, wait, it's a session reject... whatever!")
                    
                self.send_message(request)
                
                self.is_established = True
                self.unpracked_rpr = None
                self.unresponded_prack = request
                return True
            elif self.unacked_final and (session.is_accept() or session.is_reject()):
                self.logger.info("Sending ACK request with answer.")
                request = Sip.request(method="ACK", related=self.unacked_final)

                if session.is_accept():
                    sdp = self.build_sdp(session)
                    add_sdp(request, sdp)
                else:
                    self.logger.warning("Oh, wait, it's a session reject... whatever!")
                    
                self.send_message(request)

                self.unacked_final = None
                return True
            else:
                self.logger.info("Can't send session in current INVITE, must queue.")
                self.queue_session(session)
                return False
        else:
            if self.unresponded_prack and (session.is_accept() or session.is_reject()):
                self.logger.info("Sending PRACK response with answer.")
                
                if session.is_accept():
                    status = Status.OK
                    sdp = self.build_sdp(session)
                else:
                    self.logger.warning("Oh, wait, it's a reject... whatever!")
                    status = Status.NOT_ACCEPTABLE_HERE
                    sdp = None
                    
                response = Sip.response(status=status, related=self.unresponded_prack)
                if sdp:
                    add_sdp(response, sdp)
                self.send_message(response)
                
                self.unresponded_prack = None
                return True
            elif self.outgoing_response_queue:
                self.logger.info("Sending queued INVITE response with session.")
                self.queue_session(session)
                self.retry()
                return True
            # We never send sessions in an INVITE response, because it may be included
            # in an explicit response.
            else:
                self.logger.info("Can't send session in current INVITE, must queue.")
                self.queue_session(session)
                return False
                
    
    # Server
    # Outgoing INVITE response may be queued.
    # Outgoing session may be queued.
    
    def in_server(self, request):
        if request.method == "INVITE":
            if self.is_busy():
                self.logger.warning("Got conflicting INVITE request, rejecting!")
                response = Sip.response(status=Status.REQUEST_PENDING, related=request)
                self.send_message(response)
                return None, None

            sdp = get_sdp(request)
            session = self.parse_sdp(sdp, False) if sdp else Session.make_query()
            self.logger.info("Got INVITE request with %s." % ("offer" if sdp else "query"))
            
            self.reset(is_client=False, is_queried=not sdp)
            self.unresponded_invite = request
            return request, session
        elif request.method == "CANCEL":
            if not self.unresponded_invite:
                self.logger.warning("Got unexpected CANCEL request, rejecting!")
                response = Sip.response(status=Status.TRANSACTION_DOES_NOT_EXIST, related=request)
                self.send_message(response)
                return None, None

            self.logger.info("Got CANCEL request.")
            
            response = Sip.response(status=Status.OK, related=request)
            self.send_message(response)
            
            response = Sip.response(status=Status.REQUEST_TERMINATED, related=self.unresponded_invite)
            self.send_message(response)
            
            self.unresponded_invite = None
            self.unacked_final = response
            return request, None
        elif request.method == "PRACK":
            if not self.unpracked_rpr:
                self.logger.warning("Got unexpected PRACK request, rejecting!")
                response = Sip.response(status=Status.BAD_REQUEST, related=request)
                self.send_message(response)
                return None, None

            rseq, rcseq, rmethod = request["rack"]
            rpr = self.unpracked_rpr
            
            if rmethod != rpr.related.method or rcseq != rpr.related["cseq"] or rseq != rpr["rseq"]:
                self.logger.warning("Got PRACK request with bad parameters, rejecting!")
                response = Sip.response(status=Status.BAD_REQUEST, related=request)
                self.send_message(response)
                return None, None

            sdp = get_sdp(request)
            
            if not sdp:
                if has_sdp(self.unpracked_rpr) and self.is_queried:
                    self.logger.warning("Got PRACK request with missing answer, rejecting!")
                    response = Sip.response(status=Status.BAD_REQUEST, related=request)
                    self.send_message(response)
                    
                    self.unpracked_rpr = None
                    self.retry()
                    return request, None
                else:
                    self.logger.info("Got empty PRACK request.")
                    response = Sip.response(status=Status.OK, related=request)
                    self.send_message(response)
                    
                    self.unpracked_rpr = None
                    self.retry()
                    return request, None
            else:
                if has_sdp(self.unpracked_rpr):
                    if self.is_queried:
                        self.logger.info("Got PRACK request with answer.")
                        session = self.parse_sdp(sdp, True)
                        response = Sip.response(status=Status.OK, related=request)
                        self.send_message(response)
                    
                        self.is_established = True
                        self.unpracked_rpr = None
                        self.retry()
                        return request, session
                    else:
                        self.logger.info("Got PRACK request with new offer.")
                        session = self.parse_sdp(sdp, False)
                    
                        self.unpracked_rpr = None
                        self.unresponded_prack = request
                        self.retry()
                        return request, session
                else:
                    self.logger.warning("Got PRACK request with unexpected SDP, rejecting!")
                    response = Sip.response(status=Status.BAD_REQUEST, related=request)
                    self.send_message(response)
                    
                    self.unpracked_rpr = None
                    self.retry()
                    return None, None
        elif request.method == "ACK":
            if not self.unacked_final:
                self.logger.warning("Got unexpected ACK, ignoring!")
                return None, None
                
            sdp = get_sdp(request)
            
            if not sdp:
                if self.is_queried and not self.is_established:
                    self.logger.warning("Got ACK with missing SDP, ignoring!")
                    return None, None

                self.logger.info("Got empty ACK.")
            
                self.unacked_final = None
                self.may_finish()
                return request, None
            else:
                if self.is_queried and not self.established:
                    self.logger.info("Got ACK with answer.")
                    session = self.parse_sdp(sdp, True)
                    self.unacked_final = None
                    self.may_finish()
                    return request, session
                else:
                    self.logger.warning("Got ACK with unexpected SDP!")
                    self.unacked_final = None
                    self.may_finish()
                    return request, None
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
        session = None

        if response.status.code >= 300:
            self.logger.info("No session needed, because it's a non-2xx response.")
        elif self.is_established:
            self.logger.info("No session needed, because it's already established.")
        elif self.invite_response_sdp:
            self.logger.info("No session needed, because already responded one.")
            add_sdp(response, self.invite_response_sdp)
        elif not self.outgoing_session_queue:
            self.logger.info("No queued session.")
        else:
            session = self.outgoing_session_queue.pop(0)
            
            if not self.is_queried and (session.is_accept() or session.is_reject()):
                self.logger.info("Unqueueing answer.")
                if session.is_accept():
                    sdp = self.build_sdp(session)
                    add_sdp(response, sdp)
                    self.invite_response_sdp = sdp
            elif self.is_queried and session.is_offer():
                self.logger.info("Unqueueing offer.")
                sdp = self.build_sdp(session)
                add_sdp(response, sdp)
                self.invite_response_sdp = sdp
            else:
                self.logger.error("Wrong type of session queued, dropping!")
                session = None
            
        if response.status.code < 200:
            if self.use_rpr:
                if not session:
                    if self.is_queried and not self.is_established:
                        self.logger.info("Can't send reliable INVITE response before offer, must queue.")
                        self.outgoing_response_queue.append(response)
                        return
                    else:
                        self.logger.info("Sending empty reliable INVITE response.")
                else:
                    if session.is_offer():
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
                if not session:
                    self.logger.info("Sending empty unreliable INVITE response.")
                else:
                    if session.is_offer():
                        self.logger.info("Sending unreliable INVITE response with offer.")
                    else:
                        self.logger.info("Sending unreliable INVITE response with answer.")
                        
                self.send_message(response)
                return
        elif response.status.code < 300:
            if not session:
                self.logger.info("Sending empty final INVITE response.")
            elif session.is_offer():
                self.logger.info("Sending final INVITE response with offer.")
            elif session.is_accept():
                self.logger.info("Sending final INVITE response with answer.")
            elif session.is_reject():
                self.logger.warning("Can't keep status of final INVITE response with reject!")
                response.status = Status.NOT_ACCEPTABLE_HERE
            else:
                raise Exception("A session query slipped through the checks!")
                
            self.send_message(response)
            
            self.unresponded_invite = None
            self.unacked_final = response
            self.invite_response_sdp = None
            return
        else:
            self.logger.info("Sending rejecting INVITE response.")
            self.send_message(response)
            
            self.unresponded_invite = None
            self.unacked_final = response
            self.invite_response_sdp = None
            return
            
        
    # UPDATE

    def in_update(self, message):
        if not message.is_response:
            request = message
            
            if self.unresponded_invite and not self.is_established:
                self.logger.warning("Got UPDATE request while INVITE unestablished, rejecting!")
                response = Sip.response(status=Status.REQUEST_PENDING, related=request)
                self.send_message(response)
                return None, None
                
            if self.unresponded_update:
                self.logger.warning("Got conflicting UPDATE request, rejecting!")
                response = Sip.response(status=Status.REQUEST_PENDING, related=request)
                self.send_message(response)
                return None, None
                
            self.logger.info("Got UPDATE request with offer.")
            sdp = get_sdp(request)
            session = self.parse_sdp(sdp, False)
            
            self.unresponded_update = request
            return request, session
        else:
            response = message
            
            if not self.unresponded_update:
                self.logger.warning("Got unexpected UPDATE response, ignoring!")
                return None, None
            
            self.logger.info("Got UPDATE response with answer.")
            sdp = get_sdp(response)
            session = self.parse_sdp(sdp, True)
            
            self.unresponded_update = None
            return response, session


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
            return None, None
