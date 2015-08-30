from copy import deepcopy

from async import WeakMethod
from format import Status, make_virtual_response
from planner import Planner, PlannedEvent

class Error(Exception): pass

class Leg(object):
    def __init__(self, call):
        self.call = call
        self.report = None
        self.ctx = {}
    
    
    #def make_media_leg(self, sdp_channel):
    #    raise NotImplementedError()
        
    
    def set_report(self, report):
        self.report = report
        
    
    def do(self, action):
        raise NotImplementedError()


    def finish(self):
        self.report(dict(type="finish"))
        
    
    def refresh(self):
        #self.report(dict(type="refresh"))
        self.call.refresh_media()
        

class Session(object):
    def __init__(self):
        self.local_sdp = None
        self.remote_sdp = None
        self.pending_local_sdp = None
        self.pending_remote_sdp = None
        
        
    def set_local_offer(self, sdp):
        if sdp is None:
            raise Error("No SDP specified!")
        elif self.pending_local_sdp:
            raise Error("Outgoing offer already pending!")
        elif self.pending_remote_sdp:
            raise Error("Incoming offer also pending!")
        else:
            self.pending_local_sdp = sdp


    def set_remote_offer(self, sdp):
        if sdp is None:
            raise Error("No SDP specified!")
        elif self.pending_local_sdp:
            raise Error("Outgoing offer also pending!")
        elif self.pending_remote_sdp:
            raise Error("Incoming offer already pending!")
        else:
            self.pending_remote_sdp = sdp
            
            
    def set_local_answer(self, sdp):
        if sdp is None:
            raise Error("No SDP specified!")
        elif not self.pending_remote_sdp:
            raise Error("Incoming offer not pending!")
        else:
            # empty is rejection
            if not sdp.is_empty():
                self.remote_sdp = self.pending_remote_sdp
                self.local_sdp = sdp
                
            self.pending_remote_sdp = None


    def set_remote_answer(self, sdp):
        if sdp is None:
            raise Error("No SDP specified!")
        elif not self.pending_local_sdp:
            raise Error("Outgoing offer not pending!")
        else:
            # empty is rejection
            if not sdp.is_empty():
                self.local_sdp = self.pending_local_sdp
                self.remote_sdp = sdp
                
            self.pending_local_sdp = None


    def get_local_offer(self):
        if self.pending_local_sdp:
            return self.pending_local_sdp
        else:
            raise Error("Outgoing offer not pending!")
        
        
    def get_remote_offer(self):
        if self.pending_remote_sdp:
            return self.pending_remote_sdp
        else:
            raise Error("Incoming offer not pending!")
            
            
    def get_local_answer(self):
        if self.pending_local_sdp:
            raise Error("Outgoing offer is pending!")
        elif self.pending_remote_sdp:
            raise Error("Incoming offer still pending!")
        elif not self.local_sdp:
            raise Error("No outgoing answer yet!")
        else:
            return self.local_sdp


    def get_remote_answer(self):
        if self.pending_local_sdp:
            raise Error("Outgoing offer still pending!")
        elif self.pending_remote_sdp:
            raise Error("Incoming offer is pending!")
        elif not self.remote_sdp:
            raise Error("No incoming answer yet!")
        else:
            return self.remote_sdp


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
    

    def __init__(self, call, dialog):
        super(SipLeg, self).__init__(call)

        self.dialog = dialog
        self.state = self.DOWN
        self.invite_state = None
        self.session = Session()
        self.media_addresses = []
        
        self.dialog.set_report(WeakMethod(self.process))


    def send_request(self, request, related=None):
        self.dialog.send_request(request, related)


    def send_response(self, response, related):
        if related and not related["is_response"]:
            self.dialog.send_response(response, related)
        else:
            raise Error("Respond to what?")


    def get_media_infos(self):
        # TODO: move this function to Call? Or to Session?
        # TODO: remove altogether, just let the MediaLegs use the MGW directly!
        infos = []

        def extract_formats(c):
            return { r.payload_type: (r.encoding, r.clock) for r in c.formats }

        # Negotiated session parameters
        lsdp = self.session.local_sdp
        rsdp = self.session.remote_sdp

        for i in range(len(lsdp.channels) if lsdp else 0):
            lc = lsdp.channels[i]
            rc = rsdp.channels[i]
            #print("XXX local: %s (%s), remote: %s (%s)" % (lc.addr, id(lc.addr), rc.addr, id(rc.addr)))
            
            info = dict(
                type="net",
                local_addr=lc.addr,
                remote_addr=rc.addr,
                send_formats=extract_formats(rc),
                recv_formats=extract_formats(lc)
            )
            
            infos.append(info)
            
        return infos
        

    def preprocess_outgoing_session(self, sdp):
        for i in range(len(self.media_addresses), len(sdp.channels)):
            # TODO: deallocate them, too!
            self.media_addresses.append(self.call.allocate_media_address(i))
        
        for i in range(len(sdp.channels)):
            # No need to make a copy again here
            if sdp.channels[i].addr:
                raise Exception("Outgoing session has channel address set! %s" % id(sdp.channels[i]))
                
            sdp.channels[i].addr = self.media_addresses[i]
            
        return sdp  # For the sake of consistency


    def postprocess_incoming_session(self, sdp):
        for c in sdp.channels:
            print("XXX c: %s (%s)" % (c, id(c)))
            
        sdp = deepcopy(sdp)

        for c in sdp.channels:
            print("XXX c: %s (%s)" % (c, id(c)))
        
        for i in range(len(sdp.channels)):
            sdp.channels[i].addr = None  # Just to be sure
            
        return sdp

        
    def process_incoming_offer(self, sdp):
        self.session.set_remote_offer(sdp)
        sdp = self.postprocess_incoming_session(sdp)
        return sdp

    
    def process_incoming_answer(self, sdp):
        self.session.set_remote_answer(sdp)
        self.refresh()
        sdp = self.postprocess_incoming_session(sdp)
        return sdp

    
    def process_outgoing_offer(self, sdp):
        sdp = self.preprocess_outgoing_session(sdp)
        self.session.set_local_offer(sdp)
        return sdp

    
    def process_outgoing_answer(self, sdp):
        sdp = self.preprocess_outgoing_session(sdp)
        self.session.set_local_answer(sdp)
        self.refresh()
        return sdp
        

    def do(self, action):
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
                self.dialog.setup_outgoing(
                    self.ctx["from"].uri, self.ctx["from"].name, self.ctx["to"].uri,
                    self.ctx.get("route"), self.ctx.get("hop")
                )
                
                invite_request = dict(method="INVITE", sdp=offer)
                self.send_request(invite_request)  # Will be extended!
                self.invite_state = InviteState(invite_request)
                self.state = self.DIALING_OUT
                return
        elif self.state in (self.DIALING_OUT, self.DIALING_OUT_RINGING):
            if type == "hangup":
                self.send_request(dict(method="CANCEL"), self.invite_state.request)
                self.state = self.DISCONNECTING_OUT
                return
        elif self.state in (self.DIALING_OUT_ANSWERED,):  # TODO: into InviteState?
            if type == "session":
                # send ACK with SDP
                if not answer:
                    raise Error("Answer expected in ACK!")
                    
                self.send_request(dict(method="ACK", sdp=answer), self.invite_state.final_response)
                self.state = self.UP
                return
        elif self.state in (self.DIALING_IN, self.DIALING_IN_RINGING):
            if self.invite_state.responded_session:
                sdp = self.invite_state.responded_session
            else:
                sdp = answer if self.invite_state.had_offer_in_request else offer
                if sdp:
                    self.invite_state.responded_session = sdp
                
            if type == "ring":
                invite_response = dict(status=Status(180, "Ringing"), sdp=sdp)
                self.send_response(invite_response, self.invite_state.request)
                self.state = self.DIALING_IN_RINGING
                return
            elif type == "session":
                status = Status(180, "Ringing") if self.state == self.DIALING_IN_RINGING else Status(183, "Session Progress")
                invite_response = dict(status=status, sdp=sdp)
                self.send_response(invite_response, self.invite_state.request)
                return
            elif type == "accept":
                invite_response = dict(status=Status(200, "OK"), sdp=sdp)
                self.send_response(invite_response, self.invite_state.request)
                self.state = self.DIALING_IN_ANSWERED  # TODO: into InviteState?
                # TODO: Do we need to block requests here, or can we already send new ones?
                # Just wait for the ACK for now.
                return
        elif self.state == self.UP:
            if type == "hangup":
                self.send_request(dict(method="BYE"))
                self.state = self.DISCONNECTING_OUT
                return
                
        raise Error("Weirdness!")


    def process(self, msg):
        is_response = msg["is_response"]
        method = msg["method"]
        status = msg.get("status")
        sdp = msg.get("sdp")
        
        if self.state == self.DOWN:
            if not is_response and method == "INVITE":
                self.ctx.update({
                    "from": msg["from"],
                    "to": msg["to"]
                })
                
                offer = sdp if sdp and sdp.is_session() else None
                if offer:
                    offer = self.process_incoming_offer(offer)  # TODO: session query?
                    
                self.invite_state = InviteState(msg)
                self.report(dict(type="dial", ctx=self.ctx, offer=offer))
                self.state = self.DIALING_IN
                return
        elif self.state in (self.DIALING_IN, self.DIALING_IN_RINGING):
            if not is_response and method == "CANCEL":
                self.report(dict(type="hangup"))
                self.send_response(dict(status=Status(487, "Request Terminated")), self.invite_state.request)
                self.send_response(dict(status=Status(200, "OK")), msg)
                self.invite_state = None
                self.state = self.DOWN
                self.finish()
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
                    if self.state == self.DIALING_OUT:
                        self.report(dict(type="ring", offer=offer, answer=answer))
                    elif offer or answer:
                        self.report(dict(type="session", offer=offer, answer=answer))
                    self.state = self.DIALING_OUT_RINGING
                    return
                elif status.code == 183:
                    if offer or answer:
                        self.report(dict(type="session", offer=offer, answer=answer))
                    return
                elif status.code >= 300:
                    self.report(dict(type="reject", status=status))
                    self.state = self.DOWN
                    # ACKed by tr
                    self.finish()
                    return
                elif status.code >= 200:
                    if self.invite_state.had_offer_in_request:
                        # send ACK without SDP now
                        self.send_request(dict(method="ACK"), msg)
                        self.state = self.UP
                    else:
                        # wait for outgoing session for the ACK
                        # beware, the report() below may send it!
                        self.invite_state.final_response = msg
                        self.state = self.DIALING_OUT_ANSWERED
                        
                    self.report(dict(type="accept", offer=offer, answer=answer))
                    return
        elif self.state == self.DIALING_IN_ANSWERED:
            if not is_response and method == "ACK":
                answer = None
                
                if self.invite_state.had_offer_in_request:
                    if sdp and sdp.is_session():
                        print("Unexpected session in ACK!")
                else:
                    if not (sdp and sdp.is_session()):
                        print("Unexpected sessionless ACK!")
                    else:
                        answer = self.process_incoming_answer(sdp)
                        self.report(dict(type="session", answer=answer))
                    
                # Stop the retransmission of the final answer
                self.send_response(make_virtual_response(), self.invite_state.request)
                # Let the ACK server transaction expire
                self.send_response(make_virtual_response(), msg)
                self.invite_state = None
                self.state = self.UP
                return
            elif not is_response and method == "NAK":  # virtual request, no ACK received
                self.send_request(dict(method="BYE"))  # required behavior
                self.state = self.DISCONNECTING_OUT
                return
        elif self.state == self.UP:
            if not is_response and method == "BYE":
                self.report(dict(type="hangup"))
                self.send_response(dict(status=Status(200, "OK")), msg)
                self.state = self.DOWN
                self.finish()
                return
        elif self.state == self.DISCONNECTING_OUT:
            if is_response and method == "BYE":
                self.state = self.DOWN
                self.finish()
                return
            elif is_response and method == "INVITE":
                print("Got cancelled invite response: %s" % (status,))
                # This was ACKed by the transaction layer
                self.state = self.DOWN
                self.finish()
                return
            elif is_response and method == "CANCEL":
                print("Got cancel response: %s" % (status,))
                return
                
        raise Error("Weirdness!")


class PlannedLeg(Leg):
    class LegPlanner(Planner):
        def wait_action(self, action_type, timeout=None):
            planned_event = yield from self.suspend(expect="action", timeout=timeout)
            action = planned_event.event
            
            if action["type"] != action_type:
                raise Exception("Expected action %s, got %s!" % (action_type, action["type"]))
                
            return action
            
            
    def __init__(self, call, metapoll):
        super(PlannedLeg, self).__init__(call)
        
        self.planner = self.LegPlanner(metapoll, self.plan, finish_handler=WeakMethod(self.finish))
    

    def do(self, action):
        self.planner.resume(PlannedEvent("action", action))


    def plan(self, planner):
        raise NotImplementedError()
        

def create_uninvited_leg(dialog_manager, invite_params):
    # TODO: real UninvitedLeg class
    leg = Leg(dialog_manager, None, None, None)
    leg.dialog.send_request(dict(method="UNINVITE"), invite_params, leg.process)  # strong ref!
    leg.state = leg.DIALING_OUT
