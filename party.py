from weakref import proxy

from ground import GroundDweller, Leg
from format import SipError, Status
import zap



class Party(GroundDweller):
    def __init__(self):
        GroundDweller.__init__(self)
        
        self.call_oid = None
        self.path = None
        
            
    def set_path(self, call_oid, path):
        self.call_oid = call_oid
        self.path = path


    def make_leg(self, li):
        leg = Leg(self, li)
        
        self.ground.setup_leg(leg, self.oid.add("leg", li))
        
        return leg
        
        
    def make_media_leg(self, type):
        return self.ground.make_media_leg(type)
        
        
    def start(self):
        raise NotImplementedError()


    def may_finish(self):
        raise NotImplementedError()


    def do_slot(self, li, action):
        self.logger.critical("No do_slot: %r" % self)
        raise NotImplementedError()




class Endpoint(Party):
    def __init__(self):
        Party.__init__(self)
        
        self.leg = None
        
            
    def start(self):
        self.leg = self.make_leg(None)
        
        return self.leg

    
    def may_finish(self):
        self.leg.may_finish()
        self.leg = None
        self.logger.info("Endpoint finished.")


    def do(self, action):
        raise NotImplementedError()
        

    def do_slot(self, li, action):
        self.do(action)




class PlannedEndpoint(zap.Planned, Endpoint):
    def __init__(self):
        zap.Planned.__init__(self)
        Endpoint.__init__(self)


    def start(self):
        self.start_plan()
        
        return Endpoint.start(self)
        
        
    def may_finish(self):
        if self.is_plan_running():
            return
            
        Endpoint.may_finish(self)
        

    def wait_action(self, timeout=None):
        event = yield from self.wait_event(timeout=timeout)
        if not event:
            return None
            
        action, = event
        
        return action
            

    def do(self, action):
        self.send_event(action)


    def plan_finished(self, error):
        if error:
            self.logger.error("Endpoint plan aborted with: %s!" % error)
        
        self.may_finish()




class Bridge(Party):
    def __init__(self):
        Party.__init__(self)
        
        self.leg_count = 0
        self.legs = {}
        
            
    def start(self):
        if self.legs:
            raise Exception("Already started!")
            
        leg = self.add_leg()
        
        return leg

    
    def add_leg(self):
        li = self.leg_count  # outgoing legs are numbered from 1
        self.leg_count += 1

        leg = self.make_leg(li)
        self.legs[li] = proxy(leg)
        
        return leg


    def remove_leg(self, li):
        leg = self.legs.pop(li)
        leg.may_finish()


    def may_finish(self):
        # TODO: we must keep the incoming Leg (when we have one) until
        # we can finish, because removing it may instantly kill us.
        # So even after anchoring, keep the SlotLeg until this is the only
        # slot, and this method is called, then remove it here.
        if len(self.legs) == 1:
            self.remove_leg(0)
            self.logger.info("Bridge finished.")
        else:
            self.logger.debug("Not finishing yet, still have %d legs." % len(self.legs))


    def dial(self, type, action):
        if action["type"] != "dial":
            raise Exception("Dial action is not a dial: %s" % action["type"])

        self.logger.debug("Dialing out to: %s" % (type,))
        
        leg = self.add_leg()
        li = leg.number
        
        party = self.ground.make_party(type, self.call_oid, self.path + [ li ])
        party_leg = party.start()
        self.ground.link_legs(leg.oid, party_leg.oid)

        leg.forward(action)




class Routing(Bridge):
    def __init__(self):
        Bridge.__init__(self)

        self.incoming_leg_rang = False
        self.routing_concluded = False
        self.queued_leg_actions = {}


    def add_leg(self):
        leg = Bridge.add_leg(self)
        self.queued_leg_actions[leg.number] = []
        return leg
        

    def queue_leg_action(self, li, action):
        self.logger.debug("Queueing %s from leg %s." % (action["type"], li))
        self.queued_leg_actions[li].append(action)


    def reject_incoming_leg(self, status):
        if not self.routing_concluded:
            self.routing_concluded = True
            self.logger.warning("Rejecting with status %s" % (status,))
            self.legs[0].forward(dict(type="reject", status=status))
            
            
    def ring_incoming_leg(self):
        if not self.incoming_leg_rang:
            self.incoming_leg_rang = True
            self.logger.debug("Sending artificial ringback.")
            self.legs[0].forward(dict(type="ring"))


    def hangup_outgoing_legs(self, except_li):
        for li, leg in list(self.legs.items()):
            if li not in (0, except_li):
                self.logger.debug("Hanging up leg %s" % li)
                leg.forward(dict(type="hangup"))
                self.remove_leg(li)


    def anchor_outgoing_leg(self, li):
        if not self.routing_concluded:
            self.routing_concluded = True
            self.logger.debug("Anchored to leg %d." % li)
            self.hangup_outgoing_legs(except_li=li)
            self.ground.collapse_legs(self.legs[0].oid, self.legs[li].oid, self.queued_leg_actions[li])
            self.remove_leg(li)
            # The incoming leg is kept to keep us alive for a while


    def may_finish(self):
        if not self.routing_concluded:
            if len(self.legs) >= 2:
                return  # Give us some more time
                
            # No outgoing legs, no conclusion, no happy ending
            self.logger.error("Routing gave up before reaching a conclusion!")
            self.reject_incoming_leg(Status(500))
            
        return Bridge.may_finish(self)
        

    def process_leg_action(self, li, action):
        type = action["type"]
        self.logger.debug("Got %s from leg %d." % (type, li))

        if li == 0:
            if type == "dial":
                raise Exception("Should have handled dial in a subclass!")
            elif type == "hangup":
                self.hangup_outgoing_legs(None)
                self.routing_concluded = True
                self.may_finish()
            else:
                raise Exception("Invalid action from incoming leg: %s" % type)
            
            return

        if type == "reject":
            # FIXME: we probably shouldn't just forward the last rejection status
            self.remove_leg(li)

            if len(self.legs) == 1:
                self.reject_incoming_leg(action["status"])

            self.may_finish()
        elif type == "ring":
            if action.get("session"):
                action["type"] = "session"
                self.queue_leg_action(li, action)
                
            self.ring_incoming_leg()
        elif type == "session":
            self.queue_leg_action(li, action)
        elif type == "accept":
            self.queue_leg_action(li, action)
            self.anchor_outgoing_leg(li)
            self.may_finish()
        elif type == "hangup":
            # Oops, we anchored this leg because it accepted, but now hangs up
            # FIXME: is this still true?
            self.legs[0].forward(action)
        else:
            raise Exception("Invalid action from outgoing leg %d: %s" % (li, type))




class SimpleRouting(Routing):
    def route(self, dial_action):
        raise NotImplementedError()


    def do_slot(self, li, action):
        if li == 0 and action["type"] == "dial":
            try:
                self.route(action)
            except SipError as e:
                self.logger.error("Simple routing SIP error: %s" % (e.status,))
                self.reject_incoming_leg(e.status)
            except Exception as e:
                self.logger.error("Simple routing internal error: %s" % (e,), exc_info=True)
                self.reject_incoming_leg(Status(500))
            else:
                self.may_finish()
        else:
            self.process_leg_action(li, action)




class PlannedRouting(zap.Planned, Routing):
    def __init__(self):
        zap.Planned.__init__(self)
        Routing.__init__(self)


    def start(self):
        self.start_plan()
        
        return Routing.start(self)


    def may_finish(self):
        if self.is_plan_running():
            return
            
        Routing.may_finish(self)
            

    def wait_leg_action(self, timeout=None):
        event = yield from self.wait_event(timeout=timeout)
        if not event:
            return None, None
            
        li, action = event

        return li, action


    def plan_finished(self, exception):
        # Take control from here
        self.logger.debug("Routing plan finished.")
        self.event_slot.plug(self.process_leg_action)
        
        if exception:
            try:
                raise exception
            except SipError as e:
                self.logger.error("Routing plan aborted with SIP error: %s" % e)
                self.reject_incoming_leg(e.status)
                self.hangup_outgoing_legs(None)
            except Exception as e:
                self.logger.error("Routing plan aborted with exception: %s" % e)
                self.reject_incoming_leg(Status(500))
                self.hangup_outgoing_legs(None)
            
        self.may_finish()
        
        
    def do_slot(self, li, action):
        self.logger.debug("Planned routing processing a %s" % action["type"])
        self.send_event(li, action)




class SimpleBridge(Bridge):
    def __init__(self):
        Bridge.__init__(self)

        
    def may_finish(self):
        if 1 in self.legs:
            self.logger.debug("Releasing outgoing leg.")
            self.remove_leg(1)

        if 0 in self.legs:
            self.logger.debug("Releasing incoming leg.")
            self.remove_leg(0)
            
        Bridge.may_finish(self)
        # Since only the legs held a reference to self, we may be destroyed
        # as soon as this method returns.
            

    def do_slot(self, li, action):
        type = action["type"]

        if li == 0 and type == "dial":
            self.dial("routing", action)  # TODO: this used to make thing with (... self.path, "routing")
            return
        
        if li == 0:
            leg = self.legs[1]
            direction = "forward"
        else:
            leg = self.legs[0]
            direction = "backward"
        
        self.logger.debug("Bridging %s %s" % (type, direction))
        leg.forward(action)

        if type in ("hangup", "reject"):
            self.may_finish()

    
class RecordingBridge(SimpleBridge):
    def hack_media(self, li, answer):
        old = len(self.legs[0].media_legs)
        new = len(answer["channels"])
        
        for i in range(old, new):
            answer_channel = answer["channels"][i]
            ctype = answer_channel["type"]
            mgw_affinity = answer_channel.get("mgw_affinity")
            mgw_sid = self.ground.select_gateway_sid(ctype, mgw_affinity)

            this = self.make_media_leg("pass")
            that = self.make_media_leg("pass")

            # Pairing must happen before setting it, because realizing needs it
            this.pair(proxy(that))
            that.pair(proxy(this))
            
            self.legs[0].set_media_leg(i, this, mgw_sid)
            self.legs[1].set_media_leg(i, that, mgw_sid)
            
            format = ("L16", 8000, 1, None)
            this.refresh(dict(filename="recorded.wav", format=format, record=True))
            that.refresh({})
            
        if len(answer["channels"]) >= 1:
            c = answer["channels"][0]

            if not c["send"]:
                self.logger.debug("Hah, the %s put us on hold!" % ("callee" if li == 0 else "caller"))

            if not c["recv"]:
                self.logger.debug("Hah, the %s put us on hold!" % ("caller" if li == 0 else "callee"))


    def do_slot(self, li, action):
        session = action.get("session")
        
        if session and session["is_answer"] and "channels" in session:
            self.hack_media(li, session)
        
        SimpleBridge.do_slot(self, li, action)