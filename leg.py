from weakref import proxy

from util import build_oid, Loggable
from format import SipError, Status
import zap


class Error(Exception): pass


class CallComponent(Loggable):
    def __init__(self):
        Loggable.__init__(self)
        
        self.call = None
        
    
    def set_call(self, call):
        self.call = call


        
        
class Leg(CallComponent):
    def __init__(self, owner, number):
        CallComponent.__init__(self)

        self.owner = owner
        self.number = number
        
        self.media_legs = []
        self.finished_slot = zap.Slot()  # TODO: is this better than a direct call?

    
    def report(self, action):  # TODO: rename to forward
        self.call.forward(self, action)
        
        
    def may_finish(self):
        for ci in range(len(self.media_legs)):
            self.set_media_leg(ci, None)

        self.logger.debug("Leg is finished.")
        self.finished_slot.zap()


    def make_media_leg(self, type):
        return self.call.make_media_leg(type)
        

    def set_media_leg(self, channel_index, media_leg):
        if channel_index > len(self.media_legs):
            raise Exception("Invalid media leg index!")
        elif channel_index == len(self.media_legs):
            self.media_legs.append(None)
            
        old = self.media_legs[channel_index]
        
        if old:
            self.logger.debug("Deleting media leg %s." % channel_index)
            self.call.media_leg_changed(self.oid, channel_index, False)
            old.delete()
        
        self.media_legs[channel_index] = media_leg
        
        if media_leg:
            self.logger.debug("Adding media leg %s." % channel_index)
            media_leg.set_oid(build_oid(self.oid, "channel", channel_index))
            self.call.media_leg_changed(self.oid, channel_index, True)
        

    def get_media_leg(self, ci):
        return self.media_legs[ci] if ci < len(self.media_legs) else None


    def do(self, action):
        self.owner.do_slot(self.number, action)




class Party(CallComponent):
    def __init__(self):
        CallComponent.__init__(self)
        
        self.path = None
        self.leg_count = 0
        self.legs = {}
        
            
    def set_path(self, path):
        self.path = path
        
        
    def start(self):
        if self.legs:
            raise Exception("Already started!")
            
        leg = self.add_leg()
        
        return leg

    
    def add_leg(self):
        li = self.leg_count  # outgoing legs are numbered from 1
        self.leg_count += 1

        leg = Leg(self, li)
        leg.set_call(self.call)
        leg.set_oid(build_oid(self.oid, "leg", li))
        self.call.add_leg(leg)

        self.legs[li] = proxy(leg)
        
        return leg


    def remove_leg(self, li):
        leg = self.legs.pop(li)
        leg.may_finish()


    def may_finish(self):
        # TODO: we must keep the incoming SlotLeg (when we have one) until
        # we can finish, because removing it may instantly kill us.
        # So even after anchoring, keep the SlotLeg until this is the only
        # slot, and this method is called, then remove it here.
        if len(self.legs) == 1:
            self.remove_leg(0)
            self.logger.info("Finished.")
        else:
            self.logger.debug("Not finishing yet, we have %d legs." % len(self.legs))


    def dial(self, type, action):
        if action["type"] != "dial":
            raise Exception("Dial action is not a dial: %s" % action["type"])

        self.logger.debug("Dialing out to: %s" % (type,))
        
        leg = self.add_leg()
        li = leg.number
        
        thing = self.call.make_thing(type, self.path + [ li ], None)
        self.call.link_leg_to_thing(leg, thing)
        leg.report(action)


    def do_slot(self, li, action):
        raise NotImplementedError()




class PlannedParty(zap.Planned, Party):
    def __init__(self):
        zap.Planned.__init__(self)
        Party.__init__(self)


    def start(self):
        self.start_plan()
        
        return Party.start(self)
        

    def wait_action(self, action_type=None, timeout=None):  # TODO: merge with routing's
        event = yield from self.wait_event(timeout=timeout)
        if not event:
            return None, None
            
        li, action = event
        
        if action_type and action["type"] != action_type:
            raise Exception("Expected action %s, got %s!" % (action_type, action["type"]))
            
        return li, action
            

    def do_slot(self, li, action):
        self.queue_event(li, action)


    def plan_finished(self, error):
        if error:
            self.logger.error("Leg plan aborted with: %s!" % error)
        
        self.may_finish()




class Routing(Party):
    def __init__(self):
        Party.__init__(self)

        self.sent_ringback = False
        self.queued_actions = {}


    def add_leg(self):
        leg = Party.add_leg(self)
        self.queued_actions[leg.number] = []
        return leg
        

    def queue(self, li, action):
        self.logger.debug("Queueing %s from leg %s." % (action["type"], li))
        self.queued_actions[li].append(action)


    def reject(self, status):
        self.logger.warning("Rejecting with status %s" % (status,))
        self.legs[0].report(dict(type="reject", status=status))
            
            
    def ringback(self):
        if not self.sent_ringback:
            self.sent_ringback = True
            self.logger.debug("Sending artificial ringback.")
            self.legs[0].report(dict(type="ring"))


    def hangup_all_outgoing(self, except_li):
        for li, leg in list(self.legs.items()):
            if li not in (0, except_li):
                self.logger.debug("Hanging up leg %s" % li)
                leg.report(dict(type="hangup"))
                self.remove_leg(li)


    def anchor(self, li):
        self.logger.debug("Anchored to leg %d." % li)
        self.hangup_all_outgoing(li)
        self.call.collapse_legs(self.legs[0], self.legs[li], self.queued_actions[li])
        self.remove_leg(li)


    def do_slot(self, li, action):
        type = action["type"]
        self.logger.debug("Got %s from leg %d." % (type, li))

        if li == 0:
            if type == "dial":
                raise Exception("Should have handled dial in a subclass!")
            elif type == "hangup":
                self.hangup_all_outgoing(None)
                self.may_finish()
            else:
                raise Exception("Invalid action from incoming leg: %s" % type)
            
            return

        if type == "reject":
            # FIXME: of course don't reject the incoming leg immediately
            # FIXME: shouldn't we finish here?
            self.reject(action["status"])
        elif type == "ring":
            if action.get("session"):
                action["type"] = "session"
                self.queue(li, action)
                
            self.ringback()
        elif type == "session":
            self.queue(li, action)
        elif type == "accept":
            self.queue(li, action)
            self.anchor(li)
            self.may_finish()
        elif type == "hangup":
            # Oops, we anchored this leg because it accepted, but now hangs up
            # FIXME: is this still true?
            self.legs[0].report(action)
        else:
            raise Exception("Invalid action from outgoing leg %d: %s" % (li, type))




class SimpleRouting(Routing):
    def route(self, action):
        raise NotImplementedError()


    def do_slot(self, li, action):
        if li == 0 and action["type"] == "dial":
            try:
                self.route(action)
                
                if not self.legs:
                    raise Exception("Simple routing finished without legs!")
            except SipError as e:
                self.logger.error("Simple routing SIP error: %s" % (e.status,))
                self.reject(e.status)
            except Exception as e:
                self.logger.error("Simple routing internal error: %s" % (e,), exc_info=True)
                self.reject(Status(500))
        else:
            Routing.do_slot(self, li, action)




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
            

    def wait_action(self, leg_index=None, action_type=None, timeout=None):
        event = yield from self.wait_event(timeout=timeout)
        if not event:
            return None, None
            
        li, action = event

        if leg_index is not None and li != leg_index:
            raise Exception("Expected action from %s, got from %s!" % (leg_index, li))

        if action_type and action["type"] != action_type:
            raise Exception("Expected action %s, got %s!" % (action_type, action["type"]))
        
        return li, action
        
            
    def process_event(self, li, action):
        # Take control after the plan finished. Even then the evens must be queued,
        # so that they don't overtake the once queued, but not yet processed ones.
        Routing.do_slot(self, li, action)
        # TODO: how could we plug it directly to event_slot?
        

    def plan_finished(self, exception):
        # Take control from here
        self.logger.debug("Routing plan finished.")
        self.event_slot.plug(self.process_event)
        
        status = None
        
        try:
            if exception:
                self.logger.debug("Routing plan finished with: %s" % exception)
                raise exception
        except SipError as e:
            self.logger.error("Routing plan aborted with SIP error: %s" % e)
            status = e.status
        except Exception as e:
            self.logger.error("Routing plan aborted with exception: %s" % e)
            status = Status(500)
            
        if status:
            # TODO: must handle double events for this to work well!
            # TODO: Hm?
            self.reject(status)
            self.hangup_all_outgoing(None)

        self.may_finish()
        
        
    def do_slot(self, li, action):
        self.logger.debug("Planned routing processing a %s" % action["type"])
        self.queue_event(li, action)




class Bridge(Party):
    def __init__(self):
        Party.__init__(self)

        
    def may_finish(self):
        if 1 in self.legs:
            self.logger.debug("Releasing outgoing leg.")
            self.remove_leg(1)

        if 0 in self.legs:
            self.logger.debug("Releasing incoming leg.")
            self.remove_leg(0)
            
        self.logger.debug("Bridge finished.")
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
        leg.report(action)

        if type in ("hangup", "reject"):
            self.may_finish()

    
class RecordingBridge(Bridge):
    def hack_media(self, li, answer):
        old = len(self.legs[0].media_legs)
        new = len(answer["channels"])
        
        for i in range(old, new):
            this = self.legs[0].make_media_leg("pass")
            that = self.legs[1].make_media_leg("pass")

            # Pairing must happen before setting it, because realizing needs it
            this.pair(proxy(that))
            that.pair(proxy(this))
            
            self.legs[0].set_media_leg(i, this)
            self.legs[1].set_media_leg(i, that)
            
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
        
        if session and session["is_answer"] and len(session) > 1:
            self.hack_media(li, session)
        
        Bridge.do_slot(self, li, action)
