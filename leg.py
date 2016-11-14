from weakref import proxy

from util import build_oid, Loggable
from format import SipError, Status
import zap


class Error(Exception): pass


class CallComponent(Loggable):
    def __init__(self):
        Loggable.__init__(self)
        
        self.call = None
        self.path = None  # To prettify oids
        
    
    def set_call(self, call, path):
        self.call = call
        self.path = path
        
        
    def start(self):
        pass  # Useful for Leg types that do something by themselves


    def stand(self):
        raise NotImplementedError()
        
        
class BareLeg(CallComponent):  # FIXME: merge with Leg now
    def __init__(self):
        CallComponent.__init__(self)
        
    
    def do(self, action):
        raise NotImplementedError()
        
        
    def report(self, action):
        self.call.forward(self, action)
        
        
    def finished(self):
        self.logger.debug("Leg is finished.")
        self.call.leg_finished(self)
        
        
    def get_media_leg(self, channel_index):
        return None
        
        
    def stand(self):
        self.call.add_leg(self)
        
        return self


class Leg(BareLeg):
    def __init__(self):
        BareLeg.__init__(self)
        
        self.media_legs = []

    
    def may_finish(self, error=None):
        if any(self.media_legs):
            return
        
        self.finished()


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
        

    def finish_media(self, error=None):
        for ci in range(len(self.media_legs)):
            self.set_media_leg(ci, None)
            
        self.may_finish(error)


    def get_media_leg(self, ci):
        return self.media_legs[ci] if ci < len(self.media_legs) else None
            
            
            

class SlotLeg(Leg):
    def __init__(self, owner, number):
        Leg.__init__(self)
        
        self.owner = owner
        self.number = number
        
        
    def do(self, action):
        self.owner.do_slot(self.number, action)




class PlannedLeg(Leg):
    def __init__(self):
        Leg.__init__(self)
        
        self.plan = zap.Plan()
        self.plan.finished_slot.plug(self.plan_finished)
        self.event_slot = zap.EventSlot()


    def __del__(self):
        if self.plan:
            self.plan.abort()


    def set_oid(self, oid):
        Leg.set_oid(self, oid)
        
        self.plan.set_oid(build_oid(self.oid, "plan"))
        
        
    def start(self):
        self.plan.start(self.leg_plan())


    def sleep(self, timeout):
        yield zap.time_slot(timeout)
        

    def wait_event(self, timeout=None):  # TODO
        slot_index, args = yield zap.time_slot(timeout), self.event_slot
        
        return args if slot_index == 1 else None
            
        
    def wait_action(self, action_type=None, timeout=None):
        event = yield from self.wait_event(timeout=timeout)
        if not event:
            return None
            
        action, = event
        
        if action_type and action["type"] != action_type:
            raise Exception("Expected action %s, got %s!" % (action_type, action["type"]))
            
        return action
            

    def do(self, action):
        self.event_slot.zap(action)


    def plan_finished(self, error):
        if error:
            self.logger.error("Leg plan aborted with: %s!" % error)
        
        # Unconditional cleanup
        self.finish_media(error=error)




class Routing(CallComponent):
    def __init__(self):
        CallComponent.__init__(self)

        self.leg_count = 0
        self.legs = {}
        self.sent_ringback = False
        self.queued_actions = {}
        self.bridge_count = 0
    
    
    def stand(self):
        slot_leg = self.add_leg()
        
        return slot_leg.stand()
        
    
    def add_leg(self):
        li = self.leg_count  # outgoing legs are numbered from 1
        self.leg_count += 1

        slot_leg = self.call.make_slot(self, li)
        self.legs[li] = proxy(slot_leg)
        
        self.queued_actions[li] = []
        
        return slot_leg


    def remove_leg(self, li):
        leg = self.legs.pop(li)
        leg.finished()
        self.may_finish()


    def may_finish(self):
        # TODO: we must keep the incoming SlotLeg (when we have one) until
        # we can finish, because removing it may instantly kill us.
        # So even after anchoring, keep the SlotLeg until this is the only
        # slot, and this method is called, then remove it here.
        if len(self.legs) == 1:
            self.remove_leg(0)
            #self.finished()


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


    def dial(self, type, action):
        if action["type"] != "dial":
            raise Exception("Dial action is not a dial: %s" % action["type"])

        self.logger.debug("Dialing out to: %s" % (type,))
        
        slot_leg = self.add_leg()
        li = slot_leg.number
        
        thing = self.call.make_thing(type, self.path + [ li ], None)
        self.call.link_leg_to_thing(slot_leg, thing)
        slot_leg.report(action)


    def do_slot(self, li, action):
        type = action["type"]
        self.logger.debug("Got %s from leg %d." % (type, li))

        if li == 0:
            if type == "dial":
                raise Exception("Should have handled dial in a subclass!")
            elif type == "hangup":
                self.hangup_all_outgoing(None)
            else:
                raise Exception("Invalid action from incoming leg: %s" % type)
            
            return

        if type == "reject":
            # FIXME: of course don't reject the incoming leg immediately
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




class PlannedRouting(Routing):
    def __init__(self):
        Routing.__init__(self)

        self.plan = zap.Plan()
        self.plan.finished_slot.plug(self.plan_finished)
        self.event_slot = zap.EventSlot()


    def __del__(self):
        if self.plan:
            self.plan.abort()
            

    def set_oid(self, oid):
        Routing.set_oid(self, oid)
        
        self.plan.set_oid(build_oid(self.oid, "plan"))
        
        
    def routing_plan(self):  # TODO: make it external?
        raise NotImplementedError()
        
        
    def start(self):
        self.plan.start(self.routing_plan())
        
        
    def may_finish(self):
        if self.plan:
            return
            
        Routing.may_finish(self)
            

    def sleep(self, timeout):
        yield zap.time_slot(timeout)
        

    def wait_event(self, timeout=None):  # TODO
        slot_index, args = yield zap.time_slot(timeout), self.event_slot
        
        return args if slot_index == 1 else None
            

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
        self.event_slot.plug(self.process_event)
        
        self.plan = None
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
        else:
            self.may_finish()
        
        
    def do_slot(self, li, action):
        self.logger.debug("Planned routing processing a %s" % action["type"])
        self.event_slot.zap(li, action)
        



class Bridge(CallComponent):
    def __init__(self):
        CallComponent.__init__(self)

        self.incoming_leg = None
        self.outgoing_leg = None
        
        
    def stand(self):
        incoming_leg = self.call.make_slot(self, 0)
        self.incoming_leg = proxy(incoming_leg)

        outgoing_leg = self.call.make_slot(self, 1)
        self.outgoing_leg = proxy(outgoing_leg)
        
        routing = self.call.make_thing("routing", self.path, "routing")
        self.call.link_leg_to_thing(outgoing_leg, routing)
        
        return incoming_leg.stand()
        

    def may_finish(self):  # TODO: this is probably screwed up now
        if self.outgoing_leg:
            self.logger.debug("Releasing outgoing leg.")
            self.outgoing_leg.finish_media()
            self.outgoing_leg = None

        if self.incoming_leg:
            self.logger.debug("Releasing incoming leg.")
            self.incoming_leg.finish_media()
            self.incoming_leg = None
            
        self.logger.debug("Bridge finished.")
        # Since only the legs held a reference to self, we may be destroyed
        # as soon as this method returns.
            

    def do_slot(self, li, action):
        type = action["type"]
        
        if li == 0:
            leg = self.outgoing_leg
            direction = "forward"
        else:
            leg = self.incoming_leg
            direction = "backward"
        
        self.logger.debug("Bridging %s %s" % (type, direction))
        leg.report(action)

        if type in ("hangup", "reject"):
            self.may_finish()

    
class RecordingBridge(Bridge):
    def hack_media(self, li, answer):
        old = len(self.incoming_leg.media_legs)
        new = len(answer["channels"])
        
        for i in range(old, new):
            this = self.incoming_leg.make_media_leg("pass")
            that = self.outgoing_leg.make_media_leg("pass")

            # Pairing must happen before setting it, because realizing needs it
            this.pair(proxy(that))
            that.pair(proxy(this))
            
            self.incoming_leg.set_media_leg(i, this)
            self.outgoing_leg.set_media_leg(i, that)
            
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
