from async import WeakMethod, Weak, WeakGeneratorMethod
from format import Status, SipError
from mgc import MediaContext
from planner import Planned
from util import build_oid, Loggable


class Routing(Loggable):
    def __init__(self, call):
        Loggable.__init__(self)

        # The report handler is for the finish and anchor events.
        # Both may be forwarded by an owning internal leg,
        # or be processed by the owning Call.
        
        self.call = call
        self.report = None
        self.leg_count = 0
        self.legs = {}
        self.sent_ringback = False
        self.queued_actions = {}
    
    
    def set_report(self, report):
        self.report = report


    def add_leg(self, leg):
        li = self.leg_count
        self.leg_count += 1

        if not self.legs:
            self.set_oid(build_oid(leg.oid, "routing"))

        self.legs[li] = leg
        self.queued_actions[li] = []
        
        leg.set_report(WeakMethod(self.process, li).bind_front())
        leg.set_call(self.call)
        
        leg.start()


    def remove_leg(self, li):
        self.legs.pop(li)
        self.queued_actions.pop(li)
        self.may_finish()
            

    def may_finish(self):
        if not self.legs:
            self.report(dict(type="finish"))


    def queue(self, li, action):
        self.logger.debug("Queueing %s from leg %s." % (action["type"], li))
        self.queued_actions[li].append(action)


    def reject(self, status):
        self.logger.warning("Rejecting with status %s" % (status,))
        self.legs[0].do(dict(type="reject", status=status))
            
            
    def ringback(self):
        if not self.sent_ringback:
            self.sent_ringback = True
            self.logger.debug("Sending artificial ringback.")
            self.legs[0].do(dict(type="ring"))


    def hangup_all_outgoing(self):
        for li, leg in list(self.legs.items()):
            self.logger.debug("Hanging up leg %s" % li)
            
            if li > 0:
                leg.do(dict(type="hangup"))

        
    def anchor(self, li):
        self.logger.debug("Anchoring to leg %d." % li)
        
        these_legs = [ self.legs[0], self.legs[li] ]
        further_legs = self.legs[li].get_further_legs()
        queued_actions = self.queued_actions[li]
        self.report(dict(
            type="anchor",
            legs=these_legs + further_legs,
            queued_actions=queued_actions
        ))

        self.remove_leg(0)
        self.remove_leg(li)
        

    def dial(self, action):
        if action["type"] != "dial":
            raise Exception("Dial action is not a dial: %s" % action["type"])
            
        uri = action["ctx"]["uri"]
        self.logger.debug("Dialing out to: %s" % (uri,))
        leg = self.call.make_outgoing_leg(uri)
        self.add_leg(leg)
        leg.do(action)


    def process(self, li, action):
        type = action["type"]
        self.logger.debug("Got %s from leg %d." % (type, li))

        if type == "finish":
            self.remove_leg(li)
        elif li == 0:
            if type == "dial":
                raise Exception("Should have handled dial in a subclass!")
            elif type == "hangup":
                self.hangup_all_outgoing()
            else:
                raise Exception("Invalid action from incoming leg: %s" % type)
        else:
            if type == "reject":
                self.reject(action["status"])
            elif type == "ring":
                if action.get("offer") or action.get("answer"):
                    action["type"] = "session"
                    self.queue(li, action)
                    
                self.ringback()
            elif type == "session":
                self.queue(li, action)
            elif type == "accept":
                self.queue(li, action)
                self.anchor(li)
                self.hangup_all_outgoing()  # the remaining legs
            else:
                raise Exception("Invalid action from outgoing leg: %s" % type)


class SimpleRouting(Routing):
    def route(self, action):
        raise NotImplementedError()


    def process(self, li, action):
        if action["type"] == "dial":
            try:
                self.route(action)
            except SipError as e:
                self.logger.error("Simple routing SIP error: %s" % e.status)
                self.reject(e.status)
            except Exception as e:
                self.logger.error("Simple routing internal error: %s" % e)
                self.reject(Status(500))
            else:
                self.dial(action)
        else:
            Routing.process(self, li, action)


class PlannedRouting(Planned, Routing):
    def __init__(self, call):
        Planned.__init__(self,
                call.switch.metapoll,
                WeakGeneratorMethod(self.plan),
                finish_handler=WeakMethod(self.plan_finished)
        )
        Routing.__init__(self, call)
        
        
    def wait_action(self, leg_index=None, action_type=None, timeout=None):
        tag, event = yield from self.suspend(expect="action", timeout=timeout)
        li, action = event

        if leg_index is not None and li != leg_index:
            raise Exception("Expected action from %s, got from %s!" % (leg_index, li))

        if action_type and action["type"] != action_type:
            raise Exception("Expected action %s, got %s!" % (action_type, action["type"]))
        
        return li, action


    def may_finish(self):
        if not self.generator:
            Routing.may_finish(self)
            

    def plan_finished(self, exception):
        for tag, event in self.event_queue:
            if tag == "action":
                li, action = event
                Routing.process(self, li, action)
                
        self.event_queue = None
        status = None
        
        try:
            if exception:
                self.logger.debug("Routing plan finished with: %s" % exception)
                raise exception
            
            if len(self.legs) < 2:
                raise Exception("Routing plan completed without creating outgoing legs!")
        except SipError as e:
            self.logger.error("Routing plan aborted with SIP error: %s" % e)
            status = e.status
        except Exception as e:
            self.logger.error("Routing plan aborted with exception: %s" % e)
            status = Status(500)
            
        if status:
            # TODO: must handle double events for this to work well!
            self.hangup_all_outgoing()
            self.reject(status)
            
        self.may_finish()
        
        
    def process(self, li, action):
        self.logger.debug("Planned routing processing a %s" % action["type"])
        
        if action["type"] == "dial":
            self.start(action)
        elif self.generator:
            self.resume("action", (li, action))
        else:
            Routing.process(self, li, action)
        

    def plan(self, action):
        raise NotImplementedError()


class Routable:  # Loggable
    def __init__(self):
        self.routing = None
        self.legs = []


    def make_routing(self):
        raise NotImplementedError()


    def generate_leg_oid(self):
        raise NotImplementedError()


    def start_routing(self, incoming_leg):
        incoming_leg.set_oid(self.generate_leg_oid())
        
        self.routing = self.make_routing()  # just to be sure it's stored
        self.routing.set_report(WeakMethod(self.reported))
        self.routing.add_leg(incoming_leg)


    def reported(self, action):
        type = action["type"]
        
        if type == "finish":
            self.routing = None
            self.logger.debug("Routing finished after anchoring %d legs." % len(self.legs))
        elif type == "anchor":
            self.legs = action["legs"]
            self.logger.debug("Routing anchored %d legs." % len(self.legs))

            for i, leg in enumerate(self.legs):
                leg.set_report(WeakMethod(self.forward, i).bind_front())
                
            for queued_action in action["queued_actions"]:
                self.forward(1, queued_action)
        else:
            self.logger.debug("Unknown routing event %s!" % type)


    def forward(self, li, action):
        type = action["type"]
        
        if type == "finish":
            self.logger.debug("Anchored leg %d finished." % li)
            self.legs[li] = None
            
            if action.get("error"):
                self.logger.error("Leg aborted with: %s" % action["error"])
                
                for leg in self.legs:
                    if leg:
                        leg.do(dict(type="hangup"))
        else:
            lj = li + 1 - 2 * (li % 2)
            self.logger.debug("Forwarding %s from leg %d to %d." % (type, li, lj))
            self.legs[lj].do(action)


class Call(Loggable, Routable):
    def __init__(self, switch):
        Loggable.__init__(self)
        Routable.__init__(self)

        self.switch = switch
        self.media_channels = []
        self.leg_count = 0


    def generate_leg_oid(self):
        leg_oid = build_oid(self.oid, "leg", self.leg_count)
        self.leg_count += 1
        
        return leg_oid


    def make_outgoing_leg(self, uri):
        leg = self.switch.make_outgoing_leg(uri)
        leg.set_oid(self.generate_leg_oid())

        return leg
        
        
    def may_finish(self):
        if any(self.legs):
            return
            
        if any(self.media_channels):
            return
            
        self.logger.debug("Call is finished.")
        self.switch.finish_call(self.oid)
        
        
    def make_routing(self):
        return Routing(Weak(self))
        
        
    def reported(self, action):
        Routable.reported(self, action)
        
        type = action["type"]
        
        if type == "finish":
            # Okay to stay if legs are present
            if not any(self.legs):
                self.finish_media()


    def make_media_leg(self, channel_index, type):
        # TODO
        sid_affinity = None
        return self.switch.mgc.make_media_leg(sid_affinity, type)
        
        
    def refresh_media(self):
        leg_count = len(self.legs)
        channel_count = max(len(leg.media_legs) for leg in self.legs)
        
        for ci in range(channel_count):
            li = 0
            media_legs = [ leg.media_legs[ci] if ci < len(leg.media_legs) else None for leg in self.legs ]
            spans = set()
            
            while li < leg_count:
                while li < leg_count and not media_legs[li]:
                    li += 1
                    
                if li == leg_count:
                    break
                    
                if li % 2 != 0:
                    li += 1
                    continue
                    
                left = li
                li += 1

                while li < leg_count and not media_legs[li]:
                    li += 1
                    
                if li == leg_count:
                    break
                    
                if li % 2 != 1:
                    continue
                
                right = li
                li += 1
                
                span = left, right
                spans.add(span)
                
            if ci >= len(self.media_channels):
                self.media_channels.append({})
                
            media_contexts_by_span = self.media_channels[ci]
            
            for span in media_contexts_by_span:
                left, right = span
                
                if span not in spans:
                    self.logger.debug("Removing media context for channel %d span %d-%d" % (ci, left, right))
                    media_contexts_by_span.pop(span)
                    
            for span in spans:
                left, right = span
                
                if span not in media_contexts_by_span:
                    self.logger.debug("Adding media context for channel %d span %d-%d" % (ci, left, right))
                    mc = MediaContext(self.switch.mgc)
                    soid = build_oid(self.oid, "legs", "%d-%d" % (left, right))
                    coid = build_oid(soid, "channel", ci)
                    mc.set_oid(coid)
                    media_contexts_by_span[span] = mc
                else:
                    mc = media_contexts_by_span[span]
                    
                mc.set_legs([ media_legs[left], media_legs[right] ])
                
        
    def forward(self, li, action):
        Routable.forward(self, li, action)
        
        type = action["type"]
        
        if type == "finish":
            # Clean up after the last leg is gone
            if not any(self.legs):
                self.finish_media()
        else:
            if action.get("answer"):
                self.refresh_media()


    def media_finished(self, ci, span):
        # Completed
        self.media_channels[ci].pop(span)
        
        self.may_finish()


    def finish_media(self):
        # Initiated
        for ci, mcbs in enumerate(self.media_channels):
            for span, mc in mcbs.items():
                left, right = span
                self.logger.debug("Finishing media channel %d span %d-%d." % (ci, left, right))
                mc.delete(WeakMethod(self.media_finished, ci, span))
            
        self.may_finish()
