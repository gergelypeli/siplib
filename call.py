from async import WeakMethod, Weak, WeakGeneratorMethod
from format import Status, SipError
from mgc import MediaChannel
from planner import Planner, PlannedEvent
from util import build_oid, Logger


class Routing(object):
    def __init__(self, call):
        # The report handler is for the finish and anchor events.
        # Both may be forwarded by an owning internal leg,
        # or be processed by the owning Call.
        
        self.call = call
        self.report = None
        self.oid = "routing=x"
        self.legs = {}
        #self.logger = logging.LoggerAdapter(logger, {})
        self.logger = Logger()
    
    
    def set_oid(self, oid):
        self.logger.set_oid(oid)
        
        
    def set_report(self, report):
        self.report = report


    def add_leg(self, leg):
        li = max(self.legs.keys()) + 1 if self.legs else 0
        leg_oid = self.call.generate_leg_oid()

        if not self.legs:
            self.set_oid(build_oid(leg_oid, "routing"))

        self.legs[li] = leg
        leg.set_report(WeakMethod(self.process, li).bind_front())
        leg.set_oid(leg_oid)
        leg.set_call(self.call)
        
        leg.start()


    def remove_leg(self, li):
        self.legs[li] = None  # keep entry to keep on counting the legs
        
        if not any(self.legs.values()):
            self.finish_routing()
            

    def finish_routing(self):
        self.report(dict(type="finish"))
        
        
    def reject(self, status):
        self.logger.warning("Rejecting with status %s" % (status,))
        
        self.legs[0].do(dict(type="reject", status=status))


    def anchor(self, li):
        self.logger.debug("Anchoring to leg %d." % li)
        
        these_legs = [ self.legs[0], self.legs[li] ]
        further_legs = self.legs[li].get_further_legs()
        
        self.report(dict(type="anchor", legs=these_legs + further_legs))

        self.remove_leg(li)
        self.remove_leg(0)
        
        
    def cancel(self, status=None):
        for li, leg in self.legs.items():
            if leg:
                self.logger.debug("Cancelling leg %s" % li)
                
                if li > 0:
                    leg.do(dict(type="cancel"))
                else:
                    # This may not happen, if we cancel after an anchoring,
                    # then the incoming leg already got a positive response.
                    self.reject(status or Status(487))


    def forward(self, li, action):
        # Any positive feedback from outgoing legs can be anchored,
        # because it means that that leg is also anchored itself.
        # But can't just anchor right after routing, because the
        # outgoing leg may still be thinking.
        self.logger.debug("Forwarding %s to incoming leg." % action["type"])
        
        incoming_leg = self.legs[0]
        self.anchor(li)
        incoming_leg.do(action)
        self.cancel()  # the remaining legs only


    def dial(self, action):
        uri = action["ctx"]["uri"]
        self.logger.debug("Dialing out to: %s" % (uri,))
        leg = self.call.switch.make_outgoing_leg(uri)
        self.add_leg(leg)
        leg.do(action)


    def default_process(self, li, action):
        type = action["type"]
        self.logger.debug("Got %s from leg %d." % (type, li))

        if type == "finish":
            self.remove_leg(li)
        elif li == 0:
            if type == "dial":
                raise Exception("Should have handled dial before default_process!")
            elif type == "cancel":
                self.cancel()
            else:
                raise Exception("Invalid action from incoming leg: %s" % type)
        else:
            if type == "reject":
                self.reject(action["status"])
            elif type in ("ring", "accept", "session"):
                self.forward(li, action)
            else:
                raise Exception("Invalid action from outgoing leg: %s" % type)


    def process(self, li, action):
        raise NotImplementedError()


class SimpleRouting(Routing):
    def route_ctx(self, ctx):
        raise NotImplementedError()


    def process(self, li, action):
        if action["type"] == "dial":
            try:
                ctx = action["ctx"].copy()
                self.route_ctx(ctx)
            except Exception:
                self.reject(Status(500))  # TODO
            else:
                self.dial(dict(action, ctx=ctx))
        else:
            self.default_process(li, action)


class PlannedRouting(Routing):
    class RoutingPlanner(Planner):
        def wait_leg_action(self, action_type=None, timeout=None):
            planned_event = yield from self.suspend(expect="action", timeout=timeout)
            li, action = planned_event.event
            
            if action_type and action["type"] != action_type:
                raise Exception("Expected action %s, got %s!" % (action_type, action["type"]))
                
            return li, action

            
    def set_oid(self, oid):
        Routing.set_oid(self, oid)
        self.oid = oid


    #def cancel(self):
        # TODO: this may be a bit too strong, why don't we just resume the plan?
        # FIXME: and why don't we resume with every action???
    #    self.planner.resume(SipError(Status(487)))
        
    
    def plan_finished(self, exception):
        status = None
        
        try:
            if exception:
                raise exception
                
            if 0 in self.legs:
                raise Exception("Routing plan completed without anchoring!")
        except SipError as e:
            status = e.status
        except:
            status = Status(500)
            
        if status:
            self.cancel(status)
        
        
    def process(self, li, action):
        if action["type"] == "dial":
            self.planner = self.RoutingPlanner(
                self.call.switch.metapoll,
                WeakGeneratorMethod(self.plan),
                finish_handler=WeakMethod(self.plan_finished)
            )
            self.planner.set_oid(build_oid(self.oid, "planner"))
            self.planner.start(action)
        else:
            self.planner.resume(PlannedEvent("action", (li, action)))
        

    def plan(self, planner, action):
        raise NotImplementedError()
        


class Call(object):
    def __init__(self, switch):
        self.switch = switch
        #self.mgc = mgc
        #self.finish_handler = finish_handler
        self.legs = None
        self.media_channels = None
        self.routing = None
        self.oid = "call=x"
        self.leg_count = 0

        self.logger = Logger()


    def set_oid(self, oid):
        self.oid = oid
        self.logger.set_oid(oid)
        
        
    def generate_leg_oid(self):
        leg_oid = build_oid(self.oid, "leg", self.leg_count)
        self.leg_count += 1

        return leg_oid
        
        
    def finish(self):
        self.switch.finish_call(self.oid)
        
        
    def make_routing(self):
        return Routing(Weak(self))
        
        
    def start_routing(self, incoming_leg):
        self.routing = self.make_routing()  # just to be sure it's stored
        self.routing.set_report(WeakMethod(self.routed))
        self.routing.add_leg(incoming_leg)
        
        
    def routed(self, action):
        type = action["type"]
        
        if type == "finish":
            self.routing = None
            
            if self.legs:
                self.logger.debug("Routing finished")
            else:
                self.logger.debug("Oops, routing finished without success!")
                self.finish()
                # TODO
        elif type == "anchor":
            self.logger.debug("Yay, anchored.")
            self.legs = action["legs"]

            # TODO: let the Routing connect them to each other?
            for i, leg in enumerate(self.legs):
                leg.set_report(WeakMethod(self.process, i).bind_front())
                
            self.media_channels = []
            #self.refresh_media()
        else:
            self.logger.debug("Unknown routing event %s!" % type)
        
        
    def select_gateway_sid(self, channel_index):
        # TODO: check existing channels
        return self.switch.mgc.select_gateway_sid()
        
        
    def allocate_media_addr(self, sid):
        return self.switch.mgc.allocate_media_addr(sid)
        
        
    def deallocate_media_addr(self, sid, addr):  # TODO: may not be necessary
        self.switch.mgc.deallocate_media_addr(sid, addr)
        
        
    def refresh_media(self):
        if self.media_channels is None:
            self.logger.debug("Not media yet to refresh.")
            return
            
        left_media_legs = self.legs[0].media_legs
        ln = len(left_media_legs)
        right_media_legs = self.legs[-1].media_legs
        rn = len(right_media_legs)
        
        if ln != rn:
            raise Exception("Media leg count mismatch, left has %d but right has %d!" % (ln, rn))
        
        channel_count = min(ln, rn)  # TODO: max?
        self.logger.debug("Refreshing media (%d channels)" % channel_count)
        
        for i in range(channel_count):
            if i < len(self.media_channels):
                c = self.media_channels[i]
            else:
                c = MediaChannel(self.switch.mgc)
                c.set_oid(build_oid(self.oid, "channel", len(self.media_channels)))
                self.media_channels.append(c)

            c.set_legs([ left_media_legs[i], right_media_legs[i] ])
        
        
    def process(self, li, action):
        type = action["type"]
        
        if type == "finish":
            # TODO: do something if a leg is just screwed up, and another is
            # still up, thinking that everything is OK!
            
            self.logger.debug("Bridged leg %d finished." % li)
            self.legs[li] = None
            
            if action.get("error"):
                self.logger.warning("Leg screwed, tearing down others!")
                for leg in self.legs:
                    if leg:
                        leg.do(dict(type="hangup"))
            
            if not any(self.legs):
                if any(self.media_channels):
                    self.logger.debug("Deleting media channels.")
                    for i, mc in enumerate(self.media_channels):
                        if mc:
                            mc.delete(WeakMethod(self.media_deleted, i))
                else:
                    self.logger.debug("Call is finished.")
                    self.finish()
        else:
            lj = 1 - li
            self.logger.debug("Bridging %s from leg %d to %d." % (type, li, lj))
            self.legs[lj].do(action)
            
            if "answer" in action:
                self.refresh_media()
            


    def media_deleted(self, li):
        self.media_channels[li] = None
        
        if not any(self.media_channels):
            self.logger.debug("Call is finished.")
            self.finish()
