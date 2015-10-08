from async import WeakMethod, Weak, WeakGeneratorMethod
from format import Status
from mgc import MediaChannel
from planner import Planner
from util import build_oid, Logger


class Routing(object):
    def __init__(self, call, report):
        # The report handler is for the finish and anchor events.
        # Both may be forwarded by an owning internal leg,
        # or be processed by the owning Call.
        
        self.call = call
        self.report = report
        self.oid = "routing=x"
        self.legs = {}
        #self.logger = logging.LoggerAdapter(logger, {})
        self.logger = Logger()
    
    
    def set_oid(self, oid):
        self.logger.set_oid(oid)


    def add_leg(self, leg):
        li = max(self.legs.keys()) + 1 if self.legs else 0

        self.legs[li] = leg
        leg.set_report(WeakMethod(self.process, li).bind_front())


    def remove_leg(self, li):
        self.legs[li] = None  # keep entry to keep on counting the legs
        
        if not any(self.legs.values()):
            self.finish_routing()
            

    def finish_routing(self):
        self.report(dict(type="finish"))
        
        
    def reject(self, status):
        self.logger.warning("Routing rejected: %s" % (status,))
        self.legs[0].do(dict(type="reject", status=status))


    def anchor(self, li):
        these_legs = [ self.legs[0], self.legs[li] ]
        further_legs = self.legs[li].get_further_legs()
        
        self.report(dict(type="anchor", legs=these_legs + further_legs))

        self.remove_leg(li)
        self.remove_leg(0)
        
        
    def dial(self, action):
        raise NotImplementedError()
        
        
    def cancel(self, action):
        for li, leg in self.legs.items():
            if li > 0 and leg:
                leg.do(action)


    def forward(self, li, action):
        # Any positive feedback from outgoing legs can be anchored,
        # because it means that that leg is also anchored itself.
        # But can't just anchor right after routing, because the
        # outgoing leg may still be thinking.
        incoming_leg = self.legs[0]
        self.anchor(li)
        incoming_leg.do(action)


    def process(self, li, action):
        type = action["type"]
        self.logger.debug("Got %s from leg %d." % (type, li))

        if type == "finish":
            self.remove_leg(li)
        elif li == 0:
            if type == "dial":
                self.dial(action)
            elif type == "cancel":
                self.cancel(action)
            else:
                raise Exception("Invalid action from incoming leg: %s" % type)
        else:
            if type == "reject":
                self.reject(action["status"])
            else:
                self.forward(li, action)


class SimpleRouting(Routing):
    def route_call(self, ctx):
        raise NotImplementedError()


    def dial(self, action):
        try:
            outgoing_leg = self.route_call(action["ctx"])
            if not outgoing_leg:
                raise Exception("Routing failed for an unknown reason!")
        except Exception:
            self.reject(Status(500))  # TODO
        else:
            oid = self.call.generate_leg_oid()
            outgoing_leg.set_oid(oid)
            self.add_leg(outgoing_leg)
            outgoing_leg.start()
            outgoing_leg.do(action)


class PlannedSimpleRouting(SimpleRouting):
    class RoutingPlanner(Planner):
        pass

            
    def __init__(self, call, report, metapoll):
        super(PlannedSimpleRouting, self).__init__(call, report)
        
        self.metapoll = metapoll


    def set_oid(self, oid):
        SimpleRouting.set_oid(self, oid)
        self.oid = oid


    def start_routing(self, action):
        self.planner = self.RoutingPlanner(
            self.metapoll,
            WeakGeneratorMethod(self.plan),
            finish_handler=WeakMethod(self.routing_finished, action),
            error_handler=WeakMethod(self.fail_routing)
        )
        self.planner.set_oid(build_oid(self.oid, "planner"))
        self.planner.start(action["ctx"])
    
    
    def routing_finished(self, outgoing_leg, action):
        self.add_leg(outgoing_leg)
        outgoing_leg.do(action)
        

    #def process(self, li, action):
    #    self.planner.resume(PlannedEvent("action", (li, action)))


    def plan(self, planner):
        raise NotImplementedError()
        


class Call(object):
    def __init__(self, mgc, finish_handler):
        self.mgc = mgc
        self.finish_handler = finish_handler
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
        self.finish_handler(self.oid)
        
        
    def make_routing(self):
        return Routing(Weak(self), WeakMethod(self.routed))
        
        
    def start_routing(self, incoming_leg):
        self.routing = self.make_routing()  # just to be sure it's stored

        leg_oid = self.generate_leg_oid()
        incoming_leg.set_oid(leg_oid)
        self.routing.set_oid(build_oid(leg_oid, "routing"))
        
        self.routing.add_leg(incoming_leg)
        incoming_leg.start()
        
        
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
                leg.set_report(WeakMethod(self.process, i))
                
            self.media_channels = []
            self.refresh_media()
        else:
            self.logger.debug("Unknown routing event %s!" % type)
        
        
    def allocate_media_address(self, channel_index):
        # TODO: check existing channels
        return self.mgc.allocate_media_address(channel_index)
        
        
    def deallocate_media_address(self, addr):  # TODO: may not be necessary
        self.mgc.deallocate_media_address(addr)
        
        
    def refresh_media(self):
        if self.media_channels is None:
            self.logger.debug("Not media yet to refresh.")
            return
            
        left_media_legs = self.legs[0].media_legs
        ln = len(left_media_legs)
        right_media_legs = self.legs[-1].media_legs
        rn = len(right_media_legs)
        channel_count = min(ln, rn)  # TODO: max?
        self.logger.debug("Refreshing media (%d channels)" % channel_count)
        
        for i in range(channel_count):
            if i < len(self.media_channels):
                c = self.media_channels[i]
            else:
                c = MediaChannel(self.mgc)
                c.set_oid(build_oid(self.oid, "channel", len(self.media_channels)))
                self.media_channels.append(c)

            c.set_legs([ left_media_legs[i], right_media_legs[i] ])
        
        
    def process(self, action, li):  # li is second arg because it is bound
        lj = 1 - li
        type = action["type"]
        
        if type == "finish":
            self.logger.debug("Bridged leg %d finished." % li)
            self.legs[li] = None
            
            for leg in self.legs:
                if leg:
                    break
            else:
                if any(self.media_channels):
                    self.logger.debug("Deleting media channels.")
                    for i, mc in enumerate(self.media_channels):
                        if mc:
                            mc.delete(WeakMethod(self.media_deleted, i))
                else:
                    self.logger.debug("Call is finished.")
                    self.finish()
        else:
            self.logger.debug("Bridging %s from leg %d." % (type, li))
            self.legs[lj].do(action)


    def media_deleted(self, li):
        self.media_channels[li] = None
        
        if not any(self.media_channels):
            self.logger.debug("Call is finished.")
            self.finish()


# The incoming leg must have a context initialized from the INVITE, then
# this leg passed to the routing, which returns the outgoing leg, whose
# context is initialized there. The outgoing leg uses its context to
# reconstruct the dial.
# Es ne kelljen a konstruktor-parametereket a legfelsobb szintig feltolni.
