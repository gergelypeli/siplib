from async import WeakMethod, Weak, WeakGeneratorMethod
from format import Status
from mgc import MediaChannel
from planner import Planner
import logging

logger = logging.getLogger(__name__)


class Routing(object):
    def __init__(self, call, report, incoming_leg):
        # The report handler is for the finish and anchor events.
        # Both may be forwarded by an owning internal leg,
        # or be processed by the owning Call.
        
        self.call = call
        self.report = report
        self.legs = {}
        self.add_leg(incoming_leg)


    def add_leg(self, leg):
        li = max(self.legs.keys()) + 1 if self.legs else 0

        self.legs[li] = leg
        leg.set_report(WeakMethod(self.process, li))


    def remove_leg(self, li):
        self.legs[li] = None  # keep entry to keep on counting the legs
        
        if not any(self.legs.values()):
            self.finish_routing()
            

    def finish_routing(self):
        self.report(dict(type="finish"))
        
        
    def fail_routing(self, exception):  # TODO
        logger.warning("Routing failed!")
        self.legs[0].do(dict(type="fail", status=Status(500)))


    def anchor_routing(self, li, further_legs=[]):  # TODO: rename to done?
        left = self.legs.pop(0)
        right = self.legs.pop(li)
        
        self.report(dict(type="anchor", legs=[ left, right ] + further_legs))
        
        if not self.legs:
            self.finish_routing()
        
        
    def process(action, li):
        raise NotImplementedError()
        
        
class SimpleRouting(Routing):
    def route_call(self, ctx):
        raise NotImplementedError()


    def start_routing(self, action):
        try:
            outgoing_leg = self.route_call(action["ctx"])
            if not outgoing_leg:
                raise Exception("Routing failed for an unknown reason!")
        except Exception as e:
            self.fail_routing(e)
        else:
            self.add_leg(outgoing_leg)
            outgoing_leg.do(action)
        
        
    def process(self, action, li):  # li is second arg because it is bound TODO: use kwargs?
        type = action["type"]
        logger.debug("Routing %s from leg %d." % (type, li))

        if type == "finish":
            self.remove_leg(li)
        elif type == "anchor":
            self.anchor_routing(li, action["legs"])
        elif type == "dial":
            self.start_routing(action)  # async friendly way
        elif type == "reject":
            incoming_leg = self.legs[0]
            incoming_leg.do(action)
        else:
            logger.debug("Implicit anchoring")
            incoming_leg = self.legs[0]
            self.anchor_routing(li)
            incoming_leg.do(action)


class PlannedSimpleRouting(SimpleRouting):
    class RoutingPlanner(Planner):
        pass

            
    def __init__(self, call, report, incoming_leg, metapoll):
        super(PlannedSimpleRouting, self).__init__(call, report, incoming_leg)
        
        self.metapoll = metapoll


    def start_routing(self, action):
        self.planner = self.RoutingPlanner(
            self.metapoll,
            WeakGeneratorMethod(self.plan),
            finish_handler=WeakMethod(self.routing_finished, action),
            error_handler=WeakMethod(self.fail_routing)
        )
        self.planner.start(action["ctx"])
    
    
    def routing_finished(self, outgoing_leg, action):
        self.add_leg(outgoing_leg)
        outgoing_leg.do(action)
        

    #def process(self, action, li):
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
        
        
    def make_routing(self, incoming_leg):
        return Routing(Weak(self), WeakMethod(self.routed), incoming_leg)
        
        
    def start_routing(self, incoming_leg):
        self.routing = self.make_routing(incoming_leg)  # just to be sure it's stored
        
        
    def routed(self, action):
        type = action["type"]
        
        if type == "finish":
            self.routing = None
            
            if self.legs:
                logger.debug("Routing finished")
            else:
                logger.debug("Oops, routing finished without success!")
                self.finish_handler(self)
                # TODO
        elif type == "anchor":
            logger.debug("Yay, anchored.")
            self.legs = action["legs"]

            # TODO: let the Routing connect them to each other?
            for i, leg in enumerate(self.legs):
                leg.set_report(WeakMethod(self.process, i))
                
            self.media_channels = []
            self.refresh_media()
        else:
            logger.debug("Unknown routing event %s!" % type)
        
        
    def allocate_media_address(self, channel_index):
        # TODO: check existing channels
        return self.mgc.allocate_media_address(channel_index)
        
        
    def deallocate_media_address(self, addr):  # TODO: may not be necessary
        self.mgc.deallocate_media_address(addr)
        
        
    def refresh_media(self):
        if self.media_channels is None:
            logger.debug("Not media yet to refresh.")
            return
            
        left_media_legs = self.legs[0].media_legs
        ln = len(left_media_legs)
        right_media_legs = self.legs[-1].media_legs
        rn = len(right_media_legs)
        channel_count = min(ln, rn)  # TODO: max?
        logger.debug("Refreshing media (%d channels)" % channel_count)
        
        for i in range(channel_count):
            if i < len(self.media_channels):
                c = self.media_channels[i]
            else:
                c = MediaChannel(self.mgc)
                self.media_channels.append(c)

            c.set_legs([ left_media_legs[i], right_media_legs[i] ])
        
        
    def process(self, action, li):  # li is second arg because it is bound
        lj = 1 - li
        type = action["type"]
        
        if type == "finish":
            logger.debug("Bridged leg %d finished." % li)
            self.legs[li] = None
            
            for leg in self.legs:
                if leg:
                    break
            else:
                if any(self.media_channels):
                    logger.debug("Deleting media channels.")
                    for i, mc in enumerate(self.media_channels):
                        if mc:
                            mc.delete(WeakMethod(self.media_deleted, i))
                else:
                    logger.debug("Call is finished.")
                    self.finish_handler(self)
        else:
            logger.debug("Bridging %s from leg %d." % (type, li))
            self.legs[lj].do(action)


    def media_deleted(self, li):
        self.media_channels[li] = None
        
        if not any(self.media_channels):
            logger.debug("Call is finished.")
            self.finish_handler(self)


# The incoming leg must have a context initialized from the INVITE, then
# this leg passed to the routing, which returns the outgoing leg, whose
# context is initialized there. The outgoing leg uses its context to
# reconstruct the dial.
# Es ne kelljen a konstruktor-parametereket a legfelsobb szintig feltolni.
