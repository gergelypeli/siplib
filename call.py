from async import WeakMethod, Weak
from mgc import MediaChannel
#from planner import Planner


class Routing(object):
    def __init__(self, call, report, incoming_leg):
        # The report handler is for the finish and anchor events.
        # Both may be forwarded by an owning internal leg,
        # or be processed by the owning Call.
        
        self.call = call
        self.report = report
        self.legs = {}
        self.add_leg(0, incoming_leg)


    def add_leg(self, li, leg):
        self.legs[li] = leg
        leg.set_report(WeakMethod(self.process, li))


    def route_call(self, ctx):
        return None


    def dial_action(self, li, action):
        lj = 1 - li

        src_ctx = action["ctx"]
        dst_ctx = src_ctx.copy()
        action["ctx"] = dst_ctx

        outgoing_leg = self.route_call(dst_ctx)
        self.add_leg(lj, outgoing_leg)
        
        self.legs[lj].do(action)


    def anchor_action(self, li, legs):
        left = self.legs.pop(0)
        right = self.legs.pop(li)
        
        self.report(dict(type="anchor", legs=[ left, right ] + legs))
        
        return left
        

    def process(self, action, li):  # li is second arg because it is bound
        #lj = 1 - li
        type = action["type"]
        print("Routing %s from leg %d." % (type, li))

        if type == "finish":
            self.legs.pop(li)
        elif type == "anchor":
            self.anchor_action(li, action["legs"])
        elif type == "dial":
            self.dial_action(li, action)
        #elif type == "refresh":
        #    print("Ignoring refresh during routing.")
        else:
            print("Implicit anchoring")
            incoming_leg = self.anchor_action(li, [])
            incoming_leg.do(action)

        if not self.legs:
            self.report(dict(type="finish"))


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
                print("Routing finished")
            else:
                print("Oops, routing finished without success!")
                self.finish_handler(self)
                # TODO
        elif type == "anchor":
            print("Yay, anchored.")
            self.legs = action["legs"]

            # TODO: let the Routing connect them to each other?
            for i, leg in enumerate(self.legs):
                leg.set_report(WeakMethod(self.process, i))
                
            self.media_channels = []
            self.refresh_media()
        else:
            print("Unknown routing event %s!" % type)
        
        
    def allocate_media_address(self, channel_index):
        # TODO: check existing channels
        return self.mgc.allocate_media_address(channel_index)
        
        
    def deallocate_media_address(self, addr):  # TODO: may not be necessary
        self.mgc.deallocate_media_address(addr)
        
        
    def refresh_media(self):
        if self.media_channels is None:
            print("Not media yet to refresh.")
            return
            
        left_media_legs = self.legs[0].media_legs
        ln = len(left_media_legs)
        right_media_legs = self.legs[-1].media_legs
        rn = len(right_media_legs)
        channel_count = min(ln, rn)  # TODO: max?
        print("Refreshing media (%d channels)" % channel_count)
        
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
            print("Bridged leg %d finished." % li)
            self.legs[li] = None
            
            for leg in self.legs:
                if leg:
                    break
            else:
                if any(self.media_channels):
                    print("Deleting media channels.")
                    for i, mc in enumerate(self.media_channels):
                        if mc:
                            mc.delete(WeakMethod(self.media_deleted, i))
                else:
                    print("Call is finished.")
                    self.finish_handler(self)
        else:
            print("Bridging %s from leg %d." % (type, li))
            self.legs[lj].do(action)


    def media_deleted(self, li):
        self.media_channels[li] = None
        
        if not any(self.media_channels):
            print("Call is finished.")
            self.finish_handler(self)


# The incoming leg must have a context initialized from the INVITE, then
# this leg passed to the routing, which returns the outgoing leg, whose
# context is initialized there. The outgoing leg uses its context to
# reconstruct the dial.
# Es ne kelljen a konstruktor-parametereket a legfelsobb szintig feltolni.
