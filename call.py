from async import WeakMethod
from planner import Planner


class Call(object):
    def __init__(self, switch):
        self.switch = switch
        self.legs = {}
        self.media_channels = []
        
        
    def add_leg(self, li, leg):
        self.legs[li] = leg
        leg.set_report(WeakMethod(self.process, li))
    
    
    def mangle_session(self, li, sdp):
        lj = 1 - li
        
        for i in range(len(sdp.channels)):
            local_addr = self.media_channels[i].legs[lj].get_local_addr()
            print("Mangling leg %d channel %d with %s" % (lj, i, local_addr))
            sdp.channels[i].addr = local_addr
        
        
    def create_media_channel(self, i):
        print("Creating media channel %d." % i)
        media_legs = { li: self.legs[li].make_media_leg(i) for li in range(len(self.legs)) }
        
        return self.switch.make_media_channel(media_legs)
        
        
    def process_offer(self, li, offer):
        for i in range(len(self.media_channels), len(offer.channels)):
            self.media_channels.append(self.create_media_channel(i))
        
        for i in range(len(offer.channels)):
            mc = self.media_channels[i]
            oc = offer.channels[i]
            
            mc.process_offer(li, oc)
        
        self.mangle_session(li, offer)
            
        
    def process_answer(self, li, answer):
        for i in range(len(answer.channels)):
            mc = self.media_channels[i]
            ac = answer.channels[i]

            mc.process_answer(li, ac)
            # TODO: if rejected, remove pending channels!

        self.mangle_session(li, answer)


    def route_call(self, ctx):
        to_uri = ctx["to"].uri
        print("Checking registered SIP contact: %s" % (to_uri,))
        contacts = self.switch.record_manager.lookup_contact_uris(to_uri)
            
        if contacts:
            ctx["to"] = Nameaddr(contacts[0])  # warn if more
            return self.switch.make_outgoing_sip_leg()
        else:
            print("No such registered SIP contact: %s" % (to_uri,))
            raise Exception("Routing failed!")  # TODO: reject!
        

    def dial_action(self, li, action):
        lj = 1 - li
        print("Dialing from leg %d." % li)

        src_ctx = action["ctx"]
        dst_ctx = src_ctx.copy()
        action["ctx"] = dst_ctx

        outgoing_leg = self.route_call(dst_ctx)
        self.add_leg(lj, outgoing_leg)
        
        self.bridge_action(li, action)
        #self.legs[lj].do(action)


    def bridge_action(self, li, action):
        lj = 1 - li
        type = action["type"]
        print("Bridging %s from leg %d." % (type, li))

        if type == "refresh":
            for mc in self.media_channels:
                mc.refresh_context()
            return
        
        offer = action.get("offer")  # OOPS: offer in dial not handled!!!
        if offer:
            self.process_offer(li, offer)

        answer = action.get("answer")
        if answer:
            self.process_answer(li, answer)
        
        self.legs[lj].do(action)
        
        if type == "hangup":
            for mc in self.media_channels:
                mc.finish()

            # TODO: we should wait for full completion before notifying!
            self.switch.finish_call(self)


    def process(self, action, li):  # li is second arg because it is bound
        type = action["type"]

        if type == "dial":
            self.dial_action(li, action)
        else:
            self.bridge_action(li, action)
        
        
    
# The incoming leg must have a context initialized from the INVITE, then
# this leg passed to the routing, which returns the outgoing leg, whose
# context is initialized there. The outgoing leg uses its context to
# reconstruct the dial.
# Es ne kelljen a konstruktor-parametereket a legfelsobb szintig feltolni.


class PlannedCall(Call):
    class CallPlanner(Planner):
        pass
    #    def wait_action(self, expect, timeout=None):
    #        planned_event = yield from self.suspend(expect="action", timeout=timeout)
    #        action = planned_event.event
            
    #        if action["type"] != action_type:
    #            raise Exception("Expected action %s, got %s!" % (action_type, action["type"]))
                
    #        return action
            
            
    def __init__(self):
        super(PlannedCall, self).__init__()
        
        self.planner = self.CallPlanner(metapoll, self.plan)
    

    def process(self, action, li):
        self.planner.resume(PlannedEvent("action", (action, li)))


    # Helper generator
    def bridge_call(self, planner):
        while True:
            tag, event = yield from planner.suspend()
            if tag == "action":
                action, li = event
                self.bridge_action(li, action)
                
                # TODO: this should be more intelligent
                if action["type"] == "hangup":
                    return
    
    
    def plan(self, planner):
        raise NotImplementedError()
