from async import WeakMethod


class Call(object):
    def __init__(self, mgc, route, finish):
        self.mgc = mgc
        self.route = route
        self.finish = finish
        self.legs = {}
        self.media_channels = []
        
        
    def add_leg(self, li, leg):
        self.legs[li] = leg
        leg.set_report(WeakMethod(self.process, li))
    
    
    def mangle_session(self, li, sdp):
        lj = 1 - li
        
        for i in range(len(sdp.channels)):
            local_addr = self.media_channels[i].legs[lj].local_addr
            print("Mangling leg %d channel %d with %s" % (lj, i, local_addr))
            sdp.channels[i].addr = local_addr
        
        
    def create_media_channel(self, i):
        media_legs = [ self.legs[li].make_media_leg(i) for li in range(len(self.legs)) ]
        
        return self.mgc.make_media_channel(media_legs)
        
        
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
        
        
    def process(self, action, li):
        lj = 1 - li

        type = action["type"]
        print("Got %s from leg %d." % (type, li))
        
        if type == "dial":
            src_ctx = action["ctx"]
            dst_ctx = src_ctx.copy()
            action["ctx"] = dst_ctx
            
            outgoing_leg = self.route(dst_ctx)
            if (outgoing_leg):
                self.add_leg(1, outgoing_leg)
            else:
                print("Routing failed!")  # TODO: reject!
        
        offer = action.get("offer")
        if offer:
            self.process_offer(li, offer)

        answer = action.get("answer")
        if answer:
            self.process_answer(li, answer)
        
        self.legs[lj].do(action)
        
        if type == "hangup":
            for mc in self.media_channels:
                mc.finish()

            self.finish(self)
        
    
# The incoming leg must have a context initialized from the INVITE, then
# this leg passed to the routing, which returns the outgoing leg, whose
# context is initialized there. The outgoing leg uses its context to
# reconstruct the dial.
# Es ne kelljen a konstruktor-parametereket a legfelsobb szintig feltolni.
