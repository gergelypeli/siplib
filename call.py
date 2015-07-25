from async import WeakMethod


def extract_formats(c):
    formats = {}
    
    for r in c.formats:
        formats[r.payload_type] = "%s/%s" % (r.encoding, r.clock)
        
    return formats
        

class ProxiedMediaLeg(object):
    def __init__(self, local_addr):
        self.local_addr = None
        self.remote_addr = None
        self.send_formats = None
        self.recv_formats = None
        

class ProxiedMediaChannel(object):
    def __init__(self, legs):
        self.context_id = None
        self.legs = legs
        self.pending_addr = None
        self.pending_formats = None


class Call(object):
    def __init__(self, mgc, route):
        self.mgc = mgc
        self.route = route
        self.legs = {}
        self.media_channels = []
        
        
    def add_leg(self, li, leg):
        self.legs[li] = leg
        leg.set_report(WeakMethod(self.process, li))
    
    
    def mangle_session(self, li, sdp):
        lj = 1 - li
        
        for i in range(len(sdp.channels)):
            local_addr = self.media_channels[i].legs[lj].local_addr
            sdp.channels[i].addr = local_addr
        
        
    def process_mgw_message(self, params, context_id):
        print("MGW %s message %s" % (context_id, params))
        
        
    def create_media_channel():
        addr0 = ("localhost", 30000)
        addr1 = ("localhost", 30001)
            
        legs = [ ProxiedMediaLeg(addr1), ProxiedMediaLeg(addr2) ]
        return ProxiedMediaChannel(legs)
        
        
    def process_media_offer(mc, oc):
        mc.pending_addr = oc.addr
        mc.pending_formats = extract_formats(oc)


    def process_media_answer(mc, ac):
        offering_leg = mc.legs[lj]
        answering_leg = mc.legs[li]

        answer_addr = ac.addr
        answer_formats = extract_formats(ac)
        
        offer_addr = mc.pending_addr
        offer_formats = mc.pending_formats

        mc.pending_addr = None
        mc.pending_formats = None
        
        answering_leg.remote_addr = answer_addr
        answering_leg.send_formats = answer_formats
        answering_leg.recv_formats = offer_formats
        
        offering_leg.remote_addr = offer_addr
        offering_leg.send_formats = offer_formats
        offering_leg.recv_formats = answer_formats

        if not mc.context_id:
            mc.context_id = generate_context_id()

        def leg_params(li):
            leg = mc.legs[li]
            
            return {
                'type': 'rtp',
                'local_addr': leg.local_addr,
                'remote_addr': leg.remote_addr,
                'send_formats': leg.send_formats,
                'recv_formats': leg.recv_formats
            }
        
        params = {
            'type': 'proxy',
            'legs': {
                '0': leg_params(0),
                '1': leg_params(1)
            }
        }
        
        callback = WeakMethod(self.process_mgw_message, mc.context_id)
        self.mgc.create_context(mc.context_id, params, callback=callback)
        
        
    def process_offer(self, li, offer):
        lj = 1 - li
        
        for i in range(len(self.media_channels), len(offer.channels)):
            self.media_channels[i] = create_media_channel()
        
        for i in range(len(offer.channels)):
            mc = self.media_channels[i]
            oc = offer.channels[i]
            
            self.process_media_offer(mc, oc)
        
        self.mangle_session(li, offer)
            
        
    def process_answer(self, li, answer):
        lj = 1 - li
        
        for i in range(len(answer.channels)):
            mc = self.media_channels[i]
            ac = answer.channels[i]

            self.process_media_answer(mc, ac)

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
        
        self.legs[lj].do(action)
        
    
# The incoming leg must have a context initialized from the INVITE, then
# this leg passed to the routing, which returns the outgoing leg, whose
# context is initialized there. The outgoing leg uses its context to
# reconstruct the dial.
# Es ne kelljen a konstruktor-parametereket a legfelsobb szintig feltolni.
