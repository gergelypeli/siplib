from async import WeakMethod


def extract_formats(c):
    formats = {}
    
    for r in c.formats:
        formats[r.payload_type] = "%s/%s" % (r.encoding, r.clock)
        
    return formats
        

class ProxiedMediaLeg(object):
    def __init__(self, local_addr):
        self.local_addr = local_addr
        self.remote_addr = None
        self.send_formats = None
        self.recv_formats = None
        

class ProxiedMediaChannel(object):
    mgc = None

    @classmethod
    def set_mgc(cls, mgc):
        cls.mgc = mgc
        
    
    def __init__(self):
        sid, addrs = self.mgc.allocate_media(2)  # TODO: deallocate, too

        self.is_created = False
        self.context_sid = sid
        self.legs = [ ProxiedMediaLeg(addrs[0]), ProxiedMediaLeg(addrs[1]) ]
        self.pending_addr = None
        self.pending_formats = None

        
    def process_mgw_request(self, sid, seq, params, target):
        print("Huh, MGW %s sent a %s message!" % (sid, target))
        
        
    def process_mgw_response(self, sid, seq, params, purpose):
        if params == "ok":
            print("Huh, MGW %s is OK for %s!" % (sid, purpose))
        else:
            print("Oops, MGW %s error for %s!" % (sid, purpose))
        

    def process_offer(self, li, oc):
        self.pending_addr = oc.addr
        self.pending_formats = extract_formats(oc)


    def process_answer(self, li, ac):
        lj = 1 - li
        
        offering_leg = self.legs[lj]
        answering_leg = self.legs[li]

        answer_addr = ac.addr
        answer_formats = extract_formats(ac)
        
        offer_addr = self.pending_addr
        offer_formats = self.pending_formats

        self.pending_addr = None
        self.pending_formats = None
        
        answering_leg.remote_addr = answer_addr
        answering_leg.send_formats = answer_formats
        answering_leg.recv_formats = offer_formats
        
        offering_leg.remote_addr = offer_addr
        offering_leg.send_formats = offer_formats
        offering_leg.recv_formats = answer_formats

        def leg_params(i):
            leg = self.legs[i]
            
            return {
                'type': 'net',
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
        
        if not self.is_created:
            request_handler = WeakMethod(self.process_mgw_request)
            response_handler = WeakMethod(self.process_mgw_response, "cctx")
            self.mgc.create_context(self.context_sid, params, response_handler=response_handler, request_handler=request_handler)
            self.is_created = True
        else:
            response_handler = WeakMethod(self.process_mgw_response, "mctx")
            self.mgc.modify_context(self.context_sid, params, response_handler=response_handler)


    def finish(self):
        if self.is_created:
            self.mgc.delete_context(self.context_sid)


class Call(object):
    def __init__(self, route, finish):
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
        
        
    def create_media_channel(self):
        return ProxiedMediaChannel()
        
        
    def process_offer(self, li, offer):
        for i in range(len(self.media_channels), len(offer.channels)):
            self.media_channels.append(self.create_media_channel())
        
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
