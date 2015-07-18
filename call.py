from async import WeakMethod


class Call(object):
    def __init__(self, route):
        self.route = route
        self.legs = {}
        
        
    def add_leg(self, i, leg):
        self.legs[i] = leg
        leg.set_report(WeakMethod(self.process, i))
        
        
    def process(self, action, i):
        j = 1 - i

        type = action["type"]
        print("Got %s from leg %d." % (type, i))
        
        if type == "dial":
            src_ctx = action["ctx"]
            dst_ctx = src_ctx.copy()
            action["ctx"] = dst_ctx
            
            outgoing_leg = self.route(dst_ctx)
            if (outgoing_leg):
                self.add_leg(1, outgoing_leg)
            else:
                print("Routing failed!")  # TODO: reject!
        
        self.legs[j].do(action)
        
    
# The incoming leg must have a context initialized from the INVITE, then
# this leg passed to the routing, which returns the outgoing leg, whose
# context is initialized there. The outgoing leg uses its context to
# reconstruct the dial.
# Es ne kelljen a konstruktor-parametereket a legfelsobb szintig feltolni.
