from async import WeakMethod


class Session(object):
    NONE = "NONE"
    AGREED = "AGREED"
    OFFERING = "OFFERING"
    ANSWERING = "ANSWERING"

    def __init__(self):
        self.state = self.NONE
        self.remote = None
        self.local = None
        self.dirty = False


    def recv(self, sdp):
        if self.state in (self.NONE, self.AGREED):
            self.state = self.OFFERING
            self.remote = sdp
            return sdp
        elif self.state == self.OFFERING:
            print("Oops, ignoring duplicate offer!")
            return None
        elif self.state == self.ANSWERING:
            self.state = self.AGREED
            self.remote = sdp
            return sdp


    def send(self, sdp):
        if self.state in (self.NONE, self.AGREED):
            self.state = self.ANSWERING
            self.local = sdp
            self.dirty = True
        elif self.state == self.OFFERING:
            self.state = self.AGREED
            self.local = sdp
            self.dirty = True
        elif self.state == self.ANSWERING:
            print("Oops, ignoring duplicate offer!")


    def check(self):
        if self.dirty:
            self.dirty = False
            return self.local
        else:
            return None
                    

class DirectMedia(object):
    def __init__(self):
        self.sessions = { 0: Session(), 1: Session() }
        
        
    def recv(self, i, sdp):
        incoming = self.sessions[i]
        outgoing = self.sessions[1 - i]
        
        dirty_sdp = incoming.recv(sdp)
        if dirty_sdp:
            outgoing.send(sdp)
            
    
    def check(self, i):
        outgoing = self.sessions[i]
        return outgoing.check()
        
        
class Call(object):
    def __init__(self, route):
        self.route = route
        self.media = DirectMedia()
        self.legs = {}
        
        
    def add_leg(self, i, leg):
        self.legs[i] = leg
        leg.set_report(WeakMethod(self.process, i))
        
        
    def process(self, action, i):
        j = 1 - i

        type = action["type"]
        print("Got %s from leg %d." % (type, i))
        
        if type == "dial":
            outgoing_leg = self.route(action["ctx"])
            if (outgoing_leg):
                self.add_leg(1, outgoing_leg)
            else:
                print("Routing failed!")  # TODO: reject!
        
        sdp = action.get("sdp")
        if sdp:
            self.media.recv(i, sdp)
        action["sdp"] = self.media.check(j)
        
        self.legs[j].do(action)
        
    
# The incoming leg must have a context initialized from the INVITE, then
# this leg passed to the routing, which returns the outgoing leg, whose
# context is initialized there. The outgoing leg uses its context to
# reconstruct the dial.
# Es ne kelljen a konstruktor-parametereket a legfelsobb szintig feltolni.
