from async import WeakMethod, WeakGeneratorMethod
from planner import Planned
from util import build_oid, Loggable


class Error(Exception): pass


class Leg(Loggable):
    def __init__(self):
        Loggable.__init__(self)
        
        self.call = None
        self.report = None
        self.ctx = {}
        self.media_legs = []
    
    
    def set_report(self, report):
        self.report = report
        
        
    def set_call(self, call):
        self.call = call


    def start(self):
        pass  # Useful for Leg types that do something by themselves

        
    def do(self, action):
        raise NotImplementedError()


    def may_finish(self, error=None):
        if any(self.media_legs):
            return
        
        self.logger.debug("Leg is finished.")
        self.report(dict(type="finish", error=error))


    def refresh_media(self):
        self.call.refresh_media()


    def make_media_leg(self, channel_index, type):
        media_leg = self.call.make_media_leg(channel_index, type)
        media_leg.set_oid(build_oid(self.oid, "channel", channel_index))
        
        if channel_index < len(self.media_legs):
            self.media_legs[channel_index] = media_leg
        elif channel_index == len(self.media_legs):
            self.media_legs.append(media_leg)
        else:
            raise Exception("Invalid media leg index!")
        
        return media_leg
        

    def media_finished(self, li, error):
        # Completed
        self.media_legs[li] = None
        
        self.may_finish(error)


    def finish_media(self, error=None):
        # Initiated
        for i, ml in enumerate(self.media_legs):
            self.logger.debug("Deleting media leg %s." % i)
            ml.delete(WeakMethod(self.media_finished, i, error))  # lame error handling
        
        self.may_finish(error)


    def anchor(self):
        self.report(dict(type="anchor"))
        

    def flatten(self, legs):
        legs.append(self)
        

class Session(object):
    def __init__(self):
        self.local_sdp = None
        self.remote_sdp = None
        self.pending_local_sdp = None
        self.pending_remote_sdp = None
        
        
    def set_local_offer(self, sdp):
        if sdp is None:
            raise Error("No SDP specified!")
        elif self.pending_local_sdp:
            raise Error("Outgoing offer already pending!")
        elif self.pending_remote_sdp:
            raise Error("Incoming offer also pending!")
        else:
            self.pending_local_sdp = sdp


    def set_remote_offer(self, sdp):
        if sdp is None:
            raise Error("No SDP specified!")
        elif self.pending_local_sdp:
            raise Error("Outgoing offer also pending!")
        elif self.pending_remote_sdp:
            raise Error("Incoming offer already pending!")
        else:
            self.pending_remote_sdp = sdp
            
            
    def set_local_answer(self, sdp):
        if sdp is None:
            raise Error("No SDP specified!")
        elif not self.pending_remote_sdp:
            raise Error("Incoming offer not pending!")
        else:
            # empty is rejection
            if not sdp.is_empty():
                self.remote_sdp = self.pending_remote_sdp
                self.local_sdp = sdp
                
            self.pending_remote_sdp = None


    def set_remote_answer(self, sdp):
        if sdp is None:
            raise Error("No SDP specified!")
        elif not self.pending_local_sdp:
            raise Error("Outgoing offer not pending!")
        else:
            # empty is rejection
            if not sdp.is_empty():
                self.local_sdp = self.pending_local_sdp
                self.remote_sdp = sdp
                
            self.pending_local_sdp = None


    def get_local_offer(self):
        if self.pending_local_sdp:
            return self.pending_local_sdp
        else:
            raise Error("Outgoing offer not pending!")
        
        
    def get_remote_offer(self):
        if self.pending_remote_sdp:
            return self.pending_remote_sdp
        else:
            raise Error("Incoming offer not pending!")
            
            
    def get_local_answer(self):
        if self.pending_local_sdp:
            raise Error("Outgoing offer is pending!")
        elif self.pending_remote_sdp:
            raise Error("Incoming offer still pending!")
        elif not self.local_sdp:
            raise Error("No outgoing answer yet!")
        else:
            return self.local_sdp


    def get_remote_answer(self):
        if self.pending_local_sdp:
            raise Error("Outgoing offer still pending!")
        elif self.pending_remote_sdp:
            raise Error("Incoming offer is pending!")
        elif not self.remote_sdp:
            raise Error("No incoming answer yet!")
        else:
            return self.remote_sdp


class PlannedLeg(Planned, Leg):
    def __init__(self, metapoll):
        Planned.__init__(self,
            metapoll,
            WeakGeneratorMethod(self.plan),
            finish_handler=WeakMethod(self.plan_finished)
        )
        Leg.__init__(self)
        
        
    def wait_action(self, action_type=None, timeout=None):
        tag, event = yield from self.suspend(expect="action", timeout=timeout)
        action = event
        
        if action_type and action["type"] != action_type:
            raise Exception("Expected action %s, got %s!" % (action_type, action["type"]))
            
        return action
            
        
    def start(self):
        Planned.start(self)


    def plan_finished(self, error):
        if error:
            self.logger.error("Leg plan aborted with: %s!" % error)
        
        # Unconditional cleanup
        self.finish_media(error=error)
        

    def do(self, action):
        self.resume("action", action)


    def plan(self):
        raise NotImplementedError()


class BridgeLeg(Leg):
    def __init__(self, bridge, bli):
        Leg.__init__(self)
        
        self.bridge = bridge
        self.bli = bli
        
        
    def do(self, action):
        self.bridge.bridge(self.bli, action)

        
    def start(self):
        self.bridge.start()


    def flatten(self, legs):
        Leg.flatten(self, legs)
        self.bridge.flatten(legs)
