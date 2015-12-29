from async import WeakMethod, WeakGeneratorMethod, Weak
from planner import Planned
from util import build_oid, Loggable
from call import Routable


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


    def make_media_leg(self, channel_index, type):
        media_leg = self.call.make_media_leg(channel_index, type)
        media_leg.set_oid(build_oid(self.oid, "media", channel_index))
        
        if channel_index < len(self.media_legs):
            self.media_legs[channel_index] = media_leg
        elif channel_index == len(self.media_legs):
            self.media_legs.append(media_leg)
        else:
            raise Exception("Invalid media leg index!")
            
        return media_leg


    def finish_media(self, li=None, error=None):
        if li is not None:
            # Completed
            self.media_legs[li] = None
        else:
            # Initiated
            for i, ml in enumerate(self.media_legs):
                self.logger.debug("Deleting media leg %s." % i)
                ml.delete(WeakMethod(self.finish_media, i, error))  # lame error handling
        
        self.may_finish(error)

    
    def get_further_legs(self):  # for the sake of internal legs
        return []
        

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


class DialInLeg(Leg):
    def __init__(self, dial_out_leg):
        Leg.__init__(self)
        
        self.dial_out_leg = dial_out_leg
        
        
    def do(self, action):
        self.dial_out_leg.bridge(action)

        if action["type"] in ("hangup", "reject"):
            self.may_finish()
        
        
    def bridge(self, action):
        self.report(action)

        if action["type"] in ("hangup",):
            self.may_finish()
        

class DialOutLeg(Leg, Routable):
    def __init__(self):
        Leg.__init__(self)
        Routable.__init__(self)
        
        self.is_anchored = False


    def make_routing(self):
        return self.call.make_routing()


    def generate_leg_oid(self):
        return self.call.generate_leg_oid()


    def set_call(self, call):
        Leg.set_call(self, call)
        # Now we can start the Routable stuff

        dial_in_leg = DialInLeg(Weak(self))
        self.dial_in_leg = Weak(dial_in_leg)
        self.start_routing(dial_in_leg)
        self.logger.debug("Dialed out to leg %s." % dial_in_leg.oid)
        

    def do(self, action):
        self.dial_in_leg.bridge(action)
        
        if action["type"] in ("hangup",):
            self.may_finish()
        
        
    def bridge(self, action):
        self.report(action)

        if action["type"] in ("hangup", "reject"):
            self.may_finish()

    
    def may_finish(self, error=None):
        if self.routing:
            return
            
        if any(self.legs):
            return
            
        Leg.may_finish(self, error)
        
        
    def reported(self, action):
        # Used before the child routing is anchored
        Routable.reported(self, action)
        
        type = action["type"]
        
        if type == "finish":
            # Clean up on routing failure
            # If we're anchored, then the legs may already be taken by the parent routing,
            # so checking in may_finish would actually finish.
            
            if not self.is_anchored:
                self.may_finish()
        elif type == "anchor":
            self.is_anchored = True


    def forward(self, li, action):
        # Used since the child routing is anchored until the parent routing is anchored
        Routable.forward(self, li, action)
        
        type = action["type"]
        
        if type == "finish":
            # Clean up after all legs finished (no media here)
            self.may_finish()


    def get_further_legs(self):
        # Don't hold the legs from the child routing once the parent routing took them
        legs = self.legs
        self.legs = []
        
        return legs


def create_uninvited_leg(dialog_manager, invite_params):
    # TODO: real UninvitedLeg class
    leg = Leg(dialog_manager, None, None, None)  # FIXME: this is seriously obsolete!
    leg.dialog.send_request(dict(method="UNINVITE"), invite_params, leg.process)  # strong ref!
    leg.state = leg.DIALING_OUT
