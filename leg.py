from async import WeakMethod, WeakGeneratorMethod, Weak
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


    def finish(self, error=None):
        if any(self.media_legs):
            self.logger.debug("Deleting media legs.")
            for i, ml in enumerate(self.media_legs):
                if ml:
                    ml.delete(WeakMethod(self.media_deleted, i, error))
        else:
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


    def media_deleted(self, li, error):
        self.media_legs[li] = None
        
        if not any(self.media_legs):
            self.logger.debug("Leg is finished.")
            self.report(dict(type="finish", error=error))

    
    #def refresh_media(self):
    #    self.call.refresh_media()
        
        
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
            self.logger.error("Leg plan screwed with %s!" % error)
            
        self.finish(error)
        

    def do(self, action):
        self.resume("action", action)


    def plan(self):
        raise NotImplementedError()


class DialInLeg(Leg):
    def __init__(self, dial_out_leg):
        self.dial_out_leg = dial_out_leg
        
        
    def do(self, action):
        self.dial_out_leg.bridge(action)

        if action["type"] in ("hangup", "reject"):
            self.report(dict(type="finish"))
        
        
    def bridge(self, action):
        self.report(action)
        

class DialOutLeg(Leg):
    def __init__(self):
        Leg.__init__(self)

        dial_in_leg = DialInLeg(Weak(self))
        dial_in_leg.set_oid(self.call.generate_leg_oid())
        self.dial_in_leg = Weak(dial_in_leg)

        self.routing = self.call.make_routing()
        self.routing.set_report(WeakMethod(self.reported))
        self.routing.add_leg(dial_in_leg)
        
        self.legs = None


    def do(self, action):
        self.dial_in_leg.bridge(action)
        
        
    def bridge(self, action):
        self.report(action)
        
        
    def reported(self, action):
        type = action["type"]
        
        if type == "finish":
            self.routing = None
            
            if self.legs:
                self.logger.debug("Dialout routing finished")
            else:
                self.logger.debug("Oops, dialout routing finished without success!")
                self.finish()
                # TODO
        elif type == "anchor":
            self.logger.debug("Yay, dialout anchored.")
            self.legs = action["legs"]

            for i, leg in enumerate(self.legs):
                leg.set_report(WeakMethod(self.forward, i).bind_front())  # FIXME: what to do here?
        else:
            self.logger.debug("Unknown dialout routing event %s!" % type)


    def forward(self, li, action):
        type = action["type"]
        
        if type == "finish":
            self.logger.debug("Bridged leg %d finished." % li)
            self.legs[li] = None
            
            if action.get("error"):
                self.logger.warning("Leg screwed, tearing down others!")
                for leg in self.legs:
                    if leg:
                        leg.do(dict(type="hangup"))  # TODO: abort? It may not be accepted yet
            
            if not any(self.legs):
                self.finish()
        else:
            lj = 1 - li
            self.logger.debug("Forwarding %s from leg %d to %d." % (type, li, lj))
            self.legs[lj].do(action)


def create_uninvited_leg(dialog_manager, invite_params):
    # TODO: real UninvitedLeg class
    leg = Leg(dialog_manager, None, None, None)  # FIXME: this is seriously obsolete!
    leg.dialog.send_request(dict(method="UNINVITE"), invite_params, leg.process)  # strong ref!
    leg.state = leg.DIALING_OUT
