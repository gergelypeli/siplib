from async import Weak, WeakMethod, WeakGeneratorMethod
from planner import Planned
from util import build_oid, Loggable
from format import SipError, Status

class Error(Exception): pass


class CallComponent(Loggable):
    def __init__(self):
        Loggable.__init__(self)
        
        self.call = None
        self.path = None  # To prettify oids
        
    
    def set_call(self, call, path):
        self.call = call
        self.path = path
        
        
    def start(self):
        pass  # Useful for Leg types that do something by themselves


    def stand(self):
        raise NotImplementedError()
        
        
class BareLeg(CallComponent):
    def __init__(self):
        CallComponent.__init__(self)
        
    
    def do(self, action):
        raise NotImplementedError()
        
        
    def report(self, action):
        self.call.ground.forward(self.oid, action)
        
        
    def finished(self):
        self.logger.debug("Leg is finished.")
        self.call.leg_finished(self)
        
        
    def get_media_leg(self, channel_index):
        return None
        
        
    def stand(self):
        self.call.add_leg(self)
        
        return self


class Leg(BareLeg):
    def __init__(self):
        BareLeg.__init__(self)
        
        self.ctx = {}
        self.media_legs = []

    
    def may_finish(self, error=None):
        if any(self.media_legs):
            return
        
        self.finished()


    def make_media_leg(self, channel_index, type, **kwargs):
        media_leg = self.call.make_media_leg(self.oid, channel_index, type, **kwargs)
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


    def get_media_leg(self, ci):
        return self.media_legs[ci] if ci < len(self.media_legs) else None
            

class SlotLeg(Leg):
    def __init__(self, owner, number):
        Leg.__init__(self)
        
        self.owner = owner
        self.number = number
        
        
    def do(self, action):
        self.owner.do_slot(self.number, action)


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
        self.start_plan()


    def plan_finished(self, error):
        if error:
            self.logger.error("Leg plan aborted with: %s!" % error)
        
        # Unconditional cleanup
        self.finish_media(error=error)
        

    def do(self, action):
        self.resume("action", action)


    def plan(self):
        raise NotImplementedError()


# TODO: create session.py!
class SessionState(object):
    def __init__(self):
        self.local_session = None
        self.remote_session = None
        self.pending_local_session = None
        self.pending_remote_session = None
        
        
    def set_local_offer(self, session):
        if not session:
            raise Error("No outgoing offer specified!")
        elif session["is_answer"]:
            raise Error("Outgoing offer is an answer!")
        elif self.pending_local_session:
            raise Error("Outgoing offer already pending!")
        elif self.pending_remote_session:
            raise Error("Incoming offer also pending!")
        else:
            self.pending_local_session = session


    def set_remote_offer(self, session):
        if not session:
            raise Error("No incoming offer specified!")
        elif session["is_answer"]:
            raise Error("Incoming offer is an answer!")
        elif self.pending_local_session:
            raise Error("Outgoing offer also pending!")
        elif self.pending_remote_session:
            raise Error("Incoming offer already pending!")
        else:
            self.pending_remote_session = session
            
            
    def set_local_answer(self, session):
        if not session:
            raise Error("No outgoing answer specified!")
        elif not self.pending_remote_session:
            raise Error("Incoming offer not pending!")
        elif not session["is_answer"]:
            raise Error("Outgoing answer is an offer!")
        elif len(session) == 1:  # rejected
            s = self.pending_remote_session
            self.pending_remote_session = None
            return s
        else:
            self.remote_session = self.pending_remote_session
            self.local_session = session
            self.pending_remote_session = None
            
        return None


    def set_remote_answer(self, session):
        if not session:
            raise Error("No incoming answer specified!")
        elif not self.pending_local_session:
            raise Error("Outgoing offer not pending!")
        elif not session["is_answer"]:
            raise Error("Incoming answer is an offer!")
        elif len(session) == 1:  # rejected
            s = self.pending_local_session
            self.pending_local_session = None
            return s
        else:
            self.local_session = self.pending_local_session
            self.remote_session = session
            self.pending_local_session = None
            
        return None


    def get_local_offer(self):
        if self.pending_local_session:
            return self.pending_local_session
        else:
            raise Error("Outgoing offer not pending!")
        
        
    def get_remote_offer(self):
        if self.pending_remote_session:
            return self.pending_remote_session
        else:
            raise Error("Incoming offer not pending!")
            
            
    def get_local_answer(self):
        if self.pending_local_session:
            raise Error("Outgoing offer is pending!")
        elif self.pending_remote_session:
            raise Error("Incoming offer still pending!")
        elif not self.local_session:
            raise Error("No local answer yet!")
        else:
            return self.local_session


    def get_remote_answer(self):
        if self.pending_local_session:
            raise Error("Outgoing offer still pending!")
        elif self.pending_remote_session:
            raise Error("Incoming offer is pending!")
        elif not self.remote_session:
            raise Error("No remote answer yet!")
        else:
            return self.remote_session


class Routing(BareLeg):
    def __init__(self):
        BareLeg.__init__(self)

        self.leg_count = 0
        self.legs = {}
        self.sent_ringback = False
        self.queued_actions = {}
        self.bridge_count = 0
    
    
    def add_leg(self, li, leg):
        slot_leg = self.call.stand_slot(self, li)
        self.legs[li] = Weak(slot_leg)
        
        self.queued_actions[li] = []
        self.call.link_legs(slot_leg, leg)
        
        return li


    def remove_leg(self, li):
        leg = self.legs.pop(li)
        leg.finished()
        self.may_finish()


    def may_finish(self):
        if not self.legs:
            self.finished()


    def queue(self, li, action):
        self.logger.debug("Queueing %s from leg %s." % (action["type"], li))
        self.queued_actions[li].append(action)


    def reject(self, status):
        self.logger.warning("Rejecting with status %s" % (status,))
        self.report(dict(type="reject", status=status))
            
            
    def ringback(self):
        if not self.sent_ringback:
            self.sent_ringback = True
            self.logger.debug("Sending artificial ringback.")
            self.report(dict(type="ring"))


    def hangup_all_outgoing(self, except_li):
        for li, leg in list(self.legs.items()):
            if li != except_li:
                self.logger.debug("Hanging up leg %s" % li)
                leg.report(dict(type="hangup"))
                self.remove_leg(li)


    def anchor(self, li):
        self.logger.debug("Anchored to leg %d." % li)
        self.hangup_all_outgoing(li)
        self.call.ground.collapse_legs(self.oid, self.legs[li].oid, self.queued_actions[li])
        self.remove_leg(li)
        
            
    def dial(self, type, action):
        if action["type"] != "dial":
            raise Exception("Dial action is not a dial: %s" % action["type"])

        self.logger.debug("Dialing out to: %s" % (type,))
        self.leg_count += 1
        li = self.leg_count  # outgoing legs are numbered from 1
        
        thing = self.call.make_thing(type)
        leg = self.call.stand_thing(thing, self.path + [ li ], None)
        #path = self.path + [ li ]
        #thing.set_call(self.call, path)
        #thing.set_oid(build_oid(self.call.oid, "leg", path))
        #leg = thing.stand()
        thing.start()
        
        self.add_leg(li, leg)
        self.legs[li].report(action)


    def do(self, action):
        type = action["type"]
        self.logger.debug("Got %s from the incoming leg." % (type,))

        if type == "dial":
            raise Exception("Should have handled dial in a subclass!")
        elif type == "hangup":
            self.hangup_all_outgoing(None)
        else:
            raise Exception("Invalid action from incoming leg: %s" % type)


    def do_slot(self, li, action):
        type = action["type"]
        self.logger.debug("Got %s from outgoing leg %d." % (type, li))

        if type == "reject":
            # FIXME: of course don't reject the incoming leg immediately
            self.reject(action["status"])
        elif type == "ring":
            if action.get("session"):
                action["type"] = "session"
                self.queue(li, action)
                
            self.ringback()
        elif type == "session":
            self.queue(li, action)
        elif type == "accept":
            self.queue(li, action)
            self.anchor(li)
            #self.hangup_all_outgoing(li)
        elif type == "hangup":
            # Oops, we anchored this leg because it accepted, but now hangs up
            self.report(action)
        else:
            raise Exception("Invalid action from outgoing leg %d: %s" % (li, type))


class SimpleRouting(Routing):
    def route(self, action):
        raise NotImplementedError()


    def do(self, action):
        if action["type"] == "dial":
            try:
                self.route(action)
                
                if not self.legs:
                    raise Exception("Simple routing finished without legs!")
            except SipError as e:
                self.logger.error("Simple routing SIP error: %s" % (e.status,))
                self.reject(e.status)
            except Exception as e:
                self.logger.error("Simple routing internal error: %s" % (e,), exc_info=True)
                self.reject(Status(500))
        else:
            Routing.do(self, action)


class PlannedRouting(Planned, Routing):
    def __init__(self, metapoll):
        Planned.__init__(self,
                metapoll,
                WeakGeneratorMethod(self.plan),
                finish_handler=WeakMethod(self.plan_finished)
        )
        Routing.__init__(self)
        
        
    def wait_action(self, leg_index=None, action_type=None, timeout=None):
        tag, event = yield from self.suspend(expect="action", timeout=timeout)
        li, action = event

        if leg_index is not None and li != leg_index:
            raise Exception("Expected action from %s, got from %s!" % (leg_index, li))

        if action_type and action["type"] != action_type:
            raise Exception("Expected action %s, got %s!" % (action_type, action["type"]))
        
        return li, action


    def may_finish(self):
        if not self.generator:
            Routing.may_finish(self)
            

    def plan_finished(self, exception):
        for tag, event in self.event_queue:
            if tag == "action":
                li, action = event
                
                if li == 0:
                    Routing.do(self, action)
                else:
                    Routing.do_slot(self, li, action)
                
        self.event_queue = None
        status = None
        
        try:
            if exception:
                self.logger.debug("Routing plan finished with: %s" % exception)
                raise exception
            
            #if len(self.legs) < 2:
            #    raise Exception("Routing plan completed without creating outgoing legs!")
        except SipError as e:
            self.logger.error("Routing plan aborted with SIP error: %s" % e)
            status = e.status
        except Exception as e:
            self.logger.error("Routing plan aborted with exception: %s" % e)
            status = Status(500)
            
        if status:
            # TODO: must handle double events for this to work well!
            self.hangup_all_outgoing(None)
            self.reject(status)
            
        self.may_finish()
        
        
    def do(self, action):
        self.logger.debug("Planned routing processing a %s" % action["type"])
        
        if action["type"] == "dial":
            self.start_plan(action)
        elif self.generator:
            self.resume("action", (0, action))
        else:
            Routing.do(self, action)


    def do_slot(self, li, action):
        self.logger.debug("Planned routing processing a %s" % action["type"])
        
        if self.generator:
            self.resume("action", (li, action))
        else:
            Routing.do_slot(self, li, action)
        

    def plan(self, action):
        raise NotImplementedError()


class Bridge(CallComponent):
    def __init__(self):
        CallComponent.__init__(self)

        self.incoming_leg = None
        self.outgoing_leg = None
        

    #def make_incoming_leg(self):
    #    # Strong reference to self to keep us alive
    #    incoming_leg = BridgeLeg(self, False)
    #    self.incoming_leg = Weak(incoming_leg)
    #    return incoming_leg
        

    #def make_outgoing_leg(self):
    #    outgoing_leg = BridgeLeg(Weak(self), True)
    #    self.outgoing_leg = Weak(outgoing_leg)
    #    return outgoing_leg
        
        
    def stand(self):
        incoming_leg = self.call.stand_slot(self, 0)
        self.incoming_leg = Weak(incoming_leg)

        outgoing_leg = self.call.stand_slot(self, 1)
        self.outgoing_leg = Weak(outgoing_leg)
        
        # TODO: copypaste from Call.start
        routing = self.call.make_thing("routing")
        self.call.stand_thing(routing, self.path, "routing")
        #routing.set_call(self.call, self.path)
        #routing.set_oid(build_oid(self.oid, "routing"))  # TODO: rerouting?
        #routing.stand()
    
        self.call.link_legs(outgoing_leg, routing)
        routing.start()
        
        return incoming_leg
        

    def may_finish(self):  # TODO: this is probably screwed up now
        if self.outgoing_leg:
            self.logger.debug("Releasing outgoing leg.")
            self.outgoing_leg.finish_media()
            self.outgoing_leg = None

        if self.incoming_leg:
            self.logger.debug("Releasing incoming leg.")
            self.incoming_leg.finish_media()
            self.incoming_leg = None
            
        self.logger.debug("Bridge finished.")
            

    def do_slot(self, li, action):
        type = action["type"]
        
        if li == 0:
            leg = self.outgoing_leg
            direction = "forward"
        else:
            leg = self.incoming_leg
            direction = "backward"
        
        self.logger.debug("Bridging %s %s" % (type, direction))
        leg.report(action)

        if type in ("hangup", "reject"):
            self.may_finish()

    
class RecordingBridge(Bridge):
    def hack_media(self, li, answer):
            
        old = len(self.incoming_leg.media_legs)
        new = len(answer["channels"])
        
        for i in range(old, new):
            this = self.incoming_leg.make_media_leg(i, "pass")
            that = self.outgoing_leg.make_media_leg(i, "pass")
            
            this.pair(Weak(that))
            that.pair(Weak(this))
            
            format = ("L16", 8000, 1, None)
            this.refresh(dict(filename="recorded.wav", format=format, record=True))
            that.refresh({})
            
        if len(answer["channels"]) >= 1:
            c = answer["channels"][0]

            if not c["send"]:
                self.logger.debug("Hah, the %s put us on hold!" % ("callee" if li == 0 else "caller"))

            if not c["recv"]:
                self.logger.debug("Hah, the %s put us on hold!" % ("caller" if li == 0 else "callee"))


    def do_slot(self, li, action):
        session = action.get("session")
        
        if session and session["is_answer"] and len(session) > 1:
            self.hack_media(li, session)
        
        Bridge.do_slot(self, li, action)
