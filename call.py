from async import WeakMethod, Weak, WeakGeneratorMethod
from format import Status, SipError
from mgc import MediaContext
from planner import Planned
from util import build_oid, Loggable
from leg import BridgeLeg


class Routing(Loggable):
    def __init__(self, call):
        Loggable.__init__(self)

        # The report handler is for the finish and anchor events.
        # Both may be forwarded by an owning internal leg,
        # or be processed by the owning Call.
        
        self.call = call
        self.report = None
        self.leg_count = 0
        self.legs = {}
        self.sent_ringback = False
        self.anchored_leg_index = None  # set locally
        self.anchorable_leg_indexes = set()
        self.queued_actions = {}
    
    
    def set_report(self, report):
        self.report = report


    def add_leg(self, leg):
        li = self.leg_count
        self.leg_count += 1

        self.legs[li] = leg
        self.queued_actions[li] = []
        
        leg.set_report(WeakMethod(self.process, li).bind_front())
        
        if not leg.call:
            raise Exception("Why you ain't have no call?")
        #leg.set_call(self.call)
        
        leg.start()


    def remove_leg(self, li):
        self.legs.pop(li)
        self.may_finish()
            

    def may_finish(self):
        if not self.legs:
            self.report(dict(type="finish"))


    def queue(self, li, action):
        self.logger.debug("Queueing %s from leg %s." % (action["type"], li))
        self.queued_actions[li].append(action)


    def reject(self, status):
        self.logger.warning("Rejecting with status %s" % (status,))
        self.legs[0].do(dict(type="reject", status=status))
            
            
    def ringback(self):
        if not self.sent_ringback:
            self.sent_ringback = True
            self.logger.debug("Sending artificial ringback.")
            self.legs[0].do(dict(type="ring"))


    def hangup_all_outgoing(self, except_li):
        for li, leg in list(self.legs.items()):
            self.logger.debug("Hanging up leg %s" % li)
            
            if li not in (0, except_li):
                leg.do(dict(type="hangup"))


    def establish_anchor(self):
        li = self.anchored_leg_index
        self.logger.debug("Anchored to leg %d." % li)

        # An occassional anchoring may wipe these out
        incoming_leg = self.legs[0]
        queued_actions = self.queued_actions[li]
        
        self.report(dict(type="anchor"))

        for action in queued_actions:
            self.logger.debug("Unqueueing %s from leg %s." % (action["type"], li))
            incoming_leg.do(action)
            
        self.queued_actions = None  # just to be sure not to use it again
        

    def set_leg_anchorable(self, li):
        if li in self.anchorable_leg_indexes:
            return
            
        self.logger.debug("Marking leg %s as anchorable." % li)
        self.anchorable_leg_indexes.add(li)
        
        if self.anchored_leg_index == li:
            self.establish_anchor()

        
    def anchor(self, li):
        if self.anchored_leg_index == li:
            return
            
        if self.anchored_leg_index is not None:
            raise Exception("Routing already anchored!")

        self.logger.debug("Choosing leg %s for anchoring." % li)
        self.anchored_leg_index = li
        
        if li in self.anchorable_leg_indexes:
            self.establish_anchor()
            
            
    def dial(self, type, action):
        if action["type"] != "dial":
            raise Exception("Dial action is not a dial: %s" % action["type"])

        self.logger.debug("Dialing out to: %s" % (type,))
        leg = self.call.make_leg(type)
        self.add_leg(leg)
        leg.do(action)


    def process(self, li, action):
        type = action["type"]
        self.logger.debug("Got %s from leg %d." % (type, li))

        if type == "finish":
            self.remove_leg(li)
        elif li == 0:
            if type == "dial":
                raise Exception("Should have handled dial in a subclass!")
            elif type == "hangup":
                self.hangup_all_outgoing(None)
            else:
                raise Exception("Invalid action from incoming leg: %s" % type)
        else:
            if type == "anchor":
                self.set_leg_anchorable(li)
                return
                
            if self.anchored_leg_index is not None:
                # Already anchored locally
                
                if li != self.anchored_leg_index:
                    self.logger.debug("Ignoring action %s from unanchored leg %d!" % (action["type"], li))
                    return
                elif li in self.anchorable_leg_indexes:
                    # Fully anchored, forward
                    self.legs[0].do(action)
                    return
                    
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
                self.set_leg_anchorable(li)  # we're doing a favor here
                self.anchor(li)              # but this is our decision
                self.hangup_all_outgoing(li)
            elif type == "hangup":
                # Oops, we anchored this leg because it accepted, but now hangs up
                self.legs[0].do(action)
            else:
                raise Exception("Invalid action from outgoing leg: %s" % type)


    def flatten(self, legs):
        li = self.anchored_leg_index
        
        if li is None or li not in self.anchorable_leg_indexes:
            raise Exception("Flatten for nonanchored routing!")
        
        legs.append(self.legs[0])
        self.legs[li].flatten(legs)
        self.remove_leg(li)
        self.remove_leg(0)
        

class SimpleRouting(Routing):
    def route(self, action):
        raise NotImplementedError()


    def process(self, li, action):
        if action["type"] == "dial":
            try:
                self.route(action)
                
                if not self.legs:
                    raise Exception("Simple routing finished without legs!")
            except SipError as e:
                self.logger.error("Simple routing SIP error: %s" % (e.status,))
                self.reject(e.status)
            except Exception as e:
                self.logger.error("Simple routing internal error: %s" % (e,))
                self.reject(Status(500))
        else:
            Routing.process(self, li, action)


class PlannedRouting(Planned, Routing):
    def __init__(self, call):
        Planned.__init__(self,
                call.switch.metapoll,
                WeakGeneratorMethod(self.plan),
                finish_handler=WeakMethod(self.plan_finished)
        )
        Routing.__init__(self, call)
        
        
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
                Routing.process(self, li, action)
                
        self.event_queue = None
        status = None
        
        try:
            if exception:
                self.logger.debug("Routing plan finished with: %s" % exception)
                raise exception
            
            if len(self.legs) < 2:
                raise Exception("Routing plan completed without creating outgoing legs!")
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
        
        
    def process(self, li, action):
        self.logger.debug("Planned routing processing a %s" % action["type"])
        
        if action["type"] == "dial":
            self.start(action)
        elif self.generator:
            self.resume("action", (li, action))
        else:
            Routing.process(self, li, action)
        

    def plan(self, action):
        raise NotImplementedError()


class Routable(Loggable):
    def __init__(self):
        Loggable.__init__(self)
        
        self.routing = None


    def make_routing(self):
        raise NotImplementedError()


    def pimp_leg(self, leg):
        raise NotImplementedError()


    def start_routing(self, incoming_leg):
        incoming_leg = self.pimp_leg(incoming_leg)
        
        self.routing = self.make_routing()
        self.routing.set_oid(build_oid(self.oid, "routing"))
        self.routing.set_report(WeakMethod(self.reported))
        self.routing.add_leg(incoming_leg)


    def reported(self, action):
        type = action["type"]
        
        if type == "finish":
            self.routing = None
            self.logger.debug("Routing finished.")
        elif type == "anchor":
            pass
        else:
            self.logger.debug("Unknown routing event %s!" % type)


class Bridge(Routable):
    def __init__(self, call):
        Routable.__init__(self)
        
        self.call = call
        self.incoming_leg = None
        self.outgoing_leg = None
        

    def make_incoming_leg(self):
        # Strong reference to self to keep us alive
        incoming_leg = BridgeLeg(self, 0)
        self.incoming_leg = Weak(incoming_leg)
        return incoming_leg
        
        
    def make_outgoing_leg(self):
        # Weak reference to self to avoid reference cycle in Bridge->Routing->BridgeLeg
        outgoing_leg = BridgeLeg(Weak(self), 1)
        self.outgoing_leg = Weak(outgoing_leg)
        return outgoing_leg
        

    def make_routing(self):
        return self.call.make_routing()


    def pimp_leg(self, leg):
        return self.call.pimp_leg(leg)


    def start(self):
        # Triggered by the incoming leg's start
        if self.outgoing_leg:
            return
            
        outgoing_leg = self.make_outgoing_leg()
        self.logger.debug("Bridging legs %s and %s." % (self.incoming_leg.oid, self.outgoing_leg.oid))
        self.start_routing(outgoing_leg)


    def may_finish(self):
        if self.outgoing_leg:
            self.logger.debug("Releasing outgoing leg.")
            self.outgoing_leg.finish_media()
            self.outgoing_leg = None
            
        if self.routing:
            return

        if self.incoming_leg:
            self.logger.debug("Releasing incoming leg.")
            self.incoming_leg.finish_media()
            self.incoming_leg = None


    def bridge(self, li, action):
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

    
    def reported(self, action):
        # Used before the child routing is anchored
        Routable.reported(self, action)
        
        type = action["type"]
        
        if type == "finish":
            # self.routing is surely None here
            if not self.outgoing_leg:
                # Seems like we had a failed routing here
                self.logger.debug("Routing finished and no outgoing leg, finishing.")
                self.may_finish()
        elif type == "anchor":
            self.incoming_leg.anchor()


    def flatten(self, legs):
        self.routing.flatten(legs)            


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


    def bridge(self, li, action):
        session = action.get("session")
        
        if session and session["is_answer"] and len(session) > 1:
            self.hack_media(li, session)
        
        Bridge.bridge(self, li, action)


class Call(Routable):
    def __init__(self, switch):
        Routable.__init__(self)

        self.switch = switch
        self.legs = []
        self.media_channels = []
        self.leg_count = 0
        self.bridge_count = 0


    def generate_leg_oid(self):
        leg_oid = build_oid(self.oid, "leg", self.leg_count)
        self.leg_count += 1
        
        return leg_oid


    def generate_bridge_oid(self):
        bridge_oid = build_oid(self.oid, "bridge", self.bridge_count)
        self.bridge_count += 1
        
        return bridge_oid


    def make_bridge(self, bridge_class):
        bridge = bridge_class(Weak(self))
        bridge.set_oid(self.generate_bridge_oid())
        return bridge
        
        
    def pimp_leg(self, leg):
        leg.set_oid(self.generate_leg_oid())
        leg.set_call(Weak(self))
        return leg
        

    def make_leg(self, type):
        return self.pimp_leg(self.switch.make_leg(self, type))


    def make_routing(self):
        return Routing(Weak(self))
        
        
    def start(self, incoming_leg):
        self.start_routing(incoming_leg)
        
        
    def may_finish(self):
        if any(self.legs):
            return
            
        if any(self.media_channels):
            return
            
        self.logger.debug("Call is finished.")
        self.switch.finish_call(self.oid)

        
    def reported(self, action):
        Routable.reported(self, action)
        
        type = action["type"]
        
        if type == "finish":
            # Okay to stay if legs are present
            if not any(self.legs):
                self.finish_media()
        elif type == "anchor":
            self.routing.flatten(self.legs)
            
            if len(self.legs) % 2 != 0:
                raise Exception("Anchored an odd number %d of legs!" % len(self.legs))
            else:
                self.logger.debug("Anchored %d legs." % len(self.legs))

            for i, leg in enumerate(self.legs):
                leg.set_report(WeakMethod(self.forward, i).bind_front())
                
            self.refresh_media()


    def allocate_media_address(self, channel_index):
        # TODO
        sid_affinity = None
        return self.switch.mgc.allocate_media_address(sid_affinity)
        
        
    def deallocate_media_address(self, addr):
        self.switch.mgc.deallocate_media_address(addr)
        

    def make_media_leg(self, channel_index, type, **kwargs):
        # TODO
        sid_affinity = None
        ml = self.switch.mgc.make_media_leg(sid_affinity, type, **kwargs)
        ml.set_report_dirty(WeakMethod(self.dirty, channel_index))
        
        return ml
        
        
    def dirty(self, channel_index):
        # TODO: this should be more intelligent!
        self.logger.debug("Channel %d is dirty!" % channel_index)
        self.refresh_media()
        
        
    def refresh_media(self):
        # TODO: do it more intelligently, only checking the dirty legs!
        if not self.legs:
            # Can happen before anchoring
            return
            
        leg_count = len(self.legs)
        media_leg_lists = [ leg.media_legs if leg else [] for leg in self.legs ]
        channel_count = max(len(mll) for mll in media_leg_lists)
        
        for ci in range(channel_count):
            li = 0
            
            # Pretend that unrealized legs don't exist
            media_legs = [ mll[ci] if ci < len(mll) and mll[ci].is_created else None for mll in media_leg_lists ]
            
            # Sanity check
            sids = set(ml.sid for ml in media_legs if ml)
            if len(sids) != 1:
                raise Exception("Channel %d media legs has %d sids!" % (ci, len(sids)))
                
            sid = sids.pop()
            spans = set()
            
            while li < leg_count:
                while li < leg_count and not media_legs[li]:
                    li += 1
                    
                if li == leg_count:
                    break
                    
                if li % 2 != 0:
                    self.logger.debug("Right media leg %d has no left pair, ignoring!" % li)
                    li += 1
                    continue
                    
                left = li
                li += 1

                while li < leg_count and not media_legs[li]:
                    li += 1
                    
                if li == leg_count:
                    self.logger.debug("Left media leg %d has no right pair, ignoring!" % left)
                    break
                    
                if li % 2 != 1:
                    self.logger.debug("Left media leg %d has no right pair, ignoring!" % left)
                    continue
                
                right = li
                li += 1
                
                span = left, right
                spans.add(span)
                
            if ci >= len(self.media_channels):
                self.media_channels.append({})
                
            media_contexts_by_span = self.media_channels[ci]
            
            for span in list(media_contexts_by_span.keys()):
                left, right = span
                
                if span not in spans:
                    self.logger.debug("Removing media context for channel %d span %d-%d" % (ci, left, right))
                    media_contexts_by_span.pop(span)
                    
            for span in spans:
                left, right = span
                
                if span not in media_contexts_by_span:
                    self.logger.debug("Adding media context for channel %d span %d-%d" % (ci, left, right))
                    mc = MediaContext(self.switch.mgc)
                    soid = build_oid(self.oid, "legs", "%d-%d" % (left, right))
                    coid = build_oid(soid, "channel", ci)
                    mc.set_oid(coid)
                    media_contexts_by_span[span] = mc
                else:
                    mc = media_contexts_by_span[span]
                    
                mc.set_sid_and_leg_oids(sid, [ media_legs[left].oid, media_legs[right].oid ])
                
        
    def forward(self, li, action):
        type = action["type"]
        
        if type == "finish":
            self.logger.debug("Anchored leg %d finished." % li)
            self.legs[li] = None
            
            if action.get("error"):
                self.logger.error("It aborted with: %s" % action["error"])
                
                for leg in self.legs:
                    if leg:
                        leg.do(dict(type="hangup"))

            # Clean up after the last leg is gone
            if not any(self.legs):
                self.finish_media()
        elif type == "anchor":
            raise Exception("Pls, something is wrong here!")
        else:
            lj = li + 1 - 2 * (li % 2)
            self.logger.debug("Forwarding %s from anchored leg %d to %d." % (type, li, lj))
            self.legs[lj].do(action)


    def media_finished(self, ci, span):
        # Completed
        self.media_channels[ci].pop(span)
        
        self.may_finish()


    def finish_media(self):
        # Initiated
        for ci, mcbs in enumerate(self.media_channels):
            for span, mc in mcbs.items():
                left, right = span
                self.logger.debug("Finishing media channel %d span %d-%d." % (ci, left, right))
                mc.delete(WeakMethod(self.media_finished, ci, span))
            
        self.may_finish()
