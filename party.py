from weakref import proxy

from ground import GroundDweller
from format import Status, Reason
from sdp import Session
import zap


class Party(GroundDweller):
    def __init__(self):
        GroundDweller.__init__(self)
        
        self.finished_slot = zap.Slot()
        self.media_things = []


    def set_call_info(self, call_info):
        self.call_info = call_info
        

    def get_call_info(self):
        return self.call_info


    def identify(self, params):
        raise NotImplementedError()
        
            
    def make_leg(self, li):
        return self.ground.make_leg(proxy(self), li)
        
        
    def add_media_thing(self, type, mgw_sid):
        i = len(self.media_things)
        thing = self.ground.make_media_thing(type)
        
        thing.set_oid(self.oid.add("media", i))
        thing.set_mgw(mgw_sid)
        thing.create()
        
        self.media_things.append(thing)
        return thing
        
        
    def remove_media_thing(self):
        self.media_things.pop()
        
        
    def start(self):
        raise NotImplementedError()


    def abort(self):
        raise NotImplementedError()
        

    def may_finish(self):
        # FIXME: hm, it's currently possible that we get here multiple times,
        # can't we do something about it?
        if self.finished_slot:
            self.logger.info("Party finished.")
            self.finished_slot.zap()
            self.finished_slot = None


    def do_leg(self, li, action):
        self.logger.critical("No do_leg: %r" % self)
        raise NotImplementedError()




class Endpoint(Party):
    def __init__(self):
        Party.__init__(self)
        
        self.leg = None
        
            
    def start(self):
        self.leg = self.make_leg(None)
        
        return self.leg

    
    def may_finish(self):
        self.leg.may_finish()
        self.leg = None
        
        Party.may_finish(self)


    def do(self, action):
        raise NotImplementedError()
        

    def do_leg(self, li, action):
        self.do(action)


    def forward(self, action):
        self.leg.forward(action)


    def dial(self, src, session=None):
        action = dict(
            type="dial",
            call_info=self.get_call_info(),
            src=src,
            ctx={},
            session=session
        )
        
        self.forward(action)

        
    def process_transfer(self, action):
        self.ground.transfer_leg(self.leg.oid, action)
        

class PlannedEndpoint(zap.Planned, Endpoint):
    def __init__(self):
        zap.Planned.__init__(self)
        Endpoint.__init__(self)


    def start(self):
        self.start_plan()
        
        return Endpoint.start(self)
        
        
    def may_finish(self):
        if self.is_plan_running():
            return
            
        Endpoint.may_finish(self)
        

    def wait_action(self, timeout=None):
        event = yield from self.wait_event(timeout=timeout)
        if not event:
            return None
            
        action, = event
        
        return action
            

    def do(self, action):
        self.send_event(action)


    def plan_finished(self, error):
        if error:
            self.logger.error("Endpoint plan aborted with: %s!" % error)
        
        self.may_finish()




class Bridge(Party):
    def __init__(self):
        Party.__init__(self)
        
        self.leg_count = 0
        self.legs = {}
        self.queued_leg_actions = {}

        self.is_ringing = False  # if a ring action was sent to the incoming leg
        self.is_anchored = False  # if a routing decision was made on the outgoing legs
        self.is_accepted = False  # if an accept was sent to the incoming leg
        
            
    def start(self):
        if self.legs:
            raise Exception("Already started!")
            
        li = self.add_leg()
        
        return self.legs[li]

    
    def add_leg(self):
        li = self.leg_count  # outgoing legs are numbered from 1
        self.leg_count += 1

        leg = self.make_leg(li)
        self.legs[li] = leg
        self.queued_leg_actions[li] = []
        
        return li


    def remove_leg(self, li):
        leg = self.legs.pop(li)
        leg.may_finish()


    def queue_leg_action(self, li, action):
        self.logger.debug("Queueing %s from leg %s." % (action["type"], li))
        self.queued_leg_actions[li].append(action)


    def forward_leg(self, li, action):
        self.legs[li].forward(action)
        

    def dial(self, action, type=None, **kwargs):
        li = self.add_leg()
        dst = dict(type=type, **kwargs) if type else None
        action = dict(action, dst=dst)
        self.forward_leg(li, action)
        
        return li


    def hangup_outgoing_legs(self, except_li=None):
        for li, leg in list(self.legs.items()):
            if li not in (0, except_li):
                self.logger.debug("Hanging up outgoing leg %s" % li)
                leg.forward(dict(type="hangup"))
                self.remove_leg(li)


    def ring_incoming_leg(self):
        if self.is_ringing:
            return
            
        self.is_ringing = True
        self.logger.debug("Sending artificial ringback.")
        self.forward_leg(0, dict(type="ring"))


    def accept_incoming_leg(self, action):
        if self.is_accepted:
            return
            
        self.is_accepted = True
        self.is_ringing = False  # make reringing possible
        self.logger.info("Accepting incoming leg.")
        self.forward_leg(0, action)
            

    def reject_incoming_leg(self, status):
        if self.is_accepted:
            raise Exception("Incoming leg already accepted!")
            
        self.logger.warning("Rejecting incoming leg with status %s" % (status,))
        self.forward_leg(0, dict(type="reject", status=status))
        self.remove_leg(0)
            

    def anchor_outgoing_leg(self, li):
        if self.is_anchored:
            raise Exception("An outgoing leg already anchored!")
            
        self.logger.debug("Anchoring to outgoing leg %d." % li)
        self.hangup_outgoing_legs(except_li=li)
        
        self.ground.legs_anchored(self.legs[0].oid, self.legs[li].oid)
        self.is_anchored = True

        for action in self.queued_leg_actions[li]:
            self.forward_leg(0, action)


    def unanchor_legs(self):
        if not self.is_anchored:
            raise Exception("Legs were not anchored!")
        
        if len(self.legs) != 2:
            raise Exception("Not two legs are anchored!")
        
        self.is_anchored = False
        oli = max(self.legs.keys())
        self.ground.legs_unanchored(self.legs[0].oid, self.legs[oli].oid)


    def collapse_unanchored_legs(self, queue0, queue1):
        # Let other Party-s still use media once we're no longer present.
        if self.is_anchored:
            raise Exception("Legs still anchored!")
        
        if len(self.legs) != 2:
            raise Exception("Not two legs were unanchored!")
        
        oli = max(self.legs.keys())
        self.ground.collapse_legs(self.legs[0].oid, self.legs[oli].oid, queue0, queue1)
        
        self.remove_leg(0)
        self.remove_leg(oli)


    def may_finish(self):
        # For a clean finish we need either the incoming leg with some outgoing one,
        # or no legs at all. Every other case is an error, and we clean it up here.
        
        if 0 in self.legs:
            if len(self.legs) > 1:
                return
            else:
                # No outgoing legs? We surely won't dial by ourselves, so terminate here.
                self.logger.error("Routing ended without outgoing legs!")
                
                if self.is_accepted:
                    # We once sent an accept, so now we can forward the hangup
                    self.forward_leg(0, dict(type="hangup"))
                    self.remove_leg(0)
                else:
                    # Havent accepted yet, so send a reject instead
                    self.reject_incoming_leg(Status.SERVER_INTERNAL_ERROR)
        else:
            if self.legs:
                # No incoming leg, no fun
                self.logger.error("Routing ended without incoming leg!")
                self.hangup_outgoing_legs()
                
        Party.may_finish(self)
        
        
    def process_dial(self, action):
        # Just pass it to the next routing
        self.dial(action)


    def process_leg_transfer(self, li, action):
        self.ground.transfer_leg(self.legs[li].oid, action)


    def process_leg_action(self, li, action):
        type = action["type"]
        self.logger.debug("Got %s from leg %d." % (type, li))

        if 0 not in self.legs:
            raise Exception("Lost the incoming leg!")

        if li == 0:
            # Actions from the incoming leg
            
            if type == "dial":
                # Let's be nice and factor this out for the sake of simplicity
                try:
                    self.process_dial(action)
                except Exception as e:
                    self.logger.error("Dial processing error: %s" % (e,), exc_info=True)
                    self.reject_incoming_leg(Status.SERVER_INTERNAL_ERROR)
                    self.hangup_outgoing_legs()
                else:
                    # If the dial action is executed by this fallback code, then the
                    # user is probably satisfied with this many outgoing legs, so
                    # if there's only one, then anchor it right now.
                    
                    if not self.is_anchored and len(self.legs) == 2:
                        oli = max(self.legs.keys())
                        self.logger.info("Auto-anchoring outgoing leg %d." % oli)
                        self.anchor_outgoing_leg(oli)
            elif type == "hangup":
                if self.is_anchored:
                    self.unanchor_legs()
                    self.collapse_unanchored_legs([], [ action ])
                else:
                    self.hangup_outgoing_legs()
                    self.remove_leg(0)
            elif type == "transfer":
                self.process_leg_transfer(0, action)
            elif self.is_anchored:
                oli = max(self.legs.keys())
                self.forward_leg(oli, action)
            else:
                # The user accepted the incoming leg, and didn't handle the consequences.
                raise Exception("Unexpected action from unanchored incoming leg: %s" % type)
        else:
            # Actions from an outgoing leg

            # First the important actions
            if type == "reject":
                # FIXME: we probably shouldn't just forward the last rejection status.
                # But we should definitely reject here explicitly, because the may_finish
                # cleanup rejects with 500 always.
                
                if self.is_anchored:
                    self.unanchor_legs()
                    self.collapse_unanchored_legs([ action ], [])
                else:
                    self.remove_leg(li)

                    if len(self.legs) == 1:
                        # No more incoming legs, so time to give up
                        self.forward_leg(0, action)
                        self.remove_leg(0)
                        
                    # TODO: shall we auto-anchor the last remaining outgoing leg?
            elif type == "accept":
                if not self.is_anchored:
                    self.anchor_outgoing_leg(li)
                    
                self.accept_incoming_leg(action)  # must use this to set flags properly
            elif type == "hangup":
                if self.is_anchored:
                    self.unanchor_legs()
                    self.collapse_unanchored_legs([ action ], [])
                else:
                    self.remove_leg(li)
                    # Somebody was lazy here, dialed out, even if an accept came in it
                    # didn't anchor the leg, and now it's hanging up. If this was the last
                    # outgoing leg, may_finish will clean up this mess.
                    pass
            elif type == "transfer":
                self.process_leg_transfer(li, action)
            else:
                # The less important actions
            
                if type == "ring":
                    # This is kinda special because we have a flag for it partially
                    self.ring_incoming_leg()
            
                    if action.get("session"):
                        action["type"] = "session"
                        type = "session"

                if type != "ring":
                    if self.is_anchored:
                        self.forward_leg(0, action)
                    else:
                        self.queue_leg_action(li, action)
            
        self.may_finish()


    def do_leg(self, li, action):
        self.process_leg_action(li, action)


class Routing(Bridge):
    def identify(self, params):
        identity = "%s" % self.call_info["routing_count"]
        self.call_info["routing_count"] += 1
        
        return identity
        

    def process_dial(self, action):
        self.reject_incoming_leg(Status.DOES_NOT_EXIST_ANYWHERE)

        
    def anchor_outgoing_leg(self, li):
        # We overload this method completely. Skip the anchoring thing in Bridge,
        # because we collapse instead of unanchoring. And the queued actions will
        # only be sent once we're out of the game.
        
        self.logger.debug("Routing anchored to outgoing leg %d." % li)
        self.hangup_outgoing_legs(except_li=li)
        self.collapse_unanchored_legs(self.queued_leg_actions[li], [])
        
        # After having no legs, may_finish will terminate us as soon as it can


class PlannedRouting(zap.Planned, Routing):
    def __init__(self):
        zap.Planned.__init__(self)
        Routing.__init__(self)


    def start(self):
        self.start_plan()
        
        return Routing.start(self)


    def may_finish(self):
        if self.is_plan_running():
            return
            
        Routing.may_finish(self)
            

    def wait_leg_action(self, timeout=None):
        event = yield from self.wait_event(timeout=timeout)
        if not event:
            return None, None
            
        li, action = event

        return li, action


    def plan_finished(self, exception):
        # Take control from here
        self.logger.debug("Routing plan finished.")
        self.event_slot.plug(self.process_leg_action)
        
        if exception:
            self.logger.error("Routing plan aborted with exception: %s" % exception)
            self.reject_incoming_leg(Status.SERVER_INTERNAL_ERROR)
            self.hangup_outgoing_legs(None)
        else:
            # Auto-anchoring
            if not self.is_anchored and len(self.legs) == 2:
                oli = max(self.legs.keys())
                self.logger.info("Auto-anchoring outgoing leg %d." % oli)
                self.anchor_outgoing_leg(oli)
            
        self.may_finish()
        
        
    def do_leg(self, li, action):
        self.logger.debug("Planned routing processing a %s" % action["type"])
        self.send_event(li, action)


class RecordingBridge(Bridge):
    def identify(self, params):
        return params["id"]
        
        
    def hack_media(self, li, answer):
        old = len(self.media_things)
        new = len(answer["channels"])
        
        for i in range(old, new):
            answer_channel = answer["channels"][i]
            ctype = answer_channel["type"]
            mgw_affinity = answer_channel.get("mgw_affinity")
            mgw_sid = self.ground.select_gateway_sid(ctype, mgw_affinity)

            thing = self.add_media_thing("record", mgw_sid)
            
            self.legs[0].add_media_leg(thing.get_leg(0))
            self.legs[1].add_media_leg(thing.get_leg(1))
            
            format = ("L16", 8000, 1, None)
            thing.modify(dict(filename="recorded.wav", format=format, record=True))
            
        if len(answer["channels"]) >= 1:
            c = answer["channels"][0]

            if not c["send"]:
                self.logger.info("Hah, the %s put us on hold." % ("callee" if li == 0 else "caller"))

            if not c["recv"]:
                self.logger.info("Hah, the %s put us on hold." % ("caller" if li == 0 else "callee"))

            if c["send"] and c["recv"]:
                self.logger.info("Hah, a two-way call.")
                

    def do_leg(self, li, action):
        session = action.get("session")
        
        if session and session.is_accept():
            self.hack_media(li, session)
        
        Bridge.do_leg(self, li, action)


class RedialBridge(Bridge):
    def __init__(self):
        Bridge.__init__(self)

        self.is_ringing = False
        self.is_playing = False
        
        # We use leg #1 party session to play media to, because it is updated
        # automatically for backward actions before do_leg is called.
        

    def identify(self, dst):
        return None
        
        
    def play_ringback(self):
        media_thing = self.media_things[0] if self.media_things else None
        ss = self.legs[1].session_state

        # We may not get the answer until the call is accepted
        session = ss.get_party_session() or ss.pending_party_session
    
        if not media_thing and session:
            self.logger.info("Creating media thing for artificial ringback tone.")
            channel = session["channels"][0]
            ctype = channel["type"]
            mgw_affinity = channel.get("mgw_affinity")

            mgw_sid = self.ground.select_gateway_sid(ctype, mgw_affinity)
            channel["mgw_affinity"] = mgw_sid
            
            media_thing = self.add_media_thing("player", mgw_sid)

            self.legs[0].add_media_leg(media_thing.get_leg(0))
        
        if media_thing and self.is_ringing and not self.is_playing:
            self.is_playing = True
            self.logger.info("Playing artificial ringback tone.")
            format = ("PCMA", 8000, 1, None)  # FIXME
            media_thing.play("ringtone.wav", format, volume=0.1)
        
        
    def stop_ringback(self):
        media_thing = self.media_things[0] if self.media_things else None

        if self.is_playing:
            self.logger.info("Stopping artificial ringback tone.")
            self.is_playing = False

        if media_thing:
            self.legs[0].remove_media_leg()
            self.remove_media_thing()
            
        
    def do_leg(self, li, action):
        self.logger.info("Redial do_leg %d: %s" % (li, action))
        
        
        if li > 0:
            type = action["type"]
            
            if type == "ring":
                self.is_ringing = True
                self.play_ringback()
                self.logger.info("Not forwarding ring action.")
                return

            elif type == "accept":
                self.stop_ringback()
                
                self.logger.info("Not forwarding accept action, collapsing instead.")
                self.unanchor_legs()
                self.collapse_unanchored_legs([], [])
                return

            elif type == "reject":
                self.stop_ringback()
                
                status = action.get("status") or Status.SERVER_INTERNAL_ERROR
                reason = Reason("SIP", dict(cause=str(status.code), text=status.reason))

                self.logger.info("Forwarding reject as hangup with reason %s." % (reason,))
                action = dict(type="hangup", reason=[ reason ])
                
            else:
                self.play_ringback()  # just try, maybe we have the session right

        Bridge.do_leg(self, li, action)


class SessionNegotiatorBridge(Bridge):
    CHANNEL_KEYS = ("type", "proto", "send", "recv", "formats", "attributes", "mgw_affinity")
    
    
    def identify(self, dst):
        self.forward_session = dst["forward_session"].copy()
        self.backward_session = dst.get("backward_session")
        self.next_dst = dst["next_dst"]
        self.is_backward = None
        
        # We must give a chance to parties put on hold to come off the hold.
        # Otherwise if the transferred party was put on hold before the transfer,
        # it will be in recvonly forever.
        for c in self.forward_session["channels"]:
            c["send"] = True
            c["recv"] = True
        
        return None
        
        
    def simply(self, s):
        return [ [ c.get(k) for k in self.CHANNEL_KEYS ] for c in s["channels"] ] if s else None
    
    
    def process_dial(self, action):
        self.dial(action, **(self.next_dst or {}))
        
        
    def do_leg(self, li, action):
        type = action["type"]
        session = action.pop("session", None)

        if self.is_backward is None:
            if session:
                raise Exception("Unexpected initial session!")
                
            if li != 0:
                raise Exception("Unexpected initial direction!")
                
            if self.forward_session.is_offer():
                offer = self.forward_session
            else:
                offer = self.forward_session.flipped()
                    
            self.logger.info("Sending initial forward offer in %s action." % type)
            action["session"] = offer
            self.is_backward = False
        elif session:
            if session.is_query():
                self.logger.warning("Got unexpected session query, ignoring!")
            elif session.is_offer():
                self.logger.warning("Got unexpected session offer, rejecting!")
                self.forward_leg(li, dict(type="session", session=Session.make_reject()))
            elif session.is_accept():
                if li == 0:
                    # Forward answer
                    
                    if self.is_backward:
                        if self.simply(session) == self.simply(self.forward_session):
                            self.logger.info("Got the same forward answer, completing.")
                            self.unanchor_legs()
                            actions = [ action ] if type != "session" else []
                            self.collapse_unanchored_legs([], actions)
                            return
                        else:
                            self.logger.info("Got different forward answer, reoffering.")
                            self.forward_session = session
                            action["session"] = session.flipped()
                            self.is_backward = False
                    else:
                        self.logger.warning("Got unexpected forward answer, ignoring!")
                else:
                    # Backward answer
                    
                    if self.is_backward:
                        self.logger.warning("Got unexpected backward answer, ignoring!")
                    else:
                        if self.simply(session) == self.simply(self.backward_session):
                            self.logger.info("Got the same backward answer, completing.")
                            self.unanchor_legs()
                            actions = [ action ] if type != "session" else []
                            self.collapse_unanchored_legs(actions, [])
                            return
                        else:
                            self.logger.info("Got different backward answer, reoffering.")
                            self.backward_session = session
                            action["session"] = session.flipped()
                            self.is_backward = True
            elif session.is_reject():
                # We should probably hang up both parties here.
                self.logger.warning("Session rejected, now what?")
                return
            
        if type == "session" and not action.get("session"):
            return
            
        Bridge.do_leg(self, li, action)
