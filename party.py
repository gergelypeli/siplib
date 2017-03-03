from weakref import proxy
from collections import namedtuple

from ground import GroundDweller
from format import Status, Cause
from sdp import Session
from zap import Slot, Plug, Planned


MediaChannel = namedtuple("MediaChannel", [ "things", "links", "legs"])


class Party(GroundDweller):
    def __init__(self):
        GroundDweller.__init__(self)
        
        self.finished_slot = Slot()
        self.legs = {}
        self.media_channels = []


    def set_call_info(self, call_info):
        self.call_info = call_info
        

    def get_call_info(self):
        return self.call_info


    def identify(self, params):
        raise NotImplementedError()
        
            
    def make_leg(self, li):
        return self.ground.make_leg(proxy(self), li)


    def select_gateway_sid(self, channel):
        ctype = channel["type"]
        mgw_affinity = channel.get("mgw_affinity")
        mgw_sid = self.ground.select_gateway_sid(ctype, mgw_affinity)

        channel.setdefault("mgw_affinity", mgw_sid)
        
        return mgw_sid
        

    def make_media_thing(self, type, mgw_sid):
        thing = self.ground.make_media_thing(type)
        thing.set_mgw(mgw_sid)
        
        return thing
            
        
    def add_media_thing(self, ci, name, thing):
        while ci >= len(self.media_channels):
            self.media_channels.append(MediaChannel({}, {}, {}))

        c = self.media_channels[ci]
        thing.set_oid(self.oid.add("media", str(ci)).add(name))
        thing.create()
            
        c.things[name] = thing


    def remove_media_thing(self, ci, name):
        c = self.media_channels[ci]
        c.things.pop(name)


    def get_media_thing(self, ci, name):
        c = self.media_channels[ci] if ci < len(self.media_channels) else None
        
        return c.things.get(name) if c else None

        
    def link_media_things(self, ci, name0, li0, name1, li1):
        c = self.media_channels[ci]
        
        slot0 = (name0, li0)
        slot1 = (name1, li1)
        
        if slot0 in c.links or slot1 in c.links:
            raise Exception("Media things already linked!")
            
        c.links[slot0] = slot1
        c.links[slot1] = slot0
        
        if not name0:
            c.legs[li0] = c.things[name1].get_leg(li1)
            self.ground.media_leg_appeared(self.legs[li0].oid, ci)

        if not name1:
            c.legs[li1] = c.things[name0].get_leg(li0)
            self.ground.media_leg_appeared(self.legs[li1].oid, ci)


    def unlink_media_things(self, ci, name0, li0, name1, li1):
        c = self.media_channels[ci]
        
        slot0 = (name0, li0)
        slot1 = (name1, li1)
        
        if c.links[slot0] != slot1 or c.links[slot1] != slot0:
            raise Exception("Media things not linked!")
            
        c.links.pop(slot0)
        c.links.pop(slot1)
        
        if not name0:
            self.ground.media_leg_disappearing(self.legs[li0].oid, ci)
            c.legs.pop(li0)

        if not name1:
            self.ground.media_leg_disappearing(self.legs[li1].oid, ci)
            c.legs.pop(li1)
        
        
    def get_media_leg(self, li, ci):
        c = self.media_channels[ci] if ci < len(self.media_channels) else None
        
        return c.legs.get(li) if c else None
        
        
    def get_anchored_leg(self, li):
        return None


    def remove_leg(self, li):
        for i, c in enumerate(self.media_channels):
            slot0 = (None, li)
            slot1 = c.links.get(slot0)
            
            if slot1:
                name0, li0 = slot0
                name1, li1 = slot1
                
                self.unlink_media_things(i, name0, li0, name1, li1)
        
        self.legs.pop(li).may_finish()
        
        
    def start(self):
        if self.legs:
            raise Exception("Already started!")

        self.legs[0] = self.make_leg(0)
        
        return self.legs[0]


    def abort(self):
        raise NotImplementedError()
        

    def may_finish(self):
        for i, c in enumerate(self.media_channels):
            while c.links:
                slot0 = next(iter(c.links))
                slot1 = c.links[slot0]
                
                name0, li0 = slot0
                name1, li1 = slot1
                
                self.unlink_media_things(i, name0, li0, name1, li1)
                
            while c.things:
                name = next(iter(c.things))
                self.remove_media_thing(i, name)
            
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
        
            
    def may_finish(self):
        self.remove_leg(0)
        
        Party.may_finish(self)


    def do(self, action):
        raise NotImplementedError()
        

    def do_leg(self, li, action):
        self.do(action)


    def forward(self, action):
        self.legs[0].forward(action)


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
        self.ground.transfer_leg(self.legs[0].oid, action)
        

class PlannedEndpoint(Planned, Endpoint):
    def __init__(self):
        Planned.__init__(self)
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
        self.queued_leg_actions = {}

        self.is_ringing = False  # if an artificial ring action was sent to the incoming leg
        self.is_anchored = False  # if a routing decision was made on the outgoing legs
        self.is_accepted = False  # if an accept was sent to the incoming leg
        
            
    def start(self):
        leg = Party.start(self)
        
        self.leg_count = 1
        
        return leg

    
    def add_leg(self):
        li = self.leg_count  # outgoing legs are numbered from 1
        self.leg_count += 1

        leg = self.make_leg(li)
        self.legs[li] = leg
        self.queued_leg_actions[li] = []
        
        return li


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
                cause = Cause.NORMAL_CLEARING
                leg.forward(dict(type="hangup", cause=cause))
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
        cause = None  # TODO: maybe
        self.forward_leg(0, dict(type="reject", status=status, cause=cause))
        self.remove_leg(0)
            

    def anchor_outgoing_leg(self, li):
        if self.is_anchored:
            raise Exception("An outgoing leg already anchored!")
            
        self.logger.debug("Anchoring to outgoing leg %d." % li)
        self.hangup_outgoing_legs(except_li=li)
        
        oid0 = self.legs[0].oid
        oid1 = self.legs[li].oid
        
        self.is_anchored = True
        self.ground.legs_anchored(oid0, oid1)
        
        for action in self.queued_leg_actions[li]:
            self.forward_leg(0, action)


    def unanchor_legs(self):
        if not self.is_anchored:
            raise Exception("Legs were not anchored!")
        
        if len(self.legs) != 2:
            raise Exception("Not two legs are anchored!")
        
        li = max(self.legs.keys())
        
        oid0 = self.legs[0].oid
        oid1 = self.legs[li].oid
        
        self.ground.legs_unanchored(oid0, oid1)
        self.is_anchored = False


    def get_anchored_leg(self, li):
        if not self.is_anchored:
            return None
        elif li > 0:
            return self.legs[0]
        else:
            return self.legs[max(self.legs.keys())]
            

    def collapse_anchored_legs(self, queue0, queue1):
        # Let other Party-s still use media once we're no longer present.
        if not self.is_anchored:
            raise Exception("Legs to collapse are not anchored!")
        
        if len(self.legs) != 2:
            raise Exception("Not two legs were anchored!")
        
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
                    # We once sent an accept, so now we can send a hangup
                    cause = Cause.NO_ROUTE_DESTINATION
                    self.forward_leg(0, dict(type="hangup", cause=cause))
                    self.remove_leg(0)
                else:
                    # Havent accepted yet, so send a reject instead
                    self.reject_incoming_leg(Status.NOT_FOUND)
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
                    #self.unanchor_legs()
                    self.collapse_anchored_legs([], [ action ])
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
                    #self.unanchor_legs()
                    self.collapse_anchored_legs([ action ], [])
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
                    #self.unanchor_legs()
                    self.collapse_anchored_legs([ action ], [])
                else:
                    self.remove_leg(li)
                    # Somebody was lazy here, dialed out, even if an accept came in it
                    # didn't anchor the leg, and now it's hanging up. If this was the last
                    # outgoing leg, may_finish will clean up this mess.
                    pass
            elif type == "transfer":
                self.process_leg_transfer(li, action)
            elif type == "ring":
                # This is kinda special because we have a flag for it partially
                session = action.get("session")
                
                if self.is_anchored:
                    if not self.is_ringing:
                        self.forward_leg(0, action)
                    elif session:
                        self.forward_leg(0, dict(type="session", session=session))
                else:
                    self.ring_incoming_leg()
    
                    if session:
                        self.queue_leg_action(li, dict(type="session", session=session))
            else:
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
        actions = self.queued_leg_actions[li]
        self.queued_leg_actions[li] = []
        
        Bridge.anchor_outgoing_leg(self, li)
        
        #self.logger.debug("Routing anchored to outgoing leg %d." % li)
        #self.hangup_outgoing_legs(except_li=li)
        self.collapse_anchored_legs(actions, [])
        
        # After having no legs, may_finish will terminate us as soon as it can


class PlannedRouting(Planned, Routing):
    def __init__(self):
        Planned.__init__(self)
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
        Plug(self.process_leg_action).attach(self.event_slot)
        
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
        old = len(self.media_channels)
        new = len(answer["channels"])
        
        for i in range(old, new):
            mgw_sid = self.select_gateway_sid(answer["channels"][i])

            thing = self.make_media_thing("record", mgw_sid)
            self.add_media_thing(i, "rec", thing)
            self.link_media_things(i, None, 0, "rec", 0)
            self.link_media_things(i, None, 1, "rec", 1)
            
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
        media_thing = self.get_media_thing(0, "ring")
        ss = self.legs[1].session_state

        # We may not get the answer until the call is accepted
        session = ss.get_party_session() or ss.pending_party_session
    
        if not media_thing and session:
            self.logger.info("Creating media thing for artificial ringback tone.")
            mgw_sid = self.select_gateway_sid(session["channels"][0])
            
            media_thing = self.make_media_thing("player", mgw_sid)
            self.add_media_thing(0, "ring", media_thing)
            self.link_media_things(0, None, 0, "ring", 0)
        
        if media_thing and self.is_ringing and not self.is_playing:
            self.is_playing = True
            self.logger.info("Playing artificial ringback tone.")
            format = ("PCMA", 8000, 1, None)  # FIXME
            media_thing.play("ringtone.wav", format, volume=0.1)
        
        
    def stop_ringback(self):
        media_thing = self.get_media_thing(0, "ring")

        if self.is_playing:
            self.logger.info("Stopping artificial ringback tone.")
            self.is_playing = False

        if media_thing:
            self.unlink_media_things(0, None, 0, "ring", 0)
            self.remove_media_thing(0, "ring")
            
        
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
                #self.unanchor_legs()
                self.collapse_anchored_legs([], [])
                return

            elif type == "reject":
                self.stop_ringback()
                
                status = action.get("status")
                cause = action.get("cause")
                self.logger.info("Forwarding reject as hangup with cause %s." % (cause,))
                action = dict(type="hangup", status=status, cause=cause)
                
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
                            #self.unanchor_legs()
                            actions = [ action ] if type != "session" else []
                            self.collapse_anchored_legs([], actions)
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
                            #self.unanchor_legs()
                            actions = [ action ] if type != "session" else []
                            self.collapse_anchored_legs(actions, [])
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
