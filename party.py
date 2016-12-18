from weakref import proxy

from ground import GroundDweller, Leg
from format import Status
import zap



class Party(GroundDweller):
    def __init__(self):
        GroundDweller.__init__(self)
        
        self.call_oid = None
        self.path = None
        self.finished_slot = zap.Slot()
        
            
    def set_path(self, call_oid, path):
        self.call_oid = call_oid
        self.path = path


    def make_leg(self, li):
        leg = Leg(proxy(self), li)
        
        self.ground.setup_leg(leg, self.oid.add("leg", li))
        
        return leg
        
        
    def make_media_leg(self, type):
        return self.ground.make_media_leg(type)
        
        
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


    def do_slot(self, li, action):
        self.logger.critical("No do_slot: %r" % self)
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
        

    def do_slot(self, li, action):
        self.do(action)




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
            
        leg = self.add_leg()
        
        return leg

    
    def add_leg(self):
        li = self.leg_count  # outgoing legs are numbered from 1
        self.leg_count += 1

        leg = self.make_leg(li)
        self.legs[li] = leg
        self.queued_leg_actions[li] = []
        
        return leg


    def remove_leg(self, li):
        leg = self.legs.pop(li)
        leg.may_finish()


    def queue_leg_action(self, li, action):
        self.logger.debug("Queueing %s from leg %s." % (action["type"], li))
        self.queued_leg_actions[li].append(action)


    def dial(self, type, action):
        if action["type"] != "dial":
            raise Exception("Dial action is not a dial: %s" % action["type"])

        self.logger.debug("Dialing out to: %s" % (type,))
        
        leg = self.add_leg()
        li = leg.number
        
        party = self.ground.make_party(type, self.call_oid, self.path + [ li ])
        party_leg = party.start()
        self.ground.link_legs(leg.oid, party_leg.oid)

        leg.forward(action)


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
        self.legs[0].forward(dict(type="ring"))


    def accept_incoming_leg(self):
        if self.is_accepted:
            return
            
        self.is_accepted = True
        self.logger.warning("Accepting.")
        self.legs[0].forward(dict(type="accept"))
            

    def reject_incoming_leg(self, status):
        if self.is_accepted:
            raise Exception("Incoming leg already accepted!")
            
        self.logger.warning("Rejecting with status %s" % (status,))
        self.legs[0].forward(dict(type="reject", status=status))
        self.remove_leg(0)
            

    def anchor_outgoing_leg(self, li):
        if self.is_anchored:
            raise Exception("An outgoing leg already anchored!")
            
        self.is_anchored = True
        self.logger.debug("Anchored to outgoing leg %d." % li)
        self.hangup_outgoing_legs(except_li=li)
            
        # FIXME: here we really need async action forwarding, because we don't want
        # responses to these actions to arrive immediately. A Routing may want to remove
        # itself from Ground before those responses arrive!
        for action in self.queued_leg_actions[li]:
            self.legs[0].forward(action)
                
            #self.ground.collapse_legs(self.legs[0].oid, self.legs[li].oid, self.queued_leg_actions[li])
            #self.remove_leg(li)
            #self.remove_leg(0)


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
                    self.legs[0].forward(dict(type="hangup"))
                    self.remove_leg(0)
                else:
                    # Havent accepted yet, so send a reject instead
                    self.reject_incoming_leg(Status(500))
        else:
            if self.legs:
                # No incoming leg, no fun
                self.logger.error("Routing ended without incoming leg!")
                self.hangup_outgoing_legs()
                
        Party.may_finish(self)
        
        
    def process_dial(self, action):
        self.reject_incoming_leg(Status(604))


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
                    self.reject_incoming_leg(Status(500))
                    self.hangup_outgoing_legs()
                #raise Exception("Should have handled dial in a subclass!")
            elif type == "hangup":
                self.remove_leg(0)
                self.hangup_outgoing_legs(None)
            elif self.is_anchored:
                out_li = max(self.legs.keys())
                self.legs[out_li].forward(action)
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
                self.remove_leg(li)

                if len(self.legs) == 1:
                    # And only that, so time to give up
                    self.reject_incoming_leg(action["status"])
            elif type == "accept":
                if self.is_anchored:
                    self.legs[0].forward(action)
                else:
                    self.queue_leg_action(li, action)
                    self.anchor_outgoing_leg(li)
            elif type == "hangup":
                self.remove_leg(li)
            
                if self.is_anchored:
                    # Clean up properly in this case
                    self.legs[0].forward(action)
                    self.remove_leg(0)
                else:
                    # Somebody was lazy here, dialed out, even if an accept came in it
                    # didn't anchor the leg, and now it's hanging up. If this was the last
                    # outgoing leg, may_finish will clean up this mess.
                    pass
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
                        self.legs[0].forward(action)
                    else:
                        self.queue_leg_action(li, action)
            
        self.may_finish()


    def do_slot(self, li, action):
        self.process_leg_action(li, action)


class Routing(Bridge):
    def anchor_outgoing_leg(self, li):
        # FIXME: workaround until async action processing is implemented
        queued_actions = self.queued_leg_actions[li]
        self.queued_leg_actions[li] = []
        
        Bridge.anchor_outgoing_leg(self, li)
        
        self.ground.collapse_legs(self.legs[0].oid, self.legs[li].oid, queued_actions)
        self.remove_leg(li)
        self.remove_leg(0)
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
            self.reject_incoming_leg(Status(500))
            self.hangup_outgoing_legs(None)
            
        self.may_finish()
        
        
    def do_slot(self, li, action):
        self.logger.debug("Planned routing processing a %s" % action["type"])
        self.send_event(li, action)


class SimpleBridge(Bridge):
    def process_dial(self, action):
        self.dial("routing", action)
        
        
#    def __init__(self):
#        Bridge.__init__(self)

        
#    def may_finish(self):
#        if 1 in self.legs:
#            self.logger.debug("Releasing outgoing leg.")
#            self.remove_leg(1)
#
#        if 0 in self.legs:
#            self.logger.debug("Releasing incoming leg.")
#            self.remove_leg(0)
#            
#        Bridge.may_finish(self)
            

#    def do_slot(self, li, action):
#        type = action["type"]
#
#        if li == 0 and type == "dial":
#            self.dial("routing", action)  # TODO: this used to make thing with (... self.path, "routing")
#            return
#        
#        if li == 0:
#            leg = self.legs[1]
#            direction = "forward"
#        else:
#            leg = self.legs[0]
#            direction = "backward"
#        
#        self.logger.debug("Bridging %s %s" % (type, direction))
#        leg.forward(action)
#
#        if type in ("hangup", "reject"):
#            self.may_finish()

    
class RecordingBridge(SimpleBridge):
    def hack_media(self, li, answer):
        old = len(self.legs[0].media_legs)
        new = len(answer["channels"])
        
        for i in range(old, new):
            answer_channel = answer["channels"][i]
            ctype = answer_channel["type"]
            mgw_affinity = answer_channel.get("mgw_affinity")
            mgw_sid = self.ground.select_gateway_sid(ctype, mgw_affinity)

            this = self.make_media_leg("pass")
            that = self.make_media_leg("pass")

            # Pairing must happen before setting it, because realizing needs it
            this.pair(proxy(that))
            that.pair(proxy(this))
            
            self.legs[0].set_media_leg(i, this, mgw_sid)
            self.legs[1].set_media_leg(i, that, mgw_sid)
            
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
        
        if session and session["is_answer"] and "channels" in session:
            self.hack_media(li, session)
        
        SimpleBridge.do_slot(self, li, action)
