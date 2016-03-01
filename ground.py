from async import Weak, WeakMethod
from mgc import MediaContext
from util import build_oid, Loggable
from leg import Routing, SlotLeg

# TODO: CoidNode!

class Ground(Loggable):
    def __init__(self, mgc):
        self.mgc = mgc
        
        self.legs_by_oid = {}
        #self.unstarted_leg_ids = set()
        self.targets_by_source = {}
        self.media_contexts_by_span = {}
        self.context_count = 0
        
    
    def generate_context_oid(self):
        context_oid = build_oid(self.oid, "context", self.context_count)
        self.context_count += 1
        
        return context_oid

    
    def add_leg(self, leg):
        leg_oid = leg.oid
        if not leg_oid:
            raise Exception("Can't add leg without oid!")
        
        #leg.set_ground(Weak(self), leg_oid)
        self.logger.debug("Adding leg %s" % leg_oid)
        self.legs_by_oid[leg_oid] = leg
        #self.unstarted_leg_oids.add(leg_oid)
        
        return leg_oid
    
    
    #def make_leg(self, type):
    #    leg = self.switch.make_leg(type)
    #    leg.set_oid(self.switch.generate_leg_oid())  # TODO
    #    leg_oid = self.add_leg(leg)
        
    #    return leg_oid


    def remove_leg(self, leg_oid):
        self.logger.debug("Removing leg %s" % leg_oid)
        self.legs_by_oid.pop(leg_oid)
        #self.unstarted_leg_oids.discard(leg_oid)
        linked_leg_oid = self.targets_by_source.pop(leg_oid, None)
        
        if linked_leg_oid:
            self.targets_by_source.pop(linked_leg_oid, None)
        
        
    def link_legs(self, leg_oid0, leg_oid1):
        if leg_oid0 in self.targets_by_source:
            raise Exception("First leg already linked: %s!" % leg_oid0)

        if leg_oid1 in self.targets_by_source:
            raise Exception("Second leg already linked: %s!" % leg_oid1)
            
        self.targets_by_source[leg_oid0] = leg_oid1
        self.targets_by_source[leg_oid1] = leg_oid0
        
        #if leg_oid0 in self.unstarted_leg_oids:
        #    self.unstarted_leg_oids.remove(leg_oid0)
        #    self.legs_by_oid[leg_oid0].start()
            
        #if leg_oid1 in self.unstarted_leg_oids:
        #    self.unstarted_leg_oids.remove(leg_oid1)
        #    self.legs_by_oid[leg_oid1].start()
        
        
    def collapse_legs(self, leg_oid0, leg_oid1, queued_actions=None):
        prev_leg_oid = self.targets_by_source[leg_oid0]
        if not prev_leg_oid:
            raise Exception("No previous leg before collapsed leg %s!" % leg_oid0)
            
        next_leg_oid = self.targets_by_source[leg_oid1]
        if not next_leg_oid:
            raise Exception("No next leg after collapsed leg %s!" % leg_oid1)
        
        self.targets_by_source[leg_oid0] = None
        self.targets_by_source[leg_oid1] = None
        
        self.targets_by_source[prev_leg_oid] = next_leg_oid
        self.targets_by_source[next_leg_oid] = prev_leg_oid
        
        if queued_actions:
            for action in queued_actions:
                self.logger.debug("Forwarding queued action %s" % action["type"])
                self.legs_by_oid[prev_leg_oid].do(action)
        
        
    def start_leg(self, leg_oid):
        self.legs_by_oid[leg_oid].start()
        
    
    def forward(self, leg_oid, action):
        target = self.targets_by_source.get(leg_oid)
        
        if target:
            leg = self.legs_by_oid.get(target)
            
            if leg:
                leg.do(action)


    def refresh_media(self, slid, ci):
        tlid = self.targets_by_source.get(slid)
        if not tlid:
            self.logger.debug("Dirty leg is not linked, ignoring.")
            return
            
        sleg = self.legs_by_oid[slid]
        tleg = self.legs_by_oid[tlid]
        
        smleg = sleg.get_media_leg(ci)  # Must be non-None, since it is refreshing
        tmleg = tleg.get_media_leg(ci)
        
        # Without target changes in the source doesn't matter
        if not tmleg or not tmleg.is_created:
            self.logger.debug("Dirty channel has no linked pair, ignoring.")
            return

        soid = smleg.oid
        toid = tmleg.oid
        
        span = (soid, toid, ci) if soid < toid else (toid, soid, ci)
        
        if smleg.is_created:
            # Create
            
            if span in self.media_contexts_by_span:
                self.logger.debug("Hm, context already exists, how can you be dirty?")
            else:
                coid = self.generate_context_oid()
                self.logger.debug("Creating context %s: %s" % (coid, span))
                
                if smleg.sid != tmleg.sid:
                    raise Exception("Sid mismatch!")
                else:
                    sid = smleg.sid
                
                mc = MediaContext(self.mgc, sid)
                mc.set_oid(coid)
                self.media_contexts_by_span[span] = mc
                
                mc.set_leg_oids([ soid, toid ])
        else:
            ctx = self.media_contexts_by_span.pop(span, None)
            
            if not ctx:
                self.logger.debug("Hm, context does not exist, how can you be dirty?")
            else:
                self.logger.debug("Removing context: %s" % str(span))
                ctx.delete()


class Call(Loggable):
    def __init__(self, switch, ground):
        Loggable.__init__(self)

        self.switch = switch
        self.ground = ground
        self.leg_oids = set()
                
        self.bridge_count = 0


    def generate_leg_oid(self, type, path):
        if type == "routing":
            if path:
                return build_oid(self.oid, "leg", path, "routing")
            else:
                return build_oid(self.oid, "routing")
        elif type == "slot":
            return build_oid(self.oid, "leg", path[:-1], "routing", None, "slot", path[-1])
        else:
            return build_oid(self.oid, "leg", path)
        

    def generate_bridge_oid(self):
        bridge_oid = build_oid(self.oid, "bridge", self.bridge_count)
        self.bridge_count += 1
        
        return bridge_oid


    #def make_bridge(self, bridge_class):
    #    bridge = bridge_class(Weak(self))
    #    bridge.set_oid(self.generate_bridge_oid())
    #    return bridge
        
        
    def add_leg(self, leg, type, path):
        oid = self.generate_leg_oid(type, path)
        
        leg.set_oid(oid)
        leg.set_call(Weak(self), path)
        
        self.leg_oids.add(oid)
        self.ground.add_leg(leg)


    def remove_leg(self, leg):
        self.leg_oids.remove(leg.oid)
        self.ground.remove_leg(leg.oid)
        self.may_finish()
        

    def make_leg(self, type, path):
        leg = self.switch.make_leg(type)
        self.add_leg(leg, type, path)

        return leg


    def make_slot_leg(self, owner, li):
        slot_leg = SlotLeg(owner, li)
        self.add_leg(slot_leg, "slot", owner.path + [ li ])
        
        return slot_leg


    def link_and_start_legs(self, leg0, leg1):
        self.ground.link_legs(leg0.oid, leg1.oid)
        
        # Hm, this is used for slots only, so the order shouldn't matter, leg0
        # won't do anything for start.
        self.ground.start_leg(leg0.oid)
        self.ground.start_leg(leg1.oid)


    def leg_finished(self, leg):
        self.remove_leg(leg)
        
        
    def start(self, incoming_leg):
        self.add_leg(incoming_leg, None, [ 0 ])
        
        routing = self.make_leg("routing", [])
        
        self.ground.link_legs(incoming_leg.oid, routing.oid)
        self.ground.start_leg(routing.oid)
        self.ground.start_leg(incoming_leg.oid)


    def may_finish(self):
        if self.leg_oids:
            return
            
        self.logger.debug("Call is finished.")
        self.switch.call_finished(self)

        
    def allocate_media_address(self, channel_index):
        # TODO
        sid_affinity = None
        return self.switch.mgc.allocate_media_address(sid_affinity)
        
        
    def deallocate_media_address(self, addr):
        self.switch.mgc.deallocate_media_address(addr)
        

    def make_media_leg(self, lid, channel_index, type, **kwargs):
        # TODO
        sid_affinity = None
        ml = self.switch.mgc.make_media_leg(sid_affinity, type, **kwargs)
        ml.set_report_dirty(WeakMethod(self.ground.refresh_media, lid, channel_index))
        
        return ml


    #def forward(self, loid, action):
    #    return self.ground.forward(loid, action)
