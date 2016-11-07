from weakref import proxy

from mgc import MediaContext
from util import build_oid, Loggable
from leg import SlotLeg


class Ground(Loggable):
    def __init__(self, mgc):
        self.mgc = mgc
        
        self.legs_by_oid = {}
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
            
        self.logger.debug("Adding leg %s" % leg_oid)
        self.legs_by_oid[leg_oid] = leg
        
        return leg_oid
    

    def remove_leg(self, leg_oid):
        self.logger.debug("Removing leg %s" % leg_oid)
        self.legs_by_oid.pop(leg_oid)
        linked_leg_oid = self.targets_by_source.pop(leg_oid, None)
        
        if linked_leg_oid:
            self.targets_by_source.pop(linked_leg_oid, None)
        
        
    def link_legs(self, leg_oid0, leg_oid1):
        if leg_oid0 not in self.legs_by_oid:
            raise Exception("First leg does not exist: %s!" % leg_oid0)

        if leg_oid1 not in self.legs_by_oid:
            raise Exception("Second leg does not exist: %s!" % leg_oid1)

        if leg_oid0 in self.targets_by_source:
            raise Exception("First leg already linked: %s!" % leg_oid0)

        if leg_oid1 in self.targets_by_source:
            raise Exception("Second leg already linked: %s!" % leg_oid1)
            
        self.targets_by_source[leg_oid0] = leg_oid1
        self.targets_by_source[leg_oid1] = leg_oid0
        
        
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
        
        
    def insert_legs(self, my_oid, first_oid, second_oid, queued_actions=None):
        prev_oid = self.targets_by_source[my_oid]
        if not prev_oid:
            raise Exception("No previous leg before insert leg %s!" % my_oid)
            
        self.targets_by_source[prev_oid] = first_oid
        self.targets_by_source[first_oid] = prev_oid

        self.targets_by_source[second_oid] = my_oid
        self.targets_by_source[my_oid] = second_oid

        if queued_actions:
            for action in queued_actions:
                self.logger.debug("Forwarding queued action %s" % action["type"])
                self.legs_by_oid[first_oid].do(action)
        
        
    def forward(self, leg_oid, action):
        target = self.targets_by_source.get(leg_oid)
        
        if target:
            leg = self.legs_by_oid.get(target)
            
            if leg:
                self.logger.debug("Forwarding %s from %s to %s" % (action["type"], leg_oid, target))
                leg.do(action)
                return

        self.logger.error("Couldn't forward %s from %s!" % (action["type"], leg_oid))


    def media_leg_changed(self, slid, ci, is_added):
        # Called after adding media legs, or linking legs, and also
        # before removing media legs, or unlinking legs.
        # So we only need to do anything if the referred media leg has a pair.
        
        tlid = self.targets_by_source.get(slid)
        if not tlid:
            self.logger.debug("Changed media leg is not linked, ignoring.")
            return
            
        sleg = self.legs_by_oid[slid]
        tleg = self.legs_by_oid[tlid]
        
        smleg = sleg.get_media_leg(ci)  # Must be non-None, since it is added
        tmleg = tleg.get_media_leg(ci)
        
        # Without target changes in the source doesn't matter
        if not tmleg:
            self.logger.debug("Changed media leg has no linked pair, ignoring.")
            return

        soid = smleg.oid
        toid = tmleg.oid
        span = (soid, toid, ci) if soid < toid else (toid, soid, ci)
        
        if is_added:
            if span in self.media_contexts_by_span:
                raise Exception("Hm, context already exists, how can you be added?")

            smleg.refresh({})
            tmleg.refresh({})
            
            coid = self.generate_context_oid()
            self.logger.debug("Creating context %s" % coid)
        
            if smleg.sid != tmleg.sid:
                raise Exception("Sid mismatch!")

            sid_affinity = smleg.sid
        
            mc = MediaContext()
            mc.set_oid(coid)
            self.mgc.bind_thing(mc, sid_affinity)
        
            self.media_contexts_by_span[span] = mc
            mc.set_leg_oids([ soid, toid ])
        else:
            ctx = self.media_contexts_by_span.pop(span, None)
            
            if not ctx:
                raise Exception("Hm, context does not exist, how can you be removed?")
                
            self.logger.debug("Removing context %s" % ctx.oid)
            ctx.delete()


class Call(Loggable):
    def __init__(self, switch, ground):
        Loggable.__init__(self)

        self.switch = switch
        self.ground = ground
        self.leg_oids = set()
                
        
    def add_leg(self, leg):
        if not leg.oid:
            raise Exception("Leg has no oid!")
        
        self.leg_oids.add(leg.oid)
        self.ground.add_leg(leg)


    def remove_leg(self, leg):
        self.leg_oids.remove(leg.oid)
        self.ground.remove_leg(leg.oid)
        self.may_finish()
        

    def setup_thing(self, thing, path, suffix):
        thing.set_call(proxy(self), path)
        
        oid = self.oid
        
        if path:
            oid = build_oid(oid, "leg", path)
            
        if suffix:
            oid = build_oid(oid, suffix)
            
        thing.set_oid(oid)
        

    def make_thing(self, type, path, suffix):
        thing = self.switch.make_thing(type)
        
        self.setup_thing(thing, path, suffix)

        return thing


    def make_slot(self, owner, li):
        slot_leg = SlotLeg(owner, li)
        
        slot_leg.set_call(proxy(self), None)
        slot_leg.set_oid(build_oid(owner.oid, "slot", li))
        
        return slot_leg


    def link_leg_to_thing(self, leg, thing):
        leg = leg.stand()
        thing_leg = thing.stand()
        
        self.logger.debug("Linking %s to %s" % (leg.oid, thing_leg.oid))
        self.ground.link_legs(leg.oid, thing_leg.oid)

        thing.start()


    def leg_finished(self, leg):
        self.remove_leg(leg)
        
        
    def start(self, incoming_leg):
        self.setup_thing(incoming_leg, [ 0 ], None)
        
        routing = self.make_thing("routing", [], "reception")
        self.link_leg_to_thing(incoming_leg, routing)

        incoming_leg.start()


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
        

    def media_leg_changed(self, leg_index, channel_index, is_added):
        if is_added:
            leg = self.ground.legs_by_oid[leg_index]
            ml = leg.get_media_leg(channel_index)
            sid_affinity = None  # TODO
            self.switch.mgc.bind_thing(ml, sid_affinity)
            
        self.ground.media_leg_changed(leg_index, channel_index, is_added)


    def forward(self, leg, action):
        self.ground.forward(leg.oid, action)
        
        
    def collapse_legs(self, leg0, leg1, queued_actions=None):
        self.ground.collapse_legs(leg0.oid, leg1.oid, queued_actions)
