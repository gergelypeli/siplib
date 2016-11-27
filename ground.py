from weakref import proxy

from util import Loggable


class Ground(Loggable):
    def __init__(self, switch, mgc):
        self.switch = switch
        self.mgc = mgc
        
        self.legs_by_oid = {}
        self.targets_by_source = {}
        self.media_contexts_by_span = {}
        self.context_count = 0
        
    
    def generate_context_oid(self):
        context_oid = self.oid.add("context", self.context_count)
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
            
        if not self.legs_by_oid:
            self.logger.info("Back to empty state.")
        
        
    def link_legs(self, leg_oid0, leg_oid1):
        if leg_oid0 not in self.legs_by_oid:
            raise Exception("First leg does not exist: %s!" % leg_oid0)

        if leg_oid1 not in self.legs_by_oid:
            raise Exception("Second leg does not exist: %s!" % leg_oid1)

        if leg_oid0 in self.targets_by_source:
            raise Exception("First leg already linked: %s!" % leg_oid0)

        if leg_oid1 in self.targets_by_source:
            raise Exception("Second leg already linked: %s!" % leg_oid1)
            
        self.logger.info("Linking %s to %s" % (leg_oid0, leg_oid1))
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
            self.logger.info("Must join media leg %s to %s" % (soid, toid))
            self.logger.info("Creating context %s" % coid)
        
            if smleg.sid != tmleg.sid:
                raise Exception("Sid mismatch!")

            mgw_sid = smleg.sid
            mc = self.mgc.make_media_leg("context")  # FIXME!
            mc.set_oid(coid)
            self.mgc.bind_media_leg(mc, mgw_sid)  # FIXME!
        
            self.media_contexts_by_span[span] = mc
            mc.set_leg_oids([ soid, toid ])
        else:
            ctx = self.media_contexts_by_span.pop(span, None)
            
            if not ctx:
                raise Exception("Hm, context does not exist, how can you be removed?")
                
            self.logger.info("Removing context %s" % ctx.oid)
            ctx.delete()


    def setup_leg(self, leg, oid):
        leg.set_oid(oid)
        leg.set_ground(proxy(self))
        
        self.add_leg(leg)
        

    def setup_party(self, party, base_oid, path, suffix):
        #party.set_call(proxy(self))
        # TODO: if we don't use suffix, this thing can mostly go into Party.set_path
        party.set_path(base_oid, path)
        oid = base_oid
        
        if path:
            oid = oid.add("party", path)
            
        if suffix:
            oid = oid.add(suffix)

        party.set_oid(oid)
        party.set_ground(proxy(self))


    def make_party(self, type):
        return self.switch.make_party(type)


    def select_hop_slot(self, next_uri):
        return self.switch.select_hop_slot(next_uri)


    def select_gateway_sid(self, ctype, mgw_affinity):
        return self.mgc.select_gateway_sid(ctype, mgw_affinity)
        
        
    def allocate_media_address(self, mgw_sid):
        return self.mgc.allocate_media_address(mgw_sid)
        
        
    def deallocate_media_address(self, addr):
        self.mgc.deallocate_media_address(addr)
    
    
    def make_media_leg(self, type):
        return self.mgc.make_media_leg(type)


    def bind_media_leg(self, ml, mgw_sid):
        return self.mgc.bind_media_leg(ml, mgw_sid)
