from weakref import proxy, WeakValueDictionary

from util import Loggable


# FIXME: currently we can't tell how many channels do we have to deal with
GUESSED_CHANNEL_COUNT = 1


class Ground(Loggable):
    def __init__(self, switch, mgc):
        Loggable.__init__(self)

        self.switch = switch
        self.mgc = mgc
        
        self.legs_by_oid = WeakValueDictionary()
        self.targets_by_source = {}  # TODO: rename!
        self.media_contexts_by_mlid = {}
        self.context_count = 0
        self.parties_by_oid = {}
        self.leg_oids_by_anchor = {}
        
        
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
            self.targets_by_source.pop(linked_leg_oid)
            
        # And clean up the anchor infos, too
        anchored_leg_oid = self.leg_oids_by_anchor.pop(leg_oid, None)
        if anchored_leg_oid:
            self.leg_oids_by_anchor.pop(anchored_leg_oid)
        
        
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
        
        # Create new contexts if necessary
        for ci in range(GUESSED_CHANNEL_COUNT):
            smleg = self.find_facing_media_leg(leg_oid1, ci)
            tmleg = self.find_facing_media_leg(leg_oid0, ci)
            
            if smleg and tmleg:
                self.logger.info("Media legs became facing after linking, must add context.")
                self.add_context(smleg, tmleg)
    
    
    def unlink_legs(self, leg_oid0):
        if leg_oid0 not in self.legs_by_oid:
            raise Exception("Leg does not exist: %s!" % leg_oid0)
            
        leg_oid1 = self.targets_by_source.get(leg_oid0)
        if not leg_oid1:
            raise Exception("Leg not linked: %s!" % leg_oid0)
            
        # Remove unlucky contexts
        for ci in range(GUESSED_CHANNEL_COUNT):
            smleg = self.find_facing_media_leg(leg_oid1, ci)
            tmleg = self.find_facing_media_leg(leg_oid0, ci)
            
            if smleg and tmleg:
                self.logger.info("Media legs became separated after unlinking, must remove context.")
                self.remove_context(smleg, tmleg)
            
        self.targets_by_source.pop(leg_oid0)
        self.targets_by_source.pop(leg_oid1)


    def collapse_legs(self, leg_oid0, leg_oid1, queue0=None, queue1=None):
        prev_leg_oid = self.targets_by_source[leg_oid0]
        if not prev_leg_oid:
            raise Exception("No previous leg before collapsed leg %s!" % leg_oid0)
            
        next_leg_oid = self.targets_by_source[leg_oid1]
        if not next_leg_oid:
            raise Exception("No next leg after collapsed leg %s!" % leg_oid1)
        
        self.logger.info("Collapsing legs.")
        
        # Do this explicitly, because breaking it down to two unlinks and a link
        # may unnecessarily remove and recreate contexts.
        
        for ci in range(GUESSED_CHANNEL_COUNT):
            # Source, Target, Previous, Next
            smleg = self.legs_by_oid[leg_oid0].get_media_leg(ci)
            tmleg = self.legs_by_oid[leg_oid1].get_media_leg(ci)
            
            if not smleg and not tmleg:
                self.logger.info("Channel %d is unaffected." % ci)
                continue
            
            self.logger.info("Channel %d needs context changes." % ci)
            pmleg = self.find_facing_media_leg(leg_oid0, ci)
            nmleg = self.find_facing_media_leg(leg_oid1, ci)
            
            # Remove previous context if necessary
            if pmleg and smleg:
                self.remove_context(pmleg, smleg)
                
            # Remove next context if necessary
            if tmleg and nmleg:
                self.remove_context(tmleg, nmleg)
            
            # Create collapsed context if necessary
            if pmleg and nmleg:
                self.add_context(pmleg, nmleg)
        
        self.targets_by_source.pop(leg_oid0)
        self.targets_by_source.pop(leg_oid1)
        self.targets_by_source[prev_leg_oid] = next_leg_oid
        self.targets_by_source[next_leg_oid] = prev_leg_oid
        
        #self.unlink_legs(prev_leg_oid)
        #self.unlink_legs(next_leg_oid)
        #self.link_legs(prev_leg_oid, next_leg_oid)

        if queue0:
            for action in queue0:
                self.logger.debug("Forwarding queued action to previous leg: %s" % action["type"])
                self.legs_by_oid[prev_leg_oid].do(action)

        if queue1:
            for action in queue1:
                self.logger.debug("Forwarding queued action to next leg: %s" % action["type"])
                self.legs_by_oid[next_leg_oid].do(action)

        
    def forward(self, leg_oid, action):
        target = self.targets_by_source.get(leg_oid)
        
        if target:
            leg = self.legs_by_oid[target]
            self.logger.debug("Forwarding %s from %s to %s" % (action["type"], leg_oid, target))
            leg.do(action)
            return
        elif action["type"] == "dial":
            dst = action.pop("dst", None)
            call_info = action.get("call_info")
            
            if not call_info:
                raise Exception("Missing call_info from dial action!")
            
            type = dst.pop("type") if dst else "routing"
            party = self.make_party(type, dst, call_info)

            party_leg = party.start()
            self.link_legs(leg_oid, party_leg.oid)
            party_leg.do(action)
        else:
            self.logger.error("Couldn't forward %s from %s!" % (action["type"], leg_oid))


    def legs_anchored(self, leg_oid0, leg_oid1):
        if leg_oid0 in self.leg_oids_by_anchor:
            raise Exception("Leg already anchored: %s" % leg_oid0)
            
        if leg_oid1 in self.leg_oids_by_anchor:
            raise Exception("Leg already anchored: %s" % leg_oid1)
            
        self.leg_oids_by_anchor[leg_oid0] = leg_oid1
        self.leg_oids_by_anchor[leg_oid1] = leg_oid0


    def legs_unanchored(self, leg_oid0, leg_oid1):
        x = self.leg_oids_by_anchor.pop(leg_oid0, None)
        if x != leg_oid1:
            raise Exception("Leg %s was not anchored to %s!" % (leg_oid0, leg_oid1))

        y = self.leg_oids_by_anchor.pop(leg_oid1, None)
        if y != leg_oid0:
            raise Exception("Leg %s was not anchored to %s!" % (leg_oid1, leg_oid0))
        
        
    def find_facing_media_leg(self, lid, ci):
        # Opposite facing channel, may be our pair
        plid = self.targets_by_source.get(lid)
        if not plid:
            return None
                
        pleg = self.legs_by_oid[plid]
        pml = pleg.get_media_leg(ci)
            
        if pml:
            return pml
            
        return self.find_similar_media_leg(plid, ci)
        
        
    def find_similar_media_leg(self, plid, ci):
        # Same facing channel, may shadow us
        lid = self.leg_oids_by_anchor.get(plid)
        if not lid:
            return None
                
        leg = self.legs_by_oid[lid]
        ml = leg.get_media_leg(ci)
            
        if ml:
            return None
            
        return self.find_facing_media_leg(lid, ci)
            
            
    def remove_context(self, smleg, tmleg):
        smc = self.media_contexts_by_mlid.pop(smleg.oid)
        tmc = self.media_contexts_by_mlid.pop(tmleg.oid)
        
        if smc != tmc:
            raise Exception("Media legs were unexpectedly not in the same context!")
            
        self.logger.info("Removing context %s" % smc.oid)
        smc.delete()

            
    def add_context(self, smleg, tmleg):
        self.logger.info("Must link media leg %s to %s" % (smleg.oid, tmleg.oid))

        # Make sure the media legs are already realized
        smleg.create()
        tmleg.create()
        
        if smleg.sid != tmleg.sid:
            raise Exception("Sid mismatch!")  # FIXME: this will happen eventually

        mgw_sid = smleg.sid

        mcid = self.generate_context_oid()
        self.logger.info("Creating context %s" % mcid)
    
        mc = self.mgc.make_media_leg("context")  # FIXME!
        mc.set_oid(mcid)
        mc.set_mgw(mgw_sid)
        mc.modify({ 'legs': [ smleg.label, tmleg.label ] })
        
        self.media_contexts_by_mlid[smleg.oid] = mc
        self.media_contexts_by_mlid[tmleg.oid] = mc
        
        
    def media_leg_appeared(self, slid, ci):
        smleg = self.legs_by_oid[slid].get_media_leg(ci)
        tmleg = self.find_facing_media_leg(slid, ci)
        rmleg = self.find_similar_media_leg(slid, ci)

        if not tmleg:
            self.logger.debug("Appeared media leg has no facing pair, no context to add.")
            return
        
        if rmleg:
            self.logger.debug("Appeared media leg shadows a similar leg, must remove context.")
            self.remove_context(rmleg, tmleg)

        self.logger.info("Appeared media leg has a facing pair, must add context.")
        self.add_context(smleg, tmleg)
        
        
    def media_leg_disappearing(self, slid, ci):
        smleg = self.legs_by_oid[slid].get_media_leg(ci)
        tmleg = self.find_facing_media_leg(slid, ci)
        rmleg = self.find_similar_media_leg(slid, ci)

        if not tmleg:
            self.logger.debug("Disappearing media leg had no facing pair, no context to remove.")
            return
        
        self.logger.info("Disappearing media leg had a facing pair, must remove context.")
        self.remove_context(smleg, tmleg)
        
        if rmleg:
            self.logger.debug("Disappearing media leg shadowed a similar leg, must add context.")
            self.add_context(rmleg, tmleg)
        

    def make_leg(self, party, li):
        leg = Leg(party, li)
        leg.set_oid(party.oid.add("leg", li))
        leg.set_ground(proxy(self))
        self.add_leg(leg)
        
        return leg
        
        
    def make_party(self, type, params, call_info):
        party = self.switch.make_party(type)
        party.set_call_info(call_info)
        identity = party.identify(params)
        oid = call_info["oid"].add(type, identity)
        self.logger.info("Made party %s" % oid)
        
        #party.set_path(call_oid, path)
        
        #pathstr = ".".join(str(x) for x in path) if path else None
        #oid = call_oid.add(type, pathstr)
        
        if oid in self.parties_by_oid:
            raise Exception("Duplicate party oid: %s" % oid)
            
        party.set_oid(oid)
        self.parties_by_oid[oid] = party
        
        party.set_ground(proxy(self))
        party.finished_slot.plug(self.party_finished, oid=oid)
        
        return party


    def party_finished(self, oid):
        self.parties_by_oid.pop(oid)
            
        if not self.parties_by_oid:
            self.logger.info("No more parties left.")
        

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




class GroundDweller(Loggable):
    def __init__(self):
        Loggable.__init__(self)
        
        self.ground = None
        
    
    def set_ground(self, ground):
        if not ground:
            raise Exception("No ground!")
            
        self.ground = ground




class Error(Exception):
    pass


class SessionState:
    def __init__(self):
        self.ground_session = None
        self.party_session = None
        self.pending_ground_session = None
        self.pending_party_session = None
        
        
    def set_ground_session(self, session):
        if not session:
            raise Error("No ground session specified!")
        elif session.is_query():
            return
        elif session.is_offer():
            # Offer
            
            if self.pending_ground_session:
                raise Error("Ground offer already pending!")
            elif self.pending_party_session:
                raise Error("Party offer also pending!")
            else:
                self.pending_ground_session = session
        else:
            # Answer

            if not self.pending_party_session:
                raise Error("Party offer not pending!")
            elif session.is_reject():
                self.pending_party_session = None
            else:
                self.party_session = self.pending_party_session
                self.ground_session = session
                self.pending_party_session = None


    def set_party_session(self, session):
        if not session:
            raise Error("No party session specified!")
        elif session.is_query():
            return
        elif session.is_offer():
            # Offer
            
            if self.pending_ground_session:
                raise Error("Ground offer also pending!")
            elif self.pending_party_session:
                raise Error("Party offer already pending!")
            else:
                self.pending_party_session = session
        else:
            # Answer
            
            if not self.pending_ground_session:
                raise Error("Ground offer not pending!")
            elif session.is_reject():
                self.pending_ground_session = None
            else:
                self.ground_session = self.pending_ground_session
                self.party_session = session
                self.pending_ground_session = None
            

    def get_ground_session(self):
        return self.ground_session
        
        
    def get_party_session(self):
        return self.party_session
        
        
class Leg(GroundDweller):
    def __init__(self, owner, number):
        GroundDweller.__init__(self)

        self.owner = owner
        self.number = number
        
        self.media_legs = []
        self.session_state = SessionState()


    def forward(self, action):
        self.ground.forward(self.oid, action)
        
        
    def may_finish(self):
        while self.media_legs:
            self.remove_media_leg()

        self.logger.debug("Leg is finished.")
        self.ground.remove_leg(self.oid)


    def add_media_leg(self, media_leg):
        ci = len(self.media_legs)
        self.logger.debug("Adding %s media leg %s." % (type, ci))

        self.media_legs.append(media_leg)
        media_leg.set_oid(self.oid.add("channel", ci))
        
        # By this the media leg must be added, and set up, so in case Ground
        # wants to immediately put it in a context, it will work.
        self.ground.media_leg_appeared(self.oid, ci)
        
        return ci


    def remove_media_leg(self):
        ci = len(self.media_legs) - 1
        self.logger.debug("Deleting media leg %s." % ci)
        
        # Must call this when everything is still in place
        self.ground.media_leg_disappearing(self.oid, ci)
        old = self.media_legs.pop()
        old.delete()
        
        return ci
        

    def get_media_leg(self, ci):
        return self.media_legs[ci] if ci < len(self.media_legs) else None


    def get_media_leg_count(self):
        return len(self.media_legs)


    def do(self, action):
        self.owner.do_slot(self.number, action)
