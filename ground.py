from weakref import proxy, WeakValueDictionary

from util import Loggable


class Ground(Loggable):
    def __init__(self, switch, mgc):
        Loggable.__init__(self)

        self.switch = switch
        self.mgc = mgc
        
        self.legs_by_oid = WeakValueDictionary()
        self.targets_by_source = {}
        self.media_contexts_by_span = {}
        self.context_count = 0
        self.parties_by_oid = {}
        
        
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
        

    def make_party(self, type, call_oid, path):
        party = self.switch.make_party(type)
        
        party.set_path(call_oid, path)
        
        pathstr = ",".join(str(x) for x in path) if path else None
        oid = call_oid.add(type, pathstr)
        party.set_oid(oid)

        self.parties_by_oid[oid] = party
        party.set_ground(proxy(self))
        party.finished_slot.plug(self.party_finished, oid=oid)
        
        return party


    def party_finished(self, oid):
        self.parties_by_oid.pop(oid)
            
        if not self.parties_by_oid:
            self.logger.info("Back to empty state.")
        

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
        elif not session["is_answer"]:
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
            elif "channels" not in session:  # rejected
                self.pending_party_session = None
            else:
                self.party_session = self.pending_party_session
                self.ground_session = session
                self.pending_party_session = None


    def set_party_session(self, session):
        if not session:
            raise Error("No party session specified!")
        elif not session["is_answer"]:
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
            elif "channels" not in session:  # rejected
                self.pending_ground_session = None
            else:
                self.ground_session = self.pending_ground_session
                self.party_session = session
                self.pending_ground_session = None
            

    def get_ground_offer(self):
        if self.pending_ground_session:
            return self.pending_ground_session
        else:
            raise Error("Ground offer not pending!")
        
        
    def get_party_offer(self):
        if self.pending_party_session:
            return self.pending_party_session
        else:
            raise Error("Party offer not pending!")
            
            
    def get_ground_answer(self):
        if self.pending_ground_session:
            raise Error("Ground offer is pending!")
        elif self.pending_party_session:
            raise Error("Party offer still pending!")
        elif not self.ground_session:
            raise Error("No ground answer yet!")
        elif not self.ground_session["is_answer"]:
            raise Error("Ground was not the answering one!")
        else:
            return self.ground_session


    def get_party_answer(self):
        if self.pending_ground_session:
            raise Error("Ground offer still pending!")
        elif self.pending_party_session:
            raise Error("Party offer is pending!")
        elif not self.party_session:
            raise Error("No party answer yet!")
        elif not self.party_session["is_answer"]:
            raise Error("Party was not the answering one!")
        else:
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
        for ci in range(len(self.media_legs)):
            self.set_media_leg(ci, None, None)

        self.logger.debug("Leg is finished.")
        self.ground.remove_leg(self.oid)


    def set_media_leg(self, channel_index, media_leg, mgw_sid):
        if channel_index > len(self.media_legs):
            raise Exception("Invalid media leg index!")
        elif channel_index == len(self.media_legs):
            self.media_legs.append(None)
            
        old = self.media_legs[channel_index]
        
        if old:
            self.logger.debug("Deleting media leg %s." % channel_index)
            self.ground.media_leg_changed(self.oid, channel_index, False)
            old.delete()
        
        self.media_legs[channel_index] = media_leg
        
        if media_leg:
            self.logger.debug("Adding media leg %s." % channel_index)
            media_leg.set_oid(self.oid.add("channel", channel_index))
            self.ground.bind_media_leg(media_leg, mgw_sid)
            self.ground.media_leg_changed(self.oid, channel_index, True)
        

    def get_media_leg(self, ci):
        return self.media_legs[ci] if ci < len(self.media_legs) else None


    def do(self, action):
        self.owner.do_slot(self.number, action)
