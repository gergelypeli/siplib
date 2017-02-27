from collections import namedtuple

from zap import Plug
from sdp import STATIC_PAYLOAD_TYPES, Session


AllocatedMedia = namedtuple("AllocatedMedia", "mgw_sid local_addr cached_params")

# Supposed to be a side class of an Endpoint
class SessionHelper:
    def __init__(self):
        self.allocated_media = []
        
        # These are not Loggable (yet)
        #self.sdp_builder = SdpBuilder()
        #self.sdp_parser = SdpParser()


    def media_thing_notified(self, type, params, mti):
        raise NotImplementedError()
                

    def allocate_local_media(self, local_session):
        # Must be called for outgoing offers and accepts (before realizing media)
        local_channels = local_session["channels"] if local_session else []
        
        channel_count = len(local_channels)
        allocated_count = len(self.allocated_media)
        
        # Allocate
        for i in range(allocated_count, channel_count):
            #self.logger.debug("Allocating local media address for channel %d (%s@%s)" % (i, ctype, mgw_affinity))
            mgw_sid = self.select_gateway_sid(local_channels[i])
            local_addr = self.ground.allocate_media_address(mgw_sid)
            
            allocated_media = AllocatedMedia(mgw_sid, local_addr, {})
            self.allocated_media.append(allocated_media)


    def deallocate_local_media(self, local_session):
        # Must be called for incoming rejects, and after realizing media
        local_channels = local_session["channels"] if local_session else []
        
        channel_count = len(local_channels)
        allocated_count = len(self.allocated_media)
                
        # Deallocate
        for i in reversed(range(channel_count, allocated_count)):
            am = self.allocated_media.pop()
            self.logger.debug("Deallocating local media address for channel %d" % i)
            self.ground.deallocate_media_address(am.local_addr)


    def add_local_info(self, local_session):
        # Must be called for outgoing offers and accepts
        local_channels = local_session["channels"] if local_session else []
        
        for i, local_channel in enumerate(local_channels):
            am = self.allocated_media[i]
            local_channel["rtp_local_addr"] = am.local_addr
            
            next_payload_type = 96
            used_payload_types = set()
            
            for f in local_channel["formats"]:
                format_info = (f["encoding"], f["clock"], f["encp"], f["fmtp"])
                rpt = f.get("rtp_remote_payload_type")  # the one from the other endpoint
                
                for spt, info in STATIC_PAYLOAD_TYPES.items():
                    if info == format_info:
                        pt = spt
                        break
                else:
                    if rpt and rpt not in used_payload_types:
                        pt = rpt
                    else:
                        while next_payload_type in used_payload_types:
                            next_payload_type += 1
                        
                        pt = next_payload_type
                        next_payload_type += 1
                        #raise Exception("Couldn't find payload type for %s!" % (encoding, clock))

                    used_payload_types.add(pt)

                f["rtp_local_payload_type"] = pt
            
        
    def flatten_formats(self, formats, pt_key):
        return { f[pt_key]: (f["encoding"], f["clock"], f["encp"], f["fmtp"]) for f in formats }


    def realize_local_media(self, local_session, remote_session):
        # Must be called for all accepts
        # Allocating resources must precede it, and deallocating them must follow
        local_channels = local_session["channels"] if local_session else []
        remote_channels = remote_session["channels"] if remote_session else []
        
        if len(local_channels) != len(remote_channels):
            raise Exception("Channel count mismatch!")
            
        channel_count = len(local_channels)
        media_channel_count = len(self.media_channels)

        # Create
        for i in range(media_channel_count, channel_count):
            self.logger.debug("Making media thing for channel %d" % i)
            am = self.allocated_media[i]
            
            mt = self.make_media_thing("rtp", am.mgw_sid)
            self.add_media_thing(i, "net", mt)
            self.link_media_things(i, None, 0, "net", 0)
            Plug(self.media_thing_notified, mti=i).attach(mt.event_slot)
            
        # Delete (currently can only happen on shutdown, not on session exchange)
        for i in reversed(range(channel_count, media_channel_count)):
            self.unlink_media_things(i, None, 0, "net", 0)
            self.remove_media_thing(i, "net")
            
        # Modify
        for i in range(channel_count):
            lc = local_channels[i]
            rc = remote_channels[i]
            mt = self.get_media_thing(i, "net")
            am = self.allocated_media[i]
            
            params = {
                'local_addr': lc["rtp_local_addr"],
                'remote_addr': rc["rtp_remote_addr"],
                'send_formats': self.flatten_formats(rc["formats"], "rtp_remote_payload_type"),
                'recv_formats': self.flatten_formats(lc["formats"], "rtp_local_payload_type")
            }
            
            diff = {}
            for key, value in params.items():
                if value != am.cached_params.get(key):
                    diff[key] = value
            
            if not diff:
                self.logger.debug("No need to modify media thing %d." % i)
            else:
                self.logger.debug("Modifying media thing %d: %s" % (i, diff))
                am.cached_params.update(diff)
                mt.modify(diff)
            
            
    def add_remote_info(self, remote_session):
        remote_channels = remote_session["channels"]
        
        for i in range(len(self.allocated_media)):
            remote_channels[i]["mgw_affinity"] = self.allocated_media[i].mgw_sid


    def process_remote_session(self, remote_session):
        local_session = self.legs[0].session_state.pending_ground_session  # get_ground_session()
        
        if remote_session.is_offer():
            self.add_remote_info(remote_session)
        elif remote_session.is_accept():
            self.add_remote_info(remote_session)
            self.realize_local_media(local_session, remote_session)
            self.deallocate_local_media(local_session)
        elif remote_session.is_reject():
            self.deallocate_local_media(local_session)
            
    
    def process_local_session(self, local_session):
        remote_session = self.legs[0].session_state.get_party_session()
        
        if local_session.is_offer():
            self.allocate_local_media(local_session)
            self.add_local_info(local_session)
        elif local_session.is_accept():
            self.allocate_local_media(local_session)
            self.add_local_info(local_session)
            self.realize_local_media(local_session, remote_session)
            self.deallocate_local_media(local_session)
        elif local_session.is_reject():
            pass


    def clear_local_media(self):
        self.logger.info("Clearing local media")
        
        fake_offer = Session.make_offer(channels=[])
        fake_accept = Session.make_accept(channels=[])
        
        self.realize_local_media(fake_accept, fake_offer)
        self.deallocate_local_media(fake_accept)
