from weakref import proxy, WeakSet

from format import Uri, Nameaddr, Status, AbsoluteUri, CallInfo, Reason
from party import PlannedEndpoint, Bridge
from sdp import Session
from subscript import SubscriptionManager, MessageSummaryEventSource, DialogEventSource
from log import Loggable
from mgc import Controller  #, PlayerMediaLeg  #, EchoMediaLeg
import zap
import resolver


TEST_ENDPOINT_SESSION = dict(
    attributes=[],
    bandwidth=None,
    channels=[ {
        'type': 'audio',
        'proto': 'RTP/AVP',
        'send': True,
        'recv': True,
        'attributes': [('ptime', '20')],
        'formats': [
            None  # To be filled in
        , {
            'clock': 8000, 'encoding': 'telephone-event', 'encp': 1, 'fmtp': '0-15'
        } ]
    } ]
)


class TestEndpoint(PlannedEndpoint):
    def identify(self, params):
        self.media_filename = params.get("media_filename", "playback.wav")
        self.media_format = params.get("media_format", ("PCMA", 8000, 1, None))
        
        return None
        
        
    def wait_this_action(self, action_type):
        action = yield from self.wait_action()
        assert action and action["type"] == action_type
        return action
        
        
    def add_media_leg(self, channel):
        ctype = channel["type"]
        mgw_affinity = channel.get("mgw_affinity")
        mgw_sid = self.ground.select_gateway_sid(ctype, mgw_affinity)

        channel["mgw_affinity"] = mgw_sid
        media_leg = self.make_media_leg("player")
        media_leg.set_mgw(mgw_sid)
        self.leg.add_media_leg(media_leg)
    

    def update_session(self, session):
        if session.is_query():
            self.logger.info("TestEndpoint received a session query.")

            offer = Session.make_offer(TEST_ENDPOINT_SESSION)
            encoding, clock, encp, fmtp = format
        
            offer["channels"][0]["formats"][0] = dict(encoding=encoding, clock=clock, encp=encp, fmtp=fmtp)
            return offer
            
        elif session.is_offer():
            self.logger.info("TestEndpoint received a session offer.")
            
            answer = session.flipped()
            encoding = self.media_format[0]
            
            for c in answer["channels"]:
                c["formats"] = [ f for f in c["formats"] if f["encoding"] in (encoding, "telephone-event") ]
                c["send"], c["recv"] = c["recv"], c["send"]

            if not self.leg.media_legs:
                self.add_media_leg(answer["channels"][0])
        
            return answer
        elif session.is_accept():
            self.logger.info("TestEndpoint received a session accept.")
            
            if not self.leg.media_legs:
                self.add_media_leg(session["channels"][0])
                
            return None
        elif session.is_reject():
            self.logger.info("TestEndpoint received a session reject.")
            
            raise Exception("Eiii...")
        
        
    def idle(self):
        ml = self.leg.get_media_leg(0)
        
        if ml:
            ml.play(self.media_filename, self.media_format, volume=0.1, fade=3)
        else:
            self.logger.error("No media leg was created!")

        while True:
            action = yield from self.wait_action()
            type = action["type"]
            self.logger.info("Got a %s while idle." % type)
            
            if type == "session":
                session = self.update_session(action["session"])
                if session:
                    self.forward(dict(type="session", session=session))
            elif type == "tone":
                self.logger.info("Okay, got a tone %s, fading out..." % action["name"])
                break
            elif type == "hangup":
                self.logger.info("Okay, got a hangup, fading out...")
                break
            elif type == "transfer":
                self.logger.info("Got a transfer, being transferred...")
                self.process_transfer(action)
            else:
                self.logger.critical("Don't know what to do with %s, continuing..." % type)
        
        if self.leg.media_legs:
            self.leg.media_legs[0].play(volume=0, fade=3)
            yield from self.sleep(3)
            
        if type != "hangup":
            self.forward(dict(type="hangup"))


class UnreachableEndpoint(TestEndpoint):
    def plan(self):
        self.logger.debug("Unreachable endpoint created.")
        
        yield from self.wait_this_action("dial")
        self.logger.debug("Got dial, but not answering anything.")

        yield from self.wait_this_action("hangup")
        self.logger.debug("And now the caller hung up.")


class RejectingEndpoint(TestEndpoint):
    def plan(self):
        self.logger.debug("Rejecting endpoint created.")
        
        yield from self.wait_this_action("dial")
        self.logger.debug("Got dial, but not now rejecting it.")

        self.forward(dict(type="reject", status=Status(603)))
        self.logger.debug("And now done.")


class RingingEndpoint(TestEndpoint):
    def plan(self):
        self.logger.debug("Ringing endpoint created.")
        
        yield from self.wait_this_action("dial")
        self.logger.debug("Got dial, now ringing forever.")

        self.forward(dict(type="ring"))

        yield from self.wait_this_action("hangup")
        self.logger.debug("And now the caller hung up.")


class ReRingingEndpoint(TestEndpoint):
    def plan(self):
        self.logger.debug("ReRinging endpoint created.")
        
        action = yield from self.wait_this_action("dial")
        
        offer = action.get("session")
        answer = self.update_session(offer)

        self.logger.info("Ringing normally.")
        self.forward(dict(type="ring", session=answer))
        yield from self.sleep(1)

        self.logger.info("Accepting call.")
        self.forward(dict(type="accept"))
        yield from self.sleep(3)
        
        self.logger.info("Reringing.")
        self.forward(dict(type="ring"))
        yield from self.sleep(3)

        self.logger.info("Rerejecting.")
        self.forward(dict(type="reject"))
        
        self.logger.debug("And now we rerejected the caller.")


class CalleeEndpoint(TestEndpoint):
    def plan(self):
        self.logger.debug("Callee created.")
        
        action = yield from self.wait_this_action("dial")
        offer = action.get("session")
        self.logger.debug("Got offer:\n%s" % repr(offer))
        
        yield from self.sleep(1)
        answer = self.update_session(offer)
        self.forward(dict(type="ring", session=answer))
        
        # Force an UPDATE
        yield from self.sleep(1)
        offer = answer.flipped()
        self.forward(dict(type="session", session=offer))

        action = yield from self.wait_this_action("session")
        answer = action.get("session")
        self.logger.debug("Got answer:\n%s" % repr(answer))

        yield from self.sleep(1)
        self.forward(dict(type="accept"))

        yield from self.idle()
        
        self.logger.debug("Callee done.")


class CallerEndpoint(TestEndpoint):
    def identify(self, dst):
        self.sip_from = dst["from"]
        self.sip_to = dst["to"]

        return TestEndpoint.identify(self, dst)


    def plan(self):
        self.logger.debug("Caller created")
        
        #address = yield from resolver.wait_resolve("index.hu")
        #self.logger.debug("Resolved index.hu asynchronously to: %s" % address)
        
        yield from self.wait_input("Press any key to dial out!")
        
        src = {
            'type': "sip",  # FIXME: we lie here for mkctx
            'from': self.sip_from,
            'to': self.sip_to
        }
        
        offer = None
        action = dict(
            type="dial",
            call_info=self.get_call_info(),
            src=src,
            ctx={},
            session=offer
        )
        self.forward(action)

        while True:
            action = yield from self.wait_action()
        
            if action["type"] == "reject":
                self.logger.error("Oops, we got rejected!")
                return
            elif action["type"] == "accept":
                self.logger.info("Yay, accepted!")
                session = action.get("session")
                
                if session:
                    answer = self.update_session(session)
                    
                    if answer:
                        self.forward(dict(type="session", session=answer))
                    
                break
            elif action["type"] == "session":
                session = action.get("session")
                
                if session.is_offer():
                    # This is the offer from an RPR, we'll answer in a PRACK request
                    offer = session
                    answer = self.update_session(offer)
                    self.forward(dict(type="session", session=answer))
                    
                    # Then send another offer to force an UPDATE
                    offer = answer.flipped()
                    self.forward(dict(type="session", session=offer))
                else:
                    # This is the answer from the UPDATE response, no need to process it
                    pass
            else:
                self.logger.error("Hm, we got: %s!" % action["type"])
            
        # Be more flexible here!
        yield from self.idle()

        self.logger.debug("Caller done.")


class VoicemailEventSource(MessageSummaryEventSource):
    def identify(self, params):
        self.mailbox = params["mailbox"]

        self.state = 0
        zap.time_slot(30, True).plug(self.fake)
        
        return self.mailbox
        
        
    def fake(self):
        self.notify_all()
        self.state += 1

        
    def get_message_state(self):
        return dict(voice=self.state % 2, voice_old=self.state / 2 % 2)


class BusylampEventSource(DialogEventSource):
    def identify(self, params):
        self.set_entity(params["entity"])

        self.calls_by_number = {}
        self.state = 0

        return self.entity
        
        
    def update(self, call_number, is_outgoing, is_confirmed):
        if is_confirmed is None:
            self.calls_by_number.pop(call_number)
        else:
            self.calls_by_number[call_number] = dict(is_outgoing=is_outgoing, is_confirmed=is_confirmed)
            
        self.logger.info("%s: %s" % (self.entity, self.calls_by_number))
        
        self.notify_all()
            
        
    def fake(self):
        self.notify_all()
        self.state += 1
        
        
    def get_dialog_state(self):
        return self.calls_by_number
            

class TestSubscriptionManager(SubscriptionManager):
    def __init__(self, switch, voicemail_number):
        SubscriptionManager.__init__(self, switch)
        
        self.voicemail_number = voicemail_number
        
        
    def identify_event_source(self, request):
        event = request["event"]
        from_uri = request["from"].uri
        to_uri = request["to"].uri
        
        if event == "message-summary" and to_uri.username == self.voicemail_number:
            return "voicemail", from_uri.username
            
        if event == "dialog":
            return "busylamp", to_uri.username
            
        return None
        
        
    def make_event_source(self, type):
        if type == "voicemail":
            return VoicemailEventSource()
        elif type == "busylamp":
            return BusylampEventSource()
        else:
            return None


class TestController(Controller):
    def start(self, addr_set):
        # Must use IP4 addresses or FQDN in SDP, so let's go with IP4
        # Media host may be different from the signaling host
        self.media_addresses = addr_set
        
    
    def allocate_media_address(self, mgw_sid):
        return self.media_addresses.pop()
    
    
    def deallocate_media_address(self, addr):
        self.media_addresses.add(addr)
        
        
class TestLine(Bridge):
    def __init__(self, manager):
        Bridge.__init__(self)
        
        self.manager = manager
        

    def identify(self, dst):
        self.addr = dst["addr"]
        self.username = dst["username"]
        self.is_outgoing = dst.get("is_outgoing", False)  # From the device's perspective
        self.is_confirmed = False
        self.is_unconfirmed = False  # fell back to ringing after a blind transfer
        self.ctx = {}
        
        self.manager.register(self.username, self)
        
        return "%s@%s" % (self.username, self.addr)
        
        
    def update_busylamp_state(self):
        es = self.ground.switch.get_busylamp(self.username)
        
        if es:
            es.update(self.call_info["number"], self.is_outgoing, self.is_confirmed)
        
        
    def process_dial(self, action):
        ctx = action["ctx"]
        self.ctx.update(ctx)
        
        src_username = ctx["src_username"]
        src_addr = ctx["src_addr"]
        src_name = ctx["src_name"]
        
        dst_username = ctx["dst_username"]
        dst_addr = ctx["dst_addr"]

        self.update_busylamp_state()
        
        if dst_addr != self.addr or dst_username != self.username:
            self.dial(action)
            return
            
        record_uri = Uri(dst_addr, dst_username)
        contacts = self.ground.switch.registrar.lookup_contacts(record_uri)
        
        if not contacts:
            self.logger.debug("Record %s has no SIP contacts, rejecting!" % (record_uri,))
            self.reject_incoming_leg(Status(404))
            
        self.logger.debug("Record %s has %d SIP contacts." % (record_uri, len(contacts)))
        sip_from = Nameaddr(Uri(src_addr, src_username), src_name)
        sip_to = Nameaddr(Uri(dst_addr, dst_username))
        
        for contact in contacts:
            dst = {
                'type': "sip",
                'uri': contact.uri,
                'hop': contact.hop,
                'from': sip_from,
                'to':  sip_to,
                'route': [],
                'x_alert_info': [ CallInfo(AbsoluteUri("http", "//www.notused.com"), dict(info="alert-autoanswer", delay="0")) ]
            }
        
            self.dial(action, **dst)
            
            
    def process_leg_transfer(self, li, action):
        if not self.is_outgoing and li > 0:
            self.forward_leg(0, action)
        elif self.is_outgoing and li == 0:
            self.forward_leg(max(self.legs.keys()), action)
        else:
            Bridge.process_leg_transfer(self, li, action)
            
            
    def forward_leg(self, li, action):
        #self.logger.info("Line forward_leg %d: %s" % (li, action["type"]))
        
        session = action.get("session")
        if session:
            self.legs[li].session_state.set_party_session(session)

        Bridge.forward_leg(self, li, action)
        
            
    def do_slot(self, li, action):
        session = action.get("session")
        if session:
            self.legs[li].session_state.set_ground_session(session)
        
        type = action["type"]
        
        if type == "accept":
            self.is_confirmed = True
            self.update_busylamp_state()
        elif type in ("reject", "hangup"):
            self.is_confirmed = None
            self.update_busylamp_state()
        
        to_me = (self.is_outgoing and li > 0) or (not self.is_outgoing and li == 0)
        
        if to_me:
            my_li = max(self.legs.keys()) if li == 0 else 0
            my_leg = self.legs[my_li]
        
            if self.is_confirmed and not self.is_unconfirmed and action["type"] == "ring":
                self.logger.info("Call unconfirmed, playing artificial ringing tone.")
                self.is_unconfirmed = True
            
                media_leg = my_leg.get_media_leg(0)
            
                if not media_leg:
                    party_session = my_leg.session_state.get_party_session()
                    channel = party_session["channels"][0]
            
                    ctype = channel["type"]
                    mgw_affinity = channel.get("mgw_affinity")
                    mgw_sid = self.ground.select_gateway_sid(ctype, mgw_affinity)

                    channel["mgw_affinity"] = mgw_sid
                    media_leg = self.make_media_leg("player")
                    media_leg.set_mgw(mgw_sid)
                    my_leg.add_media_leg(media_leg)
                
                media_leg.play("ringtone.wav", ("PCMA", 8000, 1, None), volume=0.1)
            
                # Don't forward this ring
                return
            
            if self.is_unconfirmed and action["type"] == "accept":
                self.logger.info("Call reconfirmed, stopping artificial ringing tone.")
                self.is_unconfirmed = False
                my_leg.remove_media_leg()
            
                # Don't forward this accept
                return
            
            if self.is_unconfirmed and action["type"] == "reject":
                self.logger.info("Call rerejected, hanging up instead.")
                self.is_unconfirmed = False
                my_leg.remove_media_leg()
            
                status = action.get("status")
                action = dict(type="hangup")
            
                if status:
                    action["reason"] = Reason("SIP", dict(cause=status.code, text=status.reason))
                
                # Do forward this hangup
            
        return Bridge.do_slot(self, li, action)
        
        
    def call_pickup(self, dst_uri):
        if self.is_outgoing or self.is_confirmed:
            self.logger.warning("Call pickup is not possible right now!")
            return False
            
        self.logger.info("Call pickup to %s." % (dst_uri,))
        
        tid = self.ground.make_transfer("blind")  # TODO: pickup?
        src = {
            'type': "sip",
            'from': Nameaddr(Uri(self.ctx["src_addr"], self.ctx["src_username"]), self.ctx["src_name"]),
            'to': Nameaddr(dst_uri)
        }

        # FIXME: A session query was necessary here because the caller was Caller,
        # and sent a query itself. This can get messy, implement properly!
        action = dict(type="transfer", transfer_id=tid, call_info=self.call_info, ctx={}, src=src, session=Session.make_query())
        self.forward_leg(0, action)
        return True
        
        
class TestLineManager(Loggable):
    def __init__(self):
        Loggable.__init__(self)
        
        self.line_sets_by_username = {}
        
        
    def make_line(self):
        return TestLine(proxy(self))
        
        
    def register(self, username, line):
        self.logger.info("Registering line %s" % username)
        self.line_sets_by_username.setdefault(username, WeakSet()).add(line)
        
        
    def call_pickup(self, username, dst_uri):
        s = self.line_sets_by_username.get(username)
        
        if s:
            for line in s:
                ok = line.call_pickup(dst_uri)
                
                if ok:
                    return True
        
        self.logger.error("Didn't find any call for pickup on line %s!" % username)
        return False
