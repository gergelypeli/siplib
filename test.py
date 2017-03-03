from weakref import proxy, WeakSet
import datetime

from format import Uri, Nameaddr, Status, AbsoluteUri, CallInfo
from party import PlannedEndpoint, Bridge
from sdp import Session
from subscript import SubscriptionManager, EventSource, MessageSummaryFormatter, DialogFormatter, PresenceFormatter
from log import Loggable
from mgc import Controller  #, PlayerMediaLeg  #, EchoMediaLeg
from zap import Plug
#import resolver


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
        
        
    def add_media(self, channel):
        mgw_sid = self.select_gateway_sid(channel)
        
        media_thing = self.make_media_thing("player", mgw_sid)
        self.add_media_thing(0, "gen", media_thing)
        self.link_media_things(0, None, 0, "gen", 0)
    

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

            if not self.media_channels:
                self.add_media(answer["channels"][0])
        
            return answer
        elif session.is_accept():
            self.logger.info("TestEndpoint received a session accept.")
            
            if not self.media_channels:
                self.add_media(session["channels"][0])
                
            return None
        elif session.is_reject():
            self.logger.info("TestEndpoint received a session reject.")
            
            raise Exception("Eiii...")


    def idle(self, in_call):
        mt = self.get_media_thing(0, "gen")
        
        if mt and in_call:
            self.logger.info("Idle, fading in media.")
            mt.play(self.media_filename, self.media_format, volume=0.1, fade=3)
        else:
            self.logger.info("Idle, no media to fade in.")

        while True:
            action = yield from self.wait_action()
            type = action["type"]
            self.logger.info("Got a %s while idle." % type)
            
            if type == "session":
                session = self.update_session(action["session"])
                if session:
                    self.forward(dict(type="session", session=session))
            elif type == "tone" and in_call:
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
        
        mt = self.get_media_thing(0, "gen")
        
        if mt and in_call:
            self.logger.info("Fading out media.")
            mt.play(volume=0, fade=3)
            yield from self.sleep(3)
        else:
            self.logger.info("No media to fade out.")
            
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

        self.forward(dict(type="reject", status=Status.DECLINE))
        self.logger.debug("And now done.")


class RingingEndpoint(TestEndpoint):
    def plan(self):
        self.logger.debug("Ringing endpoint created.")
        
        action = yield from self.wait_this_action("dial")
        offer = action.get("session")
        self.logger.debug("Got dial with offer:\n%s" % repr(offer))

        answer = self.update_session(offer)
        self.forward(dict(type="ring", session=answer))
        self.logger.debug("Now ringing forever.")

        yield from self.idle(False)
        self.logger.debug("And now the caller hung up.")


class BlindTransferringEndpoint(TestEndpoint):
    def identify(self, dst):
        self.referred_by = dst["referred_by"]  # Nameaddr
        self.refer_to = dst["refer_to"]  # Nameaddr
        
        TestEndpoint.identify(self, dst)
        
        return self.referred_by.uri.username
        
        
    def plan(self):
        self.logger.debug("Blind transferring endpoint created.")
        
        action = yield from self.wait_this_action("dial")
        
        offer = action.get("session")
        answer = self.update_session(offer)

        self.logger.info("Ringing a bit.")
        self.forward(dict(type="ring", session=answer))
        yield from self.sleep(1)

        self.logger.info("Accepting call.")
        self.forward(dict(type="accept"))
        yield from self.sleep(3)
        
        self.logger.info("Transferring to %s." % (self.refer_to,))
        tid = self.ground.make_transfer("blind")
        src = { 'type': "sip", 'from': self.referred_by, 'to': self.refer_to }
        action = dict(type="transfer", transfer_id=tid, call_info=self.call_info, ctx={}, src=src)
        self.forward(action)
        
        yield from self.wait_this_action("hangup")
        self.logger.debug("And now we're hung up.")


class CalleeEndpoint(TestEndpoint):
    def plan(self):
        self.logger.debug("Callee created.")
        
        action = yield from self.wait_this_action("dial")
        offer = action.get("session")
        self.logger.debug("Got dial with offer:\n%s" % repr(offer))
        
        yield from self.sleep(1)
        answer = self.update_session(offer)
        self.forward(dict(type="ring", session=answer))
        self.logger.debug("Sent ringing.")
        
        # Force an UPDATE
        # FIXME: this may not reach the caller, and we may never get the answer, if the
        # call is forked, such as calling 360!
        # Also it works quite badly if a call was transferred here, and a session negotiator
        # is in action, sending here an offer.
        # So only use this for basic calls.
        #yield from self.sleep(1)
        #offer = answer.flipped()
        #self.forward(dict(type="session", session=offer))

        #action = yield from self.wait_this_action("session")
        #answer = action.get("session")
        #self.logger.debug("Got answer:\n%s" % repr(answer))

        yield from self.sleep(3)
        self.forward(dict(type="accept"))
        self.logger.debug("Sent accept.")

        yield from self.idle(True)
        
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
        
        #yield from self.wait_input("Press any key to dial out!")
        
        src = {
            'type': "sip",  # FIXME: we lie here for mkctx
            'from': self.sip_from,
            'to': self.sip_to
        }

        self.dial(src, session=None)

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
        yield from self.idle(True)

        self.logger.debug("Caller done.")


class VoicemailEventSource(EventSource):
    def __init__(self):
        EventSource.__init__(self, { "message-summary" })

        self.mailbox = None
        self.state = 0
        Plug(self.fake).attach_time(10, True)
        self.formatter = MessageSummaryFormatter()
        
        
    def identify(self, params):
        self.mailbox = params["mailbox"]
        
        return self.mailbox
        
        
    def fake(self):
        self.notify_all()
        self.state += 1

        
    def get_state(self, format):
        if format == "message-summary":
            s = dict(voice=self.state % 2, voice_old=self.state / 2 % 2)
            return self.formatter.format(s)
        else:
            raise Exception("Invalid format: %s!" % format)


class BusylampEventSource(EventSource):
    # To make call pickup work with Snom-s, one of these options should be used,
    # in order of increasing preference:
    #
    # * Report local and remote targets correctly. Add and artificially created, but safe
    #   call-id value. The remote URI is the switch's contact address, the Snom will send
    #   and INVITE to this URI with a Replaces header containing the call-id. The switch
    #   must accept this INVITE, extract the Replaces, and treat this as a pickup.
    #   This is not implemented yet, and probably never will be.
    #
    # * Report screwed up pickup code in the remote target with no call-id. Normal
    #   pickup will follow.
    #
    # * Don't report anything, set explicit pickup prefixes on the phone.
    
    def __init__(self):
        EventSource.__init__(self, { "dialog", "presence" })

        self.entity = None
        self.calls_by_number = {}
        self.state = 0
        self.dialog_formatter = DialogFormatter()
        self.presence_formatter = PresenceFormatter()
        
    
    def identify(self, params):
        self.entity = params["entity"]
        self.dialog_formatter.set_entity(self.entity)

        return self.entity
        
        
    def update(self, call_number, is_outgoing, is_confirmed):
        if is_confirmed is None:
            self.calls_by_number.pop(call_number)
        else:
            state = dict(is_outgoing=is_outgoing, is_confirmed=is_confirmed)
            self.calls_by_number[call_number] = state
            
        self.logger.info("%s: %s" % (self.entity, self.calls_by_number))
        
        self.notify_all()
            
        
    def fake(self):
        self.notify_all()
        self.state += 1
        
        
    def get_state(self, format):
        if format == "dialog":
            return self.dialog_formatter.format(self.calls_by_number)
        elif format == "presence":
            cisco_is_ringing = any(not s["is_outgoing"] and not s["is_confirmed"] for s in self.calls_by_number.values())
            cisco_is_busy = bool(self.calls_by_number)
            cisco_is_dnd = False
                
            s = dict(is_open=True, cisco=dict(is_ringing=cisco_is_ringing, is_busy=cisco_is_busy, is_dnd=cisco_is_dnd))
            return self.presence_formatter.format(s)
        else:
            raise Exception("Invalid format: %s!" % format)
            

class TestSubscriptionManager(SubscriptionManager):
    def __init__(self, switch, voicemail_number):
        SubscriptionManager.__init__(self, switch)
        
        self.voicemail_number = voicemail_number
        
        
    def identify_subscription(self, request):
        event = request["event"]
        from_uri = request["from"].uri
        to_uri = request["to"].uri
        
        if event == "message-summary" and to_uri.username == self.voicemail_number:
            return "voicemail", from_uri.username, event
            
        if event in ("dialog", "presence"):
            return "busylamp", to_uri.username, event
            
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
    #REC_TEMPLATE = "rec_%Y-%m-%d_%H:%M:%S.wav"
    REC_TEMPLATE = "rec_%H:%M:%S.wav"

    def __init__(self, manager):
        Bridge.__init__(self)
        
        self.manager = manager
        

    def identify(self, dst):
        self.addr = dst["addr"]
        self.username = dst["username"]
        self.is_outgoing = dst.get("is_outgoing", False)  # From the device's perspective
        self.is_confirmed = False
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

        if dst_addr != self.addr or dst_username != self.username:
            self.update_busylamp_state()
            self.dial(action)
            return
            
        record_uri = Uri(dst_addr, dst_username)
        contacts = self.ground.switch.registrar.lookup_contacts(record_uri)
        
        if not contacts:
            self.logger.debug("Record %s has no SIP contacts, rejecting!" % (record_uri,))
            self.reject_incoming_leg(Status.NOT_FOUND)
            
        self.update_busylamp_state()
        
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
            
            
    def is_device_li(self, li):
        return (li == 0) if self.is_outgoing else (li > 0)
        
        
    def other_li(self, li):
        return 0 if li > 0 else max(self.legs.keys())
        
            
    def process_leg_transfer(self, li, action):
        if self.is_device_li(li):
            self.forward_leg(self.other_li(li), action)
        else:
            Bridge.process_leg_transfer(self, li, action)
            
            
    def control(self, control):
        type = control["type"]
        
        if type == "record":
            on = control["on"]
            self.logger.info("Turning recording %s..." % ("on" if on else "off"))
            
            if on:
                s = self.legs[0].session_state.ground_session
                mgw_sid = self.select_gateway_sid(s["channels"][0])

                thing = self.make_media_thing("record", mgw_sid)
                self.add_media_thing(0, "rec", thing)
                self.link_media_things(0, None, 0, "rec", 0)
                self.link_media_things(0, None, 1, "rec", 1)
            
                format = ("L16", 8000, 1, None)
                filename = datetime.datetime.now().strftime(self.REC_TEMPLATE)
                thing.modify(dict(filename=filename, format=format, record=True))
            else:
                self.unlink_media_things(0, None, 0, "rec", 0)
                self.unlink_media_things(0, None, 1, "rec", 1)
                self.remove_media_thing(0, "rec")
        else:
            self.logger.error("Unknown line control type: %s!" % type)
            
            
    def do_leg(self, li, action):
        type = action["type"]
        
        if type == "accept":
            self.is_confirmed = True
            self.update_busylamp_state()
        elif type in ("reject", "hangup"):
            self.is_confirmed = None
            self.update_busylamp_state()
        elif type == "control":
            if action["target"] == "line":
                if self.is_device_li(li):
                    self.control(action["control"])
                else:
                    self.logger.warning("Ignoring line control from outside!")
                
                return
        
        return Bridge.do_leg(self, li, action)
        
        
    def call_pickup(self, dst_uri):
        if self.is_outgoing or self.is_confirmed:
            self.logger.warning("Call pickup is not possible right now!")
            return False
            
        self.logger.info("Call pickup to %s." % (dst_uri,))
        
        tid = self.ground.make_transfer("pickup")
        src = {
            'type': "sip",
            'from': Nameaddr(Uri(self.ctx["src_addr"], self.ctx["src_username"]), self.ctx["src_name"]),
            'to': Nameaddr(dst_uri)
        }

        # FIXME: A session query was necessary here because the caller was Caller,
        # and sent a query itself. This can get messy, implement properly!
        action = dict(type="transfer", transfer_id=tid, src=src)  #, session=Session.make_query())
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
