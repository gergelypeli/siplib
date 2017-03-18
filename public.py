import datetime
import collections
from weakref import proxy

from format import Status, Sip
from parser import Xml
from transactions import make_simple_response
from log import Loggable
from zap import Plug, EventSlot
from util import generate_state_etag, generate_tag, EventKey


class EventParser:
    def parse(self, content_type, body):
        raise NotImplementedError()
        
        
class PresenceParser(EventParser):
    def get_is_open(self, xml):
        is_open = False
        
        if xml.tag == "presence":
            for x in xml.content:
                if x.tag == "tuple":
                    for y in x.content:
                        if y.tag == "status":
                            for z in y.content:
                                if z.tag == "basic":
                                    for w in z.content:
                                        if not w.tag:
                                            is_open = (True if w.content == "open" else False if w.content == "closed" else None)
        
        return is_open


    def parse(self, content_type, body):
        if content_type != "application/pidf+xml":
            raise Exception("Not pidf+xml content!")
            
        xml = Xml.parse(body.decode("utf8"))
        is_open = self.get_is_open(xml)
        
        return dict(is_open=is_open)


class CiscoPresenceParser(PresenceParser):
    def get_activities(self, xml):
        is_ringing, is_busy, is_dnd = False, False, False
        
        if xml.tag == "presence":
            for x in xml.content:
                if x.tag == "dm:person":
                    for y in x.content:
                        if y.tag == "e:activities":
                            for z in y.content:
                                if z.tag == "ce:alerting":
                                    is_ringing = True
                                elif z.tag == "e:on-the-phone":
                                    is_busy = True
                                elif z.tag == "ce:dnd":
                                    is_dnd = True
                    
        return is_ringing, is_busy, is_dnd


    def parse(self, content_type, body):
        if content_type != "application/pidf+xml":
            raise Exception("Not pidf+xml content!")
            
        xml = Xml.parse(body.decode("utf8"))
        is_open = self.get_is_open(xml)
        is_ringing, is_busy, is_dnd = self.get_activities(xml)
        
        return dict(is_open=is_open, is_ringing=is_ringing, is_busy=is_busy, is_dnd=is_dnd)


FragmentInfo = collections.namedtuple("FragmentInfo", [
    "expiration_deadline", "expiration_plug"
])


class LocalState(Loggable):
    DEFAULT_EXPIRES = 3600

    def __init__(self):
        Loggable.__init__(self)
    
        self.manager = None
        self.fragment_infos_by_etag = {}
        self.state_change_slot = EventSlot()


    def set_manager(self, manager):
        self.manager = manager
        
        
    def identify(self, params):
        raise NotImplementedError()
        

    def get_state(self, event, content_type, body):
        raise NotImplementedError()
        
        
    def fragment_expired(self, etag):
        self.logger.info("Fragment expired: %s" % etag)
        self.fragment_infos_by_etag.pop(etag)
        self.state_change_slot.zap(etag, None)
        #self.add_state(etag, None)


    def refresh_fragment(self, etag, expiration_deadline, expiration_plug):
        oldinfo = self.fragment_infos_by_etag.get(etag)
        
        if oldinfo:
            if oldinfo.expiration_plug:
                oldinfo.expiration_plug.detach()

        info = FragmentInfo(expiration_deadline, expiration_plug)
        self.fragment_infos_by_etag[etag] = info
        self.logger.info("Refreshed etag %s until %s." % (etag, expiration_deadline))

        
    def recv(self, format, request):
        now = datetime.datetime.now()
        
        etag = request.get("sip_if_match")
        content_type = request.get("content_type")
        data = request.body
        
        if not etag:
            # Initial publication
            
            if not data:
                self.logger.warning("Rejecting initial publication without state!")
                self.send(Sip.response(status=Status.BAD_REQUEST, related=request))
                return

            self.logger.info("Accepting initial publication with etag: %s." % etag)
            etag = generate_state_etag()
        else:
            # Refresh
            
            if etag not in self.fragment_infos_by_etag:
                self.logger.warning("Rejecting publication refresh with etag: %s!" % etag)
                self.send(Sip.response(status=Status.CONDITIONAL_REQUEST_FAILED, related=request))
                return
            
            self.logger.info("Accepting publication refresh with etag: %s." % etag)
            

        # The state may not change for a refresh        
        if data:
            state = self.get_state(format, content_type, data)

            if not state:
                self.logger.warning("Ignoring publication with invalid state!")
                self.send(Sip.response(status=Status.BAD_EVENT, related=request))
                return

            self.state_change_slot.zap(etag, state)
            #self.add_state(etag, state)
        
        seconds_left = request.get("expires", self.DEFAULT_EXPIRES)
        expiration_deadline = now + datetime.timedelta(seconds=seconds_left)
        expiration_plug = Plug(self.fragment_expired, etag=etag).attach_time(seconds_left)
        
        self.refresh_fragment(etag, expiration_deadline, expiration_plug)
        
        response = Sip.response(status=Status.OK, related=request)
        response["expires"] = seconds_left
        response["sip_etag"] = etag
        self.send(response)


    def send(self, response):
        request = response.related
        
        response.method = request.method
        response.hop = request.hop
        response["from"] = request["from"]
        response["to"] = request["to"].tagged(generate_tag())
        response["call_id"] = request["call_id"]
        response["cseq"] = request["cseq"]

        self.manager.transmit(response)


class PublicationManager(Loggable):
    def __init__(self, switch):
        Loggable.__init__(self)

        self.switch = switch
        self.local_states_by_key = {}
        self.state_change_slot = EventSlot()
        
        
    def transmit(self, msg):
        self.switch.send_message(msg)

        
    def reject_request(self, request, status):
        response = make_simple_response(request, status)
        self.transmit(response)


    def make_local_state(self, type):
        raise NotImplementedError()
        
        
    def identify_publication(self, request):
        raise NotImplementedError()


    def add_local_state(self, type, params):
        ls = self.make_local_state(type)
        ls.set_manager(proxy(self))
        id = ls.identify(params)
        self.logger.debug("Created local state: %s=%s." % (type, id))

        ls.set_oid(self.oid.add(type, id))
        key = EventKey(type, id)
        self.local_states_by_key[key] = ls
        
        Plug(self.state_changed, key=key).attach(ls.state_change_slot)

        return proxy(ls)


    def may_publish(self, publishing_uri, state_uri):
        # Third party publications not supported yet
        
        return publishing_uri.canonical_aor() == state_uri
        
        
    def process_request(self, request):
        if request.method != "PUBLISH":
            raise Exception("Publisher has nothing to do with this request!")

        publishing_uri = request["from"].uri
        state_uri = request["to"].uri.canonical_aor()
        
        if not self.may_publish(publishing_uri, state_uri):
            self.reject_request(request, Status.FORBIDDEN)
            return

        key, format = self.identify_publication(request)
        
        if not key:
            self.logger.warning("Rejecting publication for unidentifiable event source!")
            self.reject_request(request, Status.NOT_FOUND)
            return
        
        state = self.local_states_by_key.get(key)
        if not state:
            self.logger.warning("Local state not found: %s" % (key,))
            self.reject_request(request, Status.NOT_FOUND)
            return
        
        state.recv(format, request)


    def state_changed(self, etag, state, key):
        self.state_change_slot.zap(key, etag, state)
