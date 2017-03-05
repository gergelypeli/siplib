import datetime
import collections
from weakref import proxy

from format import Status, Sip
from transactions import make_simple_response
from log import Loggable
from zap import Plug, EventSlot
from util import generate_state_etag, generate_tag


class Error(Exception):
    pass


FragmentInfo = collections.namedtuple("FragmentInfo", [
    "format", "data", "expiration_deadline", "expiration_plug"
])


class LocalState(Loggable):
    DEFAULT_EXPIRES = 3600

    def __init__(self, publisher, state_uri, formats):
        Loggable.__init__(self)
    
        self.publisher = publisher
        self.state_uri = state_uri
        self.formats = formats
        self.fragments_by_etag = {}

        
    def fragment_expired(self, etag):
        self.logger.info("Fragment expired: %s" % etag)
        info = self.fragments_by_etag.pop(etag)
        self.publisher.state_changed(self.state_uri, info.format, etag, None)


    def refresh_fragment(self, format, etag, data, expiration_deadline, expiration_plug):
        oldinfo = self.fragments_by_etag.get(etag)
        
        if data:
            # Change, invoke triggers
            self.publisher.state_changed(self.state_uri, format, etag, data)
            
        if oldinfo:
            if not data:
                data = oldinfo.data
        
            if oldinfo.expiration_plug:
                oldinfo.expiration_plug.detach()

        info = FragmentInfo(format, data, expiration_deadline, expiration_plug)
        self.fragments_by_etag[etag] = info
        what = "Prolonged" if oldinfo and oldinfo.data == data else "Refreshed"
        self.logger.info("%s %s format %s etag %s until %s." % (what, self.state_uri, format, etag, expiration_deadline))

        
    def add_static_fragment(self, format, etag, data):
        self.fragments_by_etag[etag] = FragmentInfo(format, data, None, None)
        self.logger.info("Stored %s format %s etag %s permanently." % (self.state_uri, format, etag))
        

    def recv(self, request):
        now = datetime.datetime.now()
        
        format = request["event"]
        if format not in self.formats:
            self.logger.warning("Ignoring publication with unknown format: %s!" % format)
            self.send(Sip.response(status=Status.BAD_EVENT, related=request))
            return
        
        etag = request.get("sip_if_match")
        if not etag:
            etag = generate_state_etag()
        elif etag not in self.fragments_by_etag:
            self.logger.warning("Ignoring publication with etag: %s!" % etag)
            self.send(Sip.response(status=Status.CONDITIONAL_REQUEST_FAILED, related=request))
            return
            
        data = request.body
            
        if "expires" in request:
            seconds_left = request["expires"]
        else:
            seconds_left = self.DEFAULT_EXPIRES
            
        expiration_deadline = now + datetime.timedelta(seconds=seconds_left)
        expiration_plug = Plug(self.fragment_expired, etag=etag).attach_time(seconds_left)
        
        self.refresh_fragment(format, etag, data, expiration_deadline, expiration_plug)
        
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

        self.publisher.transmit(response)


class Publisher(Loggable):
    def __init__(self, switch):
        Loggable.__init__(self)

        self.switch = switch
        self.local_states_by_uri = {}
        self.state_change_slot = EventSlot()
        
        
    def transmit(self, msg):
        self.switch.send_message(msg)

        
    def reject_request(self, request, status):
        response = make_simple_response(request, status)
        self.transmit(response)


    def add_local_state(self, state_uri, formats):
        state_uri = state_uri.canonical_aor()
        
        if state_uri in self.local_states_by_uri:
            self.logger.error("Local state already added: '%s'" % (state_uri,))
            return None
        else:
            self.logger.info("Creating local state: %s" % (state_uri,))
            state = LocalState(proxy(self), state_uri, formats)
            state.set_oid(self.oid.add("state", str(state_uri)))
            self.local_states_by_uri[state_uri] = state
            
        return proxy(state)


    def may_publish(self, publishing_uri, state_uri):
        # Third party publications not supported yet
        
        return publishing_uri.canonical_aor() == state_uri
        
        
    def process_request(self, request):
        if request.method != "PUBLISH":
            raise Error("Publisher has nothing to do with this request!")
        
        publishing_uri = request["from"].uri
        state_uri = request["to"].uri.canonical_aor()
        
        if not self.may_publish(publishing_uri, state_uri):
            self.reject_request(request, Status.FORBIDDEN)
            return
        
        state = self.local_states_by_uri.get(state_uri)
        if not state:
            self.logger.warning("Local state not found: %s" % (state_uri,))
            self.reject_request(request, Status.NOT_FOUND)
            return
        
        state.recv(request)
        

    def state_changed(self, aor, format, etag, info):
        self.state_change_slot.zap(aor, format, etag, info)
