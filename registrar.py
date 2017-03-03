import datetime
import collections
from weakref import proxy

from format import Nameaddr, Status, Uri, Sip
from transactions import make_simple_response
from log import Loggable
from zap import Plug, EventSlot
from util import generate_call_id, generate_tag, MAX_FORWARDS


class Error(Exception):
    pass


UriHop = collections.namedtuple("UriHop", [ "uri", "hop" ])
ContactInfo = collections.namedtuple("ContactInfo", [
    "call_id", "cseq", "user_agent", "expiration_deadline", "expiration_plug"
])


class LocalRecord(Loggable):
    AUTH_NEVER = "AUTH_NEVER"
    AUTH_ALWAYS = "AUTH_ALWAYS"
    AUTH_IF_UNREGISTERED = "AUTH_IF_UNREGISTERED"
    AUTH_BY_HOP = "AUTH_BY_HOP"

    DEFAULT_EXPIRES = 3600

    def __init__(self, registrar, record_uri, authname, auth_policy):
        Loggable.__init__(self)
    
        self.registrar = registrar
        self.record_uri = record_uri
        self.authname = authname
        self.auth_policy = auth_policy

        self.contact_infos_by_uri_hop = {}
        
        
    def contact_expired(self, urihop):
        self.logger.info("Contact expired: %s via %s" % urihop)
        self.contact_infos_by_uri_hop.pop(urihop)
        self.registrar.record_changed(self.record_uri, urihop, None)


    def refresh_contact(self, urihop, call_id, cseq, user_agent, expiration_deadline, expiration_plug):
        info = ContactInfo(call_id, cseq, user_agent, expiration_deadline, expiration_plug)
        oldinfo = self.contact_infos_by_uri_hop.get(urihop)
        
        if oldinfo:
            if oldinfo.expiration_plug:
                oldinfo.expiration_plug.detach()
        else:
            # First time registration, invoke triggers
            self.registrar.record_changed(self.record_uri, urihop, info)
        
        self.contact_infos_by_uri_hop[urihop] = info
        self.logger.info("Registered %s from %s via %s until %s." % (self.record_uri, urihop.uri, urihop.hop, expiration_deadline))

        
    def add_static_contact(self, uri, hop):
        urihop = UriHop(uri, hop)
        
        self.contact_infos_by_uri_hop[urihop] = ContactInfo(None, None, "static", None, None)
        self.logger.info("Registered %s from %s via %s permanently." % (self.record_uri, uri, hop))
        

    def recv(self, request):
        now = datetime.datetime.now()
        hop = request.hop
        call_id = request["call_id"]
        cseq = request["cseq"]
        contact_nameaddrs = request["contact"]

        # First check if all contacts can be updated
        for contact_nameaddr in contact_nameaddrs:
            uri = contact_nameaddr.uri
            contact_info = self.contact_infos_by_uri_hop.get(UriHop(uri, hop))
            
            if contact_info and contact_info.call_id == call_id and contact_info.cseq >= cseq:
                # The RFC suggests 500, but that's a bit weird
                response = Sip.response(status=Status.SERVER_INTERNAL_ERROR, related=request)
                self.send(response)
                return

        # Okay, update them all
        # No contact is valid for just fetching the registrations
        for contact_nameaddr in contact_nameaddrs:
            uri = contact_nameaddr.uri
            urihop = UriHop(uri, hop)
            user_agent = request.get("user_agent")
            
            expires = contact_nameaddr.params.get("expires", request.get("expires"))
            seconds_left = int(expires) if expires is not None else self.DEFAULT_EXPIRES
            expiration_deadline = now + datetime.timedelta(seconds=seconds_left)
            expiration_plug = Plug(self.contact_expired, urihop=urihop).attach_time(seconds_left)
            
            self.refresh_contact(urihop, call_id, cseq, user_agent, expiration_deadline, expiration_plug)
        
        fetched = []
        for urihop, contact_info in self.contact_infos_by_uri_hop.items():
            seconds_left = int((contact_info.expiration_deadline - now).total_seconds())
            fetched.append(Nameaddr(uri=urihop.uri, params=dict(expires=str(seconds_left))))

        response = Sip.response(status=Status.OK, related=request)
        response["contact"] = fetched
        self.send(response)


    def send(self, response):
        request = response.related
        
        response.method = request.method
        response.hop = request.hop
        response["from"] = request["from"]
        response["to"] = request["to"].tagged(generate_tag())
        response["call_id"] = request["call_id"]
        response["cseq"] = request["cseq"]

        self.registrar.transmit(response)


    def get_contacts(self):
        return list(self.contact_infos_by_uri_hop.keys())


    def is_contact_hop(self, hop):
        return any(urihop.hop.contains(hop) for urihop in self.contact_infos_by_uri_hop.keys())
        

    def authenticate_request(self, hop):
        # This is not about REGISTER requests, but about regular requests that
        # just arrived.
        
        if self.auth_policy == self.AUTH_NEVER:
            self.logger.debug("Accepting request because authentication is never needed")
            return self.authname, True
        elif self.auth_policy == self.AUTH_ALWAYS:
            self.logger.debug("Authenticating request because account always needs it")
            return self.authname, False
        elif self.auth_policy == self.AUTH_IF_UNREGISTERED:
            if self.is_contact_hop(hop):
                self.logger.debug("Accepting request because account is registered")
                return self.authname, True
            else:
                self.logger.debug("Authenticating request because account is not registered")
                return self.authname, False
        elif self.auth_policy == self.AUTH_BY_HOP:
            if self.is_contact_hop(hop):
                self.logger.debug("Accepting request because hop is allowed")
                return self.authname, True
            else:
                self.logger.debug("Rejecting request because hop address is not allowed")
                return None, True
        else:
            raise Exception("WTF?")
        
        
class RemoteRecord(Loggable):
    EXPIRES_SECONDS = 300
    HAZARD_SECONDS = 5

    def __init__(self, registrar, record_uri, registrar_uri, hop):
        Loggable.__init__(self)
        self.registrar = registrar

        self.record_uri = record_uri
        self.registrar_uri = registrar_uri
        self.contact_uri = Uri(hop.local_addr)
        
        if not hop:
            raise Exception("Please select the hop before invoking the registration!")
        
        self.hop = hop
        self.local_tag = generate_tag()  # make it persistent just for the sake of safety
        self.call_id = generate_call_id()
        self.cseq = 0
        self.refresh_plug = Plug(self.refresh)
        
        
    def refresh(self):
        expires = self.EXPIRES_SECONDS
        contact = [ Nameaddr(self.contact_uri, params=dict(expires=str(expires))) ]
        
        request = Sip.request(method="REGISTER")
        request["contact"] = contact
        
        self.send(request)
        
        
    def send(self, request):
        self.cseq += 1

        request.uri = self.registrar_uri
        request.hop = self.hop
        request['from'] = Nameaddr(self.record_uri).tagged(self.local_tag)
        request['to'] = Nameaddr(self.record_uri)
        request["call_id"] = self.call_id
        request["cseq"] = self.cseq
        request["max_forwards"] = MAX_FORWARDS

        self.registrar.transmit(request)


    def recv(self, response):
        status = response.status
        
        if status.code == 401:
            # Let's try authentication! TODO: 407, too!
            request = response.related
            auth = self.registrar.provide_auth(response)
                
            if auth:
                request.update(auth)
                self.cseq += 1
                request["cseq"] = self.cseq
                request["via"] = None
                
                self.logger.debug("Trying authorization...")
                self.registrar.transmit(request)
                return
            else:
                self.logger.debug("Couldn't authorize, being rejected!")

        for contact_nameaddr in response["contact"]:
            if contact_nameaddr.uri == self.contact_uri:
                expires = contact_nameaddr.params.get("expires", response.get("expires"))
                seconds_left = int(expires)
                expiration_deadline = datetime.datetime.now() + datetime.timedelta(seconds=seconds_left)
                self.logger.info("Registered at %s until %s" % (self.registrar_uri, expiration_deadline))
                
                self.refresh_plug.detach()
                self.refresh_plug.attach_time(seconds_left - self.HAZARD_SECONDS)
                break
        else:
            self.logger.warning("Couldn't confirm my registration, consider it expired!")


class Registrar(Loggable):
    def __init__(self, switch):
        Loggable.__init__(self)

        self.switch = switch
        self.local_records_by_uri = {}
        self.remote_records_by_uri = {}
        self.record_change_slot = EventSlot()
        
        
    def reject_request(self, request, status):
        response = make_simple_response(request, status)
        self.transmit(response)


    def add_local_record(self, record_uri, authname, auth_policy):
        record_uri = record_uri.canonical_aor()
        
        if record_uri in self.local_records_by_uri:
            self.logger.error("Local record already added: '%s'" % (record_uri,))
            return None
        else:
            self.logger.info("Creating local record: %s" % (record_uri,))
            record = LocalRecord(proxy(self), record_uri, authname, auth_policy)
            record.set_oid(self.oid.add("local", str(record_uri)))
            self.local_records_by_uri[record_uri] = record
            
        return proxy(record)


    def may_register(self, registering_uri, record_uri):
        # Third party registrations not supported yet
        # We'd need to know which account is allowed to register which
        
        return registering_uri.canonical_aor() == record_uri
        
        
    def process_request(self, request):
        if request.method != "REGISTER":
            raise Error("Registrar has nothing to do with this request!")
        
        registering_uri = request["from"].uri
        record_uri = request["to"].uri.canonical_aor()
        
        if not self.may_register(registering_uri, record_uri):
            self.reject_request(request, Status.FORBIDDEN)
            return
        
        record = self.local_records_by_uri.get(record_uri)
        if not record:
            self.logger.warning("Local record not found: %s" % record_uri)
            self.reject_request(request, Status.NOT_FOUND)
            return
        
        record.recv(request)
        
        
    def transmit(self, msg):
        self.switch.send_message(msg)


    def lookup_contacts(self, record_uri):
        record_uri = record_uri.canonical_aor()
        record = self.local_records_by_uri.get(record_uri)
        
        if record:
            self.logger.debug("Found contacts for '%s'" % (record_uri,))
            return record.get_contacts()
        else:
            self.logger.debug("Not found contacts for: %s" % (record_uri,))
            return []


    def authenticate_request(self, request):
        # Checks any incoming request to see if the sender is a local account.
        # Returns:
        #   authname, True -  accept
        #   authname, False - challenge
        #   None, True -      reject
        #   None, False -     not found
        
        record_uri = request["from"].uri.canonical_aor()
        hop = request.hop
        
        record = self.local_records_by_uri.get(record_uri) or self.local_records_by_uri.get(record_uri._replace(username=None))
        
        if not record:
            self.logger.warning("Rejecting request because caller is unknown!")
            return None, False
        
        return record.authenticate_request(hop)

        
    def provide_auth(self, response):
        return self.switch.provide_auth(response)
        

    def add_remote_record(self, record_uri, registrar_uri, registrar_hop=None):
        # Predefined Route set is not supported now
        # Don't canonicalize these, we'll send them out as request URIs!
        record_uri = record_uri
        
        if not registrar_hop:
            route = None  # TODO: do we want to register using a Route?
            next_uri = route[0].uri if route else registrar_uri
        
            Plug(self.hop_resolved, record_uri=record_uri, registrar_uri=registrar_uri).attach(
                self.switch.select_hop_slot(next_uri)
            )
            return
            
        self.logger.info("Creating remote record: %s" % (record_uri,))
        record = RemoteRecord(proxy(self), record_uri, registrar_uri, registrar_hop)
        record.set_oid(self.oid.add("remote", str(record_uri)))
        self.remote_records_by_uri[record_uri] = record
        
        record.refresh()
        
        
    def hop_resolved(self, hop, record_uri, registrar_uri):
        if hop:
            self.logger.info("Remote record hop resolved to %s" % (hop,))
            self.add_remote_record(record_uri, registrar_uri, hop)
        else:
            self.logger.error("Remote record hop was not resolved, ignoring!")


    def process_response(self, response):
        record_uri = response['to'].uri
        record = self.remote_records_by_uri.get(record_uri)
        
        if record:
            record.recv(response)
        else:
            self.logger.warning("Ignoring response to unknown remote record!")


    def record_changed(self, aor, urihop, info):
        self.record_change_slot.zap(aor, urihop, info)
