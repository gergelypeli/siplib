import uuid
import datetime
import collections
from weakref import proxy

from format import Nameaddr, Status, Uri, Sip
from transactions import make_simple_response
from log import Loggable


MAX_FORWARDS = 20

class Error(Exception):
    pass


def safe_update(target, source):
    for k, v in source.items():
        if k in target:
            raise Error("Can't overwrite field %r!" % k)
        target[k] = v

    return target


def generate_call_id():
    return uuid.uuid4().hex[:8]


def generate_tag():
    return uuid.uuid4().hex[:8]


UriHop = collections.namedtuple("UriHop", [ "uri", "hop" ])
ContactInfo = collections.namedtuple("ContactInfo", [ "call_id", "cseq", "expiration" ])


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
        

    def add_contact(self, uri, hop, call_id, cseq, expiration):
        urihop = UriHop(uri, hop)
        self.contact_infos_by_uri_hop[urihop] = ContactInfo(call_id, cseq, expiration)
        self.logger.info("Registered %s from %s via %s until %s." % (self.record_uri, uri, hop, expiration))
        
        
    def add_static_contact(self, uri, hop):
        urihop = UriHop(uri, hop)
        
        self.contact_infos_by_uri_hop[urihop] = ContactInfo(None, None, None)
        self.logger.info("Registered %s from %s via %s permanently." % (self.record_uri, uri, hop))
        

    def recv_request(self, request):
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
                self.send_response(Sip.response(status=Status(500, "Out of order request is not our fault")), request)
                return

        # Okay, update them all
        # No contact is valid for just fetching the registrations
        for contact_nameaddr in contact_nameaddrs:
            uri = contact_nameaddr.uri
            expires = contact_nameaddr.params.get("expires", request.get("expires"))
            seconds_left = int(expires) if expires is not None else self.DEFAULT_EXPIRES
            expiration = now + datetime.timedelta(seconds=seconds_left)
            
            self.add_contact(uri, hop, call_id, cseq, expiration)
        
        fetched = []
        for urihop, contact_info in self.contact_infos_by_uri_hop.items():
            seconds_left = int((contact_info.expiration - now).total_seconds())
            fetched.append(Nameaddr(uri=urihop.uri, params=dict(expires=str(seconds_left))))

        self.send_response(Sip.response(status=Status(200, "OK"), contact=fetched), request)


    def send_response(self, response, related_request):
        response.is_response = True
        response.method = related_request.method
        response.hop = related_request.hop
        response["from"] = related_request["from"]
        response["to"] = related_request["to"].tagged(generate_tag())
        response["call_id"] = related_request["call_id"]
        response["cseq"] = related_request["cseq"]

        #response = safe_update(user_response, nondialog_response)
        
        self.registrar.transmit(response, related_request)


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
    EXPIRES = 300

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
        
        
    def refresh(self):
        expires = self.EXPIRES
        contact = [ Nameaddr(self.contact_uri, params=dict(expires=str(expires))) ]
        
        request = Sip.request(method="REGISTER", contact=contact)
        
        self.send_request(request)
        
        
    def send_request(self, request):
        self.cseq += 1

        request.is_response = False
        request.uri = self.registrar_uri
        request.hop = self.hop
        request['from'] = Nameaddr(self.record_uri).tagged(self.local_tag)
        request['to'] = Nameaddr(self.record_uri)
        request["call_id"] = self.call_id
        request["cseq"] = self.cseq
        request["max_forwards"] = MAX_FORWARDS
        #request["user_request": user_request.copy()

        #request = safe_update(user_request, nondialog_request)

        self.registrar.transmit(request, None)


    def recv_response(self, response, related_request):
        status = response.status
        
        if status.code == 401:
            # Let's try authentication! TODO: 407, too!
            account = self.registrar.get_remote_account(related_request.uri)
            auth = account.provide_auth(response, related_request) if account else None
                
            if auth:
                #user_request = related_request.user_request
                #related_request.clear()
                #related_request.update(user_request)
                related_request.update(auth)
                self.cseq += 1
                related_request["cseq"] = self.cseq
                related_request["via"] = None
                
                self.logger.debug("Trying authorization...")
                #print("... with: %s" % related_request)
                #self.send_request(related_request)
                self.registrar.transmit(related_request, None)
                return
            else:
                self.logger.debug("Couldn't authorize, being rejected!")

        for contact_nameaddr in response["contact"]:
            if contact_nameaddr.uri == self.contact_uri:
                expires = contact_nameaddr.params.get("expires", response.get("expires"))
                seconds_left = int(expires)
                expiration = datetime.datetime.now() + datetime.timedelta(seconds=seconds_left)
                self.logger.info("Registered at %s until %s" % (self.registrar_uri, expiration))
                break
        else:
            self.logger.warning("Couldn't confirm my registration!")
            
        # TODO: reschedule!


class Registrar(Loggable):
    def __init__(self, switch):
        Loggable.__init__(self)

        self.switch = switch
        self.local_records_by_uri = {}
        self.remote_records_by_uri = {}
        
        
    def reject_request(self, request, status):
        response = make_simple_response(request, status)
        self.transmit(response, request)


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
            self.reject_request(request, Status(403, "Forbidden"))
            return
        
        record = self.local_records_by_uri.get(record_uri)
        if not record:
            self.logger.warning("Local record not found: %s" % record_uri)
            self.reject_request(request, Status(404))
            return
        
        record.recv_request(request)
        
        
    def transmit(self, params, related_params=None):
        self.switch.send_message(params, related_params)


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

        
    def get_remote_account(self, uri):
        return self.switch.get_remote_account(uri)
        

    def add_remote_record(self, record_uri, registrar_uri, registrar_hop=None):
        # Predefined Route set is not supported now
        # Don't canonicalize these, we'll send them out as request URIs!
        record_uri = record_uri
        
        if not registrar_hop:
            route = None  # TODO: do we want to register using a Route?
            next_uri = route[0].uri if route else registrar_uri
        
            self.switch.select_hop_slot(next_uri).plug(
                self.hop_resolved,
                record_uri=record_uri,
                registrar_uri=registrar_uri
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


    def process_response(self, response, related_request):
        record_uri = response['to'].uri
        record = self.remote_records_by_uri.get(record_uri)
        
        if record:
            record.recv_response(response, related_request)
        else:
            self.logger.warning("Ignoring response to unknown remote record!")
