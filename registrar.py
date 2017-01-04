import uuid
import datetime
import collections
from weakref import proxy, WeakValueDictionary

from format import Nameaddr, Status, Uri
from transactions import make_simple_response
from util import Loggable

MAXFWD = 50

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


class NondialogInfo:
    def __init__(self, cseq):
        self.cseq = cseq
        

UriHop = collections.namedtuple("UriHop", [ "uri", "hop" ])
#NondialogInfo = collections.namedtuple("NondialogInfo", [ "cseq" ])
ContactInfo = collections.namedtuple("ContactInfo", [ "expiration", "nondialog_info" ])


class Record(Loggable):
    AUTH_NEVER = "AUTH_NEVER"
    AUTH_ALWAYS = "AUTH_ALWAYS"
    AUTH_IF_UNREGISTERED = "AUTH_IF_UNREGISTERED"
    AUTH_BY_HOP = "AUTH_BY_HOP"

    DEFAULT_EXPIRES = 3600

    def __init__(self, record_manager, record_uri, authname, auth_policy):
        Loggable.__init__(self)
    
        self.record_manager = record_manager
        self.record_uri = record_uri
        self.authname = authname
        self.auth_policy = auth_policy

        self.contact_infos_by_uri_hop = {}
        self.nondialog_infos_by_call_id = WeakValueDictionary()
        
        # It's kinda hard to decide when an incoming REGISTER corresponds to a
        # previously received one, but let's assume clients are decent enough
        # to generate unique callid-s. Also, we only allow one Contact per request.
        #self.contacts_by_call_id = {}


    def add_contact(self, uri, hop, expiration, nondialog_info):
        urihop = UriHop(uri, hop)
        self.contact_infos_by_uri_hop[urihop] = ContactInfo(expiration, nondialog_info)
        self.logger.info("Registered %s from %s via %s until %s." % (self.record_uri, uri, hop, expiration))
        
        
    def add_static_contact(self, uri, hop):
        expiration = None
        nondialog_info = None
        urihop = UriHop(uri, hop)
        
        self.contact_infos_by_uri_hop[urihop] = ContactInfo(expiration, nondialog_info)
        self.logger.info("Registered %s from %s via %s permanently." % (self.record_uri, uri, hop))
        

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


    def process_updates(self, params):
        # TODO: check authname!
        now = datetime.datetime.now()
        hop = params["hop"]
        call_id = params["call_id"]
        cseq = params["cseq"]
        contact_nameaddrs = params["contact"]
        
        nondialog_info = NondialogInfo(cseq)
        self.nondialog_infos_by_call_id[call_id] = nondialog_info
        
        # No contact is valid for just fetching the registrations
        for contact_nameaddr in contact_nameaddrs:
            uri = contact_nameaddr.uri
            expires = contact_nameaddr.params.get("expires", params.get("expires"))
            seconds_left = int(expires) if expires is not None else self.DEFAULT_EXPIRES
            expiration = now + datetime.timedelta(seconds=seconds_left)
            
            self.add_contact(uri, hop, expiration, nondialog_info)
        
        fetched = []
        for urihop, contact_info in self.contact_infos_by_uri_hop.items():
            seconds_left = int((contact_info.expiration - now).total_seconds())
            fetched.append(Nameaddr(uri=urihop.uri, params=dict(expires=str(seconds_left))))

        self.send_response(dict(status=Status(200, "OK"), contact=fetched), params)


    def take_request(self, params):
        if params["is_response"]:
            raise Error("Not a request!")

        call_id = params["call_id"]
        cseq = params["cseq"]

        nondialog_info = self.nondialog_infos_by_call_id.get(call_id)

        if nondialog_info and cseq <= nondialog_info.cseq:
            # The RFC suggests 500, but that's a bit weird
            self.send_response(dict(status=Status(500, "Lower CSeq Is Not Our Fault")), params)
            return None

        return params


    def make_response(self, user_params, related_request):
        dialog_params = {
            "is_response": True,
            "from": related_request["from"],
            "to": related_request["to"].tagged(generate_tag()),
            "call_id": related_request["call_id"],
            "cseq": related_request["cseq"],
            "method": related_request["method"],
            "hop": related_request["hop"]
        }

        return safe_update(user_params, dialog_params)


    def send_response(self, user_params, related_request):
        #self.logger.debug("Will send response: %s" % str(user_params))
        params = self.make_response(user_params, related_request)
        self.record_manager.transmit(params, related_request)
        
        
    def recv_request(self, msg):
        params = self.take_request(msg)
        if params:
            self.process_updates(params)


class RecordManager(Loggable):
    def __init__(self, switch):
        Loggable.__init__(self)

        self.switch = switch
        self.records_by_uri = {}
        
        
    def reject_request(self, request, status):
        response = make_simple_response(request, status)
        self.transmit(response, request)


    def add_record(self, record_uri, authname, auth_policy):
        record_uri = record_uri.canonical_aor()
        
        if record_uri in self.records_by_uri:
            self.logger.error("Record already added: '%s'" % (record_uri,))
            return None
        else:
            self.logger.debug("Creating record: %s" % (record_uri,))
            record = Record(proxy(self), record_uri, authname, auth_policy)
            record.set_oid(self.oid.add("record", str(record_uri)))
            self.records_by_uri[record_uri] = record
            
        return proxy(record)


    def may_register(self, registering_uri, record_uri):
        # Third party registrations not supported yet
        # We'd need to know which account is allowed to register which
        
        return registering_uri.canonical_aor() == record_uri
        
        
    def process_request(self, params):
        if params["method"] != "REGISTER":
            raise Error("RecordManager has nothing to do with this request!")
        
        registering_uri = params["from"].uri
        record_uri = params["to"].uri.canonical_aor()
        
        if not self.may_register(registering_uri, record_uri):
            self.reject_request(params, Status(403, "Forbidden"))
            return
        
        record = self.records_by_uri.get(record_uri)
        if not record:
            self.logger.warning("Record not found: %s" % record_uri)
            self.reject_request(params, Status(404))
            return
        
        record.recv_request(params)
        
        
    def transmit(self, params, related_params=None):
        self.switch.send_message(params, related_params)


    def emulate_registration(self, record_uri, contact_uri, seconds, hop):
        record_uri = record_uri.canonical_aor()
        expiration = datetime.datetime.now() + datetime.timedelta(seconds=seconds)
        self.add_record(record_uri).add_contact(contact_uri, expiration, hop)
        

    def lookup_contacts(self, record_uri):
        record_uri = record_uri.canonical_aor()
        record = self.records_by_uri.get(record_uri)
        
        if record:
            self.logger.debug("Found contacts for '%s'" % (record_uri,))
            return record.get_contacts()
        else:
            self.logger.debug("Not found contacts for: %s" % (record_uri,))
            return []


    def authenticate_request(self, params):
        # Returns:
        #   authname, True -  accept
        #   authname, False - challenge
        #   None, True -      reject
        #   None, False -     not found
        
        record_uri = params["from"].uri.canonical_aor()
        hop = params["hop"]
        
        record = self.records_by_uri.get(record_uri) or self.records_by_uri.get(record_uri._replace(username=None))
        
        if not record:
            self.logger.warning("Rejecting request because caller is unknown!")
            return None, False
        
        return record.authenticate_request(hop)
        

class Registration(object):
    EXPIRES = 300

    def __init__(self, registration_manager, registrar_uri, record_uri, hop):
        self.registration_manager = registration_manager

        self.registrar_uri = registrar_uri
        self.record_uri = record_uri
        self.contact_uri = Uri(hop.local_addr)
        
        if not hop:
            raise Exception("Please select the hop before invoking the registration!")
        
        self.hop = hop
        self.local_tag = generate_tag()  # make it persistent just for the sake of safety
        self.call_id = generate_call_id()
        self.cseq = 0
        
        
    def update(self):
        user_params = {
            'method': "REGISTER",
            'uri': self.registrar_uri,
            'from': Nameaddr(self.record_uri).tagged(self.local_tag),
            'to': Nameaddr(self.record_uri),
            'contact': [ Nameaddr(self.contact_uri, params=dict(expires=str(self.EXPIRES))) ]
        }
        
        self.send_request(user_params)
        
        
    def process_response(self, params):
        total = []

        for contact_nameaddr in params["contact"]:
            expires = contact_nameaddr.params.get("expires", params.get("expires"))
                
            uri = contact_nameaddr.uri
            seconds_left = int(expires)
            
            total.append((uri, seconds_left))

        self.registration_manager.logger.debug("Registration total: %s" % total)
        # TODO: reschedule!
        
        
    def make_request(self, user_params):
        self.cseq += 1

        nondialog_params = {
            "is_response": False,
            "call_id": self.call_id,
            "cseq": self.cseq,
            "maxfwd": MAXFWD,
            "hop": self.hop,
            "user_params": user_params.copy()
        }

        return safe_update(user_params, nondialog_params)


    def take_response(self, params, related_request):
        status = params["status"]
        
        if status.code == 401:
            # Let's try authentication! TODO: 407, too!
            auth = self.registration_manager.provide_auth(params, related_request)
                
            if auth:
                user_params = related_request["user_params"]
                related_request.clear()
                related_request.update(user_params)
                related_request.update(auth)
                
                self.registration_manager.logger.debug("Trying authorization...")
                #print("... with: %s" % related_request)
                self.send_request(related_request)
                return None
            else:
                self.registration_manager.logger.debug("Couldn't authorize, being rejected!")

        return params


    def send_request(self, user_params):
        params = self.make_request(user_params)
        self.registration_manager.transmit(params, None)


    def recv_response(self, msg, related_request):
        params = self.take_response(msg, related_request)
        if params:  # may have been retried
            self.process_response(params)


class RegistrationManager(Loggable):
    def __init__(self, switch):
        Loggable.__init__(self)

        self.switch = switch
        self.registrations_by_id = {}
        
        
    def provide_auth(self, params, related_request):
        return self.switch.provide_auth(params, related_request)
        

    def transmit(self, params, related_params=None):
        self.switch.send_message(params, related_params)


    def start_registration(self, registrar_uri, record_uri):
        route = None  # TODO: do we want to register using a Route?
        next_uri = route[0].uri if route else registrar_uri
        
        self.switch.select_hop_slot(next_uri).plug(
            self.start_registration_with_hop,
            registrar_uri=registrar_uri,
            record_uri=record_uri
        )
        
        
    def start_registration_with_hop(self, hop, registrar_uri, record_uri):
        self.logger.info("Registration hop resolved to %s" % (hop,))
        id = (registrar_uri, record_uri)
        registration = Registration(proxy(self), registrar_uri, record_uri, hop)
        self.registrations_by_id[id] = registration
        registration.update()


    def process_response(self, params, related_request):
        registrar_uri = related_request['uri']
        record_uri = related_request['to'].uri
        id = (registrar_uri, record_uri)
        registration = self.registrations_by_id.get(id)
        
        if registration:
            registration.recv_response(params, related_request)
        else:
            self.logger.warning("Ignoring response to unknown registration!")
