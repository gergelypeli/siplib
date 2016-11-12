from __future__ import print_function, unicode_literals, absolute_import

import uuid
import datetime
import collections
from weakref import proxy

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


RecordContact = collections.namedtuple("RecordContact", [ "expiration", "hop" ])


class Record(object):
    def __init__(self, record_manager, record_uri):
        self.record_manager = record_manager
        self.record_uri = record_uri

        self.call_id = None
        self.cseq = None
        
        self.contacts_by_uri = {}


    def add_contact(self, uri, expiration, hop):
        self.contacts_by_uri[uri] = RecordContact(expiration, hop)
        self.record_manager.logger.debug("Registered %s to %s via %s until %s." % (uri, self.record_uri, hop, expiration))
        

    def get_contact_uris(self):
        return list(self.contacts_by_uri.keys())
        

    def get_contact_hops(self):
        return [ c.hop for c in self.contacts_by_uri.values() ]


    def get_contacts(self):
        return [ (k, v.hop) for k, v in self.contacts_by_uri.items() ]


    def process_updates(self, params):
        # TODO: check authname!
        now = datetime.datetime.now()

        # No contact is valid for just fetching the registrations
        for contact_nameaddr in params["contact"]:
            expires = contact_nameaddr.params.get("expires", params.get("expires"))
                
            uri = contact_nameaddr.uri
            seconds_left = int(expires) if expires is not None else 3600  # FIXME: proper default!
            expiration = now + datetime.timedelta(seconds=seconds_left)
            hop = params["hop"]
            
            self.add_contact(uri, expiration, hop)
        
        contact = []
        for uri, c in self.contacts_by_uri.items():
            seconds_left = int((c.expiration - now).total_seconds())
            contact.append(Nameaddr(uri=uri, params=dict(expires=seconds_left)))

        self.send_response(dict(status=Status(200, "OK"), contact=contact), params)


    def take_request(self, params):
        if params["is_response"]:
            raise Error("Not a request!")

        call_id = params["call_id"]
        cseq = params["cseq"]

        if call_id == self.call_id and cseq <= self.cseq:
            self.send_response(dict(status=Status(400, "Whatever")), params)
            return None

        self.call_id = call_id
        self.cseq = cseq

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


    def add_record(self, record_uri):
        record = self.records_by_uri.get(record_uri)
        
        if record:
            self.logger.debug("Found record: '%s'" % (record_uri,))
        else:
            self.logger.debug("Created record: %s" % (record_uri,))
            record = Record(proxy(self), record_uri)
            self.records_by_uri[record_uri] = record
            
        return record
        
        
    def process_request(self, params):
        if params["method"] != "REGISTER":
            raise Error("RecordManager has nothing to do with this request!")
        
        registering_uri = params["from"].uri
        record_uri = params["to"].uri
        
        if registering_uri != record_uri:
            # Third party registrations not supported yet
            # We'd need to know which account is allowed to register which
            self.reject_request(params, Status(403, "Forbidden"))
            return
        
        if record_uri.scheme != "sip":
            self.reject_request(params, Status(404, "Not Found"))
            return
            
        record = self.add_record(record_uri)
        record.recv_request(params)
        
        
    def transmit(self, params, related_params=None):
        self.switch.send_message(params, related_params)


    def emulate_registration(self, record_uri, contact_uri, seconds, hop):
        expiration = datetime.datetime.now() + datetime.timedelta(seconds=seconds)
        self.add_record(record_uri).add_contact(contact_uri, expiration, hop)
        

    def lookup_contact_uris(self, record_uri):
        record = self.records_by_uri.get(record_uri)
        
        if record:
            self.logger.debug("Found contact URIs for '%s'" % (record_uri,))
            return record.get_contact_uris()
        else:
            #self.logger.debug("Not found contact URIs: %s" % (record_uri,))
            return []


    def lookup_contact_hops(self, record_uri):
        record = self.records_by_uri.get(record_uri)
        
        if record:
            self.logger.debug("Found contact hops for '%s'" % (record_uri,))
            return record.get_contact_hops()
        else:
            #self.logger.debug("Not found contact hops: %s" % (record_uri,))
            return []


    def lookup_contacts(self, record_uri):
        record = self.records_by_uri.get(record_uri)
        
        if record:
            self.logger.debug("Found contacts for '%s'" % (record_uri,))
            return record.get_contacts()
        else:
            #self.logger.debug("Not found contact hops: %s" % (record_uri,))
            return []


class Registration(object):
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
            'contact': [ Nameaddr(self.contact_uri, params=dict(expires=300)) ]
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

        dialog_params = {
            "is_response": False,
            "call_id": self.call_id,
            "cseq": self.cseq,
            "maxfwd": MAXFWD,
            "hop": self.hop,
            "user_params": user_params.copy()
        }

        return safe_update(user_params, dialog_params)


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
