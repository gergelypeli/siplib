from __future__ import print_function, unicode_literals, absolute_import

import uuid
import datetime
import collections
#import logging

from format import Nameaddr, Status
from async_base import WeakMethod, Weak
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
            seconds_left = int(expires) if expires is not None else 3600
            expiration = now + datetime.timedelta(seconds=seconds_left)
            hop = params["hop"]
            
            self.contacts_by_uri[uri] = RecordContact(expiration, hop)
            self.record_manager.logger.debug("Registered %s to %s via %s until %s." % (uri, self.record_uri, hop, expiration))
        
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
    def __init__(self, transmission):
        Loggable.__init__(self)

        self.transmission = transmission
        self.records_by_uri = {}
        
        
    def reject_request(self, msg, status):
        if msg:
            response = make_simple_response(msg, status)
            self.transmission(response, msg)


    def reject(self, code, reason):
        return WeakMethod(self.reject_request, Status(code, reason))
        
        
    def match_incoming_request(self, params):
        if params["method"] != "REGISTER":
            raise Error("RecordManager has nothing to do with this request!")
        
        registering_uri = params["from"].uri
        record_uri = params["to"].uri
        
        if registering_uri != record_uri:
            # Third party registrations not supported yet
            # We'd need to know which account is allowed to register which
            return self.reject(403, "Forbidden")
        
        if record_uri.scheme != "sip":
            return self.reject(404, "Not Found")
            
        record = self.records_by_uri.get(record_uri)
        if record:
            self.logger.debug("Found record: '%s'" % (record_uri,))
        else:
            self.logger.debug("Created record: %s" % (record_uri,))
            record = Record(Weak(self), record_uri)
            self.records_by_uri[record_uri] = record
        
        return WeakMethod(record.recv_request)
        
        
    def transmit(self, params, related_params=None, report_response=None):
        self.transmission(params, related_params, report_response)


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
    def __init__(self, registration_manager, registrar_uri, record_uri, contact_uri, hop=None):
        self.registration_manager = registration_manager

        self.registrar_uri = registrar_uri
        self.record_uri = record_uri
        self.contact_uri = contact_uri
        
        self.hop = hop or self.registration_manager.get_hop(registrar_uri)
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
        
        
    def process(self, params):
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
            #"uri": self.registrar_uri,
            #"from": user_params["from"],
            #"to": user_params["to"],
            #"contact": user_params["contact"],
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
        self.registration_manager.transmit(params, None, WeakMethod(self.recv_response))


    def recv_response(self, msg, related_request):
        params = self.take_response(msg, related_request)
        if params:  # may have been retried
            self.process(params)


class RegistrationManager(Loggable):
    def __init__(self, transmission, hopping, authing):
        Loggable.__init__(self)

        self.transmission = transmission
        self.hopping = hopping
        self.authing = authing
        self.registrations_by_id = {}
        
        
    def get_hop(self, uri):
        return self.hopping(uri)


    def provide_auth(self, params, related_request):
        return self.authing(params, related_request)
        

    def transmit(self, params, related_params=None, report_response=None):
        self.transmission(params, related_params, report_response)


    def start_registration(self, registrar_uri, record_uri, contact_uri, hop=None):
        id = (registrar_uri.print(), record_uri.print())
        registration = Registration(Weak(self), registrar_uri, record_uri, contact_uri, hop)
        self.registrations_by_id[id] = registration
        registration.update()
