from __future__ import print_function, unicode_literals, absolute_import

from pprint import pprint, pformat
import uuid
import datetime
from weakref import proxy as Weak, WeakValueDictionary

import format
from format import Addr, Uri, Nameaddr, Via, Status
from sdp import generate_session_id, Origin
from async import WeakMethod


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


class Record(object):
    def __init__(self, record_manager, record_uri):
        self.record_manager = record_manager
        self.record_uri = record_uri

        self.call_id = None
        self.cseq = None
        
        self.expirations_by_contact_uri = {}


    def process_updates(self, params):
        # TODO: check authname!
        now = datetime.datetime.now()

        for contact_nameaddr in params["contact"]:
            expires = contact_nameaddr.params.get("expires", params.get("expires"))
                
            uri = contact_nameaddr.uri.print()
            seconds_left = int(expires) if expires is not None else 3600
            expiration = now + datetime.timedelta(seconds=seconds_left)
            
            self.expirations_by_contact_uri[uri] = expiration
            print("Registered %s to %s for %s seconds." % (uri, self.record_uri, expiration))
        
        contact = []
        for uri, expiration in self.expirations_by_contact_uri.items():
            seconds_left = int((expiration - now).total_seconds())
            uri = Uri.parse(uri)
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

        if not params["contact"]:
            self.send_response(dict(status=Status(400, "Whatever")), params)
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
        print("Will send response: %s" % str(user_params))
        params = self.make_response(user_params, related_request)
        self.record_manager.transmit(params, related_request)
        
        
    def recv_request(self, msg):
        params = self.take_request(msg)
        if params:
            self.process_updates(params)


class RecordManager(object):
    def __init__(self, transmission):
        self.transmission = transmission
        self.records_by_id = {}
        
        
    def match_incoming_request(self, params):
        if params["method"] != "REGISTER":
            raise Error("RecordManager has nothing to do with this!")
            
        uri = params["to"].uri
        id = uri.print()
        record = self.records_by_id.get(id)
        
        if record:
            print("Found record: %s" % id)
        else:
            print("Created record: %s" % id)
            record = Record(Weak(self), uri)
            self.records_by_id[id] = record
            
        return WeakMethod(record.recv_request)
        
        
    def transmit(self, params, related_params=None, report_response=None):
        self.transmission(params, related_params, report_response)


    def lookup_contact_uris(self, record_uri):
        id = record_uri.print()
        record = self.records_by_id.get(id)
        
        if record:
            return [ Uri.parse(uri) for uri in record.expirations_by_contact_uri ]
        else:
            return None
    
    
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

        print("Registration total: %s" % total)
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
                
                print("Trying authorization...")
                #print("... with: %s" % related_request)
                self.send_request(related_request)
                return None
            else:
                print("Couldn't authorize, being rejected!")

        return params


    def send_request(self, user_params):
        params = self.make_request(user_params)
        self.registration_manager.transmit(params, None, WeakMethod(self.recv_response))


    def recv_response(self, msg, related_request):
        params = self.take_response(msg, related_request)
        if params:  # may have been retried
            self.process(params)


class RegistrationManager(object):
    def __init__(self, transmission, hopping, authing):
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
