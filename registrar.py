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


class Dialog(object):
    def __init__(self, dialog_manager, registrar, register_to, register_by, register_at, hop=None):
        self.dialog_manager = dialog_manager
        self.report = None

        self.registrar_uri = registrar
    
        # Things in the From/To fields
        self.register_to_nameaddr = register_to
        self.register_by_nameaddr = register_by
        self.register_at_nameaddr = register_at

        self.call_id = generate_call_id()
        self.last_sent_cseq = 0
        
        self.last_recved_call_id = None
        self.last_recved_cseq = None

        self.hop = hop or self.dialog_manager.get_hop(registrar)


    def set_report(self, report):
        self.report = report


    def set_local_credentials(self, cred):
        self.local_cred = cred


    def make_request(self, user_params):
        self.last_sent_cseq += 1
        cseq = self.last_sent_cseq

        dialog_params = {
            "is_response": False,
            "uri": self.registrar_uri,
            "from": self.register_by_nameaddr,
            "to": self.register_to_nameaddr,
            "contact": [ self.register_at_nameaddr ],
            "call_id": self.call_id,
            "cseq": cseq,
            "maxfwd": MAXFWD,
            "authorization": auth,
            "hop": self.hop
        }

        return safe_update(user_params, dialog_params)


    # TODO: the Record takes it
    def take_request(self, params):
        if params["is_response"]:
            raise Error("Not a request!")

        call_id = params["call_id"]
        cseq = params["cseq"]

        if call_id == self.last_recved_call_id and cseq < self.last_recved_cseq:
            return None

        self.last_recved_call_id = call_id
        self.last_recved_cseq = cseq
        
        return params


    # TODO: the Record makes it
    def make_response(self, user_params, related_request):
        dialog_params = {
            "is_response": True,
            "from": related_request["from"],
            "to": related_request["to"],
            "call_id": related_request["call_id"],
            "cseq": related_request["cseq"],
            "method": related_request["method"],
            "hop": related_request["hop"]
        }

        return safe_update(user_params, dialog_params)


    def take_response(self, params, related_request):
        status = params["status"]
        
        if status.code == 401:
            # Let's try authentication! TODO: 407, too!
            auth = self.dialog_manager.provide_auth(self.local_cred, params, related_request)
                
            if auth:
                print("Trying authorization...")
                user_params = dict(method=related_request["method"], authorization=auth)
                self.send_request(user_params)
                return None
            else:
                print("Couldn't authorize, being rejected!")

        return params


    def send_request(self, user_params):
        params = self.make_request(user_params)
            
        self.dialog_manager.transmit(params, None, WeakMethod(self.recv_response))


    # TODO: the Record
    def send_response(self, user_params, related_request):
        print("Will send response: %s" % str(user_params))
        params = self.make_response(user_params, related_request)
        self.dialog_manager.transmit(params, related_request)
        
        
    # TODO: the Record
    def recv_request(self, msg):
        request = self.take_request(msg)
        if request:  # may have been denied
            self.report(request)
    
    
    def recv_response(self, msg, related_request):
        response = self.take_response(msg, related_request)
        if response:  # may have been retried
            self.report(response)


class DialogManager(object):
    def __init__(self, transmission, hopping, authing):
        self.transmission = transmission
        self.hopping = hopping
        self.authing = authing
        self.dialogs_by_id = WeakValueDictionary()  # weak? id?
        
        
    def get_hop(self, uri):
        return self.hopping(uri)


    def provide_auth(self, cred, params, related_request):
        return self.authing(cred, params, related_request)
        

    def match_incoming_request(self, params):
        did = params["to"]
        dialog = self.dialogs_by_id.get(did)
        
        if dialog:
            print("Found dialog: %s" % (did,))
            return WeakMethod(dialog.recv_request)

        #print("No dialog %s" % (did,))
        return None
        
        
    def transmit(self, params, related_params=None, report_response=None):
        self.transmission(params, related_params, report_response)
