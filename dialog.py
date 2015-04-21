from __future__ import print_function, unicode_literals, absolute_import

from pprint import pprint, pformat
import uuid
import datetime

import format
from format import Addr, Uri, Nameaddr, Via, Status
from async import WeakMethod


class Error(Exception):
    pass


def safe_update(target, source):
    for k, v in source.items():
        if k in target:
            raise Error("Can't overwrite field %r!" % k)
        target[k] = v

    return target


def generate_tag():
    return uuid.uuid4().hex[:8]


def generate_call_id():
    return uuid.uuid4().hex[:8]


def identify_dialog(dialog):
    call_id = dialog.call_id
    local_tag = dialog.local_nameaddr.params.get("tag")
    remote_tag = dialog.remote_nameaddr.params.get("tag")
        
    did = (call_id, local_tag, remote_tag)
    return did


def identify_incoming_request(params):
    call_id = params["call_id"]
    remote_tag = params["from"].params.get("tag")
    local_tag = params["to"].params.get("tag")
    
    did = (call_id, local_tag, remote_tag)
    return did
    

class Dialog(object):
    def __init__(self, dialog_manager, report_request, local_uri, local_name, remote_uri=None, proxy_addr=None):
        self.dialog_manager = dialog_manager
        self.report_request = report_request
    
        # Things in the From/To fields
        self.local_nameaddr = Nameaddr(local_uri, local_name, dict(tag=generate_tag()))
        self.remote_nameaddr = Nameaddr(remote_uri)

        self.my_contact = Nameaddr(local_uri)
        # The peer's contact address, received in Contact, sent in RURI
        self.peer_contact = Nameaddr(remote_uri)
        self.proxy_addr = proxy_addr

        self.call_id = None
        self.routes = []
        self.last_sent_cseq = 0


    def make_request(self, user_params):
        self.last_sent_cseq += 1

        if not self.call_id:
            self.dialog_manager.remove_dialog(self)
            self.call_id = generate_call_id()
            self.dialog_manager.add_dialog(self)

        params = {
            "is_response": False,
            "uri": self.peer_contact.uri,
            "from": self.local_nameaddr,
            "to": self.remote_nameaddr,
            "call_id": self.call_id,
            "cseq": self.last_sent_cseq,
            "maxfwd": 50
        }

        if user_params["method"] == "INVITE":
            params["contact"] = self.my_contact

        safe_update(params, user_params)

        return params


    def take_request(self, params):
        if "uri" not in params:
            raise Error("Not a request!")

        from_nameaddr = params["from"]
        from_tag = from_nameaddr.params["tag"]
        to_nameaddr = params["to"]
        to_tag = to_nameaddr.params.get("tag", None)
        call_id = params["call_id"]
        local_tag = self.local_nameaddr.params["tag"]
        remote_tag = self.remote_nameaddr.params.get("tag")
        peer_contact = params.get("contact", None)
        #cseq = params["cseq"]  # TODO: use?

        if to_nameaddr.uri != self.local_nameaddr.uri:
            raise Error("Mismatching recipient: %s %s" % (to_nameaddr.uri, self.local_nameaddr.uri))

        if remote_tag:
            if call_id != self.call_id:
                raise Error("Mismatching call id!")

            if from_tag != remote_tag:
                raise Error("Mismatching remote tag!")

            if to_tag != local_tag:
                raise Error("Mismatching local tag!")
        else:
            if to_tag:
                raise Error("Unexpected to tag!")

            self.dialog_manager.remove_dialog(self)
            self.remote_nameaddr = from_nameaddr
            self.call_id = call_id
            self.dialog_manager.add_dialog(self)

        if peer_contact:
            self.peer_contact = peer_contact

        return params


    def make_response(self, user_params, request_params):
        status = user_params["status"]
        to = request_params["to"]
        if status.code != 100:
            to = Nameaddr(to.uri, to.name, dict(to.params, tag=self.local_nameaddr.params["tag"]))

        params = {
            "is_response": True,
            "from": request_params["from"],
            "to": to,
            "call_id": request_params["call_id"],
            "cseq": request_params["cseq"],
            "via": request_params["via"],
            "method": request_params["method"]  # only for internal use
        }

        if params["method"] == "INVITE":
            params["contact"] = self.my_contact

        safe_update(params, user_params)

        return params


    def take_response(self, params):
        if "status" not in params:
            raise Error("Not a response!")

        from_nameaddr = params["from"]
        from_tag = from_nameaddr.params["tag"]
        to_nameaddr = params["to"]
        to_tag = to_nameaddr.params.get("tag", None)
        call_id = params["call_id"]
        local_tag = self.local_nameaddr.params["tag"]
        remote_tag = self.remote_nameaddr.params.get("tag")
        peer_contact = params.get("contact", None)

        if from_nameaddr != self.local_nameaddr:
            raise Error("Mismatching recipient!")

        if remote_tag:
            if call_id != self.call_id:
                raise Error("Mismatching call id!")

            if to_tag != remote_tag:
                raise Error("Mismatching remote tag!")

            if from_tag != local_tag:
                raise Error("Mismatching local tag!")
        else:
            if not to_tag:
                raise Error("Missing to tag!")

            self.remote_tag = from_tag

        if peer_contact:
            self.peer_contact = peer_contact

        return params


    def send_request(self, user_params, related_params=None, report=None):
        if user_params["method"] in ("ACK", "CANCEL"):
            # These requests are cloned from the INVITE in the transaction layer
            params = dict(user_params, is_response=False)
        else:
            params = self.make_request(user_params)
            
        self.dialog_manager.transmit(params, related_params, WeakMethod(self.recv_response, report))


    def send_response(self, user_params, related_params=None):
        params = self.make_response(user_params, related_params)
        self.dialog_manager.transmit(params, related_params)
        
        
    def recv_request(self, msg):
        request = self.take_request(msg)
        self.report_request(request)
    
    
    def recv_response(self, msg, report):
        response = self.take_response(msg)
        report(response)
        

class DialogManager(object):
    def __init__(self, transmission):
        self.dialogs_by_id = {}
        self.transmission = transmission


    def add_dialog(self, dialog):
        did = identify_dialog(dialog)
        self.dialogs_by_id[did] = dialog
        print("Added dialog %s" % (did,))


    def remove_dialog(self, dialog):
        did = identify_dialog(dialog)
        del self.dialogs_by_id[did]
        print("Removed dialog %s" % (did,))
        

    def auth_invite(self, uri):
        # Accept everything for now
        wself = weakref.proxy(self)
        dialog = Dialog(wself, uri, "Lo Cal")  # TODO: dialogs are created by INVITE responses!
        return dialog
        
    
    def handle_incoming_message(self, params):
        if params["is_response"]:
            print("Response not handled in DialogManager.")
            return None  # Oops
        
        call_id, local_tag, remote_tag = did = identify_incoming_request(params)
        dialog = self.dialogs_by_id.get(did)
        
        if dialog:
            print("Found dialog: %s" % (did,))
            return WeakMethod(dialog.recv_request)

        print("No dialog %s" % (did,))

        if local_tag:
            print("In-dialog request has no dialog.")
            return None
            
        if params["method"] == "INVITE":
            uri = params["uri"]
            
            dialog = self.auth_invite(uri)
            self.add_dialog(dialog)
            # TODO: send a proper message if rejected
            
            return WeakMethod(dialog.recv_request)
            
        return None


    def transmit(self, params, related_params=None, report=None):
        self.transmission(params, related_params, report)


    def create_dialog(self, local_uri, local_name, remote_uri=None, proxy_addr=None):
        wself = weakref.proxy(self)
        dialog = Dialog(wself, local_uri, local_name, remote_uri, proxy_addr)
        self.add_dialog(dialog)
