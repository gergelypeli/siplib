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

        self.local_sdp_session_id = Origin.generate_session_id()
        self.local_sdp_session_version = 0
        self.local_sdp_session_host = self.dialog_manager.get_local_addr()[0]
        self.remote_sdp_session_version = None
        
        self.dialog_manager.add_dialog(self)


    def uninvite(self, invite_params):
        self.local_nameaddr = invite_params["from"]
        self.remote_nameaddr = invite_params["to"]
        self.peer_contact = Nameaddr(invite_params["uri"])
        self.call_id = invite_params["call_id"]
        self.last_sent_cseq = invite_params["cseq"]


    def make_sdp(self, params):
        sdp = params.get("sdp")
        if sdp:
            self.local_sdp_session_version += 1
            sdp.origin = Origin(
                "-",
                self.local_sdp_session_id,
                self.local_sdp_session_version,
                "IN",
                "IP4",
                self.local_sdp_session_host
            )


    def take_sdp(self, params):
        sdp = params.get("sdp")
        if sdp:
            if sdp.origin.session_version == self.remote_sdp_session_version:
                params["sdp"] = None
                return
                
            self.remote_sdp_session_version = sdp.origin.session_version
    

    def make_request(self, user_params, related_params=None):
        method = user_params["method"]
        if method == "CANCEL":
            raise Error("CANCEL should be out of dialog!")
        elif method == "ACK":
            cseq = related_params["cseq"]
        else:
            self.last_sent_cseq += 1
            cseq = self.last_sent_cseq

        if not self.call_id:
            self.dialog_manager.remove_dialog(self)
            self.call_id = generate_call_id()
            self.dialog_manager.add_dialog(self)

        dialog_params = {
            "is_response": False,
            "uri": self.peer_contact.uri,
            "from": self.local_nameaddr,
            "to": self.remote_nameaddr,
            "call_id": self.call_id,
            "cseq": cseq,
            "maxfwd": MAXFWD
        }

        if method == "INVITE":
            dialog_params["contact"] = self.my_contact

        safe_update(user_params, dialog_params)
        self.make_sdp(user_params)

        return user_params  # Modified!


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

            if params["method"] == "CANCEL" and not to_tag:
                pass
            elif to_tag != local_tag:
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

        self.take_sdp(params)

        return params


    def make_response(self, user_params, request_params):
        status = user_params["status"]
        if status.code == 100:
            raise Error("Eyy, 100 is generated by the transaction layer!")
        
        dialog_params = {
            "is_response": True,
            "from": self.remote_nameaddr,
            "to": self.local_nameaddr,
            "call_id": self.call_id,
            "cseq": request_params["cseq"],
            "method": request_params["method"]  # only for internal use
        }

        if dialog_params["method"] == "INVITE":
            dialog_params["contact"] = self.my_contact

        safe_update(user_params, dialog_params)
        self.make_sdp(user_params)

        return user_params  # Modified!


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

            self.dialog_manager.remove_dialog(self)
            self.remote_nameaddr = to_nameaddr
            self.dialog_manager.add_dialog(self)

        if peer_contact:
            self.peer_contact = peer_contact

        self.take_sdp(params)

        return params


    def send_request(self, user_params, related_params=None, report=None):
        if user_params["method"] == "UNINVITE":
            self.uninvite(related_params)
        elif user_params["method"] == "CANCEL":
            # CANCELs are cloned from the INVITE in the transaction layer
            params = user_params
            params["is_response"] = False
        else:
            # Even 2xx ACKs are in-dialog
            params = self.make_request(user_params, related_params)
            
        self.dialog_manager.transmit(params, related_params, WeakMethod(self.recv_response, report))


    def send_response(self, user_params, related_params=None):
        print("Will send response: %s" % str(user_params))
        params = self.make_response(user_params, related_params)
        self.dialog_manager.transmit(params, related_params)
        
        
    def recv_request(self, msg):
        request = self.take_request(msg)
        self.report_request(request)
    
    
    def recv_response(self, msg, report):
        response = self.take_response(msg)
        report(response)
        

class DialogManager(object):
    def __init__(self, local_addr, transmission):
        self.local_addr = local_addr
        self.transmission = transmission
        self.dialogs_by_id = WeakValueDictionary()
        
        
    def get_local_addr(self):
        return self.local_addr


    def add_dialog(self, dialog):
        did = identify_dialog(dialog)
        self.dialogs_by_id[did] = dialog
        print("Added dialog %s" % (did,))


    def remove_dialog(self, dialog):
        did = identify_dialog(dialog)
        del self.dialogs_by_id[did]
        print("Removed dialog %s" % (did,))
        

    def handle_incoming_request(self, params):
        call_id, local_tag, remote_tag = did = identify_incoming_request(params)
        dialog = self.dialogs_by_id.get(did)
        
        if dialog:
            print("Found dialog: %s" % (did,))
            return WeakMethod(dialog.recv_request)

        print("No dialog %s" % (did,))

        if local_tag:
            print("In-dialog request has no dialog!")
            return None
            
        return None


    def transmit(self, params, related_params=None, report=None):
        self.transmission(params, related_params, report)


    def create_dialog(self, local_uri, local_name, remote_uri=None, proxy_addr=None):
        dialog = Dialog(Weak(self), local_uri, local_name, remote_uri, proxy_addr)
        self.add_dialog(dialog)
