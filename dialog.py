from __future__ import print_function, unicode_literals, absolute_import

import uuid
from weakref import WeakValueDictionary
import logging

from format import Uri, Nameaddr
from sdp import Origin
from async import WeakMethod
from util import resolve

MAXFWD = 50
logger = logging.getLogger(__name__)


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


def first(x):
    return x[0] if x else None
    

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
    def __init__(self, dialog_manager):
        self.dialog_manager = dialog_manager
        self.report = None
    
        # Things in the From/To fields
        self.local_nameaddr = None
        self.remote_nameaddr = None

        self.my_contact = None  # may depend on stuff
        # The peer's contact address, received in Contact, sent in RURI
        self.peer_contact = None

        self.route = []
        self.hop = None

        self.call_id = None
        self.last_sent_cseq = 0
        self.last_recved_cseq = None

        self.local_sdp_session_id = Origin.generate_session_id()
        self.local_sdp_session_version = 0
        self.local_sdp_session_host = resolve(self.dialog_manager.get_local_addr())[0]
        self.remote_sdp_session_version = None


    def set_report(self, report):
        self.report = report


    def is_established(self):
        return self.remote_nameaddr and "tag" in self.remote_nameaddr.params;
        

    def setup_incoming(self, params):
        self.local_nameaddr = params["to"].tagged(generate_tag())
        self.remote_nameaddr = params["from"]
        self.my_contact = self.dialog_manager.get_my_contact()  # TODO: improve
        self.peer_contact = first(params["contact"])
        
        self.route = params["record_route"]
        self.hop = params["hop"]
        self.call_id = params["call_id"]

        self.dialog_manager.dialog_established(self)
        
        
    def setup_outgoing(self, request_uri, from_nameaddr, to_nameaddr, route=None, hop=None):
        self.local_nameaddr = from_nameaddr.tagged(generate_tag())
        self.remote_nameaddr = to_nameaddr
        self.my_contact = self.dialog_manager.get_my_contact()  # TODO: improve
        self.peer_contact = Nameaddr(request_uri)
        
        self.route = route or []
        self.hop = hop or self.dialog_manager.get_hop(route[0].uri if route else request_uri)
        self.call_id = generate_call_id()


    def setup_outgoing2(self, params):
        self.remote_nameaddr = params["to"]
        self.peer_contact = first(params["contact"])
        self.route = reversed(params["record_route"])
        
        self.dialog_manager.dialog_established(self)
        

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
            if self.remote_sdp_session_version is not None and sdp.origin.session_version <= self.remote_sdp_session_version:
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

        dialog_params = {
            "is_response": False,
            "uri": self.peer_contact.uri,
            "from": self.local_nameaddr,
            "to": self.remote_nameaddr,
            "call_id": self.call_id,
            "cseq": cseq,
            "maxfwd": MAXFWD,
            "hop": self.hop,
            "user_params": user_params.copy()  # save original for auth retries
        }

        if method == "INVITE":
            dialog_params["contact"] = [ self.my_contact ]

        safe_update(user_params, dialog_params)
        self.make_sdp(user_params)

        return user_params


    def take_request(self, params):
        if params["is_response"]:
            raise Error("Not a request!")

        from_nameaddr = params["from"]
        from_tag = from_nameaddr.params["tag"]
        to_nameaddr = params["to"]
        to_tag = to_nameaddr.params.get("tag")
        call_id = params["call_id"]
        cseq = params["cseq"]
        peer_contact = first(params["contact"])

        if self.last_recved_cseq is not None and cseq < self.last_recved_cseq:
            return None
        else:
            self.last_recved_cseq = cseq

        if self.is_established():
            if call_id != self.call_id:
                raise Error("Mismatching call id!")

            if from_tag != self.remote_nameaddr.params["tag"]:
                raise Error("Mismatching remote tag!")

            if to_tag != self.local_nameaddr.params["tag"]:
                raise Error("Mismatching local tag!")
        else:
            if to_tag:
                raise Error("Unexpected to tag!")

            self.setup_incoming(params)

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
            "method": request_params["method"],  # only for internal use
            "hop": self.hop
        }

        if dialog_params["method"] == "INVITE":
            dialog_params["contact"] = [ self.my_contact ]

        safe_update(user_params, dialog_params)
        self.make_sdp(user_params)

        return user_params


    def take_response(self, params, related_request):
        status = params["status"]
        
        from_nameaddr = params["from"]
        from_tag = from_nameaddr.params["tag"]
        to_nameaddr = params["to"]
        to_tag = to_nameaddr.params.get("tag")
        call_id = params["call_id"]
        peer_contact = first(params["contact"])

        if from_nameaddr != self.local_nameaddr:
            raise Error("Mismatching recipient, from %s, local %s!" % (from_nameaddr, self.local_nameaddr))

        if self.is_established():
            if call_id != self.call_id:
                raise Error("Mismatching call id!")

            if from_tag != self.local_nameaddr.params["tag"]:
                raise Error("Mismatching local tag!")

            if to_tag != self.remote_nameaddr.params["tag"]:
                raise Error("Mismatching remote tag!")
        elif status.code < 300 and to_tag:
            # Failure responses don't establish a dialog (including 401/407).
            # Also, provisional responses may not have a to tag.
            
            self.setup_outgoing2(params)
            
        if status.code == 401:
            # Let's try authentication! TODO: 407, too!
            auth = self.dialog_manager.provide_auth(params, related_request)
                
            if auth:
                # Retrying this request is a bit tricky, because our owner must
                # see the changes in case it wants to CANCEL it later. So we must modify
                # the same dict instead of creating a new one.
                user_params = related_request["user_params"]
                related_request.clear()
                related_request.update(user_params)
                related_request.update(auth)
                
                logger.debug("Trying authorization...")
                self.send_request(related_request)
                return None
            else:
                logger.debug("Couldn't authorize, being rejected!")

        if peer_contact:
            self.peer_contact = peer_contact

        self.take_sdp(params)

        return params


    def send_request(self, user_params, related_params=None):
        method = user_params["method"]
        
        if method == "UNINVITE":
            self.uninvite(related_params)
        elif method == "CANCEL":
            # CANCELs are cloned from the INVITE in the transaction layer
            params = user_params
            params["is_response"] = False
            
        else:
            # Even 2xx ACKs are in-dialog
            params = self.make_request(user_params, related_params)
            
        self.dialog_manager.transmit(params, related_params, WeakMethod(self.recv_response))


    def send_response(self, user_params, related_params=None):
        #print("Will send response: %s" % str(user_params))
        params = self.make_response(user_params, related_params)
        self.dialog_manager.transmit(params, related_params)
        
        
    def recv_request(self, msg):
        request = self.take_request(msg)
        if request:  # may have been denied
            self.report(request)
    
    
    def recv_response(self, msg, related_request):
        response = self.take_response(msg, related_request)
        if response:  # may have been retried
            self.report(response)


class DialogManager(object):
    def __init__(self, local_addr, transmission, hopping, authing):
        self.local_addr = local_addr
        self.transmission = transmission
        self.hopping = hopping
        self.authing = authing
        self.dialogs_by_id = WeakValueDictionary()
        
        
    def get_local_addr(self):
        return self.local_addr


    def get_my_contact(self):
        return Nameaddr(Uri(self.local_addr))  # TODO: more flexible?
        
        
    def get_hop(self, uri):
        return self.hopping(uri)


    def provide_auth(self, params, related_request):
        return self.authing(params, related_request)
        

    def dialog_established(self, dialog):
        did = identify_dialog(dialog)
        self.dialogs_by_id[did] = dialog
        logger.debug("Established dialog %s" % (did,))
        

    def match_incoming_request(self, params):
        call_id, local_tag, remote_tag = did = identify_incoming_request(params)
        dialog = self.dialogs_by_id.get(did)
        
        if dialog:
            logger.debug("Found dialog: %s" % (did,))
            return WeakMethod(dialog.recv_request)

        #print("No dialog %s" % (did,))

        if local_tag:
            logger.debug("In-dialog request has no dialog!")
            return None
            
        return None
        
        
    def transmit(self, params, related_params=None, report_response=None):
        self.transmission(params, related_params, report_response)
