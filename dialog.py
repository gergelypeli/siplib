from __future__ import print_function, unicode_literals, absolute_import

import uuid
from weakref import WeakValueDictionary
#import logging

from format import Uri, Nameaddr
from sdp import Origin
from async_base import WeakMethod
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
    

class Dialog(Loggable):
    def __init__(self, dialog_manager):
        Loggable.__init__(self)

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
        self.local_sdp_session_host = self.dialog_manager.get_local_addr().resolve()[0]
        self.remote_sdp_session_version = None


    def set_report(self, report):
        self.report = report


    def is_established(self):
        return self.remote_nameaddr and "tag" in self.remote_nameaddr.params;
        
        
    def fix_hop(self, hop):
        next_uri = self.route[0].uri if self.route else self.peer_contact.uri
        is_next_uri_usable = True  # TODO: not if URI is local while hop is public
        
        if is_next_uri_usable:
            self.logger.debug("Next URI usable, resolving to hop.")
            self.hop = self.dialog_manager.get_hop(next_uri)
        else:
            self.logger.debug("Next URI fishy, using network hop.")
            self.hop = hop
        

    def setup_incoming(self, params):
        self.local_nameaddr = params["to"].tagged(generate_tag())
        self.remote_nameaddr = params["from"]
        self.my_contact = self.dialog_manager.get_my_contact()  # TODO: improve
        self.call_id = params["call_id"]
        
        self.peer_contact = first(params["contact"])
        self.route = params["record_route"]
        self.fix_hop(params["hop"])

        self.dialog_manager.dialog_established(self)
        
        
    def setup_outgoing(self, request_uri, from_nameaddr, to_nameaddr, route=None, hop=None):
        self.local_nameaddr = from_nameaddr.tagged(generate_tag())
        self.remote_nameaddr = to_nameaddr
        self.my_contact = self.dialog_manager.get_my_contact()  # TODO: improve
        self.call_id = generate_call_id()
        
        self.peer_contact = Nameaddr(request_uri)
        self.route = route or []
        self.fix_hop(hop)


    def setup_outgoing2(self, params):
        self.remote_nameaddr = params["to"]
        
        self.peer_contact = first(params["contact"])
        self.route = list(reversed(params["record_route"]))
        self.fix_hop(params["hop"])
        
        self.dialog_manager.dialog_established(self)


    def setup_bastard(self, invite_params):
        self.local_nameaddr = invite_params["from"]
        self.remote_nameaddr = invite_params["to"]
        self.peer_contact = Nameaddr(invite_params["uri"])
        self.call_id = invite_params["call_id"]
        self.last_sent_cseq = invite_params["cseq"]


    def bastard_reaction(self, invite_params, response_params):
        # Multiple dialogs from a single INVITE request CAN'T be supported.
        # Doing so would make every outgoing call implicitly forkable into an
        # indefinite number of calls. That would allow a 2xx response to
        # arrive during the lifetime of the INVITE client request, so non-2xx
        # responses couldn't be reported until the transaction expires, that is
        # some 30 seconds. HOW. FUCKED. UP. IS. THAT.
        
        status = response_params["status"]
        
        if status.code >= 200 and status.code < 300:
            self.logger.warning("Initiating bastard reaction for status %s!" % status.code)
            
            bastard = Dialog(self.dialog_manager)
            bastard.setup_bastard(invite_params)
            bastard.setup_outgoing2(response_params)
            
            bastard.send_request(dict(method="ACK"), response_params)
            bastard.send_request(dict(method="BYE"))
        else:
            self.logger.warning("Skipping bastard reaction for status %s!" % status.code)


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

        method = params["method"]
        from_nameaddr = params["from"]
        from_tag = from_nameaddr.params["tag"]
        to_nameaddr = params["to"]
        to_tag = to_nameaddr.params.get("tag")
        call_id = params["call_id"]
        cseq = params["cseq"]

        # The Cseq for CANCEL and ACK may be lower than the last received one
        if self.last_recved_cseq is not None and cseq <= self.last_recved_cseq and method not in ("CANCEL", "ACK"):
            self.logger.debug("Dropping out of order request with Cseq %d" % cseq)
            return None
        else:
            self.last_recved_cseq = cseq

        if self.is_established():
            if call_id != self.call_id:
                raise Error("Mismatching call id!")

            if from_tag != self.remote_nameaddr.params["tag"]:
                raise Error("Mismatching remote tag!")

            # CANCEL-s may have not To tag
            if to_tag != self.local_nameaddr.params["tag"] and not (method == "CANCEL" and not to_tag):
                raise Error("Mismatching local tag!")
                
            # TODO: 12.2 only re-INVITE-s modify the peer_contact, and nothing the route set
            peer_contact = first(params.get("contact"))
            if peer_contact:
                self.peer_contact = peer_contact
                self.fix_hop(params["hop"])
        else:
            if to_tag:
                raise Error("Unexpected to tag!")

            self.setup_incoming(params)

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
            "hop": request_params["hop"]
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

        if call_id != self.call_id:
            raise Error("Mismatching call id!")

        if from_tag != self.local_nameaddr.params["tag"]:
            raise Error("Mismatching local tag!")

        if self.is_established() and to_tag != self.remote_nameaddr.params["tag"]:
            if related_request.method != "INVITE":
                raise Error("Mismatching remote tag!")
            else:
                return self.bastard_reaction(related_request, params)

        if status.code < 300:
            # Only successful or early responses create a dialog

            if not self.is_established():
                self.setup_outgoing2(params)
            else:
                # TODO: 12.2 only re-INVITE-s modify the peer_contact, and nothing the route set
                peer_contact = first(params.get("contact"))
                if peer_contact:
                    self.peer_contact = peer_contact
                    self.fix_hop(params["hop"])
        elif status.code == 401:
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
                
                self.logger.debug("Trying authorization...")
                self.send_request(related_request)
                return None
            else:
                self.logger.debug("Couldn't authorize, being rejected!")

        self.take_sdp(params)

        return params


    def send_request(self, user_params, related_params=None):
        method = user_params["method"]
        
        if method == "CANCEL":
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


class DialogManager(Loggable):
    def __init__(self, local_addr, transmission, hopping, authing):
        Loggable.__init__(self)

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
        self.logger.debug("Established dialog %s" % (did,))
        

    def match_incoming_request(self, params):
        call_id, local_tag, remote_tag = did = identify_incoming_request(params)
        dialog = self.dialogs_by_id.get(did)
        
        if dialog:
            self.logger.debug("Found dialog: %s" % (did,))
            return WeakMethod(dialog.recv_request)

        #print("No dialog %s" % (did,))

        if local_tag:
            self.logger.debug("In-dialog request has no dialog!")
            return None
            
        return None
        
        
    def transmit(self, params, related_params=None, report_response=None):
        self.transmission(params, related_params, report_response)
