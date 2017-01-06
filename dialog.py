import uuid
from weakref import WeakValueDictionary

from format import Uri, Nameaddr
from log import Loggable
import zap


MAX_FORWARDS = 20

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
    

class Dialog(Loggable):
    def __init__(self, dialog_manager):
        Loggable.__init__(self)

        self.dialog_manager = dialog_manager
        self.report_slot = zap.EventSlot()

        self.local_tag = generate_tag()
    
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


    def get_remote_aor(self):
        return self.remote_nameaddr.uri.canonical_aor() if self.remote_nameaddr else None
                

    def get_remote_tag(self):
        return self.remote_nameaddr.params.get("tag") if self.remote_nameaddr else None


    def get_local_tag(self):
        return self.local_tag
        
        
    def get_call_id(self):
        return self.call_id

        
    def make_my_contact(self, hop):
        return Nameaddr(Uri(hop.local_addr))
        
        
    def setup_incoming(self, params):
        self.local_nameaddr = params["to"].tagged(self.local_tag)
        self.remote_nameaddr = params["from"]
        self.my_contact = self.make_my_contact(params["hop"])
        self.call_id = params["call_id"]
        
        self.peer_contact = first(params["contact"])
        self.route = params["record_route"]
        self.hop = params["hop"]

        self.dialog_manager.register_by_local_tag(self.local_tag, self)
        
        # Forge To tag to make it possible for CANCEL-s to find this dialog
        params["to"] = self.local_nameaddr
        
        
    def setup_outgoing(self, request_uri, from_nameaddr, to_nameaddr, route, hop):
        self.local_nameaddr = from_nameaddr.tagged(self.local_tag)
        self.remote_nameaddr = to_nameaddr
        self.my_contact = self.make_my_contact(hop)
        self.call_id = generate_call_id()
        
        self.peer_contact = Nameaddr(request_uri)
        self.route = route or []
        self.hop = hop
        
        self.dialog_manager.register_by_local_tag(self.local_tag, self)


    def setup_outgoing_responded(self, params):
        self.remote_nameaddr = params["to"]
        
        self.peer_contact = first(params["contact"])
        self.route = list(reversed(params["record_route"]))


    def setup_outgoing_bastard(self, invite_params):
        self.local_nameaddr = invite_params["from"]
        self.remote_nameaddr = invite_params["to"]
        self.peer_contact = Nameaddr(invite_params["uri"])
        self.call_id = invite_params["call_id"]
        self.last_sent_cseq = invite_params["cseq"]
        self.hop = invite_params["hop"]
        
        # Don't register, we already have a dialog with the same local tag!


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
            bastard.setup_outgoing_bastard(invite_params)
            bastard.setup_outgoing_responded(response_params)
            
            bastard.send_request(dict(method="ACK"), response_params)
            bastard.send_request(dict(method="BYE"))
        else:
            self.logger.warning("Skipping bastard reaction for status %s!" % status.code)


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
            "max_forwards": MAX_FORWARDS,
            "hop": self.hop,
            "user_params": user_params.copy()  # save original for auth retries
        }

        if method == "INVITE":
            dialog_params["contact"] = [ self.my_contact ]

        safe_update(user_params, dialog_params)

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

        remote_tag = self.get_remote_tag()
        if remote_tag:
            if call_id != self.call_id:
                raise Error("Mismatching call id!")

            if from_tag != remote_tag:
                raise Error("Mismatching remote tag!")

            # CANCEL-s may have not To tag
            if to_tag != self.get_local_tag() and not (method == "CANCEL" and not to_tag):
                raise Error("Mismatching local tag!")
                
            # TODO: 12.2 only re-INVITE-s modify the peer_contact, and nothing the route set
            peer_contact = first(params.get("contact"))
            if peer_contact:
                self.peer_contact = peer_contact
        else:
            if to_tag:
                raise Error("Unexpected to tag!")

            self.setup_incoming(params)

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
            "hop": request_params["hop"]  # always use the request's hop, just in case
        }

        if dialog_params["method"] == "INVITE":
            dialog_params["contact"] = [ self.my_contact ]

        safe_update(user_params, dialog_params)

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

        remote_tag = self.get_remote_tag()
        if remote_tag and to_tag != remote_tag:
            if related_request.method != "INVITE":
                raise Error("Mismatching remote tag!")
            else:
                return self.bastard_reaction(related_request, params)

        if status.code < 300:
            # Only successful or early responses create a dialog

            if not remote_tag:
                self.setup_outgoing_responded(params)
            else:
                # TODO: 12.2 only re-INVITE-s modify the peer_contact, and nothing the route set
                peer_contact = first(params.get("contact"))
                if peer_contact:
                    self.peer_contact = peer_contact
        elif status.code == 401:
            # Let's try authentication! TODO: 407, too!
            account = self.registration_manager.get_remote_account(params["to"].uri)
            auth = account.provide_auth(params, related_request) if account else None
                
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
            
        self.dialog_manager.transmit(params, related_params)


    def send_response(self, user_params, related_params=None):
        #print("Will send response: %s" % str(user_params))
        params = self.make_response(user_params, related_params)
        self.dialog_manager.transmit(params, related_params)
        
        
    def recv_request(self, msg):
        request = self.take_request(msg)
        if request:  # may have been denied
            self.report_slot.zap(request)
    
    
    def recv_response(self, msg, related_request):
        response = self.take_response(msg, related_request)
        if response:  # may have been retried
            self.report_slot.zap(response)


class DialogManager(Loggable):
    def __init__(self, switch):
        Loggable.__init__(self)

        self.switch = switch
        self.dialogs_by_local_tag = WeakValueDictionary()

        
    def get_remote_account(self, uri):
        return self.switch.get_remote_account(uri)
        
        
    def transmit(self, params, related_params=None):
        self.switch.send_message(params, related_params)


    def register_by_local_tag(self, local_tag, dialog):
        self.dialogs_by_local_tag[local_tag] = dialog
        self.logger.debug("Registered dialog with local tag %s" % (local_tag,))
        
        
    def find_dialog(self, params):
        local_aor = params["from"] if params["is_response"] else params["to"]
        local_tag = local_aor.params.get("tag")
        
        return self.dialogs_by_local_tag.get(local_tag)
        

    def process_request(self, params, related_params=None):
        method = params["method"]
        dialog = self.find_dialog(params)
        
        if dialog:
            self.logger.debug("Found dialog for %s request: %s" % (method, dialog.get_local_tag()))
            dialog.recv_request(params)
            return True

        if method == "CANCEL" and not params["to"].params.get("tag") and related_params:
            # TODO: use hop, too, for safety!
            forged_to_tag = related_params["to"].params.get("tag")
            
            if forged_to_tag:
                dialog = self.dialogs_by_local_tag.get(forged_to_tag)
                
                if dialog:
                    self.logger.debug("Found dialog for INVITE CANCEL: %s" % forged_to_tag)
                    dialog.recv_request(params)
                    return True
            
        return False
        

    def process_response(self, params, related_request):
        method = params["method"]
        dialog = self.find_dialog(params)
        
        if dialog:
            self.logger.debug("Found dialog for %s response: %s" % (method, dialog.get_local_tag()))
            dialog.recv_response(params, related_request)
            return True

        return False
