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
        self.request_slot = zap.EventSlot()
        self.response_slot = zap.EventSlot()

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
        
        
    def setup_incoming(self, request):
        # Forge To tag to make it possible for CANCEL-s to find this dialog
        request["to"].params["tag"] = self.local_tag

        self.local_nameaddr = request["to"]
        self.remote_nameaddr = request["from"]
        self.my_contact = self.make_my_contact(request["hop"])
        self.call_id = request["call_id"]
        
        self.peer_contact = first(request["contact"])
        self.route = request["record_route"]
        self.hop = request["hop"]

        self.dialog_manager.register_by_local_tag(self.local_tag, self)
        
        
    def setup_outgoing(self, request_uri, from_nameaddr, to_nameaddr, route, hop):
        self.local_nameaddr = from_nameaddr.tagged(self.local_tag)
        self.remote_nameaddr = to_nameaddr
        self.my_contact = self.make_my_contact(hop)
        self.call_id = generate_call_id()
        
        self.peer_contact = Nameaddr(request_uri)
        self.route = route or []
        self.hop = hop
        
        self.dialog_manager.register_by_local_tag(self.local_tag, self)


    def setup_outgoing_responded(self, response):
        self.remote_nameaddr = response["to"]
        
        self.peer_contact = first(response["contact"])
        self.route = list(reversed(response["record_route"]))


    def setup_outgoing_bastard(self, request):
        self.local_nameaddr = request["from"]
        self.remote_nameaddr = request["to"]
        self.peer_contact = Nameaddr(request["uri"])
        self.call_id = request["call_id"]
        self.last_sent_cseq = request["cseq"]
        self.hop = request["hop"]
        
        # Don't register, we already have a dialog with the same local tag!


    def bastard_reaction(self, request, response):
        # Multiple dialogs from a single INVITE request CAN'T be supported.
        # Doing so would make every outgoing call implicitly forkable into an
        # indefinite number of calls. That would allow a 2xx response to
        # arrive during the lifetime of the INVITE client request, so non-2xx
        # responses couldn't be reported until the transaction expires, that is
        # some 30 seconds. HOW. FUCKED. UP. IS. THAT.
        
        status = response["status"]
        
        if status.code >= 200 and status.code < 300:
            self.logger.warning("Initiating bastard reaction for status %s!" % status.code)
            
            bastard = Dialog(self.dialog_manager)
            bastard.setup_outgoing_bastard(request)
            bastard.setup_outgoing_responded(response)
            
            bastard.send_request(dict(method="ACK"), response)
            bastard.send_request(dict(method="BYE"))
        else:
            self.logger.warning("Skipping bastard reaction for status %s!" % status.code)


    def make_request(self, user_request, related_params=None):
        method = user_request["method"]
        
        if method == "CANCEL":
            raise Error("CANCEL should be out of dialog!")
        elif method == "ACK":
            cseq = related_params["cseq"]
        else:
            self.last_sent_cseq += 1
            cseq = self.last_sent_cseq

        dialog_request = {
            "is_response": False,
            "uri": self.peer_contact.uri,
            "from": self.local_nameaddr,
            "to": self.remote_nameaddr,
            "call_id": self.call_id,
            "cseq": cseq,
            "max_forwards": MAX_FORWARDS,
            "hop": self.hop,
            "user_request": user_request.copy()  # save original for auth retries
        }

        if method == "INVITE":
            dialog_request["contact"] = [ self.my_contact ]

        safe_update(user_request, dialog_request)

        return user_request


    def take_request(self, request):
        if request["is_response"]:
            raise Error("Not a request!")

        method = request["method"]
        from_nameaddr = request["from"]
        from_tag = from_nameaddr.params["tag"]
        to_nameaddr = request["to"]
        to_tag = to_nameaddr.params.get("tag")
        call_id = request["call_id"]
        cseq = request["cseq"]

        # The Cseq for CANCEL and ACK may be lower than the last received one
        if  method in ("CANCEL", "ACK"):
            pass
        elif self.last_recved_cseq is not None and cseq <= self.last_recved_cseq:
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

            # Even out-of-dialog CANCEL-s got a forged To tag if it got here.
            if to_tag != self.get_local_tag():
                raise Error("Mismatching local tag!")
                
            # TODO: 12.2 only re-INVITE-s modify the peer_contact, and nothing the route set
            peer_contact = first(request.get("contact"))
            if peer_contact:
                self.peer_contact = peer_contact
        else:
            if to_tag:
                raise Error("Unexpected to tag!")

            self.setup_incoming(request)

        return request


    def make_response(self, user_response, related_request):
        status = user_response["status"]
        if status.code == 100:
            raise Error("Eyy, 100 should be generated by the transaction layer!")
        
        dialog_response = {
            "is_response": True,
            "from": self.remote_nameaddr,
            "to": self.local_nameaddr,
            "call_id": self.call_id,
            "cseq": related_request["cseq"],
            "method": related_request["method"],  # only for internal use
            "hop": related_request["hop"]  # always use the request's hop, just in case
        }

        if dialog_response["method"] == "INVITE":
            dialog_response["contact"] = [ self.my_contact ]

        safe_update(user_response, dialog_response)

        return user_response


    def take_response(self, response, related_request):
        status = response["status"]
        from_nameaddr = response["from"]
        from_tag = from_nameaddr.params["tag"]
        to_nameaddr = response["to"]
        to_tag = to_nameaddr.params.get("tag")
        call_id = response["call_id"]

        if call_id != self.call_id:
            raise Error("Mismatching call id!")

        if from_tag != self.local_tag:
            raise Error("Mismatching local tag!")

        remote_tag = self.get_remote_tag()
        
        if remote_tag and to_tag != remote_tag:
            if related_request.method != "INVITE":
                raise Error("Mismatching remote tag!")
            else:
                return self.bastard_reaction(related_request, response)

        if status.code < 300:
            # Only successful or early responses create a dialog.
            # We don't get 100 responses, and any other response must contain a To tag.

            if not remote_tag:
                self.setup_outgoing_responded(response)
            else:
                # TODO: 12.2 only re-INVITE-s modify the peer_contact, and nothing the route set
                peer_contact = first(response.get("contact"))
                if peer_contact:
                    self.peer_contact = peer_contact
        elif status.code == 401:
            # Let's try authentication! TODO: 407, too!
            account = self.dialog_manager.get_remote_account(related_request["uri"])
            auth = account.provide_auth(response, related_request) if account else None
                
            if auth:
                # Retrying this request is a bit tricky, because our owner must
                # see the changes in case it wants to CANCEL it later. So we must modify
                # the same dict instead of creating a new one.
                user_request = related_request["user_request"]
                related_request.clear()
                related_request.update(user_request)
                related_request.update(auth)
                
                self.logger.debug("Trying authorization...")
                self.send_request(related_request)
                return None
            else:
                self.logger.debug("Couldn't authorize, being rejected!")

        return response


    def send_request(self, user_request, related_params=None):
        method = user_request["method"]
        
        if method == "CANCEL":
            # CANCELs are cloned from the INVITE in the transaction layer
            request = user_request
            request["is_response"] = False
        else:
            # Even 2xx ACKs are in-dialog
            request = self.make_request(user_request, related_params)
            
        self.dialog_manager.transmit(request, related_params)


    def send_response(self, user_response, related_request):
        #print("Will send response: %s" % str(user_params))
        response = self.make_response(user_response, related_request)
        self.dialog_manager.transmit(response, related_request)
        
        
    def recv_request(self, msg):
        request = self.take_request(msg)
        if request:  # may have been denied
            self.request_slot.zap(request)
    
    
    def recv_response(self, msg, related_request):
        response = self.take_response(msg, related_request)
        if response:  # may have been retried
            self.response_slot.zap(response)


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
        
        
    def process_request(self, request, related_params=None):
        method = request["method"]
        local_tag = request["to"].params.get("tag")
        dialog = self.dialogs_by_local_tag.get(local_tag)
        
        if dialog:
            self.logger.debug("Found dialog for %s request: %s" % (method, dialog.get_local_tag()))
            dialog.recv_request(request)
            return True

        if method == "CANCEL" and not local_tag and related_params:
            # TODO: use hop, too, for safety!
            forged_local_tag = related_params["to"].params.get("tag")
            
            if forged_local_tag:
                request["to"].params["tag"] = forged_local_tag
                dialog = self.dialogs_by_local_tag.get(forged_local_tag)
                
                if dialog:
                    self.logger.debug("Found dialog for INVITE CANCEL: %s" % forged_local_tag)
                    dialog.recv_request(request)
                    return True
            
        return False
        

    def process_response(self, response, related_request):
        method = response["method"]
        local_tag = response["from"].params.get("tag")
        dialog = self.dialogs_by_local_tag.get(local_tag)
        
        if dialog:
            self.logger.debug("Found dialog for %s response: %s" % (method, dialog.get_local_tag()))
            dialog.recv_response(response, related_request)
            return True

        return False
