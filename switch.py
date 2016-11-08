from weakref import proxy

from format import Status
from transport import UdpTransport
from transactions import TransactionManager, make_simple_response
from dialog import Dialog, DialogManager
from leg import Routing, Bridge, RecordingBridge
from leg_sip import SipLeg
from ground import Ground, Call
from authority import Authority
from registrar import RegistrationManager, RecordManager
from account import Account, AccountManager
from util import build_oid, Loggable
from mgc import Controller


class Switch(Loggable):
    def __init__(self, local_addr,
        transport=None, transaction_manager=None,
        record_manager=None, authority=None, registration_manager=None,
        dialog_manager=None, mgc=None, account_manager=None
    ):
        Loggable.__init__(self)
        local_addr.assert_resolved()

        self.calls_by_oid = {}
        self.call_count = 0

        self.local_addr = local_addr
        
        self.transport = transport or UdpTransport(
            self.local_addr,
        )
        self.transaction_manager = transaction_manager or TransactionManager(
            self.local_addr, proxy(self.transport)
        )
        self.record_manager = record_manager or RecordManager(
            proxy(self)
        )
        self.authority = authority or Authority(
        )
        self.registration_manager = registration_manager or RegistrationManager(
            proxy(self)
        )
        self.dialog_manager = dialog_manager or DialogManager(
            local_addr,
            proxy(self)
        )
        self.mgc = mgc or Controller(
        )
        self.account_manager = account_manager or AccountManager(
        )
        self.ground = Ground(proxy(self.mgc))
        
        self.transaction_manager.request_slot.plug(self.process_request)
        self.transaction_manager.response_slot.plug(self.process_response)


    def set_oid(self, oid):
        Loggable.set_oid(self, oid)

        self.account_manager.set_oid(build_oid(oid, "accman"))
        self.authority.set_oid(build_oid(oid, "authority"))
        self.record_manager.set_oid(build_oid(oid, "recman"))
        self.registration_manager.set_oid(build_oid(oid, "regman"))
        self.transport.set_oid(build_oid(oid, "transport"))
        self.transaction_manager.set_oid(build_oid(oid, "transman"))
        self.dialog_manager.set_oid(build_oid(oid, "diaman"))
        self.mgc.set_oid(build_oid(oid, "mgc"))
        self.ground.set_oid(build_oid(oid, "ground"))


    def set_name(self, name):
        self.mgc.set_name(name)
        
        
    def select_hop(self, uri):
        return self.transport.select_hop(uri)
        

    def provide_auth(self, response, request):
        creds = self.account_manager.get_our_credentials()
        return self.authority.provide_auth(response, request, creds)
        
        
    def send_message(self, msg, related_msg=None):
        return self.transaction_manager.send_message(msg, related_msg)
        
    
    def reject_request(self, request, status):
        response = make_simple_response(request, status)
        self.transaction_manager.send_message(response, request)


    def challenge_request(self, msg, challenge):
        if msg:
            response = make_simple_response(msg, Status(401, "Hey"), challenge)
            self.transaction_manager.send_message(response, msg)


    def make_media_leg(self, type):
        return self.mgc.make_media_leg(type)
        

    def make_thing(self, type):
        if type == "routing":
            return Routing()
        elif type == "sip":
            return SipLeg(Dialog(proxy(self.dialog_manager)))
        elif type == "bridge":
            return Bridge()
        elif type == "record":
            return RecordingBridge()
        else:
            raise Exception("Unknown leg type '%s'!" % type)


    def make_call(self):
        return Call(proxy(self), proxy(self.ground))
        
        
    def start_call(self, incoming_leg):
        call = self.make_call()
        
        oid = build_oid(self.oid, "call", self.call_count)
        self.call_count += 1
        call.set_oid(oid)
        
        self.calls_by_oid[oid] = call

        call.start(incoming_leg)


    def start_sip_call(self, params):
        incoming_dialog = Dialog(proxy(self.dialog_manager))
        incoming_leg = SipLeg(incoming_dialog)
        self.start_call(incoming_leg)
        
        # The dialog must be fed directly, since the request contains no local tag yet.
        incoming_dialog.recv_request(params)
        

    def call_finished(self, call):
        self.logger.debug("Finishing call %s" % call.oid)
        self.calls_by_oid.pop(call.oid)
        
        
    def auth_request(self, params):
        method = params["method"]
        from_uri = params["from"].uri
        hop = params["hop"]

        auth_policy = self.account_manager.get_account_auth_policy(from_uri)
        
        if method in ("CANCEL", "ACK"):
            self.logger.debug("Accepting request because it can't be authenticated anyway")
            return False
        elif method == "PRACK":
            # TODO: this is only for debugging
            self.logger.debug("Accepting request because we're lazy to authenticate a PRACK")
            return False
        elif not auth_policy:
            self.logger.debug("Rejecting request because account is unknown")
            self.reject_request(params, Status(403, "Forbidden"))
            return True
        elif auth_policy == Account.AUTH_NEVER:
            self.logger.debug("Accepting request because authentication is never needed")
            return False
        elif auth_policy == Account.AUTH_ALWAYS:
            self.logger.debug("Authenticating request because account always needs it")
        elif auth_policy == Account.AUTH_IF_UNREGISTERED:
            hop_unknown = hop not in self.record_manager.lookup_contact_hops(from_uri)
            
            if hop_unknown:
                self.logger.debug("Authenticating request because account is not registered")
            else:
                self.logger.debug("Accepting request because account is registered")
                return False
        elif auth_policy == Account.AUTH_BY_HOP:
            self.logger.debug("Hop: %r, hops: %r" % (hop, self.account_manager.get_account_hops(from_uri)))
            hop_unknown = hop not in self.account_manager.get_account_hops(from_uri)
            
            if hop_unknown:
                self.logger.debug("Rejecting request because hop address is not allowed")
                self.reject_request(params, Status(403, "Forbidden"))
                return True
            else:
                self.logger.debug("Accepting request because hop address is allowed")
                return False
        else:
            raise Exception("WTF?")

        creds = self.account_manager.get_account_credentials(from_uri)
        challenge = self.authority.require_auth(params, creds)
        
        if challenge:
            self.logger.debug("Challenging request without proper authentication")
            self.challenge_request(params, challenge)
            return True
        else:
            self.logger.debug("Accepting request with proper authentication")
            return False
    

    def process_request(self, params):
        method = params["method"]
        request_uri = params["uri"]
        
        if request_uri.scheme != "sip":  # TODO: add some addr checks, too
            self.reject_request(params, Status(404, "Not found"))
            return

        processed = self.auth_request(params)
        if processed:
            return

        if method == "REGISTER":
            # If the From URI was OK, then the To URI is as well, because
            # we don't support third party registrations now.
            self.record_manager.process_request(params)
            return

        processed = self.dialog_manager.process_request(params)
        if processed:
            return
    
        if method == "INVITE" and "tag" not in params["to"].params:
            self.start_sip_call(params)
            return
    
        self.reject_request(params, Status(400, "Bad request"))


    def process_response(self, params, related_request):
        method = params["method"]
        
        if method == "REGISTER":
            self.registration_manager.process_response(params, related_request)
            return

        processed = self.dialog_manager.process_response(params, related_request)
        if processed:
            return

        self.logger.warning("Ignoring unknown response!")
