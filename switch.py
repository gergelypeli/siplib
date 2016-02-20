from async import WeakMethod, Weak
from format import Status
from transport import UdpTransport
from transactions import TransactionManager, make_simple_response
from dialog import Dialog, DialogManager
from leg_sip import SipLeg
from call import Call, Bridge, RecordingBridge
from authority import Authority
from registrar import RegistrationManager, RecordManager
from account import Account, AccountManager
from util import build_oid, Loggable
from mgc import Controller


class Switch(Loggable):
    def __init__(self, local_addr, metapoll,
        transport=None, transaction_manager=None,
        record_manager=None, authority=None, registration_manager=None,
        dialog_manager=None, mgc=None, account_manager=None
    ):
        Loggable.__init__(self)

        self.calls_by_oid = {}
        self.call_count = 0

        self.local_addr = local_addr
        self.metapoll = metapoll
        
        self.transport = transport or UdpTransport(
            metapoll, local_addr, WeakMethod(self.reception)
        )
        self.transaction_manager = transaction_manager or TransactionManager(
            local_addr, WeakMethod(self.transport.send)
        )
        self.record_manager = record_manager or RecordManager(
            WeakMethod(self.transaction_manager.send_message)
        )
        self.authority = authority or Authority(
        )
        self.registration_manager = registration_manager or RegistrationManager(
            WeakMethod(self.transaction_manager.send_message),
            WeakMethod(self.transport.get_hop),
            WeakMethod(self.authing)
        )
        self.dialog_manager = dialog_manager or DialogManager(
            local_addr,
            WeakMethod(self.transaction_manager.send_message),
            WeakMethod(self.transport.get_hop),
            WeakMethod(self.authing)
        )
        self.mgc = mgc or Controller(
            metapoll
        )
        self.account_manager = account_manager or AccountManager(
        )


    def set_oid(self, oid):
        Loggable.set_oid(self, oid)

        self.account_manager.set_oid(build_oid(oid, "accman"))
        self.authority.set_oid(build_oid(oid, "authority"))
        self.record_manager.set_oid(build_oid(oid, "recman"))
        self.registration_manager.set_oid(build_oid(oid, "regman"))
        self.transport.set_oid(build_oid(oid, "transport"))
        self.dialog_manager.set_oid(build_oid(oid, "diaman"))
        self.mgc.set_oid(build_oid(oid, "mgc"))


    def authing(self, response, request):
        creds = self.account_manager.get_our_credentials()
        return self.authority.provide_auth(response, request, creds)
        
    
    def reject_request(self, msg, status):
        if msg:
            response = make_simple_response(msg, status)
            self.transaction_manager.send_message(response, msg)


    def challenge_request(self, msg, challenge):
        if msg:
            response = make_simple_response(msg, Status(401, "Hey"), challenge)
            self.transaction_manager.send_message(response, msg)


    def make_leg(self, call, uri):
        if uri.scheme == "dial":
            return call.make_bridge(Bridge).make_incoming_leg()
        elif uri.scheme == "record":
            return call.make_bridge(RecordingBridge).make_incoming_leg()
        elif uri.scheme == "sip":
            return SipLeg(Dialog(Weak(self.dialog_manager)))
        else:
            raise Exception("Unknown URI scheme '%s' for creating outgoing leg!" % uri.scheme)


    def make_call(self):
        return Call(Weak(self))
        
        
    def start_call(self, incoming_leg):
        call = self.make_call()
        
        oid = build_oid(self.oid, "call", self.call_count)
        self.call_count += 1
        call.set_oid(oid)
        
        self.calls_by_oid[oid] = call

        call.start(incoming_leg)


    def start_sip_call(self, params):  # TODO: params unused
        incoming_dialog = Dialog(Weak(self.dialog_manager))
        incoming_leg = SipLeg(incoming_dialog)
        
        self.start_call(incoming_leg)
        
        return WeakMethod(incoming_dialog.recv_request)
        

    def finish_call(self, oid):
        self.logger.debug("Finishing call %s" % oid)
        self.calls_by_oid.pop(oid)
        
        
    def auth_request(self, params):
        method = params["method"]
        from_uri = params["from"].uri
        hop = params["hop"]

        auth_policy = self.account_manager.get_account_auth_policy(from_uri)
        
        if method in ("CANCEL", "ACK"):
            self.logger.debug("Accepting request because it can't be authenticated anyway")
            return None
        elif not auth_policy:
            self.logger.debug("Rejecting request because account is unknown")
            return WeakMethod(self.reject_request, Status(403, "Forbidden"))
        elif auth_policy == Account.AUTH_NEVER:
            self.logger.debug("Accepting request because authentication is never needed")
            return None
        elif auth_policy == Account.AUTH_ALWAYS:
            self.logger.debug("Challenging request because authentication is always needed")
        elif auth_policy == Account.AUTH_IF_UNREGISTERED:
            hop_unknown = hop not in self.record_manager.lookup_contact_hops(from_uri)
            
            if hop_unknown:
                self.logger.debug("Challenging request because account is not registered")
            else:
                self.logger.debug("Accepting request because account is registered")
                return None
        elif auth_policy == Account.AUTH_BY_HOP:
            self.logger.debug("Hop: %r, hops: %r" % (hop, self.account_manager.get_account_hops(from_uri)))
            hop_unknown = hop not in self.account_manager.get_account_hops(from_uri)
            
            if hop_unknown:
                self.logger.debug("Rejecting request because hop address is not allowed")
                return WeakMethod(self.reject_request, Status(403, "Forbidden"))
            else:
                self.logger.debug("Accepting request because hop address is allowed")
                return None
        else:
            raise Exception("WTF?")

        creds = self.account_manager.get_account_credentials(from_uri)
        challenge = self.authority.require_auth(params, creds)
        
        if challenge:
            self.logger.debug("Rejecting request without proper authentication")
            return WeakMethod(self.challenge_request, challenge)
        else:
            self.logger.debug("Accepting request with proper authentication")
            return None
    

    def process_request(self, params):
        method = params["method"]
        request_uri = params["uri"]
        
        if request_uri.scheme != "sip":  # TODO: add some addr checks, too
            return WeakMethod(self.reject_request, Status(404, "Not found"))

        report = self.auth_request(params)
        if report:
            return report

        if method == "REGISTER":
            # If the From URI was OK, then the To URI is as well, because
            # we don't support third party registrations now.
            return self.record_manager.match_incoming_request(params)

        report = self.dialog_manager.match_incoming_request(params)
        if report:
            return report
    
        if method == "INVITE" and "tag" not in params["to"].params:
            return self.start_sip_call(params)
    
        return WeakMethod(self.reject_request, Status(400, "Bad request"))
        

    def reception(self, params):
        #print("Got message from transport.")
        #print("Req params: %s" % req_params)
    
        if self.transaction_manager.match_incoming_message(params):
            return

        report = self.process_request(params)
        
        self.transaction_manager.create_incoming_request(params, report)
