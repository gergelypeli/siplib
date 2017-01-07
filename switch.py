from weakref import proxy

from format import Status
from transport import TransportManager
from transactions import TransactionManager, make_simple_response
from dialog import Dialog, DialogManager
from party import Bridge, RecordingBridge, Routing
from party_sip import SipManager
from ground import Ground
from registrar import Registrar
from subscript import SubscriptionManager
from account import AccountManager
from log import Loggable
from mgc import Controller


class Switch(Loggable):
    def __init__(self,
        transport_manager=None, transaction_manager=None,
        registrar=None, subscription_manager=None,
        dialog_manager=None, sip_manager=None, mgc=None, account_manager=None
    ):
        Loggable.__init__(self)

        self.call_count = 0

        self.transport_manager = transport_manager or TransportManager(
        )
        self.transaction_manager = transaction_manager or TransactionManager(
            proxy(self.transport_manager)
        )
        self.registrar = registrar or Registrar(
            proxy(self)
        )
        self.subscription_manager = subscription_manager or SubscriptionManager(
            proxy(self)
        )
        self.dialog_manager = dialog_manager or DialogManager(
            proxy(self)
        )
        self.sip_manager = sip_manager or SipManager(
        )
        self.mgc = mgc or Controller(
        )
        self.account_manager = account_manager or AccountManager(
        )
        self.ground = Ground(
            proxy(self),
            proxy(self.mgc)
        )
        
        self.transaction_manager.request_slot.plug(self.process_request)
        self.transaction_manager.response_slot.plug(self.process_response)


    def set_oid(self, oid):
        Loggable.set_oid(self, oid)

        self.account_manager.set_oid(oid.add("accman"))
        self.registrar.set_oid(oid.add("registrar"))
        self.subscription_manager.set_oid(oid.add("subman"))
        self.transport_manager.set_oid(oid.add("tportman"))
        self.transaction_manager.set_oid(oid.add("tactman"))
        self.dialog_manager.set_oid(oid.add("diaman"))
        self.sip_manager.set_oid(oid.add("sipman"))
        self.mgc.set_oid(oid.add("mgc"))
        self.ground.set_oid(oid.add("ground"))


    def set_name(self, name):
        self.mgc.set_name(name)
        
        
    def select_hop_slot(self, next_uri):
        return self.transport_manager.select_hop_slot(next_uri)
        

    def get_remote_account(self, uri):
        return self.account_manager.get_remote_account(uri)
        
        
    # FIXME: is this still needed?
    def send_message(self, msg, related_msg=None):
        return self.transaction_manager.send_message(msg, related_msg)
        
    
    def reject_request(self, request, code, reason=None):
        response = make_simple_response(request, Status(code, reason))
        self.transaction_manager.send_message(response, request)


    def challenge_request(self, msg, challenge):
        response = make_simple_response(msg, Status(401, "Come Again"), challenge)
        self.transaction_manager.send_message(response, msg)


    def make_dialog(self):
        return Dialog(proxy(self.dialog_manager))
        

    def make_party(self, type):
        if type == "routing":
            return Routing()  # this does nothing useful, though
        elif type == "sip":
            return self.sip_manager.make_endpoint(self.make_dialog())
        elif type == "bridge":
            return Bridge()
        elif type == "record":
            return RecordingBridge()
        else:
            raise Exception("Unknown party type '%s'!" % type)


    def start_call(self, incoming_type, incoming_params=None):
        call_oid = self.oid.add("call", self.call_count)
        call_info = dict(number=self.call_count, oid=call_oid, party_count=0, routing_count=0)
        self.call_count += 1

        incoming_party = self.ground.make_party(incoming_type, incoming_params, call_info)
        incoming_party.start()
        incoming_party.set_call_info(call_info)
        
        #routing = self.ground.make_party("routing", call_oid, [])
        #routing_leg = routing.start()

        #self.ground.link_legs(incoming_leg.oid, routing_leg.oid)
        
        return incoming_party


    def start_sip_call(self, params):
        incoming_party = self.start_call("sip")
        
        # The dialog must be fed directly, since the request contains no local tag yet.
        incoming_party.get_dialog().recv_request(params)
        

    def call_finished(self, call):
        self.logger.debug("Finishing call %s" % call.oid)
        self.calls_by_oid.pop(call.oid)
        
        
    def auth_request(self, params):
        authname, sure = self.registrar.authenticate_request(params)
        # Returned:
        #   authname, True -  accept
        #   authname, False - challenge
        #   None, True -      reject
        #   None, False -     not found

        if not authname:
            if sure:
                self.reject_request(params, 403, "Hop not allowed")
                return True
            else:
                self.reject_request(params, 403, "Sender not allowed")
                return True

        if sure:
            return False
            
        account = self.account_manager.get_local_account(authname)
        if not account:
            self.logger.error("Account %s referred, but not found!" % authname)
            self.reject_request(params, 500)
            return True
            
        method = params["method"]
        
        if method in ("CANCEL", "ACK", "NAK"):
            self.logger.debug("Accepting request because it can't be authenticated anyway")
            return False
        elif method == "PRACK":
            # FIXME: this is only for debugging
            self.logger.debug("Accepting request because we're lazy to authenticate a PRACK")
            return False

        if not account.check_auth(params):
            self.logger.debug("Challenging request without proper authentication")
            challenge = account.require_auth(params)
            self.challenge_request(params, challenge)
            return True
            
        self.logger.debug("Accepting request with proper authentication")
        return False
    

    def process_request(self, params, related_params):
        method = params["method"]

        processed = self.auth_request(params)
        if processed:
            return

        # No dialogs for registrations
        if method == "REGISTER":
            self.registrar.process_request(params)
            return

        # Out of dialog requests
        if "tag" not in params["to"].params:
            if method == "INVITE":
                self.start_sip_call(params)
                return
            elif method == "SUBSCRIBE":
                self.subscription_manager.process_request(params)
                return
            elif method == "CANCEL":
                # The related_params may be None if the transaction manager didn't find it
                if related_params and related_params["method"] == "INVITE":
                    self.dialog_manager.process_request(params, related_params)
                    return
                else:
                    self.reject_request(params, 481, "Transaction Does Not Exist")
                    return
    
            self.reject_request(params, 501, "Method Not Implemented")
        else:
            # In dialog requests
            processed = self.dialog_manager.process_request(params)
            if processed:
                return
    
            self.reject_request(params, 481, "Dialog Does Not Exist")


    def process_response(self, params, related_request):
        method = params["method"]
        
        if method == "REGISTER":
            self.registrar.process_response(params, related_request)
            return

        processed = self.dialog_manager.process_response(params, related_request)
        if processed:
            return

        if method == "BYE":
            # This will happen for bastard dialogs
            self.logger.debug("No dialog for BYE response, oh well.")
        elif method == "NOTIFY":
            # This will happen for final notifications after expiration
            self.logger.debug("No dialog for NOTIFY response, oh well.")
        else:
            self.logger.warning("No dialog for incoming %s response!" % (method,))
