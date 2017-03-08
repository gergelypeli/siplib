from weakref import proxy

from format import Status
from transport import TransportManager
from transactions import TransactionManager, make_simple_response
from dialog import Dialog, DialogManager
from party import Bridge, RecordingBridge, Routing, RedialBridge, SessionNegotiatorBridge
from endpoint_sip import SipManager
from ground import Ground
from registrar import Registrar
from public import PublicationManager
from subscript import SubscriptionManager
from account import AccountManager
from log import Loggable
from mgc import Controller
from zap import Plug


class Switch(Loggable):
    def __init__(self,
        transport_manager=None, transaction_manager=None,
        registrar=None, publication_manager=None, subscription_manager=None,
        dialog_manager=None, sip_manager=None, mgc=None, account_manager=None
    ):
        Loggable.__init__(self)

        self.call_count = 0
        self.unsolicited_ids_by_urihop = {}

        self.transport_manager = transport_manager or TransportManager(
        )
        self.transaction_manager = transaction_manager or TransactionManager(
            proxy(self.transport_manager)
        )
        self.registrar = registrar or Registrar(
            proxy(self)
        )
        Plug(self.record_changed).attach(self.registrar.record_change_slot)
        self.publication_manager = publication_manager or PublicationManager(
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
        
        Plug(self.process).attach(self.transaction_manager.message_slot)


    def set_oid(self, oid):
        Loggable.set_oid(self, oid)

        self.account_manager.set_oid(oid.add("accman"))
        self.registrar.set_oid(oid.add("registrar"))
        self.publication_manager.set_oid(oid.add("pubman"))
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
        

    def provide_auth(self, response):
        return self.account_manager.provide_auth(response)
        
        
    def send_message(self, msg):
        return self.transaction_manager.send_message(msg)
        
    
    def reject_request(self, request, status):
        response = make_simple_response(request, status)
        self.send_message(response)


    def challenge_request(self, request, challenge):
        response = make_simple_response(request, Status.UNAUTHORIZED, challenge)
        self.send_message(response)


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
        elif type == "redial":
            return RedialBridge()
        elif type == "session_negotiator":
            return SessionNegotiatorBridge()
        else:
            raise Exception("Unknown party type '%s'!" % type)


    def make_call_info(self):
        call_oid = self.oid.add("call", self.call_count)
        call_info = dict(number=self.call_count, oid=call_oid, party_count=0, routing_count=0)
        self.call_count += 1
        
        return call_info
        

    def start_call(self, incoming_type, dst=None):
        call_info = self.make_call_info()
        incoming_party = self.ground.make_party(incoming_type, dst, call_info)
        incoming_party.start()
        incoming_party.set_call_info(call_info)
        
        return incoming_party


    def start_sip_call(self, request):
        incoming_party = self.start_call("sip")
        
        # The dialog must be fed directly, since the request contains no local tag yet.
        incoming_party.get_dialog().recv(request)
        

    def call_finished(self, call):
        self.logger.debug("Finishing call %s" % call.oid)
        self.calls_by_oid.pop(call.oid)


    def record_changed(self, aor, urihop, info):
        if info and info.user_agent and "Cisco" in info.user_agent:
            self.logger.info("A Cisco device registered, initiating unsolicited MWI.")
            es_type = "voicemail"
            es_id = aor.username
            format = "message-summary"
            local_uri = aor._replace(scheme="sip")
            remote_uri = urihop.uri
            hop = urihop.hop
            
            id = self.subscription_manager.unsolicited_subscribe(es_type, es_id, format, local_uri, remote_uri, hop)
            self.unsolicited_ids_by_urihop[urihop] = id
        elif not info and urihop in self.unsolicited_ids_by_urihop:
            self.logger.info("A Cisco device unregistered, stopping unsolicited MWI.")
            es_type = "voicemail"
            es_id = aor.username
            id = self.unsolicited_ids_by_urihop.pop(urihop)
            
            self.subscription_manager.unsolicited_unsubscribe(es_type, es_id, id)
            
            
    def state_changed(self, aor, format, etag, info):
        self.logger.info("State changed for %s format %s etag %s: %s" % (aor, format, etag, info))

        
    def auth_request(self, request):
        authname, sure = self.registrar.authenticate_request(request)
        # Returned:
        #   authname, True -  accept
        #   authname, False - challenge
        #   None, True -      reject
        #   None, False -     not found

        if not authname:
            if sure:
                self.reject_request(request, Status.FORBIDDEN)  # hop not allowed
                return True
            else:
                self.reject_request(request, Status.FORBIDDEN)  # sender not allowed
                return True

        if sure:
            return False
            
        account = self.account_manager.get_local_account(authname)
        if not account:
            self.logger.error("Account %s referred, but not found!" % authname)
            self.reject_request(request, Status.SERVER_INTERNAL_ERROR)
            return True
            
        method = request.method
        
        if method in ("CANCEL", "ACK", "NAK"):
            self.logger.debug("Accepting request because it can't be authenticated anyway")
            return False
        elif method == "PRACK":
            # FIXME: this is only for debugging
            self.logger.debug("Accepting request because we're lazy to authenticate a PRACK")
            return False

        if not account.check_auth(request):
            self.logger.debug("Challenging request without proper authentication")
            challenge = account.require_auth(request)
            self.challenge_request(request, challenge)
            return True
            
        self.logger.debug("Accepting request with proper authentication")
        return False
    

    def process(self, msg):
        if not msg.is_response:
            request = msg
            method = request.method

            processed = self.auth_request(request)
            if processed:
                return

            # No dialogs for registrations
            if method == "REGISTER":
                self.registrar.process_request(request)
                return

            if method == "PUBLISH":
                self.publication_manager.process_request(request)
                return

            if "tag" not in request["to"].params:
                # Out of dialog requests
                
                if method == "INVITE":
                    self.start_sip_call(request)
                    return
                elif method == "SUBSCRIBE":
                    self.subscription_manager.process(request)
                    return
                elif method == "CANCEL":
                    # The related msg may be None if the transaction manager didn't find it
                    if request.related and request.related.method == "INVITE":
                        self.dialog_manager.process(request)
                        return
                    else:
                        self.reject_request(request, Status.TRANSACTION_DOES_NOT_EXIST)
                        return
    
                self.reject_request(request, Status.NOT_IMPLEMENTED)
            else:
                # In dialog requests
                
                if method == "ACK" and msg.related:
                    # ACK for a non-2xx response, DJ, drop it!
                    return
                
                processed = self.dialog_manager.process(request)
                if processed:
                    return
    
                self.reject_request(request, Status.DIALOG_DOES_NOT_EXIST)
        else:
            response = msg
            method = response.method
        
            if method == "REGISTER":
                self.registrar.process_response(response)
                return

            if method == "PUBLISH":
                self.publication_manager.process_response(response)
                return

            processed = self.dialog_manager.process(response)
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
