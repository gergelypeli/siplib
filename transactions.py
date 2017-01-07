import uuid
import datetime
from weakref import proxy
from log import Loggable

from format import Via, Status, make_simple_response, make_ack, make_virtual_response, make_timeout_nak, make_timeout_response, is_virtual_response
import zap

# tr id: (branch, method)

# INVITE request
# initially: transmitting
# got prov resp: passive provisioning, may expire, timer restarts for each response
# got final resp: lingering, if ACK created, then calls its retransmit

# ACK request
# created by INVITE tr, may copy branch, keeps reference for retransmits, lingering
# for non-2xx resp: copies branch
# for 2xx resp: generates new branch

# INVITE response
# initially: auto 100, active provisioning, retransmits on timeout and request retransmits
# on prov resp: some more active provisioning
# on final resp: transmitting
# got ack notification: lingering

# On receiving ACK
# Find INVITE by dialog and cseq, notify it for ack, create ACK sink, lingering

# plain request
# initially: transmitting
# got final response: lingering

# plain response
# initially: lingering, retransmits on request

# incoming request;
#  if has transaction -> check
#  if ACK and has a related outgoing 2xx response -> do
#  if CANCEL and has related incoming INVITE -> do
#  if cseq > last -> do
#  reject

# generate branch id here

# 2xx ACK branch is different from INVITE

# sending the ACK should destroy the sent INVITE, even if keeping its branch


class Error(Exception): pass

    
class Transaction:
    STARTING = "STARTING"
    WAITING = "WAITING"
    PROVISIONING = "PROVISIONING"
    TRANSMITTING = "TRANSMITTING"
    LINGERING = "LINGERING"

    # Initial retransmission interval
    T1 = datetime.timedelta(milliseconds=500)
    
    # Maximum retransmission interval
    T2 = datetime.timedelta(milliseconds=4000)
    
    # Our provisioning timeout. A transaction without activity for 3 minutes can be
    # dropped by proxies, so it is recommended to provision every one minute.
    TP = datetime.timedelta(milliseconds=60000)


    def __init__(self, manager, branch, method):
        self.manager = manager
        self.branch = branch
        self.method = method

        self.outgoing_msg = None
        self.state = self.STARTING

        self.retransmit_interval = None
        self.retransmit_plug = None
        self.expiration_plug = None


    def finish(self):
        raise NotImplementedError()
        
        
    def report(self, msg):
        raise NotImplementedError()


    def process(self, msg):
        raise NotImplementedError()

        
    def transmission_timed_out(self):
        self.report(None)
        self.finish()
        

    def change_state(self, state):
        if state == self.state and state == self.TRANSMITTING:
            raise Error("Oops, transmitting twice!")

        self.state = state
        
        if self.retransmit_plug:
            self.retransmit_plug.unplug()
            self.retransmit_plug = None
            
        if self.expiration_plug:
            self.expiration_plug.unplug()
            self.expiration_plug = None
            
        if self.state == self.WAITING:
            # Waiting for something indefinitely
            self.retransmit_interval = None
        elif self.state == self.TRANSMITTING:
            # Transmit the message with backoff until stopped explicitly, or timing out
            self.retransmit_interval = self.T1
            self.expiration_plug = zap.time_slot(self.T1 * 64).plug(self.transmission_timed_out)
        elif self.state == self.PROVISIONING:
            # Transmit the provisional response somewhat rarely to keep proxies happy
            self.retransmit_interval = self.TP
        elif self.state == self.LINGERING:
            # Just wait to adsorb incoming duplicates, then time out
            self.retransmit_interval = None
            self.expiration_plug = zap.time_slot(self.T1 * 64).plug(self.finish)
        else:
            raise Error("Change to what state?")


    def retransmit(self):
        self.manager.transmit(self.outgoing_msg)

        if self.retransmit_interval:
            if self.retransmit_plug:
                self.retransmit_plug.unplug()
                
            self.retransmit_plug = zap.time_slot(self.retransmit_interval).plug(self.retransmit)

        if self.state == self.TRANSMITTING:
            self.retransmit_interval = min(self.retransmit_interval * 2, self.T2)


    def send(self, msg):
        self.outgoing_msg = msg
        self.retransmit()


class PlainClientTransaction(Transaction):
    # STARTING -> send request -> TRANSMITTING -> recv response -> LINGERING -> timeout -> DONE

    def report(self, response):
        if not response:
            response = make_timeout_response(self.outgoing_msg)
            
        self.manager.report_response(response, self.outgoing_msg)


    def finish(self):
        self.manager.remove_client_transaction((self.branch, self.method))
        
        
    def send(self, request):
        self.change_state(self.TRANSMITTING)

        if request.get("via"):
            raise Error("Don't mess with the request Via headers!")

        hop = request["hop"]
        request["via"] = [ Via(hop.transport, hop.local_addr, dict(branch=self.branch)) ]
        
        Transaction.send(self, request)


    def process(self, response):
        if self.state == self.TRANSMITTING:
            self.report(response)
            self.change_state(self.LINGERING)
        elif self.state == self.LINGERING:
            pass  # Already reported, this is a duplicate
        else:
            raise Error("Hm?")


class PlainServerTransaction(Transaction):
    # STARTING -> recv request -> WAITING -> send response -> LINGERING -> timeout -> DONE

    def __init__(self, manager, branch, method, related_msg=None):
        Transaction.__init__(self, manager, branch, method)
        
        self.incoming_msg = None
        self.related_msg = related_msg  # This is only used for incoming CANCELs
        
        
    def report(self, request):
        if not request:
            request = make_timeout_nak(self.outgoing_msg)
            
        self.manager.report_request(request, self.related_msg)
        

    def finish(self):
        self.manager.remove_server_transaction((self.branch, self.method))
        
        
    def process(self, request):
        if self.state == self.STARTING:
            self.incoming_msg = request
            self.report(request)
            self.change_state(self.WAITING)
        elif self.state == self.WAITING:
            pass
        elif self.state == self.LINGERING:
            self.retransmit()
        else:
            raise Error("Hm?")


    def send(self, response):
        self.change_state(self.LINGERING)
        
        if response.get("via"):
            raise Error("Don't mess with the response Via headers!")
            
        response["via"] = self.incoming_msg["via"]

        Transaction.send(self, response)


# This is so that we can retransmit ACK-s easily
class AckClientTransaction(PlainClientTransaction):
    # STARTING -> send request -> WAITING -> removed by owner INVITE
    
    def send(self, request):
        self.change_state(self.WAITING)
        
        PlainClientTransaction.send(self, request)


    def process(self, response):
        raise Error("WAT?")


class InviteClientTransaction(PlainClientTransaction):
    # STARTING -> send request -> TRANSMITTING -> recv prov -> WAITING -> recv final -> WAITING -> send ACK -> LINGERING -> timeout -> DONE
    
    def __init__(self, manager, branch, method):
        PlainClientTransaction.__init__(self, manager, branch, method)

        # Oh my god! They let an INVITE create multiple dialogs! You bastards!
        self.acks_by_remote_tag = {}


    def create_and_send_ack(self, ack_branch, msg):
        # Don't extend the lingering time with multiple ACK-s
        if self.state != self.LINGERING:
            self.change_state(self.LINGERING)
        
        # These won't be public transactions
        ack = AckClientTransaction(self.manager, ack_branch, "ACK")
        remote_tag = msg["to"].params.get("tag")
        self.acks_by_remote_tag[remote_tag] = ack
        ack.send(msg)


    def process(self, response):
        remote_tag = response["to"].params.get("tag")
        ack = self.acks_by_remote_tag.get(remote_tag)
        
        if ack:
            ack.retransmit()
            return
            
        code = response["status"].code
        
        # Don't remember and report 100 responses, as thay may have no remote tag.
        # And they are h2h anyway, so the dialog shouldn't care.
        if code > 100 and remote_tag:
            if code >= 300:
                # final non-2xx responses are ACK-ed here in the same transaction (17.1.1.3)
                # send this ACK before an upper layer changes the outgoing message,
                # it can happen with authorization!
                ack_params = make_ack(self.outgoing_msg, remote_tag)
                self.create_and_send_ack(self.branch, ack_params)
        
            # LOL, 17.1.1.2 originally required the INVITE client transaction to
            # be destroyed upon 2xx responses, and mindlessly report further 2xx
            # responses to nonexistent requests the core. Aside from being braindead,
            # it was later corrected by RFC 6026. Quote of the day:
            #   It also forbids forwarding stray responses to INVITE
            #   requests (not just 2xx responses), which RFC 3261 requires.
        
            self.report(response)

        if self.state == self.TRANSMITTING:
            # After a provisional response we should wait indefinitely for a final,
            # and after a final wait indefinitely for a user ACK.
            self.change_state(self.WAITING)


class InviteServerTransaction(PlainServerTransaction):
    # STARTING -> recv request -> WAITING -> send prov -> PROVISIONING ->
    # send 100rel -> TRANSMITTING -> send virt -> PROVISIONING ->
    # send final -> TRANSMITTING -> send virt -> LINGERING -> timeout -> DONE
    
    def process(self, request):
        if self.state == self.STARTING:
            self.incoming_msg = request
            self.manager.report_request(request)
                
            # Send 100 only for initial INVITE-s
            if not request["to"].params.get("tag"):
                self.send(make_simple_response(request, Status(100, "Trying")))
        elif self.state == self.WAITING:
            pass
        elif self.state == self.PROVISIONING:
            self.retransmit()
        elif self.state == self.TRANSMITTING:
            pass  # response is retransmitted automatically now
        else:
            pass  # stupid client retransmitted the request after acking the response


    def send(self, response):
        if is_virtual_response(response):
            # A virtual response means we got (PR)ACKed, stop retransmissions
            # A PRACKed response shouldn't be retransmitted again as it is,
            # but we can dumb it down to an unreliable response before retransmitting it.
            was_final = self.outgoing_msg["status"].code >= 200
            
            if was_final:
                self.change_state(self.LINGERING)
            else:
                unreliable_msg = dict(self.outgoing_msg)
                unreliable_msg.pop("rseq")
                unreliable_msg["require"].remove("100rel")
                self.outgoing_msg = unreliable_msg
                self.change_state(self.PROVISIONING)
        else:
            is_rpr = "100rel" in response.get("require", set())
            is_final = response["status"].code >= 200

            if is_rpr or is_final:
                self.change_state(self.TRANSMITTING)
            else:
                self.change_state(self.PROVISIONING)
                
            PlainServerTransaction.send(self, response)


class AckServerTransaction(PlainServerTransaction):
    # STARTING -> recv request -> WAITING -> send virt -> LINGERING -> timeout -> DONE
    # Created only for 2xx responses to drop duplicates.
    # For the sake of consistentcy, we expect a virtual response to go lingering.
    
    def send(self, response):
        if not is_virtual_response(response):
            raise Error("Nonvirtual response to AckServerTransaction!")

        self.change_state(self.LINGERING)
        # Don't send anything


    def process(self, msg):
        if self.state == self.LINGERING:
            pass  # Duplicates shouldn't trigger retransmissions, because there isn't any
        else:
            PlainServerTransaction.process(self, msg)
        

class TransactionManager(Loggable):
    def __init__(self, transport):
        Loggable.__init__(self)

        self.transport = transport
        self.client_transactions = {}  # by (branch, method)
        self.server_transactions = {}  # by (branch, method)
        
        self.request_slot = zap.EventSlot()
        self.response_slot = zap.EventSlot()
        
        self.transport_plug = self.transport.process_slot.plug(self.process_message)

        
    def transmit(self, msg):
        self.transport.send_message(msg)
        

    def generate_branch(self):
        return Via.BRANCH_MAGIC + uuid.uuid4().hex[:8]


    def identify(self, params):
        try:
            branch = params["via"][0].params["branch"]
        except Exception:
            raise Error("No Via header in incoming message!")

        method = params["method"]
        
        return branch, method


    def add_client_transaction(self, tr):
        tr_id = (tr.branch, tr.method)
        self.logger.debug("Added client transaction %s/%s." % tr_id)
        self.client_transactions[tr_id] = tr


    def add_server_transaction(self, tr):
        tr_id = (tr.branch, tr.method)
        self.logger.debug("Added server transaction %s/%s." % tr_id)
        self.server_transactions[tr_id] = tr
        
        
    def remove_client_transaction(self, tr_id):
        self.logger.debug("Removed client transaction %s/%s." % tr_id)
        self.client_transactions.pop(tr_id)
        
        
    def remove_server_transaction(self, tr_id):
        self.logger.debug("Removed server transaction %s/%s." % tr_id)
        self.server_transactions.pop(tr_id)


    def process_message(self, msg):
        branch, method = self.identify(msg)
        
        if msg["is_response"]:
            tr = self.client_transactions.get((branch, method))
            
            if tr:
                tr.process(msg)
            else:
                self.logger.debug("Incoming response to unknown request, ignoring!")
                
            return

        tr = self.server_transactions.get((branch, method))
        if tr:
            tr.process(msg)
            return
            
        if method == "CANCEL":
            # CANCEL-s may arrive for out-of-dialog requests that would create a
            # dialog here. We don't know if that dialog was already created, so link
            # the CANCEL to the original request, so that somebody can handle this.
            invite_str = self.server_transactions.get((branch, "INVITE"))
            related_msg = invite_str.incoming_msg if invite_str else None
            
            tr = PlainServerTransaction(proxy(self), branch, method, related_msg)
        elif method == "ACK":
            invite_str = self.server_transactions.get((branch, "INVITE"))
            
            if invite_str:
                # We must have sent a non-200 response to this, so no dialog was created.
                # Send a virtual ACK response to notify the transaction.
                # But don't create an AckServerTransaction here, we have no one to notify.
                invite_str.send(make_virtual_response())
                return

            # Must create a server transaction to swallow duplicates,
            # we don't want to bother the dialogs unnecessarily.
            # Too bad we can't find the related INVITE for this ACK, that would need
            # a lookup by dialog and cseq.
            tr = AckServerTransaction(proxy(self), branch, method)
        elif method == "INVITE":
            tr = InviteServerTransaction(proxy(self), branch, method)
        else:
            tr = PlainServerTransaction(proxy(self), branch, method)

        self.add_server_transaction(tr)
        tr.process(msg)
        

    def send_message(self, msg, related_msg=None):
        if msg["is_response"]:
            if not related_msg:
                raise Error("Related request is not given for response!")
                
            request = related_msg
            tr = self.server_transactions.get(self.identify(request))
            if tr:
                tr.send(msg)
            else:
                self.logger.debug("Outgoing response to unknown request, ignoring!")
                
            return

        method = msg["method"]

        if method == "ACK":
            # 2xx-ACK from Dialog
            if not related_msg:
                raise Error("Related response is not given for ACK!")
                
            response_params = related_msg
            invite_tr = self.client_transactions.get(self.identify(response_params))
            if not invite_tr:
                raise Error("No transaction to ACK!")
            
            invite_tr.create_and_send_ack(self.generate_branch(), msg)
            return
        elif method == "CANCEL":
            if not related_msg:
                raise Error("Related request is not given for CANCEL!")
            
            request_params = related_msg
            branch, method = self.identify(request_params)

            tr = PlainClientTransaction(proxy(self), branch, method)

            # TODO: more sophisticated CANCEL generation
            # TODO: shouldn't send CANCEL before provisional response
            msg = request_params.copy()
            msg["method"] = "CANCEL"
            msg["via"] = None  # ClientTransaction is a bit sensitive for this
            msg["authorization"] = None
            msg["content_type"] = None
            msg["body"] = None
        elif method == "INVITE":
            tr = InviteClientTransaction(proxy(self), self.generate_branch(), method)
        else:
            tr = PlainClientTransaction(proxy(self), self.generate_branch(), method)

        self.add_client_transaction(tr)
        tr.send(msg)


    def report_response(self, params, related_request):
        self.response_slot.zap(params, related_request)


    def report_request(self, params, related_request=None):
        self.request_slot.zap(params, related_request)
