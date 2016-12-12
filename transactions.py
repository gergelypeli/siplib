import uuid
import datetime
from weakref import proxy
from util import Loggable

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


BRANCH_MAGIC = "z9hG4bK"


class Error(Exception): pass


def identify(params):
    try:
        branch = params["via"][0].branch
    except Exception:
        raise Error("No Via header in incoming message!")

    method = params["method"]
        
    return branch, method


def generate_branch():
    return BRANCH_MAGIC + uuid.uuid4().hex[:8]

    
class Transaction:
    WAITING = "WAITING"
    PROVISIONING = "PROVISIONING"
    TRANSMITTING = "TRANSMITTING"
    LINGERING = "LINGERING"

    T1 = datetime.timedelta(milliseconds=500)
    T2 = datetime.timedelta(milliseconds=4000)
    TP = datetime.timedelta(milliseconds=10000)

    def __init__(self, manager, branch):
        self.manager = manager
        self.branch = branch

        self.outgoing_msg = None
        self.state = self.WAITING

        self.retransmit_interval = None
        self.retransmit_deadline = None
        self.expiration_deadline = None


    def change_state(self, state):
        if state == self.state and state == self.TRANSMITTING:
            raise Error("Oops, transmitting twice!")

        self.state = state
        self.retransmit_deadline = None

        if self.state == self.TRANSMITTING:
            self.retransmit_interval = self.T1
            self.expiration_deadline = datetime.datetime.now() + self.TP
        elif self.state == self.PROVISIONING:
            self.retransmit_interval = self.TP * 0.9
            self.expiration_deadline = None
        elif self.state == self.LINGERING:
            self.retransmit_interval = None
            self.expiration_deadline = datetime.datetime.now() + self.TP
        elif self.state == self.WAITING:
            self.retransmit_interval = None
            self.expiration_deadline = None
        else:
            raise Error("Change to what state?")


    def transmit(self, msg):
        self.outgoing_msg = msg
        self.retransmit()


    def retransmit(self):
        self.manager.transmit(self.outgoing_msg)

        if self.retransmit_interval:
            self.retransmit_deadline = datetime.datetime.now() + self.retransmit_interval

        if self.state == self.TRANSMITTING:
            self.retransmit_interval = min(self.retransmit_interval * 2, self.T2)


    def maintain(self, now):
        if self.expiration_deadline and self.expiration_deadline <= now:
            self.expired()
            return False
            
        if self.retransmit_deadline and self.retransmit_deadline <= now:
            self.retransmit()
            
        return True


    def send(self, msg):
        pass


    def process(self, msg):
        pass


    def expired(self):
        pass


class PlainClientTransaction(Transaction):
    def transmit(self, msg):
        if msg.get("via"):
            raise Error("Don't mess with the request Via headers!")

        hop = msg["hop"]
        msg["via"] = [ Via(hop.transport, hop.local_addr, self.branch) ]
        Transaction.transmit(self, msg)


    def send(self, request):
        self.change_state(self.TRANSMITTING)
        self.transmit(request)


    def process(self, response):
        if self.state == self.TRANSMITTING:
            self.manager.report_response(response, self.outgoing_msg)
            self.change_state(self.LINGERING)
        elif self.state == self.LINGERING:
            pass
        else:
            raise Error("Hm?")


    def expired(self):
        if self.state == self.TRANSMITTING:
            self.manager.report_response(make_timeout_response(self.outgoing_msg), None)


class PlainServerTransaction(Transaction):
    def __init__(self, manager, branch):
        Transaction.__init__(self, manager, branch)
        
        self.incoming_via = None
        
        
    def transmit(self, msg):
        if msg.get("via"):
            raise Error("Don't mess with the response Via headers!")
            
        msg["via"] = self.incoming_via
        Transaction.transmit(self, msg)


    def process(self, request):
        if self.state == self.WAITING:
            if not self.incoming_via:
                self.incoming_via = request["via"]
                self.manager.report_request(request)
        elif self.state == self.LINGERING:
            self.retransmit()
        else:
            raise Error("Hm?")


    def send(self, response):
        self.change_state(self.LINGERING)
        self.transmit(response)


class AckClientTransaction(PlainClientTransaction):
    def send(self, request):
        self.change_state(self.LINGERING)
        self.transmit(request)


    def process(self, response):
        raise Error("WAT?")


class Bastard(object):
    def __init__(self):
        self.ack = None
        

class InviteClientTransaction(PlainClientTransaction):
    def __init__(self, *args, **kwargs):
        PlainClientTransaction.__init__(self, *args, **kwargs)

        # Oh my god! They let an INVITE create multiple dialogs! You bastards!
        self.bastards = {}


    def rt(self, msg):
        return msg["to"].params.get("tag")
    
    
    def create_and_send_ack(self, ack_branch, msg):
        if self.state != self.LINGERING:
            self.change_state(self.LINGERING)  # now we can expire
        
        ack = AckClientTransaction(self.manager, ack_branch)

        # These won't be public
        self.bastards[self.rt(msg)].ack = ack
        ack.send(msg)


    def process(self, response):
        remote_tag = self.rt(response)
        him = self.bastards.get(remote_tag)
        
        if him and him.ack:
            him.ack.retransmit()  # don't extend the lingering time
        else:
            code = response["status"].code
            
            # Don't remember and report 100 responses, as thay may have no remote tag.
            # And they are h2h anyway, so the dialog shouldn't care.
            if code > 100 and remote_tag:
                if not him:
                    # New remote tag, create new bastard
                    him = Bastard()
                    self.bastards[remote_tag] = him

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
            
                # FIXME: this check is too strict, the same status code may arrive
                # with different content, either check it fully, or drop this check!
                if True:  # code not in him.statuses:
                    #him.statuses.add(code)
                    self.manager.report_response(response, self.outgoing_msg)

            if self.state != self.WAITING:
                if code < 200:
                    self.change_state(self.LINGERING)  # may extend lingering time
                else:
                    self.change_state(self.WAITING)  # indefinitely for creating an ACK


    def expired(self):
        if self.state == self.TRANSMITTING:  # TODO: do we still need explicit timeout response?
            self.manager.report_response(make_timeout_response(self.outgoing_msg), self.outgoing_msg)  # nothing at all
        elif self.state == self.LINGERING:
            # FIXME: don't reuse the LINGERING state after sending an ACK!
            sent_ack = any(him.ack for him in self.bastards.values())

            if not sent_ack:
                self.manager.report_response(make_timeout_response(self.outgoing_msg), self.outgoing_msg)
        else:
            raise Error("Invite client expired while %s!" % self.state)


class InviteServerTransaction(PlainServerTransaction):
    def process(self, request):
        if self.state == self.WAITING:
            if not self.incoming_via:
                self.incoming_via = request["via"]
                self.manager.report_request(request)
                
                if self.state == self.WAITING:
                    # No response yet, say something
                    self.send(make_simple_response(request, Status(100, "Trying")))
        elif self.state == self.PROVISIONING:
            self.retransmit()
        elif self.state == self.TRANSMITTING:
            pass  # response is retransmitted automatically now
        else:
            pass  # stupid client retransmitted the request after acking the response


    def send(self, response):
        if is_virtual_response(response):
            # A virtual response means we got (PR)ACKed, stop retransmissions
            # A PRACKed response shouldn't be retransmitted again, it must be
            # updated by the upper layers properly.
            was_final = self.outgoing_msg["status"].code >= 200
            
            if self.state in (self.TRANSMITTING, self.PROVISIONING):
                self.change_state(self.LINGERING if was_final else self.WAITING)

        else:
            is_rpr = "100rel" in response.get("require", set())
            is_final = response["status"].code >= 200
            is_reliable = is_rpr or is_final
            
            self.change_state(self.TRANSMITTING if is_reliable else self.PROVISIONING)
            self.transmit(response)


    def expired(self):
        if self.state == self.TRANSMITTING:
            # Got no ACK, complain
            self.manager.report_request(make_timeout_nak(self.outgoing_msg))
        elif self.state == self.LINGERING or self.state == self.WAITING:
            pass
        else:
            raise Error("Invite server expired while %s!" % self.state)


class AckServerTransaction(PlainServerTransaction):
    # Created only for 2xx responses to drop duplicates for the Dialog.
    # For the sake of consistentcy, we expect a virtual response to go lingering.
    
    def send(self, response):
        if not is_virtual_response(response):
            raise Error("Nonvirtual response to AckServerTransaction!")

        # Don't send anything


class TransactionManager(Loggable):
    def __init__(self, transport):
        Loggable.__init__(self)

        self.transport = transport
        self.client_transactions = {}  # by (branch, method)
        self.server_transactions = {}  # by (branch, method)
        
        self.request_slot = zap.EventSlot()
        self.response_slot = zap.EventSlot()
        
        self.transport_plug = self.transport.process_slot.plug(self.process_message)
        zap.time_slot(0.2, repeat=True).plug(self.maintenance)

        
    def transmit(self, msg):
        self.transport.send_message(msg)
        
        
    def maintain_transactions(self, now, transactions):
        for tr_id, tr in list(transactions.items()):
            keep = tr.maintain(now)
            
            if not keep:
                transactions.pop(tr_id)


    def maintenance(self):
        now = datetime.datetime.now()

        self.maintain_transactions(now, self.client_transactions)
        self.maintain_transactions(now, self.server_transactions)


    def add_transaction(self, tr, method):
        tr_id = (tr.branch, method)

        if isinstance(tr, PlainClientTransaction):
            self.client_transactions[tr_id] = tr
        elif isinstance(tr, PlainServerTransaction):
            self.server_transactions[tr_id] = tr
        else:
            raise Error("WAT?")


    def process_message(self, msg):
        #print("Match incoming:")
        #pprint(msg)
        branch, method = identify(msg)
        
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
            tr = PlainServerTransaction(proxy(self), branch)
            self.add_transaction(tr, "CANCEL")
            tr.process(msg)
            return
            
        if method == "ACK":
            invite_tr = self.server_transactions.get((branch, "INVITE"))
            if invite_tr:
                # We must have sent a non-200 response to this, so no dialog was created.
                # Send a virtual ACK response to notify the transaction.
                # But don't create an AckServerTransaction here, we have no one to notify.
                invite_tr.send(make_virtual_response())
                return
                
        # Create new
        if method == "INVITE":
            tr = InviteServerTransaction(proxy(self), branch)
        elif method == "ACK":
            # Must create a server transaction to swallow duplicates,
            # we don't want to bother the dialogs unnecessarily.
            tr = AckServerTransaction(proxy(self), branch)
        else:
            tr = PlainServerTransaction(proxy(self), branch)

        self.add_transaction(tr, method)
        tr.process(msg)
        

    def send_message(self, msg, related_msg=None):
        if msg["is_response"]:
            if not related_msg:
                raise Error("Related request is not given for response!")
                
            request = related_msg
            tr = self.server_transactions.get(identify(request))
            if tr:
                tr.send(msg)
            else:
                self.logger.debug("Outgoing response to unknown request, ignoring!")
                
            return

        #if not report_response:
        #    raise Error("No report_response handler for a request!")

        method = msg["method"]

        if method == "ACK":
            # 2xx-ACK from Dialog
            if not related_msg:
                raise Error("Related response is not given for ACK!")
                
            response_params = related_msg
            invite_tr = self.client_transactions.get(identify(response_params))
            if not invite_tr:
                raise Error("No transaction to ACK!")
            
            #branch = generate_branch()
            #remote_tag = response_params["to"].params["tag"]
            #sdp = msg.get("sdp")
            invite_tr.create_and_send_ack(generate_branch(), msg)
        elif method == "CANCEL":
            if not related_msg:
                raise Error("Related request is not given for CANCEL!")
            
            request_params = related_msg
            branch, method = identify(request_params)

            tr = PlainClientTransaction(proxy(self), branch)
            self.add_transaction(tr, "CANCEL")

            # TODO: more sophisticated CANCEL generation
            # TODO: shouldn't send CANCEL before provisional response
            cancel_params = request_params.copy()
            cancel_params["method"] = "CANCEL"
            cancel_params["via"] = None  # ClientTransaction is a bit sensitive for this
            cancel_params["authorization"] = None
            cancel_params["sdp"] = None
        
            tr.send(cancel_params)
        elif method == "INVITE":
            tr = InviteClientTransaction(proxy(self), generate_branch())
            self.add_transaction(tr, method)
            tr.send(msg)
        else:
            tr = PlainClientTransaction(proxy(self), generate_branch())
            self.add_transaction(tr, method)
            tr.send(msg)


    def report_response(self, params, related_request):
        self.response_slot.zap(params, related_request)


    def report_request(self, params):
        if params["method"] == "CANCEL":
            # CANCEL-s may happen outside of any dialogs, so process them here
            # FIXME: not necessarily INVITE
            branch, method = identify(params)
            invite_str = self.server_transactions.get((branch, "INVITE"))
            
            if not invite_str:
                status = Status(481, "Transaction Does Not Exist")
                cancel_response = make_simple_response(params, status)
                cancel_str = self.server_transactions.get((branch, "CANCEL"))
                cancel_str.send(cancel_response)
                return

            # The responses to INVITE and CANCEL must have a To tag, so they must
            # be generated by the Dialog.
            # And it has to accept that the CANCEL request may have no to tag!
    
        self.request_slot.zap(params)
