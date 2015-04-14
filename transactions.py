from __future__ import print_function, unicode_literals, absolute_import

from pprint import pprint, pformat
import uuid
import datetime
import weakref

import format
from format import Addr, Uri, Nameaddr, Via, Status

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


def identify(params):
    try:
        branch = params["via"][0].branch
    except Exception:
        raise Error("No Via header in incoming message!")

    method = params["method"]
        
    return branch, method


def generate_branch():
    return uuid.uuid4().hex


def make_virtual_response():
    return dict(status=Status(0, "Virtual"))
    
    
def is_virtual_response(msg):
    return msg["status"].code == 0
    

class Transaction(object):
    WAITING = 1
    PROVISIONING = 2
    TRANSMITTING = 3
    LINGERING = 4

    T1 = datetime.timedelta(milliseconds=500)
    T2 = datetime.timedelta(milliseconds=4000)
    TP = datetime.timedelta(milliseconds=10000)

    def __init__(self, manager, report, branch):
        self.manager = manager
        self.report = report
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
            self.retransmit_interval = self.TP
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


    def send(self, msg):
        pass


    def recved(self, msg):
        pass


    def expired(self):
        pass


class PlainClientTransaction(Transaction):
    def transmit(self, msg):
        msg["via"] = [ Via(self.manager.get_addr(), self.branch) ]
        super(PlainClientTransaction, self).transmit(msg)


    def send(self, request):
        self.change_state(self.TRANSMITTING)
        self.transmit(request)


    def recved(self, response):
        if self.state == self.TRANSMITTING:
            self.report(response)
            self.change_state(self.LINGERING)
        elif self.state == self.LINGERING:
            pass
        else:
            raise Error("Hm?")


    def expired(self):
        if self.state == self.TRANSMITTING:
            self.report(None)


class PlainServerTransaction(Transaction):
    def __init__(self, *args, **kwargs):
        super(PlainServerTransaction, self).__init__(*args, **kwargs)
        
        self.incoming_via = None
        
        
    def transmit(self, msg):
        if msg["via"] != self.incoming_via:  # FIXME: what kind of equality is this?
            raise Error("Don't mess with the Via headers!")
        super(PlainServerTransaction, self).transmit(msg)


    def recved(self, request):
        if self.state == self.WAITING:
            if not self.incoming_via:
                self.incoming_via = request["via"]
                self.report(request)
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


    def recved(self, response):
        raise Error("WAT?")


class InviteClientTransaction(PlainClientTransaction):
    def __init__(self, *args, **kwargs):
        super(InviteClientTransaction, self).__init__(*args, **kwargs)

        self.acks_by_remote_tag = {}
        self.statuses_by_remote_tag = {}


    def create_and_send_ack(self, ack_branch, remote_tag, sdp):
        if self.state != self.LINGERING:
            self.change_state(self.LINGERING)  # now we can expire
        
        ack = AckClientTransaction(self.manager, None, ack_branch)
        
        ack_params = self.outgoing_msg.copy()
        to = ack_params["to"]
        
        ack_params["method"] = "ACK"
        ack_params["to"] = Nameaddr(to.uri, to.name, dict(to.params, tag=remote_tag))
        ack_params["sdp"] = sdp

        # These won't be public
        self.acks_by_remote_tag[remote_tag] = ack
        ack.send(ack_params)


    #def create_cancel(self):
    #    self.change_state(self.LINGERING)  # may extend lingering time (for 487)
    #    return PlainClientTransaction(self.manager, self.report, self.branch)


    def recved(self, response):
        remote_tag = response["to"].params.get("tag")
        ack = self.acks_by_remote_tag.get(remote_tag)
        
        if ack:
            ack.retransmit()  # so we don't extend the lingering time as well
        else:
            code = response["status"].code
            
            # Don't remember and report 100 responses, as thay may have no remote tag.
            # And they are h2h anyway, so the dialog shouldn't care.
            if code > 100:
                if not remote_tag:
                    print("Invite response without to tag!")
                    
                statuses = self.statuses_by_remote_tag.setdefault(remote_tag, set())
            
                if code not in statuses:
                    statuses.add(code)
                    self.report(response)

            if code >= 300:
                # final non-2xx responses are ACK-ed here in the same transaction
                self.create_and_send_ack(self.branch, remote_tag, None)

            if self.state != self.WAITING:
                if code < 200:
                    self.change_state(self.LINGERING)  # may extend lingering time
                else:
                    self.change_state(self.WAITING)  # indefinitely for creating an ACK


    def expired(self):
        if self.state == self.TRANSMITTING:
            self.report(None)  # nothing at all
        elif self.state == self.LINGERING:
            # expired only after a provisional response, or after sending an ACK
            if not self.acks_by_remote_tag:
                self.report(None)
        else:
            raise Error("Hm?")


class InviteServerTransaction(PlainServerTransaction):
    def recved(self, request):
        if self.state == self.WAITING:
            if not self.incoming_via:
                self.incoming_via = request["via"]
                self.report(request)
        elif self.state == self.PROVISIONING:
            self.retransmit()
        elif self.state == self.TRANSMITTING:
            pass  # response is retransmitted automatically now
        else:
            pass  # stupid client retransmitted the request after acking the response


    def send(self, response):
        if is_virtual_response(response):
            # A virtual response means we got ACKed
            self.recved_ack()
        else:
            new_state = self.PROVISIONING if response["status"].code < 200 else self.TRANSMITTING
            self.change_state(new_state)
            self.transmit(response)


    def expired(self):
        if self.state == self.TRANSMITTING:
            self.report(None)
        else:
            raise Error("Hm?")


    def recved_ack(self):
        print("InviteServer ACKed!")
        
        if self.state == self.TRANSMITTING:
            self.change_state(self.LINGERING)
        elif self.state == self.LINGERING:
            pass  # duplicate ACK received
        else:
            pass  # stupid client, we haven't even responded yet


class AckServerTransaction(PlainServerTransaction):
    def retransmit(self):
        if not is_virtual_response(self.outgoing_msg):
            raise Error("Nonvirtual response to AckServerTransaction!")


class TransactionManager(object):
    def __init__(self, transmission, addr):
        self.transmission = transmission
        self.addr = addr
        self.client_transactions = {}  # by (branch, method)
        self.server_transactions = {}  # by (branch, method)


    def transmit(self, msg):
        self.transmission(msg)
        
        
    def get_addr(self):
        return self.addr


    def maintain_transactions(self, now, transactions):
        for tr_id, tr in list(transactions.items()):
            if tr.expiration_deadline and tr.expiration_deadline <= now:
                tr.expired()
                transactions.pop(tr_id)
            elif tr.retransmit_deadline and tr.retransmit_deadline <= now:
                tr.retransmit()


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


    def match_incoming_message(self, msg):
        #print("Match incoming:")
        #pprint(msg)
        branch, method = identify(msg)
        
        if msg["is_response"]:
            tr = self.client_transactions.get((branch, method))
            if tr:
                tr.recved(msg)
            else:
                print("Incoming response to unknown request, ignoring!")
            return True

        tr = self.server_transactions.get((branch, method))
        if tr:
            tr.recved(msg)
            return True
            
        if method == "CANCEL":
            # CANCEL-s may happen outside of any dialogs, so process them here
            report = None

            # FIXME: not necessarily INVITE
            invite_tr = self.server_transactions.get((branch, "INVITE"))
            if invite_tr:
                report = invite_tr.report

            tr = PlainServerTransaction(weakref.proxy(self), report, branch)
            self.add_transaction(tr, "CANCEL")

            tr.recved(msg)
            
            if invite_tr:
                pass # Send OK here for the cancel
            else:
                pass # Or an error
                
            return True
            
        if method == "ACK":
            invite_tr = self.server_transactions.get((branch, "INVITE"))
            if invite_tr:
                # We must have sent a non-200 response to this, so no dialog was created.
                # Send a virtual ACK response to notify the transaction.
                # But don't create an AckServerTransaction here.
                print("ACKing non-200 response")
                invite_tr.send(make_virtual_response())
                return True
                
        return False


    def create_incoming_request(self, msg, report):
        branch, method = identify(msg)

        if method == "INVITE":
            tr = InviteServerTransaction(weakref.proxy(self), report, branch)
        elif method == "ACK":
            # Must create a server transaction to swallow duplicates,
            # we don't want to bother the dialogs unnecessarily.
            tr = AckServerTransaction(weakref.proxy(self), report, branch)
        else:
            tr = PlainServerTransaction(weakref.proxy(self), report, branch)

        self.add_transaction(tr, method)
        tr.recved(msg)
        

    def send_message(self, msg, related_msg=None, report=None):
        if msg["is_response"]:
            if not related_msg:
                raise Error("Related request is not given for response!")
                
            request = related_msg
            tr = self.server_transactions.get(identify(request))
            if tr:
                tr.send(msg)
            else:
                print("Outgoing response to unknown request, ignoring!")
            return

        if not report:
            raise Error("No report handler for a request!")

        method = msg["method"]

        if method == "ACK":
            if not related_msg:
                raise Error("Related response is not given for ACK!")
                
            response_params = related_msg
            invite_tr = self.client_transactions.get(identify(response_params))
            if not invite_tr:
                raise Error("No transaction to ACK!")
            
            branch = generate_branch()  # 2xx-ACK
            remote_tag = response_params["to"].params["tag"]
            sdp = msg.get("sdp")
            invite_tr.create_and_send_ack(branch, remote_tag, sdp)
        elif method == "CANCEL":
            if not related_msg:
                raise Error("Related request is not given for CANCEL!")
            
            request_params = related_msg
            branch, method = identify(request_params)

            tr = PlainClientTransaction(weakref.proxy(self), report, branch)
            self.add_transaction(tr, "CANCEL")
        
            cancel_params = request_params.copy()
            cancel_params["method"] = "CANCEL"
        
            tr.send(cancel_params)
        elif method == "INVITE":
            tr = InviteClientTransaction(weakref.proxy(self), report, generate_branch())
            self.add_transaction(tr, method)
            tr.send(msg)
        else:
            tr = PlainClientTransaction(weakref.proxy(self), report, generate_branch())
            self.add_transaction(tr, method)
            tr.send(msg)
