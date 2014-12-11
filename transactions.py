from __future__ import print_function, unicode_literals, absolute_import

from pprint import pprint, pformat
import uuid
import datetime

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


class Owner(object):
    def recved_request(self, request):
        pass

    def recved_response(self, response):
        pass

    def no_response(self, request):
        pass


class Transaction(object):
    WAITING = 1
    PROVISIONING = 2
    TRANSMITTING = 3
    LINGERING = 4

    T1 = datetime.timedelta(milliseconds=500)
    T2 = datetime.timedelta(milliseconds=4000)
    TP = datetime.timedelta(milliseconds=10000)

    def __init__(self, transport, report, branch=None):
        self.transport = transport
        self.report = report
        self.branch = branch or uuid.uuid4().hex

        self.incoming_msg = None
        self.outgoing_msg = None
        self.state = self.WAITING

        self.retransmit_interval = None
        self.retransmit_deadline = None
        self.expiration_deadline = None


    def change_state(self, state, outgoing_msg=None):
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
        else:
            raise Error("Change to what state?")

        if outgoing_msg:
            self.outgoing_msg = self.prepare(outgoing_msg)
            self.retransmit()


    def retransmit(self):
        self.transport.send(self.outgoing_msg)

        if self.retransmit_interval:
            self.retransmit_deadline = datetime.datetime.now() + self.retransmit_interval

        if self.state == self.TRANSMITTING:
            self.retransmit_interval = min(self.retransmit_interval * 2, self.T2)


    def prepare(self, msg):
        return msg


    def send(self, msg):
        pass


    def recved(self, msg):
        pass


    def expired(self):
        pass


class PlainRequest(Transaction):
    def prepare(self, msg):
        msg["via"] = [ Via(self.transport.get_addr(), self.branch) ]
        return msg


    def send(self, request):
        self.change_state(self.TRANSMITTING, request)


    def recved(self, response):
        if self.state == self.TRANSMITTING:
            self.incoming_msg = response
            self.report(response)
            self.change_state(self.LINGERING)
        elif self.state == self.LINGERING:
            pass
        else:
            raise Error("Hm?")


    def expired(self):
        if self.state == self.TRANSMITTING:
            self.report(None)


class PlainResponse(Transaction):
    def prepare(self, msg):
        if msg["via"] != self.incoming_msg["via"]:
            raise Error("Don't mess with the Via headers!")

        return msg


    def recved(self, request):
        if self.state == self.WAITING:
            if not self.incoming_msg:
                self.incoming_msg = request
                self.report(request)
        elif self.state == self.LINGERING:
            self.retransmit()
        else:
            raise Error("Hm?")


    def send(self, response):
        self.change_state(self.LINGERING, response)


class AckRequest(PlainRequest):
    def send(self, request):
        self.change_state(self.LINGERING, request)


    def recved(self, response):
        raise Error("WAT?")


class InviteRequest(PlainRequest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.ack = None


    def create_ack(self):
        code = self.incoming_msg["status"].code

        if code < 200:
            raise Error("Hm?")
        elif code < 300:
            ack_branch = self.generate_branch()
        else:
            ack_branch = self.branch

        self.change_state(self.LINGERING)  # now we can expire
        self.ack = AckRequest(self.transport, self.report, ack_branch)
        return self.ack


    def create_cancel(self):
        return PlainRequest(self.transport, self.report, self.branch)


    def recved(self, response):
        if self.ack:
            self.ack.retransmit()  # so we don't extend the lingering time as well
        elif self.state in (self.TRANSMITTING, self.LINGERING):
            if response != self.incoming_msg:
                self.incoming_msg = response
                self.report(response)

            # linger for provisional responses, wait (for ack creation) for final ones
            new_state = self.LINGERING if response["status"].code < 200 else self.WAITING
            self.change_state(new_state)
        else:
            raise Error("Hm?")


    def expired(self):
        if self.state == self.TRANSMITTING:
            self.report(None)  # nothing at all
        elif self.state == self.LINGERING:
            if not self.ack:
                self.report(None)  # no final response
        else:
            raise Error("Hm?")


class InviteResponse(PlainResponse):
    def recved(self, request):
        if self.state == self.WAITING:
            if not self.incoming_msg:
                self.incoming_msg = request
                self.report(request)
        elif self.state == self.PROVISIONING:
            self.retransmit()
        elif self.state == self.TRANSMITTING:
            pass  # response is retransmitted automatically now
        else:
            pass  # stupid client retransmitted the request after acking the response


    def send(self, response):
        new_state = self.PROVISIONING if response["status"].code < 200 else self.TRANSMITTING
        self.change_state(new_state, response)


    def expired(self):
        if self.state == self.TRANSMITTING:
            self.report(None)
        else:
            raise Error("Hm?")


    def recved_ack(self):
        if self.state == self.TRANSMITTING:
            self.change_state(self.LINGERING)
        elif self.state == self.LINGERING:
            pass  # duplicate ACK received
        else:
            pass  # stupid client, we haven't even responded yet


class AckSink(PlainResponse):
    def recved(self, request):
        pass


    def send(self, response):
        raise Error("Responding to an ACK?")


class TransactionManager(object):
    def __init__(self, transport):
        self.transport = transport
        self.transactions = {}  # by (branch, method)


    def cleanup(self):
        now = datetime.datetime.now()

        for tr_id, tr in list(self.transactions.items()):
            if tr.expiration_deadline and tr.expiration_deadline <= now:
                tr.expired()
                self.transactions.pop(tr_id)


    def add_transaction(self, tr, method):
        tr_id = (tr.branch, method)
        self.transactions[tr_id] = tr


    def find_transaction(self, msg):
        try:
            branch = msg["via"][0].branch
        except Exception as e:
            raise Error("No Via header in message!")

        method = msg["method"]  # present in responses, too
        tr_id = (branch, method)
        return self.transactions.get(tr_id, None)


    def find_invite_transaction(self, related_msg, goal):
        if not related_msg:
            raise Error("Need related INVITE for sending %s!" % goal)

        if related_msg["method"] != "INVITE":
            raise Error("Related message is not an INVITE for sending %s!" % goal)

        invite_tr = self.find_transaction(related_msg)
        if not invite_tr:
            raise Error("No related INVITE found for sending %s!" % goal)

        return invite_tr


    def recved(self, msg, report=None):
        tr = self.find_transaction(msg)

        if not tr:
            method = msg["method"]
            try:
                branch = msg["via"][0].branch
            except Exception:
                raise Error("No Via header in incoming message!")

            if not report:
                if method == "CANCEL":
                    invite_tr = self.transactions.get((branch, "INVITE"), None)
                    if not invite_tr:
                        # CANCEL for nonexistent INVITE? Consider it handled
                        return True

                    report = invite_tr.report
                else:
                    return False

            # We're now retrying after finding an owner, so it must be an incoming request
            if method == "INVITE":
                tr = InviteResponse(self.transport, report, branch=branch)
            elif method == "ACK":
                tr = AckSink(self.transport, report, branch=branch)
            else:
                tr = PlainResponse(self.transport, report, branch=branch)

            self.add_transaction(tr, method)

        tr.recved(msg)
        return False


    def send(self, msg, report=None, related_msg=None):
        tr = self.find_transaction(msg)

        if not tr:
            if "status" in msg:
                raise Error("No transaction for sending response!")

            if not report:
                raise Error("No owner to send request!")

            method = msg["method"]
            if method == "INVITE":
                tr = InviteRequest(self.transport, report)
            elif method == "ACK":
                tr = self.find_invite_transaction(related_msg, "ACK").create_ack()
            elif method == "CANCEL":
                tr = self.find_invite_transaction(related_msg, "CANCEL").create_cancel()
            else:
                tr = PlainRequest(self.transport, report)

            self.add_transaction(tr, method)

        tr.send(msg)

