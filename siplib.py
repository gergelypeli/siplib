from __future__ import print_function, unicode_literals, absolute_import

import socket
import uuid
from pprint import pprint, pformat

import format
from format import Addr, Uri, Contact, Via, Status

# addr = (host, port)
# user = (username, displayname)


class Error(Exception):
    pass


def safe_update(target, source):
    for k, v in source.items():
        if k in target:
            raise Error("Can't overwrite field %r!" % k)
        target[k] = v

    return target


class Leg(object):
    def __init__(self, local_uri, local_name, remote_uri=None, proxy_addr=None):
        # Things in the From/To fields
        self.local_contact = Contact(local_uri, local_name, dict(tag=uuid.uuid4().hex))
        self.remote_contact = Contact(remote_uri)

        # The peer's contact address, received in Contact, sent in RURI
        self.peer_uri = remote_uri
        self.proxy_addr = proxy_addr

        self.call_id = None
        self.routes = []
        self.last_sent_cseq = 0

        self.socket = None


    def socket(self):
        if not self.local_socket:
            self.local_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.local_socket.bind(self.local_contact.uri.addr)

        return self.local_socket


    def print_request(self, user_params):
        self.last_sent_cseq += 1
        branch = "xxx"

        if not self.call_id:
            self.call_id = uuid.uuid4().hex

        f = {
            "uri": self.peer_uri,
            "from": self.local_contact,
            "to": self.remote_contact,
            "call_id": self.call_id,
            "cseq": self.last_sent_cseq,
            "via": [Via(self.local_contact.uri.addr, branch)],
            "maxfwd": 50,
            "contact": Contact(self.local_contact.uri)
        }
        safe_update(f, user_params)

        return format.print_structured_message(f)


    def parse_request(self, msg):
        params = format.parse_structured_message(msg)

        if "uri" not in params:
            raise Error("Not a request!")

        from_contact = params["from"]
        from_tag = from_contact.params["tag"]
        to_contact = params["to"]
        to_tag = to_contact.params.get("tag", None)
        call_id = params["call_id"]
        local_tag = self.local_contact.params["tag"]
        remote_tag = self.remote_contact.params.get("tag")
        #cseq = params["cseq"]  # TODO: use?

        if to_contact.uri != self.local_contact.uri:
            raise Error("Mismatching recipient: %s %s" % (to_contact.uri, self.local_contact.uri))

        if remote_tag:
            if call_id != self.call_id:
                raise Error("Mismatching call id!")

            if from_tag != remote_tag:
                raise Error("Mismatching remote tag!")

            if to_tag != local_tag:
                raise Error("Mismatching local tag!")
        else:
            if to_tag:
                raise Error("Unexpected to tag!")

            self.remote_contact = from_contact
            self.call_id = call_id

        return params


    def print_response(self, user_params, request_params):
        status = user_params["status"]
        to_contact = request_params["to"]  # TODO: don't alter
        if status.code != 100:
            to_contact.params["tag"] = self.local_contact.params["tag"]

        params = {
            "from": request_params["from"],
            "to": request_params["to"],
            "call_id": request_params["call_id"],
            "cseq": request_params["cseq"],
            "via": request_params["via"],
            "method": request_params["method"],  # only for internal use
            "contact": Contact(self.local_contact.uri)
        }
        safe_update(params, user_params)

        return format.print_structured_message(params)


    def parse_response(self, msg):
        params = format.parse_structured_message(msg)

        if "status" not in params:
            raise Error("Not a response!")

        from_contact = params["from"]
        from_tag = from_contact.params["tag"]
        to_contact = params["to"]
        to_tag = to_contact.params.get("tag", None)
        call_id = params["call_id"]
        local_tag = self.local_contact.params["tag"]
        remote_tag = self.remote_contact.params.get("tag")

        if from_contact != self.local_contact:
            raise Error("Mismatching recipient!")

        if remote_tag:
            if call_id != self.call_id:
                raise Error("Mismatching call id!")

            if to_tag != remote_tag:
                raise Error("Mismatching remote tag!")

            if from_tag != local_tag:
                raise Error("Mismatching local tag!")
        else:
            if not to_tag:
                raise Error("Missing to tag!")

            self.remote_tag = from_tag

        return params


    def send_request(self, params):
        msg = self.print_request(params)
        self.socket().sendto(msg, proxy_addr or peer_addr)


    def recv_request(self):
        msg, addr = self.socket().recvfrom(65535)
        return parse_request(msg)

