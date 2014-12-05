from __future__ import print_function, unicode_literals, absolute_import

import socket
import uuid
from pprint import pprint

import format

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
    def __init__(self, local_addr, local_user, remote_addr=None, remote_user=None, proxy_addr=None):
        # Things in the From/To fields
        self.local_addr = local_addr
        self.local_user = local_user
        self.local_tag = uuid.uuid4().hex
        self.remote_addr = remote_addr
        self.remote_user = remote_user
        self.remote_tag = None

        # The peer's contact address, received in Contact, sent in RURI
        self.peer_addr = remote_addr
        self.peer_user = remote_user
        self.proxy_addr = proxy_addr

        self.call_id = None
        self.routes = []

        self.last_sent_cseq = 0
        self.last_recved_method = None
        self.last_recved_cseq = None
        self.last_recved_vias = None

        self.socket = None


    def socket(self):
        if not self.local_socket:
            self.local_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.local_socket.bind(local_addr)

        return self.local_socket


    def print_request(self, user_params):
        self.last_sent_cseq += 1
        branch = "xxx"

        if not self.call_id:
            self.call_id = uuid.uuid4().hex

        f = {
            "uri": (self.remote_addr, self.remote_user),
            "from": (self.local_addr, self.local_user, dict(tag=self.local_tag)),
            "to": (self.remote_addr, self.remote_user, dict(tag=self.remote_tag)),
            "call_id": self.call_id,
            "cseq": self.last_sent_cseq,
            "via": [(self.local_addr, branch)],
            "maxfwd": 50,
            "contact": (self.local_addr, self.local_user, {})
        }
        safe_update(f, user_params)

        return format.print_structured_message(f)


    def parse_request(self, msg):
        params = format.parse_structured_message(msg)

        if "uri" not in params:
            raise Error("Not a request!")

        from_addr, from_user, from_params = params["from"]
        from_tag = from_params["tag"]
        to_addr, to_user, to_params = params["to"]
        to_tag = to_params.get("tag", None)
        call_id = params["call_id"]
        #cseq = params["cseq"]  # TODO: use?

        if to_addr != self.local_addr or to_user[0] != self.local_user[0]:
            raise Error("Mismatching recipient!")

        if self.remote_tag:
            if call_id != self.call_id:
                raise Error("Mismatching call id!")

            if from_tag != self.remote_tag:
                raise Error("Mismatching remote tag!")

            if to_tag != self.local_tag:
                raise Error("Mismatching local tag!")
        else:
            if to_tag:
                raise Error("Unexpected to tag!")

            self.remote_addr = from_addr
            self.remote_user = from_user
            self.remote_tag = from_tag
            self.call_id = call_id

        return params


    def print_response(self, user_params, request_params):
        code, reason = user_params["status"]
        to_addr, to_user, to_params = request_params["to"]
        if code != 100:
            to_params = dict(to_params, tag=self.local_tag)

        params = {
            "from": request_params["from"],
            "to": (to_addr, to_user, to_params),
            "call_id": request_params["call_id"],
            "cseq": request_params["cseq"],
            "via": request_params["via"],
            "method": request_params["method"],  # only for internal use
            "contact": (self.local_addr, self.local_user, {})
        }
        safe_update(params, user_params)

        return format.print_structured_message(params)


    def parse_response(self, msg):
        params = format.parse_structured_message(msg)

        if "status" not in params:
            raise Error("Not a response!")

        from_addr, from_user, from_params = params["from"]
        from_tag = from_params["tag"]
        to_addr, to_user, to_params = params["to"]
        to_tag = to_params.get("tag", None)
        call_id = params["call_id"]
        #cseq = params["cseq"]  # TODO: use?

        if from_addr != self.local_addr or from_user[0] != self.local_user[0]:
            raise Error("Mismatching recipient!")

        if self.remote_tag:
            if call_id != self.call_id:
                raise Error("Mismatching call id!")

            if to_tag != self.remote_tag:
                raise Error("Mismatching remote tag!")

            if from_tag != self.local_tag:
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

