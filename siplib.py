import socket
import uuid
import re
import collections


# addr = (host, port)
# user = (username, displayname)


class Error(Exception):
    pass


def safe_update(target, source):
    for k, v in source:
        if k in target:
            raise Error("Can't overwrite field %r!" % k)
        target[k] = v

    return target


def print_sip_uri(addr, user):
    u, d = user
    h, p = addr

    return "sip:%s@%s:%d" % (u, h, p) if u else "sip:%s:%d" % (h, p)


def parse_sip_uri(uri, display_name=None):
    m = re.search("^sip:(([\\w.+-]+)@)?([\\w.-]+)(:(\\d+))?$", uri)
    if not m:
        raise Error("Invalid SIP URI: %r" % uri)

    u = m.group(2)
    h = m.group(3)
    p = int(m.group(5)) if m.group(5) else 5060

    addr = h, p
    user = u, display_name

    return addr, user


def print_user(addr, user, tag):
    dn = user[1]

    uri = print_sip_uri(addr, user)
    name = '"%s"' if dn and " " in dn else dn
    first_part = ["%s <%s>" % (name, uri) if name else uri]
    last_parts = ["%s=%s" % ("tag", tag)] if tag else []
    full = ";".join(first_part + last_parts)

    return full


def parse_user(s):
    parts = s.split(";")

    m = re.search('^\\s*("(.*?)"|\\S*?)\\s*<(\\S+?)>', parts[0])
    if m:
        display_name = m.group(2) or m.group(1)
        addr, user = parse_sip_uri(m.group(3), display_name)
    else:
        addr, user = parse_sip_uri(parts[0])

    tag = None
    for part in parts:
        if part.startswith("tag="):
            tag = part[4:]

    return addr, user, tag


def print_message(initial_line, params):
    header_lines = []

    for k, v in params.items():
        field = k.replace("_", "-").title()

        if isinstance(v, list):
            header_lines.extend(["%s: %s" % (field, i) for i in v])
        elif k != "sdp":
            header_lines.append("%s: %s" % (field, v))

    lines = [initial_line] + header_lines + ["", ""]  # FIXME: use SDP
    return "\r\n".join(lines)


def parse_message(msg, initial_pattern):
    lines = msg.split("\r\n")
    initial_line = lines.pop(0)

    m = re.search(initial_pattern, initial_line)
    if not m:
        raise Error("Message initial line mismatch!")

    params = dict(m.groupdict(),
        via=[],
        route=[],
        record_route=[]
    )

    while lines:
        line = lines.pop(0)

        if not line:
            params['sdp'] = "\r\n".join(lines)
            break

        k, s, v = line.partition(":")
        k = k.strip().replace("-", "_").lower()
        v = v.strip()

        if k not in params:
            params[k] = v
        elif isinstance(params[k], list):
            params[k].extend([x.strip() for x in v.split(",")])  # TODO: which fields?
        else:
            raise Error("Duplicate field received: %s" % k)

    return params


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
        cseq = self.last_sent_cseq
        branch = "xxx"

        if not self.call_id:
            self.call_id = uuid.uuid4().hex

        user_params = user_params.copy()
        method = user_params.pop("method")
        request_line = "%s %s SIP/2.0" % (
            method, print_sip_uri(self.remote_addr, self.remote_user)
        )

        f = collections.OrderedDict()
        f["from"] = print_user(self.local_addr, self.local_user, self.local_tag)
        f["to"] = print_user(self.remote_addr, self.remote_user, self.remote_tag)
        f["call_id"] = self.call_id
        f["cseq"] = "%d %s" % (cseq, method)  # ACK? CANCEL?
        f["via"] = ["SIP/2.0/UDP %s:%d;branch=z9hG4bK%s" % (self.local_addr + (branch,))]
        f["maxfwd"] = "50"
        safe_update(f, user_params)

        return print_message(request_line, f)


    def parse_request(self, msg):
        initial_pattern = "^(?P<method>\\w+)\\s+(?P<uri>\\S+)\\s*SIP/2.0\\s*$"
        params = parse_message(msg, initial_pattern)

        method = params["method"]
        uri = params["uri"]
        from_addr, from_user, from_tag = parse_user(params["from"])
        to_addr, to_user, to_tag = parse_user(params["to"])
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
        user_params = user_params.copy()
        status = int(user_params.pop("status"))
        reason = user_params.pop("reason")
        response_line = "SIP/2.0 %d %s" % (status, reason)

        to_addr, to_user, to_tag = parse_user(request_params["to"])
        to_tag = self.local_tag if status != 100 else to_tag

        params = collections.OrderedDict()
        params["from"] = request_params["from"]
        params["to"] = print_user(to_addr, to_user, to_tag)
        params["call_id"] = request_params["call_id"]
        params["cseq"] = request_params["cseq"]
        params["via"] = request_params["via"]
        safe_update(params, user_params)

        return print_message(response_line, params)


    def parse_response(self, msg):
        initial_pattern = "^SIP/2.0\\s+(?P<status>\\d\\d\\d)\\s+(?P<reason>.+)$"
        params = parse_message(msg, initial_pattern)

        status = int(params["status"])
        reason = params["reason"]
        from_addr, from_user, from_tag = parse_user(params["from"])
        to_addr, to_user, to_tag = parse_user(params["to"])
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

