from __future__ import print_function, unicode_literals, absolute_import

import re
import collections
from sdp import Sdp

META_HEADER_FIELDS = [ "is_response", "method", "uri", "status", "sdp", "hop" ]

class FormatError(Exception):
    pass


Addr = collections.namedtuple("Addr", [ "host", "port" ])
Status = collections.namedtuple("Status", [ "code", "reason" ])
Via = collections.namedtuple("Via", [ "addr", "branch" ])  # TODO: improve!


class Hop(object):
    def __init__(self, local_addr=None, remote_addr=None, iface=None):
        self.local_addr = local_addr
        self.remote_addr = remote_addr
        self.iface = iface
        
    def __repr__(self):
        return "Hop(local_addr=%r, remote_addr=%r, iface=%r)" % (
            self.local_addr, self.remote_addr, self.iface
        )


def parse_digest_params(value):
    m = re.search(r"^Digest\s+(.*)", value)
    if not m:
        return None
        
    items = {}
    for x in m.group(1).split(","):
        k, v = x.split("=", 1)
        items[k] = v.strip('"')
        
    return items


class WwwAuth(collections.namedtuple("WwwAuth", [ "realm", "nonce" ])):
    def print(self):
        return 'Digest realm="%s",nonce="%s"' % (self)
        
    @classmethod
    def parse(cls, value):
        items = parse_digest_params(value)
        values = [ items[x] for x in cls._fields ]
        return cls(*values)
        
    
class Auth(collections.namedtuple("Auth", [ "realm", "nonce", "username", "uri", "response" ])):

    def print(self):
        return 'Digest realm="%s",nonce="%s",username="%s",uri="%s",response="%s"' % (self)
        
    @classmethod
    def parse(cls, value):
        items = parse_digest_params(value)
        values = [ items[x] for x in cls._fields ]
        return cls(*values)


def parse_parts(parts):
    params = collections.OrderedDict()

    for part in parts:
        if "=" in part:
            k, v = part.split("=")
            params[k] = v
        else:
            params[part] = True

    return params


def print_params(params):
    return [ "%s=%s" % (k, v) if v is not True else k for k, v in params.items() if v]


class Uri(collections.namedtuple("Uri", "addr user params")):
    def __new__(cls, addr, user=None, params=None):
        return super(Uri, cls).__new__(cls, addr, user, params or {})


    def print(self):
        host, port = self.addr
        hostport = "%s:%d" % (host, port) if port else host
        userhostport = "%s@%s" % (self.user, hostport) if self.user else hostport
        params = print_params(self.params)

        return "sip:" + ";".join([userhostport] + params)


    @classmethod
    def parse(cls, uri):
        parts = uri.split(";")
        m = re.search("^sip:(([\\w.+-]+)@)?([\\w.-]+)(:(\\d+))?$", parts[0])
        if not m:
            raise FormatError("Invalid SIP URI: %r" % uri)

        user = m.group(2)
        host = m.group(3)
        port = int(m.group(5)) if m.group(5) else None

        params = parse_parts(parts[1:])

        return cls(Addr(host, port), user, params)


class Nameaddr(collections.namedtuple("Nameaddr", "uri name params")):
    def __new__(cls, uri, name=None, params=None):
        return super(Nameaddr, cls).__new__(cls, uri, name, params or {})


    def print(self):
        # If the URI contains URI parameters, not enclosing it in angle brackets would
        # be interpreted as header parameters. So enclose them always just to be safe.

        uri = self.uri.print()
        name = '"%s"' if self.name and " " in self.name else self.name
        first_part = ["%s <%s>" % (name, uri) if name else "<%s>" % uri]
        last_parts = print_params(self.params)
        full = ";".join(first_part + last_parts)

        return full


    @classmethod
    def parse(cls, contact):
        m = re.search('^\\s*(".*?"|[^"]*?)\\s*<(.*?)>(.*)$', contact)
        if m:
            name = m.group(1).strip('"')
            name = name or None
            uri = m.group(2)  # may contain semicolons itself
            parts = m.group(3).split(";")
        else:
            name = None
            parts = contact.split(";")
            uri = parts[0]

        return cls(uri=Uri.parse(uri), name=name, params=parse_parts(parts[1:]))
        
        
    def tagged(self, tag):
        if self.params.get("tag") or not tag:
            return self
        else:
            return Nameaddr(self.uri, self.name, dict(self.params, tag=tag))


def print_message(initial_line, params, body):
    header_lines = []

    for k, v in params.items():
        field = k.replace("_", "-").title()

        if isinstance(v, list):
            header_lines.extend(["%s: %s" % (field, i) for i in v])
        else:
            header_lines.append("%s: %s" % (field, v))

    lines = [initial_line] + header_lines + ["", body]
    return "\r\n".join(lines)


def parse_message(msg):
    lines = msg.split("\r\n")
    initial_line = lines.pop(0)
    body = ""

    params = {
        "via": [],
        "route": [],
        "record_route": []
    }

    while lines:
        line = lines.pop(0)

        if not line:
            body = "\r\n".join(lines)
            break

        k, s, v = line.partition(":")
        k = k.strip().replace("-", "_").lower()
        v = v.strip()

        if k not in params:
            params[k] = v
        elif isinstance(params[k], list):
            params[k].extend([x.strip() for x in v.split(",")])  # TODO: which fields?
        else:
            raise FormatError("Duplicate field received: %s" % k)

    return initial_line, params, body


def print_structured_message(params):
    if params["is_response"] is True:
        code, reason = params["status"]
        initial_line = "SIP/2.0 %d %s" % (code, reason)
    elif params["is_response"] is False:
        initial_line = "%s %s SIP/2.0" % (params["method"], params["uri"].print())
    else:
        raise FormatError("Invalid structured message!")

    p = collections.OrderedDict()
    mandatory_fields = ["from", "to", "call_id", "cseq", "via"]  # order these nicely
    other_fields = [f for f in params if f not in mandatory_fields]

    for field in mandatory_fields + other_fields:
        if params[field] is None:
            pass
        elif field in ("from", "to", "contact", "www_authenticate", "authorization"):
            p[field] = params[field].print()
        elif field == "cseq":
            p[field] = "%d %s" % (params[field], params["method"])  # ACK? CANCEL?
        elif field == "via":
            p[field] = ["SIP/2.0/UDP %s:%d;branch=z9hG4bK%s" % (a + (b,)) for a, b in params[field]]
        elif field not in META_HEADER_FIELDS:
            p[field] = params[field]

    body = ""
    sdp = params.get("sdp")
    if sdp:
        body = sdp.print()

    return print_message(initial_line, p, body)


def parse_structured_message(msg):
    initial_line, params, body = parse_message(msg)
    p = {}

    m = re.search("^SIP/2.0\\s+(\\d\\d\\d)\\s+(.+)$", initial_line)
    if m:
        p["is_response"] = True
        p["status"] = Status(code=int(m.group(1)), reason=m.group(2))

    m = re.search("^(\\w+)\\s+(\\S+)\\s*SIP/2.0\\s*$", initial_line)
    if m:
        p["is_response"] = False
        method, uri = m.groups()
        p["method"] = method
        p["uri"] = Uri.parse(uri)

    if not p:
        raise FormatError("Invalid message!")

    for field in params:
        if field in ("from", "to", "contact"):
            p[field] = Nameaddr.parse(params[field])
        elif field in ("www_authenticate"):
            p[field] = WwwAuth.parse(params[field])
        elif field in ("authorization"):
            p[field] = Auth.parse(params[field])
        elif field == "cseq":
            cseq_num, cseq_method = params[field].split()
            p[field] = int(cseq_num)
            if "method" in p:
                if p["method"] != cseq_method:
                    print("Mismatching method in cseq field!")
            else:
                p["method"] = cseq_method.upper()  # Necessary for CANCEL responses
        elif field == "via":
            def do_one(s):
                m = re.search("SIP/2.0/UDP ([^:;]+)(:(\\d+))?;branch=z9hG4bK([^;]+)", s)
                if not m:
                    raise FormatError("Invalid Via!")
                host, port, branch = m.group(1), int(m.group(3)) if m.group(3) else None, m.group(4)
                return Via(Addr(host, port), branch)

            p[field] = [do_one(s) for s in params[field]]
        elif field not in META_HEADER_FIELDS:
            p[field] = params[field]
        else:
            print("Warning, header field ignored: '%s'!" % field)

    sdp = None
    if body:
        sdp = Sdp.parse(body)
        
    p["sdp"] = sdp

    return p

