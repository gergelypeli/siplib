from __future__ import print_function, unicode_literals, absolute_import

import re
import collections
from sdp import Sdp

# TODO: use attributes instead for these
META_HEADER_FIELDS = [ "is_response", "method", "uri", "status", "sdp", "hop", "user_params", "authname" ]
LIST_HEADER_FIELDS = [ "via", "route", "record_route", "contact" ]


class FormatError(Exception):
    pass


class SipError(Exception):
    def __init__(self, status):
        self.status = status
        
        
    def __str__(self):
        return "%d %s" % self.status
        

Addr = collections.namedtuple("Addr", [ "host", "port" ])
Addr.__new__.__defaults__ = (None,)

class Status(collections.namedtuple("Status", [ "code", "reason" ])):
    REASONS_BY_CODE = {
        100: "Trying",
        180: "Ringing",
        183: "Session Progress",
        200: "OK",
        400: "Bad Request",
        401: "Unauthorized",
        403: "Forbidden",
        404: "Not Found",
        408: "Request Timeout",
        481: "Transaction Does Not Exist",
        482: "Loop Detected",
        483: "Too Many Hops",
        486: "Busy Here",
        487: "Request Terminated",
        488: "Not Acceptable Here",
        491: "Request Pending",
        500: "Internal Error",
        501: "Not Implemented",
        503: "Service Unavailable",
        504: "Server Timeout",
        600: "Busy Everywhere",
        603: "Decline",
        604: "Does Not Exist Anywhere",
        606: "Not Acceptable"
    }
    
    def __new__(cls, code, reason=None):
        reason = reason or cls.REASONS_BY_CODE.get(code) or "Just because"
        return super().__new__(cls, code, reason)


class Rack(collections.namedtuple("Rack", [ "rseq", "cseq", "method" ])):
    def print(self):
        return "%d %d %s" % (self.rseq, self.cseq, self.method)
        
        
    @classmethod
    def parse(cls, value):
        rseq, cseq, method = value.split()
        
        return cls(int(rseq), int(cseq), method.upper())


class Via(collections.namedtuple("Via", [ "addr", "branch" ])):
    def print(self):
        return "SIP/2.0/UDP %s:%d;branch=z9hG4bK%s" % (self.addr + (self.branch,))


    @classmethod
    def parse(cls, value):
        m = re.search("SIP/2.0/UDP ([^:;]+)(:(\\d+))?;branch=z9hG4bK([^;]+)", value)
        if not m:
            raise FormatError("Invalid Via!")
        host, port, branch = m.group(1), int(m.group(3)) if m.group(3) else None, m.group(4)
        return cls(Addr(host, port), branch)


class Hop(collections.namedtuple("Hop", [ "local_addr", "remote_addr", "interface" ])):
    def __str__(self):
        return "%s:%d >-(%s)-> %s:%d" % (self.local_addr + (self.interface,) + self.remote_addr)


def must_match(pattern, s):
    m = re.search(pattern, s)
    if m:
        return m.groups()
    else:
        raise Exception("Not matched %r!" % pattern)


def parse_digest_params(value):
    rest, = must_match(r"^Digest\s+(.*)", value)
    items = {}
    
    while True:
        key, rest = must_match(r"^(\w+)=(.*)", rest)
        
        if rest.startswith('"'):
            value, rest = must_match(r'^"(.*?)"(.*)', rest)
        else:
            value, rest = must_match(r'^([^,]*)(.*)', rest)
            
        items[key] = value
        
        if not rest:
            break
        
        rest, = must_match("^,(.*)", rest)
        
    return items


class WwwAuth(collections.namedtuple("WwwAuth",
    [ "realm", "nonce", "domain", "opaque", "algorithm", "stale", "qop" ]
)):
    def __new__(cls, realm, nonce, domain=None, opaque=None, algorithm=None, stale=None, qop=None):
        return super().__new__(cls, realm, nonce, domain, opaque, algorithm, stale, qop)


    def print(self):
        components = [
            'realm="%s"' % self.realm if self.realm else None,
            'nonce="%s"' % self.nonce if self.nonce else None,
            'domain="%s"' % self.domain if self.domain else None,
            'opaque="%s"' % self.opaque if self.opaque else None,
            'algorithm=%s' % self.algorithm if self.algorithm else None,
            'stale=true' if self.stale else None,
            'qop="%s"' % ",".join(self.qop) if self.qop else None
        ]
        
        return 'Digest %s' % ",".join(c for c in components if c)

        
    @classmethod
    def parse(cls, value):
        items = parse_digest_params(value)
        
        if "stale" in items:
            items["stale"] = True if items["stale"].lower() == "true" else False
        if "qop" in items:
            items["qop"] = items["qop"].split(",")
            
        return cls(**items)
        
    
class Auth(collections.namedtuple("Auth",
    [ "realm", "nonce", "username", "uri", "response", "opaque", "algorithm", "qop", "cnonce", "nc" ]
)):
    def __new__(cls, realm, nonce, username, uri, response, opaque=None, algorithm=None, qop=None, cnonce=None, nc=None):
        self = super().__new__(cls, realm, nonce, username, uri, response, opaque, algorithm, qop, cnonce, nc)
        self.stale = False  # internally used attribute
        return self


    def print(self):
        components = [
            'realm="%s"' % self.realm if self.realm else None,
            'nonce="%s"' % self.nonce if self.nonce else None,
            'username="%s"' % self.username if self.username else None,
            'uri="%s"' % self.uri if self.uri else None,
            'response="%s"' % self.response if self.response else None,
            'opaque="%s"' % self.opaque if self.opaque else None,
            'algorithm=%s' % self.algorithm if self.algorithm else None,
            'qop=%s' % self.qop if self.qop else None,  # single token
            'cnonce="%s"' % self.cnonce if self.cnonce else None,
            'nc=%08x' % self.nc if self.nc is not None else None
        ]
    
        return 'Digest %s' % ",".join(c for c in components if c)
        

    @classmethod
    def parse(cls, value):
        items = parse_digest_params(value)
        
        if "nc" in items:
            items["nc"] = int(items["nc"], 16)
            
        return cls(**items)


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


class Uri(collections.namedtuple("Uri", "addr user scheme params")):
    def __new__(cls, addr, user=None, scheme=None, params=None):
        return super().__new__(cls, addr, user, scheme or "sip", params or {})


    def __hash__(self):
        # It's unlikely that two URIs differ only in parameters, but even in
        # that case the equality will sort that out, hashing is only a speedup.
        return hash((self.addr, self.user, self.scheme))
        
        
    def __str__(self):
        return self.print()
        

    def print(self):
        host, port = self.addr
        hostport = "%s:%d" % (host, port) if port else host
        parts = [ hostport ] + print_params(self.params)
        rest = ";".join(parts)
        rest = "%s@%s" % (self.user, rest) if self.user else rest
        uri = "%s:%s" % (self.scheme, rest)

        return uri


    @classmethod
    def parse(cls, uri):
        scheme, rest = uri.split(':', 1)
        
        if "@" in rest:
            user, rest = rest.split('@', 1)
        else:
            user = None
            
        # TODO: split password from user!
        parts = rest.split(";")
        
        if ":" in parts[0]:
            host, port = parts[0].split(':', 1)
            port = int(port)
        else:
            host = parts[0]
            port = None
        
        params = parse_parts(parts[1:])

        return cls(Addr(host, port), user, scheme, params)


class Nameaddr(collections.namedtuple("Nameaddr", "uri name params")):
    def __new__(cls, uri, name=None, params=None):
        return super().__new__(cls, uri, name, params or {})


    def print(self):
        # If the URI contains URI parameters, not enclosing it in angle brackets would
        # be interpreted as header parameters. So enclose them always just to be safe.

        uri = self.uri.print()
        name = '"%s"' % self.name if self.name and " " in self.name else self.name
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
            # Some header types cannot be joined into a comma separated list,
            # such as the authorization ones, since they contain a comma themselves.
            # So output separate headers always.
            header_lines.extend(["%s: %s" % (field, i) for i in v])
        else:
            header_lines.append("%s: %s" % (field, v))

    lines = [ initial_line ] + header_lines + [ "", body ]
    return "\r\n".join(lines)


def parse_message(msg):
    lines = msg.split("\r\n")
    initial_line = lines.pop(0)
    body = ""
    params = { field: [] for field in LIST_HEADER_FIELDS }  # TODO: auth related?

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
        elif field in ("from", "to", "www_authenticate", "authorization", "rack"):
            p[field] = params[field].print()
        elif field == "cseq":
            p[field] = "%d %s" % (params[field], params["method"])
        elif field == "rseq":
            p[field] = "%d" % params[field]
        elif field in ("contact", "via"):
            p[field] = [ f.print() for f in params[field] ]
        elif field in ("supported", "require"):
            p[field] = ", ".join(sorted(params[field]))
        elif field not in META_HEADER_FIELDS:
            p[field] = params[field]

    body = ""
    sdp = params.get("sdp")
    if sdp:
        body = sdp.print()
        p["content_type"] = "application/sdp"
        p["content_length"] = len(body)

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
        # TODO: refactor a bit!
        if field in ("from", "to"):
            p[field] = Nameaddr.parse(params[field])
        elif field in ("contact",):
            p[field] = [ Nameaddr.parse(s) for s in params[field] ]
        elif field in ("www_authenticate"):
            p[field] = WwwAuth.parse(params[field])
        elif field in ("authorization"):
            p[field] = Auth.parse(params[field])
        elif field == "cseq":
            cseq_num, cseq_method = params[field].split()
            p[field] = int(cseq_num)
            if "method" in p:
                if p["method"] != cseq_method:
                    raise FormatError("Mismatching method in cseq field!")
            else:
                p["method"] = cseq_method.upper()  # Necessary for CANCEL responses
        elif field == "rseq":
            p[field] = int(params[field])
        elif field == "rack":
            p[field] = Rack.parse(params[field])
        elif field == "via":
            p[field] = [ Via.parse(s) for s in params[field] ]
        elif field in ("supported", "require"):
            p[field] = set( x.strip() for x in params[field].split(",") )
        elif field not in META_HEADER_FIELDS:
            p[field] = params[field]
        else:
            print("Warning, header field ignored: '%s'!" % field)

    sdp = None
    if body:
        sdp = Sdp.parse(body)
        
    p["sdp"] = sdp

    return p


def make_simple_response(request, status, others=None):
    tag = "ROTFLMAO" if status.code > 100 else None
    
    params = {
        "is_response": True,
        "method": request["method"],
        "status": status,
        "from": request["from"],
        "to": request["to"].tagged(tag),
        "call_id": request["call_id"],
        "cseq": request["cseq"],
        "hop": request["hop"]
    }
    
    if others:
        params.update(others)
        
    return params


def make_ack(request, tag):
    return {
        'is_response': False,
        'method': "ACK",
        'uri': request["uri"],
        'from': request["from"],
        'to': request["to"].tagged(tag),
        'call_id': request["call_id"],
        'cseq': request["cseq"],
        'route': request.get("route"),
        'hop': request["hop"]
    }


def make_timeout_response(request):
    return make_simple_response(request, Status(408, "Request Timeout"))


def make_timeout_nak(response):
    return {
        "is_response": False,
        "method": "NAK",
        "from": response["from"],
        "to": response["to"],
        "call_id": response["call_id"],
        "cseq": response["cseq"],
        "hop": None
    }


def make_virtual_response():
    return dict(status=Status(0, "Virtual"))
    
    
def is_virtual_response(msg):
    return msg["status"].code == 0
