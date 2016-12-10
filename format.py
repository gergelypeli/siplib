import collections
import socket

from sdp import Sdp
from async_net import HttpLikeMessage

# TODO: use attributes instead for these
META_HEADER_FIELDS = [ "is_response", "method", "uri", "status", "sdp", "hop", "user_params", "authname" ]
#LIST_HEADER_FIELDS = [ "via", "route", "record_route", "contact" ]


class FormatError(Exception):
    pass


class SipError(Exception):
    def __init__(self, status):
        self.status = status
        
        
    def __str__(self):
        return "%d %s" % self.status


LWS = ' \t'
ALPHANUM = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
MARK = "-_.!~*'()"
UNRESERVED = ALPHANUM + MARK
RESERVED = ";/?:@&=+$,"
ESCAPED = "%"  # followed by two hex digits, but we ignore that here
URIC = RESERVED + UNRESERVED + ESCAPED
REASON = URIC + LWS
TOKEN = ALPHANUM + "-.!%*_+`'~"


class Parser:
    def __init__(self, text):
        # The text should end with line terminators, so we don't have to check for length
        # And should replace all tabs with spaces, which is legal.
        self.text = text + '\n'
        self.pos = 0


    def __repr__(self):
        return "Parser(...%r)" % self.text[self.pos:]
        
        
    def startswith(self, what):
        return self.text.startswith(what)


    def skip_lws(self, pos):
        while True:
            if self.text[pos] in LWS:
                pos += 1
            elif self.text[pos] == '\r' and self.text[pos + 1] == '\n' and self.text[pos + 2] in LWS:
                pos += 3
            else:
                return pos
            
        
    def grab_string(self, acceptable):
        start = self.pos
        pos = start
        
        while self.text[pos] in acceptable:
            pos += 1
            
        end = pos
        value = self.text[start:end]
        self.pos = pos
        
        return value
        
        
    def grab_newline(self):
        pos = self.pos
        pos = self.skip_lws(pos)  # Skips line folding, too
            
        if self.text[pos] != '\r' or self.text[pos + 1] != '\n':
            raise Exception("Expected newline!")
            
        pos += 2
        self.pos = pos
        
        
    def grab_whitespace(self):
        pos = self.skip_lws(self.pos)
        if pos == self.pos:
            raise Exception("Expected whitespace at %r" % self)
            
        self.pos = pos


    def grab_number(self):
        start = self.pos
        
        while self.text[self.pos].isdigit():
            self.pos += 1
            
        end = self.pos
        number = self.text[start:end]
        
        if not number:
            raise Exception("Expected number!")
            
        return int(number)


    def can_grab_separator(self, wanted, left_pad=False, right_pad=False):
        pos = self.pos
        
        if left_pad:
            pos = self.skip_lws(pos)
            
        separator = self.text[pos]
        if separator != wanted:
            return False
            
        pos += 1
        
        if right_pad:
            pos = self.skip_lws(pos)
            
        self.pos = pos
        
        return True


    def grab_separator(self, wanted, left_pad=False, right_pad=False):
        if not self.can_grab_separator(wanted, left_pad, right_pad):
            raise Exception("Expected separator %r at %r" % (wanted, self))
            

    def grab_token(self):
        start = self.pos
        pos = start
        
        while self.text[pos] in TOKEN:
            pos += 1
            
        end = pos
        token = self.text[start:end]
        self.pos = pos
        
        if not token:
            raise Exception("Expected token at %r" % self)
        
        return token
        

    def grab_quoted(self):
        pos = self.skip_lws(self.pos)
            
        if self.text[pos] != '"':
            raise Exception("Expected quoted-string!")
            
        pos += 1
        quoted = ""
        
        while self.text[pos] != '"':
            if self.text[pos] == '\\':
                pos += 1
                
                if self.text[pos] in "\n\r":
                    raise Exception("Illegal escaping at %r!" % self)
                
            quoted += self.text[pos]
            pos += 1
        
        pos += 1
        self.pos = self.skip_lws(pos)
        
        return quoted


    def grab_token_or_quoted(self):
        if self.text[self.pos] in '"' + LWS:
            return self.grab_quoted()
        else:
            return self.grab_token()
    
    
    def grab_word(self):  # To be used for callid only
        start = self.pos
        pos = start
        
        while not self.text[pos].isspace():
            pos += 1
            
        end = pos
        word = self.text[start:end]
        self.pos = pos
        
        return word
        

class Addr(collections.namedtuple("Addr", [ "host", "port" ])):
    def __str__(self):
        return self.print()
        
    
    def print(self):
        return "%s:%d" % self if self.port is not None else "%s" % self.host


    @classmethod
    def parse(cls, parser):
        host = parser.grab_token()
        port = None
        
        if parser.can_grab_separator(':'):
            port = parser.grab_number()
            
        return cls(host, port)
        

    def resolved(self):
        return Addr(socket.gethostbyname(self.host), self.port)


    def assert_resolved(self):
        try:
            socket.inet_aton(self.host)
        except Exception:
            raise Exception("Host address is not numeric!")
        

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
    def parse(cls, parser):
        rseq = parser.grab_number()
        parser.grab_whitespace()
        
        cseq = parser.grab_number()
        parser.grab_whitespace()
        
        method = parser.grab_token().upper()
        
        return cls(rseq, cseq, method)


class Via(collections.namedtuple("Via", [ "transport", "addr", "branch" ])):
    def print(self):
        return "SIP/2.0/%s %s;branch=%s" % (self.transport, self.addr, self.branch)


    @classmethod
    def parse(cls, parser):
        proto = parser.grab_token()
        parser.grab_separator("/")
        
        version = parser.grab_token()
        parser.grab_separator("/")
        
        transport = parser.grab_token()
        parser.grab_whitespace()
        
        if proto != "SIP" or version != "2.0":
            raise Exception("Expected vanilla Via!")
        
        addr = Addr.parse(parser)
        params = {}
        
        while parser.can_grab_separator(';'):
            key = parser.grab_token()
            value = None
            
            if parser.can_grab_separator('='):
                value = parser.grab_token_or_quoted()
                
            params[key] = value
            
        # FIXME: we can relax this now
        branch = params["branch"]

        return cls(transport, addr, branch)


# TODO: this shouldn't be here
class Hop(collections.namedtuple("Hop", [ "transport", "interface", "local_addr", "remote_addr" ])):
    def __new__(cls, transport, interface, local_addr, remote_addr):
        if local_addr:
            local_addr.assert_resolved()
            
        if remote_addr:
            remote_addr.assert_resolved()
        
        return super().__new__(cls, transport, interface, local_addr, remote_addr)
        
        
    def __str__(self):
        return "%s/%s/%s/%s" % (self.transport, self.interface, self.local_addr, self.remote_addr)


def parse_digest(parser):
    token = parser.grab_token()
    if token != 'Digest':
        raise Exception("Expected 'Digest'!")
    
    parser.grab_whitespace()
    
    params = {}
    
    while not params or parser.can_grab_separator(","):
        key = parser.grab_token()
        parser.grab_separator("=")
        value = parser.grab_token_or_quoted()
        
        params[key] = value

    return params
    

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
    def parse(cls, parser):
        params = parse_digest(parser)
    
        if "stale" in params:
            params["stale"] = True if params["stale"].lower() == "true" else False
            
        if "qop" in params:
            params["qop"] = params["qop"].split(",")
            
        return cls(**params)
        
    
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
    def parse(cls, parser):
        params = parse_digest(parser)

        if "nc" in params:
            params["nc"] = int(params["nc"], 16)
            
        return cls(**params)


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


def parse_semicolon_params(parser):
    params = {}
    
    while parser.can_grab_separator(";"):
        key = parser.grab_token()
        value = None
    
        if parser.can_grab_separator("="):
            value = parser.grab_token_or_quoted()
        
        params[key] = value


    return params
    

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
        parts = [ self.addr.print() ] + print_params(self.params)
        rest = ";".join(parts)
        rest = "%s@%s" % (self.user, rest) if self.user else rest
        uri = "%s:%s" % (self.scheme, rest)

        return uri


    @classmethod
    def parse(cls, parser, bare_scheme=None):
        if bare_scheme:
            # It is given if the Uri was found not enclosed between angle brackets.
            # Then the parser already grabbed the scheme and the colon by accident.
            # Also, in this case we have no URI parameters.
            scheme = bare_scheme
        else:
            scheme = parser.grab_token()
            parser.grab_separator(":")

        # OK, there's no sane way to parse this shit incrementally
        uric = parser.grab_string(URIC)
        
        if scheme not in ("sip", "sips"):
            return cls(None, None, scheme, uric)
    
        username = None
        password = None
        
        if "@" in uric:
            userinfo, hostport = uric.split("@")
            
            if ":" in userinfo:
                username, password = userinfo.split(":")
                raise Exception("Unexpected password!")  # FIXME
            else:
                username = userinfo
        else:
            hostport = uric
            
        if hostport.startswith("["):
            raise Exception("Can't parse IPV6 references yet!")
            
        if ":" in hostport:
            host, port = hostport.split(":")
            try:
                port = int(port)
            except Exception:
                raise Exception("Expected numeric port!")
        else:
            host, port = hostport, None
            
        addr = Addr(host, port)
        
        params = parse_semicolon_params(parser) if not bare_scheme else {}
            
        return cls(addr, username, scheme, params)


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
    def parse(cls, parser):
        displayname = None
        uri = None
        bare_scheme = None

        # Leading LWS is already grabbed by the header parsing, or the
        # comma parsing.
        if parser.startswith('"'):
            displayname = parser.grab_quoted()
        elif not parser.startswith("<"):
            # This is either a bare display name, or the scheme of a URI
            token = parser.grab_token()
            
            if parser.can_grab_separator(":"):
                bare_scheme = token
            else:
                displayname = token
                parser.grab_whitespace()  # This is not optional
        
        if bare_scheme:        
            uri = Uri.parse(parser, bare_scheme)
        else:
            parser.grab_separator("<", True, False)
            uri = Uri.parse(parser)
            parser.grab_separator(">", False, True)
        
        params = parse_semicolon_params(parser)
        
        return cls(uri=uri, name=displayname, params=params)
        
        
    def tagged(self, tag):
        if self.params.get("tag") or not tag:
            return self
        else:
            return Nameaddr(self.uri, self.name, dict(self.params, tag=tag))


class SipMessage(HttpLikeMessage):
    LIST_HEADER_FIELDS = [ "via", "route", "record_route", "contact" ]
    

def print_structured_message(params):
    if params["is_response"] is True:
        code, reason = params["status"]
        initial_line = "SIP/2.0 %d %s" % (code, reason)
    elif params["is_response"] is False:
        initial_line = "%s %s SIP/2.0" % (params["method"], params["uri"].print())
    else:
        raise FormatError("Invalid structured message!")

    headers = collections.OrderedDict()
    mandatory_fields = ["from", "to", "call_id", "cseq", "via"]  # order these nicely
    other_fields = [f for f in params if f not in mandatory_fields]

    for field in mandatory_fields + other_fields:
        if params[field] is None:
            pass
        elif field in ("from", "to", "www_authenticate", "authorization", "rack"):
            headers[field] = params[field].print()
        elif field == "cseq":
            headers[field] = "%d %s" % (params[field], params["method"])
        elif field == "rseq":
            headers[field] = "%d" % params[field]
        elif field in ("contact", "route"):
            headers[field] = [ f.print() for f in params[field] ]
        elif field in ("via",):
            headers[field] = [ f.print() for f in params[field] ]
        elif field in ("supported", "require"):
            headers[field] = ", ".join(sorted(params[field]))
        elif field not in META_HEADER_FIELDS:
            headers[field] = params[field]

    body = b""
    sdp = params.get("sdp")
    if sdp:
        body = sdp.print()
        headers["content_type"] = "application/sdp"
        headers["content_length"] = len(body)

    message = SipMessage()
    message.initial_line = initial_line
    message.headers = headers
    message.body = body
    
    return message


def parse_structured_message(message):
    p = {}
    parser = Parser(message.initial_line)

    token = parser.grab_token()
    
    if token == "SIP":
        # Response
        parser.grab_separator("/")
    
        version = parser.grab_token()
        parser.grab_whitespace()
        if version != "2.0":
            raise Exception("Expected SIP version 2.0!")
    
        code = parser.grab_number()
        parser.grab_whitespace()
        
        reason = parser.grab_string(REASON)
        
        p["is_response"] = True
        p["status"] = Status(code=code, reason=reason)
    else:
        # Request
        method = token.upper()
        parser.grab_whitespace()
        
        uri = Uri.parse(parser)
        parser.grab_whitespace()
        
        token = parser.grab_token()
        parser.grab_separator("/")
        
        version = parser.grab_token()
        if version != "2.0":
            raise Exception("Expected SIP version 2.0!")
        
        p["is_response"] = False
        p["method"] = method
        p["uri"] = uri

    for field, header in message.headers.items():
        # TODO: refactor a bit!
        if field in ("from", "to"):
            p[field] = Nameaddr.parse(Parser(header))
        elif field in ("contact", "route"):
            p[field] = []
            
            for h in header:
                parser = Parser(h)
                nameaddrs = []
                
                while not nameaddrs or parser.can_grab_separator(","):
                    nameaddrs.append(Nameaddr.parse(parser))
                    
                p[field].extend(nameaddrs)
        elif field in ("www_authenticate"):
            p[field] = WwwAuth.parse(Parser(header))
        elif field in ("authorization"):
            p[field] = Auth.parse(Parser(header))
        elif field == "cseq":  # TODO
            parser = Parser(header)
            number = parser.grab_number()
            parser.grab_whitespace()
            method = parser.grab_token().upper()
            
            p[field] = number
            if "method" in p:
                if p["method"] != method:
                    raise FormatError("Mismatching method in CSeq field: %r vs %r" % (p["method"], method))
            else:
                p["method"] = method  # Necessary for CANCEL responses
        elif field == "rseq":
            p[field] = int(header)  # TODO
        elif field == "rack":
            p[field] = Rack.parse(Parser(header))
        elif field == "via":
            p[field] = [ Via.parse(Parser(h)) for h in header ]
        elif field in ("supported", "require"):
            p[field] = set( x.strip() for x in header.split(",") )  # TODO
        elif field not in META_HEADER_FIELDS:
            p[field] = header
        else:
            print("Warning, header field ignored: '%s'!" % field)

    sdp = None
    if message.body:
        sdp = Sdp.parse(message.body)
        
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
