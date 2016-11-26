import collections
import socket
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
        
        
    def grab_literal(self, value):
        if self.text[self.pos:self.pos+len(value)] != value:
            raise Exception("Expected literal %r: %r!" % (value, self.text[self.pos:]))
            
        self.pos += len(value)
        
        
    def grab_whitespace(self):
        if self.text[self.pos] not in ' \t':
            raise Exception("Expected whitespace: %r" % self.text[self.pos:])
            
        while self.text[self.pos] in ' \t':
            self.pos += 1


    def grab_until(self, delimiters):
        start = self.pos
        
        while self.text[self.pos] not in delimiters + '\n':
            self.pos += 1
            
        end = self.pos
        chunk = self.text[start:end]
        
        return chunk
            

    def grab_number(self):
        start = self.pos
        
        while self.text[self.pos].isdigit():
            self.pos += 1
            
        end = self.pos
        number = self.text[start:end]
        
        if not number:
            raise Exception("Expected number!")
            
        return int(number)


    def grab_separator(self, only=None):
        pos = self.pos
        
        while self.text[pos] in ' \t':
            pos += 1
            
        separator = self.text[pos]
        if only and separator not in only:
            return None
            
        self.pos = pos + 1
        
        if separator not in '()<>@,;:\"/[]?={}\n':  # Newline is our addition
            raise Exception("Expected separator!")
        
        return separator


    def lookahead(self, find, before):
        pos = self.pos
        
        while self.text[pos] not in before + '\n':
            if self.text[pos] in find:
                return True
            
            pos += 1
                
        return False
        

    def is_token_character(self, c):
        return c.isalnum() or c in "-.!%*_+`'~"

        
    def grab_token(self):
        start = self.pos
        
        while self.is_token_character(self.text[self.pos]):
            self.pos += 1
            
        end = self.pos
        token = self.text[start:end]
        
        if not token:
            raise Exception("Expected token: %r" % self.text[self.pos:])
        
        return token
        

    def grab_quoted(self):
        while self.text[self.pos] in ' \t':
            self.pos += 1
            
        if self.text[self.pos] != '"':
            raise Exception("Expected quoted-string!")
            
        self.pos += 1
        quoted = ""
        
        while self.text[self.pos] != '"':
            if self.text[self.pos] == '\\':
                self.pos += 1
                
            quoted += self.text[self.pos]
            self.pos += 1
        
        self.pos += 1
        
        return quoted


    def grab_token_or_quoted(self):
        if self.text[self.pos] in '" \t':
            return self.grab_quoted()
        else:
            return self.grab_token()
    
    
    def grab_word(self):  # To be used for callid only
        start = self.pos
        
        while not self.text[self.pos].isspace():
            self.pos += 1
            
        end = self.pos
        word = self.text[start:end]
        
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
        
        if parser.grab_separator(':'):
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


class Via(collections.namedtuple("Via", [ "addr", "branch" ])):
    def print(self):
        return "SIP/2.0/UDP %s:%d;branch=z9hG4bK%s" % (self.addr + (self.branch,))


    @classmethod
    def parse(cls, parser):
        proto = parser.grab_token()
        parser.grab_literal("/")
        
        version = parser.grab_token()
        parser.grab_literal("/")
        
        transport = parser.grab_token()
        parser.grab_whitespace()
        
        if proto != "SIP" or version != "2.0" or transport != "UDP":
            raise Exception("Expected vanilla Via!")
        
        addr = Addr.parse(parser)
        params = {}
        
        while parser.grab_separator(';'):
            key = parser.grab_token()
            value = None
            
            if parser.grab_separator('='):
                value = parser.grab_token_or_quoted()
                
            params[key] = value
            
        # FIXME: we can relax this now
        branch = params["branch"]
        
        if not branch.startswith("z9hG4bK"):
            raise Exception("Expected modern Via!")
            
        branch = branch[7:]
        
        return cls(addr, branch)


# TODO: this shouldn't be here
class Hop(collections.namedtuple("Hop", [ "interface", "local_addr", "remote_addr" ])):
    def __new__(cls, interface, local_addr, remote_addr):
        local_addr.assert_resolved()
        remote_addr.assert_resolved()
        
        return super().__new__(cls, interface, local_addr, remote_addr)
        
        
    def __str__(self):
        return "%s/%s/%s" % (self.interface, self.local_addr, self.remote_addr)


def parse_digest(parser):
    parser.grab_literal("Digest")
    parser.grab_whitespace()
    
    params = {}
    
    while not params or parser.grab_separator(","):
        key = parser.grab_token()
        parser.grab_literal("=")
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
    
    while parser.grab_separator(";"):
        key = parser.grab_token()
        value = None
    
        if parser.grab_separator("="):
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
    def parse(cls, parser, no_params=False):
        scheme = parser.grab_token()
        parser.grab_literal(":")
        
        if scheme not in ("sip", "sips"):
            nonsip = parser.grab_until(" >")
            return cls(None, None, scheme, nonsip)
    
        username = None
        password = None
    
        if parser.lookahead('@', ';>'):
            username = parser.grab_token()
            
            if parser.grab_separator(':'):
                password = parser.grab_token()
                
            parser.grab_literal('@')
            
        addr = Addr.parse(parser)
        params = parse_semicolon_params(parser) if not no_params else {}
            
        if password:
            raise Exception("Unexpected password!")
            
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
        angles = False

        if parser.startswith("<"):
            angles = True
        elif parser.startswith('"'):
            displayname = parser.grab_quoted()
            angles = True
        elif not parser.lookahead(":", "<"):
            displayname = parser.grab_token()
            angles = True
            
        if angles:
            if not parser.grab_separator("<"):
                raise Exception("Expected left angle bracket!")
                
            uri = Uri.parse(parser)
        
            if not parser.grab_separator(">"):
                raise Exception("Expected right angle bracket!")
        else:
            uri = Uri.parse(parser, no_params=True)
        
        params = parse_semicolon_params(parser)
        
        return cls(uri=uri, name=displayname, params=params)
        
        
    def tagged(self, tag):
        if self.params.get("tag") or not tag:
            return self
        else:
            return Nameaddr(self.uri, self.name, dict(self.params, tag=tag))


def print_message(initial_line, headers, body):
    header_lines = []

    for field, value in headers.items():
        field = field.replace("_", "-").title()

        if isinstance(value, list):
            # Some header types cannot be joined into a comma separated list,
            # such as the authorization ones, since they contain a comma themselves.
            # So output separate headers always.
            header_lines.extend(["%s: %s" % (field, v) for v in value])
        else:
            header_lines.append("%s: %s" % (field, value))

    lines = [ initial_line ] + header_lines + [ "", body ]
    return "\r\n".join(lines)


def parse_message(msg):
    lines = msg.split("\r\n")
    initial_line = lines.pop(0)
    body = ""
    headers = { field: [] for field in LIST_HEADER_FIELDS }  # TODO: auth related?
    last_field = None

    while lines:
        line = lines.pop(0)

        if not line:
            body = "\r\n".join(lines)
            break

        if line.startswith(" "):
            if not last_field:
                raise Exception("First header is a continuation!")
                
            if isinstance(headers[last_field], list):
                headers[last_field][-1] += line
            else:
                headers[last_field] += line
        else:
            field, colon, value = line.partition(":")
            field = field.strip().replace("-", "_").lower()
            value = value.strip()

            if field not in headers:
                headers[field] = value
            elif isinstance(headers[field], list):
                headers[field].append(value)
            else:
                raise FormatError("Duplicate header received: %s" % field)

    return initial_line, headers, body


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

    body = ""
    sdp = params.get("sdp")
    if sdp:
        body = sdp.print()
        headers["content_type"] = "application/sdp"
        headers["content_length"] = len(body)

    return print_message(initial_line, headers, body)


def parse_structured_message(msg):
    initial_line, headers, body = parse_message(msg)
    p = {}
    parser = Parser(initial_line)

    token = parser.grab_token()
    
    if token == "SIP":
        # Response
        parser.grab_literal("/")
    
        version = parser.grab_token()
        parser.grab_whitespace()
        if version != "2.0":
            raise Exception("Expected SIP version 2.0!")
    
        code = parser.grab_number()
        parser.grab_whitespace()
        
        reason = parser.grab_until("")
        
        p["is_response"] = True
        p["status"] = Status(code=code, reason=reason)
    else:
        # Request
        method = token.upper()
        parser.grab_whitespace()
        
        uri = Uri.parse(parser)
        parser.grab_whitespace()
        
        token = parser.grab_token()
        parser.grab_literal("/")
        
        version = parser.grab_token()
        if version != "2.0":
            raise Exception("Expected SIP version 2.0!")
        
        p["is_response"] = False
        p["method"] = method
        p["uri"] = uri

    for field, header in headers.items():
        # TODO: refactor a bit!
        if field in ("from", "to"):
            p[field] = Nameaddr.parse(Parser(header))
        elif field in ("contact", "route"):
            p[field] = []
            
            for h in header:
                parser = Parser(h)
                nameaddrs = []
                
                while not nameaddrs or parser.grab_separator(","):
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
