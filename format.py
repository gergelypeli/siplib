from collections import namedtuple, OrderedDict
import urllib.parse
import socket

from async_net import HttpLikeMessage

# TODO: use attributes instead for these
META_HEADER_FIELDS = [ "is_response", "method", "uri", "status", "body", "hop", "user_params", "authname" ]


class FormatError(Exception):
    pass


class SipError(Exception):
    def __init__(self, status):
        self.status = status
        
        
    def __str__(self):
        return "%d %s" % self.status


# The _ESCAPED suffix just reminds us to unescape them after grabbing them from the parser.
LWS = ' \t'
ALPHANUM = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
MARK_URLLIB = '_.-'
MARK_NON_URLLIB = "!~*'()"
MARK = MARK_URLLIB + MARK_NON_URLLIB
UNRESERVED = ALPHANUM + MARK
RESERVED = ";/?:@&=+$,"
ESCAPED = "%"  # followed by two hex digits, but we ignore that here
URIC_ESCAPED = RESERVED + UNRESERVED + ESCAPED
REASON_ESCAPED = URIC_ESCAPED + LWS
TOKEN = ALPHANUM + "-.!%*_+`'~"
HOST_NONTOKEN = "[]:"
WORD = ALPHANUM + "-.!%*_+`'~()<>:\/[]?{}" + '"'  # notably no semicolon, used for call-id
CALL_ID = WORD + "@"  # although at most one @ is allowed, fuck that

URI_HEADER_UNRESERVED = "[]/?:+$"  # hnv
URI_HEADER_ESCAPED = URI_HEADER_UNRESERVED + UNRESERVED + ESCAPED

URI_PARAM_UNRESERVED = "[]/:&+$"
URI_PARAM_ESCAPED = URI_PARAM_UNRESERVED + UNRESERVED + ESCAPED

GENERIC_PARAM_KEY = TOKEN
GENERIC_PARAM_VALUE = TOKEN + HOST_NONTOKEN  # plus quoted


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


    def grab_until(self, separator):
        start = self.pos
        end = self.text.find(separator, start)
        
        if end >= 0:
            self.pos = end + len(separator)
            return self.text[start:end]
        else:
            return None


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
            
        
    def grab_token(self, acceptable=TOKEN):
        start = self.pos
        pos = start
        
        while self.text[pos] in acceptable:
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


    def grab_token_or_quoted(self, acceptable=TOKEN):
        if self.text[self.pos] in '"' + LWS:
            return self.grab_quoted()
        else:
            return self.grab_token(acceptable)
    
    
    def grab_word(self):  # To be used for callid only
        start = self.pos
        pos = start
        
        while not self.text[pos].isspace():
            pos += 1
            
        end = pos
        word = self.text[start:end]
        self.pos = pos
        
        return word


def unescape(escaped):
    return urllib.parse.unquote(escaped)
    
    
def escape(raw, allow=''):
    # The characters not needing escaping vary from entity to entity. But characters in
    # the UNRESERVED class seems to be allowed always. That's ALPHANUM + MARK. This
    # function never quotes alphanumeric characters and '_.-', so we only need to
    # specify the rest explicitly.
    
    return urllib.parse.quote(raw, safe=MARK_NON_URLLIB + allow)
    

# unquoting happens during the parsing, otherwise quoted strings can't even be parsed
def quote(raw):
    return '"' + raw.replace('\\', '\\\\').replace('"', '\\"') + '"'


def quote_if_not(acceptable, raw):
    return raw if all(c in acceptable for c in raw) else quote(raw)
    

def parse_generic_params(parser):
    params = {}
    
    while parser.can_grab_separator(';'):
        key = parser.grab_token(GENERIC_PARAM_KEY)
        value = None
        
        if parser.can_grab_separator('='):
            value = parser.grab_token_or_quoted(GENERIC_PARAM_VALUE)
            
        params[key] = value
        
    return params
    
    
def print_generic_params(params):
    text = ""
    
    for k, v in params.items():
        text += ";" + k
        
        if v is not None:
            text += "=" + quote_if_not(GENERIC_PARAM_VALUE, v)
            
    return text


class Addr(namedtuple("Addr", [ "host", "port" ])):
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


    def contains(self, other):
        if not other:
            return False
            
        if self.host and self.host != other.host:
            return False
            
        if self.port and self.port != other.port:
            return False
            
        return True


Addr.__new__.__defaults__ = (None,)


class Status(namedtuple("Status", [ "code", "reason" ])):
    INTERNAL_TIMEOUT = 8
    INTERNAL_CEASE = 87
    
    REASONS_BY_CODE = {
          8: "Internal Request Timeout",
         87: "Internal Response Ceased",
        100: "Trying",
        180: "Ringing",
        183: "Session Progress",
        200: "OK",
        400: "Bad Request",
        401: "Unauthorized",
        403: "Forbidden",
        404: "Not Found",
        408: "Request Timeout",
        421: "Extension Required",
        423: "Interval Too Brief",
        480: "Temporarily Unavailable",
        481: "Call/Transaction Does Not Exist",
        482: "Loop Detected",
        483: "Too Many Hops",
        486: "Busy Here",
        487: "Request Terminated",
        488: "Not Acceptable Here",
        489: "Bad Event",
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


class Rack(namedtuple("Rack", [ "rseq", "cseq", "method" ])):
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


class Via(namedtuple("Via", [ "transport", "addr", "params" ])):
    BRANCH_MAGIC = "z9hG4bK"


    def print(self):
        return "SIP/2.0/%s %s%s" % (self.transport, self.addr, print_generic_params(self.params))
        

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
        params = parse_generic_params(parser)

        return cls(transport, addr, params)


# TODO: this shouldn't be here
class Hop(namedtuple("Hop", [ "transport", "interface", "local_addr", "remote_addr" ])):
    def __new__(cls, transport, interface, local_addr, remote_addr):
        if local_addr:
            local_addr.assert_resolved()
            
        if remote_addr:
            remote_addr.assert_resolved()
        
        return super().__new__(cls, transport, interface, local_addr, remote_addr)
        
        
    def __str__(self):
        return "%s/%s/%s/%s" % (self.transport, self.interface, self.local_addr, self.remote_addr)


    def contains(self, other):
        if not other:
            return False

        if self.transport and self.transport != other.transport:
            return False
            
        if self.interface and self.interface != other.interface:
            return False
            
        if self.local_addr and not self.local_addr.contains(other.local_addr):
            return False
                
        if self.remote_addr and not self.remote_addr.contains(other.remote_addr):
            return False
            
        return True
        

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
    

class WwwAuth(namedtuple("WwwAuth",
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
            'qop="%s"' % ", ".join(self.qop) if self.qop else None
        ]
        
        return 'Digest %s' % ",".join(c for c in components if c)

        
    @classmethod
    def parse(cls, parser):
        params = parse_digest(parser)
    
        if "stale" in params:
            params["stale"] = True if params["stale"].lower() == "true" else False
            
        if "qop" in params:
            params["qop"] = [ x.strip() for x in params["qop"].split(",") ]
            
        return cls(**params)
        
    
class Auth(namedtuple("Auth",
    [ "realm", "nonce", "username", "uri", "response", "opaque", "algorithm", "qop", "cnonce", "nc" ]
)):
    def __new__(cls, realm, nonce, username, uri, response, opaque=None, algorithm=None, qop=None, cnonce=None, nc=None):
        self = super().__new__(cls, realm, nonce, username, uri, response, opaque, algorithm, qop, cnonce, nc)
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
        # Well, some of these field must be quoted-string, but we don't care,
        # and just accept tokens unquoted.
        params = parse_digest(parser)

        if "nc" in params:
            params["nc"] = int(params["nc"], 16)
            
        return cls(**params)


class AbsoluteUri(namedtuple("AbsoluteUri", [ "scheme", "rest" ])):
    def __str__(self):
        return self.print()
        
        
    def print(self):
        return self.scheme + ":" + self.rest
        
        
    @classmethod
    def parse(cls, parser, scheme=None):
        if not scheme:
            scheme = parser.grab_token()
            parser.grab_separator(":")

        # This may contain escaped parts, but we don't know how to unescape it properly,
        # because the separators are unknown, and only the separated parts are supposed
        # to be unescaped. So just return it as we found it.
        rest = parser.grab_token(URIC_ESCAPED)
        
        return cls(scheme, rest)
        

class Uri(namedtuple("Uri", "addr username scheme params headers")):
    def __new__(cls, addr, username=None, scheme=None, params=None, headers=None):
        if scheme == "any":
            raise Exception("Don't use scheme 'any'!")
            
        return super().__new__(cls, addr, username, scheme or "sip", params or {}, headers or {})


    def __hash__(self):
        # It's unlikely that two URIs differ only in parameters, but even in
        # that case the equality will sort that out, hashing is only a speedup.
        return hash((self.addr, self.username, self.scheme))
        
        
    def __str__(self):
        return self.print()
        

    def print(self):
        text = (self.scheme or "any") + ":"
        
        if self.username:
            text += escape(self.username) + "@"
            
        text += str(self.addr)
        
        if self.params:
            text += "".join(";" + escape(k) + "=" + escape(v) for k, v in self.params.items())
                
        if self.headers:
            text += "?" + "&".join(escape(k.replace("_", "-").title()) + "=" + escape(v) for k, v in self.headers.items())
            
        return text


    @classmethod
    def parse(cls, parser, bare_scheme=None):
        if bare_scheme:
            # It is given if the Uri was found not enclosed between angle brackets.
            # Then the parser already grabbed the scheme and the colon by accident.
            # Also, in this case we have no URI parameters or headers, because unenclosed
            # URIs can't contain semicolon or question mark.
            scheme = bare_scheme
        else:
            scheme = parser.grab_token()
            parser.grab_separator(":")

        if scheme not in ("sip", "sips"):
            return AbsoluteUri.parse(parser, bare_scheme)
    
        username = None
        password = None
        
        userinfo = parser.grab_until("@")
        
        if userinfo:
            if ":" in userinfo:
                username, password = userinfo.split(":")
                username = unescape(username)
                #password = unescape(username)
                raise Exception("Unexpected password!")  # FIXME
            else:
                username = unescape(userinfo)
            
        if parser.startswith("["):
            raise Exception("Can't parse IPV6 references yet!")
            
        addr = Addr.parse(parser)
        params = {}
        headers = {}
        
        if not bare_scheme:
            while parser.can_grab_separator(";"):
                key = unescape(parser.grab_token(URI_PARAM_ESCAPED))
                value = None
    
                if parser.can_grab_separator("="):
                    value = unescape(parser.grab_token(URI_PARAM_ESCAPED))
        
                params[key] = value

            if parser.can_grab_separator("?"):
                while True:
                    key = unescape(parser.grab_token(URI_HEADER_ESCAPED))
                    parser.grab_separator("=")
                    value = unescape(parser.grab_token(URI_HEADER_ESCAPED))
                    
                    headers[key.replace("-", "_").lower()] = value
                    
                    if not parser.can_grab_separator("&"):
                        break
        
        return cls(addr, username, scheme, params, headers)


    def contains(self, other):
        if not other:
            return False

        if self.scheme and self.scheme != other.scheme:
            return False
            
        if self.username and self.username != other.username:
            return False
            
        if self.addr and not self.addr.contains(other.addr):
            return False
            
        return True
        

    def canonical_aor(self):
        return self._replace(scheme=None, params={})


    def resolved(self):
        return self._replace(addr=self.addr.resolved())
        
        
    def assert_resolved(self):
        self.addr.assert_resolved()
        

class Nameaddr(namedtuple("Nameaddr", "uri name params")):
    def __new__(cls, uri, name=None, params=None):
        return super().__new__(cls, uri, name, params or {})


    def __str__(self):
        return self.print()


    def print(self):
        # If the URI contains URI parameters, not enclosing it in angle brackets would
        # be interpreted as header parameters. So enclose them always just to be safe.

        text = "" if self.name is None else quote_if_not(TOKEN, self.name) + " "
        text += "<" + str(self.uri) + ">"
        text += print_generic_params(self.params)

        return text
    

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

        params = parse_generic_params(parser)

        return cls(uri=uri, name=displayname, params=params)
        
        
    def tagged(self, tag):
        if self.params.get("tag") or not tag:
            return self
        else:
            return Nameaddr(self.uri, self.name, dict(self.params, tag=tag))


class TargetDialog(namedtuple("TargetDialog", [ "call_id", "params" ])):
    def print(self):
        return self.call_id + print_generic_params(self.params)
        

    @classmethod
    def parse(cls, parser):
        call_id = parser.grab_token(CALL_ID)
        params = parse_generic_params(parser)
        
        return cls(call_id, params)


class CallInfo(namedtuple("CallInfo", [ "uri", "params" ])):
    def print(self):
        return "<" + self.uri.print() + ">" + print_generic_params(self.params)
        
        
    @classmethod
    def parse(cls, parser):
        parser.grab_separator("<")
        uri = AbsoluteUri.parse(parser)
        parser.grab_separator(">")
        params = parse_generic_params(parser)
        
        return cls(uri, params)


class Reason(namedtuple("Reason", [ "protocol", "params" ])):
    def print(self):
        return self.protocol + print_generic_params(self.params)
        

    @classmethod
    def parse(cls, parser):
        protocol = parser.grab_token()
        params = parse_generic_params(parser)
        
        return cls(protocol, params)


class SipMessage(HttpLikeMessage):
    LIST_HEADER_FIELDS = [ "via", "route", "record_route", "contact" ]
    

def print_structured_message(params):
    if params["is_response"] is True:
        code, reason = params["status"]
        initial_line = "SIP/2.0 %d %s" % (code, escape(reason, allow=' '))
    elif params["is_response"] is False:
        initial_line = "%s %s SIP/2.0" % (params["method"], params["uri"].print())
    else:
        raise FormatError("Invalid structured message!")

    headers = OrderedDict()
    mandatory_fields = [ "from", "to", "via", "call_id", "cseq" ]  # order these nicely
    last_fields = [ f for f in [ "content_type" ] if f in params ]
    other_fields = [ f for f in params if f not in mandatory_fields + last_fields ]

    for field in mandatory_fields + other_fields + last_fields:
        x = params[field]
        
        if x is None:
            continue
        elif field in ("from", "to", "refer_to", "referred_by"):
            y = x.print()
        elif field == "cseq":
            y = "%d %s" % (x, params["method"])
        elif field in ("www_authenticate", "authorization", "rack"):  # FIXME: separate
            y = x.print()
        elif field in ("rseq", "expires", "max_forwards"):
            y = "%d" % x
        elif field in ("contact", "route"):
            y = [ f.print() for f in x ]
        elif field in ("via",):
            y = [ f.print() for f in x ]
        elif field in ("supported", "require", "allow"):
            y = ", ".join(sorted(x))  # TODO: sorted here?
        elif field in ("refer_to", "referred_by"):
            y = x.print()
        elif field in ("target_dialog", "replaces"):
            y = x.print()
        elif field in ("call_info", "alert_info"):
            y = ", ".join(f.print() for f in x)
        elif field in ("reason",):
            y = ", ".join(f.print() for f in x)
        elif field not in META_HEADER_FIELDS:
            y = x
        else:
            continue
            
        headers[field] = y

    body = params.get("body", b"")

    return SipMessage(initial_line, headers, body)


def parse_comma_separated(Item, header):
    items = []
    parser = Parser(header)
    
    while not items or parser.can_grab_separator(","):
        items.append(Item.parse(parser))
        
    return items


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
        
        reason = unescape(parser.grab_token(REASON_ESCAPED))
        
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

    for field, x in message.headers.items():
        # TODO: refactor a bit!
        if field in ("from", "to", "refer_to", "referred_by"):
            y = Nameaddr.parse(Parser(x))
        elif field in ("contact", "route"):
            y = []
            
            for h in x:
                y.extend(parse_comma_separated(Nameaddr, h))
        elif field in ("www_authenticate"):
            y = WwwAuth.parse(Parser(x))
        elif field in ("authorization"):
            y = Auth.parse(Parser(x))
        elif field == "cseq":  # TODO
            parser = Parser(x)
            number = parser.grab_number()
            parser.grab_whitespace()
            method = parser.grab_token().upper()
            
            y = number
            
            if "method" in p:
                if p["method"] != method:
                    raise FormatError("Mismatching method in CSeq field: %r vs %r" % (p["method"], method))
            else:
                p["method"] = method  # Necessary for CANCEL responses
        elif field in ("rseq", "expires", "max_forwards"):
            y = int(x)  # TODO
        elif field == "rack":
            y = Rack.parse(Parser(x))
        elif field == "via":
            y = [ Via.parse(Parser(h)) for h in x ]
        elif field in ("supported", "require", "allow"):
            y = set( i.strip() for i in x.split(",") )  # TODO
        elif field in ("target_dialog", "replaces"):
            y = TargetDialog.parse(Parser(x))
        elif field in ("call_info", "alert_info"):
            y = parse_comma_separated(CallInfo, x)
        elif field in ("reason",):
            y = parse_comma_separated(Reason, x)
        elif field not in META_HEADER_FIELDS:
            y = x
        else:
            continue
            
        p[field] = y

    p["body"] = message.body

    return p


def make_simple_response(request, status, others=None):
    tag = "goodbye" if status.code > 100 else None
    
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


def make_non_2xx_ack(request, tag):
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
    return make_simple_response(request, Status(Status.INTERNAL_TIMEOUT))


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


def make_cease_response():
    return dict(status=Status(Status.INTERNAL_CEASE))
    
    
def is_cease_response(msg):
    return msg["status"].code == Status.INTERNAL_CEASE
