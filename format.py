from collections import namedtuple
import urllib.parse
import socket

from async_net import HttpLikeMessage


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


Status = namedtuple("Status", [ "code", "reason" ])

Status.INTERNAL_REQUEST_TIMEOUT = Status(8, "Internal Request Timeout")
Status.INTERNAL_RESPONSE_CEASED = Status(87, "Internal Response Ceased")

Status.TRYING = Status(100, "Trying")
Status.RINGING = Status(180, "Ringing")
Status.SESSION_PROGRESS = Status(183, "Session Progress")

Status.OK = Status(200, "OK")

Status.BAD_REQUEST = Status(400, "Bad Request")
Status.UNAUTHORIZED = Status(401, "Unauthorized")
Status.FORBIDDEN = Status(403, "Forbidden")
Status.NOT_FOUND = Status(404, "Not Found")
Status.REQUEST_TIMEOUT = Status(408, "Request Timeout")
Status.EXTENSION_REQUIRED = Status(421, "Extension Required")
Status.INTERVAL_TOO_BRIEF = Status(423, "Interval Too Brief")
Status.BAD_INFO_PACKAGE = Status(469, "Bad Info Package")
Status.TEMPORARILY_UNAVAILABLE = Status(480, "Temporarily Unavailable")
Status.CALL_DOES_NOT_EXIST = Status(481, "Call Does Not Exist")  # twins
Status.TRANSACTION_DOES_NOT_EXIST = Status(481, "Transaction Does Not Exist")  # twins
Status.DIALOG_DOES_NOT_EXIST = Status(481, "Dialog Does Not Exist")  # twins
Status.LOOP_DETECTED = Status(482, "Loop Detected")
Status.TOO_MANY_HOPS = Status(483, "Too Many Hops")
Status.BUSY_HERE = Status(486, "Busy Here")
Status.REQUEST_TERMINATED = Status(487, "Request Terminated")
Status.NOT_ACCEPTABLE_HERE = Status(488, "Not Acceptable Here")
Status.BAD_EVENT = Status(489, "Bad Event")
Status.REQUEST_PENDING = Status(491, "Request Pending")

Status.SERVER_INTERNAL_ERROR = Status(500, "Server Internal Error")
Status.NOT_IMPLEMENTED = Status(501, "Not Implemented")
Status.SERVICE_UNAVAILABLE = Status(503, "Service Unavailable")
Status.SERVER_TIMEOUT = Status(504, "Server Timeout")
Status.BUSY_EVERYWHERE = Status(600, "Busy Everywhere")
Status.DECLINE = Status(603, "Decline")
Status.DOES_NOT_EXIST_ANYWHERE = Status(604, "Does Not Exist Anywhere")
Status.NOT_ACCEPTABLE = Status(606, "Not Acceptable")


Cause = namedtuple("Cause", [ "code", "reason" ])

Cause.UNSPECIFIED = Cause(0, "Unspecified")
Cause.UNALLOCATED_NUMBER = Cause(1, "Unallocated number")
Cause.NO_ROUTE_TRANSIT_NET = Cause(2, "No route to specified transit network")
Cause.NO_ROUTE_DESTINATION = Cause(3, "No route to destination")
Cause.NORMAL_CLEARING = Cause(16, "Normal call clearing")
Cause.USER_BUSY = Cause(17, "User busy")
Cause.NO_USER_RESPONSE = Cause(18, "No user responding")
Cause.NO_ANSWER = Cause(19, "No answer from the user")
Cause.SUBSCRIBER_ABSENT = Cause(20, "Subscriber absent")
Cause.CALL_REJECTED = Cause(21, "Call rejected")
Cause.NUMBER_CHANGED = Cause(22, "Number changed")
Cause.REDIRECTION_TO_NEW_DESTINATION = Cause(23, "Redirection to new destination")
Cause.DESTINATION_OUT_OF_ORDER = Cause(27, "Destination out of order")
Cause.INVALID_NUMBER_FORMAT = Cause(28, "Address incomplete")
Cause.FACILITY_REJECTED = Cause(29, "Facility rejected")
Cause.NORMAL_UNSPECIFIED = Cause(31, "Normal")
Cause.NORMAL_CIRCUIT_CONGESTION = Cause(34, "No circuit/channel available")
Cause.NETWORK_OUT_OF_ORDER = Cause(38, "Network out of order")
Cause.NORMAL_TEMPORARY_FAILURE = Cause(41, "Temporary failure")
Cause.SWITCH_CONGESTION = Cause(42, "Switching equipment congestion")
Cause.RESOURCE_UNAVAILABLE = Cause(47, "Resource unavailable")
Cause.INCOMING_CALLS_BARRED_WITHIN_CUG = Cause(55, "Incoming calls barred within CUG")
Cause.BEARERCAPABILITY_NOTAUTH = Cause(57, "Bearer capability not authorized")
Cause.BEARERCAPABILITY_NOTAVAIL = Cause(58, "Bearer capability not presently available")
Cause.BEARERCAPABILITY_NOTIMPL = Cause(65, "Bearer capability not implemented")
Cause.FACILITY_NOT_IMPLEMENTED = Cause(69, "Requested facility not implemented")
Cause.WRONG_CALL_STATE = Cause(101, "Wrong call state")


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
    

class WwwAuthenticate(namedtuple("WwwAuthenticate",
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
        
    
class Authorization(namedtuple("Authorization",
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
        else:
            # TODO: A good question is if the URI was not enclosed in angle brackets, had no
            # parameters, but does headers, should they be parsed? According to the
            # RFC this is invalid, but the Snom may do so unless configured otherwise.
            # So we may parse headers if no params were present just to be nice.
            # Or reject it completely. But just ignoring them is bad.
            if parser.startswith("?"):
                raise Exception("URI with headers not enclosed in angle brackets!")
        
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


class Sip(dict):
    def __init__(self, is_response=None, method=None, uri=None, status=None, body=None, hop=None, related=None):
        dict.__init__(self)
        
        self.is_response = is_response
        self.method = method
        self.uri = uri
        self.status = status
        self.body = body
        self.hop = hop
        self.related = related
        
        # The related field is for branch buddies. Responses point to their request,
        # CANCEL-s to their INVITE, ACK-s to their INVITE response.
        # The latter is true for outgoing ACK-s, since they have to be associated to
        # the InviteServer, but not true for incoming ACK-s, as 2xx ACK-s belong to
        # a different transaction, so related will be None, while non-2xx ACK-s
        # will be swallowed by the transaction layer anyway.
        
        
    def __bool__(self):
        return True
        
        
    @classmethod
    def request(cls, method=None, uri=None, body=None, hop=None, related=None):
        if not method:
            raise Exception("No method for request!")
            
        return cls(is_response=False, method=method, uri=uri, body=body, hop=hop, related=related)
        
        
    @classmethod
    def response(cls, status=None, method=None, body=None, hop=None, related=None):
        if not status:
            raise Exception("No status for response!")
            
        return cls(is_response=True, status=status, method=method, body=body, hop=hop, related=related)
            

def print_structured_message(msg):
    if msg.is_response is True:
        code, reason = msg.status
        initial_line = "SIP/2.0 %d %s" % (code, escape(reason, allow=' '))
    elif msg.is_response is False:
        initial_line = "%s %s SIP/2.0" % (msg.method, msg.uri.print())
    else:
        raise FormatError("Invalid structured message!")

    headers = []
    mandatory_fields = [ "from", "to", "via", "call_id", "cseq" ]  # order these nicely
    last_fields = [ f for f in [ "content_type" ] if f in msg ]
    other_fields = [ f for f in msg if f not in mandatory_fields + last_fields ]

    for field in mandatory_fields + other_fields + last_fields:
        x = msg[field]
        
        if x is None:
            continue
        elif field in ("from", "to", "refer_to", "referred_by", "diversion"):
            y = x.print()
        elif field == "cseq":
            y = "%d %s" % (x, msg.method)
        elif field in ("www_authenticate", "authorization", "rack"):  # FIXME: separate
            y = x.print()
        elif field in ("rseq", "expires", "max_forwards"):
            y = "%d" % x
        elif field in ("contact", "route", "record_route"):
            for na in x:
                headers.append((field, na.print()))
            continue
        elif field in ("via",):
            for v in x:
                headers.append((field, v.print()))
            continue
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
        else:
            y = x
            
        headers.append((field, y))

    body = msg.body or b""

    return HttpLikeMessage(initial_line, headers, body)


def parse_comma_separated(Item, header):
    items = []
    parser = Parser(header)
    
    while not items or parser.can_grab_separator(","):
        items.append(Item.parse(parser))
        
    return items


def parse_structured_message(hlm):
    parser = Parser(hlm.initial_line)

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
        
        status = Status(code=code, reason=reason)
        msg = Sip.response(status=status, related=None)  # TODO: just to circumvent our own check
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
        
        msg = Sip.request(method=method, uri=uri, related=None)  # TODO: yepp...

    for field in [ "via", "route", "record_route", "contact", "call_info", "alert_info", "reason" ]:
        msg[field] = []

    for field, x in hlm.headers:
        # TODO: refactor a bit!
        if field in ("from", "to", "refer_to", "referred_by", "diversion"):
            y = Nameaddr.parse(Parser(x))
        elif field in ("contact", "route", "record_route"):
            msg[field].extend(parse_comma_separated(Nameaddr, x))
            continue
        elif field == "via":
            msg[field].extend(parse_comma_separated(Via, x))
            continue
        elif field in ("www_authenticate"):
            y = WwwAuthenticate.parse(Parser(x))
        elif field in ("authorization"):
            y = Authorization.parse(Parser(x))
        elif field == "cseq":  # TODO
            parser = Parser(x)
            number = parser.grab_number()
            parser.grab_whitespace()
            method = parser.grab_token().upper()
            
            y = number
            
            if msg.method:
                if msg.method != method:
                    raise FormatError("Mismatching method in CSeq field: %r vs %r" % (msg.method, method))
            else:
                msg.method = method  # Necessary for CANCEL responses
        elif field in ("rseq", "expires", "max_forwards"):
            y = int(x)  # TODO
        elif field == "rack":
            y = Rack.parse(Parser(x))
        elif field in ("supported", "require", "allow"):
            y = set( i.strip() for i in x.split(",") )  # TODO
        elif field in ("target_dialog", "replaces"):
            y = TargetDialog.parse(Parser(x))
        elif field in ("call_info", "alert_info"):
            y = parse_comma_separated(CallInfo, x)
        elif field in ("reason",):
            y = parse_comma_separated(Reason, x)
        else:
            y = x
            
        msg[field] = y

    msg.body = hlm.body

    return msg


def make_simple_response(request, status, others=None):
    tag = "goodbye" if status.code > 100 else None
    
    response = Sip.response(status=status, method=request.method, hop=request.hop, related=request)
    response["from"] = request["from"]
    response["to"] = request["to"].tagged(tag)
    response["call_id"] = request["call_id"]
    response["cseq"] = request["cseq"]
    
    if others:
        response.update(others)
    
    return response


def make_non_2xx_ack(response, method, uri, route=None):
    request = Sip.request(method=method, uri=uri, hop=response.hop, related=response)
    request["from"] = response["from"]
    request["to"] = response["to"]
    request["call_id"] = response["call_id"]
    request["cseq"] = response["cseq"]
    request["route"] = route

    return request


def make_cease_response(request):
    return Sip.response(status=Status.INTERNAL_RESPONSE_CEASED, related=request)
    
    
def is_cease_response(msg):
    return msg.status == Status.INTERNAL_RESPONSE_CEASED
