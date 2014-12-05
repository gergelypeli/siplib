from __future__ import print_function, unicode_literals, absolute_import

import re
import collections


Addr = collections.namedtuple("Addr", ["host", "port"])
Status = collections.namedtuple("Status", ["code", "reason"])
Via = collections.namedtuple("Via", ["addr", "branch"])  # TODO: improve!


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


class FormatError(Exception):
    pass


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


class Contact(collections.namedtuple("Contact", "uri name params")):
    def __new__(cls, uri, name=None, params=None):
        return super(Contact, cls).__new__(cls, uri, name, params or {})


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
    if "status" in params:
        code, reason = params["status"]
        initial_line = "SIP/2.0 %d %s" % (code, reason)
    elif "uri" in params:
        initial_line = "%s %s SIP/2.0" % (params["method"], params["uri"].print())
    else:
        raise FormatError("Invalid structured message!")

    p = collections.OrderedDict()
    mandatory_fields = ["from", "to", "call_id", "cseq", "via"]  # order these nicely
    other_fields = [f for f in params if f not in mandatory_fields]

    for field in mandatory_fields + other_fields:
        if field in ("from", "to", "contact"):
            p[field] = params[field].print()
        elif field == "cseq":
            p[field] = "%d %s" % (params[field], params["method"])  # ACK? CANCEL?
        elif field == "via":
            p[field] = ["SIP/2.0/UDP %s:%d;branch=z9hG4bK%s" % (a + (b,)) for a, b in params[field]]
        elif field not in ("method", "uri", "status", "sdp"):
            p[field] = params[field]

    body = params.get("sdp", "")

    return print_message(initial_line, p, body)


def parse_structured_message(msg):
    initial_line, params, body = parse_message(msg)
    p = {}

    m = re.search("^SIP/2.0\\s+(\\d\\d\\d)\\s+(.+)$", initial_line)
    if m:
        p["status"] = Status(code=int(m.group(1)), reason=m.group(2))

    m = re.search("^(\\w+)\\s+(\\S+)\\s*SIP/2.0\\s*$", initial_line)
    if m:
        method, uri = m.groups()
        p["method"] = method
        p["uri"] = Uri.parse(uri)

    if not p:
        raise FormatError("Invalid message!")

    for field in params:
        if field in ("from", "to", "contact"):
            p[field] = Contact.parse(params[field])
        elif field == "cseq":
            p[field] = int(params[field].split()[0])
        elif field == "via":
            def do_one(s):
                m = re.search("SIP/2.0/UDP ([^:;]+)(:(\\d+))?;branch=z9hG4bK([^;]+)", s)
                if not m:
                    raise FormatError("Invalid Via!")
                host, port, branch = m.group(1), int(m.group(3)) if m.group(3) else None, m.group(4)
                return Via(Addr(host, port), branch)

            p[field] = [do_one(s) for s in params[field]]
        else:
            p[field] = params[field]

    p["sdp"] = body

    return p

