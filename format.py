from __future__ import print_function, unicode_literals, absolute_import

import re
import collections


# addr = (host, port)
# user = (username, displayname)
# ...
# uri = (addr, username, uriparameters)
# headerfield = (uri, displayname, headerparameters)


class FormatError(Exception):
    pass


# TODO: We can't handle URI parameters yet, only header parameters

def print_sip_uri(addr, user):
    u, d = user
    h, p = addr

    return "sip:%s@%s:%d" % (u, h, p) if u else "sip:%s:%d" % (h, p)


def parse_sip_uri(uri, display_name=None):
    m = re.search("^sip:(([\\w.+-]+)@)?([\\w.-]+)(:(\\d+))?$", uri)
    if not m:
        raise FormatError("Invalid SIP URI: %r" % uri)

    u = m.group(2)
    h = m.group(3)
    p = int(m.group(5)) if m.group(5) else 5060

    addr = h, p
    user = u, display_name

    return addr, user


def print_user(addr, user, header_params):
    dn = user[1]

    # If the URI contains URI parameters, not enclosing it in angle brackets would
    # be interpreted as header parameters. So enclose them always just to be safe.
    uri = print_sip_uri(addr, user)
    name = '"%s"' if dn and " " in dn else dn
    first_part = ["%s <%s>" % (name, uri) if name else "<%s>" % uri]
    last_parts = ["%s=%s" % (k, v) for k, v in header_params.items() if v]
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

    header_params = collections.OrderedDict()
    for part in parts[1:]:
        if "=" in part:
            k, v = part.split("=")
            header_params[k] = v
        else:
            header_params[part] = True

    return addr, user, header_params


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
        addr, user = params["uri"]
        initial_line = "%s %s SIP/2.0" % (params["method"], print_sip_uri(addr, user))
    else:
        raise FormatError("Invalid structured message!")

    p = collections.OrderedDict()
    mandatory_fields = ["from", "to", "call_id", "cseq", "via"]  # order these nicely
    other_fields = [f for f in params if f not in mandatory_fields]

    for field in mandatory_fields + other_fields:
        if field in ("from", "to", "contact"):
            addr, user, header_params = params[field]
            p[field] = print_user(addr, user, header_params)
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
        code, reason = int(m.group(1)), m.group(2)
        p["status"] = code, reason

    m = re.search("^(\\w+)\\s+(\\S+)\\s*SIP/2.0\\s*$", initial_line)
    if m:
        method, uri = m.groups()
        p["method"] = method
        p["uri"] = parse_sip_uri(uri)

    if not p:
        raise FormatError("Invalid message!")

    for field in params:
        if field in ("from", "to", "contact"):
            addr, user, header_params = parse_user(params[field])
            p[field] = (addr, user, header_params)
        elif field == "cseq":
            p[field] = int(params[field].split()[0])
        elif field == "via":
            def do_one(s):
                m = re.search("SIP/2.0/UDP ([^:;]+)(:(\\d+))?;branch=z9hG4bK([^;]+)", s)
                if not m:
                    raise FormatError("Invalid Via!")
                host, port, branch = m.group(1), int(m.group(3)) if m.group(3) else 5060, m.group(4)
                return ((host, port), branch)

            p[field] = [do_one(s) for s in params[field]]
        else:
            p[field] = params[field]

    p["sdp"] = body

    return p

