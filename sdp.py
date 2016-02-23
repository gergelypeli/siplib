from __future__ import print_function, unicode_literals

import collections
import datetime
import re
#from pprint import pprint

STATIC_PAYLOAD_TYPES = {
    0: ("PCMU", 8000, 1, None),
    3: ("GSM",  8000, 1, None),
    8: ("PCMA", 8000, 1, None),
    9: ("G722", 8000, 1, None)
}

class Error(Exception): pass

last_session_id = 0

def generate_session_id():
    global last_session_id
    last_session_id += 1
    return last_session_id


def rip_direction(attributes):
    for i, (k, v) in enumerate(attributes):
        if k == "sendrecv":
            attributes.pop(i)
            return True, True
        elif k == "sendonly":
            attributes.pop(i)
            return True, False
        elif k == "recvonly":
            attributes.pop(i)
            return False, True
        elif k == "inactive":
            attributes.pop(i)
            return False, False

    return None


def add_direction(attributes, send, recv):
    if send:
        if recv:
            dir_attr = "sendrecv"
        else:
            dir_attr = "sendonly"
    else:
        if recv:
            dir_attr = "recvonly"
        else:
            dir_attr = "inactive"
            
    attributes.append((dir_attr, None))


class Timing(collections.namedtuple("Timing", "start stop")):
    NTP_EPOCH = datetime.datetime(1900, 1, 1)

    def print(self):
        return "%d %d" % ((self.start - self.NTP_EPOCH).total_seconds(), (self.stop - self.NTP_EPOCH).total_seconds())


    @classmethod
    def parse(cls, s):
        start, stop = s.split()
        start = cls.NTP_EPOCH + datetime.timedelta(seconds=int(start))
        stop = cls.NTP_EPOCH + datetime.timedelta(seconds=int(stop))
        return cls(start, stop)


class Bandwidth(collections.namedtuple("Bandwidth", "type value")):
    def print(self):
        return "%s:%d" % (self.type, self.value)
        
        
    @classmethod
    def parse(cls, s):
        type, value = s.split(":")
        return cls(type, int(value))
    

class Connection(collections.namedtuple("Connection", "net_type addr_type host")):
    def print(self):
        return "%s %s %s" % (self.net_type, self.addr_type, self.host)
        
        
    @classmethod
    def parse(cls, s):
        net_type, addr_type, host = s.split()
        if net_type != "IN" or addr_type != "IP4" or not re.search("^[0-9.]+$", host):
            raise Error("Invalid SDP Connection: %r" % s)
            
        return cls(net_type, addr_type, host)


class Origin(collections.namedtuple("Origin", "username session_id session_version net_type addr_type host")):
    last_session_id = 0
    
    @classmethod
    def generate_session_id(cls):
        cls.last_session_id += 1
        return cls.last_session_id
        
        
    def print(self):
        return "%s %s %s %s %s %s" % self
        
        
    @classmethod
    def parse(cls, s):
        username, session_id, session_version, net_type, addr_type, host = s.split()
        if net_type != "IN" or addr_type != "IP4" or not re.search(r"^[\w.]+$", host):
            raise Error("Invalid SDP Origin: %r" % s)
            
        return cls(username, int(session_id), int(session_version), net_type, addr_type, host)


class RtpFormat(object):
    def __init__(self, pt, encoding=None, clock=None, encp=None, fmtp=None):
        if encoding is None:
            encoding, clock, encp, fmtp = STATIC_PAYLOAD_TYPES.get(pt, (None, None, None, None))
        
        self.payload_type = pt
        self.encoding = encoding
        self.clock = clock
        self.encp = encp
        self.fmtp = fmtp


    def __repr__(self):
        return "RtpFormat(payload_type=%r, encoding=%r, clock=%r, encp=%r, fmtp=%r)" % (
            self.payload_type, self.encoding, self.clock, self.encp, self.fmtp
        )
        
        
    def print_rtpmap(self):
        if self.encoding:
            s = "%s/%d" % (self.encoding, self.clock)
            if self.encp != 1:
                s += "/%d" % self.encp
            return s
        else:
            return None


    def print_fmtp(self):
        return self.fmtp
        
        
    def parse_rtpmap(self, x):
        enc = x.split("/")
        
        self.encoding = enc[0]
        self.clock = int(enc[1])
        if len(enc) > 2:
            self.encp = int(enc[2])
        else:
            self.encp = 1

            
    def parse_fmtp(self, x):
        self.fmtp = x


class Channel(object):
    def __init__(self, connection=None, port=None, type=None, proto=None, formats=None, attributes=None):
        self.connection = connection
        self.port = port
        self.type = type
        self.proto = proto
        self.formats = formats or []
        self.attributes = attributes or []
        
        
    def __repr__(self):
        return "Channel(type=%r, conn=%r, port=%r, proto=%r, formats=%r, attributes=%r)" % (
            self.type, self.conn, self.port, self.proto, self.formats, self.attributes
        )
        
        
    def print(self):
        payload_types = [ str(f.payload_type) for f in self.formats ]
        media = "%s %d %s %s" % (self.type, self.port, self.proto, " ".join(payload_types))
        result = [ ("m", media) ]

        if self.connection:
            result.append(("c", self.connection.print()))
        
        for f in self.formats:
            rtpmap = f.print_rtpmap()
            if rtpmap:
                result.append(("a", "rtpmap:%d %s" % (f.payload_type, rtpmap)))
                
            fmtp = f.print_fmtp()
            if fmtp:
                result.append(("a", "fmtp:%d %s" % (f.payload_type, fmtp)))

        for k, v in self.attributes:
            result.append(("a", ("%s:%s" % (k, v) if v is not None else k)))

        return result
    
    
    def parse(self, key, value):
        if key == "m":
            type, port, proto, formats = value.split(None, 3)
            if proto != "RTP/AVP":
                raise Error("Media with not RTP protocol: '%s'" % proto)
            
            self.type = type
            self.port = int(port)
            self.proto = proto
            self.formats = [ RtpFormat(int(pt)) for pt in formats.split() ]
        elif key == "c":
            self.connection = Connection.parse(value)
        elif key == "a":
            x = value.split(":", 1) if ":" in value else (value, None)

            if x[0] == "rtpmap":
                pt, rtpmap = x[1].split(None, 1)
                for f in self.formats:
                    if f.payload_type == int(pt):
                        f.parse_rtpmap(rtpmap)
                        break
                else:
                    raise Error("No payload type %s!" % pt)
            elif x[0] == "fmtp":
                pt, fmtp = x[1].split(None, 1)
                for f in self.formats:
                    if f.payload_type == int(pt):
                        f.parse_fmtp(fmtp)
                        break
                else:
                    raise Error("No payload type %s!" % pt)
            else:
                self.attributes.append(x)


class Sdp:
    def __init__(self, origin, connection, bandwidth, channels, attributes):
        # v ignored
        self.origin = origin
        self.connection = connection
        # s, i, u, e, p ignored
        self.bandwidth = bandwidth
        # t, r, z, k ignored
        self.channels = channels
        self.attributes = attributes


    def __repr__(self):
        return "Sdp(origin=%r, connection=%r, bandwidth=%r, attributes=%r, channels=%r)" % (
            self.origin, self.connection, self.bandwidth, self.attributes, self.channels
        )


    def print(self):
        lines = [
            "v=%s" % 0,
            "o=%s" % self.origin.print(),
            "s=%s" % " "
        ]

        if self.connection:
            lines.append("c=%s" % self.connection.print())
            
        if self.bandwidth:
            lines.append("b=%s" % self.bandwidth.print())
            
        lines.append("t=%s" % Timing(Timing.NTP_EPOCH, Timing.NTP_EPOCH).print())
            
        for k, v in self.attributes:
            lines.append("a=%s" % ("%s:%s" % (k, v) if v is not None else k))
            
        for c in self.channels:
            for k, v in c.print():
                lines.append("%s=%s" % (k, v))
                
        return "\n".join(lines) + "\n"
        

    @classmethod
    def parse(cls, s):
        origin, connection, bandwidth = None, None, None
        channels = []
        attributes = []  # one key may appear multiple times, also keep order just in case
        current_channel = None
    
        for line in s.splitlines():
            if not line:
                continue
                
            try:
                key, value = line.split("=", 1)
            except Exception:
                raise Error("Invalid SDP line: %r" % line)
        
            if key == "m":
                current_channel = Channel()
                channels.append(current_channel)
        
            if current_channel:
                current_channel.parse(key, value)
                continue
        
            if key == "o":
                origin = Origin.parse(value)
            elif key == "c":
                connection = Connection.parse(value)
            elif key == "b":
                bandwidth = Bandwidth.parse(value)
            elif key == "a":
                x = value.split(":", 1) if ":" in value else (value, None)
                attributes.append(x)
            else:
                pass
    
        return cls(origin, connection, bandwidth, channels, attributes)


class SdpBuilder:
    def __init__(self, host):
        self.host = host
        self.session_id = generate_session_id()
        self.last_session_version = 0
        
        
    def build(self, session):
        channels = []
        
        directions = set((c["send"], c["recv"]) for c in session["channels"])
        session_direction = directions.pop() if len(directions) == 1 else None
        
        hosts = set(c["rtp_local_addr"][0] for c in session["channels"])
        session_host = hosts.pop() if len(hosts) == 1 else None
        
        for i, c in enumerate(session["channels"]):
            addr = c["rtp_local_addr"]
            
            type = c["type"]
            proto = c["proto"]
            formats = []
            
            for f in c["formats"]:
                encoding = f.get("encoding")
                clock = f.get("clock")
                fmtp = f.get("fmtp")
                encp = f.get("encp")
                pt = f.get("rtp_local_payload_type")
                
                format = RtpFormat(pt, encoding, clock, encp, fmtp)
                formats.append(format)
                
            attributes = list(c["attributes"])
            if not session_direction:
                add_direction(attributes, c["send"], c["recv"])
                
            connection = Connection("IN", "IP4", addr[0]) if not session_host else None
            port = addr[1]
            
            channel = Channel(connection, port, type, proto, formats, attributes)
            channels.append(channel)
        
        self.last_session_version += 1
        
        origin = Origin(
            username="siplib",
            session_id=self.session_id,
            session_version=self.last_session_version,
            net_type="IN",
            addr_type="IP4",
            host=self.host
        )
        
        connection = Connection("IN", "IP4", session_host) if session_host else None
        bandwidth = session.pop("bandwidth")
        attributes = list(session["attributes"])
        
        if session_direction:
            add_direction(attributes, *session_direction)
        
        sdp = Sdp(
            origin=origin,
            connection=connection,
            bandwidth=bandwidth,
            channels=channels,
            attributes=attributes
        )
        
        return sdp


class SdpParser:
    def __init__(self):
        pass
        

    def parse(self, sdp, is_answer):
        channels = []
        session_attributes = list(sdp.attributes)
        session_dir = rip_direction(session_attributes) or (True, True)
        session_connection = sdp.connection
        
        for i, c in enumerate(sdp.channels):
            channel_attributes = list(c.attributes)
            channel_dir = rip_direction(channel_attributes) or session_dir

            channel_connection = c.connection or session_connection
            addr = (channel_connection.host, c.port)

            formats = []
            
            for f in c.formats:
                format = dict(
                    encoding=f.encoding,
                    clock=f.clock,
                    encp=f.encp,
                    fmtp=f.fmtp,
                    rtp_remote_payload_type=f.payload_type
                )
                formats.append(format)
                
            channel = dict(
                type=c.type,
                proto=c.proto,
                send=channel_dir[0],
                recv=channel_dir[1],
                formats=formats,
                attributes=channel_attributes,
                rtp_remote_addr=addr
            )
            channels.append(channel)
            
        session = dict(
            is_answer=is_answer,
            channels=channels,
            bandwidth=sdp.bandwidth,
            attributes=session_attributes
        )
        
        return session


# dict(
#   is_answer=False,
#   bandwidth=0,
#   channels=[
#     dict(
#       clock=8000,
#       ptime=20,
#       send=True,
#       recv=True,
#       formats=[
#         dict(encoding="G729", fmtp="annexb=no", encp=1)
#       ]
#     )
#   ]
#)
