from __future__ import print_function, unicode_literals

import collections
import datetime
import re
from pprint import pprint

STATIC_PAYLOAD_TYPES = {
    0: ("PCMU", 8000),
    3: ("GSM",  8000),
    8: ("PCMA", 8000),
    9: ("G722", 8000)
}

class Error(Exception): pass

last_session_id = 0

def generate_session_id():
    global last_session_id
    last_session_id += 1
    return last_session_id
    

class Direction(collections.namedtuple("Direction", "send recv")):
    @staticmethod
    def is_valid(s):
        return s in ("inactive", "sendonly", "recvonly", "sendrecv")
        
        
    @classmethod
    def parse(cls, s):
        if s == "inactive":
            return cls(False, False)
        elif s == "sendonly":
            return cls(True, False)
        elif s == "recvonly":
            return cls(False, True)
        elif s == "sendrecv":
            return cls(True, True)
        else:
            raise Error("Invalid direction: %s!" % s)
            
            
    def print(self):
        if self.send:
            if self.recv:
                return "sendrecv"
            else:
                return "sendonly"
        else:
            if self.recv:
                return "recvonly"
            else:
                return "inactive"


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
    def __init__(self, pt):
        encoding, clock = STATIC_PAYLOAD_TYPES.get(pt, (None, None))
        
        self.payload_type = pt
        self.encoding = encoding
        self.clock = clock
        self.encp = None
        self.fmtp = None


    def __repr__(self):
        return "RtpFormat(payload_type=%r, encoding=%r, clock=%r, encp=%r, fmtp=%r)" % (
            self.payload_type, self.encoding, self.clock, self.encp, self.fmtp
        )
        
        
    def print_rtpmap(self):
        if self.encoding:
            s = "%s/%d" % (self.encoding, self.clock)
            if self.encp:
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

            
    def parse_fmtp(self, x):
        self.fmtp = x


class Channel(object):
    def __init__(self, session_host, session_direction):
        self.type = None
        self.addr = (session_host, None)  # default
        self.proto = None
        self.formats = []
        self.direction = session_direction  # default
        self.attributes = []
        
        
    def __repr__(self):
        return "Channel(type=%r, addr=%r, proto=%r, formats=%r, direction=%r, attributes=%r)" % (
            self.type, self.addr, self.proto, self.formats, self.direction, self.attributes
        )
        
        
    def print(self, session_host, session_direction):
        payload_types = [ str(f.payload_type) for f in self.formats ]
        media = "%s %d %s %s" % (self.type, self.addr[1], self.proto, " ".join(payload_types))
        result = [ ("m", media) ]

        if self.addr[0] != session_host:
            result.append(("c", Connection("IN", "IP4", self.addr[0]).print()))
        
        if self.direction != session_direction:
            result.append(("a", self.direction.print()))
        
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
                raise Error("Media with not RTP protocol: %r" % s)
            
            self.type = type
            self.addr = (self.addr[0], int(port))
            self.proto = proto
            self.formats = [ RtpFormat(int(pt)) for pt in formats.split() ]
        elif key == "c":
            self.addr = (Connection.parse(value).host, self.addr[1])
        elif key == "a":
            x = value.split(":", 1) if ":" in value else (value, None)

            if Direction.is_valid(x[0]):
                self.direction = Direction.parse(x[0])
            elif x[0] == "rtpmap":
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


class EmptySdp(object):
    def is_empty(self):
        return True
        
        
    def is_session(self):
        return False
        

class Sdp(EmptySdp):
    def __init__(self, origin, bandwidth, attributes, channels):
        # v ignored
        self.origin = origin
        # s, i, u, e, p ignored
        #self.connection = connection
        self.bandwidth = bandwidth
        # t, r, z, k ignored
        #self.direction = direction
        self.attributes = attributes
        self.channels = channels


    def __repr__(self):
        return "Sdp(origin=%r, bandwidth=%r, attributes=%r, channels=%r)" % (
            self.origin, self.bandwidth, self.attributes, self.channels
        )


    def is_empty(self):
        return False
        
        
    def is_session(self):
        return True


    #def copy(self):
    #    return Sdp(self.origin, self.bandwidth, self.attributes, self.channels)
        

    def print(self):
        directions = set(c.direction for c in self.channels)
        session_direction = directions.pop() if len(directions) == 1 else None

        hosts = set(c.addr[0] for c in self.channels)
        session_host = hosts.pop() if len(hosts) == 1 else None

        lines = [
            "v=%s" % 0,
            "o=%s" % self.origin.print(),
            "s=%s" % " "
        ]

        if session_host:
            lines.append("c=%s" % Connection("IN", "IP4", session_host).print())
            
        if self.bandwidth:
            lines.append("b=%s" % self.bandwidth.print())
            
        lines.append("t=%s" % Timing(Timing.NTP_EPOCH, Timing.NTP_EPOCH).print())
            
        if session_direction:
            lines.append("a=%s" % session_direction.print())

        for k, v in self.attributes:
            lines.append("a=%s" % ("%s:%s" % (k, v) if v is not None else k))
            
        for c in self.channels:
            for k, v in c.print(session_host, session_direction):
                lines.append("%s=%s" % (k, v))
                
        return "\n".join(lines) + "\n"
        

    @classmethod
    def parse(cls, s):
        origin, session_host, bandwidth, session_direction = None, None, None, None
        channels = []
        attributes = []  # one key may appear multiple times, also keep order just in case
        current_channel = None
    
        for line in s.splitlines():
            if not line:
                continue
                
            try:
                key, value = line.split("=", 1)
            except Exception as e:
                raise Error("Invalid SDP line: %r" % line)
        
            if key == "m":
                current_channel = Channel(session_host, session_direction)
                channels.append(current_channel)
        
            if current_channel:
                current_channel.parse(key, value)
                continue
        
            if key == "o":
                origin = Origin.parse(value)
            elif key == "c":
                session_host = Connection.parse(value).host
            elif key == "b":
                bandwidth = Bandwidth.parse(value)
            elif key == "a":
                x = value.split(":", 1) if ":" in value else (value, None)

                if Direction.is_valid(x[0]):
                    session_direction = Direction.parse(x[0])
                else:
                    attributes.append(x)
            else:
                pass
    
        return cls(origin, bandwidth, attributes, channels)
