import struct
import wave
import collections

import g711
from async import WeakMethod
from util import Loggable


BYTES_PER_SAMPLE = 2


class Base(Loggable):
    def __init__(self):
        pass
        
        
    def msecs(self, ticks, clock):
        return ticks * 1000 // clock
        
        
    def ticks(self, msecs, clock):
        return msecs * clock // 1000


    def packet_duration(self, packet):
        if packet.format.encoding in ("PCMA", "PCMU") and packet.format.encp == 1:
            return len(packet.payload)
        else:
            raise Exception("Can't compute duration of %s!" % packet.format.encoding)
        

class Error(Exception): pass

Format = collections.namedtuple("Format", [ "encoding", "clock", "encp", "fmtp" ])

Packet = collections.namedtuple("Packet", [ "format", "timestamp", "marker", "payload" ])


def read_wav(filename):
    f = wave.open(filename, "rb")
    x = f.readframes(f.getnframes())
    f.close()
    
    return bytearray(x)  # Make it mutable


def write_wav(filename, data1, data2 = None):
    f = wave.open(filename, "wb")
    f.setsampwidth(2)
    f.setframerate(8000)
    
    if data2 is None:
        f.setnchannels(1)
        f.writeframes(data1)
    else:
        n = min(len(data1), len(data2)) // 2
        data = bytearray(2 * 2 * n)
        
        for i in range(n):
            data[2 * 2 * i + 0 : 2 * 2 * i + 2] = data1[2 * i : 2 * i + 2]
            data[2 * 2 * i + 2 : 2 * 2 * i + 4] = data2[2 * i : 2 * i + 2]
        
        f.setnchannels(2)
        f.writeframes(data)
        
    f.close()


def amplify_wav(samples, volume):
    for i in range(int(len(samples) / BYTES_PER_SAMPLE)):
        offset = i * BYTES_PER_SAMPLE
        old = struct.unpack_from("<h", samples, offset)[0]
        struct.pack_into('<h', samples, offset, int(volume * old))


def encode_samples(encoding, samples):
    if encoding == "PCMA":
        payload = g711.encode_pcma(samples)
    elif encoding == "PCMU":
        payload = g711.encode_pcmu(samples)
    else:
        raise Error("WTF?")
        
    return payload


def decode_samples(encoding, payload):
    if encoding == "PCMA":
        samples = g711.decode_pcma(payload)
    elif encoding == "PCMU":
        samples = g711.decode_pcmu(payload)
    else:
        raise Error("Can't decode %s!" % (encoding,))
        
    return samples


def build_rtp(ssrc, seq, timestamp, marker, payload_type, payload):
    version = 2
    padding = 0
    extension = 0
    csrc_count = 0
    
    packet = bytearray(12 + len(payload))
    packet[0] = version << 6 | padding << 5 | extension << 4 | csrc_count
    packet[1] = int(marker) << 7 | payload_type & 0x7f
    struct.pack_into('!H', packet, 2, seq)
    struct.pack_into('!I', packet, 4, timestamp)
    struct.pack_into('!I', packet, 8, ssrc)
    packet[12:] = payload
    
    return packet


def parse_rtp(packet):
    payload_type = packet[1] & 0x7f
    marker = bool(packet[1] & 0x80)
    seq = struct.unpack_from("!H", packet, 2)[0]
    timestamp = struct.unpack_from("!I", packet, 4)[0]
    ssrc = struct.unpack_from("!I", packet, 8)[0]
    payload = packet[12:]

    return ssrc, seq, timestamp, marker, payload_type, payload


def build_telephone_event(event, end, volume, duration):
    te = (event & 0xff) << 24 | (int(end) & 0x1) << 23 | (volume & 0x3f) << 16 | (duration & 0xffff)
    return struct.pack('!I', te)

    
def parse_telephone_event(payload):
    te = struct.unpack_from("!I", payload)[0]
    event = (te & 0xff000000) >> 24
    end = bool(te & 0x00800000)
    volume = (te & 0x003f0000) >> 16
    duration = (te & 0x0000ffff)
    
    return event, end, volume, duration


class RtpProcessor(Base):
    pass
    

class RtpPlayer(RtpProcessor):
    def __init__(self, metapoll, format, data, handler, volume=1, fade=0, ptime_ms=20):
        RtpProcessor.__init__(self)

        self.timestamp = 0
        
        self.metapoll = metapoll
        self.data = data  # mono 16 bit LSB LPCM
        self.handler = handler
        self.ptime_ms = ptime_ms
        self.format = format
        
        self.volume = 0
        self.set_volume(volume, fade)
        
        if data:
            self.handle = self.metapoll.register_timeout(self.ptime_ms / 1000, WeakMethod(self.play), repeat=True)
        
        
    def __del__(self):
        if self.handle:
            self.metapoll.unregister_timeout(self.handle)


    def set_volume(self, volume, fade):
        self.fade_steps = fade * 1000 // self.ptime_ms + 1
        self.fade_step = (volume - self.volume) / self.fade_steps
        

    def play(self):
        timestamp = self.timestamp
        self.timestamp += self.ticks(self.ptime_ms, self.format.clock)
        
        old_offset = timestamp * BYTES_PER_SAMPLE
        new_offset = self.timestamp * BYTES_PER_SAMPLE
        samples = self.data[old_offset:new_offset]
        
        if self.fade_steps:
            self.fade_steps -= 1
            self.volume += self.fade_step
        
        # TODO: resample? Must know the input clock, too!
        amplify_wav(samples, self.volume)

        payload = encode_samples(self.format.encoding, samples)
            
        packet = Packet(self.format, timestamp, True, payload)
        #packet = build_rtp(self.ssrc, self.base_seq + seq, self.base_timestamp + timestamp, 127, payload)
        self.handler(packet)
        
        if new_offset >= len(self.data):
            self.metapoll.unregister_timeout(self.handle)
            self.handle = None


class RtpRecorder(RtpProcessor):
    def __init__(self, format, ptime_ms=20):
        RtpProcessor.__init__(self)
        
        # Even if internal timestamps are relative, we'll need the start
        # time of the recording, too.
        self.base_ms = None
        self.ptime_ms = ptime_ms
        
        if format.encoding != "L16":
            raise Error("Unknown encoding for recording: %s" % (format.encoding,))
        
        self.format = format
        
        self.chunks = []  # mono 16 bit LSB LPCM
        
        
    def record_packet(self, packet):
        if self.base_ms is None:
            self.base_ms = self.msecs(packet.timestamp, packet.format.clock)
            
        rec_time_ms = self.msecs(packet.timestamp, packet.format.clock) - self.base_ms
        if rec_time_ms < 0:
            return  # Oops, started recording later!
        
        rec_index = rec_time_ms // self.ptime_ms
        #self.logger.info("Recording chunk %d" % n)

        samples = decode_samples(packet.format.encoding, packet.payload)
        
        if packet.format.clock != self.format.clock:
            pass  # resample!
        
        while rec_index >= len(self.chunks):
            self.chunks.append(None)
            
        self.chunks[rec_index] = samples
        
        
    def get(self):
        silence = bytes(self.ticks(self.ptime_ms, self.format.clock) * BYTES_PER_SAMPLE)
        
        samples = b"".join(chunk or silence for chunk in self.chunks)

        return samples


class DtmfBase(Base):
    DTMF_DURATION_MS = 40
    
    keys_by_event = {
        0: "0", 1: "1", 2: "2", 3: "3", 4: "4", 5: "5", 6: "6", 7: "7", 8: "8", 9: "9",
        10: "*", 11: "#", 12: "A", 13: "B", 14: "C", 15: "D"
    }
    
    events_by_key = {
        "0": 0, "1": 1, "2": 2, "3": 3, "4": 4, "5": 5, "6": 6, "7": 7, "8": 8, "9": 9,
        "*": 10, "#": 11, "A": 12, "B": 13, "C": 14, "D": 15
    }
    

class DtmfExtractor(DtmfBase):
    def __init__(self, report):
        DtmfBase.__init__(self)
        
        self.report = report
        self.next_time_ms = None
        
        
    def process(self, packet):
        if packet.format.encoding != "telephone-event":
            return False
            
        this_time_ms = self.msecs(packet.timestamp, packet.format.clock)

        if self.next_time_ms is not None:
            if this_time_ms < self.next_time_ms:
                return True

        event, end, volume, duration = parse_telephone_event(packet.payload)
        this_duration_ms = self.msecs(duration, packet.format.clock)
        self.logger.debug("A DTMF with duration %s = %dms" % (duration, this_duration_ms))
        
        if this_duration_ms < self.DTMF_DURATION_MS:
            return True
            
        self.next_time_ms = this_time_ms + this_duration_ms

        key = self.keys_by_event.get(event)
        if key:
            self.report(key)
            
        return True


class DtmfInjector(DtmfBase):
    def __init__(self):
        DtmfBase.__init__(self)

        self.format = None
        self.next_time_ms = None


    def set_format(self, format):
        self.format = format
        
        
    def inject(self, name):
        # TODO: this is wrong if one frame is made of multiple packets, we'd
        # cut it in half with this! Must delay sending until the current frame ends!
        # Wait until the time stamp increases? No, we can't wait for external packets...
        
        event = self.events_by_key.get(name)
        if not event:
            return []

        volume = 10
        timestamp = self.ticks(self.next_time_ms, self.format.clock)
        duration = self.ticks(self.DTMF_DURATION_MS, self.format.clock)
    
        payload = build_telephone_event(event, True, volume, duration)
        packet = Packet(self.format, timestamp, True, payload)
        
        self.next_time_ms += self.DTMF_DURATION_MS
        
        return [ packet, packet, packet ]
        
        
    def process(self, packet):
        this_time_ms = self.msecs(packet.timestamp, packet.format.clock)
        
        if self.next_time_ms is not None:
            if this_time_ms < self.next_time_ms:
                return True
        
        duration = self.packet_duration(packet)
        duration_ms = self.msecs(duration, packet.format.clock)
        
        self.next_time_ms = this_time_ms + duration_ms
            
        return False


class RtpBuilder:
    def __init__(self):
        self.ssrc = 1  # TODO: generate, Snom doesn't play SSRC 0???
        self.last_seq = 0  # TODO: generate
        self.base_timestamp = 0  # TODO: generate
        self.payload_types_by_format = {}
        
        
    def set_payload_types_by_format(self, ptbf):
        self.payload_types_by_format = ptbf
        
        
    def build(self, packet):
        payload_type = self.payload_types_by_format.get(packet.format)
        if payload_type is None:
            return None
            
        self.last_seq += 1
        
        udp = build_rtp(self.ssrc, self.last_seq, packet.timestamp + self.base_timestamp, packet.marker, payload_type, packet.payload)
        
        return udp


class RtpParser:
    def __init__(self):
        self.last_seq = None
        self.base_timestamp = None
        self.formats_by_payload_type = {}
        
        
    def set_formats_by_payload_type(self, fbpt):
        self.formats_by_payload_type = fbpt
        
        
    def parse(self, udp):
        ssrc, seq, timestamp, marker, payload_type, payload = parse_rtp(udp)
        
        format = self.formats_by_payload_type.get(payload_type)
        if format is None:
            return None
            
        if self.last_seq is not None and seq < self.last_seq:
            return None
            
        self.last_seq = seq
            
        if self.base_timestamp is None:
            self.base_timestamp = timestamp
            
        packet = Packet(format, timestamp - self.base_timestamp, marker, payload)
        
        return packet
