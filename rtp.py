import struct
import datetime
import wave
import math

import g711
from async import WeakMethod
from util import Loggable


class Error(Exception): pass


def get_payload_type(packet):
    return packet[1] & 0x7f
    
    
def set_payload_type(packet, pt):
    packet[1] = packet[1] & 0x80 | pt & 0x7f


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
    BYTES_PER_SAMPLE = 2
    
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
        raise Error("WTF?")
        
    return samples


# TODO: add the marker flag!
def build_rtp(ssrc, seq, timestamp, payload_type, payload):
    version = 2
    padding = 0
    extension = 0
    csrc_count = 0
    marker = 0
    
    packet = bytearray(12 + len(payload))
    packet[0] = version << 6 | padding << 5 | extension << 4 | csrc_count
    packet[1] = marker << 7 | payload_type & 0x7f
    struct.pack_into('!H', packet, 2, seq)
    struct.pack_into('!I', packet, 4, timestamp)
    struct.pack_into('!I', packet, 8, ssrc)
    packet[12:] = payload
    
    return packet


def parse_rtp(packet):
    payload_type = packet[1] & 0x7f
    seq = struct.unpack_from("!H", packet, 2)[0]
    timestamp = struct.unpack_from("!I", packet, 4)[0]
    ssrc = struct.unpack_from("!I", packet, 8)[0]
    payload = packet[12:]

    return ssrc, seq, timestamp, payload_type, payload


def parse_telephone_event(payload):
    te = struct.unpack_from("!I", payload)[0]
    event = (te & 0xff000000) >> 24
    end = bool(te & 0x00800000)
    volume = (te & 0x003f0000) >> 16
    duration = (te & 0x0000ffff)
    
    return event, end, volume, duration


def build_telephone_event(event, end, volume, duration):
    te = (event & 0xff) << 24 | (int(end) & 0x1) << 23 | (volume & 0x3f) << 16 | (duration & 0xffff)
    struct.pack('!I', te)


def prid(sid):
    addr, label = sid
    host, port = addr
    
    return "%s:%d@%s" % (host, port, label)


class RtpBase(Loggable):
    PTIME = 20
    PLAY_INFO = {
        ("*",    8000): (1,),  # artificial codec for recording
        ("PCMU", 8000): (1,),
        ("PCMA", 8000): (1,)
    }
    BYTES_PER_SAMPLE = 2
    
    
    def __init__(self):
        self.ssrc = None
        self.base_seq = None
        self.base_timestamp = None
        
        self.format = None
        self.encoding = None
        self.clock = None
        self.bytes_per_sample = None
        self.samples_per_packet = None

        
    def set_format(self, format):
        if format not in self.PLAY_INFO:
            raise Error("Unknown format for playing: %s" % format)
            
        self.format = format
        self.encoding, self.clock = format
        self.bytes_per_sample, = self.PLAY_INFO[format]
        self.samples_per_packet = int(self.clock * self.PTIME / 1000)
    

class RtpPlayer(RtpBase):
    def __init__(self, metapoll, format, data, handler, volume=1, fade=0):
        RtpBase.__init__(self)

        self.ssrc = 0  # should be random
        self.base_seq = 0  # too
        self.base_timestamp = 0  # too
        
        self.metapoll = metapoll
        self.data = data  # mono 16 bit LSB LPCM
        self.handler = handler
        
        self.offset = 0
        self.next_seq = 0
        self.volume = 0
        
        self.set_format(format)
        self.set_volume(volume, fade)
        
        if data:
            ptime = datetime.timedelta(milliseconds=self.PTIME)
            self.handle = self.metapoll.register_timeout(ptime, WeakMethod(self.play), repeat=True)
        
        
    def __del__(self):
        if self.handle:
            self.metapoll.unregister_timeout(self.handle)


    def set_volume(self, volume, fade):
        self.fade_steps = int(fade * 1000 / self.PTIME) + 1
        self.fade_step = (volume - self.volume) / self.fade_steps
        

    def play(self):
        seq = self.next_seq
        self.next_seq += 1
        
        timestamp = seq * self.samples_per_packet
        
        new_offset = self.offset + self.samples_per_packet * self.BYTES_PER_SAMPLE
        samples = self.data[self.offset:new_offset]
        self.offset = new_offset
        
        if self.fade_steps:
            self.fade_steps -= 1
            self.volume += self.fade_step
        
        amplify_wav(samples, self.volume)
        payload = encode_samples(self.encoding, samples)
            
        packet = build_rtp(self.ssrc, self.base_seq + seq, self.base_timestamp + timestamp, 127, payload)
        self.handler(self.format, packet)
        
        if self.offset >= len(self.data):
            self.metapoll.unregister_timeout(self.handle)
            self.handle = None


class RtpRecorder(RtpBase):
    def __init__(self, format):
        RtpBase.__init__(self)
        
        self.chunks = []  # mono 16 bit LSB LPCM
        self.last_seq = None

        self.set_format(format)
        
        
    def record(self, format, packet):
        encoding, clock = format
        
        if clock != self.clock:
            self.logger.warning("Clock %s is not the expected %s!" % (clock, self.clock))
            return
            
        ssrc, seq, timestamp, payload_type, payload = parse_rtp(packet)
    
        if self.ssrc is None:
            self.ssrc = ssrc
            self.base_seq = seq
            self.base_timestamp = timestamp
            self.last_seq = seq
        elif seq > self.last_seq:
            self.last_seq = seq
        else:
            # TODO: maybe order a bit?
            return
        
        n = int((timestamp - self.base_timestamp) / self.samples_per_packet)
        #self.logger.info("Recording chunk %d" % n)

        samples = decode_samples(encoding, payload)
        
        while n >= len(self.chunks):
            self.chunks.append(None)
            
        self.chunks[n] = samples
        
        
    def get(self):
        silence = bytes(self.samples_per_packet * self.BYTES_PER_SAMPLE)
        
        samples = b"".join(chunk or silence for chunk in self.chunks)

        return samples


class DtmfBase:
    MIN_DURATION = datetime.timedelta(milliseconds=40)
    PTIME = datetime.timedelta(milliseconds=20)
    
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
        self.report = report
        self.last_timestamp = None
        self.last_duration = None
        
        
    def process(self, format, packet):
        encoding, clock = format
        
        if encoding != "telephone-event":
            return False
            
        ssrc, seq, timestamp, payload_type, payload = parse_rtp(packet)

        if self.last_timestamp is not None:
            if timestamp < self.last_timestamp + self.last_duration:
                return True

        event, end, volume, duration = parse_telephone_event(payload)
        
        if duration < int(clock * self.MIN_DURATION.total_seconds()):
            return True
            
        self.last_timestamp = timestamp
        self.last_duration = duration
        key = self.keys_by_event.get(event)
        
        if key:
            self.report(key)
            
        return True


class DtmfInjector(DtmfBase):
    def __init__(self):
        self.payload_type = None
        self.clock = None
        self.dtmf_duration = None
        
        self.ssrc = None
        self.last_seq = None
        self.last_timestamp = None
        self.last_duration = None


    def set_clock_and_payload_type(self, clock, payload_type):
        self.clock = clock
        self.payload_type = payload_type
        
        dtmf_length = self.PTIME * math.ceil(self.MIN_DURATION / self.PTIME)
        self.dtmf_duration = int(self.clock * dtmf_length)
        
        
    def inject(self, keys):
        # TODO: this is wrong if one frame is made of multiple packets, we'd
        # cut it in half with this! Must delay sending until the current frame ends!
        # Wait until the time stamp increases? No, we can't wait for external packets...
        
        packets = []
        
        for key in keys:
            event = self.events_by_key.get(key)
            if not event:
                continue

            volume = 10

            self.last_timestamp += self.last_duration
            self.last_duration = self.dtmf_duration
        
            payload = build_telephone_event(event, True, volume, self.last_duration)
        
            for i in range(3):
                self.last_seq += 1
                packet = build_rtp(self.ssrc, self.last_seq, self.last_timestamp, self.payload_type, payload)
                packets.append(packet)

        return packets
        
        
    def process(self, packet):
        ssrc, seq, timestamp, payload_type, payload = parse_rtp(packet)
        
        if self.ssrc is not None:
            if timestamp < self.last_timestamp + self.last_duration:
                return True
        
        self.ssrc = ssrc
        self.last_seq = seq
        self.last_timestamp = timestamp
        self.last_duration = int(self.PTIME.total_seconds() * self.clock)
            
        return False
