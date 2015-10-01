import socket
import struct
import logging
from format import Addr
from async import WeakMethod
import sys, traceback


def resolve(addr):  # TODO: make this an Addr method?
    return Addr(socket.gethostbyname(addr.host), addr.port)


def vacuum(d):
    return { k: v for k, v in d.items() if v is not None }
    

class Rtp(object):
    def __init__(self, metapoll, local_addr, remote_addr, receiving_callback):
        self.metapoll = metapoll
        self.remote_addr = remote_addr
        self.receiving_callback = receiving_callback
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(local_addr)
        self.metapoll.register_reader(self.socket, WeakMethod(self.recv))
        
        self.ssrc = 0
        self.seq = 0
        self.timestamp = 0
        
        
    def send(self, type, payload):
        version = 2
        padding = 0
        extension = 0
        csrc_count = 0
        marker = 0
        self.seq += 1
        self.timestamp += 8000
        
        data = (
            chr(version << 6 | padding << 5 | extension << 4 | csrc_count) +
            chr(marker << 7 | type) +
            struct.pack('!H', self.seq) +
            struct.pack('!I', self.timestamp) +
            struct.pack('!I', self.ssrc) +
            payload
        )
        
        self.socket.sendto(data, self.remote_addr)
        
        
    def recv(self):
        data, addr = self.socket.recvfrom(65535)
        if addr != self.remote_addr:
            return
            
        type = ord(data[1]) & 0x7f
        payload = data[12:]
        
        if self.receiving_callback:
            self.receiving_callback(type, payload)


def my_exchandler(type, value, tb):
    # Warning: watch for a bit more Python 3-specific code below
    traceback.print_exception(type, value, tb)

    while tb.tb_next:
        tb = tb.tb_next

    print("Locals:", file=sys.stderr)
    for k, v in tb.tb_frame.f_locals.items():
        if not (k.startswith('__') and k.endswith('__')) or True:
            try:
                print('  {} = {}'.format(k, v), file=sys.stderr)
            except Exception:
                print("  {} CAN'T BE PRINTED!".format(k), file=sys.stderr)


def setup_exchandler():
    sys.excepthook = my_exchandler


def build_oid(parent, key, value=None):
    kv = "%s=%s" % (key, value) if value is not None else key
    return "%s,%s" % (parent, kv) if parent is not None else kv


class Logger(logging.LoggerAdapter):
    def __init__(self, oid=None):
        logging.LoggerAdapter.__init__(self, logging.getLogger(), dict(oid=oid))
        
        
    def set_oid(self, oid):
        # Nasty hack!
        self.extra['oid'] = oid


class Loggable(object):
    def __init__(self):
        self.logger = None
        
        
    def set_oid(self, oid):  # TODO: accept oid, key, value=None?
        self.logger = logging.LoggerAdapter(logging.getLogger(), dict(oid=oid))
