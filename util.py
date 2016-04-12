import socket
import struct
import logging
from async_base import WeakMethod
import sys, traceback

FULL_OIDS = True


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


# Maybe
OID_SWITCH = "switch"
OID_CALL = "call"
OID_LEG = "leg"
OID_ROUTING = "routing"
OID_SLOT = "slot"
OID_CHANNEL = "channel"
OID_MGC = "mgc"
OID_MGW = "mgw"
OID_DIALOG = "dialog"
OID_DIALOG_MANAGER = "diaman"
OID_MSGP = "msgp"
OID_GROUND = "ground"
OID_CONTEXT = "context"
OID_TRANSPORT = "transport"
OID_AUTHORITY = "authority"


def build_oid(parent, *args):
    args = list(args)
    oid = parent
    
    while args:
        key = args.pop(0)
        key = key[0 if FULL_OIDS else 1] if isinstance(key, tuple) else key
        
        value = args.pop(0) if args else None
        value = ".".join(str(x) for x in value) if isinstance(value, list) else value
        
        kv = "%s=%s" % (key, value) if value is not None else key
        oid = "%s,%s" % (oid, kv) if oid is not None else kv
        
    return oid


class Loggable(object):
    def __init__(self):
        self.logger = None
        self.oid = None
        
        
    def set_oid(self, oid):  # TODO: accept oid, key, value=None?
        self.logger = logging.LoggerAdapter(logging.getLogger(), dict(oid=oid))
        self.oid = oid


def setup_logging():
    class OidLogFilter(logging.Filter):
        def filter(self, record):
            if not hasattr(record, 'oid'):
                record.oid = record.name
            
            #if "/" in record.oid:
                #record.oid = ".".join(
                #    x[:1] if not x.isdigit() else x for x in record.oid.split(".")
                #)
            #    record.oid = "/".join(
            #        "%s=%s" % (k[:1], v) for k, v in (x.split("=") for x in record.oid.split("/"))
            #    )
            
            return True
    
    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'default': {
                #'format': '%(asctime)s.%(msecs)03d  %(name)s  %(levelname)s  %(message)s',
                #'format': '%(name)-10s | %(message)s',
                'format': '%(oid)s | %(message)s',
                'datefmt': '%F %T'
            }
        },
        'filters': {
            'oidfilter': {
                '()': OidLogFilter
            }
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'formatter': 'default',
                'filters': [ 'oidfilter' ]
            },
            'file': {
                'class': 'logging.FileHandler',
                'formatter': 'default',
                'filters': [ 'oidfilter' ],
                'filename': 'siplibtest.log',
                'mode': 'w'
            }
        },
        'loggers': {
            '': {
                'level': logging.DEBUG,
                'handlers': [ 'console', 'file' ]
            },
            'msgp': {
                'level': logging.INFO
            }
        }
    })
