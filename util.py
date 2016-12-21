#import socket
#import struct
import logging
import sys, traceback

FULL_OIDS = True


def vacuum(d):
    return { k: v for k, v in d.items() if v is not None }
    

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


class Oid(str):
    def add(self, key, value=None):
        if isinstance(key, tuple):
            key = key[0 if FULL_OIDS else 1]
            
        kv = "%s=%s" % (key, value) if value is not None else key
        oid = "%s,%s" % (self, kv) if self else kv
        
        return Oid(oid)


class Loggable(object):
    # NOTE: both methods are idempotent, so if that ugliness happens that
    # they're called multiple times due to multiple inheritance, it won't
    # be a problem.
    
    def __init__(self):
        self.oid = None
        self.logger_dict = dict(oid="rogue,%s" % self.__class__)
        self.logger = logging.LoggerAdapter(logging.getLogger(), self.logger_dict)
        
        
    def set_oid(self, oid):
        self.oid = oid
        self.logger_dict["oid"] = oid


def setup_logging():
    MARKS_BY_LEVEL = {
        logging.DEBUG:    ' ',
        logging.INFO:     ':',
        logging.WARNING:  '?',
        logging.ERROR:    '!',
        logging.CRITICAL: '@'
    }
    
    def default_filter(record):
        if not hasattr(record, 'oid'):
            record.oid = record.name
        
        record.mark = MARKS_BY_LEVEL.get(record.levelno, '#')
        
        return True

    def console_filter(record):
        if record.levelno < logging.INFO:
            return False
            
        return True
    
    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'console': {
                #'format': '%(asctime)s.%(msecs)03d  %(name)s  %(levelname)s  %(message)s',
                'format': '%(mark)s | %(oid)s | %(message)s',
                'datefmt': '%F %T'
            },
            'file': {
                'format': '%(asctime)s.%(msecs)06d | %(mark)s | %(oid)s | %(message)s',
                'datefmt': '%T'
            }

        },
        'filters': {
            'default': {
                '()': lambda: default_filter
            },
            'console': {
                '()': lambda: console_filter
            }
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'formatter': 'console',
                'filters': [ 'default', 'console' ]
            },
            'file': {
                'class': 'logging.FileHandler',
                'formatter': 'file',
                'filters': [ 'default' ],
                'filename': 'siplibtest.log',
                'mode': 'w'
            }
        },
        'loggers': {
            '': {
                'level': logging.DEBUG,
                'handlers': [ 'console', 'file' ]
            }
        }
    })
