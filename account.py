from weakref import proxy
import uuid
import hashlib

from format import Auth, WwwAuth
from log import Loggable

# Note: ha1 = md5("authname:realm:password")


def generate_nonce():
    return uuid.uuid4().hex[:8]


def md5(x):
    return hashlib.md5(x.encode()).hexdigest()
    

def digest(method, uri, ha1, nonce, qop=None, cnonce=None, nc=None):
    if not qop:
        ha2 = md5("%s:%s" % (method, uri))
        response = md5("%s:%s:%s" % (ha1, nonce, ha2))
        return response
    elif qop == "auth":
        ha2 = md5("%s:%s" % (method, uri))
        response = md5("%s:%s:%08x:%s:%s:%s" % (ha1, nonce, nc, cnonce, qop, ha2))
        return response
    else:
        raise Exception("Don't know QOP %s!" % qop)
        


class Authority(Loggable):
    def __init__(self):
        Loggable.__init__(self)

        self.nonces = set()


    def get_realm(self, params):
        return params["to"].uri.addr.host
            

    def check_digest_ha1(self, params, ha1):
        method = params["method"]
        uri = params["uri"].print()
        auth = params.get("authorization")
        
        if not auth:
            self.logger.debug("No Authorization header!")
            return False

        if not ha1:
            self.logger.debug("No ha1!")
            return False

        if auth.realm != self.get_realm(params):
            self.logger.debug("Wrong realm!")
            return False
        
        if auth.nonce not in self.nonces:
            self.logger.debug("Wrong nonce!")
            auth.stale = True
            return False
        
        if auth.uri != uri:  # TODO: this can be more complex than this
            self.logger.debug("Wrong uri")
            return False

        if auth.qop != "auth":
            self.logger.debug("QOP is not auth!")
            return False
        
        if auth.algorithm not in (None, "MD5"):
            self.logger.debug("Digest algorithm not MD5!")
            return False
        
        if not auth.cnonce:
            self.logger.debug("Cnonce not set!")
            return False

        if not auth.nc:
            self.logger.debug("Nc not set!")
            return False

        response = digest(method, uri, ha1, auth.nonce, auth.qop, auth.cnonce, auth.nc)
        
        if auth.response != response:
            self.logger.debug("Wrong response!")
            return False
            
        self.nonces.remove(auth.nonce)
        return True


    def get_digest_authname(self, params):
        auth = params.get("authorization")
        
        return auth.username if auth else None
        

    def check_auth(self, params, creds):
        cred_authname, cred_ha1 = creds
    
        authname = self.get_digest_authname(params)
        if not authname:
            self.logger.debug("No credentials in request")
            return False
            
        if authname != cred_authname:
            self.logger.debug("Wrong authname in request '%s'" % (authname,))
            return False
        
        if not self.check_digest_ha1(params, cred_ha1):
            self.logger.debug("Incorrect digest in request")
            return False

        self.logger.debug("Request authorized for '%s'" % (authname,))
        return True


    def require_auth(self, params, creds):
        # At this point we already decided the this request must be authorized
        if params["method"] in ("CANCEL", "ACK", "NAK"):
            raise Exception("Method %s can't be authenticated!" % params["method"])

        ok = self.check_auth(params, creds)
        if ok:
            return None
            
        auth = params.get("authorization")
        realm = self.get_realm(params)
        stale = auth.stale if auth else False
        nonce = generate_nonce()
        self.nonces.add(nonce)  # TODO: must clean these up
        www = WwwAuth(realm, nonce, stale=stale, qop=[ "auth" ])
        
        return { 'www_authenticate': www }


    def provide_auth(self, response, request, creds):
        www_auth = response.get("www_authenticate")
        if not www_auth:
            self.logger.debug("No known challenge found, sorry!")
            return None
            
        if "authorization" in request and not www_auth.stale:
            self.logger.debug("Already tried and not even stale, sorry!")
            return None

        if "auth" not in www_auth.qop:
            self.logger.debug("Digest QOP auth not available!")
            return None
        
        if www_auth.algorithm not in (None, "MD5"):
            self.logger.debug("Digest algorithm not MD5!")
            return None

        authname, ha1 = creds
        
        method = request["method"]
        uri = request["uri"].print()
        realm = www_auth.realm
        nonce = www_auth.nonce
        opaque = www_auth.opaque
        qop = "auth"
        cnonce = generate_nonce()
        nc = 1  # we don't reuse server nonce-s

        response = digest(method, uri, ha1, nonce, qop, cnonce, nc)
        auth = Auth(realm, nonce, authname, uri, response, opaque=opaque, qop=qop, cnonce=cnonce, nc=nc)
        return { 'authorization':  auth }


class Account:
    def __init__(self, manager, authname, ha1):
        self.manager = manager
        self.authname = authname
        self.ha1 = ha1
        
        
    def challenge_request(self, params):
        if not self.ha1:
            raise Exception("Can't challenge without ha1!")
            
        creds = self.authname, self.ha1
        return self.manager.authority.require_auth(params, creds)
        

class AccountManager(Loggable):
    def __init__(self):
        Loggable.__init__(self)

        self.authority = Authority()
        self.accounts_by_authname = {}
        self.our_credentials = None  # TODO: improve!


    def set_oid(self, oid):
        Loggable.set_oid(self, oid)
        
        self.authority.set_oid(oid.add("authority"))
        
        
    def add_account(self, authname, ha1):
        if authname in self.accounts_by_authname:
            raise Exception("Account already exists: %s!" % (authname,))
            
        self.logger.info("Adding account %s." % (authname,))
        account = Account(proxy(self), authname, ha1)
        self.accounts_by_authname[authname] = account
        
        return proxy(account)


    def get_account(self, authname):
        return self.accounts_by_authname.get(authname)
        

    def set_our_credentials(self, authname, ha1):
        # TODO: eventually we'd need multiple accounts for different places
        self.our_credentials = (authname, ha1)
        
        
    def provide_auth(self, response, request):
        return self.authority.provide_auth(response, request, self.our_credentials)
