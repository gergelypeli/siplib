from weakref import proxy
import uuid
import hashlib

from format import Auth, WwwAuth
from log import Loggable


class Authority(Loggable):
    def __init__(self, authname, ha1):
        Loggable.__init__(self)

        self.authname = authname
        self.ha1 = ha1
        self.nonces = set()


    def generate_nonce(self):
        return uuid.uuid4().hex[:8]


    def md5(self, x):
        return hashlib.md5(x.encode()).hexdigest()
    

    def compute_ha1(self, realm, authname, password):
        # Just a reminder
        return self.md5("%s:%s:%s" % (authname, realm, password))
        

    def compute_digest(self, method, uri, ha1, nonce, qop=None, cnonce=None, nc=None):
        if not qop:
            ha2 = self.md5("%s:%s" % (method, uri))
            response = self.md5("%s:%s:%s" % (ha1, nonce, ha2))
            return response
        elif qop == "auth":
            ha2 = self.md5("%s:%s" % (method, uri))
            response = self.md5("%s:%s:%08x:%s:%s:%s" % (ha1, nonce, nc, cnonce, qop, ha2))
            return response
        else:
            raise Exception("Don't know this quality of protection: %s!" % qop)
        

    def check_digest(self, method, auth, realm, ha1):
        if not auth:
            self.logger.debug("No Authorization header!")
            return False

        if not ha1:
            self.logger.debug("No ha1!")
            return False

        if auth.realm != realm:
            self.logger.debug("Wrong realm!")
            return False
        
        # FIXME: what shall we require? The RURI can be anything due to forwarding.
        # And can be different than the To header, too.
        #if auth.uri != uri:  # TODO: this can be more complex than this
        #    self.logger.debug("Wrong uri")
        #    return False

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

        response = self.compute_digest(method, auth.uri, ha1, auth.nonce, auth.qop, auth.cnonce, auth.nc)
        
        if auth.response != response:
            self.logger.debug("Wrong response!")
            return False
            
        self.nonces.remove(auth.nonce)
        return True



class Account(Authority):
    def __init__(self, manager, authname, ha1):
        Authority.__init__(self, authname, ha1)
        
        self.manager = manager


class LocalAccount(Account):
    def __init__(self, manager, authname, ha1, realm):
        Authority.__init__(self, authname, ha1)
        
        self.realm = realm
        

    def check_auth(self, params):
        method = params["method"]
        auth = params.get("authorization")
        
        if not auth:
            self.logger.debug("No credentials in request.")
            return False

        if auth.nonce not in self.nonces:
            self.logger.debug("Stale nonce in credentials, client should retry.")
            return False
        
        if auth.username != self.authname:
            self.logger.debug("Wrong authname in request '%s'!" % (auth.username,))
            return False
        
        if not self.check_digest(method, auth, self.realm, self.ha1):
            self.logger.debug("Incorrect digest in request!")
            return False

        self.logger.debug("Request authorized for '%s'." % (self.authname,))
        return True


    def require_auth(self, params):
        auth = params.get("authorization")
        stale = auth.nonce not in self.nonce if auth else False  # client should retry
        nonce = self.generate_nonce()
        self.nonces.add(nonce)  # TODO: must clean these up
        
        www_auth = WwwAuth(self.realm, nonce, stale=stale, qop=[ "auth" ])
        return { 'www_authenticate': www_auth }


class RemoteAccount(Account):
    def provide_auth(self, response, request):
        www_auth = response.get("www_authenticate")
        auth = request.get("authorization")
        
        if not www_auth:
            self.logger.debug("No challenge found in response, giving up!")
            return None
            
        if www_auth.stale:
            self.logger.debug("Our nonce was stale, retrying with a fresh one.")
        elif auth:
            self.logger.debug("Already tried this with a fresh nonce, giving up!")
            return None

        if "auth" not in www_auth.qop:
            self.logger.debug("Digest QOP auth not available!")
            return None
        
        if www_auth.algorithm not in (None, "MD5"):
            self.logger.debug("Digest algorithm not MD5!")
            return None

        method = request["method"]
        # Since the other and will have no clue how the RURI changed during the routing,
        # it should only use the URI we put in this header for computations.
        uri = request["to"].uri.print()
        realm = www_auth.realm
        nonce = www_auth.nonce
        opaque = www_auth.opaque
        qop = "auth"
        cnonce = self.generate_nonce()
        nc = 1  # we don't reuse server nonce-s

        response = self.compute_digest(method, uri, self.ha1, nonce, qop, cnonce, nc)
        auth = Auth(realm, nonce, self.authname, uri, response, opaque=opaque, qop=qop, cnonce=cnonce, nc=nc)
        return { 'authorization':  auth }
        

class AccountManager(Loggable):
    def __init__(self):
        Loggable.__init__(self)

        self.local_accounts_by_authname = {}
        self.remote_accounts_by_uri = {}


    def add_local_account(self, authname, ha1, realm):
        if authname in self.local_accounts_by_authname:
            raise Exception("Local account already exists: %s!" % (authname,))
            
        self.logger.info("Adding local account %s@%s." % (authname, realm))
        account = LocalAccount(proxy(self), authname, ha1, realm)
        account.set_oid(self.oid.add("local", authname))
        self.local_accounts_by_authname[authname] = account
        
        return proxy(account)


    def get_local_account(self, authname):
        return self.local_accounts_by_authname.get(authname)
        

    def add_remote_account(self, uri, authname, ha1):
        uri = uri.assert_resolved()
        
        if uri in self.remote_accounts_by_uri:
            raise Exception("Remote account for %s already exists: %s!" % (uri, authname))
            
        self.logger.info("Adding remote account for %s: %s" % (uri, authname,))
        account = RemoteAccount(proxy(self), authname, ha1)
        account.set_oid(self.oid.add("remote", str(uri)))
        self.remote_accounts_by_uri[uri] = account
        
        return proxy(account)


    def get_remote_account(self, request_uri):
        request_uri.assert_resolved()
        
        for uri in self.remote_accounts_by_uri:
            if uri.contains(request_uri):
                return self.remote_accounts_by_uri[uri]

        self.logger.warning("Couldn't find a remote account for %s!" % (request_uri,))
        return None
