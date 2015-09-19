from __future__ import unicode_literals, print_function

import uuid
import hashlib
import logging
from format import Auth, WwwAuth

logger = logging.getLogger(__name__)

# ha1 = md5("authname:realm:password")


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
        


class Authority(object):
    def __init__(self):
        self.nonces = set()


    def get_realm(self, params):
        return params["to"].uri.addr.host
            

    def authorize(self, params):
        # return:
        #   True - if the request may pass
        return True
    
    
    def get_remote_credentials(self, params):
        # return:
        #   (authname, ha1) - how to identify ourselves for this request
        return None
        

    def check_digest_ha1(self, params, ha1):
        method = params["method"]
        uri = params["uri"].print()
        auth = params.get("authorization")
        
        if not auth:
            logger.debug("Authority: no Authorization header!")
            return False

        if not ha1:
            logger.debug("Authority: no ha1!")
            return False

        if auth.realm != self.get_realm(params):
            logger.debug("Authority: wrong realm!")
            return False
        
        if auth.nonce not in self.nonces:
            logger.debug("Authority: wrong nonce!")
            auth.stale = True
            return False
        
        if auth.uri != uri:  # TODO: this can be more complex than this
            logger.debug("Authority: wrong uri")
            return False

        if auth.qop != "auth":
            logger.debug("Authority: QOP is not auth!")
            return False
        
        if auth.algorithm not in (None, "MD5"):
            logger.debug("Authority: digest algorithm not MD5!")
            return False
        
        if not auth.cnonce:
            logger.debug("Authority: cnonce not set!")
            return False

        if not auth.nc:
            logger.debug("Authority: nc not set!")
            return False

        response = digest(method, uri, ha1, auth.nonce, auth.qop, auth.cnonce, auth.nc)
        
        if auth.response != response:
            logger.debug("Authority: wrong response!")
            return False
            
        self.nonces.remove(auth.nonce)
        return True


    def get_digest_authname(self, params):
        auth = params.get("authorization")
        
        return auth.username if auth else None
        

    def check_digest_authname_ha1(self, params, authname, ha1):
        return self.get_digest_authname(params) == authname and self.check_digest_ha1(params, ha1)


    def require_auth(self, params):
        if params["method"] in ("CANCEL", "ACK"):
            return None
        
        auth = params.get("authorization")
        if auth:
            auth.stale = False  # temporary attribute
            
        ok = self.authorize(params)
        logger.debug("Authorized: %s" % ok)
        if ok:
            return None
            
        # No user or invalid user, come again
        realm = self.get_realm(params)
        stale = auth.stale if auth else False
        nonce = generate_nonce()
        self.nonces.add(nonce)  # TODO: must clean these up
        www = WwwAuth(realm, nonce, stale=stale, qop=[ "auth" ])
        return { 'www_authenticate': www }


    def provide_auth(self, response, request):
        www_auth = response.get("www_authenticate")
        if not www_auth:
            logger.debug("No known challenge found, sorry!")
            return None
            
        if "authorization" in request and not www_auth.stale:
            logger.debug("Already tried and not even stale, sorry!")
            return None

        if "auth" not in www_auth.qop:
            logger.debug("Digest QOP auth not available!")
            return None
        
        if www_auth.algorithm not in (None, "MD5"):
            logger.debug("Digest algorithm not MD5!")
            return None

        info = self.get_remote_credentials(request)
        if not info:
            logger.debug("Can't identify myself, sorry!")
            return None
            
        authname, ha1 = info
        
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


class SimpleAuthority(Authority):
    def is_trusted_without_authentication(self, params):
        return False
        
        
    def get_local_credentials(self, authname):
        return None
        
        
    def authorize(self, params):
        from_uri = params["from"].uri
    
        if self.is_trusted_without_authentication(params):
            logger.debug("Trusting request without authentication from '%s'" % (from_uri,))
            return True
        
        authname = self.get_digest_authname(params)
        if not authname:
            logger.debug("Not trusting request without credentials!")
            return False
            
        local_credentials = self.get_local_credentials(authname)
        if not local_credentials:
            logger.debug("Not trusting request using unknown authname '%s'" % (authname,))
            return False
        
        ha1, record_uris = local_credentials
        
        if not self.check_digest_ha1(params, ha1):
            logger.debug("Not trusting request with invalid credentials!")
            return False

        if from_uri not in record_uris:
            logger.debug("Not trusting request with unauthorized authname '%s'!" % (authname,))
            return False

        logger.debug("Trusting request from authorized authname '%s'" % (authname,))
        return True
