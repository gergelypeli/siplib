from __future__ import unicode_literals, print_function

import uuid
import hashlib

from format import Auth, WwwAuth


def generate_nonce():
    return uuid.uuid4().hex[:8]


def md5(x):
    return hashlib.md5(x).hexdigest()
    

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
    def __init__(self, realm):
        self.realm = realm
        self.nonces = set()


    def authenticate(self, hop, authname):
        # return:
        #   None - request authentication unconditionally
        #   (authname, ha1) - check, and if wrong then request authentication
        #   (authname, None) - accept without check
        return ("anonymous", None)
    
    
    def identify(self, uri):
        # return:
        #   (authname, ha1) - how to identify ourselves at uri
        return None
        

    def challenge(self, stale=False):
        nonce = generate_nonce()
        self.nonces.add(nonce)  # TODO: must clean these up
        www = WwwAuth(self.realm, nonce, stale=stale, qop=[ "auth" ])
        return { 'www_authenticate': www }
        

    def required_digest_challenge(self, method, uri, auth, ha1):
        if not auth:
            print("Authority: no Authorization header!")
            return self.challenge()

        if not ha1:
            print("Authority: unknown user!")
            return self.challenge()
            
        if auth.realm != self.realm:
            print("Authority: wrong realm!")
            return self.challenge()
            
        if auth.nonce not in self.nonces:
            print("Authority: wrong nonce!")
            return self.challenge(True)
            
        if auth.uri != uri:  # TODO: this can be more complex than this
            print("Authority: wrong uri")
            return self.challenge()

        if auth.qop != "auth":
            print("Authority: QOP is not auth!")
            return self.challenge()
            
        if auth.algorithm not in (None, "MD5"):
            print("Authority: digest algorithm not MD5!")
            return self.challenge()
            
        if not auth.cnonce:
            print("Authority: cnonce not set!")
            return self.challenge()

        if not auth.nc:
            print("Authority: nc not set!")
            return self.challenge()

        response = digest(method, uri, ha1, auth.nonce, auth.qop, auth.cnonce, auth.nc)
        
        if auth.response != response:
            print("Authority: wrong response!")
            return self.challenge()
            
        self.nonces.remove(auth.nonce)
        return None


    def require_auth(self, params):
        if params["method"] in ("CANCEL", "ACK"):
            return None
            
        auth = params.get("authorization")
        authname = auth.username if auth else None
        
        # Identify peer by address or authname
        info = self.authenticate(params["hop"], authname)
        
        if not info:
            # No user or invalid user, come again
            return self.challenge()
            
        authname, ha1 = info
        if ha1:
            # Correct password required
            challenge = self.required_digest_challenge(params["method"], params["uri"].print(), auth, ha1)
            
            # Invalid password, come again
            if challenge:
                return challenge
        
        # Profit!
        params["authname"] = authname
        return None


    def provided_digest_response(self, method, uri, www_auth, authname, ha1):
        if "auth" not in www_auth.qop:
            print("Digest QOP auth not available!")
            return None
        
        if www_auth.algorithm not in (None, "MD5"):
            print("Digest algorithm not MD5!")
            return None
        
        realm = www_auth.realm
        nonce = www_auth.nonce
        opaque = www_auth.opaque
        qop = "auth"
        cnonce = generate_nonce()
        nc = 1  # we don't reuse server nonce-s
        
        response = digest(method, uri, ha1, nonce, qop, cnonce, nc)

        auth = Auth(realm, nonce, authname, uri, response, opaque=opaque, qop=qop, cnonce=cnonce, nc=nc)
        return { 'authorization':  auth }
    
    
    def provide_auth(self, response, request):
        www_auth = response.get("www_authenticate")
        if not www_auth:
            print("No known challenge found, sorry!")
            return None
            
        if "authorization" in request and not www_auth.stale:
            print("Already tried and not even stale, sorry!")
            return None

        info = self.identify(request["uri"])
        if not info:
            print("Can't identify myself, sorry!")
            return None
            
        authname, ha1 = info
        return self.provided_digest_response(request["method"], request["uri"].print(), www_auth, authname, ha1)
