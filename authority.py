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


    def get_ha1(self, username):
        return None
        
        
    def is_allowed(self, username, params):
        return False
        

    def need_auth(self, params):
        if params["method"] in ("CANCEL", "ACK"):
            return None
            
        def challenge(stale=False):
            nonce = generate_nonce()
            self.nonces.add(nonce)  # TODO: must clean these up
            www = WwwAuth(self.realm, nonce, stale=stale, qop=[ "auth" ])
            return { 'www_authenticate': www }
            
        auth = params.get("authorization")
        if not auth:
            print("Authority: no Authorization header!")
            return challenge()
            
        if auth.realm != self.realm:
            print("Authority: wrong realm!")
            return challenge()
            
        if auth.nonce not in self.nonces:
            print("Authority: wrong nonce!")
            return challenge(True)
            
        if auth.uri != params["uri"].print():  # TODO: this can be more complex than this
            print("Authority: wrong uri")
            return challenge()

        if auth.qop != "auth":
            print("Authority: QOP is not auth!")
            return challenge()
            
        if auth.algorithm not in (None, "MD5"):
            print("Authority: digest algorithm not MD5!")
            return challenge()
            
        if not auth.cnonce:
            print("Authority: cnonce not set!")
            return challenge()

        if not auth.nc:
            print("Authority: nc not set!")
            return challenge()

        ha1 = self.get_ha1(auth.username)
        if not ha1:
            print("Authority: unknown username!")
            return challenge()
            
        #if cred.hop and params["hop"] != cred.hop:
        #    print("Authority: wrong hop")
        #    return challenge()
            
        response = digest(params["method"], auth.uri, ha1, auth.nonce, auth.qop, auth.cnonce, auth.nc)
        
        if auth.response != response:
            print("Authority: wrong response!")
            return challenge()
            
        self.nonces.remove(auth.nonce)
        
        if not self.is_allowed(auth.username, params):
            print("Authority: user not authorized for this operation!")
            return challenge()
            
        return None


    def provide_auth(self, username, response, request):
        if not username:
            return None

        www_auth = response["www_authenticate"]
            
        if "authorization" in request and not www_auth.stale:
            print("Already tried and not even stale, sorry!")
            return None
        
        if "auth" not in www_auth.qop:
            print("Digest QOP auth not available!")
            return None
        
        if www_auth.algorithm not in (None, "MD5"):
            print("Digest algorithm not MD5!")
            return None
        
        realm = www_auth.realm
        nonce = www_auth.nonce
        opaque = www_auth.opaque
        uri = request["uri"].print()
        qop = "auth"
        cnonce = generate_nonce()
        nc = 1  # we don't reuse server nonce-s
        ha1 = self.get_ha1(username)
        
        response = digest(request["method"], uri, ha1, nonce, qop, cnonce, nc)

        auth = Auth(realm, nonce, username, uri, response, opaque=opaque, qop=qop, cnonce=cnonce, nc=nc)
        return { 'authorization':  auth }
