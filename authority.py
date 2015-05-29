from __future__ import unicode_literals, print_function

import uuid
import hashlib

from format import Auth, WwwAuth


class Credentials(object):
    def __init__(self, realm, username, ha1, hop):
        self.realm = realm
        self.username = username
        self.ha1 = ha1
        self.hop = hop


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
    def __init__(self):
        self.nonces = set()


    def need_auth(self, cred, params):
        if params["method"] in ("CANCEL", "ACK"):
            return None
            
        if not cred:
            return None

        def challenge(stale=False):
            nonce = generate_nonce()
            self.nonces.add(nonce)  # TODO: must clean these up
            www = WwwAuth(cred.realm, nonce, stale=stale, qop=[ "auth" ])
            return { 'www_authenticate': www }
            
        auth = params.get("authorization")
        if not auth:
            print("Authority: no Authorization header")
            return challenge()
            
        if auth.username != cred.username:
            print("Authority: wrong username")
            return challenge()
            
        if auth.realm != cred.realm:
            print("Authority: wrong realm")
            return challenge()
            
        if auth.nonce not in self.nonces:
            print("Authority: wrong nonce")
            return challenge(True)
            
        if cred.hop and params["hop"] != cred.hop:
            print("Authority: wrong hop")
            return challenge()
            
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
            
        response = digest(params["method"], auth.uri, cred.ha1, auth.nonce, auth.qop, auth.cnonce, auth.nc)
        
        if auth.response != response:
            print("Authority: wrong response")
            return challenge()
            
        self.nonces.remove(auth.nonce)
        return None


    def provide_auth(self, cred, response, request):
        if not cred:
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
        username = cred.username
        uri = request["uri"].print()
        qop = "auth"
        cnonce = generate_nonce()
        nc = 1  # we don't reuse server nonce-s
        response = digest(request["method"], uri, cred.ha1, nonce, qop, cnonce, nc)

        auth = Auth(realm, nonce, username, uri, response, opaque=opaque, qop=qop, cnonce=cnonce, nc=nc)
        return { 'authorization':  auth }
