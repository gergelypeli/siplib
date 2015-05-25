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
    

def digest(method, uri, ha1, nonce):
    ha2 = md5("%s:%s" % (method, uri))
    response = md5("%s:%s:%s" % (ha1, nonce, ha2))
    return response


class Authority(object):
    def __init__(self):
        self.nonces = set()


    def need_auth(self, cred, params):
        if params["method"] in ("CANCEL", "ACK"):
            return None
            
        if not cred:
            return None

        def challenge():
            nonce = generate_nonce()
            self.nonces.add(nonce)
            return WwwAuth(cred.realm, nonce)
            
        auth = params.get("authorization")
        if not auth:
            print("Authority: no Authorization header")
            return challenge()
            
        # auth: username, realm, nonce, uri, response
        # cred: username, realm, nonce, ha1, hop
            
        if auth.username != cred.username:
            print("Authority: wrong username")
            return challenge()
            
        if auth.realm != cred.realm:
            print("Authority: wrong realm")
            return challenge()
            
        if auth.nonce not in self.nonces:
            print("Authority: wrong nonce")
            return challenge()
            
        if cred.hop and params["hop"] != cred.hop:
            print("Authority: wrong hop")
            return challenge()
            
        if auth.uri != params["uri"].print():  # TODO: this can be more complex than this
            print("Authority: wrong uri")
            return challenge()
            
        response = digest(params["method"], auth.uri, cred.ha1, auth.nonce)
        
        if auth.response != response:
            print("Authority: wrong response")
            return challenge()
            
        self.nonces.remove(auth.nonce)
        return None


    def provide_auth(self, cred, response, request):
        if not cred:
            return None
            
        www_auth = response["www_authenticate"]
        
        realm = www_auth.realm
        nonce = www_auth.nonce
        username = cred.username
        uri = request["uri"].print()
        response = digest(request["method"], uri, cred.ha1, nonce)

        return Auth(realm, nonce, username, uri, response)
