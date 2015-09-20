
class Account(object):
    AUTH_NEVER = "AUTH_NEVER"
    AUTH_ALWAYS = "AUTH_ALWAYS"
    AUTH_IF_UNREGISTERED = "AUTH_IF_UNREGISTERED"
    AUTH_BY_ADDRESS = "AUTH_BY_ADDRESS"
    
    def __init__(self, display_name, auth_policy):
        self.display_name = display_name
        self.auth_policy = auth_policy
        

class AccountManager(object):
    def __init__(self):
        self.accounts_by_uri = {}
        
        
    def add_account(self, uri, display_name, auth_policy):
        if uri in self.accounts_by_uri:
            raise Exception("Account already exists: %s!" % (uri,))
            
        self.accounts_by_uri[uri] = Account(display_name, auth_policy)


    def get_account_auth_policy(self, uri):
        account = self.accounts_by_uri.get(uri)
        return account.auth_policy if account else None
