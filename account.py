from util import Loggable


class Account(object):
    AUTH_NEVER = "AUTH_NEVER"
    AUTH_ALWAYS = "AUTH_ALWAYS"
    AUTH_IF_UNREGISTERED = "AUTH_IF_UNREGISTERED"
    AUTH_BY_HOP = "AUTH_BY_HOP"
    
    def __init__(self, display_name, auth_policy, authname, ha1, hops=None):
        self.display_name = display_name
        self.auth_policy = auth_policy
        self.authname = authname
        self.ha1 = ha1
        self.hops = hops
        

class AccountManager(Loggable):
    def __init__(self):
        Loggable.__init__(self)
        
        self.accounts_by_uri = {}
        self.our_credentials = None  # TODO: improve!
        
        
    def add_account(self, uri, display_name, auth_policy, authname, ha1, hops=None):
        if uri in self.accounts_by_uri:
            raise Exception("Account already exists: %s!" % (uri,))
            
        self.accounts_by_uri[uri] = Account(display_name, auth_policy, authname, ha1, hops)


    def get_account(self, uri):
        account = self.accounts_by_uri.get(uri)
        
        if not account:
            account = self.accounts_by_uri.get(uri._replace(user=None))
            
        return account
    

    def get_account_auth_policy(self, uri):
        #self.logger.debug("get_account_auth_policy for %s" % str(uri))
        #self.logger.debug("but have %s" % str(self.accounts_by_uri.keys()))
        
        account = self.get_account(uri)
        return account.auth_policy if account else None


    def get_account_credentials(self, uri):
        account = self.get_account(uri)
        return (account.authname, account.ha1) if account else None


    def get_account_hops(self, uri):
        account = self.get_account(uri)
        return account.hops if account else None


    def set_our_credentials(self, authname, ha1):
        # TODO: eventually we'd need multiple accounts for different places
        self.our_credentials = (authname, ha1)
        
        
    def get_our_credentials(self):
        return self.our_credentials
