from weakref import proxy

from util import Loggable


class Account(object):
    AUTH_NEVER = "AUTH_NEVER"
    AUTH_ALWAYS = "AUTH_ALWAYS"
    AUTH_IF_UNREGISTERED = "AUTH_IF_UNREGISTERED"
    AUTH_BY_HOP = "AUTH_BY_HOP"
    
    def __init__(self, manager, authname, policy, ha1):
        self.manager = manager
        self.authname = authname
        self.policy = policy
        self.ha1 = ha1
        
        self.records_by_aor = {}
        self.hops = set() if policy == self.AUTH_BY_HOP else None
        
        
    def add_record(self, record):
        aor = record.record_uri
        
        self.records_by_aor[aor] = record
        self.manager.map_aor(aor, self.authname)
        
        
    def add_hop(self, hop):
        self.hops.add(hop)
        
        
    def get_policy(self):
        return self.policy
        
        
    def get_ha1(self):
        return self.ha1
        
        

class AccountManager(Loggable):
    def __init__(self):
        Loggable.__init__(self)
        
        self.authnames_by_aor = {}
        self.accounts_by_authname = {}
        self.our_credentials = None  # TODO: improve!
        
        
    def add_account(self, authname, policy, ha1=None):
        if authname in self.accounts_by_authname:
            raise Exception("Account already exists: %s!" % (authname,))
            
        if policy in (Account.AUTH_ALWAYS, Account.AUTH_IF_UNREGISTERED) and not ha1:
            raise Exception("Policy %s needs a password!" % policy)
            
        self.logger.info("Adding account %s with policy %s." % (authname, policy))
        account = Account(proxy(self), authname, policy, ha1)
        self.accounts_by_authname[authname] = account
        
        return proxy(account)


    def map_aor(self, aor, authname):
        if aor in self.authnames_by_aor:
            raise Exception("AoR %s already mapped to %s!" % (aor, self.authnames_by_aor[aor]))
            
        self.logger.info("Mapping %s to authname %s." % (aor, authname))
        self.authnames_by_aor[aor] = authname
        
        
    def unmap_aor(self, aor):
        self.authnames_by_aor.pop(aor, None)
        
        
    def get_account_by_authname(self, authname):
        return self.accounts_by_authname.get(authname)
        
        
    def get_account_by_aor(self, aor):
        authname = self.authnames_by_aor.get(aor) or self.authnames_by_aor.get(aor._replace(username=None))
        
        return self.accounts_by_authname.get(authname)
        
        
    def auth_request(self, params):
        # Returns:
        #  (authname, None) to accept
        #  (authname, ha1) to challenge
        #  None to reject

        aor = params["from"].uri.canonical_aor()
        hop = params["hop"]

        authname = self.authnames_by_aor.get(aor) or self.authnames_by_aor.get(aor._replace(username=None))
        
        if not authname:
            self.logger.warning("Rejecting request because record is unknown")
            return None
            
        account = self.accounts_by_authname.get(authname)
        
        if not account:
            self.logger.error("Rejecting request because account is unknown!")
            return None
        elif account.policy == Account.AUTH_NEVER:
            self.logger.debug("Accepting request because authentication is never needed")
            return authname, None
        elif account.policy == Account.AUTH_ALWAYS:
            self.logger.debug("Authenticating request because account always needs it")
        elif account.policy == Account.AUTH_IF_UNREGISTERED:
            record = account.records_by_aor.get(aor)
            contacts = record.get_contacts()
            allowed_hops = [ contact.hop for contact in contacts ]

            self.logger.debug("Hop: %s, hops: %s" % (hop, allowed_hops))
            is_allowed = any(allowed_hop.contains(hop) for allowed_hop in allowed_hops)
            
            if not is_allowed:
                self.logger.debug("Authenticating request because account is not registered")
            else:
                self.logger.debug("Accepting request because account is registered")
                return authname, None
        elif account.policy == Account.AUTH_BY_HOP:
            allowed_hops = account.hops

            self.logger.debug("Hop: %s, hops: %s" % (hop, allowed_hops))
            is_allowed = any(allowed_hop.contains(hop) for allowed_hop in allowed_hops)
            
            if not is_allowed:
                self.logger.debug("Rejecting request because hop address is not allowed")
                return None
            else:
                self.logger.debug("Accepting request because hop address is allowed")
                return authname, None
        else:
            raise Exception("WTF?")

        ha1 = account.get_ha1()
        
        if not ha1:
            # Should have been AUTH_NEVER
            self.logger.error("No password set for account %s!" % authname)
        
        return authname, ha1
        

    def set_our_credentials(self, authname, ha1):
        # TODO: eventually we'd need multiple accounts for different places
        self.our_credentials = (authname, ha1)
        
        
    def get_our_credentials(self):
        return self.our_credentials
