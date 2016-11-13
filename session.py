
class Error(Exception):
    pass


class SessionState:
    def __init__(self):
        self.local_session = None
        self.remote_session = None
        self.pending_local_session = None
        self.pending_remote_session = None
        
        
    def set_local_offer(self, session):
        if not session:
            raise Error("No outgoing offer specified!")
        elif session["is_answer"]:
            raise Error("Outgoing offer is an answer!")
        elif self.pending_local_session:
            raise Error("Outgoing offer already pending!")
        elif self.pending_remote_session:
            raise Error("Incoming offer also pending!")
        else:
            self.pending_local_session = session


    def set_remote_offer(self, session):
        if not session:
            raise Error("No incoming offer specified!")
        elif session["is_answer"]:
            raise Error("Incoming offer is an answer!")
        elif self.pending_local_session:
            raise Error("Outgoing offer also pending!")
        elif self.pending_remote_session:
            raise Error("Incoming offer already pending!")
        else:
            self.pending_remote_session = session
            
            
    def set_local_answer(self, session):
        if not session:
            raise Error("No outgoing answer specified!")
        elif not self.pending_remote_session:
            raise Error("Incoming offer not pending!")
        elif not session["is_answer"]:
            raise Error("Outgoing answer is an offer!")
        elif len(session) == 1:  # rejected
            s = self.pending_remote_session
            self.pending_remote_session = None
            return s
        else:
            self.remote_session = self.pending_remote_session
            self.local_session = session
            self.pending_remote_session = None
            
        return None


    def set_remote_answer(self, session):
        if not session:
            raise Error("No incoming answer specified!")
        elif not self.pending_local_session:
            raise Error("Outgoing offer not pending!")
        elif not session["is_answer"]:
            raise Error("Incoming answer is an offer!")
        elif len(session) == 1:  # rejected
            s = self.pending_local_session
            self.pending_local_session = None
            return s
        else:
            self.local_session = self.pending_local_session
            self.remote_session = session
            self.pending_local_session = None
            
        return None


    def get_local_offer(self):
        if self.pending_local_session:
            return self.pending_local_session
        else:
            raise Error("Outgoing offer not pending!")
        
        
    def get_remote_offer(self):
        if self.pending_remote_session:
            return self.pending_remote_session
        else:
            raise Error("Incoming offer not pending!")
            
            
    def get_local_answer(self):
        if self.pending_local_session:
            raise Error("Outgoing offer is pending!")
        elif self.pending_remote_session:
            raise Error("Incoming offer still pending!")
        elif not self.local_session:
            raise Error("No local answer yet!")
        else:
            return self.local_session


    def get_remote_answer(self):
        if self.pending_local_session:
            raise Error("Outgoing offer still pending!")
        elif self.pending_remote_session:
            raise Error("Incoming offer is pending!")
        elif not self.remote_session:
            raise Error("No remote answer yet!")
        else:
            return self.remote_session
