
class Error(Exception):
    pass


class SessionState:
    def __init__(self):
        self.ground_session = None
        self.party_session = None
        self.pending_ground_session = None
        self.pending_party_session = None
        
        
    def set_ground_session(self, session):
        if not session:
            raise Error("No ground session specified!")
        elif not session["is_answer"]:
            # Offer
            
            if self.pending_ground_session:
                raise Error("Ground offer already pending!")
            elif self.pending_party_session:
                raise Error("Party offer also pending!")
            else:
                self.pending_ground_session = session
        else:
            # Answer

            if not self.pending_party_session:
                raise Error("Party offer not pending!")
            elif len(session) == 1:  # rejected
                self.pending_party_session = None
            else:
                self.party_session = self.pending_party_session
                self.ground_session = session
                self.pending_party_session = None


    def set_party_session(self, session):
        if not session:
            raise Error("No party session specified!")
        elif not session["is_answer"]:
            # Offer
            
            if self.pending_ground_session:
                raise Error("Ground offer also pending!")
            elif self.pending_party_session:
                raise Error("Party offer already pending!")
            else:
                self.pending_party_session = session
        else:
            # Answer
            
            if not self.pending_ground_session:
                raise Error("Ground offer not pending!")
            elif len(session) == 1:  # rejected
                self.pending_ground_session = None
            else:
                self.ground_session = self.pending_ground_session
                self.party_session = session
                self.pending_ground_session = None
            

    def get_ground_offer(self):
        if self.pending_ground_session:
            return self.pending_ground_session
        else:
            raise Error("Ground offer not pending!")
        
        
    def get_party_offer(self):
        if self.pending_party_session:
            return self.pending_party_session
        else:
            raise Error("Party offer not pending!")
            
            
    def get_ground_answer(self):
        if self.pending_ground_session:
            raise Error("Ground offer is pending!")
        elif self.pending_party_session:
            raise Error("Party offer still pending!")
        elif not self.ground_session:
            raise Error("No ground answer yet!")
        elif not self.ground_session["is_answer"]:
            raise Error("Ground was not the answering one!")
        else:
            return self.ground_session


    def get_party_answer(self):
        if self.pending_ground_session:
            raise Error("Ground offer still pending!")
        elif self.pending_party_session:
            raise Error("Party offer is pending!")
        elif not self.party_session:
            raise Error("No party answer yet!")
        elif not self.party_session["is_answer"]:
            raise Error("Party was not the answering one!")
        else:
            return self.party_session
