
MAX_FORWARDS = 20
BRANCH_MAGIC = "z9hG4bK"


def generate_tag():
    return uuid.uuid4().hex[:8]


def generate_call_id():
    return uuid.uuid4().hex[:8]


def generate_msgp_session_id(self):
    return uuid.uuid4().hex[:8]


def generate_nonce(self):
    return uuid.uuid4().hex[:8]


def generate_branch(self):
    return BRANCH_MAGIC + uuid.uuid4().hex[:8]


last_sdp_session_id = 0

def generate_sdp_session_id():
    global last_sdp_session_id
    last_sdp_session_id += 1
    return last_sdp_session_id
