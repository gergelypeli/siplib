from uuid import uuid4

MAX_FORWARDS = 20
BRANCH_MAGIC = "z9hG4bK"


def generate_tag():
    return uuid4().hex[:8]


def generate_call_id():
    return uuid4().hex[:8]


def generate_msgp_session_id():
    return uuid4().hex[:8]


def generate_nonce():
    return uuid4().hex[:8]


def generate_branch():
    return BRANCH_MAGIC + uuid4().hex[:8]


def generate_state_etag():
    return uuid4().hex[:8]


last_sdp_session_id = 0

def generate_sdp_session_id():
    global last_sdp_session_id
    last_sdp_session_id += 1
    return last_sdp_session_id
