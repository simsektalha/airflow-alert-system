import re

SECRET_PATTERNS = [
    # AWS credentials
    r"AKIA[0-9A-Z]{16}",
    r"\baws_secret_access_key\b\s*[:=]\s*[A-Za-z0-9/+=]{30,}",
    r"\baws_access_key_id\b\s*[:=]\s*[A-Za-z0-9/+=]{20,}",
    r"\baws_session_token\b\s*[:=]\s*[A-Za-z0-9/+=]{100,}",

    # Generic secrets
    r"\bsecret\b\s*[:=]\s*[^,\s'\";]+",
    r"\bpassword\b\s*[:=]\s*[^,\s'\";]+",
    r"\btoken\b\s*[:=]\s*[^,\s'\";]+",
    r"\bkey\b\s*[:=]\s*[A-Za-z0-9/+=]{20,}",
    r"\bapi_key\b\s*[:=]\s*[A-Za-z0-9/+=]{20,}",

    # JWT tokens
    r"authorization:\s*bearer\s+[A-Za-z0-9.\-]+",
    r"\beyJ[a-zA-Z0-9_\-]{10,}\.[a-zA-Z0-9.\-]{10,}\.[a-zA-Z0-9.\-]{10,}\b",

    # Email addresses
    r"[A-Za-z0-9.%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}",

    # Database connection strings
    r"postgresql://[^@]+@[^/]+",
    r"mysql://[^@]+@[^/]+",
    r"mongodb://[^@]+@[^/]+",
    r"redis://[^@]+@[^/]+",

    # URLs with credentials
    r"https?://[^@]+@[^/\s]+",

    # Private keys
    r"-----BEGIN [A-Z ]+ PRIVATE KEY-----",
    r"-----BEGIN [A-Z ]+ KEY-----",
]
SECRET_RE = re.compile("|".join(SECRET_PATTERNS), flags=re.IGNORECASE)


def redact_conn_strings(s: str) -> str:
    """Redact connection strings with credentials."""
    # Database connection strings
    s = re.sub(r"([a-zA-Z]+:\/\/)([^:@\s]+):([^@\/\s]+)@", r"\1[REDACTED]:[REDACTED]@", s)
    # URLs with credentials
    s = re.sub(r"(https?://)([^@]+)@([^/\s]+)", r"\1[REDACTED]@\3", s)
    return s


def redact_text(s: str) -> str:
    """Redact sensitive information from log text."""
    if not s:
        return ""

    # Redact connection strings first
    s = redact_conn_strings(s)

    # Redact other sensitive patterns
    s = SECRET_RE.sub("[REDACTED]", s)

    return s
