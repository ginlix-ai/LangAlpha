"""Credential scrubbing for error text exposed or persisted by the server."""

import re


# httpx includes request URLs in some exception messages. Strip basic-auth
# userinfo before those messages reach clients or durable conversation rows.
# Any scheme, not just http(s): the exceptions most likely to carry a password
# here are the connection failures, and those quote a DSN
# (`postgresql://user:pw@host`, `redis://…`, `amqp://…`) rather than a URL.
_URL_USERINFO_RE = re.compile(r"([a-zA-Z][a-zA-Z0-9+.-]*://)[^@/\s]+@")

# Provider exceptions can echo request headers or key parameters. Mask the
# common credential shapes without replacing otherwise useful diagnostics.
_BEARER_TOKEN_RE = re.compile(r"(?i)\b(bearer)\s+[A-Za-z0-9._~+/=-]{8,}")
_KEY_PARAM_RE = re.compile(
    r"(?i)\b(api[-_]?key|x-api-key|authorization|access[-_]?token|client[-_]?secret)"
    r"(\s*[=:]\s*)([\"']?)[A-Za-z0-9._~+/=-]{8,}"
)
_SK_TOKEN_RE = re.compile(r"\bsk-[A-Za-z0-9_-]{8,}\b")
_URL_KEY_QUERY_RE = re.compile(
    r"(?i)([?&](?:key|apikey|token|secret|password|credential)=)[^&\s\"']+"
)
_GOOGLE_KEY_RE = re.compile(r"\bAIza[0-9A-Za-z_-]{16,}\b")


def sanitize_error_text(text: str) -> str:
    """Scrub credential-shaped values from raw exception text."""
    text = _URL_USERINFO_RE.sub(r"\1", text)
    text = _BEARER_TOKEN_RE.sub(r"\1 [REDACTED]", text)
    text = _KEY_PARAM_RE.sub(r"\1\2\3[REDACTED]", text)
    text = _URL_KEY_QUERY_RE.sub(r"\1[REDACTED]", text)
    text = _SK_TOKEN_RE.sub("[REDACTED]", text)
    return _GOOGLE_KEY_RE.sub("[REDACTED]", text)


def single_line(text: str) -> str:
    """Escape CR/LF so untrusted text cannot forge log lines.

    Provider exceptions quote response bodies verbatim, and a body the client
    influenced can carry a newline followed by a convincing fake entry.
    """
    return text.replace("\r", "\\r").replace("\n", "\\n")


# The file panel categorizes a 503 by matching this prefix, so every producer
# must spell it identically or the same condition renders a different card.
SANDBOX_UNREACHABLE_PREFIX = "Sandbox is not reachable: "


def sandbox_unreachable_detail(exc: BaseException) -> str:
    """Client-safe 503 detail for an unreachable sandbox.

    Provider exceptions quote request URLs, sandbox ids and SDK response bodies,
    none of which may cross to a client, so the raw text never reaches this
    string and survives only in the server log. Reading "starting" back out of
    the message is a protocol read rather than the message matching this module
    otherwise avoids: the sandbox layer stamps that word deliberately as the
    carrier for the in-flight case, and it is the one distinction the file panel
    renders differently.
    """
    if "starting" in str(exc).lower():
        return f"{SANDBOX_UNREACHABLE_PREFIX}the sandbox is still starting"
    return f"{SANDBOX_UNREACHABLE_PREFIX}please retry in a moment"
