"""Credential scrubbing for error text exposed or persisted by the server."""

import re

from fastapi import HTTPException
from pydantic import ValidationError

from ptc_agent.core.sandbox.providers.ssh_errors import connect_failure


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


def validation_error_text(e: ValidationError) -> str:
    """Each refusal as its field and the validator's own sentence.

    Never the input it rejected, which can carry a credential, nor the framing
    and link that a ValidationError's str() adds.
    """
    parts = []
    for err in e.errors(include_url=False):
        msg = err["msg"].removeprefix("Value error, ")
        field = ".".join(str(p) for p in err["loc"])
        parts.append(f"{field}: {msg}" if field else msg)
    return "; ".join(parts) or "validation error"


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
    renders differently. An SSH computer's connect failure is the exception:
    the machine is its owner's, so the detail says it is theirs that is not
    answering, in the provider's words naming it, rather than suggest the
    service is down. A tunnel that is down is told in its own words, which say
    whose side it is down on.
    """
    failure = connect_failure(exc)
    if failure is not None:
        owner = failure.for_owner
        lead = "" if owner == str(failure) else "your machine is not answering. "
        return f"{SANDBOX_UNREACHABLE_PREFIX}{lead}{owner}"
    if "starting" in str(exc).lower():
        return f"{SANDBOX_UNREACHABLE_PREFIX}the sandbox is still starting"
    return f"{SANDBOX_UNREACHABLE_PREFIX}please retry in a moment"


def sandbox_failure_http(exc: BaseException) -> HTTPException:
    """422 when an owner's SSH machine answered and refused, 503 otherwise.

    A refusal (a key it does not take, a different host key, an address or a
    platform this server does not serve, a root folder the login user cannot
    have) is the owner's to fix, and the provider's sentence says how; a
    machine that did not answer may on retry. One function for every route
    that starts a machine, so a computer and a workspace on it answer alike.
    """
    failure = connect_failure(exc)
    if failure is not None and failure.answered:
        return HTTPException(status_code=422, detail=str(failure))
    return HTTPException(status_code=503, detail=sandbox_unreachable_detail(exc))
