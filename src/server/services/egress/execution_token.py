"""The one-execution credential an approved order carries to the relay.

The relay JWT proves which workspace is calling and lives for hours. It cannot
say that *this* order, with *these* arguments, was approved and has not run
yet. This token does: it is minted only after the ledger hands out the single
execution an approval buys, it names the attempt, the call, the tool and the
hash of the arguments, and it expires in minutes.

Everything it signs, the relay derives again from the frame and the ledger row,
so a token cannot be moved onto another call, another tool, or another set of
arguments: the recomputation simply fails the MAC.

Shape is ``attempt_id.exp.sig``, deliberately not a JWT. There are no claims to
negotiate and no algorithm to be told, which is the whole attack surface a JWT
adds here.
"""

from __future__ import annotations

import base64
import hmac
import time
import uuid
from dataclasses import dataclass
from hashlib import sha256

__all__ = [
    "EXECUTION_HEADER",
    "DEFAULT_TTL_SECONDS",
    "ExecutionTokenError",
    "ParsedExecutionToken",
    "mint_execution_token",
    "parse_execution_token",
    "verify_execution_token",
]

# Named in the relay's request allowlist by its absence: the allowlist is what
# keeps this from reaching the vendor, so the header must never be added there.
EXECUTION_HEADER = "X-Relay-Execution"

# Long enough for a vendor handshake and a slow relay hop, short enough that a
# token captured off a loopback socket is worthless by the time it is replayed.
# Replay is already refused by the consumed row; this bounds the window in
# which a token is even syntactically live.
DEFAULT_TTL_SECONDS = 120

_VERSION = "v1"


class ExecutionTokenError(Exception):
    """The presented token is not a valid grant for this call."""


@dataclass(frozen=True)
class ParsedExecutionToken:
    """The token read without trusting it: enough to find the row it names."""

    attempt_id: str
    expires_at: int
    signature: str


def _b64(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _signature(
    secret: str,
    *,
    attempt_id: str,
    tool_call_id: str,
    tool: str,
    args_sha256: str,
    expires_at: int,
) -> str:
    message = "\x00".join(
        (
            _VERSION,
            attempt_id,
            tool_call_id,
            tool,
            args_sha256 or "",
            str(expires_at),
        )
    ).encode("utf-8")
    return _b64(hmac.new(secret.encode("utf-8"), message, sha256).digest())


def mint_execution_token(
    secret: str,
    *,
    attempt_id: str,
    tool_call_id: str,
    tool: str,
    args_sha256: str | None,
    ttl: int = DEFAULT_TTL_SECONDS,
    now: int | None = None,
) -> str:
    """Sign one execution of one call. Raises if the relay has no secret."""
    if not secret:
        raise ExecutionTokenError("the egress relay is not configured")
    expires_at = int(now if now is not None else time.time()) + int(ttl)
    signature = _signature(
        secret,
        attempt_id=attempt_id,
        tool_call_id=tool_call_id,
        tool=tool,
        args_sha256=args_sha256 or "",
        expires_at=expires_at,
    )
    return f"{attempt_id}.{expires_at}.{signature}"


def parse_execution_token(token: str) -> ParsedExecutionToken:
    """Split the token without checking it, so the row it names can be read.

    The attempt id is the lookup key and the signature covers it, so reading
    the row before verifying costs one indexed read and gives the verification
    the ``tool_call_id`` only the ledger knows. Only the uuid form minting
    writes is accepted, because that read binds the id to a uuid column, where
    any other string fails as a database error instead of a refusal.
    """
    parts = (token or "").strip().split(".")
    if len(parts) != 3 or not all(parts):
        raise ExecutionTokenError("malformed execution token")
    attempt_id, expires, signature = parts
    try:
        canonical = str(uuid.UUID(attempt_id))
    except ValueError:
        canonical = None
    if canonical != attempt_id:
        raise ExecutionTokenError("malformed execution token attempt id")
    try:
        expires_at = int(expires)
    except ValueError:
        raise ExecutionTokenError("malformed execution token expiry") from None
    return ParsedExecutionToken(
        attempt_id=attempt_id, expires_at=expires_at, signature=signature
    )


def verify_execution_token(
    secret: str,
    token: str,
    *,
    tool_call_id: str,
    tool: str,
    args_sha256: str | None,
    now: int | None = None,
) -> ParsedExecutionToken:
    """Check the token against what this call actually is, or raise.

    ``args_sha256`` is recomputed by the caller from the frame it is about to
    forward, never taken from the token, which is what binds an approval to the
    arguments that execute.
    """
    if not secret:
        raise ExecutionTokenError("the egress relay is not configured")
    parsed = parse_execution_token(token)
    if parsed.expires_at < int(now if now is not None else time.time()):
        raise ExecutionTokenError("execution token expired")
    expected = _signature(
        secret,
        attempt_id=parsed.attempt_id,
        tool_call_id=tool_call_id,
        tool=tool,
        args_sha256=args_sha256 or "",
        expires_at=parsed.expires_at,
    )
    if not hmac.compare_digest(expected, parsed.signature):
        raise ExecutionTokenError("execution token does not match this call")
    return parsed
