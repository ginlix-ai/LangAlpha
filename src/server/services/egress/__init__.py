"""Generic sandbox egress relay — grants, sandbox auth, and streaming passthrough.

Sandboxes never hold vendor credentials; they dial ``/v1/egress/{grant_id}``
with a workspace-scoped relay JWT and the relay attaches the credential
host-side per request. MCP OAuth connections are the v1 grant producer; the
resolver layer is the extension point for further credential kinds.
"""

import unicodedata
from collections.abc import Iterable, Mapping
from enum import StrEnum


def header(headers: Mapping[str, str] | None, name: str) -> str | None:
    """``name``'s value in a plain header mapping, matched without case."""
    name = name.lower()
    return next((v for k, v in (headers or {}).items() if k.lower() == name), None)


def fold_tool_name(name: str | None) -> str:
    """A tool name reduced to what two spellings of it have in common.

    The one comparison the denial is ever read with, shared so that the relay's
    refusal and the registry's filter cannot disagree about which tool a
    declined group named. A vendor that recases or pads a name would otherwise
    have it blocked per call while the prompt, the generated wrappers and the
    per-tool docs went on advertising it.

    Only ever used to compare names, never to build the denial or to rewrite
    what is forwarded -- the vendor still receives the exact bytes the sandbox
    sent.
    """
    return unicodedata.normalize("NFKC", name or "").strip().casefold()


def folded(names: Iterable[str | None] | None) -> frozenset[str]:
    return frozenset(fold_tool_name(name) for name in names or ())


def folded_contains(names: Iterable[str | None] | None, tool: str | None) -> bool:
    return fold_tool_name(tool) in folded(names)


class RelayError(StrEnum):
    """Every value the relay can put in ``X-Relay-Error``.

    The sandbox-side client keys its user-facing hints off these codes but
    cannot import this module (it runs inside the sandbox, with none of the
    server package available), so the vocabulary is duplicated there by
    necessity. ``tests/unit/core/test_relay_error_hints.py`` is what keeps the
    two in step — extend that hint table whenever a member is added here.
    """

    # prepare_relay
    RELAY_DISABLED = "relay_disabled"
    RELAY_AUTH = "relay_auth"
    BAD_REQUEST = "bad_request"
    NOT_FOUND = "not_found"
    NEEDS_REAUTH = "needs_reauth"
    METHOD_BLOCKED = "method_blocked"
    TOOL_BLOCKED = "tool_blocked"
    POLICY_MISSING = "policy_missing"
    EXECUTION_REQUIRED = "execution_required"
    REFRESH_IN_PROGRESS = "refresh_in_progress"
    CREDENTIAL_UNAVAILABLE = "credential_unavailable"
    TERMS_REQUIRED = "terms_required"
    # metering.admit
    CREDITS_EXHAUSTED = "credits_exhausted"
    CHARGES_PENDING = "charges_pending"
    COST_EXCEEDS_BALANCE = "cost_exceeds_balance"
    LIMIT_REQUIRED = "limit_required"
    CREDIT_CHECK_UNAVAILABLE = "credit_check_unavailable"
    # open_upstream
    DESTINATION_BLOCKED = "destination_blocked"
    UPSTREAM_UNREACHABLE = "upstream_unreachable"
    VENDOR_REDIRECT = "vendor_redirect"
    # the route's own budgets
    LIMITED_RATE = "limited_rate"
    LIMITED_CONCURRENCY = "limited_concurrency"
    WALL_CLOCK = "wall_clock"


class AccountUnavailable(Exception):
    """No key can be had for the user on a platform account right now.

    Raised by a ``PlatformAccount.key_for`` or ``provision``; ``code`` says why
    for the log, and ``retryable`` decides whether the sandbox, or the app's
    accept, is told to come back.
    """

    def __init__(self, code: str, message: str, *, retryable: bool = False):
        super().__init__(message)
        self.code = code
        self.retryable = retryable


class RelayRejection(Exception):
    """Terminal per-request outcome, mapped by the router to an HTTP answer.

    ``code`` is machine-readable and surfaced as an X-Relay-Error header so
    the generated client (and the agent) can distinguish relay-auth failures
    from vendor-auth failures without parsing bodies. Every rejection the
    pipeline can raise is one of these, so the route needs exactly one arm.
    """

    def __init__(
        self,
        status: int,
        code: RelayError,
        detail: str = "",
        *,
        retry_after: int | None = None,
    ):
        self.status = status
        self.code = RelayError(code)
        self.detail = detail or str(self.code)
        self.retry_after = retry_after
        super().__init__(self.detail)


class Dispatch:
    """How far one relayed request got toward the vendor.

    A paid call that ends without a reply is free only if it provably never
    left: a vendor bills a call it received whether or not its answer made it
    back. ``sent`` turns on as the request's headers start out (httpcore's
    ``trace`` extension), so a refused pin, a failed connect and a wait for a
    pooled connection all leave it off, while a timeout or a cancellation
    after that leaves it on. ``status`` is the vendor's answer, once one came.
    """

    __slots__ = ("sent", "status")

    def __init__(self) -> None:
        self.sent = False
        self.status: int | None = None

    async def trace(self, event: str, info: Mapping[str, object]) -> None:
        if event.endswith(".send_request_headers.started"):
            self.sent = True
