"""The auth service as every gate reaches it: whether there is one to ask,
how a call authenticates, the validate call itself, and the 503 a gate answers
when it gets no verdict."""

import asyncio
import logging
import os
import time
from typing import Optional

from fastapi import HTTPException

from src.config.settings import AUTH_SERVICE_URL, HOST_MODE
from src.server.utils.http_client import get_http_client
from src.server.services.shared_workspaces.holder import refuse_holder

logger = logging.getLogger(__name__)


class QuotaServiceUnavailable(Exception):
    """The quota service could not be asked at all.

    Distinct from it answering "no": a denial arrives as a 200 whose ``quota``
    says ``allowed: false``. Everything else (transport failure, 5xx, a token
    the service rejects) means we have no verdict, and only the caller knows
    whether proceeding without one is acceptable.
    """


# The quota service deploys in place and refuses connections while it restarts,
# for about five seconds. Strict callers retry on this schedule, 7.5 s of waiting
# in all, so a check that lands in that gap waits it out; an outage that outlasts
# it is real, and they fail closed. Deterministic 4xx answers (a rejected service
# token, a malformed request) are not retried: they will not change.
#
# Only ``strict`` callers retry. For everyone else an unanswered check already
# means "carry on", so more attempts buy a slightly better answer at the cost of
# seconds per call, including inside the always-on reconciler, which probes once
# per owner. The caller that pays for accuracy is the one that blocks on it.
_VALIDATE_RETRY_DELAYS = (0.5, 1.0, 2.0, 2.0, 2.0)  # seconds before each retry
# The schedule assumes a refused connection, which fails at once. A service that
# accepts and then hangs costs the client timeout per attempt, so no wait starts
# that would end past this, and only the attempt already under way runs over.
_VALIDATE_RETRY_WINDOW = 8.0  # seconds from the first attempt

# Long enough that a retry lands after an in-place restart, short enough that a
# client which honours it does not look hung.
SERVICE_UNAVAILABLE_RETRY_AFTER = 15  # seconds

# -- the declared surface other modules call this one through ------------------
# Three names are the whole of how this service talks to the quota service:
# whose client (``get_http_client``, kept in ``src.server.utils.http_client``
# so the auth layer borrows it without importing this module), whether to talk
# at all, and what it authenticates with. They are public because the credit
# gate's port needs the same three, and a borrowed underscore name is a
# dependency no refactor here can see.


def platform_gating_active() -> bool:
    """True when platform scope/quota gates should run (not OSS and an auth URL set)."""
    return HOST_MODE != "oss" and bool(AUTH_SERVICE_URL)


def service_headers(user_id: Optional[str] = None) -> dict:
    """Headers for a service-to-service call to the quota service.

    The token is a shared secret, not a JWT, and is omitted when unset — which
    is a deployment fault rather than a mode: every gated endpoint answers 401
    without it. Callers that fail open on a non-200 cannot tell that apart from
    an outage, so they check for it themselves.
    """
    # Loud, not quiet: a holder is no account of the auth service's, so a
    # call naming one is a bug wherever it was made.
    refuse_holder(user_id, "service_headers")
    headers = {}
    if user_id:
        headers["X-User-Id"] = user_id
    token = os.getenv("INTERNAL_SERVICE_TOKEN", "")
    if token:
        headers["X-Service-Token"] = token
    return headers


def service_token_missing() -> bool:
    """Platform gating is on but nothing is configured to authenticate with."""
    return platform_gating_active() and not os.getenv(
        "INTERNAL_SERVICE_TOKEN", ""
    ).strip()


def quota_service_unavailable() -> HTTPException:
    """The 503 for a request the quota service gave no verdict on.

    One shape for every gate that refuses rather than guess, so a client shows
    an outage and retries instead of reading the status as a quota denial or a
    missing plan.
    """
    return HTTPException(
        status_code=503,
        detail={
            # Same wording the quota service uses for its own guard, so the
            # user reads one sentence whichever side went dark.
            "message": "Service temporarily unavailable. Please try again shortly.",
            "type": "service_unavailable",
            "retry_after": SERVICE_UNAVAILABLE_RETRY_AFTER,
        },
        headers={"Retry-After": str(SERVICE_UNAVAILABLE_RETRY_AFTER)},
    )


async def call_validate(
    user_id: str,
    check_quota: Optional[str] = None,
    byok: bool = False,
    strict: bool = False,
    surface: str | None = None,
    account_contract: int | None = None,
) -> Optional[dict]:
    """POST to the auth/quota service at /api/auth/validate.

    Returns None in OSS mode. When the service cannot be reached, ``strict``
    callers get :class:`QuotaServiceUnavailable` once the retry schedule runs
    out, and everyone else gets None on the first failure, which most of them
    read as "carry on". See the fail-open notes at those call sites for why that
    is safe there and not at the credit gate.
    """
    if not platform_gating_active():
        return None

    client = await get_http_client()
    headers = service_headers(user_id)
    if account_contract is not None:
        headers["X-Account-Contract"] = str(account_contract)

    body = {}
    if check_quota:
        body["check_quota"] = check_quota
    if byok:
        body["byok"] = True
    if surface:
        body["surface"] = surface

    url = f"{AUTH_SERVICE_URL.rstrip('/')}/api/auth/validate"
    reason = "unknown"
    delays = _VALIDATE_RETRY_DELAYS if strict else ()
    give_up_at = time.monotonic() + _VALIDATE_RETRY_WINDOW

    for attempt in range(len(delays) + 1):
        if attempt:
            delay = delays[attempt - 1]
            if time.monotonic() + delay > give_up_at:
                break
            await asyncio.sleep(delay)
        try:
            resp = await client.post(url, json=body if body else None, headers=headers)
        except Exception as e:
            reason = f"unreachable: {e}"
            logger.warning("auth/quota service unreachable (attempt %d): %s", attempt + 1, e)
            continue

        if resp.status_code == 200:
            try:
                return resp.json()
            except Exception as e:
                # A 200 we cannot read is still no verdict: an interposed proxy
                # serving its own HTML, a truncated body. It has to rejoin the
                # no-verdict path here, or it escapes as an unhandled 500 and
                # the gate that exists to fail closed fails differently instead.
                reason = f"unparseable 200: {e}"
                logger.warning(
                    "auth/quota service returned an unreadable 200 (attempt %d): %s",
                    attempt + 1, e,
                )
                continue

        reason = f"HTTP {resp.status_code}"
        logger.warning(
            "auth/quota service validate returned %d (attempt %d): %s",
            resp.status_code, attempt + 1, resp.text[:200],
        )
        if resp.status_code < 500:
            break  # deterministic answer; a retry returns the same thing

    if strict:
        raise QuotaServiceUnavailable(reason)
    return None
