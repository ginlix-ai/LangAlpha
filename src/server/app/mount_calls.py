"""What a computer's mount endpoints share (``livefs`` and its Drive actions).

Both authenticate the same mount token (``caller``) and file each change's
outcome under the call id of the command that made it (``mutation``), so a
refused save reaches the tool result. Each endpoint brings its own token
kind, its own refusals and what its daemon sends again.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import Any

from fastapi import Header, HTTPException, Request, Response

from ptc_agent.core.paths import logged_path
from ptc_agent.core.sandbox.livefs_mount import CallContext
from ptc_agent.core.sandbox.livefs_runtime.protocol import CALL_HEADER
from src.server.auth.computer_token_core import Kind
from src.server.services.livefs import outcomes, tokens
from src.server.services.livefs.tokens import LivefsIdentity
from src.server.utils.api import admit_caller

#: A change's outcome fields (None ones left out) and the response it answers with.
Change = Callable[[str | None, CallContext | None], Awaitable[tuple[dict[str, Any], Any]]]
#: An error the endpoint answers as a refusal: the outcome's fields and the
#: response; None for an error that is no refusal.
Refuser = Callable[[Exception], tuple[dict[str, Any], Response] | None]
#: Why the daemon sends a change that did not land again, which leaves its
#: outcome to the retry; None when the outcome is final.
Resent = Callable[[Exception], str | None]


def _never(_: Exception) -> str | None:
    return None


def caller(kind: Kind, what: str) -> Callable[..., Awaitable[LivefsIdentity]]:
    """The dependency that says who a request acts for, by a token of
    ``kind``: refused 401 for a token that is not live, 429 past the
    computer's rate, and closed with the account."""

    async def identify(
        request: Request, authorization: str | None = Header(None)
    ) -> LivefsIdentity:
        try:
            identity = await tokens.authenticate(authorization, kind=kind)
        except tokens.LivefsAuthError:
            raise HTTPException(
                status_code=401,
                detail="Invalid or expired mount token",
                headers={"WWW-Authenticate": "Bearer"},
            ) from None
        except tokens.LivefsThrottled as exc:
            # Refused before the request runs, so a retried save cannot land
            # twice. The program sees EAGAIN, which most never retry, so the
            # command's tool result says to.
            call_id = outcomes.valid_call_id(request.headers.get(CALL_HEADER))
            if call_id:
                await outcomes.throttled(exc.identity.computer_id, call_id)
            raise HTTPException(
                status_code=429,
                detail=f"Too many {what} requests from this computer",
                headers={"Retry-After": str(exc.retry_after)},
            ) from None
        # What the mount serves is the account's, so it closes with the account.
        await admit_caller(request, identity.user_id)
        return identity

    return identify


def _logged(outcome: dict[str, Any]) -> dict[str, Any]:
    """An outcome as the log keeps it. The report names the user's
    automations and a refusal's text quotes what it refused (a file name, a
    holding), so the log keeps the refusal's code; an automation's file is
    named after it, so in that folder the log keeps the folder alone. The
    tool result carries all of it."""
    kept = {k: v for k, v in outcome.items() if k not in ("report", "error")}
    for key in ("path", "from"):
        if kept.get(key):
            kept[key] = logged_path(kept[key])
    return kept


async def mutation(
    request: Request,
    identity: LivefsIdentity,
    op: str,
    change: Change,
    *,
    log: logging.Logger,
    label: str,
    refusal: Refuser,
    failed_path: str,
    resent: Resent = _never,
) -> Any:
    """Make one change and file its outcome for the command that made it.
    A change whose command filed nothing (Redis was down, or the filing
    expired) runs for no conversation or workspace."""
    call_id = outcomes.valid_call_id(request.headers.get(CALL_HEADER))
    context = await outcomes.call_context(identity.computer_id, call_id)

    async def report(outcome: dict[str, Any]) -> None:
        log.info(
            "%s %s",
            label,
            op,
            extra={
                "computer_id": identity.computer_id,
                "user_id": identity.user_id,
                "call_id": call_id,
                **_logged(outcome),
            },
        )
        # An id no command filed is no call's, so ``record`` reads nothing again.
        filed = call_id if context is not None else None
        await outcomes.record(identity.computer_id, filed, outcome, context)

    try:
        fields, response = await change(call_id, context)
    except Exception as exc:
        refused = refusal(exc)
        # Once answered, error or not, the daemon does not send a change
        # again unless ``resent`` says it does, so unreported it is lost.
        outcome = {"op": op, "ok": False}
        if refused is None:
            outcome.update(path=failed_path, code="unavailable", error="the server failed; retry")
        else:
            outcome.update(refused[0])
        why = resent(exc)
        if why is not None:
            log.info("%s %s %s refused", label, why, op, extra=_logged(outcome))
        else:
            await report(outcome)
        if refused is None:
            raise
        return refused[1]
    kept = {k: v for k, v in fields.items() if v is not None}
    await report({"op": op, "ok": True, **kept})
    return response

