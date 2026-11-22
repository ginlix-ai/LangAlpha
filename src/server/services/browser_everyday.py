"""Bringing the sign-ins of a person's everyday browser into the kept profile of
the agent's browser on that same machine (``browser_runtime/everyday.py``).

Only a machine the person brings has everyday browsers, and only one that keeps
sign-ins has a profile to bring them into. Everything happens on the machine:
the cookies are read, unlocked and set there, and only their count comes back.
A computer that is not running is not started for it.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any, Literal, Union

from pydantic import BaseModel, NonNegativeInt, TypeAdapter, ValidationError

from ptc_agent.core.sandbox.browser_runtime.protocol import BringRefusal
from ptc_agent.core.sandbox.providers import provider_class
from src.server.models.computer import (
    BrowserSigninBringResponse,
    BrowserSigninRefusal,
    BrowserSigninSource,
    BrowserSigninSourcesResponse,
)
from src.server.services.browser_settings import keeps_signins
from src.server.services.browser_view import NOT_RUNNING, ask_running

logger = logging.getLogger(__name__)

__all__ = ["bring", "sources"]

_LIST_TIMEOUT_S = 15
# The Mac waits on the person to let the keychain be read (90 seconds on the
# machine), then the browser starts and the cookies are set.
_BRING_TIMEOUT_S = 180


class _Brought(BaseModel):
    done: Literal[True]
    count: NonNegativeInt
    skipped: NonNegativeInt = 0


class _Refused(BaseModel):
    done: Literal[False]
    reason: BringRefusal


# What the machine answers a bring with. A reason only the server gives, or one
# the machine has no word for, is a failure.
_BRING_ANSWER: TypeAdapter[Union[_Brought, _Refused]] = TypeAdapter(Union[_Brought, _Refused])


async def sources(computer: Mapping[str, Any]) -> BrowserSigninSourcesResponse:
    """The everyday browser profiles on *computer*'s machine, or why none are listed."""
    refusal = _refusal(computer)
    if refusal is not None:
        return BrowserSigninSourcesResponse(reason=refusal)
    answer = await _ask(computer, "list", timeout=_LIST_TIMEOUT_S)
    if isinstance(answer, str):
        return BrowserSigninSourcesResponse(reason=answer)
    listed = answer.get("sources")
    if not isinstance(listed, list):
        return BrowserSigninSourcesResponse(reason="failed")
    return BrowserSigninSourcesResponse(
        sources=[source for source in map(_source, listed) if source is not None]
    )


def _source(item: object) -> BrowserSigninSource | None:
    """*item* as a source a bring would take, or None: one it would refuse is not listed."""
    try:
        return BrowserSigninSource.model_validate(item)
    except ValidationError:
        return None


async def bring(computer: Mapping[str, Any], browser: str, profile: str) -> BrowserSigninBringResponse:
    """Bring *browser*'s sign-ins at *profile* into the kept profile: how many
    cookies came, how many were left behind, or why none came."""
    refusal = _refusal(computer)
    if refusal is not None:
        return BrowserSigninBringResponse(done=False, reason=refusal)
    answer = await _ask(computer, "bring", browser, profile, timeout=_BRING_TIMEOUT_S)
    if isinstance(answer, str):
        return BrowserSigninBringResponse(done=False, reason=answer)
    try:
        decoded = _BRING_ANSWER.validate_python(answer)
    except ValidationError:
        return BrowserSigninBringResponse(done=False, reason="failed")
    return BrowserSigninBringResponse.model_validate(decoded.model_dump())


def _refusal(computer: Mapping[str, Any]) -> BrowserSigninRefusal | None:
    """Why the server does not ask the machine: only a machine of the person's
    own has everyday browsers, and only one keeping sign-ins a profile to bring
    them into. Not the machine's ``not_kept``, which is a browser already open
    without them."""
    if not provider_class(str(computer.get("kind"))).installs_browser or not keeps_signins(computer):
        return "unavailable"
    return None


async def _ask(
    computer: Mapping[str, Any], *command: str, timeout: float
) -> dict[str, Any] | BrowserSigninRefusal:
    """The machine's answer to ``everyday`` *command*, or why there is none."""
    try:
        answer = await ask_running(computer, "everyday", *command, timeout=timeout)
    except Exception:
        logger.warning(
            f"[browser] could not reach the everyday browsers of computer {computer.get('computer_id')}",
            exc_info=True,
        )
        return "failed"
    if answer is NOT_RUNNING:
        return "not_running"
    return answer if isinstance(answer, dict) else "failed"
