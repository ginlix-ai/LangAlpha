"""Where a Claude Code row runs: only as an analyst turn's own model.

A role, a fallback, a subagent pin and a Background job each send their
request through a chat client, which such a row does not build
(``src.llms.claude_code``), and Chief of Staff does not run on the runtime
yet. ``resolve_llm_config`` decides once whether the turn is a Claude Code
turn; everything else asks this module.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, NoReturn

from src.llms.claude_code import (
    FEATURE,
    ClaudeTurnFailed,
    another_model_replied_last,
    is_claude_code_model,
    runtime_available,
)
from src.llms.error_classification import iter_exception_chain
from src.server.database.oauth_tokens import CLAUDE_CODE_PROVIDER, get_oauth_tokens
from src.utils.cache.redis_cache import get_cache_client

from .availability import drop_models, model_resolves

if TYPE_CHECKING:
    from ptc_agent.agent.roles import AgentRole

#: Why a Claude Code row cannot run this turn, as the user is told it.
CLAUDE_CODE_OFF = {
    "server": "This server does not run Claude Code models. Pick another model.",
    "account": "Claude Code is off for your account. Turn it on in Settings under Experiments, or pick another model.",
    "chief_of_staff": "Chief of Staff does not run on Claude Code models yet. Pick another model.",
    "other_model": "Claude Code cannot continue a thread another model has replied on. Start a new thread to use this model.",
    "signed_out": "Add a Claude Code setup token in Settings under Model access to use this model.",
}


def raise_claude_code_off(model_name: str, reason: str) -> NoReturn:
    """Raise the 400 for a Claude Code row this turn cannot run, ``reason``
    keying ``CLAUDE_CODE_OFF``.

    Not ``model_removed``: the row is in the catalog, so nothing saved is
    cleared. Nor one of the refusals a service turn answers by running the
    account default, since that would move the turn to another model.
    """
    from fastapi import HTTPException

    raise HTTPException(status_code=400, detail=_refusal(model_name, reason))


def _refusal(model_name: str | None, reason: str) -> dict[str, Any]:
    return {"message": CLAUDE_CODE_OFF[reason], "type": "claude_code_off", "model": model_name, "reason": reason}


def claude_code_refusal(exc: BaseException, model_name: str | None) -> dict[str, Any] | None:
    """The refusal a turn ends with when Anthropic refused its setup token
    after admission let it run, worded as admission would have refused it;
    None for any other failure."""
    for current in iter_exception_chain(exc):
        if isinstance(current, ClaudeTurnFailed) and current.kind == "auth_failed":
            return _refusal(model_name, "signed_out")
    return None


def require_runnable(config, model: str, role: AgentRole) -> None:
    """Refuse a turn on ``model``, a Claude Code row, that cannot run it."""
    if not runtime_available():
        raise_claude_code_off(model, "server")
    if not config.feature_enabled(FEATURE):
        raise_claude_code_off(model, "account")
    if role == "chief_of_staff":
        raise_claude_code_off(model, "chief_of_staff")


#: How long a token's check stands. A turn Anthropic refuses the token on
#: records that at once (``SavedToken.refused``), so this bounds only a
#: token revoked while no turn runs on it.
TOKEN_CHECK_TTL_S = 3600


def token_check_key(token: str) -> str:
    """Where the status check keeps its answer for ``token``: by hash, never the token itself."""
    return f"claude_code:token_check:{hashlib.sha256(token.encode()).hexdigest()}"


async def stored_row(user_id: str) -> dict | None:
    """The token store's row for the setup token ``user_id`` saved, or None."""
    row = await get_oauth_tokens(user_id, CLAUDE_CODE_PROVIDER)
    return row if row and row["access_token"] else None


async def stored_token(user_id: str) -> str | None:
    """The setup token ``user_id`` saved for Claude Code, or None."""
    row = await stored_row(user_id)
    return row["access_token"] if row else None


async def require_signed_in(model: str, user_id: str) -> None:
    """Refuse a turn on ``model`` whose token is known not to work: none
    saved, or one the status check found refused.

    A token not checked yet runs, so admission never waits on Anthropic; one
    refused there ends the turn with this same message.
    """
    token = await stored_token(user_id)
    if token is None or await get_cache_client().get(token_check_key(token)) is False:
        raise_claude_code_off(model, "signed_out")


@dataclass(frozen=True)
class SavedToken:
    """The setup token ``user_id`` saved, as a Claude Code turn uses it:
    read as its run starts, so a token replaced meanwhile is the one it runs
    on, and recorded as refused when Anthropic refuses it, so Settings and
    admission say so from then on."""

    user_id: str

    async def read(self) -> str | None:
        return await stored_token(self.user_id)

    async def refused(self, token: str) -> None:
        await get_cache_client().set(token_check_key(token), False, ttl=TOKEN_CHECK_TTL_S)


def role_resolves(model_pref: dict, name: str) -> bool:
    """Whether a role, a fallback or a subagent pin can run ``name``."""
    return not is_claude_code_model(name) and model_resolves(model_pref, name)


def drop_claude_code_roles(
    model_pref: dict, keys: Iterable[str]
) -> tuple[dict, list[tuple[str, str]]]:
    """``model_pref`` as if no Claude Code row were saved under ``keys`` or in
    ``fallback_models``, plus what it left out.

    A Claude Code row runs only as a turn's own model. A role, a fallback or
    a Background job sends its request through a chat client, which such a
    row does not build, so there it reads as unset, as a dead name does. The
    view is read-only: the saved pick stays for the turn that can run it.
    """
    return drop_models(model_pref, keys, lambda name: not is_claude_code_model(name))


async def require_claude_code_continues(
    thread_id: str, name: str | None, checkpoint_id: str | None = None
) -> None:
    """Refuse a Claude Code row on a thread another model last replied on,
    before the thread keeps it: Claude's session holds only what Claude said,
    so it would carry on without that reply and what led to it.

    Read where the turn would start: ``checkpoint_id`` for an edit or a
    regenerate, else the base an ordinary turn takes.
    """
    from src.server.services.history.reader import CheckpointHistoryReader
    from src.server.utils.checkpoint_helpers import get_checkpointer, turn_base_checkpoint_id

    if not is_claude_code_model(name):
        return
    base = checkpoint_id or await turn_base_checkpoint_id(get_checkpointer(), thread_id)
    state = await CheckpointHistoryReader.get_instance().aget_state(thread_id, base)
    messages = (state.values or {}).get("messages") or [] if state else []
    if another_model_replied_last(messages):
        raise_claude_code_off(name, "other_model")
