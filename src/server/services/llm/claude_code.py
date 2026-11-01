"""Where a Claude Code row runs: only as an analyst turn's own model.

A role, a fallback, a subagent pin and a Background job each send their
request through a chat client, which such a row does not build
(``src.llms.claude_code``), and Chief of Staff does not run on the runtime
yet. ``resolve_llm_config`` decides once whether the turn is a Claude Code
turn; everything else asks this module.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, NoReturn

from ptc_agent.core.claude_code_managed import local_managed
from ptc_agent.core.sandbox.providers.ssh import MUSL_UNSUPPORTED
from src.llms.claude_code import (
    FEATURE,
    ClaudeTurnFailed,
    another_model_replied_last,
    is_claude_code_model,
    backend_available,
    rows_available,
)
from src.llms.error_classification import iter_exception_chain
from src.server.services.claude_code.flow import POLICY_BLOCKED
from src.server.services.claude_code.machine import NEEDS_PERL, computer_state, read_answer
from src.server.services.claude_code.saved import usable_token
from src.server.services.ssh_computers import is_own_computer

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
    "needs_own_computer": (
        "On this server, Claude Code models run only on your own computers. Move this workspace to one of them, "
        "or pick another model."
    ),
    "policy_blocked": POLICY_BLOCKED,
    "needs_perl": NEEDS_PERL,
    "computer_unsupported": MUSL_UNSUPPORTED,
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


#: The turn failures admission would have refused the turn for, by the
#: reason it gives: Anthropic refused the setup token, the launcher found
#: an organization's policy installed where the CLI would run, or a computer
#: with no perl or on musl, or the turn's workspace is not on an own computer
#: where the backend runs none.
_ADMISSION_REASON = {
    "auth_failed": "signed_out", "policy_blocked": "policy_blocked", "needs_own_computer": "needs_own_computer",
    "needs_perl": "needs_perl", "computer_unsupported": "computer_unsupported",
}


def claude_code_refusal(exc: BaseException, model_name: str | None) -> dict[str, Any] | None:
    """The refusal a turn ends with when it failed, after admission let it
    run, for what admission refuses a turn for, worded as admission would
    have refused it; None for any other failure."""
    for current in iter_exception_chain(exc):
        if isinstance(current, ClaudeTurnFailed) and current.kind in _ADMISSION_REASON:
            return _refusal(model_name, _ADMISSION_REASON[current.kind])
    return None


async def require_runnable(config, model: str, role: AgentRole, *, user_id: str, workspace_id: str | None) -> None:
    """Refuse a turn on ``model``, a Claude Code row, that cannot run it
    where it would run.

    A workspace on the user's own computer runs the CLI on that machine, on
    any server. Anywhere else the CLI runs on the backend, which only a
    self-hosted server offers, and only where no organization's policy is
    installed. Neither waits on Anthropic or dials a machine.
    """
    from src.server.database.computer import get_computer_for_workspace

    if not rows_available():
        raise_claude_code_off(model, "server")
    if not config.feature_enabled(FEATURE):
        raise_claude_code_off(model, "account")
    if role == "chief_of_staff":
        raise_claude_code_off(model, "chief_of_staff")
    computer = await get_computer_for_workspace(workspace_id) if workspace_id else None
    if is_own_computer(computer):
        await require_computer_signed_in(model, user_id, str(computer["computer_id"]))
        return
    if not backend_available():
        raise_claude_code_off(model, "needs_own_computer")
    if await local_managed():
        raise_claude_code_off(model, "policy_blocked")
    await require_signed_in(model, user_id)


async def require_signed_in(model: str, user_id: str) -> None:
    """Refuse a turn on the backend whose token is known not to work: none
    saved, or one the status check found refused.

    A token not checked yet runs, so admission never waits on Anthropic; one
    refused there ends the turn with this same message.
    """
    if await usable_token(user_id) is None:
        raise_claude_code_off(model, "signed_out")


async def require_computer_signed_in(model: str, user_id: str, computer_id: str) -> None:
    """Refuse a turn on an own computer whose fresh answer reads, as Settings
    shows it (``computer_state``), as one that cannot run there: an
    organization's policy is installed there, the machine has no
    perl or runs musl, or it has no usable
    token (Claude refused the machine's, or it has none and nothing can be
    relayed). A relayed token not known to be refused runs, as on the backend.

    Never dials the machine: a computer not checked lately runs, and one
    refused there ends the turn with this same message.
    """
    answer = await read_answer(computer_id)
    if answer is None or not answer.fresh:
        return
    relayed = answer.relays and backend_available() and await usable_token(user_id) is not None
    state = computer_state(answer, "signed_in" if relayed else None)
    if state in ("policy_blocked", "needs_perl", "computer_unsupported"):
        raise_claude_code_off(model, state)
    if state in ("rejected", "no_token"):
        raise_claude_code_off(model, "signed_out")


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
