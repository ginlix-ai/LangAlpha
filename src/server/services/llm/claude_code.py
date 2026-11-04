"""Where a Claude Code row runs: only as an analyst turn's own model.

A role, a fallback, a subagent pin and a Background job each send their
request through a chat client, which such a row does not build
(``src.llms.claude_code``), and Chief of Staff does not run on the runtime
yet. ``resolve_llm_config`` decides once whether the turn is a Claude Code
turn; everything else asks this module.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, NamedTuple, NoReturn

from ptc_agent.core.claude_code_managed import local_managed
from ptc_agent.core.sandbox.providers.ssh import MUSL_UNSUPPORTED
from src.llms.claude_code import (
    FEATURE,
    ClaudeTurnFailed,
    another_model_replied_last,
    is_claude_code_model,
    rows_available,
)
from src.llms.error_classification import iter_exception_chain
from src.config.settings import get_claude_code_config
from src.server.database.claude_code_hosted import STORED_TURN, WORKER_TURN, live_worker_turns
from src.server.services.claude_code.flow import POLICY_BLOCKED
from src.server.services.claude_code.hosted import hosted, placement, relay_offered
from src.server.services.claude_code.machine import NEEDS_PERL, MachineAnswer, computer_state, read_answer
from src.server.services.claude_code.saved import saved_refusal, usable_token

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
    "token_refused": (
        "Claude refused your setup token. It may have expired or been revoked. Add a new one in Settings under "
        "Model access to use this model."
    ),
    "needs_own_computer": (
        "On this server, Claude Code models run only on your own computers. Move this workspace to one of them, "
        "or pick another model."
    ),
    "policy_blocked": POLICY_BLOCKED,
    "needs_perl": NEEDS_PERL,
    "computer_unsupported": MUSL_UNSUPPORTED,
    "hosted_off": (
        "Claude Code runs only on your own computers for your account, so workspaces on cloud computers "
        "can't use Claude Code models. Choose where it runs in Settings under Model access, or move this "
        "workspace to one of your computers."
    ),
    "needs_api_key": (
        "For workspaces on cloud computers, LangAlpha runs Claude Code on its servers with your Anthropic API "
        "key, and you haven't saved one. Add one in Settings under Model access, or move this workspace to one of "
        "your computers."
    ),
    "api_key_refused": (
        "Anthropic refused your API key, or its account has no credit left. Check the key in Settings under "
        "Model access."
    ),
    "api_key_endpoint": (
        "Claude Code on cloud computers sends your Anthropic API key only to Anthropic, and yours is set to a "
        "custom Base URL. Clear the Base URL in Settings under Model access, or move this workspace to one of "
        "your computers."
    ),
    "busy": "Claude Code is busy on this server right now. Try again in a minute.",
    "too_many_turns": (
        "You've reached the limit on Claude Code threads running at once on cloud computers ({limit}). Try again "
        "when one finishes."
    ),
    "hosted_paused": (
        "Claude Code on cloud computers is paused on this server. It still runs on your own computers. Pick another "
        "model, or move this workspace to one of them."
    ),
    "stored_blocked": (
        "Storing a setup token isn't available for your account, so Claude Code runs only on your own computers. "
        "Pick another model, or move this workspace to one of them."
    ),
    "stored_unattended": (
        "Automations, wakeups and turns another agent sends don't run on the setup token LangAlpha stores for you. "
        "Choose your Anthropic API key in Settings under Model access, or run this workspace on a computer of "
        "yours with its own token."
    ),
    "choice_changed": (
        "How Claude Code runs on cloud computers changed as this turn started. Send your message again."
    ),
}


def raise_claude_code_off(model_name: str, reason: str, **fields: Any) -> NoReturn:
    """Raise the 400 for a Claude Code row this turn cannot run, ``reason``
    keying ``CLAUDE_CODE_OFF``, whose message ``fields`` fill and the
    detail carries beside it.

    Not ``model_removed``: the row is in the catalog, so nothing saved is
    cleared. Nor one of the refusals a service turn answers by running the
    account default, since that would move the turn to another model.
    """
    from fastapi import HTTPException

    raise HTTPException(status_code=400, detail=_refusal(model_name, reason, **fields))


def _refusal(model_name: str | None, reason: str, **fields: Any) -> dict[str, Any]:
    message = CLAUDE_CODE_OFF[reason].format(**fields) if fields else CLAUDE_CODE_OFF[reason]
    return {"message": message, "type": "claude_code_off", "model": model_name, "reason": reason, **fields}


#: The turn failures admission would have refused the turn for, by the
#: reason it gives: Anthropic refused the setup token (the saved one, which
#: admission refuses from then on, or a machine's), the launcher found
#: an organization's policy installed where the CLI would run, or a computer
#: with no perl or on musl, or the turn's workspace is not on an own computer
#: where the backend runs none, or the user's choice for cloud computers
#: changed since admission. Two only a run meets say so the same way:
#: Anthropic refused the user's API key, and the worker had no slot free.
_ADMISSION_REASON = {
    "auth_failed": "signed_out", "token_refused": "token_refused", "policy_blocked": "policy_blocked",
    "needs_own_computer": "needs_own_computer", "needs_perl": "needs_perl", "computer_unsupported": "computer_unsupported",
    "hosted_off": "hosted_off", "needs_api_key": "needs_api_key", "api_key_endpoint": "api_key_endpoint",
    "api_key_refused": "api_key_refused", "busy": "busy", "hosted_paused": "hosted_paused",
    "stored_blocked": "stored_blocked", "choice_changed": "choice_changed",
}


def claude_code_refusal(exc: BaseException, model_name: str | None) -> dict[str, Any] | None:
    """The refusal a turn ends with when it failed, after admission let it
    run, for what admission refuses a turn for, worded as admission would
    have refused it; None for any other failure."""
    for current in iter_exception_chain(exc):
        if isinstance(current, ClaudeTurnFailed) and current.kind in _ADMISSION_REASON:
            return _refusal(model_name, _ADMISSION_REASON[current.kind])
    return None


class Admitted(NamedTuple):
    """What admission let a Claude Code turn run on: ``hosted``, its credential
    on a cloud computer (``Hosted.runs_on``, None on an own computer), and
    ``saved``, which setup token the user saved it may sign in with, there or
    relayed to an own computer (``saved_token`` or ``stored_token``, as
    ``Hosted.runs_on`` names them), None where it may sign in with none."""

    hosted: str | None = None
    saved: str | None = None


async def require_runnable(
    config, model: str, role: AgentRole, *, user_id: str, workspace_id: str | None, thread_id: str | None = None,
) -> Admitted:
    """Refuse a turn on ``model``, a Claude Code row, that cannot run it
    where it would run.

    A workspace on the user's own computer runs the CLI on that machine, on
    any server. Anywhere else the CLI runs on the backend, on the credential
    the user's turns on a cloud computer run on (``hosted``), and only where
    no organization's policy is installed; a setup token only while it is
    usable, and on the platform each also waits for a turn of the user's
    there to finish once they run ``claude_code.hosted_turns_per_user`` at
    once, the thread's own aside, since a message to a running thread steers
    it. The cap is soft: two turns admitted at once may both pass. Neither
    waits on Anthropic or dials a machine.

    Returns what the turn was admitted on, which the run's row is stamped
    from (``run_stamp``) and the run signs in with.
    """
    if not rows_available():
        raise_claude_code_off(model, "server")
    if not config.feature_enabled(FEATURE):
        raise_claude_code_off(model, "account")
    if role == "chief_of_staff":
        raise_claude_code_off(model, "chief_of_staff")
    where = await placement(user_id, workspace_id)
    if where.own_computer is not None:
        return Admitted(saved=await require_computer_signed_in(model, user_id, str(where.own_computer["computer_id"])))
    state = where.hosted
    if state is None:
        raise_claude_code_off(model, "needs_own_computer")
    if state.refusal is not None:
        raise_claude_code_off(model, state.refusal)
    if await local_managed():
        raise_claude_code_off(model, "policy_blocked")
    if state.token_signs_in:
        await require_signed_in(model, user_id)
    if state.runs_on != "saved_token":
        limit = get_claude_code_config().hosted_turns_per_user
        if await live_worker_turns(user_id, besides=thread_id) >= limit:
            raise_claude_code_off(model, "too_many_turns", limit=limit)
    return Admitted(state.runs_on, state.runs_on if state.token_signs_in else None)


def run_stamp(config) -> dict[str, Any]:
    """What a Claude Code run's row carries on the platform, from what
    admission let it run on (``require_runnable``): counted as a turn of its
    user's on a cloud computer (``WORKER_TURN``), where the count caps them,
    and found as one that may hold the setup token they stored
    (``STORED_TURN``) when it goes."""
    from src.config.settings import HOST_MODE

    if HOST_MODE != "platform" or config is None:
        return {}
    return {
        **(WORKER_TURN if config.claude_code_hosted else {}), **(STORED_TURN if config.claude_code_saved else {}),
    }


def unattended(request) -> bool:
    """Whether nobody started the turn ``request`` asks for, or the work it
    carries on: a wakeup's or a price move's, an automation's firing, or one
    another agent sends (a Chief of Staff hand-off, a computer's ``la`` CLI).
    A report-back the server posts (``query_type`` ``system``: a command's
    end, a subagent's result) carries on a turn the user started."""
    return bool(request.wakeup or request.market_target) or (
        request.origin is not None and request.origin.type in ("automation", "agent")
    )


def require_attended(config, request) -> None:
    """Refuse a turn nobody started (``unattended``) that admission let sign in
    with the setup token the user stored, on a cloud computer or relayed to
    one of theirs: the server runs their Claude plan only for turns they
    start. Their API key and a computer's own token run such turns."""
    if "stored_token" in (config.claude_code_hosted, config.claude_code_saved) and unattended(request):
        raise_claude_code_off(config.llm.name, "stored_unattended")


async def require_signed_in(model: str, user_id: str) -> None:
    """Refuse a turn on the backend whose token is known not to work: none
    saved, or one that needs replacing (Claude refused it on a turn or a
    check, or it expired) or that the status check found refused.

    A token not checked yet runs, so admission never waits on Anthropic.
    """
    if reason := await saved_refusal(user_id):
        raise_claude_code_off(model, reason)


async def relayed_token(user_id: str, answer: MachineAnswer | None, *, conn=None) -> str | None:
    """Which setup token the user saved a turn on an own computer whose last
    answer is ``answer`` is relayed: one with none of its own, or one not
    looked at yet, while that token signs in their turns."""
    if not ((answer is None or answer.relays) and relay_offered()):
        return None
    state = await hosted(user_id, conn=conn)
    return state.runs_on if state is not None and state.token_signs_in else None


async def require_computer_signed_in(model: str, user_id: str, computer_id: str) -> str | None:
    """Refuse a turn on an own computer whose fresh answer reads, as Settings
    shows it (``computer_state``), as one that cannot run there: an
    organization's policy is installed there, the machine has no
    perl or runs musl, or it has no usable
    token (Claude refused the machine's, or it has none and nothing can be
    relayed). A relayed token not known to be refused runs, as on the backend.

    Never dials the machine: a computer not checked lately runs, and one
    refused there ends the turn with this same message. A computer last found
    to sign in with the relayed token is refused as the backend is once
    Claude refused that token, however long ago it was found so.

    Returns which token the user saved the turn may sign in with
    (``Admitted.saved``), relayed where it signs in their turns
    (``Hosted.token_signs_in``) to a machine with none of its own, or one not
    looked at yet; the launcher relays no other.
    """
    answer = await read_answer(computer_id)
    relays = await relayed_token(user_id, answer)
    if answer is None:
        return relays
    if relays and await saved_refusal(user_id) == "token_refused":
        raise_claude_code_off(model, "token_refused")
    if not answer.fresh:
        return relays
    relayed = relays is not None and await usable_token(user_id) is not None
    state = computer_state(answer, "signed_in" if relayed else None)
    if state in ("policy_blocked", "needs_perl", "computer_unsupported"):
        raise_claude_code_off(model, state)
    if state in ("rejected", "no_token"):
        raise_claude_code_off(model, "signed_out")
    return relays


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
