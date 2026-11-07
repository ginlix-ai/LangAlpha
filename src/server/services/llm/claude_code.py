"""Where a Claude Code row runs: as an analyst turn's own model, and as its
subagents'.

A role, a fallback, a Background job and a subagent pin on any other turn
each send their request through a chat client, which such a row does not
build (``src.llms.claude_code``), and Chief of Staff does not run on the
runtime yet. ``resolve_llm_config`` decides once whether the turn is a Claude Code
turn; everything else asks this module.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, NamedTuple, NoReturn

from ptc_agent.core.sandbox.providers.ssh import MUSL_UNSUPPORTED
from src.llms.claude_code import (
    FEATURE,
    ClaudeTurnFailed,
    PlanStopped,
    PlanWindow,
    another_model_replied_last,
    binding_limit,
    is_claude_code_model,
    rows_available,
)
from src.llms.error_classification import iter_exception_chain
from src.config.settings import get_claude_code_config
from src.server.database.claude_code_hosted import CLAUDE_CODE_TURN, CLOUD_TURN, STORED_TURN, live_cloud_turns
from src.server.services.claude_code.flow import POLICY_BLOCKED
from src.server.services.claude_code.hosted import hosted, placement, relay_offered
from src.server.services.claude_code.machine import NEEDS_PERL, MachineAnswer, computer_state, read_answer
from src.server.database import claude_code_plan
from src.server.database.claude_code_plan import SAVED, plan_credential
from src.server.services.claude_code.plan import PlanState, plan_out
from src.server.services.claude_code.saved import saved_refusal, usable_token

from .availability import drop_models, model_resolves

if TYPE_CHECKING:
    from ptc_agent.agent.roles import AgentRole

logger = logging.getLogger(__name__)

#: Why a Claude Code row cannot run this turn, as the user is told it.
CLAUDE_CODE_OFF = {
    "server": "This server does not run Claude Code models. Pick another model.",
    "account": "Claude Code is off for your account. Turn it on in Settings under Model access, or pick another model.",
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
        "On cloud computers, Claude Code runs on the computer itself with your Anthropic API key, and you haven't "
        "saved one. Add one in Settings under Model access, or move this workspace to one of your computers."
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
    "needs_bigger_computer": (
        "Claude Code runs on Medium or larger cloud computers, and this workspace's computer is smaller. "
        "Change its spec, or move this workspace to a computer of your own."
    ),
    "plan_stopped": (
        "Your Claude plan's {window} limit is used up, so this turn stopped. It resets at {time}. "
        "Nothing ran on another model."
    ),
    "plan_limited": (
        "Your Claude plan's {window} limit is used up until {time}. Pick another model, or send this again after "
        "it resets."
    ),
}

#: The windows as the server's own words name them.
_PLAN_WINDOW_WORDS: dict[PlanWindow, str] = {
    "five_hour": "5-hour", "seven_day": "weekly", "seven_day_opus": "weekly Opus", "seven_day_sonnet": "weekly Sonnet",
    "seven_day_overage_included": "weekly Fable",
}
#: A plan refusal's words where Claude said no reset, by reason.
_PLAN_NO_RESET = {
    "plan_stopped": "Your Claude plan's limit is used up, so this turn stopped. Nothing ran on another model.",
    "plan_limited": (
        "Your Claude plan's limit is used up, and Claude didn't say when it resets. Pick another model, or send "
        "this again later."
    ),
}
#: Added to ``plan_stopped`` where usage credits paid for requests first.
_CREDITS_BILLED = "Requests already under way were billed to your usage credits."

#: The reasons the platform words otherwise, where a larger computer comes
#: with the user's LangAlpha plan.
_PLATFORM_OFF = {
    "needs_bigger_computer": (
        "Claude Code runs on Medium or larger cloud computers, and this workspace's computer is smaller. Change "
        "its spec or upgrade your LangAlpha plan, or move this workspace to a computer of your own."
    ),
}


def claude_code_off_message(reason: str) -> str:
    """The words of the ``CLAUDE_CODE_OFF`` reason ``reason`` on this server."""
    from src.config.settings import HOST_MODE

    return (_PLATFORM_OFF.get(reason) if HOST_MODE == "platform" else None) or CLAUDE_CODE_OFF[reason]


def raise_claude_code_off(model_name: str, reason: str, *, message: str | None = None, **fields: Any) -> NoReturn:
    """Raise the 400 for a Claude Code row this turn cannot run, ``reason``
    keying ``CLAUDE_CODE_OFF``, whose message ``fields`` fill, unless
    ``message`` words it, and the detail carries beside it.

    Not ``model_removed``: the row is in the catalog, so nothing saved is
    cleared. Nor one of the refusals a service turn answers by running the
    account default, since that would move the turn to another model.
    """
    from fastapi import HTTPException

    raise HTTPException(status_code=400, detail=_refusal(model_name, reason, message=message, **fields))


def _refusal(model_name: str | None, reason: str, *, message: str | None = None, **fields: Any) -> dict[str, Any]:
    if message is None:
        words = claude_code_off_message(reason)
        message = words.format(**fields) if fields else words
    return {"message": message, "type": "claude_code_off", "model": model_name, "reason": reason, **fields}


def _plan_fields(reason: str, window: PlanWindow | None, until: datetime | None) -> dict[str, Any]:
    """A plan refusal's words from the server, for a client that words none
    itself, and the window and its reset (ISO 8601) the frame carries, which
    the app words in the reader's zone and language, so the reset here is in
    UTC, since the server does not know the reader's zone. Where Claude said
    no reset (``until`` None), words that name none, and neither field."""
    if window is None or until is None:
        return {"message": _PLAN_NO_RESET[reason]}
    time = until.astimezone(UTC).strftime("%b %-d, %H:%M UTC")
    message = claude_code_off_message(reason).format(window=_PLAN_WINDOW_WORDS[window], time=time)
    return {"message": message, "window": window, "until": until.isoformat()}


def plan_refusal(model_name: str | None, stopped: PlanStopped) -> dict[str, Any]:
    """The ``plan_stopped`` refusal a turn the Claude plan stopped ends with."""
    fields = _plan_fields("plan_stopped", stopped.window, stopped.until)
    message = fields.pop("message")
    return _refusal(
        model_name, "plan_stopped", message=f"{message} {_CREDITS_BILLED}" if stopped.credits_billed else message,
        credits_billed=stopped.credits_billed, **fields,
    )


#: The turn failures admission would have refused the turn for, by the
#: reason it gives: Anthropic refused the setup token (the saved one, which
#: admission refuses from then on, or a machine's), the launcher found
#: an organization's policy installed where the CLI would run, or a computer
#: with no perl or on musl, or the turn's workspace is not on an own computer
#: where the backend runs none, or the user's choice for cloud computers
#: changed since admission, or the computer is now below the tier Claude Code
#: runs on. One only a run meets says so the same way: Anthropic refused the
#: user's API key.
_ADMISSION_REASON = {
    "auth_failed": "signed_out", "token_refused": "token_refused", "policy_blocked": "policy_blocked",
    "needs_own_computer": "needs_own_computer", "needs_perl": "needs_perl", "computer_unsupported": "computer_unsupported",
    "hosted_off": "hosted_off", "needs_api_key": "needs_api_key", "api_key_endpoint": "api_key_endpoint",
    "api_key_refused": "api_key_refused", "hosted_paused": "hosted_paused", "stored_blocked": "stored_blocked",
    "choice_changed": "choice_changed", "needs_bigger_computer": "needs_bigger_computer",
}


def claude_code_refusal(exc: BaseException, model_name: str | None) -> dict[str, Any] | None:
    """The refusal a turn ends with when it failed, after admission let it
    run, for what admission refuses a turn for, worded as admission would
    have refused it, and the Claude plan's stop (``plan_stopped``), which
    names the window and its reset as admission's ``plan_limited`` does;
    None for any other failure."""
    for current in iter_exception_chain(exc):
        if isinstance(current, PlanStopped):
            return plan_refusal(model_name, current)
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
    any server. Anywhere else the turn is refused for the first reason
    ``cloud_placement`` gives, the saved token known not to work among
    them. Each turn there also waits for one of the user's to finish once
    they run ``claude_code.hosted_turns_per_user`` at once, the thread's own
    aside, since a message to a running thread steers it; each computer's
    own count of its CLIs, subagents' included, bounds them there
    (``hosted.cli_cap``). The cap is soft: two turns admitted at once may both
    pass. A turn whose credential's Claude plan is used up is refused until
    it resets (``require_plan``). None of it waits on Anthropic or dials a
    machine.

    Returns what the turn was admitted on, which the run's row is stamped
    from (``run_stamp``) and the run signs in with.
    """
    if not rows_available():
        raise_claude_code_off(model, "server")
    if not config.feature_enabled(FEATURE):
        raise_claude_code_off(model, "account")
    if role == "chief_of_staff":
        raise_claude_code_off(model, "chief_of_staff")
    where = await placement(user_id, workspace_id, token=True)
    if where.own_computer is not None:
        computer_id = str(where.own_computer["computer_id"])
        answer = await read_answer(computer_id)
        saved = await require_computer_signed_in(model, user_id, answer)
        if answer is not None and answer.fresh:
            await require_plan(model, user_id, plan_credential("saved" if saved else "machine", computer_id))
        return Admitted(saved=saved)
    if where.refusal is not None:
        raise_claude_code_off(model, where.refusal)
    state = where.hosted
    if state.token_signs_in:
        await require_plan(model, user_id, SAVED)
    limit = get_claude_code_config().hosted_turns_per_user
    if await live_cloud_turns(user_id, besides=thread_id) >= limit:
        raise_claude_code_off(model, "too_many_turns", limit=limit)
    return Admitted(state.runs_on, state.runs_on if state.token_signs_in else None)


async def require_plan(model: str, user_id: str, credential: str) -> None:
    """Refuse a turn on ``model`` whose credential's Claude plan is used up
    for it until a time still ahead (``plan_limited``, naming the limit
    that lasts longest), before any CLI starts: a weekly family limit
    refuses only that family's rows. Under Stop the turn is refused
    whatever the account's usage credits; under Use usage credits only where
    those are refused too, since it would run on them otherwise. Reads one
    row, never Anthropic; a row that can't be read refuses nothing, since the
    run's gate still stops it before its first request, and a choice that
    can't be read is Stop, as that gate reads it.

    An own computer whose answer is not known yet, or no longer fresh, is
    not asked: whether its turn signs in with the computer's own token or
    the saved one is known only once the CLI starts, where that gate reads
    the right row.
    """
    try:
        row = (await claude_code_plan.get_plans(user_id, [credential])).get(credential)
    except Exception as e:  # noqa: BLE001
        logger.warning(f"[claude_code] could not read the plan state at admission for user_id={user_id}: {type(e).__name__}")
        return
    state = PlanState.of((row or {}).get("state"))
    if (limit := binding_limit(state.held(), _model_id(model))) is None:
        return
    try:
        out = await plan_out(user_id) if state.credits_value != "off" else "stop"
    except Exception as e:  # noqa: BLE001
        logger.warning(f"[claude_code] could not read the plan choice at admission for user_id={user_id}: {type(e).__name__}")
        out = "stop"
    if out == "credits":
        return
    until = limit.until if limit.known else None
    raise_claude_code_off(model, "plan_limited", **_plan_fields("plan_limited", limit.window, until))


def _model_id(model: str) -> str:
    """The model a catalog row runs, by its API id, which names its family."""
    from src.llms.llm import LLM

    entry = LLM.get_model_config().get_model_config(model) or {}
    return entry.get("model_id") or model


def run_stamp(config) -> dict[str, Any]:
    """What a Claude Code run's row carries, from what admission let it run
    on (``require_runnable``): counted as a CLI its computer runs
    (``CLAUDE_CODE_TURN``), as a turn of its user's on a cloud
    computer (``CLOUD_TURN``) on any server, where the count caps them, and
    on the platform found as one that may hold the setup token they stored
    (``STORED_TURN``) when it goes."""
    from src.config.settings import HOST_MODE

    if config is None:
        return {}
    return {
        **(CLAUDE_CODE_TURN if config.claude_code else {}),
        **(CLOUD_TURN if config.claude_code_hosted else {}),
        **(STORED_TURN if HOST_MODE == "platform" and config.claude_code_saved else {}),
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


async def relayed_token(user_id: str, answer: MachineAnswer | None, *, conn=None) -> str | None:
    """Which setup token the user saved a turn on an own computer whose last
    answer is ``answer`` is relayed: one with none of its own, or one not
    looked at yet, while that token signs in their turns."""
    if not ((answer is None or answer.relays) and relay_offered()):
        return None
    state = await hosted(user_id, conn=conn)
    return state.runs_on if state is not None and state.token_signs_in else None


async def require_computer_signed_in(model: str, user_id: str, answer: MachineAnswer | None) -> str | None:
    """Refuse a turn on an own computer whose fresh answer reads, as Settings
    shows it (``computer_state``), as one that cannot run there: an
    organization's policy is installed there, the machine has no
    perl or runs musl, or it has no usable
    token (Claude refused the machine's, or it has none and nothing can be
    relayed). A relayed token not known to be refused runs, as on the backend.

    ``answer`` is the computer's last (``read_answer``). Never dials the
    machine: a computer not checked lately runs, and one
    refused there ends the turn with this same message. A computer last found
    to sign in with the relayed token is refused as the backend is once
    Claude refused that token, however long ago it was found so.

    Returns which token the user saved the turn may sign in with
    (``Admitted.saved``), relayed where it signs in their turns
    (``Hosted.token_signs_in``) to a machine with none of its own, or one not
    looked at yet; the launcher relays no other.
    """
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
    if state == "rejected":
        raise_claude_code_off(model, "token_refused")
    if state == "no_token":
        raise_claude_code_off(model, "signed_out")
    return relays


def role_resolves(model_pref: dict, name: str, *, claude_code_turn: bool = False) -> bool:
    """Whether a role, a fallback or a subagent pin can run ``name``: a
    Claude Code row only as a subagent of a Claude Code turn
    (``claude_code_turn``), whose subagents run CLIs of their own."""
    return (claude_code_turn or not is_claude_code_model(name)) and model_resolves(model_pref, name)


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
