"""Where a Claude Code row runs: as an analyst turn's own model, and as its
subagents', on that turn or on a turn of another model in a workspace where
Claude Code runs.

A role, a fallback and a Background job each send their request through a
chat client, which such a row does not build (``src.llms.claude_code``). The
Chief of Staff runs it as an analyst does, its CLI placed by the same rule
(``cli_placement``), and runs a Claude Code Default only where that would
run (``chief_of_staff_default_runs``). ``resolve_llm_config`` decides once
whether the turn is a Claude Code turn, and whether its subagents' Claude
Code pins run; everything else asks this module.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Any, NoReturn

from ptc_agent.config.agent import Admitted
from ptc_agent.core.sandbox.providers.ssh import MUSL_UNSUPPORTED
from src.llms.claude_code import (
    FEATURE,
    ClaudeTurnFailed,
    PlanLimit,
    PlanStopped,
    PlanWindow,
    another_model_replied_last,
    binding_limit,
    is_claude_code_model,
    rows_available,
)
from src.llms.error_classification import iter_exception_chain
from src.config.settings import get_claude_code_config
from src.server.database.claude_code_hosted import (
    CLAUDE_CODE_TURN, CLI_COMPUTER, CLOUD_TURN, STORED_TURN, live_cloud_turns,
)
from src.server.services.claude_code.flow import POLICY_BLOCKED
from src.server.services.claude_code.hosted import OnOwn, Placement, Refused, cli_placement, hosted, relay_offered
from src.server.services.claude_code.machine import NEEDS_PERL, MachineAnswer, computer_state, read_answer
from src.server.database import claude_code_plan
from src.server.database.claude_code_plan import SAVED, plan_credential
from src.server.services.claude_code.plan import PlanState, plan_out
from src.server.services.claude_code.saved import saved_refusal, usable_token

from .availability import drop_models, model_resolves

logger = logging.getLogger(__name__)

#: Why a Claude Code row cannot run this turn, as the user is told it.
CLAUDE_CODE_OFF = {
    "server": "This server does not run Claude Code models. Pick another model.",
    "account": "Claude Code is off for your account. Turn it on in Settings under Model access, or pick another model.",
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
    "no_own_computer": (
        "Claude Code runs on your own computers, and none of yours with its own setup token is online now. Turn "
        "one on, or add a token for one in Settings under Model access."
    ),
    "cli_computer_offline": "{name}, where Claude Code runs for you, is offline. Turn it on, or pick another model.",
    "policy_blocked": POLICY_BLOCKED,
    "needs_perl": NEEDS_PERL,
    "computer_unsupported": MUSL_UNSUPPORTED,
    "too_many_turns": (
        "You've reached the limit on Claude Code threads running at once on cloud computers ({limit}). Try again "
        "when one finishes."
    ),
    "choice_changed": (
        "How Claude Code runs on cloud computers changed as this turn started. Send your message again."
    ),
    "needs_bigger_computer": (
        "Claude Code runs on Medium or larger cloud computers, and {where} is smaller. "
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
PLAN_WINDOW_WORDS: dict[PlanWindow, str] = {
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

#: What ``Task`` answers for a Claude Code subagent the Claude plan holds:
#: with the limit and its reset, with a reset but no window, and with none.
SUBAGENT_PLAN_REFUSAL = (
    "Not started: the Claude plan's {window} limit is used up until {time}, so Claude Code subagents can't run "
    "before then."
)
SUBAGENT_PLAN_REFUSAL_UNNAMED = (
    "Not started: the Claude plan's limit is used up until {time}, so Claude Code subagents can't run before then."
)
SUBAGENT_PLAN_REFUSAL_NO_RESET = (
    "Not started: the Claude plan's limit is used up, and Claude didn't say when it resets, so Claude Code "
    "subagents can't run before then."
)

#: The reasons the platform words otherwise, where a larger computer comes
#: with the user's LangAlpha plan and the user chooses to run Claude Code on
#: their own computers instead.
_PLATFORM_OFF = {
    "needs_bigger_computer": (
        "Claude Code runs on Medium or larger cloud computers, and {where} is smaller. Change its spec or upgrade "
        "your LangAlpha plan, or choose to run Claude Code on your own computers in Settings under Model access."
    ),
}
#: Home can't move to another computer, so its words for a computer too
#: small offer only the spec, on a self-hosted server.
_HOME_TOO_SMALL = "Claude Code runs on Medium or larger cloud computers, and {where} is smaller. Change its spec."
#: The computer a refusal for its size names: Home's, or the workspace's.
_TOO_SMALL_WHERE = {True: "Home's computer", False: "this workspace's computer"}


def claude_code_off_message(reason: str, **fields: Any) -> str:
    """The words of the ``CLAUDE_CODE_OFF`` reason ``reason`` on this server,
    filled with what they name (``fields``): for a computer too small,
    whether it is Home's (``home``), which names it."""
    from src.config.settings import HOST_MODE

    home = bool(fields.get("home"))
    words = (_PLATFORM_OFF.get(reason) if HOST_MODE == "platform" else None) or CLAUDE_CODE_OFF[reason]
    if reason == "needs_bigger_computer" and home and HOST_MODE != "platform":
        words = _HOME_TOO_SMALL
    return words.format(**fields, where=_TOO_SMALL_WHERE[home])


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
        message = claude_code_off_message(reason, **fields)
    return {"message": message, "type": "claude_code_off", "model": model_name, "reason": reason, **fields}


def plan_words(
    reason: str, window: PlanWindow | None, until: datetime | None, *, zone: str | None = None,
    credits_billed: bool = False,
) -> str:
    """A plan refusal's words, the reset in ``zone``: an automation's report
    and delivery are read by its owner, on their clock. Without one the
    reset is in UTC, for a frame the app words itself in the reader's zone.
    Where Claude said no reset (``until`` None), words that name none."""
    if window is None or until is None:
        message = _PLAN_NO_RESET[reason]
    else:
        message = claude_code_off_message(reason, window=PLAN_WINDOW_WORDS[window], time=_plan_time(until, zone))
    return f"{message} {_CREDITS_BILLED}" if credits_billed else message


def _plan_time(until: datetime, zone: str | None) -> str:
    """A reset on ``zone``'s clock: the time alone on today's date there."""
    from src.tools.wakeups.clock import user_zone_of

    if zone is None:
        return until.astimezone(UTC).strftime("%b %-d, %H:%M UTC")
    local = until.astimezone(user_zone_of(zone))
    today = local.date() == datetime.now(local.tzinfo).date()
    return local.strftime("%-I:%M %p %Z" if today else "%a %b %-d, %-I:%M %p %Z")


def plan_detail(exc: BaseException) -> dict[str, Any] | None:
    """The detail of ``exc`` where it is a Claude plan refusal: admission's
    ``plan_limited``, or the ``plan_stopped`` a run ended with."""
    from fastapi import HTTPException

    if isinstance(exc, HTTPException) and isinstance(exc.detail, dict):
        detail = exc.detail
    else:
        detail = claude_code_refusal(exc, None) or {}
    if detail.get("type") == "claude_code_off" and detail.get("reason") in ("plan_limited", "plan_stopped"):
        return detail
    return None


def reword_plan(detail: dict[str, Any], zone: str | None) -> str:
    """``detail``'s words (``plan_detail``), the reset on ``zone``'s clock."""
    until = detail.get("until")
    return plan_words(
        detail["reason"], detail.get("window"), datetime.fromisoformat(until) if until else None,
        zone=zone, credits_billed=bool(detail.get("credits_billed")),
    )


def reword_subscription_limit(detail: dict[str, Any], zone: str | None) -> str:
    """Another subscription's used-up limit (``plan_limit_detail``) in words,
    the reset on ``zone``'s clock."""
    window, until = detail.get("window"), detail.get("until")
    limit = f"{PLAN_WINDOW_WORDS[window]} limit" if window in PLAN_WINDOW_WORDS else "limit"
    message = f"Your {detail['plan']} plan's {limit} is used up."
    if until:
        message += f" It resets at {_plan_time(datetime.fromisoformat(until), zone)}."
    return message


def _plan_fields(reason: str, window: PlanWindow | None, until: datetime | None) -> dict[str, Any]:
    """A plan refusal's words from the server, for a client that words none
    itself, and the window and its reset (ISO 8601) the frame carries, which
    the app words in the reader's zone and language. Where Claude said no
    reset (``until`` None), words that name none, and neither field."""
    message = plan_words(reason, window, until)
    if window is None or until is None:
        return {"message": message}
    return {"message": message, "window": window, "until": until.isoformat()}


def plan_refusal(model_name: str | None, stopped: PlanStopped) -> dict[str, Any]:
    """The ``plan_stopped`` refusal a turn the Claude plan stopped ends with."""
    fields = _plan_fields("plan_stopped", stopped.window, stopped.until)
    fields["message"] = plan_words("plan_stopped", stopped.window, stopped.until, credits_billed=stopped.credits_billed)
    return _refusal(model_name, "plan_stopped", credits_billed=stopped.credits_billed, **fields)


#: The turn failures admission would have refused the turn for, by the
#: reason it gives: Anthropic refused the setup token (the saved one, which
#: admission refuses from then on, or a machine's), the launcher found
#: an organization's policy installed where the CLI would run, or a computer
#: with no perl or on musl, or the turn's workspace is not on an own computer
#: where the backend runs none, or the user's choice for cloud computers
#: changed since admission, or the computer is now below the tier Claude Code
#: runs on, or the own computer picked to run the CLI is offline, or another
#: model last replied on the thread, which a turn admission never saw (an
#: automation's firing, on the thread it continues) reaches only here.
_ADMISSION_REASON = {
    "auth_failed": "signed_out", "token_refused": "token_refused", "policy_blocked": "policy_blocked",
    "needs_own_computer": "needs_own_computer", "needs_perl": "needs_perl", "computer_unsupported": "computer_unsupported",
    "choice_changed": "choice_changed", "needs_bigger_computer": "needs_bigger_computer",
    "cli_computer_offline": "cli_computer_offline", "other_model_history": "other_model",
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
            return _refusal(model_name, _ADMISSION_REASON[current.kind], **current.fields)
    return None


async def require_runnable(
    config, model: str, *, user_id: str, workspace_id: str | None, thread_id: str | None = None,
    where: Placement | None = None,
) -> Admitted:
    """Refuse a turn on ``model``, a Claude Code row, that cannot run it
    where it would run.

    A workspace on the user's own computer runs the CLI on that machine, on
    any server, and so does one on a cloud computer, Home included, that runs
    none for this user, on the computer of theirs ``cli_placement`` picks.
    Anywhere else the turn is refused for the first reason
    ``cloud_placement`` gives, the saved token known not to work among
    them. Each turn there also waits for one of the user's to finish once
    they run ``claude_code.hosted_turns_per_user`` at once, the thread's own
    aside, since a message to a running thread steers it; each computer's
    own count of its CLIs, subagents' included, bounds them there
    (``hosted.cli_cap``). The cap is soft: two turns admitted at once may both
    pass. A turn whose credential's Claude plan is used up is refused until
    it resets (``require_plan``). None of it waits on Anthropic or dials a
    machine. ``where`` is the placement a caller read
    already, as ``cli_placement`` gives it with ``token``.

    Returns what the turn was admitted on, which the run's row is stamped
    from (``run_stamp``) and the run signs in with.
    """
    if not rows_available():
        raise_claude_code_off(model, "server")
    if not config.feature_enabled(FEATURE):
        raise_claude_code_off(model, "account")
    if where is None:
        where = await cli_placement(user_id, workspace_id, token=True)
    admitted = await _admitted(model, user_id, where, plan=True)
    if isinstance(where, OnOwn):
        return admitted
    limit = get_claude_code_config().hosted_turns_per_user
    if await live_cloud_turns(user_id, besides=thread_id) >= limit:
        raise_claude_code_off(model, "too_many_turns", limit=limit)
    return admitted


async def _admitted(model: str | None, user_id: str, where: Placement, *, plan: bool = False) -> Admitted:
    """What a Claude Code CLI placed at ``where`` runs on, or the 400 that
    refuses it there: its refusal, or an own computer's answer that says it
    cannot run (``require_computer_signed_in``). With ``plan``, also the
    Claude plan behind that credential once it is known (``require_plan``)."""
    if isinstance(where, Refused):
        raise_claude_code_off(model, where.reason, **where.fields)
    if isinstance(where, OnOwn):
        computer_id = str(where.computer["computer_id"])
        answer = await read_answer(computer_id)
        saved = await require_computer_signed_in(model, user_id, answer)
        if plan and answer is not None and answer.fresh:
            await require_plan(model, user_id, plan_credential("saved" if saved else "machine", computer_id))
        return Admitted(saved=saved, cli_computer=computer_id if where.picked else None)
    state = where.hosted
    if plan and state.token_signs_in:
        await require_plan(model, user_id, SAVED)
    return Admitted(state.runs_on, state.runs_on if state.token_signs_in else None)


async def admitted_there(user_id: str, where: Placement) -> Admitted | None:
    """What a Claude Code CLI placed at ``where`` is admitted on
    (``_admitted``), short of the plan; None where it is refused there, and
    where that could not be read."""
    from fastapi import HTTPException

    try:
        return await _admitted(None, user_id, where)
    except HTTPException:
        return None
    except Exception as e:  # noqa: BLE001
        logger.warning(f"[claude_code] could not read where Claude Code runs for user_id={user_id}: {type(e).__name__}")
        return None


async def claude_code_placement(config, *, user_id: str, workspace_id: str | None) -> Admitted | None:
    """What a Claude Code CLI the user starts in ``workspace_id`` would run on,
    read as admission reads it (``require_runnable``): where ``cli_placement``
    puts it, an own computer the server picked included, on that computer's
    answer. None where Claude Code runs nowhere there for them now, and where
    it could not be read (``admitted_there``).

    Asked at turn start for a subagent's Claude Code pin on a turn of another
    model, which runs on the turn's model where this is None. Neither the
    Claude plan nor the cap on the user's cloud turns is asked: a pin's run is
    stopped by its plan as it starts, and bounded by the count of CLIs on the
    computer that runs it, as a Claude Code turn's subagent is.
    """
    if not rows_available() or not config.feature_enabled(FEATURE):
        return None
    try:
        where = await cli_placement(user_id, workspace_id, token=True)
    except Exception as e:  # noqa: BLE001
        logger.warning(f"[claude_code] could not read where Claude Code runs for user_id={user_id}: {type(e).__name__}")
        return None
    return await admitted_there(user_id, where)


async def default_runs(user_id: str, where: Placement) -> bool:
    """Whether the Chief of Staff runs a Claude Code Default the user saved
    where admission places its turn (``where``, read with the saved token):
    its CLI is admitted there (``admitted_there``), the one rule for Home on
    a cloud computer and on one of the user's own."""
    return await admitted_there(user_id, where) is not None


async def chief_of_staff_default_runs(
    user_id: str, workspace_id: str | None, *, thread_id: str | None = None, checkpoint_id: str | None = None,
    conn=None,
) -> Placement | None:
    """Where the Chief of Staff's turn in ``workspace_id`` runs the Claude Code
    Default the user saved rather than the deployment's, None where it does
    not: the thread ``thread_id`` holds a reply from another model where the
    turn starts (``checkpoint_id`` for an edit or a regenerate, as
    ``require_claude_code_continues`` reads it, else its head), or by
    where admission would place it (``cli_placement`` with the token) its CLI
    has nowhere to run, or runs where the answer there says it cannot
    (``admitted_there``). One rule wherever Home
    is, on a cloud computer or one of the user's own. Short of what only
    admission reads (the plan). The caller checks first that this server
    runs Claude Code and the user has it on."""
    if thread_id and await another_model_replied(thread_id, checkpoint_id):
        return None
    where = await cli_placement(user_id, workspace_id, conn=conn, token=True)
    return where if await default_runs(user_id, where) else None


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
    model_id = _model_id(model)
    if (limit := await plan_hold(model_id, user_id, credential)) is None:
        return
    until = limit.until if limit.known else None
    raise_claude_code_off(
        model, "plan_limited", held_until=limit.until.isoformat(), held_known=limit.known,
        held_credential=credential, held_model=model_id,
        **_plan_fields("plan_limited", limit.window, until),
    )


async def plan_hold(model_id: str, user_id: str, credential: str) -> PlanLimit | None:
    """The limit of ``credential``'s Claude plan that holds a run on
    ``model_id`` now, as admission refuses a turn for it (``require_plan``)
    and ``Task`` a Claude Code subagent; None where none holds, or the row
    can't be read."""
    if (state := await _plan_state(user_id, credential)) is None:
        return None
    if (limit := binding_limit(state.held(), model_id)) is None:
        return None
    try:
        out = await plan_out(user_id) if state.credits_value != "off" else "stop"
    except Exception as e:  # noqa: BLE001
        logger.warning(f"[claude_code] could not read the plan choice for user_id={user_id}: {type(e).__name__}")
        out = "stop"
    return None if out == "credits" else limit


async def plan_still_holds(model_id: str, user_id: str, credential: str) -> bool:
    """Whether ``credential``'s Claude plan still holds a run on ``model_id``
    back, whatever the usage credits choice: a wake the plan held that ran on
    credits, or under a limit that outlasted the one it waited for, did not
    wait out a reset. A row that can't be read counts as holding."""
    state = await _plan_state(user_id, credential)
    return state is None or binding_limit(state.held(), model_id) is not None


async def _plan_state(user_id: str, credential: str) -> PlanState | None:
    try:
        row = (await claude_code_plan.get_plans(user_id, [credential])).get(credential)
    except Exception as e:  # noqa: BLE001
        logger.warning(f"[claude_code] could not read the plan state for user_id={user_id}: {type(e).__name__}")
        return None
    return PlanState.of((row or {}).get("state"))


async def subagent_plan_refusal(computer: dict[str, Any], model_id: str, saved: bool) -> str | None:
    """What ``Task`` answers where the Claude plan behind the credential a
    Claude Code subagent on ``model_id`` would sign in with is used up for
    it, under Stop; None where it would run. The credential is the one its
    turn's admission read (``require_runnable``): on the user's own
    computer, the saved token where admission relayed it (``saved``) and
    the machine still has none of its own, else the computer's own, once the
    computer's answer says which; on a cloud
    computer, the saved token it signs in with.
    The reset is on the user's clock, which the agent tells them."""
    from src.server.database.automation import get_user_timezone
    from src.server.services.ssh_computers import is_own_computer

    user_id, computer_id = str(computer["user_id"]), str(computer["computer_id"])
    if is_own_computer(computer):
        answer = await read_answer(computer_id)
        if answer is None or not answer.fresh:
            return None
        credential = plan_credential("saved" if saved and answer.relays else "machine", computer_id)
    elif saved:
        credential = SAVED
    else:
        return None
    if (limit := await plan_hold(model_id, user_id, credential)) is None:
        return None
    if not limit.known:
        return SUBAGENT_PLAN_REFUSAL_NO_RESET
    try:
        zone = await get_user_timezone(user_id)
    except Exception:  # noqa: BLE001
        zone = None
    time = _plan_time(limit.until, zone)
    if limit.window is None:
        return SUBAGENT_PLAN_REFUSAL_UNNAMED.format(time=time)
    return SUBAGENT_PLAN_REFUSAL.format(window=PLAN_WINDOW_WORDS[limit.window], time=time)


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
    (``STORED_TURN``) when it goes, and naming the own computer that runs its
    CLI for a workspace on another (``CLI_COMPUTER``), which counts it in
    the workspace's computer's place."""
    from src.config.settings import HOST_MODE

    if config is None:
        return {}
    admitted = config.claude_code_admitted or Admitted()
    return {
        **(CLAUDE_CODE_TURN if config.claude_code else {}),
        **(CLOUD_TURN if admitted.hosted else {}),
        **(STORED_TURN if HOST_MODE == "platform" and admitted.saved else {}),
        **({CLI_COMPUTER: admitted.cli_computer} if admitted.cli_computer else {}),
    }


async def relayed_token(user_id: str, answer: MachineAnswer | None, *, conn=None) -> str | None:
    """Which setup token the user saved a turn on an own computer whose last
    answer is ``answer`` is relayed: one with none of its own, or one not
    looked at yet, while that token signs in their turns."""
    if not ((answer is None or answer.relays) and relay_offered()):
        return None
    state = await hosted(user_id, conn=conn)
    return state.runs_on if state is not None and state.token_signs_in else None


async def require_computer_signed_in(model: str | None, user_id: str, answer: MachineAnswer | None) -> str | None:
    """Refuse a turn on an own computer whose fresh answer reads, as Settings
    shows it (``computer_state``), as one that cannot run there: an
    organization's policy is installed there, the machine has no
    perl or runs musl, or it has no usable
    token (Claude refused the machine's, or it has none and nothing can be
    relayed). A token of the machine's past the expiry its sign-in gave it
    is refused however old the answer, as Settings reads it. A relayed token not known to be refused runs, as on the backend.

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
    if answer.expired and computer_state(answer, None) in ("signed_in", "unverified"):
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


def role_resolves(model_pref: dict, name: str, *, claude_code: bool = False) -> bool:
    """Whether a subagent pin can run ``name``: a Claude Code row only where
    Claude Code runs the turn's subagents (``claude_code``), each in a CLI
    of its own."""
    return (claude_code or not is_claude_code_model(name)) and model_resolves(model_pref, name)


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
    if is_claude_code_model(name) and await another_model_replied(thread_id, checkpoint_id):
        raise_claude_code_off(name, "other_model")


async def another_model_replied(thread_id: str, checkpoint_id: str | None = None) -> bool:
    """Whether a model other than Claude Code last replied on the thread, read
    where its turn would start (``require_claude_code_continues``)."""
    from src.server.services.history.reader import CheckpointHistoryReader
    from src.server.utils.checkpoint_helpers import get_checkpointer, turn_base_checkpoint_id

    base = checkpoint_id or await turn_base_checkpoint_id(get_checkpointer(), thread_id)
    state = await CheckpointHistoryReader.get_instance().aget_state(thread_id, base)
    messages = (state.values or {}).get("messages") or [] if state else []
    return another_model_replied_last(messages)
