"""Where a user's Claude Code stands as a whole, for its entry under Connect a
subscription: one answer, read whether they turned it on or not, so the entry
says what is missing instead of showing only once something works."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from ptc_agent.core.claude_code_managed import local_managed
from src.llms.claude_code import FEATURE, backend_available, rows_available
from src.server.database.computer import get_computers_for_user
from src.server.models.claude_code import (
    ClaudeCodeCloudComputer, ClaudeCodeComputer, ClaudeCodeCustody, ClaudeCodeSetup, ClaudeCodeStatus, SetupState,
)
from src.config.features import USER_CONTROLLABLE_GATES, system_flag
from src.server.services.features import user_feature_enabled

from . import hosted, saved
from .machine import list_computers

_WORKS = frozenset({"signed_in", "unverified"})
#: The verdicts for cloud computers the user can act on from the entry: a
#: choice of local, or an API key missing or set to another endpoint. A
#: choice paused on the server, or storing blocked for them, is one only
#: where another mode is offered instead.
_FIXABLE = frozenset({"hosted_off", "needs_api_key", "api_key_endpoint"})


def _effective(state: str, expires_at: datetime | None, now: datetime) -> str:
    """A token that works by its last check but is past its expiry is refused by now."""
    return "rejected" if state in _WORKS and expires_at is not None and expires_at <= now else state


def summary(
    computers: list[ClaudeCodeComputer], status: ClaudeCodeStatus, *, api_key_runs: bool, cloud_ready: bool,
    now: datetime,
) -> SetupState:
    """The first state that holds over every way the user's Claude Code runs:
    their own computers, the saved token where it signs turns in (``status``
    not ``off``), their Anthropic key on cloud computers (``api_key_runs``),
    and whether a cloud computer would run it once they act (``cloud_ready``)."""
    states = [_effective(c.state, c.expires_at, now) for c in computers]
    if status.state != "off":
        states.append(_effective(status.state, status.expires_at, now))
    if api_key_runs or any(s in _WORKS for s in states):
        return "connected"
    if "checking" in states:
        return "checking"
    if "rejected" in states:
        return "refused"
    if cloud_ready or any(s in ("no_token", "off") for s in states):
        return "not_connected"
    if "offline" in states:
        return "offline"
    return "unsupported" if computers or status.state == "policy_blocked" else "no_computer"


def _cloud_ready(custody: ClaudeCodeCustody | None) -> bool:
    """Whether a cloud computer runs it, or would once the user acts in the entry."""
    if custody is None:
        return False
    refusal = custody.refusal
    return custody.runs_on is not None or refusal in _FIXABLE or (
        refusal == "stored_blocked" and "api_key" in custody.offered
    ) or (refusal == "hosted_paused" and bool(custody.offered))


async def setup_state(user_id: str) -> ClaudeCodeSetup | None:
    """None where this server offers no Claude Code at all, or offers it
    only to others: the operator's gate (a plan, or none) keeps it off for
    this user, who could not turn it on."""
    if not rows_available():
        return None
    on = await user_feature_enabled(user_id, FEATURE)
    if not on and system_flag(FEATURE).gate not in USER_CONTROLLABLE_GATES:
        return None
    computers, status, refusals = await asyncio.gather(
        list_computers(user_id, mark_offline=True), saved.status(user_id), hosted.cloud_refusals(user_id),
    )
    custody = await hosted.custody(user_id)
    # Before opt-in, whether the server would run turns on a saved token:
    # not where an organization's policy is installed on its machine.
    backend_runs = not on and backend_available() and not await local_managed()
    state = summary(
        computers.computers, status, api_key_runs=on and custody is not None and custody.runs_on == "api_key",
        cloud_ready=_cloud_ready(custody) or backend_runs, now=datetime.now(timezone.utc),
    )
    saved_token_runs = status.state != "off" if on else backend_runs
    return ClaudeCodeSetup(
        state=state, on=on, computers=computers.computers, saved_token_runs=saved_token_runs,
        cloud=custody is not None and bool(custody.offered) and custody.refusal != "needs_own_computer",
        cli_in_sandbox=hosted.in_sandbox(),
        too_small=[
            ClaudeCodeCloudComputer(computer_id=str(c["computer_id"]), name=c["name"])
            for c in (await get_computers_for_user(user_id) if refusals else [])
            if str(c["computer_id"]) in refusals
        ],
    )
