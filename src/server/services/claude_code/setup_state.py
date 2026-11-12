"""Where a user's Claude Code stands as a whole, for its entry under Connect a
subscription: one answer, read whether they turned it on or not, so the entry
says what is missing instead of showing only once something works."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Literal

from ptc_agent.core.claude_code_managed import local_managed
from src.llms.claude_code import FEATURE, backend_available, rows_available
from src.server.database.computer import get_computers_for_user
from src.server.models.claude_code import (
    ClaudeCodeChiefOfStaff, ClaudeCodeCloudComputer, ClaudeCodeComputer, ClaudeCodeCustody, ClaudeCodeSetup,
    ClaudeCodeStatus, SetupState,
)
from src.config.features import USER_CONTROLLABLE_GATES, system_flag
from src.server.services.features import user_feature_enabled

from . import hosted, saved
from .machine import list_computers

_WORKS = frozenset({"signed_in", "unverified"})


def _effective(state: str, expires_at: datetime | None, now: datetime) -> str:
    """A token that works by its last check but is past its expiry is refused by now."""
    return "rejected" if state in _WORKS and expires_at is not None and expires_at <= now else state


#: Where a cloud computer stands for the user's Claude Code (``_cloud``):
#: ``ready`` runs it, or would once they add a token; ``chosen`` would only
#: once they chose to store one, so a computer of theirs that is offline,
#: which stands in the way of the choice they made, is said first.
Cloud = Literal["ready", "chosen"]


def summary(computers: list[ClaudeCodeComputer], status: ClaudeCodeStatus, *, cloud: Cloud | None, now: datetime) -> SetupState:
    """The first state that holds over every way the user's Claude Code runs:
    their own computers, the saved token where it signs turns in (``status``
    not ``off``), and a cloud computer (``cloud``)."""
    states = [_effective(c.state, c.expires_at, now) for c in computers]
    if status.state != "off":
        states.append(_effective(status.state, status.expires_at, now))
    if any(s in _WORKS for s in states):
        return "connected"
    if "checking" in states:
        return "checking"
    if "rejected" in states:
        return "refused"
    if cloud == "ready" or any(s in ("no_token", "off") for s in states):
        return "not_connected"
    if "offline" in states:
        return "offline"
    if cloud == "chosen":
        return "not_connected"
    return "unsupported" if computers or status.state == "policy_blocked" else "no_computer"


def _cloud(custody: ClaudeCodeCustody | None, *, on: bool, backend_runs: bool) -> Cloud | None:
    """Where a cloud computer stands (``Cloud``). Off, where it runs is not
    chosen yet, so storing a token for cloud computers is as open to the user
    as adding one."""
    if backend_runs or (custody is not None and custody.runs_on is not None):
        return "ready"
    if custody is not None and custody.refusal == "hosted_off":
        return "chosen" if on else "ready"
    return None


async def chief_of_staff(user_id: str, *, thread_id: str | None = None) -> ClaudeCodeChiefOfStaff:
    """Where the Chief of Staff's Claude Code runs, by the rule every turn
    follows (``hosted.cli_placement`` for Home, the saved token's check
    included), and whether it runs a Claude Code Default the user saved
    (``default_runs``), as its turns are admitted. With ``thread_id``, on
    that Home thread, which runs the deployment's Default once another model
    replied on it last (``chief_of_staff_default_runs``); its checkpoint is
    read only where the Default would run otherwise."""
    from src.server.database.home_workspace import get_home_workspace_id
    from src.server.services.llm.claude_code import another_model_replied, default_runs

    where = await hosted.cli_placement(user_id, get_home_workspace_id(user_id), token=True)
    runs = await default_runs(user_id, where)
    if runs and thread_id:
        runs = not await another_model_replied(thread_id)
    return ClaudeCodeChiefOfStaff(**hosted.runs_on_view(where).model_dump(), default_runs=runs)


async def setup_state(user_id: str) -> ClaudeCodeSetup | None:
    """None where this server offers no Claude Code at all, or offers it
    only to others: the operator's gate (a plan, or none) keeps it off for
    this user, who could not turn it on."""
    if not rows_available():
        return None
    on = await user_feature_enabled(user_id, FEATURE)
    if not on and system_flag(FEATURE).gate not in USER_CONTROLLABLE_GATES:
        return None
    computers, status, (refusals, _) = await asyncio.gather(
        list_computers(user_id, mark_offline=True), saved.status(user_id), hosted.cloud_view(user_id),
    )
    custody = await hosted.custody(user_id)
    # Before opt-in, whether the server would run turns on a saved token:
    # not where an organization's policy is installed on its machine.
    backend_runs = not on and backend_available() and not await local_managed()
    state = summary(
        computers.computers, status, now=datetime.now(timezone.utc),
        cloud=_cloud(custody, on=on, backend_runs=backend_runs),
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
        chief_of_staff=await chief_of_staff(user_id) if on else None,
    )
