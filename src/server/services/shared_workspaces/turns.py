"""Turns in a shared workspace: stopping a former member's, and which of a
thread's earlier turns a member may replace or answer. A send into a thread
while another member's turn runs is refused at admission
(``admission_gate.wait_or_steer``)."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional

from fastapi import HTTPException

from src.server.contracts.status import Stopper
from src.server.database import computer_data_tokens
from src.server.database.conversation import senders
from src.server.database.runs import lifecycle as tl_db
from src.server.database.runs import subagent_runs as sr_db
from src.server.database.runs.command_runs import request_former_member_stops
from src.server.database.runs.outbox import drop_former_member_steers
from src.server.database import shared_workspaces as db
from src.server.database import workspace as workspace_db
from src.server.database.shared_workspaces import (
    SharedRow,
    computers_minting_as_former,
    running_work,
    shared_computers_minting_as,
)
from src.server.services import account, cancel_dispatch, data_tokens, preview_capabilities
from src.server.services.shared_workspaces.access import shared_access, verdict
from src.server.database.shared_workspaces import is_holder

logger = logging.getLogger(__name__)


async def stop_turns_of_former_members(workspace_id: str, user_id: Optional[str]) -> None:
    """Stop the turns, subagent tasks and background commands that someone
    who lost access to the workspace has running there, as a suspension
    stops an account's. The messages they had queued are dropped: only a
    turn of theirs could take one, and they send no more here."""
    turns, tasks = await running_work(workspace_id, user_id)
    for run in turns:
        # Recorded as an agent's stop: no person asked for it.
        await cancel_dispatch.cancel_workflow(
            str(run["conversation_thread_id"]),
            str(run["conversation_response_id"]),
            by="agent",
        )
    for run in tasks:
        await cancel_dispatch.cancel_subagent_task(
            str(run["thread_id"]), str(run["task_id"]), task_run_id=str(run["task_run_id"])
        )
    # The watcher's next pass ends each one, with no report.
    commands = await request_former_member_stops(workspace_id, user_id)
    queued = await drop_former_member_steers(workspace_id, user_id)
    if turns or tasks or commands or queued:
        logger.info(
            "[shared] stopped %d turns, %d tasks and %d commands of a former member "
            "in %s, and dropped %d queued messages",
            len(turns), len(tasks), len(commands), workspace_id, queued,
        )


async def revoke_former_members_tokens(
    workspace_id: str, holder_id: str, user_id: Optional[str]
) -> None:
    """End the shared computer's market-data token when it was minted on the
    seat of someone who lost access (``user_id``, or with None anyone no
    longer listed), and forget them as whom the sweep refreshes it, so the
    next token is minted by a member's turn."""
    for computer_id in await computers_minting_as_former(workspace_id, holder_id, user_id):
        member = user_id
        if member is None:
            member = (await computer_data_tokens.load(computer_id)).user_id
        await data_tokens.revoke(computer_id, member=member)


async def end_access(workspace_id: str, holder_id: str, user_id: Optional[str]) -> None:
    """After someone loses the workspace: stop their running turns here (all
    of the turns of people no longer listed when ``user_id`` is None), end
    the market-data token the computer holds on their seat, and revoke
    previews on the workspace's computer. The one path for "this person no
    longer reaches this workspace", however they lost it; their feed and
    every request already read their reach afresh."""
    try:
        await stop_turns_of_former_members(workspace_id, user_id)
    except Exception:
        logger.exception("[shared] stopping a former member's turns in %s failed", workspace_id)
    try:
        await revoke_former_members_tokens(workspace_id, holder_id, user_id)
    except Exception:
        logger.exception("[shared] revoking a former member's data token in %s failed", workspace_id)
    try:
        workspace = await workspace_db.get_workspace(workspace_id)
        sandbox_id = (workspace or {}).get("sandbox_id")
        if sandbox_id:
            await preview_capabilities.revoke(sandbox_id)
    except Exception:
        logger.exception("[shared] revoking previews in %s failed", workspace_id)


async def lost(row: SharedRow, user_id: str) -> bool:
    """Whether ``user_id`` no longer reaches the workspace ``row``. An
    account that cannot be read keeps someone the workspace's own list or
    everyone keeps, so an outage does not stop every member; someone only
    its team could keep is lost, since whether they are in it cannot be
    read, and an unreadable account refuses rather than allows. A failed
    read of the workspace's own rows loses no one."""
    try:
        reach = await verdict(shared_access(row.workspace_id, row.holder_id, user_id))
        if reach != "unknown":
            return reach == "not"
        if row.everyone:
            return False
        return await db.get_member_role(row.workspace_id, user_id) is None
    except Exception:
        logger.warning("[shared] access check failed user=%s", user_id, exc_info=True)
        return False


async def end_access_of_unreached(row: SharedRow) -> None:
    """After the workspace ``row`` (as it is now) closed to everyone or
    changed its team: :func:`end_access` for each person it no longer
    reaches. With no team only its listed people reach it, so everyone else
    is ended at once without reading an account, as before teams existed;
    with one, each unlisted person who had anything there is asked."""
    if row.everyone:
        return
    if row.team_id is None:
        await end_access(row.workspace_id, row.holder_id, None)
        return
    try:
        people = await db.unlisted_actors(row.workspace_id, row.holder_id)
    except Exception:
        logger.exception("[shared] reading who acts in %s failed", row.workspace_id)
        return
    for user_id in people:
        if await lost(row, user_id):
            await end_access(row.workspace_id, row.holder_id, user_id)


# Strong references to the checks in flight. Process-local by design: nothing
# reads this set as state, and a check lost with its process leaves the turn
# to its own refusal at the next request it makes.
_checks: set[asyncio.Task] = set()


def stop_shared_work_if_gone_soon(user_id: str) -> None:
    """After a notice that the user's organization changed, stop what they
    have running in shared workspaces they can no longer use, in the
    background: leaving the organization ends it as leaving the workspace
    does."""
    task = asyncio.create_task(
        stop_shared_work_if_gone(user_id), name=f"shared_gone_{user_id[:12]}"
    )
    _checks.add(task)
    task.add_done_callback(_checks.discard)


async def stop_shared_work_if_gone(user_id: str) -> None:
    """:func:`end_access` for each shared workspace where ``user_id`` has
    work or a token and that they no longer reach (left its team, or the
    organization). A suspended account's turns are its suspension's to
    stop, marked as such, so for it only the tokens go: the holder's
    computer is not the account's own, and the suspension's stop leaves it."""
    try:
        (turns, tasks), tokens, commands, standing = await asyncio.gather(
            running_work(None, user_id),
            shared_computers_minting_as(user_id),
            db.workspaces_running_commands_of(user_id),
            account.read_standing(user_id, unknown_on_cache_error=True),
        )
        suspended = standing.suspended
    except Exception:
        logger.warning("[shared] reading shared work failed user=%s", user_id, exc_info=True)
        return
    places = {w for w, _, _ in tokens}
    if not suspended:
        places |= {r["workspace_id"] for r in turns + tasks} | set(commands)
    for workspace_id in sorted(places):
        row = await db.get_shared_workspace(workspace_id)
        if row is None or not await lost(row, user_id):
            continue
        if suspended:
            for _, holder_id, computer_id in tokens:
                if holder_id == row.holder_id:
                    await data_tokens.revoke(computer_id, member=user_id)
            continue
        await end_access(row.workspace_id, row.holder_id, user_id)


async def stop_target(
    thread_id: str, run_id: Optional[str], user_id: str
) -> tuple[Optional[str], Stopper]:
    """The run a stop in a shared workspace's thread is for (the thread's
    active one when none is named), and who stops it: another member's turn
    is stopped as a member's stop, whose queued messages stay its sender's
    rather than coming back to the member who stopped it. Named here so the
    stop is the run read."""
    run = await tl_db.get_run(run_id) if run_id else await tl_db.get_active_run(thread_id)
    if run is None or str(run.get("conversation_thread_id")) != thread_id:
        return run_id, "user"
    stopper: Stopper = "member" if run.get("user_id") != user_id else "user"
    return str(run["conversation_response_id"]), stopper


async def another_member_detail(running_user_id: str | None, *, waiting: bool = False) -> dict:
    """The conflict for a send into a shared workspace's thread whose turn
    another member sent and is still theirs: running, where steering it would
    put this sender's request on that member's seat, or ``waiting`` on a
    question or approval only they answer. Names the member, for the composer
    to say who."""
    from src.server.database.user import names_of

    name = (await names_of([running_user_id])).get(running_user_id) if running_user_id else None
    message = (
        f"Waiting for {name or 'another member'} to answer. "
        "You can send when their turn finishes."
        if waiting
        else f"{name or 'Another member'}'s turn is running in this thread. "
        "You can send when it ends."
    )
    return {
        "code": "another_member",
        "message": message,
        "member": {"user_id": running_user_id, "name": name},
    }


def _not_yours(message: str) -> HTTPException:
    return HTTPException(
        status_code=403, detail={"type": "another_members_turn", "message": message}
    )


def refuse_crossing_workspaces(
    thread_meta: Optional[dict[str, Any]], workspace_id: str, owner_id: str
) -> None:
    """Refuse a send that runs an existing thread in a workspace other than
    its own when either side is shared: a shared thread run on a member's own
    computer would put their personal tools and secrets under every member's
    messages, and a personal thread run on the shared computer would load its
    history there. A personal pair keeps whatever it did before (a former
    Home row, for one)."""
    if thread_meta is None or str(thread_meta["workspace_id"]) == workspace_id:
        return
    if is_holder(thread_meta["user_id"]) or is_holder(owner_id):
        raise HTTPException(
            status_code=400,
            detail={
                "type": "thread_in_another_workspace",
                "message": "This thread is in another workspace. Send it "
                "without a workspace_id, or with its own.",
            },
        )


async def refuse_others_turns(
    thread_id: str, request: Any, user_id: str, *, replaced: Optional[int]
) -> None:
    """In a shared workspace's thread, an edit, regenerate or fork from turn
    n replaces every turn from n on, so it is the sender's only when every
    one of those turns is theirs; and a paused turn is answered by the
    member whose turn it is, who alone may send while it waits.

    ``replaced`` is the first turn the request leaves off the branch, None
    when it only adds to it. The caller reads it off the branch
    (``first_turn_replaced``), so a run from an earlier checkpoint (a send
    naming one, a retry or a replay) is held to the same rule as an edit."""
    if replaced is not None and await senders.others_turns_from(thread_id, replaced, user_id):
        raise _not_yours(
            "Another member sent a turn from here on, so it can't be edited "
            "or regenerated. Send a new message instead."
        )
    if request.hitl_response and await senders.latest_turn_sender(thread_id) != user_id:
        raise _not_yours("Only the member whose turn this is can answer it.")
    if not request.hitl_response:
        # A new message would end the pause unanswered and take the thread
        # from them, so it waits as it would behind their running turn.
        paused = await senders.paused_turn_sender(thread_id)
        if paused is not None and paused != user_id:
            raise HTTPException(
                status_code=409, detail=await another_member_detail(paused, waiting=True)
            )


async def refuse_others_retry(run_id: str, user_id: str) -> None:
    """A retry runs a turn again as its sender; only that sender may."""
    if await senders.run_sender(run_id) != user_id:
        raise _not_yours("Only the member who sent this turn can retry it.")


async def refuse_others_review(thread_id: str, turn_index: int, user_id: str) -> None:
    """In a shared workspace's thread, a human review is asked for only by
    the member who sent the turn: it is a refund request for a turn they
    paid for, and their agreement that the team reads the conversation.
    Any member may still rate the turn."""
    if await senders.turn_sender(thread_id, turn_index) != user_id:
        raise _not_yours("Only the member who sent this turn can ask for a human review of it.")


async def refuse_others_task(thread_id: str, task_id: str, user_id: str) -> None:
    """In a shared workspace's thread, a running subagent task is instructed
    only by the member whose turn launched it: a message to it is a steer
    into their work, on their seat. A task not running is left to the
    steer's own answer."""
    run = await sr_db.get_active_task_run(thread_id, task_id)
    if run is not None and run.get("user_id") not in (None, user_id):
        raise HTTPException(
            status_code=403,
            detail={
                "type": "another_members_task",
                "message": "This task is part of another member's turn. Only they can instruct it.",
            },
        )
