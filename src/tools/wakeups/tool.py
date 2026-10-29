"""`schedule_wakeup`: the agent sets, cancels and lists its own follow-ups on
a thread, and ends a turn a wakeup started without a reply.

A ``BaseTool`` subclass rather than a decorated function because the call is
validated here, not by pydantic before it: a generic validation error would
replace the per-entry refusals the model is meant to read, and one bad entry
would sink the good ones sent beside it. The schema the model sees is still
the strict one.
"""

from __future__ import annotations

import inspect
import logging
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Annotated, Any, Dict, List, Literal, Mapping, Optional, Sequence, Tuple

from langchain_core.messages import ToolMessage
from langchain_core.runnables import RunnableConfig
from langchain_core.tools import BaseTool, InjectedToolCallId
from langgraph.prebuilt import InjectedState
from pydantic import BaseModel

from src.server.database.wakeups import arm as arm_db
from src.server.database.wakeups import cancel as cancel_db
from src.server.database.wakeups import read as read_db
from src.server.database.wakeups.arm import WakeupSpec
from src.server.database.wakeups.cancel import CancelOutcome
from src.server.services.wakeups.changes import wakeups_changed
from src.tools.wakeups import orders as order_wakeups
from src.tools.wakeups import price as price_wakeups
from src.tools.wakeups.call import parse_call
from src.tools.wakeups.clock import header
from src.tools.wakeups.describe import pending_line, set_line, short_desc
from src.tools.wakeups.models import ScheduleWakeupArgs, WakeupArtifact
from src.tools.wakeups.price import PricePlan
from src.tools.wakeups.quiet import TOOL_NAME, changed_this_turn, quiet_refusal
from src.tools.wakeups.schedule import (
    MAX_PENDING,
    OrderAsk,
    PriceAsk,
    Refused,
    check_trigger,
    refusal_lines,
)
from src.tools.wakeups.wording import (
    BUILD_TOOL_RESULTS,
    SCHEDULE_WAKEUP_DOC,
    TEMPLATES,
    TOOL_RESULTS,
)

logger = logging.getLogger(__name__)

_ENDED = ("cancelled", "cancelled_in_flight")
_QUIET_REFUSALS = {
    "user_turn": TOOL_RESULTS["quiet_refused"],
    "acted": BUILD_TOOL_RESULTS["quiet_refused_acted"],
    "other_calls": BUILD_TOOL_RESULTS["quiet_refused_calls"],
}


class _ToolInput(ScheduleWakeupArgs):
    """The model's arguments plus what the tool node injects, which
    ``tool_call_schema`` leaves out of what the model sees."""

    state: Annotated[Optional[dict], InjectedState] = None
    tool_call_id: Annotated[Optional[str], InjectedToolCallId] = None


def _artifact(*, changed: bool = False) -> WakeupArtifact:
    return WakeupArtifact(quiet=False, changed=changed)


class _Entry:
    """One trigger entry on its way to a row: its spec, or why it has none."""

    __slots__ = ("label", "spec", "problems", "price", "attempt", "last", "level")

    def __init__(self) -> None:
        self.label: Optional[str] = None
        self.spec: Optional[WakeupSpec] = None
        self.problems: List[str] = []
        self.price: Optional[PricePlan] = None
        # The order an order entry watches, which its lines describe.
        self.attempt: Optional[Dict[str, Any]] = None
        self.last: Optional[float] = None
        self.level: Optional[float] = None

    def row_of(self, row: Mapping[str, Any]) -> Dict[str, Any]:
        """``row`` (or a spec read as one) as the renderers read it."""
        return {**row, "attempt": self.attempt} if self.attempt is not None else dict(row)


async def _flag_on(user_id: Optional[str]) -> bool:
    """The setting as it is now: a turn built while it was on keeps the tool
    after the user turns it off. Fails open, since the build read it on."""
    if not user_id:
        return True
    try:
        from src.server.services.features import user_feature_enabled

        return await user_feature_enabled(str(user_id), "wakeups")
    except Exception:
        logger.warning("[Wakeups] setting re-check failed; going with the build", exc_info=True)
        return True


class ScheduleWakeupTool(BaseTool):
    name: str = TOOL_NAME
    description: str = inspect.cleandoc(SCHEDULE_WAKEUP_DOC)
    args_schema: type[BaseModel] = _ToolInput
    response_format: Literal["content", "content_and_artifact"] = "content_and_artifact"

    def _parse_input(self, tool_input: Any, tool_call_id: Optional[str]) -> Dict[str, Any]:
        args = dict(tool_input) if isinstance(tool_input, dict) else {}
        args["tool_call_id"] = tool_call_id
        return args

    def _run(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError("schedule_wakeup is async only")

    async def _arun(self, *, run_config: RunnableConfig, **kwargs: Any) -> Tuple[Any, WakeupArtifact]:
        call_id = kwargs.pop("tool_call_id", None)
        state = kwargs.pop("state", None) or {}
        configurable = (run_config or {}).get("configurable") or {}
        text, artifact, error = await _execute(kwargs, state, configurable)
        if call_id is None:
            return text, artifact
        message = ToolMessage(
            content=text,
            artifact=artifact,
            tool_call_id=call_id,
            name=self.name,
            status="error" if error else "success",
        )
        return message, artifact


def build_schedule_wakeup_tool() -> BaseTool:
    return ScheduleWakeupTool()


# ---------------------------------------------------------------------------
# One call
# ---------------------------------------------------------------------------


async def _execute(
    args: Dict[str, Any], state: Mapping[str, Any], configurable: Mapping[str, Any]
) -> Tuple[str, WakeupArtifact, bool]:
    """``(text, artifact, is_error)`` in the harness's order: argument
    problems, cancel, set, refusals, the listing, quiet, or the listing alone."""
    thread_id = configurable.get("thread_id")
    user_id = configurable.get("user_id")
    user_tz = str(configurable.get("timezone") or "UTC")
    messages: Sequence[Any] = list(state.get("messages") or [])
    now = datetime.now(timezone.utc)

    call = parse_call(args)
    if call.problems:
        text = BUILD_TOOL_RESULTS["top_header"] + "\n" + "\n".join(call.problems)
        return text, _artifact(changed=changed_this_turn(messages, args)), False
    if not thread_id:
        # A build outside a thread never binds the tool; this guards a caller
        # that invokes it directly.
        return TOOL_RESULTS["none"], _artifact(), False

    # Both changes first and the monitors told at once, so a read-back that
    # fails after them cannot hide what they did.
    outcomes: List[CancelOutcome] = []
    if call.cancel is not None:
        outcomes = await cancel_db.cancel_wakeups(
            thread_id, None if call.cancel == "all" else list(call.cancel), by="agent"
        )
    armed: List[Tuple[Dict[str, Any], _Entry]] = []
    refused: List[str] = []
    if call.triggers:
        armed, refused = await _set(call.triggers, thread_id, user_id, user_tz, now)
    if any(
        o.outcome in _ENDED and o.row is not None and o.row.get("kind") == "price"
        for o in outcomes
    ) or any(row.get("kind") == "price" for row, _entry in armed):
        await wakeups_changed(thread_id)

    sections: List[str] = []
    if call.cancel is not None:
        sections.extend(_cancel_lines(outcomes))
    if armed:
        block = await _pending_block(thread_id, user_id, user_tz, now, armed)
        ids = ", ".join(row["short_id"] for row, _ in armed)
        sections.append(f"Set {ids}. {block}")
    if refused:
        sections.append(TOOL_RESULTS["refused_header"] + "\n" + "\n".join(refused))
    if not armed and (call.cancel is not None or refused):
        block = await _pending_block(thread_id, user_id, user_tz, now)
        if block is not None:
            sections.append(block)

    artifact = _artifact(changed=changed_this_turn(messages, args))
    error = False
    if call.quiet is not None:
        refusal = quiet_refusal(messages, configurable)
        if refusal is None:
            sections.append(TOOL_RESULTS["quiet_ok"])
            artifact["quiet"] = True
        else:
            sections.append(_QUIET_REFUSALS[refusal])
            error = True
    if call.cancel is None and not call.triggers and call.quiet is None:
        block = await _pending_block(thread_id, user_id, user_tz, now)
        if block is None:
            # Nothing was asked but the listing, so its failure is the call's.
            raise RuntimeError("the pending wakeups could not be read")
        sections.append(block)
    return "\n".join(sections) or TOOL_RESULTS["none"], artifact, error


# ---------------------------------------------------------------------------
# Cancel
# ---------------------------------------------------------------------------


def _cancel_lines(outcomes: List[CancelOutcome]) -> List[str]:
    by_id = {o.row["wakeup_id"]: o.row for o in outcomes if o.row is not None}
    done: List[str] = []
    lines: List[str] = []
    # In the order they were set, as the listing reads; ids with no row last.
    known = sorted(
        (o for o in outcomes if o.wakeup_id in by_id),
        key=lambda o: str(by_id[o.wakeup_id].get("created_at") or ""),
    )
    for o in [*known, *(o for o in outcomes if o.wakeup_id not in by_id)]:
        row = by_id.get(o.wakeup_id)
        if o.outcome == "cancelled" and row is not None:
            done.append(f"{o.short_id} ({short_desc(row)})")
        elif o.outcome == "cancelled_in_flight" and row is not None:
            lines.append(
                BUILD_TOOL_RESULTS["cancelled_in_flight"].format(
                    desc=f"{o.short_id} ({short_desc(row)})",
                    n=int(row.get("last_fired_seq") or 0),
                )
            )
        elif o.outcome == "too_late":
            lines.append(TEMPLATES["cancel_late"].format(id=o.short_id))
        else:
            lines.append(TEMPLATES["cancel_unknown"].format(id=o.short_id))
    if done:
        lines.insert(0, TEMPLATES["cancelled"].format(items=", ".join(done)))
    return lines


# ---------------------------------------------------------------------------
# Set
# ---------------------------------------------------------------------------


async def _entry_for(raw: Any, thread_id: str, user_tz: str, now: datetime) -> _Entry:
    """An entry checked as far as it can be before prices are fetched: a
    timer's spec, a price's plan, an order's spec, or its refusal."""
    entry = _Entry()
    try:
        ask = check_trigger(raw, now, user_tz)
        entry.label = ask.label
        if isinstance(ask, PriceAsk):
            entry.price = price_wakeups.plan_price(ask)
        elif isinstance(ask, OrderAsk):
            entry.attempt, entry.spec = await order_wakeups.order_spec(thread_id, ask, now)
        else:
            entry.spec = ask
    except Refused as e:
        entry.label = entry.label or e.label
        entry.problems = e.lines
    return entry


async def _set(
    triggers: List[Any], thread_id: str, user_id: Any, user_tz: str, now: datetime
) -> Tuple[List[Tuple[Dict[str, Any], _Entry]], List[str]]:
    """Arm every valid entry; ``([(row, entry)], refusal lines)`` in entry order."""
    many = len(triggers) > 1
    if not await _flag_on(user_id):
        return [], [BUILD_TOOL_RESULTS["setting_off"]]

    entries = [await _entry_for(raw, thread_id, user_tz, now) for raw in triggers]
    priced = [e for e in entries if e.price is not None]
    if priced:
        snaps = await price_wakeups.snapshots(
            [(e.price.listing.key, e.price.listing.market) for e in priced], user_id
        )
        for e in priced:
            listing = (e.price.listing.key, e.price.listing.market)
            try:
                e.spec, e.last, e.level = price_wakeups.price_spec(
                    e.price, e.label, snaps.get(listing) or {}, now
                )
            except Refused as r:
                e.problems = r.lines

    ready = [e for e in entries if e.spec is not None and not e.problems]
    armed: List[Tuple[Dict[str, Any], _Entry]] = []
    if ready:
        try:
            if not user_id:
                raise RuntimeError("no user to set wakeups for")
            rows, left_out = await arm_db.arm_wakeups(
                thread_id, str(user_id), [e.spec for e in ready], cap=MAX_PENDING
            )
        except Exception:
            logger.warning("[Wakeups] arming failed thread_id=%s", thread_id, exc_info=True)
            for e in ready:
                # Unlabelled: a refusal names its entry by the label itself.
                row = e.row_of({**asdict(e.spec), "label": None})
                e.problems = [BUILD_TOOL_RESULTS["save_failed"].format(desc=short_desc(row))]
        else:
            for row, e in zip(rows, ready):
                armed.append((e.row_of(row), e))
            for i in left_out:
                ready[i].problems = [TOOL_RESULTS["refused_cap"]]
    if armed and not await _flag_on(user_id) and await _end_overtaken(thread_id, armed):
        return [], [BUILD_TOOL_RESULTS["setting_off"]]

    refused: List[str] = []
    for e in entries:
        refused.extend(refusal_lines(e.label, e.problems, many=many))
    return armed, refused


async def _end_overtaken(
    thread_id: str, armed: Sequence[Tuple[Dict[str, Any], _Entry]]
) -> bool:
    """End what was just set when the setting turned off during the call: the
    off sweep may have read before these rows committed. False when that
    fails, and the rows stand as set; delivery ends them while it stays off."""
    try:
        await cancel_db.cancel_wakeups(
            thread_id, [str(row["wakeup_id"]) for row, _ in armed], by="setting_off"
        )
    except Exception:
        logger.warning(
            "[Wakeups] ending a set the setting overtook failed thread_id=%s",
            thread_id,
            exc_info=True,
        )
        return False
    if any(row.get("kind") == "price" for row, _ in armed):
        await wakeups_changed(thread_id)
    return True


# ---------------------------------------------------------------------------
# Listing
# ---------------------------------------------------------------------------


async def _pending_block(
    thread_id: str,
    user_id: Any,
    user_tz: str,
    now: datetime,
    armed: Sequence[Tuple[Dict[str, Any], _Entry]] = (),
) -> Optional[str]:
    """The pending listing: entries just set as they were understood, the
    ones from before with their progress.

    A failed read falls back to what was just set, and with nothing set
    returns None, so a call that changed something still says what.
    """
    new = {row["wakeup_id"]: (row, entry) for row, entry in armed}
    try:
        rows = await read_db.list_pending(thread_id)
    except Exception:
        logger.warning("[Wakeups] pending read failed thread_id=%s", thread_id, exc_info=True)
        if not armed:
            return None
        rows = [row for row, _ in armed]
    if not rows and not armed:
        return TOOL_RESULTS["none"]
    prices = await price_wakeups.readback(
        [r for r in rows if r["wakeup_id"] not in new], user_id
    )
    lines: List[str] = []
    for row in rows:
        wid = row["wakeup_id"]
        if wid in new:
            shown, entry = new[wid]
            lines.append(set_line(shown, now, user_tz, last=entry.last, level=entry.level))
        else:
            last, level = prices.get(wid, (None, None))
            lines.append(pending_line(row, now, user_tz, last=last, level=level))
    return header(user_tz, now) + "\n" + "\n".join(lines)


__all__ = ["ScheduleWakeupTool", "TOOL_NAME", "build_schedule_wakeup_tool"]
