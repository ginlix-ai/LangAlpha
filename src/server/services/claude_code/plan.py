"""The Claude plan behind a user's Claude Code credentials, as runs keep it and Settings, admission and the gate read it.

Kept in Postgres (``claude_code_plan``) so every worker reads the same
state: admission on any of them, the gate of a sibling run in another
process, and Settings. A run writes each report in the background
(``UserPlan.record``), merged into the row's state (``PlanState``): a lost
write costs freshness, never the stop, which the run's own reports also
make (``PlanWatch``).
"""

from __future__ import annotations

import asyncio
import logging
import math
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, NamedTuple

from src.llms.claude_code import PLAN_LIMITS, PlanLimit, PlanOut, binding_limit
from src.server.database import claude_code_plan as store
from src.server.database.user import get_user_preferences
from src.server.models.claude_code import ClaudeCodePlan, PlanHeld, PlanUse

from .check_now import CHECK_EVERY

if TYPE_CHECKING:
    from ptc_agent.agent.claude_code.plan import SignedIn
    from src.llms.claude_code import PlanReading

logger = logging.getLogger(__name__)

#: Strong refs to writes in flight: asyncio keeps only weak ones.
_writes: set[asyncio.Task[None]] = set()
#: How long a stopped run waits for its reports to be written (``written``).
WRITTEN_WAIT_S = 2.0


async def plan_out(user_id: str) -> PlanOut:
    """The user's choice: ``credits`` only where they made it, else Stop. Read
    from the row each time, since it decides what is billed."""
    prefs = await get_user_preferences(user_id, cached=False) or {}
    return "credits" if (prefs.get("other_preference") or {}).get("claude_code_plan_out") == "credits" else "stop"


_LONG_AGO = datetime.min.replace(tzinfo=UTC)


class _Use(NamedTuple):
    used: float
    resets_at: datetime
    seen_at: datetime


class _Held(NamedTuple):
    until: datetime
    seen_at: datetime
    #: False where Claude said no reset: ``until`` is the hold's own bound (``UNKNOWN_RESET_HOLD``).
    known: bool = True


#: The key of a limit Claude named no window for, which binds every model.
UNNAMED = "unnamed"
#: The keys a limit is kept under: its window's, or ``UNNAMED``.
_HELD_KEYS = (*PLAN_LIMITS, UNNAMED)


def _limit(key: str, held: _Held) -> PlanLimit:
    return PlanLimit(None if key == UNNAMED else key, held.until, held.known)


def _covers(limit: PlanLimit, seen: datetime, key: str, held: _Held) -> bool:
    """Whether ``limit``, a limit with a reset reported at ``seen``, stands for
    ``held``, one Claude said no reset for: it is the same window, or the hold
    named none and the limit holds every model, and it was reported after
    the hold, or was holding when the hold was reported, so either order of
    writes keeps the reset."""
    same = key == limit.window or (key == UNNAMED and PLAN_LIMITS.get(limit.window) is None)
    return same and (held.seen_at <= seen or held.seen_at < limit.until)


class _Credits(NamedTuple):
    #: None where it is unknown since ``seen_at`` (``credits_unknown``).
    value: str | None
    seen_at: datetime


@dataclass(frozen=True)
class PlanState:
    """A credential's ``claude_code_plan.state``, merged from the reports its runs make.

    Each window's use, each limit and the usage-credit status keeps when the
    report it came from was seen, and moves only with a report seen after
    it, so a report carrying some windows keeps the others, a late write
    never undoes a newer one, and a limit on one window never replaces
    another's. ``cleared`` keeps, by window, when a request last ran on the
    plan on a model the window binds: a limit seen before that is over,
    so a refusal written late never brings one back. A report seen at the
    same time as one of those keeps the limit: the stop is the safe side.
    Only the windows ``PLAN_LIMITS`` knows hold limits, and ``UNNAMED``, one
    Claude named no window for. A limit Claude said no reset for holds until
    its bound, unless a limit with a reset already holds the model, and gives
    way to the next one with a reset.
    """

    windows: Mapping[str, _Use] = field(default_factory=dict)
    limits: Mapping[str, _Held] = field(default_factory=dict)
    cleared: Mapping[str, datetime] = field(default_factory=dict)
    credits: _Credits | None = None

    @classmethod
    def of(cls, state: Mapping[str, Any] | None) -> PlanState:
        state, at = state or {}, datetime.fromisoformat
        credits = state.get("credits")
        return cls(
            windows={n: _Use(w["used"], at(w["resets_at"]), at(w["seen_at"])) for n, w in (state.get("windows") or {}).items()},
            limits={w: _Held(at(v["until"]), at(v["seen_at"]), v.get("known", True))
                    for w, v in (state.get("limits") or {}).items() if w in _HELD_KEYS},
            cleared={w: at(v) for w, v in (state.get("cleared") or {}).items() if w in _HELD_KEYS},
            credits=_Credits(credits["value"], at(credits["seen_at"])) if credits else None,
        )

    def merge(self, reading: PlanReading) -> PlanState:
        """This state with ``reading`` taken in. A request that ran on the plan
        clears every limit that holds its model, a weekly family limit only
        for that family's models; a limit whose reset passed goes."""
        seen = reading.seen_at
        windows = dict(self.windows)
        for name, (used, resets_at) in reading.windows.items():
            if name not in windows or seen > windows[name].seen_at:
                windows[name] = _Use(used, resets_at, seen)
        cleared = dict(self.cleared)
        if reading.allowed:
            cleared.update({w: seen for w in _HELD_KEYS
                            if _limit(w, _Held(seen, seen)).binds(reading.model) and seen > cleared.get(w, _LONG_AGO)})
        limits = {w: held for w, held in self.limits.items()
                  if held.until > seen and held.seen_at >= cleared.get(w, _LONG_AGO)}
        if (limit := reading.limit) is not None:
            key = limit.window or UNNAMED
            told = any(held.known and (w == key or (key == UNNAMED and _limit(w, held).binds(reading.model)))
                       for w, held in limits.items())
            if limit.known and seen >= cleared.get(key, _LONG_AGO):
                rest = {w: held for w, held in limits.items() if held.known or not _covers(limit, seen, w, held)}
                if (standing := rest.get(key)) is None or seen > standing.seen_at:
                    limits = rest
            kept = limits.get(key)
            if ((kept is None or seen > kept.seen_at) and seen >= cleared.get(key, _LONG_AGO)
                    and (limit.known or not told)):
                limits[key] = _Held(limit.until, seen, limit.known)
        credits = self.credits
        if reading.credits is not None and (credits is None or seen > credits.seen_at):
            credits = _Credits(reading.credits, seen)
        return PlanState(windows, limits, cleared, credits)

    def to_json(self) -> dict[str, Any]:
        return {
            "windows": {n: {"used": u.used, "resets_at": u.resets_at.isoformat(), "seen_at": u.seen_at.isoformat()}
                        for n, u in self.windows.items()},
            "limits": {w: {"until": h.until.isoformat(), "seen_at": h.seen_at.isoformat(), **({} if h.known else {"known": False})}
                       for w, h in self.limits.items()},
            "cleared": {w: at.isoformat() for w, at in self.cleared.items()},
            **({"credits": {"value": self.credits.value, "seen_at": self.credits.seen_at.isoformat()}} if self.credits else {}),
        }

    def credits_unknown_at(self, at: datetime) -> PlanState:
        """This state with whether usage credits pay unknown as of ``at``, so
        only a report seen after it says again; one seen after it already
        written stands."""
        if self.credits is not None and self.credits.seen_at > at:
            return self
        return replace(self, credits=_Credits(None, at))

    def held(self, now: datetime | None = None) -> list[PlanLimit]:
        """The limits whose reset is still ahead. One Claude said no reset for
        holds only while usage credits may pay: with them off, a request at
        the limit is refused at no cost, and its report says more."""
        return [limit for w, h in self.limits.items()
                if (limit := _limit(w, h)).current(now) and (limit.known or self.credits_value != "off")]

    @property
    def credits_value(self) -> str | None:
        """Whether the account's usage credits pay past the limit, as last reported."""
        return self.credits.value if self.credits else None

    def use(self, window: str, now: datetime) -> PlanUse | None:
        """A window's use, none once its reset passed: the old use is wrong,
        and the new one is unknown until the next request."""
        kept = self.windows.get(window)
        return PlanUse(used=kept.used, resets_at=kept.resets_at) if kept and kept.resets_at > now else None


async def credits_unknown(user_id: str, *, conn=None) -> None:
    """Forget whether the user's Claude accounts' usage credits pay past the
    limit: they chose to run on them, perhaps having just turned them on in
    claude.ai, which only a request reports. Until one does, admission lets
    a turn through, and its first request says; one refused there is not
    billed. Every row is marked, one that knew nothing yet included, so a
    report seen before now and written after never says again."""
    at = datetime.now(UTC)
    await store.change_plans(
        user_id, lambda state: PlanState.of(state).credits_unknown_at(at).to_json(), conn=conn,
    )


class UserPlan:
    """A user's plan state as a run keeps and reads it (``plan.PlanLog``):
    the saved token's, and with ``computer_id`` that computer's own. A run
    on a saved token read before it opened (a token's check) names it by
    ``token_digest``, so it keeps nothing once another token is saved."""

    def __init__(self, user_id: str, computer_id: str | None = None, *, token_digest: str | None = None) -> None:
        self.user_id = user_id
        self.computer_id = computer_id
        self.token_digest = token_digest
        #: Each credential's generation as the run opened, which its writes go to.
        self._fences: dict[str, int] | None = None
        self._pending: set[asyncio.Task[None]] = set()
        #: A report was not kept: its write failed, its token was replaced, or
        #: the run could not read which generation to write it to.
        self._lost = False

    async def open(self) -> None:
        credentials = [c for c in (store.SAVED, store.plan_credential("machine", self.computer_id)) if c]
        rows = await store.get_plans(self.user_id, credentials)
        self._fences = {c: rows[c]["generation"] if c in rows else 0 for c in credentials}
        saved = rows.get(store.SAVED) or {}
        if self.token_digest and saved.get("token_digest") not in (None, self.token_digest):
            del self._fences[store.SAVED]

    async def launching(self) -> None:
        credential = store.plan_credential("machine", self.computer_id)
        if credential is None or credential not in (self._fences or {}):
            return
        try:
            row = (await store.get_plans(self.user_id, [credential])).get(credential)
        except Exception as e:  # noqa: BLE001 - the fence the run opened on stands
            logger.warning(f"[claude_code] could not read the computer's plan row for user_id={self.user_id}: {type(e).__name__}")
            return
        if row is not None:
            self._fences[credential] = row["generation"]

    async def took(self, token: str) -> None:
        from .saved import token_hash

        if store.SAVED not in (self._fences or {}):
            return
        try:
            row = (await store.get_plans(self.user_id, [store.SAVED])).get(store.SAVED) or {}
        except Exception as e:  # noqa: BLE001 - the fence the run opened on stands
            logger.warning(f"[claude_code] could not read the saved token's plan row for user_id={self.user_id}: {type(e).__name__}")
            return
        if row and row.get("token_digest") == token_hash(token):
            self._fences[store.SAVED] = row["generation"]

    async def out(self) -> PlanOut:
        return await plan_out(self.user_id)

    def record(self, signed_in: SignedIn, reading: PlanReading) -> None:
        credential = store.plan_credential(signed_in, self.computer_id)
        if credential is None:
            return
        if (generation := (self._fences or {}).get(credential)) is None:
            self._lost = True
            return
        task = asyncio.create_task(self._record(credential, generation, reading))
        for kept in (_writes, self._pending):
            kept.add(task)
            task.add_done_callback(kept.discard)

    async def written(self, within: float | None = None) -> bool:
        """Wait up to ``within`` (``WRITTEN_WAIT_S`` unless given) for the
        reports so far to be written; whether every one was."""
        pending = set(self._pending)
        if pending:
            await asyncio.wait(pending, timeout=WRITTEN_WAIT_S if within is None else within)
        return all(task.done() for task in pending) and not self._lost

    async def _record(self, credential: str, generation: int, reading: PlanReading) -> None:
        try:
            kept = await store.update_plan(
                self.user_id, credential, generation, reading.seen_at,
                lambda state: PlanState.of(state).merge(reading).to_json(),
            )
        except Exception as e:  # noqa: BLE001 - a lost report costs freshness only
            logger.warning(f"[claude_code] could not keep a plan report for user_id={self.user_id} {credential}: {type(e).__name__}")
            self._lost = True
            return
        if not kept:
            self._lost = True
            logger.info(f"[claude_code] dropped a plan report of a replaced token for user_id={self.user_id} {credential}")

    async def limit(self, signed_in: SignedIn, model: str) -> PlanLimit | None:
        credential = store.plan_credential(signed_in, self.computer_id)
        if credential is None:
            return None
        row = (await store.get_plans(self.user_id, [credential])).get(credential) or {}
        return binding_limit(PlanState.of(row.get("state")).held(), model)


class CheckReports:
    """What a check of a token not saved yet reported of its plan (``plan.PlanLog``),
    kept until the token is saved and then written to the row the save
    starts afresh (``keep``), so a check that met the limit leaves it there for
    the next turn. A token the check refuses is never saved, and keeps nothing."""

    def __init__(self) -> None:
        self.reports: list[tuple[SignedIn, PlanReading]] = []

    async def open(self) -> None:
        pass

    async def launching(self) -> None:
        pass

    async def took(self, token: str) -> None:
        pass

    async def out(self) -> PlanOut:
        return "stop"

    def record(self, signed_in: SignedIn, reading: PlanReading) -> None:
        self.reports.append((signed_in, reading))

    async def limit(self, signed_in: SignedIn, model: str) -> PlanLimit | None:
        return None

    async def written(self) -> None:
        pass

    async def keep(self, plan: UserPlan) -> None:
        """Write the reports to ``plan``'s rows as they stand after the save."""
        if not self.reports:
            return
        try:
            await plan.open()
        except Exception as e:  # noqa: BLE001 - a lost report costs freshness only
            logger.warning(f"[claude_code] could not keep a token check's plan reports for user_id={plan.user_id}: {type(e).__name__}")
            return
        for signed_in, reading in self.reports:
            plan.record(signed_in, reading)
        await plan.written()


def plan_view(row: dict[str, Any] | None, now: datetime | None = None) -> ClaudeCodePlan | None:
    """A row as Settings and the composer read it, None where its token
    reported nothing yet."""
    if not (row or {}).get("state"):
        return None
    now = now or datetime.now(UTC)
    state = PlanState.of(row["state"])
    return ClaudeCodePlan(
        five_hour=state.use("five_hour", now), seven_day=state.use("seven_day", now),
        limits=[PlanHeld(window=limit.window, until=limit.until, family=PLAN_LIMITS.get(limit.window),
                         reset_known=limit.known)
                for limit in state.held(now)],
        credits=state.credits_value, observed_at=row["updated_at"],
    )


def check_in(row: Mapping[str, Any]) -> int | None:
    """Whole seconds until Check now may run on the row's credential again,
    None where it may now, on the database's clock both stamps are on."""
    if (checked_at := row.get("checked_at")) is None:
        return None
    left = (checked_at + CHECK_EVERY - row["read_at"]).total_seconds()
    return math.ceil(left) if left > 0 else None


class CredentialPlan(NamedTuple):
    """What Settings shows of a credential: the plan behind it, when Check
    now may read it again, and whether a check would be billed, so is not
    offered (``check_bills``)."""

    plan: ClaudeCodePlan | None
    check_in: int | None
    check_bills: bool = False


async def plan_views(user_id: str, credentials: list[str] | None = None) -> dict[str, CredentialPlan]:
    """What the user's credentials show, by credential, ``credentials`` only
    where given. A read that fails shows nothing: the plan is beside what
    Settings reads, never a reason to fail it."""
    try:
        rows = await store.get_plans(user_id, credentials)
    except Exception as e:  # noqa: BLE001
        logger.warning(f"[claude_code] could not read the plan state for user_id={user_id}: {type(e).__name__}")
        return {}
    from .checks import check_bills

    bills = {key for key, row in rows.items() if check_bills(PlanState.of(row.get("state")), "stop")}
    if bills:
        try:
            out = await plan_out(user_id)
        except Exception as e:  # noqa: BLE001 - read as Stop, as Check now's gate reads it
            logger.warning(f"[claude_code] could not read the plan choice for user_id={user_id}: {type(e).__name__}")
            out = "stop"
        bills = bills if out == "stop" else set()
    return {key: CredentialPlan(plan_view(row), check_in(row), key in bills) for key, row in rows.items()}
