"""Validation and arithmetic for `schedule_wakeup` entries, with no I/O.

Everything a confirmation says back (the fire times, the stop reason) is
computed here from the entry as written, because the confirmation exists so a
wrong timezone, day or count shows before anything fires; ``clock`` reads it
back. The fire schedule is computed once at set and stored, so nothing here
runs at fire time.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import cache
from typing import Any, Literal, Optional, Sequence
from zoneinfo import ZoneInfo, available_timezones

from croniter import croniter
from pydantic import ValidationError

from src.server.database.wakeups.arm import WakeupSpec
from src.server.models.automation import _refuse_misread_cron
from src.tools.wakeups.clock import Zone, clock, user_zone_of, utc, zone_name
from src.tools.wakeups.describe import quoted_line
from src.tools.wakeups.models import LABEL_MAX, WakeupTrigger
from src.tools.wakeups.wording import BUILD_TOOL_RESULTS, TEMPLATES, TOOL_RESULTS

MAX_PENDING = 10
MIN_LEAD = timedelta(minutes=1)
HORIZON = timedelta(days=7)
MIN_CRON_GAP = timedelta(minutes=5)
MAX_FIRES = 24
MAX_CRON_CHARS = 120  # the column's width

KINDS = ("at", "cron", "price", "order")
PRICE_KINDS = ("above", "below", "pct_up", "pct_down")
PCT_KINDS = ("pct_up", "pct_down")

# Names a model reaches for when it wants to describe a wakeup; each gets the
# "name a wakeup with label" correction rather than the generic list.
_NAME_LIKE = frozenset(
    {
        "title",
        "name",
        "description",
        "desc",
        "note",
        "notes",
        "reason",
        "message",
        "purpose",
        "memo",
        "comment",
    }
)
_ABBREV = re.compile(r"[A-Z]{2,5}(\d{1,2}[A-Z]{2,4})?")


class Refused(Exception):
    """An entry that cannot be set: the refusal lines, and the label the
    entry is named by when several were sent (None when the label is what
    is refused, since an overlong one would only repeat itself)."""

    def __init__(self, *lines: str, label: Optional[str] = None) -> None:
        super().__init__(*lines)
        self.lines = list(lines)
        self.label = label


@cache
def _iana_names() -> frozenset[str]:
    return frozenset(available_timezones())


def resolve_tz(name: Optional[str], user_tz: str) -> ZoneInfo:
    """The zone an entry is read in.

    UTC or an Area/City name. A bare abbreviation (EST) is refused because
    zoneinfo reads it as a fixed offset that ignores daylight saving.
    """
    if name is None:
        zone = user_zone_of(user_tz)
        return zone if isinstance(zone, ZoneInfo) else ZoneInfo("UTC")
    s = name.strip()
    names = _iana_names()
    if s == "UTC" or ("/" in s and s in names):
        return ZoneInfo(s)
    if _ABBREV.fullmatch(s) and s in names:
        raise Refused(TEMPLATES["refused_timezone_abbrev"].format(name=s))
    raise Refused(TEMPLATES["refused_timezone"].format(name=s))


def parse_when(raw: str, zone: Zone) -> Optional[datetime]:
    """An ISO 8601 time; an offset in the text wins over ``zone``.

    A wall time the clocks skip (02:30 on a spring-forward night) has no
    instant of its own and zoneinfo reads it on the old offset, so it goes
    through UTC to name the time it fires (03:30), as the confirmation must.
    """
    try:
        at = datetime.fromisoformat(raw.strip())
    except ValueError:
        return None
    return at if at.tzinfo else utc(at.replace(tzinfo=zone)).astimezone(zone)


# --------------------------------------------------------------------------
# Cron
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class CronPlan:
    fires: tuple[datetime, ...]
    stop: Literal["24", "7day", "until"]


def _raw_fires(expr: str, start: datetime, n: int) -> list[datetime]:
    it = croniter(expr, start)
    out: list[datetime] = []
    try:
        for _ in range(n):
            out.append(it.get_next(datetime))
    except Exception:  # a date that never comes, such as Feb 30
        pass
    return out


def plan_cron(
    expr: str,
    zone: Zone,
    now: datetime,
    until: Optional[datetime],
    *,
    until_raw: Optional[str] = None,
) -> CronPlan:
    """Every match on ``zone``'s clock from ``now``.

    The series stops at 24 fires, 7 days or ``until``, whichever comes first.
    ``stop`` is "until" only when ``until`` falls before the 7-day end, since
    otherwise the horizon is what ended it.
    """
    if not croniter.is_valid(expr):
        raise Refused(BUILD_TOOL_RESULTS["cron_invalid"].format(expr=expr))
    try:
        _refuse_misread_cron(expr)
    except ValueError as e:
        raise Refused(TEMPLATES["refused_cron_misread"].format(problem=e)) from None
    local_now = now.astimezone(zone)
    raw = _raw_fires(expr, local_now, 30)
    fired = [utc(f) for f in raw]
    gap = min((b - a for a, b in zip(fired, fired[1:])), default=timedelta(days=1))
    if gap < MIN_CRON_GAP:
        raise Refused(
            TEMPLATES["refused_cron_gap"].format(
                expr=expr,
                gap=int(gap.total_seconds() // 60),
                least=int(MIN_CRON_GAP.total_seconds() // 60),
            )
        )

    end = utc(now) + HORIZON
    it = croniter(expr, local_now)
    fires: list[datetime] = []
    stop: Literal["24", "7day", "until"] = "7day"
    for _ in range(30000):
        try:
            at = it.get_next(datetime)
        except Exception:
            break
        if until is not None and at > until:
            stop = "until" if until < end else "7day"
            break
        if at > end:
            break
        fires.append(at)
        if len(fires) >= MAX_FIRES:
            stop = "24"
            break
    if not fires:
        if until is not None:
            raise Refused(
                BUILD_TOOL_RESULTS["cron_until_none"].format(
                    expr=expr, zone=zone_name(zone), raw=until_raw or until.isoformat()
                )
            )
        raise Refused(
            TEMPLATES["refused_cron_none"].format(expr=expr, zone=zone_name(zone))
        )
    return CronPlan(fires=tuple(fires), stop=stop)


# --------------------------------------------------------------------------
# One trigger entry
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class PriceAsk:
    """A price entry as written, still owing the checks that need a listing
    and a price."""

    label: Optional[str]
    symbol: str  # as the model spelled it
    kind: str
    value: float
    pct_from: Optional[str]  # tool vocabulary; a % kind's default filled in


@dataclass(frozen=True)
class OrderAsk:
    """An order entry as written, still owing the order lookup."""

    label: Optional[str]
    order_id: str
    timezone: Optional[str]  # only when the entry named one


def _error_lines(e: ValidationError) -> list[str]:
    lines = []
    for err in e.errors():
        loc = ".".join(str(x) for x in err["loc"])
        typ = err["type"]
        top = str(err["loc"][0]) if err["loc"] else ""
        if typ == "extra_forbidden":
            if len(err["loc"]) == 1:
                if top in _NAME_LIKE:
                    lines.append(TEMPLATES["refused_unknown_field"].format(key=top))
                else:
                    lines.append(
                        BUILD_TOOL_RESULTS["unknown_field_other"].format(key=top)
                    )
            else:
                lines.append(BUILD_TOOL_RESULTS["nested_unknown"].format(path=loc))
        elif typ == "string_too_long" and loc == "label":
            lines.append(TOOL_RESULTS["refused_label"])
        elif typ == "missing":
            lines.append(BUILD_TOOL_RESULTS["required"].format(path=loc))
        else:
            msg = err["msg"]
            lines.append(
                BUILD_TOOL_RESULTS["generic"].format(
                    path=loc, message=msg[0].lower() + msg[1:]
                )
            )
    return lines


def check_trigger(
    raw: Any, now: datetime, user_tz: str
) -> WakeupSpec | PriceAsk | OrderAsk:
    """One entry as the model wrote it: a timer's finished spec, or what a
    price or an order entry still has to look up. Raises `Refused`.

    Done by hand rather than by the pydantic model so every refusal is in the
    drafted wording and names its field. A label is stored as one line with
    no angle brackets, since the wake block and the cancel notice quote it
    inside their own markup, and a blank one as none.
    """
    if not isinstance(raw, dict):
        raise Refused(BUILD_TOOL_RESULTS["trigger_not_object"])
    label = raw.get("label")
    if isinstance(label, str):
        label = quoted_line(label) or None
        raw = {**raw, "label": label}
    try:
        return _check(raw, now, user_tz)
    except Refused as e:
        if isinstance(label, str) and len(label) <= LABEL_MAX:
            e.label = label
        raise


def _check(raw: dict, now: datetime, user_tz: str) -> WakeupSpec | PriceAsk | OrderAsk:
    """The first group of problems (kind, until, timezone) is reported
    together; after that, the first failure of the entry's own kind is."""
    try:
        t = WakeupTrigger.model_validate(raw)
    except ValidationError as e:
        raise Refused(*_error_lines(e)) from None
    kinds = [k for k in KINDS if getattr(t, k) is not None]
    problems: list[str] = []
    if len(kinds) != 1:
        problems.append(TOOL_RESULTS["refused_one_of"])
    if t.until is not None and t.cron is None:
        problems.append(BUILD_TOOL_RESULTS["until_needs_cron"])
    zone: Optional[ZoneInfo] = None
    try:
        zone = resolve_tz(t.timezone, user_tz)
    except Refused as e:
        problems.extend(e.lines)
    if problems:
        raise Refused(*problems)
    assert zone is not None
    kind = kinds[0]

    if kind == "at":
        text = t.at.strip()
        at = parse_when(text, zone)
        if at is None:
            raise Refused(BUILD_TOOL_RESULTS["at_unparseable"].format(s=text))
        if utc(at) - utc(now) < MIN_LEAD:
            user_now = clock(now, user_zone_of(user_tz), now)
            raise Refused(TEMPLATES["refused_soon"].format(s=text, now=user_now))
        if utc(at) - utc(now) > HORIZON:
            raise Refused(TEMPLATES["refused_horizon"].format(s=text))
        return WakeupSpec(
            kind="at", label=t.label, timezone=zone_name(at.tzinfo), schedule=(at,)
        )

    if kind == "cron":
        expr = t.cron.strip()
        if len(expr) > MAX_CRON_CHARS:
            raise Refused(BUILD_TOOL_RESULTS["cron_too_long"])
        until = None
        until_raw = None
        if t.until is not None:
            until_raw = t.until.strip()
            until = parse_when(until_raw, zone)
            if until is None:
                raise Refused(BUILD_TOOL_RESULTS["until_unparseable"].format(s=until_raw))
            if until <= now:
                raise Refused(TEMPLATES["refused_until"].format(s=until_raw))
        plan = plan_cron(expr, zone, now, until, until_raw=until_raw)
        return WakeupSpec(
            kind="cron",
            label=t.label,
            timezone=zone_name(zone),
            cron=expr,
            until_at=until,
            schedule=plan.fires,
            stop_reason=plan.stop,
        )

    if kind == "price":
        price = t.price
        price_kinds = [k for k in PRICE_KINDS if getattr(price, k) is not None]
        if len(price_kinds) != 1:
            raise Refused(BUILD_TOOL_RESULTS["price_one_kind"])
        price_kind = price_kinds[0]
        if price.pct_from and price_kind not in PCT_KINDS:
            raise Refused(TOOL_RESULTS["refused_pct_from"])
        return PriceAsk(
            label=t.label,
            symbol=price.symbol,
            kind=price_kind,
            value=float(getattr(price, price_kind)),
            pct_from=(price.pct_from or "previous_close")
            if price_kind in PCT_KINDS
            else None,
        )

    return OrderAsk(
        label=t.label,
        order_id=t.order.strip(),
        timezone=zone_name(zone) if t.timezone is not None else None,
    )


def refusal_lines(
    label: Optional[str], problems: Sequence[str], *, many: bool
) -> list[str]:
    """A labelled entry's refusals start ``- {label}: `` when several entries were sent.

    With one entry the call itself says which it was; with several, the
    refusal has to name the one it is about.
    """
    if not (many and label):
        return list(problems)
    return [f"- {label}: {line[2:]}" for line in problems]
