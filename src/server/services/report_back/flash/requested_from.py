"""Where a hand-off was asked for, so the turn reporting it back can say.

A report-back turn is posted by the server, not sent from anywhere: it runs
under the last delivery rules the thread stated, and nothing in it says where
the work was asked for. An automation run tells its agent where to send the
result and names the run when it sends there; the turn that reports a hand-off
from that run back starts later and had neither.

So a dispatch records where the turn handing the work off came from, in the
report-back record it reserves (``reserve``): the automation run whose
delivery the messaging service holds, with the targets its agent was told, or
the chat app the turn arrived on. The report-back turn is reminded of it, and
the run's id rides into that turn's config, so a send to one of the run's
targets is the run's own. Whether the outcome still belongs there is the
agent's to decide; this only reminds. A request from the web app needs no
reminder: the report-back answers in the thread it was asked in.

Continuing an analyst's thread before its last result was reported folds that
report into the next one, so a record keeps every request since the last
report, oldest first, and the reminder names each one with where it came from.
"""

from __future__ import annotations

from typing import Any, Optional

from ptc_agent.agent.middleware.runtime_context.surface import parse_surface
from ptc_agent.agent.middleware.runtime_context.turn import BUILT_IN_SURFACES
from src.server.services import automation_delivery
from src.server.services.automation_delivery import Delivery

#: The report-back record's key for the requests it reports on.
KEY = "requested_from"

#: How much of a request the reminder quotes to tell requests apart.
_ASKED_CHARS = 60
#: The most requests one record keeps; the oldest go first.
MAX_ENTRIES = 5


def of_turn(configurable: dict[str, Any], question: str) -> dict[str, Any]:
    """The request a turn hands off, and where the turn came from.

    An automation run's turn is named by the run and the targets its agent
    was told, a turn that arrived on a chat app by the app. A turn from the
    web app, or from nowhere named, records only what was asked, which on its
    own reminds of nothing.
    """
    entry: dict[str, Any] = {}
    asked = automation_delivery.one_line(question, _ASKED_CHARS)
    if asked:
        entry["asked"] = asked
    run = automation_delivery.delivery_of_turn(configurable)
    targets = automation_delivery.reached(run.targets) if run else []
    if run and targets:
        entry["delivery"] = automation_delivery.stamp(Delivery(run.id, targets))
        return entry
    surface = parse_surface(configurable.get("platform")).name
    if surface and surface not in BUILT_IN_SURFACES:
        entry["surface"] = surface
    return entry


def entries(record: Optional[dict[str, Any]]) -> list[dict[str, Any]]:
    """The requests a report-back record names; none on a record from
    before they were kept."""
    raw = (record or {}).get(KEY)
    if not isinstance(raw, list):
        return []
    return [e for e in raw if isinstance(e, dict)]


def recorded(
    entry: dict[str, Any],
    previous: Optional[dict[str, Any]],
    *,
    flash_thread_id: str,
    reported: bool,
) -> list[dict[str, Any]]:
    """The requests a dispatch records: those still waiting on a report of
    this analysis, then its own.

    The previous record's requests stay unless their report already started
    or the record reports to another thread. The same request from the same
    place is kept once, at its latest.
    """
    kept: list[dict[str, Any]] = []
    if (
        previous is not None
        and previous.get("flash_thread_id") == flash_thread_id
        and not reported
    ):
        kept = [e for e in entries(previous) if e != entry]
    return [*kept, entry][-MAX_ENTRIES:]


def _delivery(entry: dict[str, Any]) -> Optional[Delivery]:
    run = automation_delivery.read_stamp(entry.get("delivery"))
    if run is None or not automation_delivery.reached(run.targets):
        return None
    return run


def _surface(entry: dict[str, Any]) -> Optional[str]:
    surface = entry.get("surface")
    return surface if isinstance(surface, str) and surface else None


def _place(entry: dict[str, Any]) -> Optional[tuple[str, str]]:
    """Where a request came from, as a key; None for this conversation."""
    run = _delivery(entry)
    if run is not None:
        return ("delivery", run.id)
    surface = _surface(entry)
    return ("surface", surface) if surface else None


def _where(entry: dict[str, Any]) -> str:
    run = _delivery(entry)
    if run is not None:
        return (
            "an automation run that sends its results to: "
            f"{automation_delivery.described(run.targets)}"
        )
    app = automation_delivery.app_name(_surface(entry) or "")
    article = "an" if app[:1].lower() in "aeiou" else "a"
    return f"{article} {app} conversation"


def reminder(record: Optional[dict[str, Any]]) -> Optional[str]:
    """What the report-back turn is told about where its work was asked for;
    None when every request came from the conversation it reports into.

    Requests from one place get one sentence. Requests from several get a
    line each, quoted, so no outcome reads as owed to another one's place.
    """
    items = entries(record)
    places = {_place(e) for e in items}
    if places <= {None}:
        return None
    if len(places) == 1:
        entry = items[-1]
        if _delivery(entry) is not None:
            return (
                f"You handed this analysis off during {_where(entry)}. "
                "If the outcome still belongs there, send it there with send_message. "
                "Write it for chat; attach files for detail."
            )
        return (
            f"You handed this analysis off from {_where(entry)}. "
            "If the outcome still belongs there, deliver it there as that "
            "conversation's delivery rules say."
        )
    lines = [
        "You handed this analysis off more than once. Where each request came from:"
    ]
    for entry in items:
        asked = entry.get("asked")
        asked = (
            automation_delivery.one_line(asked, _ASKED_CHARS)
            if isinstance(asked, str)
            else None
        )
        quoted = f'"{asked}"' if asked else "A request"
        where = _where(entry) if _place(entry) else "this conversation"
        lines.append(f"- {quoted}: {where}")
    closing = [
        "Deliver each outcome where its request came from, if it still belongs there."
    ]
    if any(_delivery(e) is not None for e in items):
        closing.append(
            "Send to an automation run's targets with send_message, written for "
            "chat with files attached for detail."
        )
    if any(_delivery(e) is None and _surface(e) for e in items):
        closing.append("Deliver to a conversation as its delivery rules say.")
    lines.append(" ".join(closing))
    return "\n".join(lines)


def delivery(record: Optional[dict[str, Any]]) -> Optional[Delivery]:
    """The automation run the report-back turn sends for: the latest one a
    request came from, since a turn names one run."""
    for entry in reversed(entries(record)):
        run = _delivery(entry)
        if run is not None:
            return run
    return None
