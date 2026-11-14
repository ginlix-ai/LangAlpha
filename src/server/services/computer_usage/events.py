"""Each computer start and stop, as events in the ``computer-events`` file.

Measurement for choosing when to start and stop computers: what asked for a
start, from which app, how the sandbox came up and what each step cost. It
goes to an append-only file (``observability/event_log.py``) rather than a
table, so recording it costs a status write no statement and no lock, and a
start and stop are paired afterwards from the events
(``scripts/ops/computer_events.py``), not by a writer reading the last one
back. Nothing reads these to decide anything.

Events, each with the computer (or for a provider's report, its sandbox):

- ``status``: the computer was written to a status, with the cause around the
  write (``causes.py``), and for a stop its writer's reason and how long the
  computer sat unused.
- ``start_path``: a claimed start builds its sandbox (``create``/``rebuild``).
- ``runtime_start``: a reconnect started the sandbox itself, from the state
  the provider reported, and the provider's time.
- ``start_failure``: a start failed, with its code.
- ``stop_phases``: the last backup's and the provider stop's times.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping, Optional

from src.observability.event_log import write_event
from src.server.services.computer_usage.causes import current_cause

_STREAM = "computer-events"


def _cause_fields() -> dict[str, Any]:
    cause = current_cause()
    if cause is None:
        return {}
    return {
        "cause": cause.name,
        "client": cause.client,
        "detail": cause.detail,
        "asked_at": cause.at,
    }


def note_status(computer: Mapping[str, Any], status: str, *, reason: Optional[str]) -> None:
    """Record that *computer* (a computer row, as written) is now *status*."""
    idle_s = None
    last_activity = computer.get("last_activity_at")
    if status == "stopping" and isinstance(last_activity, datetime):
        idle_s = max(0, int((datetime.now(timezone.utc) - last_activity).total_seconds()))
    write_event(
        _STREAM,
        "status",
        computer_id=str(computer["computer_id"]),
        user_id=computer.get("user_id"),
        kind=computer.get("kind"),
        tier=computer.get("resource_tier"),
        sandbox=computer.get("provider_ref"),
        status=status,
        reason=reason,
        idle_s=idle_s,
        **_cause_fields(),
    )


def note_start_path(computer_id: str, path: str) -> None:
    """A claimed start builds its sandbox (``create``, ``rebuild``) rather than
    starting one the provider had."""
    write_event(_STREAM, "start_path", computer_id=computer_id, path=path)


def note_start_failure(computer_id: str, code: str) -> None:
    write_event(_STREAM, "start_failure", computer_id=computer_id, code=code)


def note_stop_phases(
    computer_id: str, *, backup_ms: Optional[int], provider_ms: Optional[int]
) -> None:
    write_event(
        _STREAM,
        "stop_phases",
        computer_id=computer_id,
        backup_ms=backup_ms,
        provider_ms=provider_ms,
    )


def _on_runtime_start(report) -> None:
    """Called inside the reconnect, so the cause is the reconnect's own."""
    if not report.started:
        return  # the sandbox was up, or another caller's start was
    write_event(
        _STREAM,
        "runtime_start",
        sandbox=report.sandbox_id,
        kind=report.provider_kind,
        path="archived" if report.state_before == "archiving" else report.state_before,
        provider_ms=report.start_ms,
        **_cause_fields(),
    )


def install_runtime_start_listener() -> None:
    from ptc_agent.core.sandbox.start_report import set_runtime_start_listener

    set_runtime_start_listener(_on_runtime_start)
