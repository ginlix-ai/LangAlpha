"""A wakeup table in memory, for the tool's set, cancel and listing paths.

The tool is tested through its database modules, faked where it imports
them, so what is pinned is the text the model reads and not the SQL.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest

from src.server.database.wakeups.cancel import CancelOutcome
from src.tools.wakeups import tool as tool_mod

# Tue 2026-10-20 09:57 EDT, which is 21:57 for a user in Asia/Shanghai.
NOW = datetime(2026, 10, 20, 13, 57, tzinfo=timezone.utc)
THREAD = "11111111-1111-1111-1111-111111111111"
USER = "u_test"
SHORT_IDS = ["w_3f2a", "w_4c11", "w_6a08", "w_91c0", "w_5d1e", "w_0be7"]


class _Frozen(datetime):
    @classmethod
    def now(cls, tz=None):
        return NOW.astimezone(tz) if tz else NOW


class FakeWakeups:
    """Rows as the table returns them; arm, list and cancel act on them."""

    def __init__(self) -> None:
        self.rows: list[dict] = []
        self.cancel_calls: list[tuple] = []
        self.late: set[str] = set()
        self.attempts: dict[str, dict] = {}

    async def arm(self, thread_id, user_id, specs, *, cap=10):
        room = max(cap - len(self.rows), 0)
        armed = []
        for spec in specs[:room]:
            n = len(self.rows)
            row = {
                "wakeup_id": str(uuid.UUID(int=n + 1)),
                "short_id": SHORT_IDS[n] if n < len(SHORT_IDS) else f"w_{n:04x}",
                "conversation_thread_id": thread_id,
                "user_id": user_id,
                "status": "armed",
                "fires_done": 0,
                "last_fired_seq": 0,
                "delivered_seq": 0,
                "created_at": NOW,
                "schedule": list(spec.schedule),
                "attempt": self.attempts.get(spec.vendor_order_id or ""),
            }
            for key in (
                "kind",
                "label",
                "timezone",
                "cron",
                "until_at",
                "stop_reason",
                "symbol",
                "market",
                "price_kind",
                "price_value",
                "pct_from",
                "armed_price",
                "warn_at",
                "expires_at",
                "attempt_id",
                "vendor_order_id",
                "baseline_filled_qty",
            ):
                row[key] = getattr(spec, key)
            self.rows.append(row)
            armed.append(dict(row))
        return armed, list(range(room, len(specs)))

    async def list_pending(self, thread_id):
        return [dict(r) for r in self.rows if r["status"] == "armed"]

    async def cancel(self, thread_id, ids, *, by):
        self.cancel_calls.append((thread_id, ids, by))
        out = []
        for given in ids if ids is not None else [r["short_id"] for r in self.rows]:
            row = next(
                (r for r in self.rows if given in (r["short_id"], r["wakeup_id"])), None
            )
            if row is None or row["status"] != "armed":
                out.append(CancelOutcome(given, given, "not_pending", None))
            elif row["short_id"] in self.late:
                out.append(
                    CancelOutcome(
                        row["wakeup_id"], row["short_id"], "too_late", dict(row)
                    )
                )
            else:
                row["status"] = "cancelled"
                out.append(
                    CancelOutcome(
                        row["wakeup_id"], row["short_id"], "cancelled", dict(row)
                    )
                )
        return out


@pytest.fixture
def db(monkeypatch):
    fake = FakeWakeups()
    monkeypatch.setattr(tool_mod.arm_db, "arm_wakeups", fake.arm)
    monkeypatch.setattr(tool_mod.read_db, "list_pending", fake.list_pending)
    monkeypatch.setattr(tool_mod.cancel_db, "cancel_wakeups", fake.cancel)
    monkeypatch.setattr(tool_mod, "wakeups_changed", AsyncMock())
    monkeypatch.setattr(tool_mod, "_flag_on", AsyncMock(return_value=True))
    monkeypatch.setattr(tool_mod, "datetime", _Frozen)
    # No market feed: a level needs no price, and a listing of levels reads none.
    monkeypatch.setattr(tool_mod.price_wakeups, "snapshots", AsyncMock(return_value={}))
    return fake


def configurable(**extra):
    return {"thread_id": THREAD, "user_id": USER, "timezone": "Asia/Shanghai", **extra}
