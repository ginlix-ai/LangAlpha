"""The automations table in memory, for tests of a save through the file.

Stands in for ``src.server.database.automation`` where the file and the
lifecycle both reach it, so a save runs the real rules against rows that
behave as Postgres keeps them. ``transaction()`` restores the rows when its
block raises, nested or not, so a savepoint and a refused save roll back as
they would; each write is journaled with the depth it ran at.
"""

from __future__ import annotations

import contextlib
import copy
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from psycopg.errors import UniqueViolation

from src.server.database import automation as automation_db

# What update_automation's query builder sets: a None is written only to a
# nullable column, and a JSON column ignores None.
_PLAIN = {
    "name", "description", "cron_expression", "timezone", "instruction", "workspace_id",
    "llm_model", "thread_strategy", "conversation_thread_id", "status",
    "max_failures", "failure_count", "disable_reason", "next_run_at", "last_run_at",
}
_NULLABLE = {"next_run_at", "last_run_at", "conversation_thread_id", "disable_reason", "description", "llm_model"}
_JSON = {"trigger_config", "additional_context", "delivery_config", "metadata"}
_UUID_COLUMNS = {"workspace_id", "conversation_thread_id"}


def _uuid(value: Any) -> UUID | None:
    return UUID(str(value)) if value else None


@dataclass(frozen=True)
class Write:
    op: str
    automation_id: str
    depth: int  # 1 is the save's transaction, 2 a savepoint inside it
    joined: bool  # ran on the save's connection


class _Cursor:
    def __init__(self, conn: FakeConn) -> None:
        self.connection = conn

    async def __aenter__(self) -> _Cursor:
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None

    async def execute(self, sql: str, params: Any = None) -> None:
        self.connection.db.sql.append(" ".join(sql.split()))


class FakeConn:
    def __init__(self, db: FakeAutomationsDb) -> None:
        self.db = db

    def cursor(self, **_: Any) -> _Cursor:
        return _Cursor(self)

    @contextlib.asynccontextmanager
    async def transaction(self) -> AsyncIterator[None]:
        snapshot = copy.deepcopy(self.db.rows)
        self.db.depth += 1
        try:
            yield
        except BaseException:
            self.db.rows = snapshot
            raise
        finally:
            self.db.depth -= 1


class FakeAutomationsDb:
    """The part of the automation database module a save uses."""

    def __init__(self, user_id: str, new_ids: list[str]) -> None:
        self.user_id = user_id
        self.rows: dict[str, dict[str, Any]] = {}
        self.conn = FakeConn(self)
        self.depth = 0
        self.sql: list[str] = []
        # The connection each read of rows ran on, None outside a save.
        self.reads: list[Any] = []
        self.writes: list[Write] = []
        # Runs once the save holds the lock, as a write that committed while
        # it waited would have landed.
        self.on_lock: Callable[[], None] = lambda: None
        # Runs as a save with changes locks its rows, as a write that
        # committed after the save read them would have landed.
        self.on_row_lock: Callable[[], None] = lambda: None
        self.user_timezone: str | None = None
        self._new_ids = iter(new_ids)

    def add(self, *rows: dict[str, Any]) -> None:
        for row in rows:
            stored = {"user_id": self.user_id, "owns_thread": False, **copy.deepcopy(row)}
            self.rows[str(stored["automation_id"])] = stored

    def list(self) -> list[dict[str, Any]]:
        mine = [r for r in self.rows.values() if r["user_id"] == self.user_id]
        return [copy.deepcopy(r) for r in sorted(mine, key=lambda r: r["file_name"])]

    def _filed(self, user_id: str, file_name: str) -> dict[str, Any] | None:
        return next(
            (r for r in self.rows.values() if r["user_id"] == user_id and r["file_name"] == file_name), None
        )

    def _taken(self, user_id: str, file_name: str) -> UniqueViolation:
        return UniqueViolation(f"duplicate key value: ({user_id}, {file_name}) already exists")

    def _owned(self, automation_id: str, user_id: str) -> dict[str, Any] | None:
        row = self.rows.get(str(automation_id))
        return row if row and row["user_id"] == user_id else None

    def _journal(self, op: str, automation_id: str, conn: Any) -> None:
        self.writes.append(Write(op, str(automation_id), self.depth, conn is self.conn))

    async def list_all_automations(self, user_id: str, *, conn: Any = None) -> list[dict[str, Any]]:
        self.reads.append(conn)
        return self.list()

    async def list_automation_file_names(self, user_id: str) -> list[str]:
        return [r["file_name"] for r in self.list()]

    async def get_automation_file(
        self, user_id: str, file_name: str, *, conn: Any = None, lock: bool = False
    ) -> dict[str, Any] | None:
        self.reads.append(conn)
        if lock:
            self.sql.append("SELECT automations FOR UPDATE OF automations")
            self.on_row_lock()
        row = self._filed(user_id, file_name)
        return copy.deepcopy(row) if row else None

    async def rename_automation_file(self, automation_id: str, user_id: str, file_name: str, *, conn: Any) -> bool:
        self._journal("rename", automation_id, conn)
        row = self._owned(automation_id, user_id)
        if row is None:
            return False
        if self._filed(user_id, file_name) is not None:
            raise self._taken(user_id, file_name)
        row["file_name"] = file_name
        return True

    async def get_user_timezone(self, user_id: str) -> str | None:
        return self.user_timezone

    async def lock_user_automations(self, user_id: str, *, conn: Any) -> None:
        await automation_db.lock_user_automations(user_id, conn=conn)
        self.on_lock()

    async def get_automation(self, automation_id: str, user_id: str, *, conn: Any = None) -> dict[str, Any] | None:
        row = self._owned(automation_id, user_id)
        return copy.deepcopy(row) if row else None

    async def create_automation(
        self,
        user_id: str,
        name: str,
        trigger_type: str,
        instruction: str,
        *,
        description: str | None = None,
        cron_expression: str | None = None,
        timezone: str = "UTC",
        trigger_config: dict[str, Any] | None = None,
        next_run_at: Any = None,
        workspace_id: str | None = None,
        llm_model: str | None = None,
        additional_context: list[dict[str, Any]] | None = None,
        thread_strategy: str = "new",
        conversation_thread_id: str | None = None,
        max_failures: int = 3,
        delivery_config: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        file_name: str | None = None,
        conn: Any = None,
    ) -> dict[str, Any]:
        # Derived as the database module derives it, from the names taken.
        if file_name is None:
            stem = automation_db.file_name_stem(name)
            file_name, n = f"{stem}.json", 1
            while self._filed(user_id, file_name) is not None:
                n += 1
                file_name = f"{stem}-{n}.json"
        elif self._filed(user_id, file_name) is not None:
            raise self._taken(user_id, file_name)
        automation_id = next(self._new_ids)
        self._journal("create", automation_id, conn)
        row = {
            "automation_id": UUID(automation_id),
            "user_id": user_id,
            "name": name,
            "file_name": file_name,
            "description": description,
            "status": "active",
            "trigger_type": trigger_type,
            "cron_expression": cron_expression,
            "next_run_at": next_run_at,
            "trigger_config": trigger_config or {},
            "timezone": timezone,
            "instruction": instruction,
            "workspace_id": _uuid(workspace_id),
            "thread_strategy": thread_strategy,
            "conversation_thread_id": _uuid(conversation_thread_id),
            "llm_model": llm_model,
            "additional_context": additional_context,
            "delivery_config": delivery_config or {},
            "metadata": metadata or {},
            "max_failures": max_failures,
            "failure_count": 0,
            "disable_reason": None,
            "last_execution": None,
            "owns_thread": False,
        }
        self.rows[automation_id] = row
        return copy.deepcopy(row)

    async def update_automation(
        self, automation_id: str, user_id: str, *, conn: Any = None, **kwargs: Any
    ) -> dict[str, Any] | None:
        self._journal("update", automation_id, conn)
        row = self._owned(automation_id, user_id)
        if row is None:
            return None
        for key, value in kwargs.items():
            if key in _JSON:
                if value is not None:
                    row[key] = copy.deepcopy(value)
            elif key in _PLAIN and (value is not None or key in _NULLABLE):
                row[key] = _uuid(value) if key in _UUID_COLUMNS else value
        return copy.deepcopy(row)

    async def delete_automation(self, automation_id: str, user_id: str, *, conn: Any = None) -> bool:
        self._journal("delete", automation_id, conn)
        if self._owned(automation_id, user_id) is None:
            return False
        del self.rows[str(automation_id)]
        return True
