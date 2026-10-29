"""The in-memory table a save runs against, and the route it goes through."""

from __future__ import annotations

import contextlib
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest

from ptc_agent.agent.backends import db_json_route
from ptc_agent.agent.backends.automations import AutomationsBackend
from src.server.services.automations import file, lifecycle
from src.server.services.llm.user_models import ModelSource
from tests.unit.server.services.automations._fake_db import FakeAutomationsDb
from tests.unit.server.services.automations.file._support import (
    CALL,
    CREATED,
    CREATED_NEXT,
    FOREIGN_WORKSPACE,
    HOME,
    NEXT_RUN,
    PINNED,
    STRANGER,
    THREAD,
    USER,
    WORKSPACE,
    _backend,
)


@pytest.fixture
def db(monkeypatch) -> FakeAutomationsDb:
    """The user's rows, with what the lifecycle reads besides them faked
    around its real rules. The lifecycle's writes are spied, not replaced."""
    db = FakeAutomationsDb(USER, [CREATED, CREATED_NEXT])

    @contextlib.asynccontextmanager
    async def connection():
        yield db.conn

    workspaces = {WORKSPACE: USER, FOREIGN_WORKSPACE: STRANGER, HOME: USER}
    # Each thread's owner and the workspace it is in: this conversation is in
    # the workspace the agent writes from, and the pinned one in Home, where
    # the fixtures' rows run.
    threads = {THREAD: (USER, WORKSPACE), PINNED: (USER, HOME)}

    async def get_workspace(workspace_id: str, conn: Any = None) -> dict[str, Any] | None:
        owner = workspaces.get(workspace_id)
        return {"workspace_id": workspace_id, "user_id": owner} if owner else None

    async def get_thread_owner_id(thread_id: str, *, conn: Any = None) -> str | None:
        return threads[thread_id][0] if thread_id in threads else None

    async def get_thread_auth_meta(thread_id: str, *, conn: Any = None) -> dict[str, Any] | None:
        if thread_id not in threads:
            return None
        owner, workspace_id = threads[thread_id]
        return {"user_id": owner, "workspace_id": UUID(workspace_id)}

    scheduler = MagicMock()
    scheduler.calculate_first_run.return_value = NEXT_RUN

    def outside_the_save(result: Any) -> AsyncMock:
        # Each read notes whether a save's transaction was open around it.
        async def read(*_: Any) -> Any:
            db.depths_read_at.append(db.depth)
            return result

        return AsyncMock(side_effect=read)

    db.depths_read_at = []
    models = MagicMock()
    models.ModelSource = ModelSource
    models.get_model_preference = outside_the_save({"custom_models": [{"name": "my-local"}]})
    models.classify_model = AsyncMock(
        side_effect=lambda uid, name, _pref_cache=None: (
            (ModelSource.SYSTEM, {}) if name in ("model-a", "my-local") else (ModelSource.UNKNOWN, {})
        )
    )
    models.get_custom_provider_config = AsyncMock(return_value=None)

    monkeypatch.setattr(file, "auto_db", db)
    monkeypatch.setattr(lifecycle, "auto_db", db)
    monkeypatch.setattr(db_json_route, "get_db_connection", connection)
    db.get_user_timezone = outside_the_save("Europe/Paris")
    monkeypatch.setattr(file, "user_models", models)
    monkeypatch.setattr(lifecycle, "get_workspace", get_workspace)
    monkeypatch.setattr("src.server.database.conversation.get_thread_owner_id", get_thread_owner_id)
    monkeypatch.setattr("src.server.database.conversation.get_thread_auth_meta", get_thread_auth_meta)
    monkeypatch.setattr(lifecycle, "AutomationScheduler", scheduler)
    monkeypatch.setattr(lifecycle, "settings", MagicMock(AUTOMATION_WEBHOOK_URL="https://hooks.example.com/x"))
    monkeypatch.setattr(lifecycle, "user_models", models)
    monkeypatch.setattr(lifecycle, "get_configured_llm_models", lambda: {"vendor": ["model-b", "model-a"]})
    for name in ("create_automation", "update_automation", "pause_automation", "resume_automation"):
        monkeypatch.setattr(lifecycle, name, AsyncMock(wraps=getattr(lifecycle, name)))
    return db


@pytest.fixture
def backend(db) -> AutomationsBackend:
    return _backend(CALL)
