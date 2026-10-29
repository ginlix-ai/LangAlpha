"""One user's subagents in memory: the rows, the bases they sit on, and the
facts the checks read, faked at the seams the service reads them through.

The rules, the merge, the views, the versions and both writers run for real,
so a test here exercises what the pane and the agent's files share.
"""

from __future__ import annotations

import contextlib
import copy
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

import pytest

from ptc_agent.agent.subagents.definition import SubagentDefinition
from ptc_agent.agent.subagents.merge import BaseSubagent
from src.server.services.subagents.context import Connector, SubagentContext

USER = "user-fake-1"
THREAD = "00000000-0000-4000-8000-00000000bbbb"
STAMP = "2026-10-14T10:00:00+00:00"

# Models the fake user's list knows, each with the effort levels it offers.
LADDERS: dict[str, tuple[str, ...]] = {
    "model-a": ("low", "medium", "high"),
    "model-flat": (),
    "model-off-plan": ("low", "high"),
    "model-pinned": ("low", "high"),
}
ON_PLAN = frozenset({"model-a", "model-flat"})
SKILLS = frozenset({"sec-filings", "dcf"})
CONNECTORS = (
    Connector("fundamentals", "builtin", "Fundamentals", True),
    Connector("my-broker", "brokerage", "Broker", True),
)


def research_base(**changes: Any) -> BaseSubagent:
    defn = SubagentDefinition(
        name="research",
        description="Finds and cites sources on the web.",
        role_prompt="Search widely, then cite every claim.",
        tools=["web_search", "filesystem"],
        source="builtin",
    )
    for key, value in changes.items():
        setattr(defn, key, value)
    return BaseSubagent("builtin:research", defn)


def pinned_base() -> BaseSubagent:
    """A base whose own model is one the user's plan doesn't carry."""
    return BaseSubagent("builtin:data-prep", SubagentDefinition(
        name="data-prep",
        description="Cleans and shapes data.",
        role_prompt="Load, clean, save.",
        tools=["execute_code", "filesystem", "bash", "finance"],
        model="model-pinned",
        reasoning_effort="low",
        source="builtin",
    ))


def dormant_base() -> BaseSubagent:
    """Defined by the deployment but not offered to this user."""
    return BaseSubagent("deployment:dormant", SubagentDefinition(
        name="dormant", description="Left out by the operator.", role_prompt="Idle.",
        tools=["web_search"], source="user",
    ))


def plugin_base() -> BaseSubagent:
    """A plugin's subagent whose plugin is off."""
    return BaseSubagent("plugin:toolkit/filings-reader", SubagentDefinition(
        name="filings-reader", description="Reads filings.", role_prompt="Read.",
        tools=["web_search"], source="plugin",
    ))


@dataclass
class FakeContext(SubagentContext):
    """The per-request context with the user's model and plan facts answered
    from the tables above rather than their stores. Search runs the real gate
    on the manifest, ungated by plan."""

    ladders: Mapping[str, tuple[str, ...]] = field(default_factory=lambda: dict(LADDERS))
    on_plan: frozenset[str] = ON_PLAN

    def model_known(self, name: str) -> bool:
        return name in self.ladders

    async def model_on_plan(self, name: str) -> bool:
        return name in self.on_plan

    async def model_has_key(self, name: str) -> bool:
        return True

    def effort_ladder(self, name: str) -> tuple[str, ...]:
        return self.ladders.get(name, ())

    async def model_options(self) -> list[str]:
        return sorted(self.on_plan)

    def main_values(self) -> dict[str, Any]:
        return {"model": "model-a", "effort": "medium", "search_provider": "tavily", "search_depth": "standard"}


class _Conn:
    """A save's connection: its transaction rolls the rows back on a raise,
    and taking or releasing the advisory lock is the moment a test can move
    the world around it."""

    def __init__(self, world: World) -> None:
        self.world = world
        self.locked = False

    @contextlib.asynccontextmanager
    async def transaction(self):
        before = copy.deepcopy(self.world.rows)
        try:
            yield
        except BaseException:
            self.world.rows = before
            raise
        if self.locked and self.world.on_release is not None:
            hook, self.world.on_release = self.world.on_release, None
            hook()

    async def execute(self, sql: str, params: Any = None) -> None:
        self.locked = True
        self.world.locks += 1
        hook, self.world.on_lock = self.world.on_lock, None
        if hook is not None:
            hook()


class World:
    """The bases, the offered names and the user's rows, by name."""

    def __init__(self) -> None:
        self.bases: dict[str, BaseSubagent] = {
            b.definition.name: b for b in (research_base(), pinned_base(), dormant_base(), plugin_base())
        }
        self.offered: tuple[str, ...] = ("research", "data-prep")
        self.rows: dict[str, dict[str, Any]] = {}
        self.locks = 0
        # Runs once, as the next writer takes the user's lock.
        self.on_lock: Callable[[], None] | None = None
        # Runs once, as the next writer to take it commits and lets it go.
        self.on_release: Callable[[], None] | None = None

    # --- seeding ---

    def custom(self, name: str, *, instructions: str = "Do the work.", roles: list[str] | None = None,
               updated_by: str = "user", **fields: Any) -> dict[str, Any]:
        fields.setdefault("description", f"Handles {name} work.")
        return self._put(name, kind="custom", base_key=None, fields=fields, instructions=instructions,
                         roles=roles or ["analyst", "chief_of_staff"], updated_by=updated_by)

    def override(self, name: str, *, base_key: str | None = None, **fields: Any) -> dict[str, Any]:
        key = base_key or (self.bases[name].key if name in self.bases else f"builtin:{name}")
        return self._put(name, kind="override", base_key=key, fields=fields, instructions=None,
                         roles=None, updated_by="user")

    def _put(self, name: str, **row: Any) -> dict[str, Any]:
        self.rows[name] = {"name": name, "workspace_id": None, "updated_thread_id": None,
                           "updated_at": STAMP, **row}
        return self.rows[name]

    # --- what the service reads ---

    def ctx(self) -> FakeContext:
        return FakeContext(
            user_id=USER, bases=dict(self.bases), offered=tuple(self.offered),
            model_pref={}, is_byok=False, permits=None,
            search_provider="tavily", search_depth="standard",
            skills=SKILLS, connectors=CONNECTORS,
            brokerages_connected=True, oauth_accounts={},
        )

    async def load_context(self, user_id: str) -> FakeContext:
        return self.ctx()

    async def roster(self, user_id: str, subagents: Any = None, *, conn: Any = None):
        return dict(self.bases), tuple(self.offered)

    @contextlib.asynccontextmanager
    async def connection(self, conn: Any = None):
        yield conn or _Conn(self)

    # --- user_subagents ---

    async def list_user_subagents(self, user_id: str, *, conn: Any = None) -> list[dict[str, Any]]:
        return [copy.deepcopy(self.rows[n]) for n in sorted(self.rows)]

    async def upsert_user_subagent(self, user_id: str, name: str, *, kind: str, fields: dict[str, Any],
                                   updated_by: str, base_key: str | None = None,
                                   instructions: str | None = None, roles: list[str] | None = None,
                                   updated_thread_id: str | None = None, conn: Any = None) -> dict[str, Any]:
        self._put(name, kind=kind, base_key=base_key, fields=copy.deepcopy(fields),
                  instructions=instructions, roles=list(roles) if roles is not None else None,
                  updated_by=updated_by)
        self.rows[name]["updated_thread_id"] = updated_thread_id
        return copy.deepcopy(self.rows[name])

    async def delete_user_subagent(self, user_id: str, name: str, *, conn: Any = None) -> bool:
        return self.rows.pop(name, None) is not None

    async def rename_user_subagent(self, user_id: str, name: str, new_name: str, *, updated_by: str,
                                   updated_thread_id: str | None = None, conn: Any = None):
        row = self.rows.get(name)
        if row is None or row["kind"] != "custom":
            return None
        del self.rows[name]
        row.update(name=new_name, updated_by=updated_by, updated_thread_id=updated_thread_id)
        self.rows[new_name] = row
        return copy.deepcopy(row)


def install(monkeypatch: pytest.MonkeyPatch) -> World:
    """Point the service's reads and writes at a fresh ``World``."""
    from ptc_agent.agent.backends import db_json_route
    from src.server.database import pool, user_subagents
    from src.server.database.conversation import threads_read
    from src.server.services.subagents import file, writes

    world = World()
    monkeypatch.setattr(writes, "load_context", world.load_context)
    monkeypatch.setattr(file, "load_context", world.load_context)
    monkeypatch.setattr(writes, "roster", world.roster)
    monkeypatch.setattr(pool, "get_db_connection", world.connection)
    monkeypatch.setattr(db_json_route, "get_db_connection", world.connection)
    for name in ("list_user_subagents", "upsert_user_subagent",
                 "delete_user_subagent", "rename_user_subagent"):
        monkeypatch.setattr(user_subagents, name, getattr(world, name))

    async def no_titles(thread_ids: list[str], user_id: str) -> list[dict[str, Any]]:
        return []

    async def main_values(user_id: str) -> dict[str, Any]:
        return world.ctx().main_values()

    async def connectors(user_id: str):
        return CONNECTORS

    monkeypatch.setattr(threads_read, "get_thread_labels", no_titles)
    monkeypatch.setattr(file, "load_main_values", main_values)
    monkeypatch.setattr(file, "list_connectors", connectors)
    return world


async def views(world: World) -> dict[str, dict[str, Any]]:
    """Every view as the pane reads it, problems included, by name."""
    from src.server.models.subagent import SubagentView
    from src.server.services.subagents.writes import list_subagents

    _, out = await list_subagents(USER, ctx=world.ctx())
    # Through the wire model too, so a problem code the response can't carry fails here.
    return {SubagentView.model_validate(v).name: v for v in out}
