"""Uninstall and update take the user's overrides with the plugin subagents
they name, and only those."""

from __future__ import annotations

import json
from contextlib import asynccontextmanager, nullcontext
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from src.server.database import plugin_subagents, plugins
from src.server.database.plugin_subagents import delete_plugin_subagents
from src.server.models.plugin import InstallReport
from src.server.services.plugins import lifecycle, subagent_fanout, validate_package
from src.server.services.plugins.agents import read_agent
from src.server.services.plugins.checks import CheckedServers
from src.server.services.plugins.grants import BindGrants
from tests.unit.server.mcp_builders import zip_package
from tests.unit.server.plugin_builders import MCP_SCHEMA, PLUGIN_ID, PLUGIN_SCHEMA, USER

OTHER_USER = "test-user-456"
OTHER_PLUGIN_ID = "44444444-4444-4444-4444-444444444444"

V1 = b"---\ndescription: Screens tickers.\n---\nScreen them.\n"
V2 = b"---\ndescription: Screens tickers and funds.\n---\nScreen them all.\n"


class _Db:
    """plugin_subagents, the override rows of user_subagents and the plugin's
    server rows, answering the statements the storage modules send. Anything
    else fails the test, so a rewritten query is noticed here rather than
    silently matched."""

    def __init__(self, rows: list[dict], overrides: list[dict], servers: list[dict] = ()):
        self.rows = rows
        self.overrides = overrides
        self.servers = list(servers)
        self.log: list[str] = []
        # Per open transaction, savepoints included: whether it took the lock,
        # which holds until the outermost one ends.
        self._locks: list[bool] = []
        self.transactions = 0
        self.unlocked_writes: list[str] = []

    @asynccontextmanager
    async def connection(self, _conn=None):
        yield self

    @asynccontextmanager
    async def transaction(self):
        if not self._locks:
            self.transactions += 1
        self._locks.append(False)
        self.log.append("begin")
        try:
            yield
        finally:
            self._locks.pop()
            self.log.append("end")

    def write(self, what: str) -> None:
        self.log.append(what)
        if not any(self._locks):
            self.unlocked_writes.append(what)

    @asynccontextmanager
    async def cursor(self, **_kw):
        yield _Cursor(self)

    async def execute(self, sql: str, params: tuple = ()) -> _Cursor:
        cur = _Cursor(self)
        await cur.execute(sql, params)
        return cur

    def names(self) -> dict[tuple[str, str], str]:
        return {(r["plugin_id"], r["name"]): r["file_text"] for r in self.rows}

    def keys(self) -> set[tuple[str, str]]:
        return {(o["user_id"], o["base_key"]) for o in self.overrides}


class _Cursor:
    def __init__(self, db: _Db):
        self.db = db
        self.rowcount = 0
        self._all: list[Any] = []
        self._one: Any = None

    async def fetchall(self):
        return self._all

    async def fetchone(self):
        return self._one

    async def execute(self, sql: str, params: tuple = ()) -> None:
        db = self.db
        sql = " ".join(sql.split())
        if sql.startswith("SELECT pg_advisory_xact_lock"):
            assert db._locks, "the override lock only holds inside a transaction"
            db._locks[-1] = True
            db.log.append("lock")
        elif sql.startswith("SELECT") and (
            "FROM plugin_subagents s WHERE s.user_id = %s AND s.plugin_id = %s" in sql
        ):
            user, plugin = params
            self._all = sorted(
                (dict(r) for r in db.rows if (r["user_id"], r["plugin_id"]) == (user, plugin)),
                key=lambda r: r["name"],
            )
        elif sql.startswith("SELECT name, plugin_server_key FROM user_mcp_servers"):
            user, plugin = params
            self._all = sorted(
                (
                    {"name": r["name"], "plugin_server_key": r["plugin_server_key"]}
                    for r in db.servers
                    if (r["user_id"], r["plugin_id"]) == (user, plugin)
                ),
                key=lambda r: r["name"],
            )
        elif sql.startswith("SELECT COUNT(*) FROM plugin_subagents WHERE user_id = %s"):
            self._one = (sum(r["user_id"] == params[0] for r in db.rows),)
        elif sql.startswith("INSERT INTO plugin_subagents") and (
            "ON CONFLICT (user_id, name) DO NOTHING" in sql
        ):
            user, plugin, name, fields, instructions, roles, text = params
            if any((r["user_id"], r["name"]) == (user, name) for r in db.rows):
                self._one = None
            else:
                row = _row(user, plugin, name, fields.obj, instructions, roles, text)
                db.rows.append(row)
                self._one = dict(row)
                db.write("insert subagent")
        elif sql.startswith("UPDATE plugin_subagents SET"):
            fields, instructions, roles, text, user, plugin, name = params
            hit = [r for r in db.rows if (r["user_id"], r["plugin_id"], r["name"]) == (user, plugin, name)]
            for r in hit:
                r.update(fields=fields.obj, instructions=instructions, roles=roles, file_text=text)
            self.rowcount = len(hit)
            db.write("replace subagent")
        elif sql.startswith("DELETE FROM plugin_subagents WHERE user_id = %s AND plugin_id = %s"):
            user, plugin, *only = params
            gone = [
                r for r in db.rows
                if (r["user_id"], r["plugin_id"]) == (user, plugin)
                and (not only or r["name"] == only[0])
            ]
            db.rows = [r for r in db.rows if r not in gone]
            self._all = [(r["name"],) for r in gone]
            self.rowcount = len(gone)
            db.write("delete subagents")
        elif sql.startswith("DELETE FROM user_subagents WHERE user_id = %s AND kind = 'override'"):
            user, arg = params
            if "starts_with(base_key, %s)" in sql:
                def match(key):
                    return key.startswith(arg)
            elif sql.endswith("base_key = %s"):
                def match(key):
                    return key == arg
            else:
                raise AssertionError(f"unexpected override delete: {sql}")
            gone = [
                o for o in db.overrides
                if o["user_id"] == user and o["kind"] == "override" and match(o["base_key"])
            ]
            db.overrides = [o for o in db.overrides if o not in gone]
            self.rowcount = len(gone)
            db.write("delete overrides")
        else:
            raise AssertionError(f"unexpected statement: {sql}")


def _row(
    user: str, plugin: str, name: str, fields: dict, instructions: str, roles, text: str
) -> dict:
    return {
        "user_id": user, "plugin_id": plugin, "name": name, "fields": fields,
        "instructions": instructions, "roles": roles, "file_text": text,
    }


def _stored(name: str, raw: bytes = V1, *, plugin: str = PLUGIN_ID, user: str = USER) -> dict:
    plan = read_agent(name, raw)
    return _row(user, plugin, name, plan.fields, plan.instructions, plan.roles, plan.text)


def _override(base_key: str, *, user: str = USER) -> dict:
    return {"user_id": user, "kind": "override", "base_key": base_key}


@pytest.fixture
def db(monkeypatch):
    def _make(rows, overrides, servers=()):
        fake = _Db(rows, overrides, servers)
        monkeypatch.setattr(plugin_subagents, "get_db_connection", fake.connection)
        monkeypatch.setattr(plugins, "get_db_connection", fake.connection)
        return fake

    return _make


@pytest.mark.asyncio
async def test_uninstall_drops_the_plugins_subagents_and_every_override_naming_it(db):
    fake = db(
        rows=[
            _stored("screener"),
            _stored("digest"),
            _stored("brief", plugin=OTHER_PLUGIN_ID),
        ],
        overrides=[
            _override("plugin:demo/screener"),
            # Written while the plugin lacked this subagent: still its.
            _override("plugin:demo/retired"),
            _override("plugin:demo-extra/screener"),
            _override("builtin:research"),
            _override("plugin:demo/screener", user=OTHER_USER),
        ],
    )

    async with fake.transaction():
        deleted = await delete_plugin_subagents(USER, PLUGIN_ID, "demo", conn=fake)

    assert deleted == ["digest", "screener"]
    assert fake.names() == {(OTHER_PLUGIN_ID, "brief"): V1.decode()}
    assert fake.keys() == {
        (USER, "plugin:demo-extra/screener"),
        (USER, "builtin:research"),
        (OTHER_USER, "plugin:demo/screener"),
    }
    # One transaction under the override lock, so a pane save racing the
    # uninstall either lands first and goes with it, or finds the base gone.
    assert fake.log == ["begin", "lock", "delete subagents", "delete overrides", "end"]


@pytest.mark.asyncio
async def test_uninstall_deletes_them_in_its_transaction_before_the_plugin_row():
    L = "src.server.services.plugins.lifecycle."
    conn = SimpleNamespace(transaction=nullcontext)
    order: list[str] = []
    subagents = AsyncMock(
        side_effect=lambda *a, **kw: order.append("subagents") or ["digest", "screener"]
    )
    plugin_row = AsyncMock(side_effect=lambda *a, **kw: order.append("plugin row"))
    with (
        patch(L + "plugin_fan_out_lock", lambda _: nullcontext()),
        patch(L + "oauth_fence", lambda *_: nullcontext()),
        patch(L + "get_db_connection", lambda: nullcontext(conn)),
        patch(L + "lock_plugin_row", new=AsyncMock(return_value={"name": "demo"})),
        patch(L + "list_plugin_server_names", new=AsyncMock(return_value=[])),
        patch(L + "list_plugin_skill_names", new=AsyncMock(return_value=[])),
        patch(L + "delete_plugin_subagents", new=subagents),
        patch(L + "delete_plugin_row", new=plugin_row),
    ):
        gone = await lifecycle.uninstall_plugin(
            USER, {"user_plugin_id": PLUGIN_ID, "name": "demo"}
        )

    # The cascade would take the rows on its own; the overrides name the
    # plugin rather than reference it, so only this call takes them.
    subagents.assert_awaited_once_with(USER, PLUGIN_ID, "demo", conn=conn)
    assert order == ["subagents", "plugin row"]
    assert gone.subagents == ["digest", "screener"]


def _incoming(agents: dict[str, bytes]):
    manifest = {"$schema": PLUGIN_SCHEMA, "name": "demo", "version": "2.0.0"}
    return validate_package(zip_package({
        "plugin.json": json.dumps(manifest),
        **{f"agents/{name}.md": raw for name, raw in agents.items()},
    }))


@pytest.mark.asyncio
async def test_an_update_takes_a_dropped_subagents_override_and_keeps_the_rest(db):
    fake = db(
        rows=[
            _stored("kept"),
            _stored("changed"),
            _stored("dropped"),
            _stored("stale"),
            _stored("taken", plugin=OTHER_PLUGIN_ID),
        ],
        overrides=[
            _override("plugin:demo/kept"),
            _override("plugin:demo/changed"),
            _override("plugin:demo/dropped"),
            _override("plugin:demo/stale"),
        ],
    )
    package = _incoming({
        "kept": V1,
        "changed": V2,
        # No longer reads; the installed one stays rather than vanishing.
        "stale": b"---\nmodel: inherit\n---\nNo description.\n",
        "new": V2,
        # Another plugin holds this name: reported, never written over.
        "taken": V2,
    })
    report = InstallReport()
    with (
        patch("src.server.services.plugins.subagent_fanout.reserved_subagent_names",
              return_value=frozenset()),
        patch("src.server.services.plugins.subagent_fanout.list_user_subagents",
              new=AsyncMock(return_value=[])),
    ):
        await subagent_fanout.reconcile_subagents(
            USER, PLUGIN_ID, "demo", package.agent_plans, report
        )

    assert {c.key: c.status for c in report.components} == {
        "dropped": "deleted",
        "changed": "updated",
        "kept": "unchanged",
        "stale": "skipped",
        "new": "created",
        "taken": "exists",
    }
    assert fake.names() == {
        (PLUGIN_ID, "kept"): V1.decode(),
        (PLUGIN_ID, "changed"): V2.decode(),
        (PLUGIN_ID, "stale"): V1.decode(),
        (PLUGIN_ID, "new"): V2.decode(),
        (OTHER_PLUGIN_ID, "taken"): V1.decode(),
    }
    changed = next(r for r in fake.rows if r["name"] == "changed")
    assert changed["fields"]["description"] == "Screens tickers and funds."
    # The user's change to a subagent the update keeps rides on the new base:
    # an update is no fresh install, so it clears none of them.
    assert fake.keys() == {
        (USER, "plugin:demo/kept"),
        (USER, "plugin:demo/changed"),
        (USER, "plugin:demo/stale"),
    }
    assert report.subagents_created == 1
    # One transaction, every write under the override lock a pane save takes,
    # so the save never diffs its override against a base being replaced.
    assert fake.transactions == 1
    assert fake.log[:2] == ["begin", "lock"]
    assert fake.unlocked_writes == []
    assert {"replace subagent", "insert subagent", "delete subagents"} <= set(fake.log)


@pytest.mark.asyncio
async def test_a_fresh_install_clears_overrides_an_earlier_install_left_under_its_name(db):
    """An uninstall by a build that predates dropping overrides took the rows
    by cascade and left these; applied, they would carry an order grant made
    to another package onto this one."""
    fake = db(
        rows=[],
        overrides=[
            _override("plugin:demo/screener"),
            _override("plugin:demo/retired"),
            _override("plugin:demo-extra/screener"),
            _override("builtin:research"),
            _override("plugin:demo/screener", user=OTHER_USER),
        ],
    )
    L = "src.server.services.plugins.lifecycle."
    F = "src.server.services.plugins.subagent_fanout."
    with (
        patch(L + "bundled_names", return_value=frozenset()),
        patch(L + "plugin_fan_out_lock", lambda _: nullcontext()),
        patch(L + "get_plugin", new=AsyncMock(return_value=None)),
        patch(L + "list_plugins", new=AsyncMock(return_value=[])),
        patch(
            "src.server.services.plugins.gate.resolve_bind_grants",
            new=AsyncMock(return_value=BindGrants(granted=frozenset())),
        ),
        patch(L + "list_catalog_servers", new=AsyncMock(return_value=[])),
        patch(L + "list_user_skills", new=AsyncMock(return_value=[])),
        patch(L + "list_user_plugin_subagent_names", new=AsyncMock(return_value=[])),
        patch(L + "get_user_secret_names", new=AsyncMock(return_value=[])),
        patch(L + "write_supplied", new=AsyncMock(return_value=([], []))),
        patch(
            L + "create_plugin",
            new=AsyncMock(return_value={"user_plugin_id": PLUGIN_ID, "name": "demo"}),
        ),
        patch(L + "claim_plugin_secrets", new=AsyncMock()),
        patch(L + "fan_out_servers", new=AsyncMock()),
        patch(L + "fan_out_skills", new=AsyncMock()),
        patch(L + "stamp_plugin_content_hash", new=AsyncMock(return_value=None)),
        patch(L + "after_secrets_changed", new=AsyncMock()),
        patch(L + "pending_secret_declarations", new=AsyncMock(return_value=[])),
        patch(F + "reserved_subagent_names", return_value=frozenset()),
        patch(F + "list_user_subagents", new=AsyncMock(return_value=[])),
    ):
        _, report = await lifecycle.install_plugin_package(
            USER, _incoming({"screener": V2}), source_type="zip", source_ref=None,
            checked=CheckedServers(),
        )

    assert fake.keys() == {
        (USER, "plugin:demo-extra/screener"),
        (USER, "builtin:research"),
        (OTHER_USER, "plugin:demo/screener"),
    }
    assert fake.names() == {(PLUGIN_ID, "screener"): V2.decode()}
    assert report.subagents_created == 1
    # Cleared under the lock a pane save takes, before the base it could
    # override exists, in the fan-out's own transaction.
    assert fake.log == ["begin", "lock", "delete overrides", "insert subagent", "end"]


def _server(name: str, key: str | None, *, plugin: str | None = PLUGIN_ID) -> dict:
    return {"user_id": USER, "plugin_id": plugin, "name": name, "plugin_server_key": key}


@pytest.mark.asyncio
@pytest.mark.parametrize("through", ["install", "update"])
@pytest.mark.parametrize(
    ("servers", "preferred"),
    [
        pytest.param(
            [_server("tracker", "tracker"), _server("class_server", "class")],
            ["elsewhere", "tracker", "class_server"],
            id="landed",
        ),
        # Both names were the user's own servers already, so neither entry
        # landed: the subagent keeps the names its file gives, which the pane
        # reports missing, rather than preferring servers it was never about.
        pytest.param(
            [_server("tracker", None, plugin=None), _server("class_server", None, plugin=None)],
            ["elsewhere", "plugin_demo_tracker", "class"],
            id="held-by-the-users-own",
        ),
    ],
)
async def test_a_subagent_prefers_its_plugins_server_only_where_that_server_landed(
    db, through, servers, preferred
):
    fake = db(rows=[], overrides=[], servers=servers)
    manifest = {"$schema": PLUGIN_SCHEMA, "name": "demo", "version": "1.0.0"}
    package = validate_package(zip_package({
        "plugin.json": json.dumps(manifest),
        # ``class`` is reserved in the sandbox, so its server lands renamed.
        "mcp.json": json.dumps({"$schema": MCP_SCHEMA, "mcpServers": {
            "tracker": {"type": "http", "url": "https://tracker.example.com/mcp"},
            "class": {"type": "stdio", "command": "npx"},
        }}),
        "agents/triager.md": (
            b"---\ndescription: Triages issues.\n"
            b"tools: [mcp__plugin_demo_tracker__get_issue, mcp__class__find]\n"
            b"preferred_connectors: [elsewhere]\n---\nTriage.\n"
        ),
    }))
    fan_out = (
        subagent_fanout.install_subagents if through == "install"
        else subagent_fanout.reconcile_subagents
    )
    with (
        patch("src.server.services.plugins.subagent_fanout.reserved_subagent_names",
              return_value=frozenset()),
        patch("src.server.services.plugins.subagent_fanout.list_user_subagents",
              new=AsyncMock(return_value=[])),
    ):
        await fan_out(USER, PLUGIN_ID, "demo", package.agent_plans, InstallReport())

    [row] = fake.rows
    assert row["fields"]["preferred_connectors"] == preferred


@pytest.mark.asyncio
async def test_a_server_that_lands_after_install_is_linked_without_an_update(db):
    # A held-back sse entry lands only when the user consents, after install
    # stored the subagent still naming the plugin's server.
    triager = _row(
        USER, PLUGIN_ID, "triager",
        {"preferred_connectors": ["elsewhere", "plugin_demo_tracker"]}, "Triage.", None, "a",
    )
    writer = _row(
        USER, PLUGIN_ID, "writer", {"preferred_connectors": ["elsewhere"]}, "Write.", None, "b",
    )
    fake = db(rows=[triager, writer], overrides=[], servers=[_server("tracker", "tracker")])

    await subagent_fanout.relink_subagents(USER, PLUGIN_ID, "demo")

    assert triager["fields"]["preferred_connectors"] == ["elsewhere", "tracker"]
    assert writer["fields"]["preferred_connectors"] == ["elsewhere"]
    assert fake.log.count("replace subagent") == 1
    assert fake.unlocked_writes == []
