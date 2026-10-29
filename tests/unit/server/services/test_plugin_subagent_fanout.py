"""Who holds a plugin subagent's name: the deployment, then the bundles, then
plugins in install order, at install and in every user's roster alike."""

from __future__ import annotations

import json
from contextlib import asynccontextmanager, contextmanager, nullcontext
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.agent.subagents.merge import (
    add_plugin_bases,
    base_subagents,
    merge_subagents,
)
from ptc_agent.config import plugins as bundle_reader
from ptc_agent.config.agent import SubagentsConfig
from src.server.database.account_disables import AccountDisables
from src.server.database.plugin_subagents import (
    MAX_PLUGIN_SUBAGENTS_PER_USER,
    plugin_key,
)
# Bound at import, before the suite's autouse stub replaces the module's
# attribute with a reader of no rows.
from src.server.database.plugin_subagents import (
    list_user_plugin_subagents as read_plugin_rows,
)
from src.server.models.plugin import InstallReport
from src.server.services.plugins import bundled, lifecycle
from src.server.services.plugins.agents import collect_agents, read_agent
from src.server.services.plugins.extension import LangalphaExtension
from src.server.services.plugins.grants import BindGrants
from src.server.services.plugins.subagent_fanout import (
    fan_out_subagents,
    reserved_subagent_names,
)
from src.server.services.subagents.plugin_bases import plugin_bases

USER = "test-user-123"
PLUGIN_ID = "22222222-2222-2222-2222-222222222222"

M = "src.server.services.plugins.subagent_fanout."
# The install's or update's locked transaction; the fan-out only passes it on.
CONN = object()


def _md(description: str | None = "Does a thing.") -> bytes:
    front = f"description: {description}" if description else "model: inherit"
    return f"---\n{front}\n---\nDo it.\n".encode()


def _plans(*names: str, broken: tuple[str, ...] = ()):
    files = {f"agents/{n}.md": _md() for n in names}
    files |= {f"agents/{n}.md": _md(None) for n in broken}
    plans, _ = collect_agents(files)
    return sorted(plans, key=lambda p: [*names, *broken].index(p.name))


@contextmanager
def _account(*, reserved=frozenset(), mine=(), held=0, insert=None):
    """Patch what the fan-out reads; ``mine`` are the user's subagent rows."""
    insert = insert or AsyncMock(side_effect=lambda u, p, row, t, **_kw: {"name": row["name"]})
    with (
        patch(M + "reserved_subagent_names", return_value=frozenset(reserved)),
        patch(M + "list_user_subagents", new=AsyncMock(return_value=list(mine))),
        patch(M + "count_plugin_subagents", new=AsyncMock(return_value=held)),
        patch(M + "insert_plugin_subagent", new=insert),
    ):
        yield insert


def _statuses(report: InstallReport) -> list[tuple[str, str]]:
    return [(c.key, c.status) for c in report.components]


class TestFanOut:
    @pytest.mark.asyncio
    async def test_each_plan_lands_or_is_reported_in_plan_order(self):
        plans = _plans("good", "research", "mine", "raced", broken=("broken",))
        insert = AsyncMock(
            side_effect=lambda u, p, row, t, **_kw: None if row["name"] == "raced" else row
        )
        report = InstallReport()
        with _account(
            reserved={"research"},
            # An override is a change to some base, not a subagent of the
            # user's own, so only the custom row holds a name.
            mine=[{"name": "mine", "kind": "custom"}, {"name": "good", "kind": "override"}],
            insert=insert,
        ):
            await fan_out_subagents(USER, PLUGIN_ID, plans, report, conn=CONN)

        assert _statuses(report) == [
            ("good", "created"),
            ("research", "skipped"),
            ("mine", "exists"),
            # Taken between the install's refusal and this write.
            ("raced", "exists"),
            ("broken", "skipped"),
        ]
        assert {(d.target, d.code) for d in report.diagnostics} == {
            ("research", "reserved_name"),
            ("broken", "missing_description"),
        }
        good = plans[0]
        assert [c.args for c in insert.await_args_list] == [
            (USER, PLUGIN_ID, good.row, good.text),
            (USER, PLUGIN_ID, plans[3].row, plans[3].text),
        ]
        # Every write in the caller's locked transaction, never a connection
        # of its own a pane save could interleave with.
        assert {tuple(c.kwargs.items()) for c in insert.await_args_list} == {(("conn", CONN),)}
        assert report.subagents_created == 1
        # A name someone else holds may be freed later; the hash must not
        # claim the tree landed whole.
        assert not report.landed_whole

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("held", "created"),
        [
            pytest.param(MAX_PLUGIN_SUBAGENTS_PER_USER - 2, 2, id="room-for-two"),
            pytest.param(MAX_PLUGIN_SUBAGENTS_PER_USER, 0, id="at-the-cap"),
        ],
    )
    async def test_the_cap_counts_every_plugin_and_reports_the_rest(self, held, created):
        assert MAX_PLUGIN_SUBAGENTS_PER_USER == 50
        plans = _plans("one", "two", "three")
        report = InstallReport()
        with _account(held=held) as insert:
            await fan_out_subagents(USER, PLUGIN_ID, plans, report, conn=CONN)

        assert insert.await_count == created
        assert report.subagents_created == created
        assert [c.status for c in report.components] == (
            ["created"] * created + ["error"] * (3 - created)
        )
        assert [(d.level, d.code) for d in report.diagnostics] == (
            [("error", "cap_reached")] * (3 - created)
        )


class _PastTheRefusal(Exception):
    """Raised by the plugin-row write, which runs only once install is allowed."""


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("plans", "refused"),
    [
        pytest.param(_plans("digest", "screener"), True, id="refused"),
        # A file that would not install never takes the name.
        pytest.param(_plans("digest", broken=("screener",)), False, id="broken-file"),
    ],
)
async def test_a_name_another_plugin_holds_refuses_the_whole_install(plans, refused):
    package = SimpleNamespace(
        name="incoming-plugin", version="1.0.0", manifest={}, mcp_document=None,
        mcp_document_invalid=False, extension=LangalphaExtension(),
        entry_plans=[], skill_plans=[], agent_plans=plans,
    )
    held = [{"name": "screener", "plugin_name": "first-plugin"}]
    L = "src.server.services.plugins.lifecycle."
    create = AsyncMock(side_effect=_PastTheRefusal)
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
        patch(L + "list_user_plugin_subagent_names", new=AsyncMock(return_value=held)),
        patch(L + "get_user_secret_names", new=AsyncMock(return_value=[])),
        patch(L + "create_plugin", new=create),
    ):
        expected = (
            pytest.raises(ValueError, match="'screener'.*'first-plugin'")
            if refused else pytest.raises(_PastTheRefusal)
        )
        with expected:
            await lifecycle.install_plugin_package(
                USER, package, source_type="zip", source_ref=None
            )
    assert create.await_count == (0 if refused else 1)


def test_reserved_names_are_every_deployment_subagent_and_every_bundles():
    deployment = SimpleNamespace(subagents=SubagentsConfig(enabled=["general-purpose"]))
    bundle_plan = read_agent("market-brief", _md())
    with (
        patch("src.server.app.setup.agent_config", deployment),
        patch.object(bundled, "bundled_agent_plans", return_value=[("house", bundle_plan)]),
    ):
        reserved = reserved_subagent_names()
    # research is defined but not offered here: it still keeps its name, so
    # turning it on later cannot collide with a plugin's.
    assert {"general-purpose", "research", "market-brief"} <= reserved


def _row(plugin: str, name: str, *, on: bool = True) -> dict:
    """A plugin_subagents row as the roster reads it, joined to its plugin."""
    plan = read_agent(name, _md(f"The {name} subagent of {plugin}."))
    return {**plan.row, "plugin_name": plugin, "plugin_enabled": on}


@contextmanager
def _plugin_rows(rows, *, bundles=(), off_bundles=frozenset()):
    with (
        patch(
            "src.server.database.plugin_subagents.list_user_plugin_subagents",
            new=AsyncMock(return_value=rows),
        ),
        patch.object(bundled, "bundled_agent_plans", return_value=list(bundles)),
        patch(
            "src.server.database.account_disables.list_account_disables",
            new=AsyncMock(return_value=AccountDisables(
                servers=frozenset(), bundles=frozenset(off_bundles),
            )),
        ),
    ):
        yield


@pytest.mark.asyncio
async def test_the_roster_gives_a_name_to_the_deployment_then_bundles_then_install_order():
    deployment = SubagentsConfig(enabled=["general-purpose"])
    rows = [
        _row("first", "research"),
        _row("first", "screener"),
        _row("first", "digest"),
        _row("second", "digest"),
        _row("second", "later", on=False),
    ]
    with _plugin_rows(rows, bundles=[("house", read_agent("screener", _md()))]):
        offered_by_plugins = await plugin_bases(USER)
    bases, offered = add_plugin_bases(
        base_subagents(deployment), deployment.enabled, offered_by_plugins
    )

    assert bases["research"].key == "builtin:research"
    assert bases["screener"].key == "plugin:house/screener"
    assert bases["digest"].key == "plugin:first/digest"
    # A switched-off plugin's subagent stays a base, so the user's changes to
    # it wait for the plugin, but it is not offered.
    assert bases["later"].key == "plugin:second/later"
    assert offered == ("general-purpose", "screener", "digest")


@pytest.mark.asyncio
async def test_every_read_stays_on_the_callers_connection():
    """A turn's snapshot and a writer's lock hold one pooled connection; a read
    that checked out its own beside it could wait out the pool timeout."""
    conn = object()
    disables = AsyncMock(return_value=AccountDisables(servers=frozenset(), bundles=frozenset()))
    rows = AsyncMock(return_value=[])
    with (
        patch("src.server.database.account_disables.list_account_disables", new=disables),
        patch("src.server.database.plugin_subagents.list_user_plugin_subagents", new=rows),
        patch.object(
            bundled, "bundled_agent_plans", return_value=[("house", read_agent("screener", _md()))]
        ),
    ):
        await plugin_bases(USER, conn=conn)
    disables.assert_awaited_once_with(USER, conn=conn)
    rows.assert_awaited_once_with(USER, conn=conn)


@pytest.mark.asyncio
async def test_installed_plugins_are_read_per_user_in_install_order():
    cursor = MagicMock()
    cursor.execute = AsyncMock()
    cursor.fetchall = AsyncMock(return_value=[])

    @asynccontextmanager
    async def _cursor(**_kw):
        yield cursor

    @asynccontextmanager
    async def _connection(_conn=None):
        yield SimpleNamespace(cursor=_cursor)

    with patch("src.server.database.plugin_subagents.get_db_connection", new=_connection):
        await read_plugin_rows(USER)

    sql, params = cursor.execute.await_args.args
    assert "WHERE s.user_id = %s" in sql
    assert "ORDER BY p.installed_at" in sql
    assert params == (USER,)


@pytest.mark.asyncio
async def test_an_override_reaches_the_subagent_its_key_names():
    with _plugin_rows([_row("demo", "screener")]):
        bases, offered = add_plugin_bases({}, (), await plugin_bases(USER))
    assert plugin_key("demo", "screener") == "plugin:demo/screener"
    assert bases["screener"].definition.source == "plugin"

    def _override(base_key: str) -> dict:
        return {
            "kind": "override", "name": "screener", "base_key": base_key,
            "workspace_id": None, "fields": {"model": "vendor-model-1"},
        }

    # The key names the plugin, so an override of one plugin's subagent never
    # reshapes another plugin's subagent of the same name.
    applied = merge_subagents(bases, offered, [_override("plugin:demo/screener")])
    assert applied.definitions["screener"].model == "vendor-model-1"
    assert applied.orphaned == ()

    elsewhere = merge_subagents(bases, offered, [_override("plugin:other/screener")])
    assert elsewhere.definitions["screener"].model is None
    assert elsewhere.orphaned == ("screener",)


@pytest.mark.asyncio
@pytest.mark.parametrize(("off", "on"), [((), True), (("house-bundle",), False)])
async def test_a_bundles_subagents_reach_every_user_unless_their_account_switched_it_off(
    monkeypatch, request, tmp_path: Path, off, on
):
    root = tmp_path / "plugins"
    agents = root / "house" / "agents"
    agents.mkdir(parents=True)
    (root / "house" / "plugin.json").write_text(json.dumps({
        "$schema": "https://agent-plugins.org/schemas/1.0.0/plugin.schema.json",
        "name": "house-bundle",
        "version": "1.0.0",
    }))
    (agents / "brief.md").write_bytes(_md())
    (agents / "broken.md").write_bytes(_md(None))
    (agents / "notes.txt").write_text("not a subagent")
    monkeypatch.setattr(bundle_reader, "BUNDLES_DIR", root)
    monkeypatch.setattr(bundled, "BUNDLES_DIR", root)
    # Read once per process, so this tree is read fresh and forgotten after.
    bundled.bundled_agent_plans.cache_clear()
    request.addfinalizer(bundled.bundled_agent_plans.cache_clear)

    # Named by the manifest, which is the name the API answers to; the
    # directory is only where the files sit.
    assert [(b, p.name) for b, p in bundled.bundled_agent_plans()] == [
        ("house-bundle", "brief")
    ]
    with patch(
        "src.server.database.account_disables.list_account_disables",
        new=AsyncMock(return_value=AccountDisables(
            servers=frozenset(), bundles=frozenset(off),
        )),
    ):
        got = await plugin_bases(USER)
    assert [(base.key, enabled) for base, enabled in got] == [
        ("plugin:house-bundle/brief", on)
    ]
