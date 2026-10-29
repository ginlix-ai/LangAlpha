"""055 against real PostgreSQL: the data it moves, and what it leaves armed.

The unit tests plan against a mock bind, and the suite's own database is
upgraded while empty, so neither ever ran 055's writes. This seeds a database
at 054 with the shapes the plan tells apart, upgrades it, and reads back what a
user would meet: which servers run where, what each vault name now holds, what
the previous build can still write, and the renames each workspace was left
with. Then it rolls back and forward again, the documented recovery path.
"""

from __future__ import annotations

import asyncio
import json
import logging
import logging.config
import tempfile
import uuid
from pathlib import Path
from typing import Any
from urllib.parse import urlparse, urlunparse

import psycopg
import pytest
import pytest_asyncio

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

#: Its own database: this test downgrades the schema, which the shared
#: session-scoped one cannot survive.
SCRATCH_DB = "langalpha_migration_055"
KEY = "test-byok-key"

U1, U2 = "user-055-a", "user-055-b"
# u1's workspaces: A and B hold local servers and secrets, F is Flash, D is
# deleted. X is u2's.
WS = {label: str(uuid.uuid4()) for label in ("A", "B", "F", "D", "X")}
LABEL = {v: k for k, v in WS.items()}

SECRET_VALUES = {
    "same-value", "account-value", "workspace-value", "new-value",
    "asked-value", "dead-value", "third-value",
}


def _uri(dbname: str) -> str:
    from tests.integration.conftest import _build_db_uri

    parts = urlparse(_build_db_uri())
    return urlunparse(parts._replace(path=f"/{dbname}"))


async def _alembic(uri: str, action: str, revision: str) -> None:
    """Threaded, as the suite's upgrade helper is: a migration calls
    ``asyncio.run`` internally."""
    from alembic import command
    from alembic.config import Config

    root = Path(__file__).resolve().parent.parent.parent
    cfg = Config(str(root / "alembic.ini"))
    cfg.set_main_option("script_location", str(root / "migrations"))
    cfg.set_main_option(
        "sqlalchemy.url", uri.replace("postgresql://", "postgresql+psycopg://", 1)
    )
    run = command.upgrade if action == "upgrade" else command.downgrade
    await asyncio.to_thread(run, cfg, revision)


async def _execute(uri: str, statements: list[tuple[str, tuple]]) -> None:
    async with await psycopg.AsyncConnection.connect(uri, autocommit=True) as conn:
        for sql, params in statements:
            await conn.execute(sql, params)


async def _fetch(uri: str, sql: str, params: tuple = ()) -> list[tuple]:
    async with await psycopg.AsyncConnection.connect(uri, autocommit=True) as conn:
        cur = await conn.execute(sql, params)
        return await cur.fetchall()


async def _sqlstate(uri: str, sql: str, params: tuple) -> str | None:
    """The SQLSTATE a write fails with, or None when it lands."""
    async with await psycopg.AsyncConnection.connect(uri, autocommit=True) as conn:
        try:
            await conn.execute(sql, params)
        except psycopg.Error as e:
            return e.sqlstate
    return None


def _at(minute: int) -> str:
    return f"2026-01-01T00:{minute:02d}:00+00:00"


def _seed() -> list[tuple[str, tuple]]:
    fork = (
        "INSERT INTO workspace_mcp_servers "
        "(workspace_id, name, source, enabled, config, created_at) "
        "VALUES (%s, %s, 'workspace', %s, %s::jsonb, %s)"
    )
    marker = (
        "INSERT INTO workspace_mcp_servers (workspace_id, name, source, enabled) "
        "VALUES (%s, %s, %s, FALSE)"
    )
    ws_secret = (
        "INSERT INTO workspace_vault_secrets (workspace_id, name, value, created_at) "
        "VALUES (%s, %s, pgp_sym_encrypt(%s, %s), %s)"
    )
    schema = (
        "INSERT INTO workspace_mcp_tool_schemas (workspace_id, server_name, config_hash) "
        "VALUES (%s, %s, %s)"
    )
    stdio = json.dumps({"transport": "stdio", "command": "uvx"})
    return [
        ("INSERT INTO users (user_id, email) VALUES (%s, %s), (%s, %s)",
         (U1, "a@example.com", U2, "b@example.com")),
        (
            "INSERT INTO workspaces (workspace_id, user_id, name, name_key, status, config) "
            "VALUES (%s, %s, 'A', 'a', 'running', '{\"sandbox_provider\": \"docker\"}'),"
            " (%s, %s, 'B', 'b', 'stopped', '{}'),"
            " (%s, %s, 'Flash', NULL, 'flash', '{\"flash_mode\": true}'),"
            " (%s, %s, 'D', 'd', 'deleted', '{}'),"
            " (%s, %s, 'X', 'x', 'running', '{}')",
            (WS["A"], U1, WS["B"], U1, WS["F"], U1, WS["D"], U1, WS["X"], U2),
        ),
        # The account: a live server the A fork shadows, a template asking for
        # ASKED, and an OAuth connection holding the name "linear".
        (
            "INSERT INTO user_mcp_servers (user_id, name, command, env, enabled) "
            "VALUES (%s, 'notes', 'uvx', '{}', TRUE),"
            " (%s, 'research', 'uvx', '{\"K\": \"${vault:ASKED}\"}', FALSE)",
            (U1, U1),
        ),
        (
            "INSERT INTO user_mcp_oauth_connections (user_id, server_name, server_url) "
            "VALUES (%s, 'linear', 'https://linear.example.com/mcp')",
            (U1,),
        ),
        (
            "INSERT INTO user_vault_secrets (user_id, name, value) "
            "VALUES (%s, 'SAME', pgp_sym_encrypt('same-value', %s)),"
            " (%s, 'DIFF', pgp_sym_encrypt('account-value', %s))",
            (U1, KEY, U1, KEY),
        ),
        # Forks, oldest first: ran, disabled, the same name in another
        # workspace, an OAuth-held name, and a built-in name the resolver
        # skipped. The deleted workspace's one is dropped.
        (fork, (WS["A"], "notes", True, json.dumps({
            "transport": "http",
            "url": "https://notes.example.com/mcp",
            "headers": {"Authorization": "Bearer ${vault:DIFF}"},
        }), _at(1))),
        (fork, (WS["A"], "wiki", False, stdio, _at(2))),
        (fork, (WS["B"], "wiki", True, stdio, _at(3))),
        (fork, (WS["A"], "linear", True, json.dumps({
            "transport": "http", "url": "https://linear.example.com/mcp",
        }), _at(4))),
        (fork, (WS["A"], "price_data", True, stdio, _at(5))),
        (fork, (WS["D"], "ghost", True, stdio, _at(6))),
        # Rows that stay: a tombstone of the account's server, a built-in marker.
        (marker, (WS["B"], "notes", "user")),
        (marker, (WS["B"], "tavily", "builtin")),
        # Workspace vaults: an equal value, a different one, a new name, a
        # name the account's template asks for, the different value again in
        # B, and a deleted workspace's.
        (ws_secret, (WS["A"], "SAME", "same-value", KEY, _at(1))),
        (ws_secret, (WS["A"], "DIFF", "workspace-value", KEY, _at(2))),
        (ws_secret, (WS["A"], "NEW", "new-value", KEY, _at(3))),
        (ws_secret, (WS["A"], "ASKED", "asked-value", KEY, _at(4))),
        (ws_secret, (WS["B"], "DIFF", "workspace-value", KEY, _at(5))),
        (ws_secret, (WS["D"], "DEAD", "dead-value", KEY, _at(6))),
        (schema, (WS["A"], "notes", "h-notes")),
        (schema, (WS["A"], "linear", "h-linear")),
        (schema, (WS["B"], "wiki", "h-wiki")),
        (schema, (WS["D"], "ghost", "h-ghost")),
    ]


async def _off(uri: str) -> dict[str, set[str]]:
    rows = await _fetch(
        uri,
        "SELECT workspace_id, name FROM workspace_mcp_servers "
        "WHERE source = 'user' AND NOT enabled",
    )
    off: dict[str, set[str]] = {}
    for workspace_id, name in rows:
        off.setdefault(LABEL.get(str(workspace_id), str(workspace_id)), set()).add(name)
    return off


async def _servers(uri: str) -> dict[str, tuple[bool, bool, dict]]:
    rows = await _fetch(
        uri,
        "SELECT name, enabled, enabled_in_new_workspaces, headers "
        "FROM user_mcp_servers WHERE user_id = %s",
        (U1,),
    )
    return {name: (enabled, flag, headers) for name, enabled, flag, headers in rows}


async def _vault(uri: str) -> dict[str, str]:
    rows = await _fetch(
        uri,
        "SELECT name, pgp_sym_decrypt(value, %s) FROM user_vault_secrets "
        "WHERE user_id = %s",
        (KEY, U1),
    )
    return dict(rows)


async def _workspace_columns(uri: str) -> dict[str, tuple[Any, ...]]:
    rows = await _fetch(
        uri, "SELECT workspace_id, config, mcp_config_version, updated_at FROM workspaces"
    )
    return {LABEL.get(str(r[0]), str(r[0])): r[1:] for r in rows}


async def _new_workspace(uri: str, user_id: str, name: str) -> str:
    """Inserted the way the previous build does: no selection call after it."""
    workspace_id = str(uuid.uuid4())
    await _execute(uri, [(
        "INSERT INTO workspaces (workspace_id, user_id, name, name_key) "
        "VALUES (%s, %s, %s, %s)",
        (workspace_id, user_id, name, name.lower()),
    )])
    LABEL[workspace_id] = name
    return name


class _Records(logging.Handler):
    def __init__(self) -> None:
        super().__init__(logging.INFO)
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


@pytest_asyncio.fixture(scope="module")
async def run() -> dict[str, Any]:
    admin, uri = _uri("postgres"), _uri(SCRATCH_DB)

    async def _recreate(create: bool) -> None:
        async with await psycopg.AsyncConnection.connect(admin, autocommit=True) as c:
            await c.execute(f'DROP DATABASE IF EXISTS "{SCRATCH_DB}"')
            if create:
                await c.execute(f'CREATE DATABASE "{SCRATCH_DB}"')

    await _recreate(create=True)
    out: dict[str, Any] = {}
    records = _Records()
    migration_log = logging.getLogger("alembic.runtime.migration")
    try:
        with pytest.MonkeyPatch.context() as mp, tempfile.TemporaryDirectory() as tmp:
            mp.setenv("BYOK_ENCRYPTION_KEY", KEY)
            # The bundled servers as shipped, whatever config sits beside the run.
            config = Path(tmp) / "agent_config.yaml"
            config.write_text("{}\n")
            mp.setenv("PTC_CONFIG_FILE", str(config))
            await _alembic(uri, "upgrade", "054")
            await _execute(uri, _seed())
            out["before"] = await _workspace_columns(uri)

            # env.py's fileConfig would strip the handler from the logger.
            with pytest.MonkeyPatch.context() as quiet:
                quiet.setattr(logging.config, "fileConfig", lambda *a, **k: None)
                migration_log.addHandler(records)
                try:
                    await _alembic(uri, "upgrade", "055")
                finally:
                    migration_log.removeHandler(records)
            out["logs"] = records.records

            out["servers"] = await _servers(uri)
            out["off"] = await _off(uri)
            out["builtin"] = await _fetch(
                uri,
                "SELECT workspace_id::text, name FROM workspace_mcp_servers "
                "WHERE source = 'builtin'",
            )
            out["local_left"] = await _fetch(
                uri, "SELECT count(*) FROM workspace_mcp_servers WHERE source = 'workspace'"
            )
            out["vault"] = await _vault(uri)
            out["workspace_vault_left"] = await _fetch(
                uri, "SELECT count(*) FROM workspace_vault_secrets"
            )
            out["schemas"] = {
                (LABEL[str(ws)], name, digest)
                for ws, name, digest in await _fetch(
                    uri,
                    "SELECT workspace_id, server_name, config_hash "
                    "FROM workspace_mcp_tool_schemas",
                )
            }
            out["after"] = await _workspace_columns(uri)

            # The previous build, still serving.
            out["new_u1"] = await _new_workspace(uri, U1, "Overlap")
            out["new_u2"] = await _new_workspace(uri, U2, "Elsewhere")
            out["overlap_off"] = await _off(uri)
            out["guards"] = {
                "local server": await _sqlstate(
                    uri,
                    "INSERT INTO workspace_mcp_servers "
                    "(workspace_id, name, source, enabled, config) "
                    "VALUES (%s, 'late', 'workspace', TRUE, '{}')",
                    (WS["A"],),
                ),
                "workspace secret": await _sqlstate(
                    uri,
                    "INSERT INTO workspace_vault_secrets (workspace_id, name, value) "
                    "VALUES (%s, 'LATE', pgp_sym_encrypt('late', %s))",
                    (WS["A"], KEY),
                ),
                "tombstone": await _sqlstate(
                    uri,
                    "INSERT INTO workspace_mcp_servers "
                    "(workspace_id, name, source, enabled, config) "
                    "VALUES (%s, 'research', 'user', FALSE, NULL)",
                    (WS["A"],),
                ),
                "builtin marker": await _sqlstate(
                    uri,
                    "INSERT INTO workspace_mcp_servers (workspace_id, name, source, enabled) "
                    "VALUES (%s, 'tavily', 'builtin', FALSE)",
                    (WS["A"],),
                ),
            }

            # Rolled back: the previous build writes its own rows again, then
            # this build upgrades once more.
            await _alembic(uri, "downgrade", "054")
            out["rolled_back_writes"] = {
                "local server": await _sqlstate(
                    uri,
                    "INSERT INTO workspace_mcp_servers "
                    "(workspace_id, name, source, enabled, config) "
                    "VALUES (%s, 'journal', 'workspace', TRUE, %s::jsonb)",
                    (WS["B"], json.dumps({"transport": "stdio", "command": "uvx"})),
                ),
                "workspace secret": await _sqlstate(
                    uri,
                    "INSERT INTO workspace_vault_secrets (workspace_id, name, value) "
                    "VALUES (%s, 'DIFF', pgp_sym_encrypt('third-value', %s))",
                    (WS["B"], KEY),
                ),
            }
            out["new_rolled_back"] = await _new_workspace(uri, U1, "Rolled back")
            out["rolled_back_off"] = await _off(uri)

            await _alembic(uri, "upgrade", "055")
            out["again_servers"] = await _servers(uri)
            out["again_vault"] = await _vault(uri)
            out["again"] = await _workspace_columns(uri)
            out["triggers"] = {
                name for (name,) in await _fetch(
                    uri, "SELECT tgname FROM pg_trigger WHERE tgname LIKE 'trg_workspace%%'"
                )
            }
        yield out
    finally:
        await _recreate(create=False)


class TestServers:
    async def test_each_fork_becomes_a_user_server_on_only_if_it_ran(self, run):
        """A fork that was off, or skipped under a built-in name, is off on the
        account too, so the Plugins page never probes its endpoint."""
        assert {
            name: (enabled, flag) for name, (enabled, flag, _h) in run["servers"].items()
        } == {
            "notes": (True, True),
            "research": (False, True),
            "notes_2": (True, False),
            "wiki": (False, False),
            "wiki_2": (True, False),
            "linear_2": (True, False),
            "price_data_2": (False, False),
        }
        assert run["local_left"] == [(0,)]

    async def test_each_runs_where_it_ran_and_nowhere_else(self, run):
        assert run["off"] == {
            # The shadowed account server and OAuth name stay off where the
            # local copy replaced them.
            "A": {"notes", "wiki", "wiki_2", "linear", "price_data_2"},
            "B": {"notes", "notes_2", "wiki", "linear_2", "price_data_2"},
            "F": {"notes_2", "wiki", "wiki_2", "linear_2", "price_data_2"},
        }
        assert run["builtin"] == [(WS["B"], "tavily")]

    async def test_a_promoted_servers_refs_follow_its_workspaces_renamed_secret(self, run):
        assert run["servers"]["notes_2"][2] == {"Authorization": "Bearer ${vault:DIFF_2}"}

    async def test_snapshots_follow_a_rename_and_go_when_refs_moved(self, run):
        assert run["schemas"] == {
            ("A", "linear_2", "h-linear"),
            ("B", "wiki_2", "h-wiki"),
        }


class TestVault:
    async def test_values_land_under_names_that_never_change_what_a_reader_saw(self, run):
        assert run["vault"] == {
            "SAME": "same-value",
            "DIFF": "account-value",
            "DIFF_2": "workspace-value",
            "NEW": "new-value",
            # The account's template asks for ASKED and never saw A's value.
            "ASKED_2": "asked-value",
        }
        assert run["workspace_vault_left"] == [(0,)]


class TestRenames:
    async def test_each_workspace_keeps_its_renames_beside_its_config(self, run):
        config = {label: cols[0] for label, cols in run["after"].items()}
        assert config["A"] == {
            "sandbox_provider": "docker",
            "mcp_migration_renames": {
                "secrets": {"DIFF": "DIFF_2", "ASKED": "ASKED_2"},
                "servers": {
                    "notes": "notes_2",
                    "linear": "linear_2",
                    "price_data": "price_data_2",
                },
            },
        }
        assert config["B"]["mcp_migration_renames"] == {
            "secrets": {"DIFF": "DIFF_2"},
            "servers": {"wiki": "wiki_2"},
        }
        assert "mcp_migration_renames" not in config["F"]
        assert "mcp_migration_renames" not in config["X"]

    async def test_renames_are_warned_by_name_never_by_value(self, run):
        warnings = [r.getMessage() for r in run["logs"] if r.levelno == logging.WARNING]
        (a,) = [w for w in warnings if WS["A"] in w]
        assert "notes -> notes_2" in a and "DIFF -> DIFF_2" in a
        assert any(WS["B"] in w and "wiki -> wiki_2" in w for w in warnings)
        messages = " ".join(r.getMessage() for r in run["logs"])
        assert not any(value in messages for value in SECRET_VALUES)
        assert KEY not in messages

    async def test_the_bump_leaves_the_gallery_order_alone(self, run):
        for label in ("A", "B", "F", "D"):
            assert run["after"][label][1] == run["before"][label][1] + 1
            assert run["after"][label][2] == run["before"][label][2]
        assert run["after"]["X"][1:] == run["before"]["X"][1:]


class TestPreviousBuild:
    async def test_its_new_workspace_starts_with_the_promoted_servers_off(self, run):
        assert run["overlap_off"]["Overlap"] == {
            "notes_2", "wiki", "wiki_2", "linear_2", "price_data_2",
        }
        assert "Elsewhere" not in run["overlap_off"]

    async def test_its_local_writes_are_refused_and_selection_rows_pass(self, run):
        assert run["guards"] == {
            "local server": "0A000",
            "workspace secret": "0A000",
            "tombstone": None,
            "builtin marker": None,
        }

    async def test_after_a_rollback_it_writes_again_and_still_gets_the_selection(self, run):
        assert run["rolled_back_writes"] == {
            "local server": None,
            "workspace secret": None,
        }
        assert run["rolled_back_off"]["Rolled back"] == {
            "notes_2", "wiki", "wiki_2", "linear_2", "price_data_2",
        }

    async def test_upgrading_again_moves_what_it_wrote_and_adds_to_the_renames(self, run):
        assert run["again_servers"]["journal"][:2] == (True, False)
        assert run["again_vault"]["DIFF_3"] == "third-value"
        config = {label: cols[0] for label, cols in run["again"].items()}
        assert config["B"]["mcp_migration_renames"] == {
            "secrets": {"DIFF": "DIFF_3"},
            "servers": {"wiki": "wiki_2"},
        }
        assert config["A"]["mcp_migration_renames"]["secrets"] == {
            "DIFF": "DIFF_2", "ASKED": "ASKED_2",
        }
        assert run["triggers"] >= {
            "trg_workspace_mcp_servers_retired",
            "trg_workspace_vault_secrets_retired",
            "trg_workspaces_start_mcp_selection",
        }
