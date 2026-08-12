"""Unit tests for the shared MCP discovery service.

Two boundaries are exercised:

- ``sanitize_discovered_tools`` — the hostile-input boundary that bounds a raw
  ``tools/list`` snapshot (count cap, sanitized-name collision, illegal names,
  description neutralization, total-schema-size cap) before anything is cached.
- ``discover_and_cache`` — per-server error isolation and the
  missing/broken-sandbox fallbacks, with the DB upsert mocked out so we assert
  the exact kwargs each branch persists.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from ptc_agent.config.core import MCPServerConfig
from src.server.services import mcp_discovery
from src.server.services.mcp_discovery import (
    MAX_SCHEMA_CHARS_PER_SERVER,
    MAX_TOOLS_PER_SERVER,
    _stale_server_names as _real_stale_server_names,
    discover_and_cache,
    mcp_discovery_fingerprint,
    sanitize_discovered_tools,
)


def _tool(name: str, *, description: str = "d", input_schema=None) -> dict:
    return {
        "name": name,
        "description": description,
        "input_schema": input_schema if input_schema is not None else {},
    }


def _server(name: str):
    """A minimal stand-in for MCPServerConfig — discover_and_cache only reads .name."""
    return SimpleNamespace(name=name)


@pytest.fixture(autouse=True)
def _no_stale_servers(monkeypatch):
    """Default: every kicked server's config is still current, so the
    stale-result guard passes everything through. Guard-specific tests
    override this with their own monkeypatch."""

    async def _none(workspace_id, servers):
        return set()

    monkeypatch.setattr(mcp_discovery, "_stale_server_names", _none)


# ---------------------------------------------------------------------------
# sanitize_discovered_tools — count cap
# ---------------------------------------------------------------------------


class TestSanitizeCountCap:
    def test_over_count_keeps_exactly_the_cap(self):
        tools = [_tool(f"tool_{i}") for i in range(MAX_TOOLS_PER_SERVER + 5)]
        kept, skipped = sanitize_discovered_tools(tools)

        assert len(kept) == MAX_TOOLS_PER_SERVER
        # The 5 overflow tools are skipped, each with a cap reason.
        assert len(skipped) == 5
        assert all(str(MAX_TOOLS_PER_SERVER) in reason and "cap" in reason
                   for _name, reason in skipped)
        # The first MAX_TOOLS_PER_SERVER tools (in order) are the ones kept.
        assert [t["name"] for t in kept] == [f"tool_{i}" for i in range(MAX_TOOLS_PER_SERVER)]
        assert [n for n, _ in skipped] == [
            f"tool_{i}" for i in range(MAX_TOOLS_PER_SERVER, MAX_TOOLS_PER_SERVER + 5)
        ]


# ---------------------------------------------------------------------------
# sanitize_discovered_tools — sanitized-name collision
# ---------------------------------------------------------------------------


class TestSanitizeCollision:
    def test_collision_first_wins_second_skipped(self):
        # 'foo-bar' and 'foo.bar' both sanitize to 'foo_bar'.
        tools = [_tool("foo-bar"), _tool("foo.bar")]
        kept, skipped = sanitize_discovered_tools(tools)

        assert len(kept) == 1
        # First occurrence wins; the ORIGINAL name is preserved on the kept entry.
        assert kept[0]["name"] == "foo-bar"
        assert len(skipped) == 1
        name, reason = skipped[0]
        assert name == "foo.bar"
        assert "foo_bar" in reason
        assert "collides" in reason


# ---------------------------------------------------------------------------
# sanitize_discovered_tools — illegal names
# ---------------------------------------------------------------------------


class TestSanitizeIllegalNames:
    def test_all_illegal_chars_skipped(self):
        tools = [_tool("###"), _tool("ok_tool")]
        kept, skipped = sanitize_discovered_tools(tools)

        assert [t["name"] for t in kept] == ["ok_tool"]
        assert len(skipped) == 1
        name, reason = skipped[0]
        assert name == "###"
        assert "not a valid Python identifier" in reason

    def test_empty_name_skipped(self):
        tools = [_tool(""), _tool("good")]
        kept, skipped = sanitize_discovered_tools(tools)

        assert [t["name"] for t in kept] == ["good"]
        assert skipped == [("", "name is not a valid Python identifier")]

    def test_missing_name_key_skipped(self):
        # A tool dict with no 'name' key coerces to "" and is skipped.
        tools = [{"description": "no name"}, _tool("good")]
        kept, skipped = sanitize_discovered_tools(tools)

        assert [t["name"] for t in kept] == ["good"]
        assert skipped == [("", "name is not a valid Python identifier")]


# ---------------------------------------------------------------------------
# sanitize_discovered_tools — description sanitization on the kept entry
# ---------------------------------------------------------------------------


class TestSanitizeDescription:
    def test_control_chars_stripped(self):
        # NUL + bell + backspace are control chars (<32, not tab/newline) → stripped.
        # Tab and newline are preserved.
        raw = "before\x00\x07\x08after\twith\nnewline"
        kept, skipped = sanitize_discovered_tools([_tool("t", description=raw)])

        assert skipped == []
        desc = kept[0]["description"]
        assert "\x00" not in desc and "\x07" not in desc and "\x08" not in desc
        assert desc == "beforeafter\twith\nnewline"

    def test_triple_quote_neutralized(self):
        raw = 'docstring breakout """ injected'
        kept, _ = sanitize_discovered_tools([_tool("t", description=raw)])

        desc = kept[0]["description"]
        # The literal triple-quote can no longer terminate a docstring.
        assert '"""' not in desc
        assert '\\"\\"\\"' in desc

    def test_none_description_becomes_empty_string(self):
        kept, _ = sanitize_discovered_tools([_tool("t", description=None)])
        assert kept[0]["description"] == ""


# ---------------------------------------------------------------------------
# sanitize_discovered_tools — total-schema-size cap (skip, not truncate)
# ---------------------------------------------------------------------------


class TestSanitizeSizeCap:
    # NOTE: only ``input_schema`` can drive the size cap. ``description`` is
    # length-capped to DEFAULT_TOOL_TEXT_MAX_LEN (2048) by sanitize_tool_text
    # BEFORE the entry is measured, and the 64-tool count cap fires long before
    # 64 * ~2KB descriptions could reach 200KB — so the size-cap path is only
    # reachable via a large (un-truncated) input_schema.

    def test_size_cap_skips_overflowing_tool(self):
        # The first tool's serialized JSON alone exceeds the per-server schema
        # cap, so the implementation SKIPS it (it does not truncate). A second
        # small tool that follows still fits and is kept.
        big_schema = {"blob": "z" * (MAX_SCHEMA_CHARS_PER_SERVER + 1000)}
        tools = [
            _tool("big", input_schema=big_schema),
            _tool("small", input_schema={}),
        ]
        kept, skipped = sanitize_discovered_tools(tools)

        assert [t["name"] for t in kept] == ["small"]
        assert len(skipped) == 1
        name, reason = skipped[0]
        assert name == "big"
        assert reason == "server exceeds total schema size cap"

    def test_size_cap_triggers_on_cumulative_total(self):
        # No single tool exceeds the cap, but the running total does. Tools are
        # kept in order until the next one would push the total past the cap,
        # then it (and the rest) are skipped. Each ~cap/3 input_schema entry
        # serializes to ~66.7KB, so two fit (~133KB) and the third (~200.2KB)
        # crosses the 200KB cap.
        each = MAX_SCHEMA_CHARS_PER_SERVER // 3
        tools = [
            _tool(f"t{i}", input_schema={"k": "z" * each}) for i in range(4)
        ]
        kept, skipped = sanitize_discovered_tools(tools)

        assert [t["name"] for t in kept] == ["t0", "t1"]
        assert [n for n, _ in skipped] == ["t2", "t3"]
        assert all(reason == "server exceeds total schema size cap"
                   for _name, reason in skipped)


# ---------------------------------------------------------------------------
# discover_and_cache — sandbox absent / lacking the discovery driver
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestDiscoverAndCacheNoSandbox:
    async def test_sandbox_none_marks_every_server_pending(self, monkeypatch):
        upsert = AsyncMock(side_effect=lambda *a, **k: {"row": a})
        monkeypatch.setattr(mcp_discovery.mcp_db, "upsert_tool_schemas", upsert)

        servers = [_server("srv_a"), _server("srv_b")]
        rows = await discover_and_cache("ws-1", None, servers)

        assert len(rows) == 2
        assert upsert.await_count == 2
        for call, server in zip(upsert.await_args_list, servers):
            assert call.args == ("ws-1", server.name, mcp_discovery_fingerprint(server))
            assert call.kwargs == {"status": "pending"}

    async def test_sandbox_without_discover_attr_marks_pending(self, monkeypatch):
        upsert = AsyncMock(return_value={"ok": True})
        monkeypatch.setattr(mcp_discovery.mcp_db, "upsert_tool_schemas", upsert)

        # An old sandbox object that predates the discovery driver.
        sandbox = SimpleNamespace()  # no discover_user_mcp_schemas attribute
        server = _server("srv_a")
        await discover_and_cache("ws-1", sandbox, [server])

        upsert.assert_awaited_once()
        assert upsert.await_args.args == ("ws-1", "srv_a", mcp_discovery_fingerprint(server))
        assert upsert.await_args.kwargs == {"status": "pending"}


# ---------------------------------------------------------------------------
# discover_and_cache — driver raises ⇒ every server marked error
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestDiscoverAndCacheDriverRaises:
    async def test_driver_exception_marks_every_server_error(self, monkeypatch):
        upsert = AsyncMock(return_value={"ok": True})
        monkeypatch.setattr(mcp_discovery.mcp_db, "upsert_tool_schemas", upsert)

        async def boom(_servers):
            raise RuntimeError("sandbox exploded")

        sandbox = SimpleNamespace(discover_user_mcp_schemas=boom)
        servers = [_server("srv_a"), _server("srv_b")]
        rows = await discover_and_cache("ws-1", sandbox, servers)

        assert len(rows) == 2
        assert upsert.await_count == 2
        for call, server in zip(upsert.await_args_list, servers):
            assert call.args == ("ws-1", server.name, mcp_discovery_fingerprint(server))
            assert call.kwargs["status"] == "error"
            # The exception text is propagated into the error field.
            assert "sandbox exploded" in call.kwargs["error"]


# ---------------------------------------------------------------------------
# discover_and_cache — per-server isolation + error-status results
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestDiscoverAndCachePerServer:
    async def test_missing_result_for_one_server_is_isolated(self, monkeypatch):
        upsert = AsyncMock(return_value={"ok": True})
        monkeypatch.setattr(mcp_discovery.mcp_db, "upsert_tool_schemas", upsert)

        # Driver returns an OK result for srv_a but nothing for srv_b.
        async def discover(_servers):
            return {
                "srv_a": {
                    "status": "ok",
                    "tools": [_tool("alpha"), _tool("foo-bar"), _tool("foo.bar")],
                }
            }

        sandbox = SimpleNamespace(discover_user_mcp_schemas=discover)
        servers = [_server("srv_a"), _server("srv_b")]
        await discover_and_cache("ws-1", sandbox, servers)

        assert upsert.await_count == 2
        call_a, call_b = upsert.await_args_list

        # srv_a persisted from its result: sanitized tools + ok status + meta.
        assert call_a.args == ("ws-1", "srv_a", mcp_discovery_fingerprint(servers[0]))
        assert call_a.kwargs["status"] == "ok"
        kept = call_a.kwargs["tools"]
        assert [t["name"] for t in kept] == ["alpha", "foo-bar"]  # foo.bar collided
        meta = call_a.kwargs["observed_meta"]
        assert meta["tool_count"] == 2
        # Skipped entries are JSON-friendly [name, reason] lists.
        assert meta["skipped"] == [
            ["foo.bar", "sanitized name 'foo_bar' collides with another tool"]
        ]

        # srv_b had no discovery result → an error row, no tools/meta kwargs.
        assert call_b.args == ("ws-1", "srv_b", mcp_discovery_fingerprint(servers[1]))
        assert call_b.kwargs["status"] == "error"
        assert "no discovery result returned" in call_b.kwargs["error"]

    async def test_error_status_result_persisted_with_error_text(self, monkeypatch):
        upsert = AsyncMock(return_value={"ok": True})
        monkeypatch.setattr(mcp_discovery.mcp_db, "upsert_tool_schemas", upsert)

        async def discover(_servers):
            return {
                "srv_a": {"status": "error", "error": "connection refused", "tools": []},
            }

        sandbox = SimpleNamespace(discover_user_mcp_schemas=discover)
        servers = [_server("srv_a")]
        await discover_and_cache("ws-1", sandbox, servers)

        upsert.assert_awaited_once()
        assert upsert.await_args.args == ("ws-1", "srv_a", mcp_discovery_fingerprint(servers[0]))
        assert upsert.await_args.kwargs["status"] == "error"
        assert upsert.await_args.kwargs["error"] == "connection refused"
        # An error result never carries tools/observed_meta into the upsert.
        assert "tools" not in upsert.await_args.kwargs
        assert "observed_meta" not in upsert.await_args.kwargs

    async def test_error_result_missing_error_field_uses_default_text(self, monkeypatch):
        upsert = AsyncMock(return_value={"ok": True})
        monkeypatch.setattr(mcp_discovery.mcp_db, "upsert_tool_schemas", upsert)

        async def discover(_servers):
            # A non-ok status with no 'error' key falls back to a default.
            return {"srv_a": {"status": "timeout"}}

        sandbox = SimpleNamespace(discover_user_mcp_schemas=discover)
        await discover_and_cache("ws-1", sandbox, [_server("srv_a")])

        assert upsert.await_args.kwargs["status"] == "error"
        assert upsert.await_args.kwargs["error"] == "discovery failed"


# ---------------------------------------------------------------------------
# Stale-result guard — a late discovery must never clobber a newer config's row
# ---------------------------------------------------------------------------


def _cfg(name: str = "acme", **kw) -> MCPServerConfig:
    base = dict(name=name, transport="stdio", command="npx", source="workspace")
    base.update(kw)
    return MCPServerConfig(**base)


def _ws_row(name: str, config: dict) -> dict:
    return {"name": name, "source": "workspace", "enabled": True, "config": config}


@pytest.mark.asyncio
class TestStaleServerNames:
    async def test_deleted_edited_and_current_classified(self, monkeypatch):
        rows = [
            _ws_row("kept", {"transport": "stdio", "command": "npx"}),
            _ws_row("edited", {"transport": "stdio", "command": "uvx"}),
        ]
        monkeypatch.setattr(
            mcp_discovery.mcp_db, "list_workspace_servers",
            AsyncMock(return_value=rows),
        )
        # Kick-time snapshots: "kept" still matches its row, "edited" was
        # npx at kick but is uvx now, "deleted" has no row anymore.
        stale = await _real_stale_server_names(
            "ws", [_cfg("kept"), _cfg("edited"), _cfg("deleted")]
        )
        assert stale == {"edited", "deleted"}

    async def test_malformed_row_counts_as_stale(self, monkeypatch):
        rows = [_ws_row("bad", {"transport": "nonsense", "bogus_field": 1})]
        monkeypatch.setattr(
            mcp_discovery.mcp_db, "list_workspace_servers",
            AsyncMock(return_value=rows),
        )
        stale = await _real_stale_server_names("ws", [_cfg("bad")])
        assert stale == {"bad"}


@pytest.mark.asyncio
class TestDiscoverAndCacheStaleGuard:
    async def test_ok_result_dropped_when_config_changed_mid_discovery(
        self, monkeypatch
    ):
        upsert = AsyncMock()
        monkeypatch.setattr(mcp_discovery.mcp_db, "upsert_tool_schemas", upsert)

        async def _stale(workspace_id, servers):
            return {"acme"}

        monkeypatch.setattr(mcp_discovery, "_stale_server_names", _stale)
        sandbox = SimpleNamespace(
            discover_user_mcp_schemas=AsyncMock(
                return_value={"acme": {"status": "ok", "tools": [_tool("t1")]}}
            )
        )

        rows = await discover_and_cache("ws", sandbox, [_server("acme")])

        assert rows == []
        upsert.assert_not_awaited()

    async def test_only_the_stale_server_is_dropped(self, monkeypatch):
        upserted: list[str] = []

        async def _upsert(workspace_id, server_name, config_hash, **kw):
            upserted.append(server_name)
            return {"server_name": server_name, **kw}

        monkeypatch.setattr(mcp_discovery.mcp_db, "upsert_tool_schemas", _upsert)

        async def _stale(workspace_id, servers):
            return {"edited"}

        monkeypatch.setattr(mcp_discovery, "_stale_server_names", _stale)
        sandbox = SimpleNamespace(
            discover_user_mcp_schemas=AsyncMock(
                return_value={
                    "edited": {"status": "ok", "tools": [_tool("a")]},
                    "kept": {"status": "ok", "tools": [_tool("b")]},
                }
            )
        )

        rows = await discover_and_cache("ws", sandbox, [_server("edited"), _server("kept")])

        assert upserted == ["kept"]
        assert len(rows) == 1

    async def test_pending_path_dropped_when_stale(self, monkeypatch):
        upsert = AsyncMock()
        monkeypatch.setattr(mcp_discovery.mcp_db, "upsert_tool_schemas", upsert)

        async def _stale(workspace_id, servers):
            return {"acme"}

        monkeypatch.setattr(mcp_discovery, "_stale_server_names", _stale)

        rows = await discover_and_cache("ws", None, [_server("acme")])

        assert rows == []
        upsert.assert_not_awaited()


# ---------------------------------------------------------------------------
# mcp_discovery_fingerprint — the per-server discovery-cache key
# ---------------------------------------------------------------------------


class TestDiscoveryFingerprint:
    """The fingerprint is what decouples a server's cached schema from the
    workspace config_version, so toggling/editing one server never re-verifies
    another."""

    def _srv(self, **kw):
        base = dict(name="acme", transport="stdio", command="npx", source="workspace")
        base.update(kw)
        return MCPServerConfig(**base)

    def test_stable_across_enabled_toggle(self):
        # Toggling a server off/on must NOT bust its cache — nothing about its
        # tools changed, so it stays connected on re-enable.
        on = self._srv(enabled=True)
        off = self._srv(enabled=False)
        assert mcp_discovery_fingerprint(on) == mcp_discovery_fingerprint(off)

    def test_ignores_prompt_only_fields(self):
        # description / instruction / tool_exposure_mode feed only the prompt,
        # never discovery — editing them must not re-verify.
        a = self._srv(description="one", instruction="x", tool_exposure_mode="summary")
        b = self._srv(description="two", instruction="y", tool_exposure_mode="detailed")
        assert mcp_discovery_fingerprint(a) == mcp_discovery_fingerprint(b)

    def test_changes_when_command_changes(self):
        assert mcp_discovery_fingerprint(self._srv(command="npx")) != mcp_discovery_fingerprint(
            self._srv(command="uvx")
        )

    def test_changes_when_args_change(self):
        assert mcp_discovery_fingerprint(self._srv(args=["a"])) != mcp_discovery_fingerprint(
            self._srv(args=["b"])
        )

    def test_changes_when_literal_env_value_changes(self):
        # A literal (non-vault) env value affects the spawned server's behavior
        # AND the generated client, so editing it MUST re-verify + re-upload
        # (regression: literal MODE=prod -> staging was silently ignored).
        a = self._srv(env={"MODE": "prod"})
        b = self._srv(env={"MODE": "staging"})
        assert mcp_discovery_fingerprint(a) != mcp_discovery_fingerprint(b)

    def test_changes_when_literal_header_value_changes(self):
        a = self._srv(transport="http", command=None,
                      url="https://api.example.com/mcp", headers={"X-Mode": "prod"})
        b = self._srv(transport="http", command=None,
                      url="https://api.example.com/mcp", headers={"X-Mode": "staging"})
        assert mcp_discovery_fingerprint(a) != mcp_discovery_fingerprint(b)

    def test_changes_on_vault_ref_retarget_same_key(self):
        # Pointing a header at a different vault secret changes what discovery
        # may send — must re-verify. Names only; literal values never hashed.
        a = self._srv(transport="http", command=None,
                      url="https://api.example.com/mcp", headers={"Auth": "${vault:OLD}"})
        b = self._srv(transport="http", command=None,
                      url="https://api.example.com/mcp", headers={"Auth": "${vault:NEW}"})
        assert mcp_discovery_fingerprint(a) != mcp_discovery_fingerprint(b)

    def test_changes_on_discovery_uses_secrets_toggle(self):
        assert mcp_discovery_fingerprint(
            self._srv(discovery_uses_secrets=False)
        ) != mcp_discovery_fingerprint(self._srv(discovery_uses_secrets=True))
