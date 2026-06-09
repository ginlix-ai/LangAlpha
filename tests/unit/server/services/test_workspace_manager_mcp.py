"""Per-workspace MCP resolution + composite caching in WorkspaceManager.

Covers the session-lifecycle deliverables: the session caches the resolved
composite + tool summary (reused without re-resolving), the version-delta check
piggybacks the post-cooldown read (regression #5: zero queries within cooldown),
and a config-version delta triggers a re-resolve + BACKGROUND discovery that is
never awaited inline and never under the per-workspace lock.
"""

import asyncio
import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.services.workspace_manager import WorkspaceManager


def _make_config():
    config = MagicMock()
    config.to_core_config.return_value = MagicMock()
    config.daytona = MagicMock(api_key="k", base_url="https://daytona.test")
    config.sandbox = MagicMock(provider="daytona")
    config.filesystem = MagicMock(working_directory="/home/workspace")
    config.skills = MagicMock(enabled=False)
    config.mcp = MagicMock(tool_exposure_mode="summary")
    return config


def _make_workspace(workspace_id, *, status="running", mcp_config_version=0, **kw):
    now = datetime.now(timezone.utc)
    data = {
        "workspace_id": workspace_id,
        "user_id": "user-1",
        "name": "WS",
        "description": None,
        "sandbox_id": "sb-1",
        "status": status,
        "mode": "ptc",
        "sort_order": 0,
        "created_at": now,
        "updated_at": now,
        "last_activity_at": now,
        "mcp_config_version": mcp_config_version,
    }
    data.update(kw)
    return data


def _make_session(*, version=None, summary=None):
    session = MagicMock()
    session.conversation_id = "ws"
    session._initialized = True
    session.config = MagicMock()
    session.config.mcp = MagicMock(servers=[])
    session.sandbox = MagicMock()
    session.sandbox.is_ready = MagicMock(return_value=True)
    session.sandbox.has_failed = MagicMock(return_value=False)
    session.sandbox.ensure_sandbox_ready = AsyncMock()
    session.sandbox.config = MagicMock()
    session.sandbox.config.mcp = MagicMock(servers=[])
    session.mcp_registry = MagicMock()
    session._builtin_mcp_registry = session.mcp_registry
    session.mcp_tool_summary = summary
    session.mcp_config_version = version
    return session


def _resolved(version, servers=None, user_names=None):
    r = MagicMock()
    r.version = version
    r.servers = servers or []
    r.builtin_names = frozenset()
    r.user_names = frozenset(user_names or [])
    return r


# ---------------------------------------------------------------------------
# Regression #5 — warm acquire within cooldown issues no workspace query
# ---------------------------------------------------------------------------


class TestWarmCooldownNoQuery:
    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_manager.db_get_workspace", new_callable=AsyncMock)
    async def test_warm_cooldown_zero_workspace_queries(self, mock_get_ws):
        """A ready cached session inside the 30s cooldown returns WITHOUT any
        db_get_workspace read — so the version check adds zero per-turn queries."""
        wm = WorkspaceManager.get_instance(config=_make_config())
        ws_id = str(uuid.uuid4())
        session = _make_session(version=0, summary="cached")
        wm._sessions[ws_id] = session
        wm._record_sync(ws_id)  # cooldown active

        # resolve must NOT be called within cooldown.
        with patch(
            "src.server.handlers.chat.mcp_config.resolve_mcp_config",
            new_callable=AsyncMock,
        ) as mock_resolve:
            result = await wm.get_session_for_workspace(ws_id, user_id="user-1")

        assert result is session
        mock_get_ws.assert_not_awaited()
        mock_resolve.assert_not_awaited()


# ---------------------------------------------------------------------------
# Session caches registry + summary; second acquire reuses without re-resolve
# ---------------------------------------------------------------------------


class TestSessionCachesMcp:
    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    @pytest.mark.asyncio
    async def test_apply_session_mcp_skips_when_current(self):
        """Same version + an installed summary ⇒ _apply_session_mcp returns None
        and never resolves (zero extra reads on an unchanged-config sync)."""
        wm = WorkspaceManager.get_instance(config=_make_config())
        session = _make_session(version=3, summary="already")

        with patch(
            "src.server.handlers.chat.mcp_config.resolve_mcp_config",
            new_callable=AsyncMock,
        ) as mock_resolve:
            out = await wm._apply_session_mcp(
                "ws", "user-1", session, ws_version=3
            )

        assert out is None
        mock_resolve.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_apply_session_mcp_resolves_and_caches(self):
        """First apply resolves, installs the composite, and stamps version +
        summary on the session."""
        wm = WorkspaceManager.get_instance(config=_make_config())
        session = _make_session(version=None, summary=None)
        resolved = _resolved(2)

        composite = MagicMock()
        with patch(
            "src.server.handlers.chat.mcp_config.resolve_mcp_config",
            new_callable=AsyncMock,
            return_value=resolved,
        ) as mock_resolve, patch(
            "ptc_agent.core.mcp_registry.build_composite_registry",
            return_value=composite,
        ), patch(
            "ptc_agent.agent.prompts.formatter.build_tool_summary_from_registry",
            return_value="SUMMARY",
        ):
            out = await wm._apply_session_mcp(
                "ws", "user-1", session, ws_version=2
            )

        assert out is resolved
        mock_resolve.assert_awaited_once()
        assert session.mcp_registry is composite
        assert session.sandbox.mcp_registry is composite
        assert session.mcp_tool_summary == "SUMMARY"
        assert session.mcp_config_version == 2

    @pytest.mark.asyncio
    async def test_install_composite_builds_from_builtin_not_prior_composite(self):
        """A re-resolve must build from the BUILTIN registry, never a prior
        composite (no composite-of-composite)."""
        wm = WorkspaceManager.get_instance(config=_make_config())
        builtin = MagicMock(name="builtin")
        prior_composite = MagicMock(name="prior")
        session = _make_session(version=1, summary="old")
        session._builtin_mcp_registry = builtin
        session.mcp_registry = prior_composite  # simulate a prior swap

        resolved = _resolved(2)
        captured = {}

        def fake_build(reg, user_servers, schemas):
            captured["reg"] = reg
            return MagicMock(name="new_composite")

        with patch(
            "ptc_agent.core.mcp_registry.build_composite_registry",
            side_effect=fake_build,
        ), patch(
            "ptc_agent.agent.prompts.formatter.build_tool_summary_from_registry",
            return_value="S",
        ):
            await wm._install_session_composite(session, resolved)

        assert captured["reg"] is builtin


# ---------------------------------------------------------------------------
# Version-delta on the post-cooldown read → re-resolve + background discovery
# ---------------------------------------------------------------------------


class TestVersionDeltaBackgroundDiscovery:
    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    @pytest.mark.asyncio
    async def test_kick_discovery_is_background_not_awaited(self):
        """_kick_mcp_discovery schedules a task and returns immediately — the
        slow discovery+sync never runs inline on the caller's coroutine."""
        wm = WorkspaceManager.get_instance(config=_make_config())
        session = _make_session(version=2, summary="s")

        started = asyncio.Event()
        release = asyncio.Event()

        async def slow_discover(*a, **k):
            started.set()
            await release.wait()
            return []

        server = MagicMock()
        server.name = "alpha"
        with patch(
            "src.server.services.mcp_discovery.discover_and_cache",
            new=AsyncMock(side_effect=slow_discover),
        ):
            wm._kick_mcp_discovery("ws", "user-1", session, [server], 2)
            # The call returned synchronously; the discovery has not finished.
            assert len(wm._mcp_discovery_tasks) == 1
            # Let the background task start, then confirm it's still pending.
            await started.wait()
            task = next(iter(wm._mcp_discovery_tasks))
            assert not task.done()
            release.set()
            await task  # drain for clean teardown

    @pytest.mark.asyncio
    async def test_kick_discovery_noop_when_no_servers(self):
        wm = WorkspaceManager.get_instance(config=_make_config())
        session = _make_session()
        wm._kick_mcp_discovery("ws", "user-1", session, [], 2)
        assert len(wm._mcp_discovery_tasks) == 0

    @pytest.mark.asyncio
    async def test_servers_needing_discovery_excludes_servers_with_tools(self):
        """Only user servers without cached tools (pending/new) need discovery."""
        wm = WorkspaceManager.get_instance(config=_make_config())
        session = _make_session()
        # Composite reports alpha has tools, beta has none.
        session.mcp_registry.get_all_tools = MagicMock(
            return_value={"alpha": [MagicMock()], "beta": []}
        )
        alpha = MagicMock()
        alpha.name = "alpha"
        alpha.source = "workspace"
        beta = MagicMock()
        beta.name = "beta"
        beta.source = "workspace"
        resolved = _resolved(2, servers=[alpha, beta])

        needing = wm._servers_needing_discovery(session, resolved)
        assert [s.name for s in needing] == ["beta"]

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_manager.db_get_workspace", new_callable=AsyncMock)
    @patch("src.server.services.workspace_manager.update_workspace_status", new_callable=AsyncMock)
    @patch("src.server.services.workspace_manager.update_workspace_activity", new_callable=AsyncMock)
    async def test_version_delta_triggers_resolve_off_the_lock(
        self, mock_activity, mock_status, mock_get_ws
    ):
        """A cached ready session whose cooldown expired + version drift triggers
        a re-resolve in Phase 2 (OUTSIDE the per-workspace lock)."""
        wm = WorkspaceManager.get_instance(config=_make_config())
        ws_id = str(uuid.uuid4())
        # Session is on version 0; workspace row now says version 5.
        session = _make_session(version=0, summary="old")
        wm._sessions[ws_id] = session
        wm._last_sync_at = {}  # cooldown expired → slow path runs

        mock_get_ws.return_value = _make_workspace(ws_id, mcp_config_version=5)

        # Assert the resolve does NOT run while the per-workspace lock is held.
        lock_held_during_resolve = {"value": False}
        orig_apply = wm._apply_session_mcp

        async def tracking_apply(*a, **k):
            lock = wm._workspace_locks.get(ws_id)
            if lock is not None:
                lock_held_during_resolve["value"] = lock.locked()
            return await orig_apply(*a, **k)

        wm._apply_session_mcp = tracking_apply
        wm._sync_sandbox_assets = AsyncMock()
        wm._maybe_restore_files = AsyncMock()

        resolved = _resolved(5)
        with patch(
            "src.server.handlers.chat.mcp_config.resolve_mcp_config",
            new_callable=AsyncMock,
            return_value=resolved,
        ) as mock_resolve, patch(
            "ptc_agent.core.mcp_registry.build_composite_registry",
            return_value=MagicMock(),
        ), patch(
            "ptc_agent.agent.prompts.formatter.build_tool_summary_from_registry",
            return_value="NEW",
        ):
            await wm.get_session_for_workspace(ws_id, user_id="user-1")

        mock_resolve.assert_awaited_once()
        assert session.mcp_config_version == 5
        # Re-resolve happened OUTSIDE the lock (regression #3).
        assert lock_held_during_resolve["value"] is False
        # Wrappers re-synced after the config change.
        wm._sync_sandbox_assets.assert_awaited()
