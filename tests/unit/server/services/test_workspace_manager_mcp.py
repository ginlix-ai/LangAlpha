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
            "src.server.services.mcp_config.resolve_mcp_config",
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
            "src.server.services.mcp_config.resolve_mcp_config",
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
            "src.server.services.mcp_config.resolve_mcp_config",
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

        def fake_build(reg, user_servers, schemas, disabled=frozenset()):
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
        # The session must be the live one for the workspace, else the liveness
        # re-check short-circuits discovery (see _cancel/_session_live).
        wm._sessions["ws"] = session

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
    async def test_kick_discovery_short_circuits_when_session_not_live(self):
        """If the session was evicted (stopped/deleted) before the task runs,
        discovery short-circuits and never calls discover_and_cache."""
        wm = WorkspaceManager.get_instance(config=_make_config())
        session = _make_session(version=2, summary="s")
        # Session is NOT registered as the live session for "ws".
        server = MagicMock()
        server.name = "alpha"
        mock_discover = AsyncMock(return_value=[])
        with patch(
            "src.server.services.mcp_discovery.discover_and_cache",
            new=mock_discover,
        ):
            wm._kick_mcp_discovery("ws", "user-1", session, [server], 2)
            task = next(iter(wm._mcp_discovery_tasks))
            await task
        mock_discover.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_cancel_mcp_discovery_cancels_in_flight_task(self):
        """_cancel_mcp_discovery cancels a workspace's in-flight discovery task
        and prunes the per-workspace map (used by stop/delete)."""
        wm = WorkspaceManager.get_instance(config=_make_config())
        session = _make_session(version=2, summary="s")
        wm._sessions["ws"] = session

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
            await started.wait()
            task = next(iter(wm._mcp_discovery_tasks_by_ws["ws"]))

            wm._cancel_mcp_discovery("ws")

            with pytest.raises(asyncio.CancelledError):
                await task
            assert task.cancelled()
            # Per-workspace map pruned; global set drained via done callback.
            assert "ws" not in wm._mcp_discovery_tasks_by_ws
            assert task not in wm._mcp_discovery_tasks

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_manager.db_get_workspace", new_callable=AsyncMock)
    @patch("src.server.services.workspace_manager.update_workspace_status", new_callable=AsyncMock)
    async def test_stop_workspace_cancels_discovery(self, mock_status, mock_get_ws):
        """stop_workspace cancels the workspace's in-flight discovery task."""
        wm = WorkspaceManager.get_instance(config=_make_config())
        ws_id = str(uuid.uuid4())
        mock_get_ws.return_value = _make_workspace(ws_id, status="running")
        mock_status.return_value = _make_workspace(ws_id, status="stopped")

        cancelled = {"value": False}

        async def never_returns(*a, **k):
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled["value"] = True
                raise
            return []

        session = _make_session(version=1, summary="s")
        session.stop = AsyncMock()
        wm._sessions[ws_id] = session
        wm._backup_files_to_db = AsyncMock()

        with patch(
            "src.server.services.mcp_discovery.discover_and_cache",
            new=AsyncMock(side_effect=never_returns),
        ):
            server = MagicMock()
            server.name = "alpha"
            wm._kick_mcp_discovery(ws_id, "user-1", session, [server], 1)
            task = next(iter(wm._mcp_discovery_tasks_by_ws[ws_id]))
            # Give the task a tick to enter discover_and_cache.
            await asyncio.sleep(0)

            await wm.stop_workspace(ws_id)

            with pytest.raises(asyncio.CancelledError):
                await task
        assert ws_id not in wm._mcp_discovery_tasks_by_ws

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
            "src.server.services.mcp_config.resolve_mcp_config",
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


# ---------------------------------------------------------------------------
# Applied-version getter + proactive apply (front-load config to a live session)
# ---------------------------------------------------------------------------


class TestAppliedVersionAndProactiveApply:
    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    def test_applied_version_none_without_session(self):
        """No warm session ⇒ the config isn't loaded anywhere live ⇒ None."""
        wm = WorkspaceManager.get_instance(config=_make_config())
        assert wm.get_applied_mcp_config_version("ws-x") is None

    def test_applied_version_reads_warm_session(self):
        wm = WorkspaceManager.get_instance(config=_make_config())
        wm._sessions["ws"] = _make_session(version=7, summary="s")
        assert wm.get_applied_mcp_config_version("ws") == 7

    @pytest.mark.asyncio
    async def test_proactive_apply_warms_without_ready_session(self):
        """No live session ⇒ still acquire, which warms (cold-starts) the
        sandbox. A user who just configured a server expects it to come up and
        verify regardless of whether the sandbox happened to be running."""
        wm = WorkspaceManager.get_instance(config=_make_config())
        wm.get_session_for_workspace = AsyncMock()
        await wm.proactively_apply_mcp_config("ws-x", "user-1")
        wm.get_session_for_workspace.assert_awaited_once_with("ws-x", user_id="user-1")

    @pytest.mark.asyncio
    async def test_proactive_apply_clears_cooldown_and_reacquires(self):
        """A warm session ⇒ clear the 30s sync cooldown (so the re-acquire
        actually re-syncs rather than short-circuiting) and re-acquire."""
        wm = WorkspaceManager.get_instance(config=_make_config())
        wm._sessions["ws"] = _make_session(version=2, summary="s")
        wm._record_sync("ws")
        assert "ws" in wm._last_sync_at
        wm.get_session_for_workspace = AsyncMock()

        await wm.proactively_apply_mcp_config("ws", "user-1")

        assert "ws" not in wm._last_sync_at
        wm.get_session_for_workspace.assert_awaited_once_with("ws", user_id="user-1")

    @pytest.mark.asyncio
    async def test_proactive_apply_swallows_errors(self):
        """Best-effort: a failure never propagates — it just falls back to the
        next-message apply, so a mutation response is never affected."""
        wm = WorkspaceManager.get_instance(config=_make_config())
        wm._sessions["ws"] = _make_session(version=2, summary="s")
        wm.get_session_for_workspace = AsyncMock(side_effect=RuntimeError("boom"))
        await wm.proactively_apply_mcp_config("ws", "user-1")  # must not raise

    @pytest.mark.asyncio
    async def test_refresh_busts_session_version_then_applies(self):
        """An out-of-band schema-cache update (manual /discover probe) has no
        version bump, so the apply path would short-circuit; refresh busts the
        session's cached version first so the re-acquire actually rebuilds."""
        wm = WorkspaceManager.get_instance(config=_make_config())
        session = _make_session(version=4, summary="cached")
        wm._sessions["ws"] = session
        wm.proactively_apply_mcp_config = AsyncMock()

        await wm.refresh_session_mcp("ws", "user-1")

        assert session.mcp_config_version is None
        wm.proactively_apply_mcp_config.assert_awaited_once_with("ws", "user-1")

    @pytest.mark.asyncio
    async def test_refresh_without_live_session_still_applies(self):
        wm = WorkspaceManager.get_instance(config=_make_config())
        wm.proactively_apply_mcp_config = AsyncMock()

        await wm.refresh_session_mcp("ws-x", "user-1")

        wm.proactively_apply_mcp_config.assert_awaited_once_with("ws-x", "user-1")


# ---------------------------------------------------------------------------
# Discovery kick must see the session already cached (recover/attach paths)
# ---------------------------------------------------------------------------


class TestDiscoveryKickSeesCachedSession:
    """The background discovery task's liveness gate is
    ``self._sessions.get(workspace_id) is session``; if the kick fires before
    the session is cached, the task exits permanently. Both _recover_sandbox
    and _attach_running_session must cache the session BEFORE the kick.
    """

    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_manager.db_get_workspace", new_callable=AsyncMock)
    @patch("src.server.services.workspace_manager.update_workspace_status", new_callable=AsyncMock)
    @patch("src.server.services.workspace_manager.update_workspace_activity", new_callable=AsyncMock)
    @patch("src.server.services.workspace_manager.SessionManager")
    async def test_recover_sandbox_caches_before_kick(
        self, mock_session_mgr, mock_activity, mock_status, mock_get_ws
    ):
        """_recover_sandbox caches the session before _kick_mcp_discovery, so the
        kick's liveness check (``_sessions.get(ws) is session``) passes."""
        wm = WorkspaceManager.get_instance(config=_make_config())
        ws_id = str(uuid.uuid4())
        mock_get_ws.return_value = {"resource_tier": "standard", "is_always_on": False}

        session = _make_session(version=1, summary="s")
        session.initialize = AsyncMock()
        session.sandbox.sandbox_id = "sb-new"
        mock_session_mgr.get_session.return_value = session

        wm._mint_sandbox_tokens = AsyncMock(return_value={})
        wm._apply_session_mcp = AsyncMock(return_value=_resolved(1, user_names=["alpha"]))
        wm._servers_needing_discovery = MagicMock(return_value=[MagicMock(name="alpha")])
        wm._sync_sandbox_assets = AsyncMock()
        wm._restore_files = AsyncMock()

        # Spy: record whether the session was already cached at kick time.
        cached_at_kick = {"value": None}

        def spy_kick(workspace_id, *a, **k):
            cached_at_kick["value"] = wm._sessions.get(workspace_id) is session
            # Don't actually schedule the background task.

        wm._kick_mcp_discovery = spy_kick

        await wm._recover_sandbox(ws_id, "user-1", MagicMock())

        assert cached_at_kick["value"] is True
        assert wm._sessions.get(ws_id) is session

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_manager.SessionManager")
    async def test_attach_running_session_caches_before_kick(self, mock_session_mgr):
        """_attach_running_session caches the session before _kick_mcp_discovery
        so the kick's liveness check passes on a cold init."""
        wm = WorkspaceManager.get_instance(config=_make_config())
        ws_id = str(uuid.uuid4())

        session = _make_session(version=1, summary="s")
        session._initialized = False
        session.initialize = AsyncMock()
        mock_session_mgr.get_session.return_value = session

        wm._apply_session_mcp = AsyncMock(return_value=_resolved(1, user_names=["alpha"]))
        wm._servers_needing_discovery = MagicMock(return_value=[MagicMock(name="alpha")])
        wm._sync_sandbox_assets = AsyncMock()
        wm._maybe_migrate_sandbox = AsyncMock(return_value=None)

        cached_at_kick = {"value": None}

        def spy_kick(workspace_id, *a, **k):
            cached_at_kick["value"] = wm._sessions.get(workspace_id) is session

        wm._kick_mcp_discovery = spy_kick

        workspace = _make_workspace(ws_id, status="running", mcp_config_version=1)
        out_session, did_init = await wm._attach_running_session(
            workspace, "user-1", None, lambda _stage: None
        )

        assert cached_at_kick["value"] is True
        assert out_session is session
        assert did_init is True
        assert wm._sessions.get(ws_id) is session

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_manager.db_get_workspace", new_callable=AsyncMock)
    @patch("src.server.services.workspace_manager.update_workspace_status", new_callable=AsyncMock)
    @patch("src.server.services.workspace_manager.update_workspace_activity", new_callable=AsyncMock)
    @patch("src.server.services.workspace_manager.SessionManager")
    async def test_recover_sandbox_unwinds_cache_on_post_kick_failure(
        self, mock_session_mgr, mock_activity, mock_status, mock_get_ws
    ):
        """If a post-kick step raises, the broken session is not left cached —
        preserving the old code's 'only cached on full success' semantics."""
        wm = WorkspaceManager.get_instance(config=_make_config())
        ws_id = str(uuid.uuid4())
        mock_get_ws.return_value = {"resource_tier": "standard", "is_always_on": False}

        session = _make_session(version=1, summary="s")
        session.initialize = AsyncMock()
        session.sandbox.sandbox_id = "sb-new"
        mock_session_mgr.get_session.return_value = session

        wm._mint_sandbox_tokens = AsyncMock(return_value={})
        wm._apply_session_mcp = AsyncMock(return_value=_resolved(1, user_names=["alpha"]))
        wm._servers_needing_discovery = MagicMock(return_value=[MagicMock(name="alpha")])
        wm._sync_sandbox_assets = AsyncMock()
        wm._restore_files = AsyncMock()
        wm._kick_mcp_discovery = MagicMock()
        wm._cancel_mcp_discovery = MagicMock()
        mock_status.side_effect = RuntimeError("status write failed")

        with pytest.raises(RuntimeError, match="status write failed"):
            await wm._recover_sandbox(ws_id, "user-1", MagicMock())

        assert wm._sessions.get(ws_id) is None
        wm._cancel_mcp_discovery.assert_called_once_with(ws_id)


# ---------------------------------------------------------------------------
# _clear_session cancels in-flight discovery (FIX B)
# ---------------------------------------------------------------------------


class TestClearSessionCancelsDiscovery:
    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_manager.SessionManager")
    async def test_clear_session_cancels_pending_discovery(self, mock_session_mgr):
        """_clear_session cancels the workspace's in-flight discovery task so it
        can't run against the torn-down session (mirrors stop/delete_workspace)."""
        mock_session_mgr.cleanup_session = AsyncMock()
        wm = WorkspaceManager.get_instance(config=_make_config())
        ws_id = str(uuid.uuid4())
        session = _make_session(version=1, summary="s")
        wm._sessions[ws_id] = session

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
            wm._kick_mcp_discovery(ws_id, "user-1", session, [server], 1)
            await started.wait()
            task = next(iter(wm._mcp_discovery_tasks_by_ws[ws_id]))

            await wm._clear_session(ws_id)

            with pytest.raises(asyncio.CancelledError):
                await task
            assert task.cancelled()
        assert ws_id not in wm._mcp_discovery_tasks_by_ws
        assert ws_id not in wm._sessions

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_manager.SessionManager")
    async def test_clear_session_cancels_before_cleanup(self, mock_session_mgr):
        """The cancel must run as the first step — before cleanup_session — so
        discovery never races the teardown."""
        order: list[str] = []

        async def record_cleanup(_ws):
            order.append("cleanup")

        mock_session_mgr.cleanup_session = AsyncMock(side_effect=record_cleanup)
        wm = WorkspaceManager.get_instance(config=_make_config())
        ws_id = str(uuid.uuid4())

        orig_cancel = wm._cancel_mcp_discovery

        def record_cancel(workspace_id):
            order.append("cancel")
            return orig_cancel(workspace_id)

        wm._cancel_mcp_discovery = record_cancel

        await wm._clear_session(ws_id)

        assert order == ["cancel", "cleanup"]


# ---------------------------------------------------------------------------
# set_workspace_spec downgrade disk-fit guard
# ---------------------------------------------------------------------------


class TestSetWorkspaceSpecDiskGuard:
    """A tier downgrade recreates the sandbox on a smaller disk; restore is
    best-effort, so the guard rejects (before any teardown) when the backed-up
    files won't fit. Upgrades and lateral moves skip the check.
    """

    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    @pytest.fixture(autouse=True)
    def _quiet_durable_probes(self):
        """The spec-change activity guard also reads the run ledgers; keep
        both quiet so these tests exercise only the disk-guard mechanics."""
        with (
            patch(
                "src.server.database.runs.lifecycle.workspace_has_active_run",
                new=AsyncMock(return_value=False),
            ),
            patch(
                "src.server.database.runs.subagent_runs.count_open_runs_for_workspace",
                new=AsyncMock(return_value=0),
            ),
        ):
            yield

    @staticmethod
    def _config_with_tiers():
        config = _make_config()
        config.sandbox.daytona.resource_tiers = {
            "standard": MagicMock(cpu=1, memory=1, disk=3),
            "performance": MagicMock(cpu=2, memory=4, disk=5),
            "max": MagicMock(cpu=4, memory=8, disk=10),
        }
        return config

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_entitlements.get_workspace_total_size", new_callable=AsyncMock)
    @patch("src.server.services.workspace_entitlements.db_set_workspace_resource_tier", new_callable=AsyncMock)
    @patch("src.server.services.workspace_manager.SessionManager")
    @patch("src.server.services.workspace_entitlements.db_get_workspace", new_callable=AsyncMock)
    async def test_running_downgrade_rejected_when_files_exceed_disk(
        self, mock_get_ws, mock_session_mgr, mock_set_tier, mock_total_size
    ):
        wm = WorkspaceManager.get_instance(config=self._config_with_tiers())
        ws_id = str(uuid.uuid4())
        mock_get_ws.return_value = _make_workspace(
            ws_id, status="running", resource_tier="max", sandbox_id="sb-1"
        )
        mock_total_size.return_value = 5 * 1024**3  # 5 GiB; standard usable ~1 GiB
        wm._sessions[ws_id] = MagicMock(sandbox=MagicMock())
        wm._backup_files_to_db = AsyncMock()
        wm._recover_sandbox = AsyncMock()

        with pytest.raises(RuntimeError, match="Cannot downgrade"):
            await wm.set_workspace_spec(ws_id, "standard", user_id="user-1")

        # Backed up (for fresh sizes) but the live sandbox is never torn down.
        wm._backup_files_to_db.assert_awaited_once()
        wm._recover_sandbox.assert_not_awaited()
        mock_session_mgr.cleanup_session.assert_not_called()
        # Tier set to target optimistically, then reverted to the original.
        assert mock_set_tier.await_args_list[0].args == (ws_id, "standard")
        assert mock_set_tier.await_args_list[-1].args == (ws_id, "max")

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_entitlements.get_workspace_total_size", new_callable=AsyncMock)
    @patch("src.server.services.workspace_entitlements.db_set_workspace_resource_tier", new_callable=AsyncMock)
    @patch("src.server.services.workspace_manager.SessionManager")
    @patch("src.server.services.workspace_entitlements.db_get_workspace", new_callable=AsyncMock)
    async def test_running_downgrade_allowed_when_files_fit(
        self, mock_get_ws, mock_session_mgr, mock_set_tier, mock_total_size
    ):
        wm = WorkspaceManager.get_instance(config=self._config_with_tiers())
        ws_id = str(uuid.uuid4())
        mock_get_ws.return_value = _make_workspace(
            ws_id, status="running", resource_tier="max", sandbox_id="sb-1"
        )
        mock_total_size.return_value = 100 * 1024**2  # 100 MiB — fits standard
        mock_session_mgr.cleanup_session = AsyncMock()
        wm._sessions[ws_id] = MagicMock(sandbox=MagicMock())
        wm._backup_files_to_db = AsyncMock()
        wm._destroy_sandbox = AsyncMock()
        wm._recover_sandbox = AsyncMock()

        await wm.set_workspace_spec(ws_id, "standard", user_id="user-1")

        wm._recover_sandbox.assert_awaited_once()
        # Tier never reverted — the last write is the target.
        assert mock_set_tier.await_args_list[-1].args == (ws_id, "standard")

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_entitlements.get_workspace_total_size", new_callable=AsyncMock)
    @patch("src.server.services.workspace_entitlements.db_set_workspace_resource_tier", new_callable=AsyncMock)
    @patch("src.server.services.workspace_manager.SessionManager")
    @patch("src.server.services.workspace_entitlements.db_get_workspace", new_callable=AsyncMock)
    async def test_upgrade_skips_disk_guard(
        self, mock_get_ws, mock_session_mgr, mock_set_tier, mock_total_size
    ):
        wm = WorkspaceManager.get_instance(config=self._config_with_tiers())
        ws_id = str(uuid.uuid4())
        mock_get_ws.return_value = _make_workspace(
            ws_id, status="running", resource_tier="standard", sandbox_id="sb-1"
        )
        mock_session_mgr.cleanup_session = AsyncMock()
        wm._sessions[ws_id] = MagicMock(sandbox=MagicMock())
        wm._backup_files_to_db = AsyncMock()
        wm._destroy_sandbox = AsyncMock()
        wm._recover_sandbox = AsyncMock()

        await wm.set_workspace_spec(ws_id, "max", user_id="user-1")

        mock_total_size.assert_not_awaited()  # no fit check on an upgrade
        wm._recover_sandbox.assert_awaited_once()

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_entitlements.get_workspace_total_size", new_callable=AsyncMock)
    @patch("src.server.services.workspace_entitlements.db_set_workspace_resource_tier", new_callable=AsyncMock)
    @patch("src.server.services.workspace_entitlements.db_get_workspace", new_callable=AsyncMock)
    async def test_stopped_downgrade_rejected_before_destroy(
        self, mock_get_ws, mock_set_tier, mock_total_size
    ):
        wm = WorkspaceManager.get_instance(config=self._config_with_tiers())
        ws_id = str(uuid.uuid4())
        mock_get_ws.return_value = _make_workspace(
            ws_id, status="stopped", resource_tier="max", sandbox_id="sb-1"
        )
        mock_total_size.return_value = 5 * 1024**3
        wm._destroy_sandbox = AsyncMock()

        with pytest.raises(RuntimeError, match="Cannot downgrade"):
            await wm.set_workspace_spec(ws_id, "standard", user_id="user-1")

        wm._destroy_sandbox.assert_not_awaited()
        assert mock_set_tier.await_args_list[-1].args == (ws_id, "max")  # reverted
