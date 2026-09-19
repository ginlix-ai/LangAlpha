"""Per-project MCP resolution + composite caching on a machine's session.

Covers the session-lifecycle deliverables: the session caches the resolved
composite + tool summary (reused without re-resolving), the version-delta check
piggybacks the post-cooldown read (regression #5: zero queries within cooldown),
and a config-version delta triggers a re-resolve + BACKGROUND discovery that is
never awaited inline and never under the machine lock.

The session, the locks and the cooldown are the computer's, so every cache key
here is a computer id and every entry point takes a ``ComputerBinding``.
"""

import asyncio
import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.server.services.computer_manager import ComputerBinding
from src.server.services.egress.session_binding import RelayBind
from src.server.services.workspace_manager import WorkspaceManager
from tests.computer_manager_patch import cm_patch
from tests.unit.server.mcp_builders import resolved_mcp
from tests.unit.server.services.conftest import _patch_identity, _patch_machine_bind


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


def _make_computer(
    computer_id, *, status="running", resource_tier="max", provider_ref="sb-1"
):
    return {
        "computer_id": computer_id,
        "status": status,
        "resource_tier": resource_tier,
        "provider_ref": provider_ref,
        "is_always_on": False,
    }


def _make_binding(workspace_id, computer_id, *, provider_ref="sb-1", **kw):
    """Which machine the project is on, as every entry point below takes it.

    ``provider_ref`` defaults to _make_workspace's sandbox_id, so the machine
    and the project shadow agree unless a test says otherwise.
    """
    return ComputerBinding(
        workspace_id=workspace_id,
        computer_id=computer_id,
        provider_ref=provider_ref,
        **kw,
    )


def _make_session(*, version=None, summary=None, config_owner="ws"):
    session = MagicMock()
    session.conversation_id = "ws"
    session._initialized = True
    session.config = MagicMock()
    session.config.mcp = MagicMock(servers=[])
    session.sandbox = MagicMock()
    # Must match _make_workspace's sandbox_id: every cached return validates the
    # session's binding against the row before handing it out.
    session.sandbox.sandbox_id = "sb-1"
    session.sandbox.is_ready = MagicMock(return_value=True)
    session.sandbox.has_failed = MagicMock(return_value=False)
    session.sandbox.ensure_sandbox_ready = AsyncMock()
    session.sandbox.config = MagicMock()
    session.sandbox.config.mcp = MagicMock(servers=[])
    session.mcp_registry = MagicMock()
    session._builtin_mcp_registry = session.mcp_registry
    session.mcp_tool_summary = summary
    session.mcp_config_version = version
    # Whose composite the version belongs to. A MagicMock default would read as
    # a foreign workspace and defeat every skip this file asserts.
    session.mcp_config_workspace_id = config_owner
    session.egress_binding = None
    return session


def _srv(name, *, source="workspace", oauth_connection_id=None):
    """A stand-in server config (MagicMock: only the read fields matter)."""
    server = MagicMock()
    server.name = name
    server.source = source
    server.oauth_connection_id = oauth_connection_id
    return server


def _resolved(version, servers=None):
    """A real ResolvedMCP whose ``servers`` are workspace-local entries."""
    return resolved_mcp(version=version, local=list(servers or []))


def _patch_certify():
    """Provisioning certifies the platform secret before any row names the sandbox."""
    return patch(
        "src.server.services.platform_secret_rollout.certify_platform_secrets",
        new=AsyncMock(return_value=0),
    )


def _patch_computer_for_workspace(computer):
    """Stub the machine lookup the proactive-apply path uses to find the cooldown."""
    return patch(
        "src.server.services.computer_manager._mcp.get_computer_for_workspace",
        new=AsyncMock(return_value=computer),
    )


# ---------------------------------------------------------------------------
# Regression #5 — warm acquire within cooldown issues no workspace query
# ---------------------------------------------------------------------------


class TestWarmCooldownNoQuery:
    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    @pytest.mark.asyncio
    @cm_patch("db_get_workspace", new_callable=AsyncMock)
    async def test_warm_cooldown_reads_identity_only(self, mock_get_ws):
        """A ready cached session inside the 30s cooldown returns after exactly one
        narrow ``status, sandbox_id`` read — never the full row.

        The identity read is not optional: workers are spawn-isolated, so a
        cached handle can name a sandbox Postgres has already replaced and every
        warm path returns without consulting the row. What the hot path must
        keep avoiding is ``db_get_workspace``, which drags the JSONB ``config``
        and ``artifacts`` columns along with it.
        """
        wm = WorkspaceManager.get_instance(config=_make_config())
        ws_id = str(uuid.uuid4())
        computer_id = str(uuid.uuid4())
        binding = _make_binding(ws_id, computer_id)
        wm.resolve_binding = AsyncMock(return_value=binding)
        wm._ensure_project_attached = AsyncMock()
        # The composite has to be this project's own; a sibling's is never
        # handed out warm, cooldown or not.
        session = _make_session(version=0, summary="cached", config_owner=ws_id)
        wm._machine(computer_id).session = session
        wm._record_sync(computer_id)  # cooldown active

        identity = AsyncMock(return_value={"status": "running", "sandbox_id": "sb-1"})
        # resolve must NOT be called within cooldown.
        with (
            patch(
                "src.server.services.mcp_config.resolve_mcp_config",
                new_callable=AsyncMock,
            ) as mock_resolve,
            cm_patch(
                "db_get_workspace_identity",
                identity,
            ),
        ):
            result = await wm.get_session_for_workspace(ws_id, user_id="user-1")

        assert result is session
        identity.assert_awaited_once_with(ws_id)
        mock_get_ws.assert_not_awaited()
        mock_resolve.assert_not_awaited()

    @pytest.mark.asyncio
    @cm_patch("db_get_workspace", new_callable=AsyncMock)
    # Only _lifecycle binds the status write now, so it is a dotted-path patch.
    @patch(
        "src.server.services.computer_manager._lifecycle.update_workspace_status",
        new_callable=AsyncMock,
    )
    @cm_patch("update_workspace_activity", new_callable=AsyncMock)
    async def test_a_superseded_resolve_gets_past_the_cooldown(
        self, mock_activity, mock_status, mock_get_ws
    ):
        """The withheld version stamp only closes the loop if something reads it.

        A superseded resolve clears ``mcp_config_version`` so the next acquire
        re-resolves, but the version is read on the slow path and the cooldown
        returns before it. So the stamp alone bought nothing: for a full cooldown
        the session kept serving a composite built from grants a newer resolve
        had already replaced -- the declined tools' wrappers and docs among them.
        """
        wm = WorkspaceManager.get_instance(config=_make_config())
        ws_id = str(uuid.uuid4())
        computer_id = str(uuid.uuid4())
        binding = _make_binding(ws_id, computer_id)
        wm.resolve_binding = AsyncMock(return_value=binding)
        wm._ensure_project_attached = AsyncMock()
        session = _make_session(version=None, summary="stale")
        wm._machine(computer_id).session = session
        wm._record_sync(computer_id)  # cooldown active
        # The mark rides the machine, the way the cooldown it has to bypass does.
        wm._machine(computer_id).resolve_superseded.add(ws_id)

        workspace = _make_workspace(ws_id, mcp_config_version=5)
        mock_get_ws.return_value = workspace
        wm._apply_session_platform_secret = AsyncMock()
        wm._sync_sandbox_assets = AsyncMock()
        wm._maybe_restore_files = AsyncMock()

        with (
            patch(
                "src.server.services.mcp_config.resolve_mcp_config",
                new_callable=AsyncMock,
                return_value=_resolved(5),
            ) as mock_resolve,
            patch(
                "src.server.services.computer_manager._mcp.sync_egress_relay",
                new=AsyncMock(return_value=RelayBind.APPLIED),
            ),
            patch(
                "ptc_agent.core.mcp_registry.build_composite_registry",
                return_value=MagicMock(),
            ),
            patch(
                "ptc_agent.agent.prompts.formatter.build_tool_summary_from_registry",
                return_value="NEW",
            ),
            _patch_identity(workspace),
        ):
            await wm.get_session_for_workspace(ws_id, user_id="user-1")

        mock_resolve.assert_awaited_once()
        assert session.mcp_config_version == 5
        # Spent, not held: the mark exists to get one acquire to the version
        # read, and the resolve it forced either succeeded or re-marked.
        assert not wm._machine(computer_id).resolve_superseded


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
                _make_binding("ws", "computer-1"), "user-1", session, ws_version=3
            )

        assert out is None
        mock_resolve.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_siblings_version_is_not_this_workspaces_version(self):
        """Config versions are per workspace and both start at zero.

        Siblings share one session on a computer, so a number match alone would
        let whoever resolved first define the tool set for the rest.
        """
        wm = WorkspaceManager.get_instance(config=_make_config())
        session = _make_session(version=3, summary="already", config_owner="ws-a")
        resolved = _resolved(3)

        with (
            patch(
                "src.server.services.mcp_config.resolve_mcp_config",
                new_callable=AsyncMock,
                return_value=resolved,
            ) as mock_resolve,
            patch(
                "src.server.services.computer_manager._mcp.sync_egress_relay",
                new=AsyncMock(return_value=RelayBind.APPLIED),
            ),
            patch(
                "ptc_agent.core.mcp_registry.build_composite_registry",
                return_value=MagicMock(),
            ),
            patch(
                "ptc_agent.agent.prompts.formatter.build_tool_summary_from_registry",
                return_value="SUMMARY",
            ),
        ):
            out = await wm._apply_session_mcp(
                _make_binding("ws-b", "computer-1"), "user-1", session, ws_version=3
            )

        assert out is resolved
        mock_resolve.assert_awaited_once()
        assert session.mcp_config_workspace_id == "ws-b"

    @pytest.mark.asyncio
    async def test_alternating_siblings_reuse_their_own_composites(self):
        wm = WorkspaceManager.get_instance(config=_make_config())
        session = _make_session(version=None, summary=None, config_owner=None)
        session.computer_id = "computer-1"
        registries = [MagicMock(name="registry-a"), MagicMock(name="registry-b")]
        resolved = [_resolved(4), _resolved(7)]

        with (
            patch(
                "src.server.services.mcp_config.resolve_mcp_config",
                new=AsyncMock(side_effect=resolved),
            ) as resolve,
            patch(
                "src.server.services.computer_manager._mcp.sync_egress_relay",
                new=AsyncMock(return_value=RelayBind.APPLIED),
            ),
            patch(
                "src.server.services.computer_manager._mcp.maybe_remint_egress_jwt",
                new=AsyncMock(),
            ),
            patch(
                "ptc_agent.core.mcp_registry.build_composite_registry",
                side_effect=registries,
            ),
            patch(
                "ptc_agent.agent.prompts.formatter.build_tool_summary_from_registry",
                side_effect=["A", "B"],
            ),
        ):
            for workspace_id, version in (
                ("ws-a", 4),
                ("ws-b", 7),
                ("ws-a", 4),
                ("ws-b", 7),
            ):
                await wm._apply_session_mcp(
                    _make_binding(workspace_id, "computer-1"),
                    "user-1",
                    session,
                    ws_version=version,
                )

        assert resolve.await_count == 2
        assert wm.tool_view(session, "ws-a").mcp_registry is registries[0]
        assert wm.tool_view(session, "ws-b").mcp_registry is registries[1]

    @pytest.mark.asyncio
    async def test_apply_session_mcp_resolves_and_caches(self):
        """First apply resolves, installs the composite, and stamps version +
        summary on the session."""
        wm = WorkspaceManager.get_instance(config=_make_config())
        session = _make_session(version=None, summary=None)
        resolved = _resolved(2)

        composite = MagicMock()
        with (
            patch(
                "src.server.services.mcp_config.resolve_mcp_config",
                new_callable=AsyncMock,
                return_value=resolved,
            ) as mock_resolve,
            patch(
                "src.server.services.computer_manager._mcp.sync_egress_relay",
                new=AsyncMock(return_value=RelayBind.APPLIED),
            ),
            patch(
                "ptc_agent.core.mcp_registry.build_composite_registry",
                return_value=composite,
            ),
            patch(
                "ptc_agent.agent.prompts.formatter.build_tool_summary_from_registry",
                return_value="SUMMARY",
            ),
        ):
            out = await wm._apply_session_mcp(
                _make_binding("ws", "computer-1"), "user-1", session, ws_version=2
            )

        assert out is resolved
        mock_resolve.assert_awaited_once()
        assert session.mcp_registry is composite
        assert session.sandbox.mcp_registry is composite
        assert session.mcp_tool_summary == "SUMMARY"
        assert session.mcp_config_version == 2
        assert session.mcp_config_workspace_id == "ws"

    @pytest.mark.asyncio
    async def test_phase2_waiter_installs_its_own_workspace_view(self):
        wm = WorkspaceManager.get_instance(config=_make_config())
        computer_id = str(uuid.uuid4())
        workspace_a = str(uuid.uuid4())
        workspace_b = str(uuid.uuid4())
        bindings = {
            workspace_a: _make_binding(workspace_a, computer_id),
            workspace_b: _make_binding(workspace_b, computer_id),
        }
        rows = {
            workspace_a: _make_workspace(workspace_a, mcp_config_version=1),
            workspace_b: _make_workspace(workspace_b, mcp_config_version=1),
        }
        session = _make_session(version=None, summary=None, config_owner=None)
        session.computer_id = computer_id
        wm._machine(computer_id).session = session
        wm.resolve_binding = AsyncMock(
            side_effect=lambda workspace_id: bindings[workspace_id]
        )
        wm._ensure_project_attached = AsyncMock()
        wm._apply_session_platform_secret = AsyncMock()
        wm._sync_sandbox_assets = AsyncMock()
        wm._servers_needing_discovery = MagicMock(return_value=[])

        owner_started = asyncio.Event()
        release_owner = asyncio.Event()
        registries = {
            workspace_a: MagicMock(name="registry-a"),
            workspace_b: MagicMock(name="registry-b"),
        }

        async def apply(binding, _user_id, active_session, *, ws_version):
            if binding.workspace_id == workspace_a:
                owner_started.set()
                await release_owner.wait()
            active_session.mcp_config_workspace_id = binding.workspace_id
            active_session.mcp_config_version = ws_version
            active_session.mcp_registry = registries[binding.workspace_id]
            active_session.mcp_tool_summary = f"summary-{binding.workspace_id}"
            active_session.direct_mcp_tools = {}
            return _resolved(ws_version or 0)

        wm._apply_session_mcp = AsyncMock(side_effect=apply)

        async def workspace_row(workspace_id):
            return rows[workspace_id]

        identity = AsyncMock(
            side_effect=lambda workspace_id: {
                "status": "running",
                "sandbox_id": rows[workspace_id]["sandbox_id"],
            }
        )
        with (
            cm_patch("db_get_workspace", AsyncMock(side_effect=workspace_row)),
            cm_patch("db_get_workspace_identity", identity),
        ):
            owner = asyncio.create_task(
                wm.get_session_for_workspace(workspace_a, user_id="user-1")
            )
            await owner_started.wait()
            waiter = asyncio.create_task(
                wm.get_session_for_workspace(workspace_b, user_id="user-1")
            )
            await asyncio.sleep(0)
            release_owner.set()
            await asyncio.gather(owner, waiter)

        view_b = wm.tool_view(session, workspace_b)
        assert view_b.mcp_registry is registries[workspace_b]

    @pytest.mark.asyncio
    async def test_a_superseded_resolve_publishes_nothing(self):
        """A resolve a newer version has overtaken must not reach the sandbox.

        Everything derived from it is stale by the same amount, including the
        tool set the wrappers and per-tool docs come from. Reporting no change
        keeps asset sync from running, and withholding the workspace entry
        forces a re-resolve without changing a sibling's view.
        """
        wm = WorkspaceManager.get_instance(config=_make_config())
        session = _make_session(version=None, summary=None)
        resolved = _resolved(2)
        computer_id = "computer-1"

        composite = MagicMock()
        with (
            patch(
                "src.server.services.mcp_config.resolve_mcp_config",
                new_callable=AsyncMock,
                return_value=resolved,
            ),
            patch(
                "src.server.services.computer_manager._mcp.sync_egress_relay",
                new=AsyncMock(return_value=RelayBind.SUPERSEDED),
            ),
            patch(
                "ptc_agent.core.mcp_registry.build_composite_registry",
                return_value=composite,
            ),
            patch(
                "ptc_agent.agent.prompts.formatter.build_tool_summary_from_registry",
                return_value="SUMMARY",
            ),
        ):
            out = await wm._apply_session_mcp(
                _make_binding("ws", computer_id), "user-1", session, ws_version=2
            )

        # None is what the callers read as "nothing changed", and it is the only
        # thing standing between a stale composite and the asset sync.
        assert out is None
        assert session.mcp_config_version is None
        assert session.mcp_registry is session._builtin_mcp_registry
        view = wm._workspace_tool_view(computer_id, "ws", session)
        assert view is not None
        assert view.mcp_registry is session._builtin_mcp_registry
        assert view.mcp_config_version is None
        # The stamp is read on the slow path only, so the mark is what carries
        # the next acquire past the sync cooldown far enough to read it. The
        # cooldown is the machine's, so the mark is too.
        assert wm._machine(computer_id).resolve_superseded == {"ws"}

    @pytest.mark.asyncio
    async def test_apply_session_mcp_withholds_the_stamp_on_a_refused_push(self):
        """A refused relay-credential push must not be stamped as applied:
        nothing else re-pushes the file, so the version stays None (the
        busted-stamp idiom) and the next acquire re-resolves and retries.
        The composite still installs — non-OAuth tools stay live."""
        wm = WorkspaceManager.get_instance(config=_make_config())
        session = _make_session(version=None, summary=None)
        resolved = _resolved(2)

        composite = MagicMock()
        with (
            patch(
                "src.server.services.mcp_config.resolve_mcp_config",
                new_callable=AsyncMock,
                return_value=resolved,
            ),
            patch(
                "src.server.services.computer_manager._mcp.sync_egress_relay",
                new=AsyncMock(return_value=RelayBind.REFUSED),
            ),
            patch(
                "ptc_agent.core.mcp_registry.build_composite_registry",
                return_value=composite,
            ),
            patch(
                "ptc_agent.agent.prompts.formatter.build_tool_summary_from_registry",
                return_value="SUMMARY",
            ),
        ):
            out = await wm._apply_session_mcp(
                _make_binding("ws", "computer-1"), "user-1", session, ws_version=2
            )

        assert out is resolved
        assert session.mcp_registry is composite
        assert session.mcp_tool_summary == "SUMMARY"
        assert session.mcp_config_version is None

    @pytest.mark.asyncio
    async def test_apply_session_mcp_withholds_the_stamp_when_the_sync_raises(self):
        """The swallowed-exception path is a failed publication too — it must
        leave the same retry signal as an explicit refusal."""
        wm = WorkspaceManager.get_instance(config=_make_config())
        session = _make_session(version=None, summary=None)
        resolved = _resolved(2)

        with (
            patch(
                "src.server.services.mcp_config.resolve_mcp_config",
                new_callable=AsyncMock,
                return_value=resolved,
            ),
            patch(
                "src.server.services.computer_manager._mcp.sync_egress_relay",
                new=AsyncMock(side_effect=RuntimeError("relay down")),
            ),
            patch(
                "ptc_agent.core.mcp_registry.build_composite_registry",
                return_value=MagicMock(),
            ),
            patch(
                "ptc_agent.agent.prompts.formatter.build_tool_summary_from_registry",
                return_value="SUMMARY",
            ),
        ):
            out = await wm._apply_session_mcp(
                _make_binding("ws", "computer-1"), "user-1", session, ws_version=2
            )

        assert out is resolved
        assert session.mcp_config_version is None

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

        with (
            patch(
                "ptc_agent.core.mcp_registry.build_composite_registry",
                side_effect=fake_build,
            ),
            patch(
                "ptc_agent.agent.prompts.formatter.build_tool_summary_from_registry",
                return_value="S",
            ),
        ):
            await wm._install_session_composite(session, resolved, workspace_id="ws")

        assert captured["reg"] is builtin

    @pytest.mark.asyncio
    async def test_composite_prefers_the_user_tier_for_inherited_servers(self):
        """A workspace snapshot of an inherited server is OAuth-blind and can
        outlive a disconnect/reconnect; the host-side user snapshot is purged
        and refreshed with the connection. The agent lane must serve the user
        tier — the same precedence the effective-list API already used."""
        from ptc_agent.config.core import MCPServerConfig
        from src.server.services.mcp_discovery import mcp_discovery_fingerprint

        wm = WorkspaceManager.get_instance(config=_make_config())
        session = _make_session(version=1, summary="old")
        inherited = MCPServerConfig(
            name="robinhood",
            transport="http",
            url="https://api.example.test/mcp",
            source="user",
        )
        resolved = resolved_mcp(version=2, inherited=[inherited])

        def _row(tool_name):
            return {
                "server_name": "robinhood",
                "status": "ok",
                "config_hash": mcp_discovery_fingerprint(inherited),
                "tools": [{"name": tool_name}],
            }

        captured = {}

        def fake_build(reg, servers, schemas, disabled=frozenset()):
            captured["schemas"] = schemas
            return MagicMock()

        with (
            patch(
                "src.server.database.mcp_tool_schemas.get_tool_schemas",
                new=AsyncMock(return_value=[_row("stale_in_sandbox")]),
            ),
            patch(
                "src.server.database.mcp_tool_schemas.get_user_tool_schemas",
                new=AsyncMock(return_value=[_row("fresh_host_side")]),
            ),
            patch(
                "ptc_agent.core.mcp_registry.build_composite_registry",
                side_effect=fake_build,
            ),
            patch(
                "ptc_agent.agent.prompts.formatter.build_tool_summary_from_registry",
                return_value="S",
            ),
        ):
            await wm._install_session_composite(
                session, resolved, user_id="user-1", workspace_id="ws"
            )

        assert [t["name"] for t in captured["schemas"]["robinhood"]] == [
            "fresh_host_side"
        ]


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
        computer_id = str(uuid.uuid4())
        # The session must be the live one for the machine, else the liveness
        # re-check short-circuits discovery (see _cancel/_session_live).
        wm._machine(computer_id).session = session

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
            wm._kick_mcp_discovery(
                _make_binding("ws", computer_id), "user-1", session, [server], 2
            )
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
        wm._kick_mcp_discovery(
            _make_binding("ws", "computer-1"), "user-1", session, [], 2
        )
        assert len(wm._mcp_discovery_tasks) == 0

    @pytest.mark.asyncio
    async def test_kick_discovery_short_circuits_when_session_not_live(self):
        """If the session was evicted (stopped/deleted) before the task runs,
        discovery short-circuits and never calls discover_and_cache."""
        wm = WorkspaceManager.get_instance(config=_make_config())
        session = _make_session(version=2, summary="s")
        # Session is NOT registered as the live session for the machine.
        server = MagicMock()
        server.name = "alpha"
        mock_discover = AsyncMock(return_value=[])
        with patch(
            "src.server.services.mcp_discovery.discover_and_cache",
            new=mock_discover,
        ):
            wm._kick_mcp_discovery(
                _make_binding("ws", "computer-1"), "user-1", session, [server], 2
            )
            task = next(iter(wm._mcp_discovery_tasks))
            await task
        mock_discover.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_cancel_mcp_discovery_cancels_in_flight_task(self):
        """_cancel_mcp_discovery cancels a machine's in-flight discovery task
        and prunes the per-machine map (used by stop/delete)."""
        wm = WorkspaceManager.get_instance(config=_make_config())
        session = _make_session(version=2, summary="s")
        computer_id = str(uuid.uuid4())
        wm._machine(computer_id).session = session

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
            wm._kick_mcp_discovery(
                _make_binding("ws", computer_id), "user-1", session, [server], 2
            )
            await started.wait()
            task = next(iter(wm._machine(computer_id).discovery_tasks))

            wm._cancel_mcp_discovery(computer_id)

            with pytest.raises(asyncio.CancelledError):
                await task
            assert task.cancelled()
            # The record's task set is pruned; the global set drains via the
            # done callback.
            assert not wm._machine(computer_id).discovery_tasks
            assert task not in wm._mcp_discovery_tasks

    @pytest.mark.asyncio
    @cm_patch("db_get_workspace", new_callable=AsyncMock)
    async def test_stop_workspace_cancels_discovery(self, mock_get_ws):
        """stop_workspace cancels the in-flight discovery on the machine it stops.

        The sandbox goes with the computer, so every project's probe on it has
        to go too, not just the one that asked for the stop.
        """
        wm = WorkspaceManager.get_instance(config=_make_config())
        ws_id = str(uuid.uuid4())
        computer_id = str(uuid.uuid4())
        binding = _make_binding(ws_id, computer_id)
        wm.resolve_binding = AsyncMock(return_value=binding)
        mock_get_ws.return_value = _make_workspace(ws_id, status="stopped")

        async def never_returns(*a, **k):
            await asyncio.Event().wait()
            return []

        session = _make_session(version=1, summary="s")
        session.stop = AsyncMock()
        wm._machine(computer_id).session = session
        wm._machine_has_active_tasks = AsyncMock(return_value=False)
        # Winning the claim is what elects this task the one that tears down,
        # and its row is where the teardown reads the sandbox to stop.
        wm._claim_machine_for_stop = AsyncMock(
            return_value=_make_computer(computer_id, status="stopping")
        )
        wm._backup_machine_files_to_db = AsyncMock()
        wm._settle_machine_stop = AsyncMock()

        with (
            patch(
                "src.server.services.mcp_discovery.discover_and_cache",
                new=AsyncMock(side_effect=never_returns),
            ),
            patch(
                "src.server.services.computer_manager._machines.update_computer_status",
                new=AsyncMock(
                    return_value=_make_computer(computer_id, status="stopping")
                ),
            ),
        ):
            server = MagicMock()
            server.name = "alpha"
            wm._kick_mcp_discovery(binding, "user-1", session, [server], 1)
            task = next(iter(wm._machine(computer_id).discovery_tasks))
            # Give the task a tick to enter discover_and_cache.
            await asyncio.sleep(0)

            await wm.stop_workspace(ws_id)

            with pytest.raises(asyncio.CancelledError):
                await task
        assert not wm._machine(computer_id).discovery_tasks

    @pytest.mark.asyncio
    async def test_servers_needing_discovery_settles_on_snapshot_not_tools(self):
        """Settlement is the recorded ok-snapshot set, not the composite's tool
        count — alpha is settled even though it contributes zero tools."""
        wm = WorkspaceManager.get_instance(config=_make_config())
        session = _make_session()
        session.mcp_settled_servers = {"alpha"}
        # The registry would have called alpha unsettled (zero tools) — the
        # predicate must not consult it.
        session.mcp_registry.get_all_tools = MagicMock(
            return_value={"alpha": [], "beta": []}
        )
        resolved = _resolved(2, servers=[_srv("alpha"), _srv("beta")])

        needing = wm._servers_needing_discovery(session, resolved)
        assert [s.name for s in needing] == ["beta"]

    @pytest.mark.asyncio
    async def test_an_ok_empty_snapshot_settles_discovery(self):
        """A server that legitimately advertises zero tools (or whose tools
        were all sanitized out) has an ok snapshot and must NOT re-probe on
        every acquire — its untrusted process would otherwise respawn forever."""
        from ptc_agent.config.core import MCPServerConfig
        from src.server.services.mcp_discovery import mcp_discovery_fingerprint

        wm = WorkspaceManager.get_instance(config=_make_config())
        session = _make_session(version=1, summary="old")
        server = MCPServerConfig(
            name="zerotool",
            transport="stdio",
            command="npx",
            args=["-y", "zero-tool-server"],
            source="workspace",
        )
        resolved = resolved_mcp(version=2, local=[server])

        with (
            patch(
                "src.server.database.mcp_tool_schemas.get_tool_schemas",
                new=AsyncMock(
                    return_value=[
                        {
                            "server_name": "zerotool",
                            "status": "ok",
                            "config_hash": mcp_discovery_fingerprint(server),
                            "tools": [],
                        }
                    ]
                ),
            ),
            patch(
                "ptc_agent.core.mcp_registry.build_composite_registry",
                return_value=MagicMock(),
            ),
            patch(
                "ptc_agent.agent.prompts.formatter.build_tool_summary_from_registry",
                return_value="S",
            ),
        ):
            await wm._install_session_composite(session, resolved, workspace_id="ws")

        assert session.mcp_settled_servers == {"zerotool"}
        assert wm._servers_needing_discovery(session, resolved) == []

    @pytest.mark.asyncio
    async def test_an_error_snapshot_still_needs_discovery(self):
        """Error rows are not settlement — the next acquire retries them."""
        from ptc_agent.config.core import MCPServerConfig
        from src.server.services.mcp_discovery import mcp_discovery_fingerprint

        wm = WorkspaceManager.get_instance(config=_make_config())
        session = _make_session(version=1, summary="old")
        server = MCPServerConfig(
            name="flaky",
            transport="stdio",
            command="npx",
            args=["-y", "flaky-server"],
            source="workspace",
        )
        resolved = resolved_mcp(version=2, local=[server])

        with (
            patch(
                "src.server.database.mcp_tool_schemas.get_tool_schemas",
                new=AsyncMock(
                    return_value=[
                        {
                            "server_name": "flaky",
                            "status": "error",
                            "config_hash": mcp_discovery_fingerprint(server),
                            "tools": [],
                        }
                    ]
                ),
            ),
            patch(
                "ptc_agent.core.mcp_registry.build_composite_registry",
                return_value=MagicMock(),
            ),
            patch(
                "ptc_agent.agent.prompts.formatter.build_tool_summary_from_registry",
                return_value="S",
            ),
        ):
            await wm._install_session_composite(session, resolved, workspace_id="ws")

        assert session.mcp_settled_servers == set()
        assert [s.name for s in wm._servers_needing_discovery(session, resolved)] == [
            "flaky"
        ]

    @pytest.mark.asyncio
    @cm_patch("db_get_workspace", new_callable=AsyncMock)
    @patch(
        "src.server.services.computer_manager._lifecycle.update_workspace_status",
        new_callable=AsyncMock,
    )
    @cm_patch("update_workspace_activity", new_callable=AsyncMock)
    async def test_version_delta_triggers_resolve_off_the_lock(
        self, mock_activity, mock_status, mock_get_ws
    ):
        """A cached ready session whose cooldown expired + version drift triggers
        a re-resolve in Phase 2 (OUTSIDE the machine lock)."""
        wm = WorkspaceManager.get_instance(config=_make_config())
        ws_id = str(uuid.uuid4())
        computer_id = str(uuid.uuid4())
        binding = _make_binding(ws_id, computer_id)
        wm.resolve_binding = AsyncMock(return_value=binding)
        wm._ensure_project_attached = AsyncMock()
        # Session is on version 0; workspace row now says version 5.
        session = _make_session(version=0, summary="old")
        wm._machine(computer_id).session = session
        # Cooldown expired, so the slow path runs.
        wm._machine(computer_id).last_sync_at = None

        workspace = _make_workspace(ws_id, mcp_config_version=5)
        mock_get_ws.return_value = workspace

        # Assert the resolve does NOT run while the machine lock is held.
        lock_held_during_resolve = {"value": False}
        orig_apply = wm._apply_session_mcp

        async def tracking_apply(*a, **k):
            lock_held_during_resolve["value"] = wm._machine_lock(computer_id).locked()
            return await orig_apply(*a, **k)

        wm._apply_session_mcp = tracking_apply
        wm._apply_session_platform_secret = AsyncMock()
        wm._sync_sandbox_assets = AsyncMock()
        wm._maybe_restore_files = AsyncMock()

        resolved = _resolved(5)
        with (
            patch(
                "src.server.services.mcp_config.resolve_mcp_config",
                new_callable=AsyncMock,
                return_value=resolved,
            ) as mock_resolve,
            patch(
                "src.server.services.computer_manager._mcp.sync_egress_relay",
                new=AsyncMock(return_value=RelayBind.APPLIED),
            ),
            patch(
                "ptc_agent.core.mcp_registry.build_composite_registry",
                return_value=MagicMock(),
            ),
            patch(
                "ptc_agent.agent.prompts.formatter.build_tool_summary_from_registry",
                return_value="NEW",
            ),
            _patch_identity(workspace),
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
        assert (
            wm.get_applied_mcp_config_version("ws-x", expected_sandbox_id="sb-1")
            is None
        )

    def test_applied_version_reads_warm_session(self):
        wm = WorkspaceManager.get_instance(config=_make_config())
        # _put_session is what indexes the project onto the machine holding its
        # session; without it the getter answers None for every workspace.
        wm._put_session(
            "computer-1", _make_session(version=7, summary="s"), workspace_id="ws"
        )
        assert wm.get_applied_mcp_config_version("ws", expected_sandbox_id="sb-1") == 7

    def test_applied_version_none_when_session_holds_a_superseded_sandbox(self):
        """The version claims what a LIVE sandbox applied. A session bound to a
        replaced sandbox can only name a number no live sandbox has."""
        wm = WorkspaceManager.get_instance(config=_make_config())
        wm._put_session(
            "computer-1", _make_session(version=7, summary="s"), workspace_id="ws"
        )
        assert (
            wm.get_applied_mcp_config_version("ws", expected_sandbox_id="sb-2") is None
        )

    @pytest.mark.asyncio
    async def test_proactive_apply_warms_without_ready_session(self):
        """No live session ⇒ still acquire, which warms (cold-starts) the
        sandbox. A user who just configured a server expects it to come up and
        verify regardless of whether the sandbox happened to be running."""
        wm = WorkspaceManager.get_instance(config=_make_config())
        wm._acquire_session = AsyncMock()
        with _patch_computer_for_workspace(None):
            await wm.proactively_apply_mcp_config("ws-x", "user-1")
        wm._acquire_session.assert_awaited_once_with("ws-x", user_id="user-1")

    @pytest.mark.asyncio
    async def test_proactive_apply_clears_cooldown_and_reacquires(self):
        """A warm session ⇒ clear the 30s sync cooldown (so the re-acquire
        actually re-syncs rather than short-circuiting) and re-acquire.

        The cooldown is keyed by machine, so clearing it means resolving the
        project to its computer first.
        """
        wm = WorkspaceManager.get_instance(config=_make_config())
        computer_id = str(uuid.uuid4())
        wm._put_session(
            computer_id, _make_session(version=2, summary="s"), workspace_id="ws"
        )
        session = wm._cached_session(computer_id)
        assert session is not None
        wm._freeze_tool_view(computer_id, "ws", session)
        wm._record_sync(computer_id, "ws")
        assert wm._sync_cooldown_ok(computer_id, "ws")
        wm._acquire_session = AsyncMock()

        with _patch_computer_for_workspace(_make_computer(computer_id)):
            await wm.proactively_apply_mcp_config("ws", "user-1")

        assert not wm._sync_cooldown_ok(computer_id, "ws")
        wm._acquire_session.assert_awaited_once_with("ws", user_id="user-1")

    @pytest.mark.asyncio
    async def test_proactive_apply_swallows_errors(self):
        """Best-effort: a failure never propagates — it just falls back to the
        next-message apply, so a mutation response is never affected."""
        wm = WorkspaceManager.get_instance(config=_make_config())
        computer_id = str(uuid.uuid4())
        wm._put_session(
            computer_id, _make_session(version=2, summary="s"), workspace_id="ws"
        )
        wm._acquire_session = AsyncMock(side_effect=RuntimeError("boom"))
        with _patch_computer_for_workspace(_make_computer(computer_id)):
            await wm.proactively_apply_mcp_config("ws", "user-1")  # must not raise

    @pytest.mark.asyncio
    async def test_refresh_busts_session_version_then_applies(self):
        """An out-of-band schema-cache update (manual /discover probe) has no
        version bump, so the apply path would short-circuit; refresh busts the
        session's cached version first so the re-acquire actually rebuilds."""
        wm = WorkspaceManager.get_instance(config=_make_config())
        session = _make_session(version=4, summary="cached")
        wm._put_session("computer-1", session, workspace_id="ws")
        wm._freeze_tool_view("computer-1", "ws", session)
        wm.proactively_apply_mcp_config = AsyncMock()

        await wm.refresh_session_mcp("ws", "user-1")

        view = wm._workspace_tool_view("computer-1", "ws", session)
        assert view is not None
        assert view.mcp_config_version is None
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
    ``self._cached_session(computer_id) is session``; if the kick fires before
    the session is cached, the task exits permanently. Both _recover_sandbox
    and _attach_running_session must cache the session BEFORE the kick.
    """

    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    @staticmethod
    def _stub_provisioning(wm):
        """Everything the provision path reaches that is not the kick itself."""
        wm._workspace_folder = AsyncMock(return_value="ws")
        wm._mint_sandbox_tokens = AsyncMock(return_value={})
        wm._apply_session_mcp = AsyncMock(
            return_value=_resolved(1, servers=[_srv("alpha")])
        )
        wm._servers_needing_discovery = MagicMock(
            return_value=[MagicMock(name="alpha")]
        )
        wm._sync_sandbox_assets = AsyncMock()
        wm._reconcile_skills = AsyncMock()
        wm._restore_files = AsyncMock()
        wm._maybe_restore_files = AsyncMock()

    @pytest.mark.asyncio
    @cm_patch("update_workspace_activity", new_callable=AsyncMock)
    @cm_patch("SessionManager")
    async def test_recover_sandbox_caches_before_kick(
        self, mock_session_mgr, mock_activity
    ):
        """_recover_sandbox caches the session before _kick_mcp_discovery, so the
        kick's liveness check (``_cached_session(computer) is session``) passes."""
        wm = WorkspaceManager.get_instance(config=_make_config())
        ws_id = str(uuid.uuid4())
        computer_id = str(uuid.uuid4())
        binding = _make_binding(ws_id, computer_id, resource_tier="standard")

        session = _make_session(version=1, summary="s")
        session.initialize = AsyncMock()
        session.sandbox.sandbox_id = "sb-new"
        mock_session_mgr.get_session.return_value = session

        self._stub_provisioning(wm)

        # Spy: record whether the session was already cached at kick time.
        cached_at_kick = {"value": None}

        def spy_kick(bound, *a, **k):
            cached_at_kick["value"] = wm._cached_session(bound.computer_id) is session
            # Don't actually schedule the background task.

        wm._kick_mcp_discovery = spy_kick

        with (
            _patch_certify(),
            _patch_machine_bind(_make_workspace(ws_id), computer_id=computer_id),
        ):
            await wm._recover_sandbox(binding, "user-1", MagicMock())

        assert cached_at_kick["value"] is True
        assert wm._cached_session(computer_id) is session

    @pytest.mark.asyncio
    @cm_patch("SessionManager")
    async def test_attach_running_session_caches_before_kick(self, mock_session_mgr):
        """_attach_running_session caches the session before _kick_mcp_discovery
        so the kick's liveness check passes on a cold init."""
        wm = WorkspaceManager.get_instance(config=_make_config())
        ws_id = str(uuid.uuid4())
        computer_id = str(uuid.uuid4())
        binding = _make_binding(ws_id, computer_id)

        session = _make_session(version=1, summary="s")
        session._initialized = False
        session.initialize = AsyncMock()
        mock_session_mgr.get_session.return_value = session

        wm._apply_session_platform_secret = AsyncMock()
        wm._apply_session_mcp = AsyncMock(
            return_value=_resolved(1, servers=[_srv("alpha")])
        )
        wm._servers_needing_discovery = MagicMock(
            return_value=[MagicMock(name="alpha")]
        )
        wm._sync_sandbox_assets = AsyncMock()
        wm._reconcile_skills = AsyncMock()
        wm._maybe_migrate_sandbox = AsyncMock(return_value=None)

        cached_at_kick = {"value": None}

        def spy_kick(bound, *a, **k):
            cached_at_kick["value"] = wm._cached_session(bound.computer_id) is session

        wm._kick_mcp_discovery = spy_kick

        workspace = _make_workspace(ws_id, status="running", mcp_config_version=1)
        out_session, did_init = await wm._attach_running_session(
            binding, workspace, "user-1", None, lambda _stage: None
        )

        assert cached_at_kick["value"] is True
        assert out_session is session
        assert did_init is True
        assert wm._cached_session(computer_id) is session

    @pytest.mark.asyncio
    @cm_patch("SessionManager")
    async def test_attach_binds_a_sandbox_it_had_to_build(self, mock_session_mgr):
        """A machine naming no sandbox must be bound to the one ``initialize`` builds.

        ``session.initialize(sandbox_id=None)`` creates a sandbox, so this path
        provisions without going through provisioning. If the binding is not
        written back, the machine still says NULL on the next request, the
        identity check reads that as stale, retires the session, and builds
        another sandbox: one billed sandbox per request, each abandoned with
        its files. Regression: the identity fencing made this fatal rather than
        untidy.
        """
        wm = WorkspaceManager.get_instance(config=_make_config())
        ws_id = str(uuid.uuid4())
        computer_id = str(uuid.uuid4())
        binding = _make_binding(ws_id, computer_id, provider_ref=None)

        session = _make_session(version=1, summary="s")
        session._initialized = False
        session.sandbox.sandbox_id = "sb-built-by-initialize"
        session.initialize = AsyncMock()
        mock_session_mgr.get_session.return_value = session

        wm._apply_session_mcp = AsyncMock(return_value=_resolved(1))
        wm._servers_needing_discovery = MagicMock(return_value=[])
        wm._sync_sandbox_assets = AsyncMock()
        wm._reconcile_skills = AsyncMock()
        wm._maybe_migrate_sandbox = AsyncMock(return_value=None)
        wm._apply_session_platform_secret = AsyncMock()
        wm._kick_mcp_discovery = MagicMock()

        workspace = _make_workspace(ws_id, status="running", mcp_config_version=1)
        workspace["sandbox_id"] = None
        bound_row = dict(workspace, sandbox_id="sb-built-by-initialize")

        with (
            _patch_certify(),
            _patch_machine_bind(bound_row, computer_id=computer_id) as mock_bind,
        ):
            await wm._attach_running_session(
                binding, workspace, "user-1", None, lambda _stage: None
            )

        # computers.provider_ref is the authority now, so the CAS is the one on
        # the machine; the project shadow rides the same statement.
        mock_bind.assert_awaited_once()
        assert mock_bind.await_args.kwargs["provider_ref"] == "sb-built-by-initialize"
        # NULL-safe CAS: the fence is the value we read, so a concurrent binder
        # that got there first makes this write lose rather than clobber.
        assert mock_bind.await_args.kwargs["expected_previous_provider_ref"] is None

    @pytest.mark.asyncio
    @cm_patch("SessionManager")
    async def test_attach_does_not_rebind_when_the_row_already_matches(
        self, mock_session_mgr
    ):
        """Reattaching to the sandbox the machine already names must not write."""
        wm = WorkspaceManager.get_instance(config=_make_config())
        ws_id = str(uuid.uuid4())
        computer_id = str(uuid.uuid4())
        binding = _make_binding(ws_id, computer_id)

        session = _make_session(version=1, summary="s")
        session._initialized = False
        session.initialize = AsyncMock()  # sandbox_id stays 'sb-1', as the row says
        mock_session_mgr.get_session.return_value = session

        wm._apply_session_mcp = AsyncMock(return_value=_resolved(1))
        wm._servers_needing_discovery = MagicMock(return_value=[])
        wm._sync_sandbox_assets = AsyncMock()
        wm._reconcile_skills = AsyncMock()
        wm._maybe_migrate_sandbox = AsyncMock(return_value=None)
        wm._apply_session_platform_secret = AsyncMock()
        wm._kick_mcp_discovery = MagicMock()

        workspace = _make_workspace(ws_id, status="running", mcp_config_version=1)
        with _patch_machine_bind(workspace, computer_id=computer_id) as mock_bind:
            await wm._attach_running_session(
                binding, workspace, "user-1", None, lambda _stage: None
            )

        mock_bind.assert_not_awaited()

    @pytest.mark.asyncio
    @cm_patch("update_workspace_activity", new_callable=AsyncMock)
    @cm_patch("SessionManager")
    async def test_recover_sandbox_unwinds_cache_on_post_kick_failure(
        self, mock_session_mgr, mock_activity
    ):
        """If a post-kick step raises, the broken session is not left cached —
        preserving the old code's 'only cached on full success' semantics."""
        wm = WorkspaceManager.get_instance(config=_make_config())
        ws_id = str(uuid.uuid4())
        computer_id = str(uuid.uuid4())
        binding = _make_binding(ws_id, computer_id, resource_tier="standard")

        session = _make_session(version=1, summary="s")
        session.initialize = AsyncMock()
        session.sandbox.sandbox_id = "sb-new"
        mock_session_mgr.get_session.return_value = session

        self._stub_provisioning(wm)
        wm._kick_mcp_discovery = MagicMock()
        wm._cancel_mcp_discovery = MagicMock()
        # The last step of a provision, i.e. genuinely after the kick.
        wm._record_sync = MagicMock(side_effect=RuntimeError("sync stamp failed"))

        with (
            pytest.raises(RuntimeError, match="sync stamp failed"),
            _patch_certify(),
            _patch_machine_bind(_make_workspace(ws_id), computer_id=computer_id),
        ):
            await wm._recover_sandbox(binding, "user-1", MagicMock())

        assert wm._cached_session(computer_id) is None
        wm._cancel_mcp_discovery.assert_called_once_with(computer_id)


# ---------------------------------------------------------------------------
# _clear_session cancels in-flight discovery (FIX B)
# ---------------------------------------------------------------------------


class TestClearSessionCancelsDiscovery:
    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    @pytest.mark.asyncio
    @cm_patch("SessionManager")
    async def test_clear_session_cancels_pending_discovery(self, mock_session_mgr):
        """_clear_session cancels the machine's in-flight discovery task so it
        can't run against the torn-down session (mirrors stop/delete_workspace)."""
        mock_session_mgr.cleanup_session = AsyncMock()
        wm = WorkspaceManager.get_instance(config=_make_config())
        computer_id = str(uuid.uuid4())
        binding = _make_binding(str(uuid.uuid4()), computer_id)
        session = _make_session(version=1, summary="s")
        wm._machine(computer_id).session = session

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
            wm._kick_mcp_discovery(binding, "user-1", session, [server], 1)
            await started.wait()
            task = next(iter(wm._machine(computer_id).discovery_tasks))

            await wm._clear_session(computer_id)

            with pytest.raises(asyncio.CancelledError):
                await task
            assert task.cancelled()
        assert not wm._machine(computer_id).discovery_tasks
        assert wm._cached_session(computer_id) is None

    @pytest.mark.asyncio
    @cm_patch("SessionManager")
    async def test_clear_session_cancels_before_cleanup(self, mock_session_mgr):
        """The cancel must run as the first step — before cleanup_session — so
        discovery never races the teardown."""
        order: list[str] = []

        async def record_cleanup(_computer):
            order.append("cleanup")

        mock_session_mgr.cleanup_session = AsyncMock(side_effect=record_cleanup)
        wm = WorkspaceManager.get_instance(config=_make_config())
        computer_id = str(uuid.uuid4())

        orig_cancel = wm._cancel_mcp_discovery

        def record_cancel(cid):
            order.append("cancel")
            return orig_cancel(cid)

        wm._cancel_mcp_discovery = record_cancel

        await wm._clear_session(computer_id)

        assert order == ["cancel", "cleanup"]


# ---------------------------------------------------------------------------
# set_computer_spec downgrade disk-fit guard
# ---------------------------------------------------------------------------


class TestSetComputerSpecDiskGuard:
    """A tier downgrade recreates the machine's sandbox on a smaller disk; restore
    is best-effort, so the guard rejects (before any teardown) when the files
    already on the machine won't fit. Upgrades and lateral moves skip the check.

    The guard lives on the computer because one disk holds every project on it.
    """

    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    @pytest.fixture(autouse=True)
    def _quiet_durable_probes(self):
        """The spec-change activity guard also reads the machine's run ledgers,
        and the replacement is durably fenced before teardown; keep all three
        quiet so these tests exercise only the disk-guard mechanics."""
        with (
            patch(
                "src.server.database.runs.lifecycle.computer_has_active_run",
                new=AsyncMock(return_value=False),
            ),
            patch(
                "src.server.database.runs.subagent_runs.count_open_runs_for_computer",
                new=AsyncMock(return_value=0),
            ),
            patch(
                "src.server.services.workspace_entitlements."
                "try_claim_computer_for_start",
                new=AsyncMock(return_value={"status": "starting"}),
            ) as claim,
        ):
            self.claim = claim
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
    @patch(
        "src.server.services.workspace_entitlements.get_workspace_total_size",
        new_callable=AsyncMock,
    )
    @patch(
        "src.server.services.workspace_entitlements.get_live_workspace_ids_for_computer",
        new_callable=AsyncMock,
    )
    @patch(
        "src.server.services.workspace_entitlements.db_set_computer_resource_tier",
        new_callable=AsyncMock,
    )
    @patch("src.server.services.workspace_entitlements.SessionManager")
    @patch(
        "src.server.services.workspace_entitlements.get_computer",
        new_callable=AsyncMock,
    )
    async def test_running_downgrade_rejected_when_files_exceed_disk(
        self,
        mock_get_computer,
        mock_session_mgr,
        mock_set_tier,
        mock_live_ids,
        mock_total_size,
    ):
        wm = WorkspaceManager.get_instance(config=self._config_with_tiers())
        ws_id = str(uuid.uuid4())
        sibling_id = str(uuid.uuid4())
        computer_id = str(uuid.uuid4())
        mock_get_computer.return_value = _make_computer(computer_id)
        # Two projects on one disk. Neither overflows the standard tier's ~1 GiB
        # of usable space on its own; together they do. Summing the machine is
        # the point of the guard: checking only the project that asked is how a
        # two-project machine passes a downgrade its combined files cannot fit.
        mock_live_ids.return_value = [ws_id, sibling_id]
        mock_total_size.return_value = 3 * 1024**3 // 4  # 0.75 GiB each
        wm._machine(computer_id).session = MagicMock(sandbox=MagicMock())
        wm._backup_machine_files_to_db = AsyncMock()
        wm._clear_session = AsyncMock()
        wm._destroy_sandbox = AsyncMock()
        wm._recover_sandbox = AsyncMock()
        wm._workspace_folder = AsyncMock(return_value="proj-1a2b")

        with pytest.raises(RuntimeError, match="Cannot downgrade"):
            await wm.set_computer_spec(
                computer_id, "standard", user_id="user-1", workspace_id=ws_id
            )

        # Backed up (for fresh sizes) but the live sandbox is never torn down.
        wm._backup_machine_files_to_db.assert_awaited_once()
        assert mock_total_size.await_count == 2
        wm._recover_sandbox.assert_not_awaited()
        wm._clear_session.assert_not_awaited()
        wm._destroy_sandbox.assert_not_awaited()
        # Rejected before the replacement was claimed, so the row still
        # advertises running/<old id> rather than being stranded at 'starting'.
        self.claim.assert_not_awaited()
        # Tier set to target optimistically, then reverted to the original.
        assert mock_set_tier.await_args_list[0].args == (computer_id, "standard")
        assert mock_set_tier.await_args_list[-1].args == (computer_id, "max")

    @pytest.mark.asyncio
    @patch(
        "src.server.services.workspace_entitlements.get_workspace_total_size",
        new_callable=AsyncMock,
    )
    @patch(
        "src.server.services.workspace_entitlements.get_live_workspace_ids_for_computer",
        new_callable=AsyncMock,
    )
    @patch(
        "src.server.services.workspace_entitlements.db_set_computer_resource_tier",
        new_callable=AsyncMock,
    )
    @patch("src.server.services.workspace_entitlements.SessionManager")
    @patch(
        "src.server.services.workspace_entitlements.get_computer",
        new_callable=AsyncMock,
    )
    async def test_running_downgrade_allowed_when_files_fit(
        self,
        mock_get_computer,
        mock_session_mgr,
        mock_set_tier,
        mock_live_ids,
        mock_total_size,
    ):
        wm = WorkspaceManager.get_instance(config=self._config_with_tiers())
        ws_id = str(uuid.uuid4())
        sibling_id = str(uuid.uuid4())
        computer_id = str(uuid.uuid4())
        mock_get_computer.return_value = _make_computer(computer_id)
        mock_live_ids.return_value = [ws_id, sibling_id]
        mock_total_size.return_value = 50 * 1024**2  # 100 MiB together, fits
        wm._machine(computer_id).session = MagicMock(sandbox=MagicMock())
        wm._backup_machine_files_to_db = AsyncMock()
        wm._clear_session = AsyncMock()
        wm._destroy_sandbox = AsyncMock()
        wm._recover_sandbox = AsyncMock()
        wm._workspace_folder = AsyncMock(return_value="proj-1a2b")

        await wm.set_computer_spec(
            computer_id, "standard", user_id="user-1", workspace_id=ws_id
        )

        wm._recover_sandbox.assert_awaited_once()
        # Tier never reverted — the last write is the target.
        assert mock_set_tier.await_args_list[-1].args == (computer_id, "standard")

    @pytest.mark.asyncio
    @patch(
        "src.server.services.workspace_entitlements.get_workspace_total_size",
        new_callable=AsyncMock,
    )
    @patch(
        "src.server.services.workspace_entitlements.db_set_computer_resource_tier",
        new_callable=AsyncMock,
    )
    @patch("src.server.services.workspace_entitlements.SessionManager")
    @patch(
        "src.server.services.workspace_entitlements.get_computer",
        new_callable=AsyncMock,
    )
    async def test_upgrade_skips_disk_guard(
        self, mock_get_computer, mock_session_mgr, mock_set_tier, mock_total_size
    ):
        wm = WorkspaceManager.get_instance(config=self._config_with_tiers())
        ws_id = str(uuid.uuid4())
        computer_id = str(uuid.uuid4())
        mock_get_computer.return_value = _make_computer(
            computer_id, resource_tier="standard"
        )
        wm._machine(computer_id).session = MagicMock(sandbox=MagicMock())
        wm._backup_machine_files_to_db = AsyncMock()
        wm._clear_session = AsyncMock()
        wm._destroy_sandbox = AsyncMock()
        wm._recover_sandbox = AsyncMock()
        wm._workspace_folder = AsyncMock(return_value="proj-1a2b")

        await wm.set_computer_spec(
            computer_id, "max", user_id="user-1", workspace_id=ws_id
        )

        mock_total_size.assert_not_awaited()  # no fit check on an upgrade
        wm._recover_sandbox.assert_awaited_once()

    @pytest.mark.asyncio
    @patch(
        "src.server.services.workspace_entitlements.get_workspace_total_size",
        new_callable=AsyncMock,
    )
    @patch(
        "src.server.services.workspace_entitlements.get_live_workspace_ids_for_computer",
        new_callable=AsyncMock,
    )
    @patch(
        "src.server.services.workspace_entitlements.db_set_computer_resource_tier",
        new_callable=AsyncMock,
    )
    @patch(
        "src.server.services.workspace_entitlements.get_computer",
        new_callable=AsyncMock,
    )
    async def test_stopped_downgrade_rejected_before_destroy(
        self, mock_get_computer, mock_set_tier, mock_live_ids, mock_total_size
    ):
        wm = WorkspaceManager.get_instance(config=self._config_with_tiers())
        ws_id = str(uuid.uuid4())
        computer_id = str(uuid.uuid4())
        mock_get_computer.return_value = _make_computer(computer_id, status="stopped")
        mock_live_ids.return_value = [ws_id]
        mock_total_size.return_value = 5 * 1024**3
        wm._destroy_sandbox = AsyncMock()

        with pytest.raises(RuntimeError, match="Cannot downgrade"):
            await wm.set_computer_spec(
                computer_id, "standard", user_id="user-1", workspace_id=ws_id
            )

        wm._destroy_sandbox.assert_not_awaited()
        assert mock_set_tier.await_args_list[-1].args == (
            computer_id,
            "max",
        )  # reverted


class TestVaultPushWritesEachTier:
    """One file per tier: the root carries the user's secrets every shared
    server reads, the workspace file carries its own effective set."""

    def setup_method(self):
        from src.server.services.computer_manager import ComputerManager

        ComputerManager.reset_instance()

    def teardown_method(self):
        from src.server.services.computer_manager import ComputerManager

        ComputerManager.reset_instance()

    @pytest.mark.asyncio
    async def test_the_user_tier_and_the_workspace_set_land_in_their_own_files(
        self,
    ):
        from src.server.services.computer_manager import ComputerManager

        manager = ComputerManager.get_instance(config=_make_config())
        sandbox = MagicMock()
        sandbox.layout.root = "/home/workspace"
        effective = AsyncMock(return_value={"API_KEY": "ws-value", "SEC": "user"})
        user_tier = AsyncMock(return_value={"API_KEY": "user-value", "SEC": "user"})
        publish = AsyncMock(return_value=True)
        with (
            patch("src.server.database.vault_secrets.get_effective_secrets", effective),
            patch(
                "src.server.database.user_vault_secrets.get_user_secrets_decrypted",
                user_tier,
            ),
            patch("ptc_agent.core.sandbox.assets.publish_vault_secrets", publish),
        ):
            await manager.push_vault_secrets("ws-a", sandbox, user_id="user-1")

        effective.assert_awaited_once_with("ws-a", "user-1")
        user_tier.assert_awaited_once_with("user-1")
        publish.assert_awaited_once_with(
            sandbox,
            {
                "_root": {"API_KEY": "user-value", "SEC": "user"},
                "ws-a": {"API_KEY": "ws-value", "SEC": "user"},
            },
        )

    @pytest.mark.asyncio
    async def test_the_owner_is_read_off_the_workspace_when_not_given(self):
        from src.server.services.computer_manager import ComputerManager

        manager = ComputerManager.get_instance(config=_make_config())
        sandbox = MagicMock()
        sandbox.layout.root = "/home/workspace"
        user_tier = AsyncMock(return_value={})
        publish = AsyncMock(return_value=False)
        with (
            patch(
                "src.server.services.computer_manager._mcp.db_get_workspace",
                AsyncMock(return_value={"user_id": "owner-9"}),
            ),
            patch(
                "src.server.database.vault_secrets.get_effective_secrets",
                AsyncMock(return_value={}),
            ),
            patch(
                "src.server.database.user_vault_secrets.get_user_secrets_decrypted",
                user_tier,
            ),
            patch("ptc_agent.core.sandbox.assets.publish_vault_secrets", publish),
        ):
            await manager.push_vault_secrets("ws-a", sandbox)

        user_tier.assert_awaited_once_with("owner-9")
        publish.assert_awaited_once_with(sandbox, {"_root": {}, "ws-a": {}})
