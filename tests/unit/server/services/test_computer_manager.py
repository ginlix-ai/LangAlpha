"""
Tests for ComputerManager, the machine a sandbox belongs to.

Covers the insert that mints a computer and the primary every new project
joins, the machine keying that lets one sandbox serve more than one project, the split-binding window that
makes a cached handle untrustworthy, the computer-scoped activity gate, and
the computer-addressed surface the router codes against.
"""

import asyncio
import hashlib
from contextlib import ExitStack, asynccontextmanager, contextmanager
from types import SimpleNamespace
from unittest.mock import ANY, AsyncMock, MagicMock, patch

import pytest

from ptc_agent.config.core import FilesystemConfig
from ptc_agent.core.sandbox.runtime import SandboxTransientError
from src.server.database.workspace_folders import WorkspaceFolderMoving
from src.server.services.computer_manager import (
    ComputerBinding,
    ComputerManager,
    SessionMetadata,
)
from src.server.services.computer_manager._types import WorkspaceNotOnComputer
from src.server.services.workspace_manager import WorkspaceManager
from tests.computer_manager_patch import cm_patch

_LIFECYCLE = "src.server.services.computer_manager._lifecycle"
_MACHINES = "src.server.services.computer_manager._machines"
_PROVISIONING = "src.server.services.computer_manager._provisioning"
_FOLDERS = "src.server.services.computer_manager._folders"
_MACHINE_BACKUP = "src.server.services.computer_manager._machine_backup"
_SESSIONS = "src.server.services.computer_manager._sessions"
_WORKSPACE_MANAGER = "src.server.services.workspace_manager"

# The six methods WP3's router codes against. Frozen: the router resolves them
# by name on whatever ComputerManager.get_instance() hands back.
COMPUTER_SURFACE = (
    "get_session_for_computer",
    "start_computer",
    "stop_computer",
    "archive_computer",
    "set_computer_spec",
    "set_computer_always_on",
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_config(provider="daytona", working_directory="/home/workspace"):
    """An AgentConfig stand-in with a real provider kind and root.

    Both are read verbatim when a computer row is minted, so they have to be
    strings rather than mock attributes.
    """
    config = MagicMock()
    config.sandbox = SimpleNamespace(provider=provider)
    config.filesystem = SimpleNamespace(working_directory=working_directory)
    config.to_core_config.return_value = SimpleNamespace(
        sandbox=SimpleNamespace(
            provider=provider,
            daytona=SimpleNamespace(api_key="test-key"),
            platform_secrets={},
        ),
        filesystem=FilesystemConfig(working_directory=working_directory),
    )
    return config


def _make_manager(**kwargs):
    return WorkspaceManager.get_instance(config=_make_config(), **kwargs)


def _make_computer(computer_id="comp-1", **overrides):
    computer = {
        "computer_id": computer_id,
        "user_id": "user-1",
        "kind": "daytona",
        "name": "My computer",
        "status": "creating",
        "provider_ref": None,
        "root_dir": "/home/workspace",
        "resource_tier": "standard",
        "is_primary": True,
    }
    computer.update(overrides)
    return computer


def _make_session():
    session = MagicMock()
    session._initialized = True
    session.sandbox = MagicMock()
    session.sandbox.sandbox_id = "sandbox-abc"
    return session


def _make_binding(workspace_id="ws-a", computer_id="comp-1", **overrides):
    """The machine a project is on, as every step below the edge resolve sees it."""
    fields = {
        "kind": "daytona",
        "root_dir": "/home/workspace",
        "provider_ref": "sandbox-abc",
        "resource_tier": "standard",
    }
    fields.update(overrides)
    return ComputerBinding(workspace_id=workspace_id, computer_id=computer_id, **fields)


def _resolving(folder=None):
    """Resolve every project onto one machine, without a row read.

    The folder rides on the binding the edge resolve returns, which is where
    every step below it now reads the project's folder from.
    """
    return AsyncMock(
        side_effect=lambda workspace_id, **_kw: _make_binding(
            workspace_id, dir_name=folder
        )
    )


@contextmanager
def _attaching(manager, session, *, folder="joiner-6c06", resolve_lands=True):
    """Patch an attach down to the tool overlay step and report what it reached.

    ``resolve_lands`` false leaves a sibling's workspace id stamped on the
    session, which is what a resolve that did not land hands the overlay step.
    """

    async def _apply(binding, _user_id, sess, *, ws_version=None):
        sess.mcp_config_workspace_id = (
            binding.workspace_id if resolve_lands else "ws-other"
        )
        return MagicMock()

    async def _synced(*_args, **_kwargs):
        session.sandbox.workspace_overlay_missing.return_value = False
        return {"refreshed": ["tool_modules"]}

    reached = SimpleNamespace(resolve=AsyncMock(side_effect=_apply), sync=AsyncMock(side_effect=_synced))
    patches = (
        ("_acquire_session", AsyncMock(return_value=session)),
        ("resolve_binding", _resolving(folder)),
        ("_ensure_workspace_dirs", AsyncMock()),
        ("_maybe_restore_files", AsyncMock(return_value=True)),
        ("_reconcile_skills", AsyncMock(return_value=True)),
        ("_apply_session_mcp", reached.resolve),
        ("_sync_sandbox_assets", reached.sync),
    )
    with ExitStack() as stack:
        for name, new in patches:
            stack.enter_context(patch.object(manager, name, new=new))
        yield reached


@contextmanager
def _attached(manager, session, *, folder="joiner-6c06"):
    """Patch an attach around the per-folder steps and report the restore.

    The tool overlay is the attach's third job and has its own class below, so
    it is answered as already built here.
    """
    patches = (
        ("_acquire_session", AsyncMock(return_value=session)),
        ("resolve_binding", _resolving(folder)),
        ("_maybe_restore_files", AsyncMock(return_value=True)),
        ("_ensure_project_tool_overlay", AsyncMock(return_value=True)),
    )
    with ExitStack() as stack:
        for name, new in patches:
            stack.enter_context(patch.object(manager, name, new=new))
        yield manager._maybe_restore_files


def _make_workspace(workspace_id="ws-a", **overrides):
    workspace = {
        "workspace_id": workspace_id,
        "user_id": "user-1",
        "computer_id": "comp-1",
        "dir_name": "alpha-1a2b",
        "status": "running",
        "sandbox_id": "sandbox-abc",
    }
    workspace.update(overrides)
    return workspace


def _decision_lock(acquired: bool):
    """Stand in for the Postgres advisory key, yielding whether it was taken."""

    @asynccontextmanager
    async def lock(_computer_id):
        yield acquired

    return lock


def _detached(runtime):
    """Stand in for ``_detached_runtime``, handing back one runtime."""

    @asynccontextmanager
    async def detached(_sandbox_id, *, binding=None):
        yield runtime

    return detached


class _Base:
    def setup_method(self):
        ComputerManager.reset_instance()
        self._retirement_marker = patch(
            f"{_MACHINES}.mark_computer_retirement_pending",
            AsyncMock(return_value=True),
        )
        self._retirement_marker.start()
        self._retirement_clear = patch(
            f"{_MACHINES}.clear_computer_retirement_pending",
            AsyncMock(return_value=True),
        )
        self.clear_retirement_pending = self._retirement_clear.start()
        self._folder_cleanup_complete = patch(
            f"{_FOLDERS}.complete_workspace_folder_cleanup",
            AsyncMock(return_value=True),
        )
        self.complete_folder_cleanup = self._folder_cleanup_complete.start()
        self._folder_cleanup_defer = patch(
            f"{_FOLDERS}.defer_workspace_folder_cleanup",
            AsyncMock(return_value=True),
        )
        self.defer_folder_cleanup = self._folder_cleanup_defer.start()

    def teardown_method(self):
        self._folder_cleanup_defer.stop()
        self._folder_cleanup_complete.stop()
        self._retirement_clear.stop()
        self._retirement_marker.stop()
        ComputerManager.reset_instance()


# ---------------------------------------------------------------------------
# The singleton
# ---------------------------------------------------------------------------


class TestSingleton(_Base):
    """One slot, so the two entry points cannot hand out different managers."""

    def test_get_instance_builds_the_workspace_facade(self):
        manager = ComputerManager.get_instance(config=_make_config())
        assert isinstance(manager, WorkspaceManager)

    def test_both_entry_points_share_the_slot(self):
        first = ComputerManager.get_instance(config=_make_config())
        assert WorkspaceManager.get_instance() is first
        assert ComputerManager.current() is first

    def test_current_is_none_before_construction(self):
        assert ComputerManager.current() is None

    def test_get_instance_requires_config_first_call(self):
        with pytest.raises(ValueError, match="config is required"):
            ComputerManager.get_instance()


# ---------------------------------------------------------------------------
# Minting a computer
# ---------------------------------------------------------------------------


class TestMintingAComputer(_Base):
    """``create_computer_for_user`` writes the row; ``ensure_primary_computer``
    is what every new project goes through, and mints at most one per user."""

    @pytest.mark.asyncio
    @patch("src.server.services.computer_manager._machines.create_computer")
    async def test_the_row_takes_the_deployments_kind_and_root(self, mock_create):
        """Both come from the running deployment rather than the request: a
        machine on a backend this build cannot dial is one nothing can start."""
        manager = _make_manager()
        mock_create.return_value = _make_computer(status="stopped")

        await manager.create_computer_for_user(
            "user-1", name="Research", resource_tier="performance"
        )

        mock_create.assert_awaited_once_with(
            "user-1",
            kind="daytona",
            name="Research",
            is_primary=False,
            status="stopped",
            resource_tier="performance",
            root_dir="/home/workspace",
        )

    @pytest.mark.asyncio
    @patch("src.server.services.computer_manager._machines.create_computer")
    async def test_a_new_machine_is_born_stopped(self, mock_create):
        """Nothing is provisioned here, so 'stopped' is what it is; it also
        means the first start claims it the way it claims any stopped machine.
        """
        manager = _make_manager()
        mock_create.return_value = _make_computer(status="stopped")

        await manager.create_computer_for_user("user-1")

        assert mock_create.await_args.kwargs["status"] == "stopped"
        assert mock_create.await_args.kwargs["resource_tier"] == "standard"

    @pytest.mark.asyncio
    @patch("src.server.services.computer_manager._machines.create_computer")
    @patch("src.server.services.computer_manager._machines.get_primary_computer")
    async def test_an_existing_primary_is_reused_not_duplicated(
        self, mock_primary, mock_create
    ):
        """A second machine per project is a second sandbox to pay for."""
        manager = _make_manager()
        mock_primary.return_value = _make_computer(status="running")

        computer = await manager.ensure_primary_computer("user-1")

        assert computer["computer_id"] == "comp-1"
        mock_create.assert_not_awaited()

    @pytest.mark.asyncio
    @patch("src.server.services.computer_manager._machines.create_computer")
    @patch("src.server.services.computer_manager._machines.get_primary_computer")
    async def test_a_user_with_none_gets_one_marked_primary(
        self, mock_primary, mock_create
    ):
        manager = _make_manager()
        mock_primary.return_value = None
        mock_create.return_value = _make_computer(status="stopped")

        computer = await manager.ensure_primary_computer("user-1", name="Research")

        assert computer["computer_id"] == "comp-1"
        assert mock_create.await_args.kwargs["is_primary"] is True

    @pytest.mark.asyncio
    @patch("src.server.services.computer_manager._machines.update_computer_status")
    @patch("src.server.services.computer_manager._machines.create_computer")
    @patch("src.server.services.computer_manager._machines.get_primary_computer")
    async def test_losing_the_primary_race_tombstones_the_loser(
        self, mock_primary, mock_create, mock_status
    ):
        """``is_primary`` is a request the insert only takes while the user has
        none, so the loser comes back non-primary. Leaving it would put a
        second machine on the account that nothing ever starts."""
        manager = _make_manager()
        winner = _make_computer(computer_id="comp-winner", status="stopped")
        mock_primary.side_effect = [None, winner]
        mock_create.return_value = _make_computer(
            computer_id="comp-loser", is_primary=False, status="stopped"
        )

        computer = await manager.ensure_primary_computer("user-1")

        assert computer["computer_id"] == "comp-winner"
        mock_status.assert_awaited_once_with("comp-loser", "deleted")

    @pytest.mark.asyncio
    @patch("src.server.services.computer_manager._machines.update_computer_status")
    @patch("src.server.services.computer_manager._machines.create_computer")
    @patch("src.server.services.computer_manager._machines.get_primary_computer")
    async def test_a_winner_that_vanished_is_reported_not_substituted(
        self, mock_primary, mock_create, mock_status
    ):
        """A delete racing the create. The row just tombstoned is not usable,
        so handing it back would name a machine nothing points at."""
        manager = _make_manager()
        mock_primary.side_effect = [None, None]
        mock_create.return_value = _make_computer(is_primary=False, status="stopped")

        with pytest.raises(RuntimeError, match="no primary computer"):
            await manager.ensure_primary_computer("user-1")


# ---------------------------------------------------------------------------
# Machine keying
# ---------------------------------------------------------------------------


class TestMachineKeying(_Base):
    """Every process cache is keyed by machine, and the project index maps onto it."""

    @staticmethod
    def _on_one_computer():
        """Both projects resolve to the same machine row."""
        return patch(
            f"{_SESSIONS}.get_computer_for_workspace",
            AsyncMock(
                return_value=_make_computer(
                    status="running", provider_ref="sandbox-abc"
                )
            ),
        )

    @pytest.mark.asyncio
    async def test_two_projects_on_one_computer_share_the_session(self):
        manager = _make_manager()
        session = _make_session()

        with self._on_one_computer():
            first = await manager.resolve_binding("ws-a")
            manager._put_session(first.computer_id, session, workspace_id="ws-a")
            second = await manager.resolve_binding("ws-b")

        assert manager._cached_session(second.computer_id) is session
        assert [
            cid for cid, m in manager._machines.items() if m.session is not None
        ] == ["comp-1"]

    @pytest.mark.asyncio
    async def test_one_lock_serves_the_whole_machine(self):
        """Two projects on one sandbox must not restart it concurrently."""
        manager = _make_manager()

        with self._on_one_computer():
            first = await manager.resolve_binding("ws-a")
            second = await manager.resolve_binding("ws-b")

        assert manager._machine_lock(first.computer_id) is manager._machine_lock(
            second.computer_id
        )

    def test_separate_machines_get_separate_locks(self):
        manager = _make_manager()

        assert manager._machine_lock("comp-1") is not manager._machine_lock("comp-2")

    @cm_patch("SessionManager")
    def test_the_session_handle_carries_both_ids(self, mock_session_mgr):
        """The machine key names the sandbox; the workspace rides along as the
        label, which is what names the project that built the handle in logs."""
        manager = _make_manager()
        core_config = MagicMock()

        manager._session_handle(
            _make_binding("ws-a", resource_tier="performance"), core_config
        )

        mock_session_mgr.get_session.assert_called_once_with(
            "comp-1",
            core_config,
            label="ws-a",
            computer_id="comp-1",
            resource_tier="performance",
        )

    def test_dropping_a_session_drops_its_bookkeeping(self):
        manager = _make_manager()
        manager._put_session("comp-1", _make_session(), workspace_id="ws-a")

        manager._drop_session("comp-1")

        assert manager._cached_session("comp-1") is None
        assert manager._machine("comp-1").meta is None
        assert manager._session_computer == {}

    def test_session_meta_names_the_machine_and_the_project(self):
        manager = _make_manager()
        manager._put_session("comp-1", _make_session(), workspace_id="ws-a")

        (meta,) = [m.meta for m in manager._machines.values() if m.meta is not None]
        assert isinstance(meta, SessionMetadata)
        assert (meta.workspace_id, meta.computer_id) == ("ws-a", "comp-1")
        assert meta.sandbox_id == "sandbox-abc"
        assert meta.request_count == 1

    def test_live_session_stats_report_one_row_per_session(self):
        manager = _make_manager()
        manager._put_session("comp-1", _make_session(), workspace_id="ws-a")

        (row,) = manager.live_session_stats()
        assert row["workspace_id"] == "ws-a"
        assert row["computer_id"] == "comp-1"
        assert row["sandbox_id"] == "sandbox-abc"

    def test_a_warm_hit_is_counted(self):
        manager = _make_manager()
        manager._put_session("comp-1", _make_session(), workspace_id="ws-a")

        manager._touch_session_meta("comp-1")

        assert manager._machine("comp-1").meta.request_count == 2


# ---------------------------------------------------------------------------
# Resolving the binding
# ---------------------------------------------------------------------------


class TestResolveBinding(_Base):
    @pytest.mark.asyncio
    @patch(f"{_SESSIONS}.get_computer_for_workspace")
    async def test_resolves_from_the_computer_row(self, mock_get):
        manager = _make_manager()
        mock_get.return_value = _make_computer(
            status="running", provider_ref="sandbox-abc"
        )

        binding = await manager.resolve_binding("ws-a")

        assert binding.computer_id == "comp-1"
        assert binding.kind == "daytona"
        assert binding.root_dir == "/home/workspace"
        assert binding.provider_ref == "sandbox-abc"

    @pytest.mark.asyncio
    @patch(f"{_MACHINES}.bind_workspace_to_computer")
    @patch(f"{_MACHINES}.create_computer")
    @patch(f"{_MACHINES}.get_computer_by_provider_ref")
    @patch(f"{_SESSIONS}.get_computer_for_workspace")
    async def test_a_row_in_hand_is_adopted_without_a_second_read(
        self, mock_get, mock_by_ref, mock_create, mock_bind
    ):
        """Migration 046 leaves a project unbound, and the resolve is the one
        place that gives it a machine. The caller's row is handed down, so an
        unbound one does not cost a second query on every teardown path."""
        manager = _make_manager()
        mock_get.return_value = None
        mock_by_ref.return_value = None
        mock_create.return_value = _make_computer(status="stopped")
        mock_bind.return_value = _make_workspace()
        row = {
            "workspace_id": "ws-a",
            "user_id": "user-1",
            "computer_id": None,
            "status": "stopped",
            "name": "Research",
            "sandbox_id": None,
            "dir_name": "research-ab12",
        }

        with cm_patch("db_get_workspace", AsyncMock()) as read:
            binding = await manager.resolve_binding("ws-a", workspace=row)

        read.assert_not_awaited()
        assert binding.computer_id == "comp-1"

    @pytest.mark.asyncio
    @patch(f"{_MACHINES}.get_workspace_dir_names_for_computer", AsyncMock(return_value=()))
    @patch(f"{_MACHINES}.publish_workspace_binding_change")
    @patch(f"{_MACHINES}.bind_workspace_to_computer")
    @patch(f"{_MACHINES}.get_computer_by_provider_ref")
    async def test_adoption_speaks_the_insert_contract(
        self, mock_by_ref, mock_bind, mock_binding_change
    ):
        """The one caller that hands create_computer a sandbox to adopt. A mock
        with the real signature is what turns a stray keyword into a failure
        here rather than on the first legacy project's first request."""
        from src.server.database import computer as computer_db

        manager = _make_manager()
        mock_by_ref.return_value = None
        mock_bind.return_value = _make_workspace(dir_name="research-ab12")
        minted = AsyncMock(
            spec=computer_db.create_computer,
            return_value=_make_computer(status="running", provider_ref="sbx-legacy"),
        )
        row = {
            "workspace_id": "ws-a",
            "user_id": "user-1",
            "computer_id": None,
            "status": "running",
            "name": "Research",
            "sandbox_id": "sbx-legacy",
            "resource_tier": "standard",
            "is_always_on": True,
            "platform_secret_version": 3,
        }

        with patch(f"{_MACHINES}.create_computer", minted):
            adopted = await manager._adopt_workspace_onto_computer(
                "ws-a", workspace=row
            )

        minted.assert_awaited_once()
        kwargs = minted.await_args.kwargs
        assert kwargs["provider_ref"] == "sbx-legacy"
        assert kwargs["origin_workspace_id"] == "ws-a"
        assert kwargs["is_always_on"] is True
        assert kwargs["platform_secret_version"] == 3
        assert kwargs["is_primary"] is True
        assert adopted["dir_name"] == "research-ab12"
        mock_binding_change.assert_awaited_once_with("ws-a", "running", "comp-1")

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "sandbox_id, folder",
        [
            # Its files are still at the root, where "Research" may be a folder
            # of its own that the move to the folder layout would then skip.
            ("sbx-legacy", "Research-" + hashlib.md5(b"ws-a").hexdigest()[:4]),
            # A machine with no sandbox yet starts on the folder layout.
            (None, "Research"),
        ],
    )
    @patch(f"{_MACHINES}.get_workspace_dir_names_for_computer", AsyncMock(return_value=()))
    @patch(f"{_MACHINES}.publish_workspace_binding_change", AsyncMock())
    @patch(f"{_MACHINES}.bind_workspace_to_computer")
    @patch(f"{_MACHINES}.create_computer")
    @patch(f"{_MACHINES}.get_computer_by_provider_ref")
    async def test_an_adopted_legacy_sandbox_places_its_workspace_until_the_layout_moves(
        self, mock_by_ref, mock_create, mock_bind, sandbox_id, folder
    ):
        manager = _make_manager()
        mock_by_ref.return_value = None
        mock_create.return_value = _make_computer(status="running", provider_ref=sandbox_id)
        mock_bind.return_value = _make_workspace(dir_name=folder)
        row = {
            "workspace_id": "ws-a",
            "user_id": "user-1",
            "computer_id": None,
            "status": "running",
            "name": "Research",
            "sandbox_id": sandbox_id,
        }

        await manager._adopt_workspace_onto_computer("ws-a", workspace=row)

        assert mock_bind.await_args.kwargs["dir_name"] == folder

    @pytest.mark.asyncio
    @patch(f"{_MACHINES}.get_workspace_dir_names_for_computer", AsyncMock(return_value=()))
    @patch(f"{_MACHINES}.bind_workspace_to_computer")
    @patch(f"{_MACHINES}.create_computer")
    @patch(f"{_MACHINES}.get_computer_by_provider_ref")
    async def test_another_users_sandbox_is_never_adopted(
        self, mock_by_ref, mock_create, mock_bind
    ):
        """A sandbox is one user's files and secrets; a row naming someone
        else's gets a machine of its own instead of a folder on theirs."""
        manager = _make_manager()
        mock_by_ref.return_value = _make_computer(
            computer_id="comp-theirs", user_id="user-2", provider_ref="sbx-shared"
        )
        mock_create.return_value = _make_computer(computer_id="comp-mine")
        mock_bind.return_value = _make_workspace(dir_name="research-ab12")
        row = {
            "workspace_id": "ws-a",
            "user_id": "user-1",
            "computer_id": None,
            "status": "running",
            "name": "Research",
            "sandbox_id": "sbx-shared",
        }

        adopted = await manager._adopt_workspace_onto_computer("ws-a", workspace=row)

        assert adopted["computer_id"] == "comp-mine"
        kwargs = mock_create.await_args.kwargs
        assert kwargs["provider_ref"] is None
        assert kwargs["origin_workspace_id"] is None
        assert kwargs["status"] == "stopped"
        assert kwargs["is_primary"] is True
        mock_bind.assert_awaited_once()
        assert mock_bind.await_args.args[1] == "comp-mine"

    @pytest.mark.asyncio
    @patch(f"{_MACHINES}.get_workspace_dir_names_for_computer", AsyncMock(return_value=()))
    @patch(f"{_MACHINES}.update_computer_status")
    @patch(f"{_MACHINES}.get_computer_for_workspace")
    @patch(f"{_MACHINES}.bind_workspace_to_computer")
    @patch(f"{_MACHINES}.create_computer")
    @patch(f"{_MACHINES}.get_computer_by_provider_ref")
    async def test_a_machine_minted_for_a_lost_bind_is_tombstoned(
        self, mock_by_ref, mock_create, mock_bind, mock_get, mock_status
    ):
        """Two first touches of one unbound project mint two rows; the loser's
        would otherwise sit in the computer list, startable and billable."""
        manager = _make_manager()
        mock_by_ref.return_value = None
        mock_create.return_value = _make_computer(computer_id="comp-loser")
        mock_bind.return_value = None
        mock_get.return_value = _make_computer(computer_id="comp-winner")
        row = {
            "workspace_id": "ws-a",
            "user_id": "user-1",
            "computer_id": None,
            "status": "stopped",
            "name": "Research",
            "sandbox_id": None,
        }

        adopted = await manager._adopt_workspace_onto_computer("ws-a", workspace=row)

        assert adopted["computer_id"] == "comp-winner"
        mock_status.assert_awaited_once_with("comp-loser", "deleted")

    @pytest.mark.asyncio
    @patch(f"{_MACHINES}.get_workspace_dir_names_for_computer", AsyncMock(return_value=()))
    @patch(f"{_MACHINES}.update_computer_status")
    @patch(f"{_MACHINES}.get_computer_for_workspace")
    @patch(f"{_MACHINES}.bind_workspace_to_computer")
    @patch(f"{_MACHINES}.create_computer")
    @patch(f"{_MACHINES}.get_computer_by_provider_ref")
    async def test_concurrent_provider_adopter_never_tombstones_the_winner(
        self, mock_by_ref, mock_create, mock_bind, mock_get, mock_status
    ):
        """A uniqueness-race loser receives the winner from create_computer.

        Both attempts have the same origin workspace, so only the caller's
        candidate id can distinguish who actually inserted the shared row.
        """
        manager = _make_manager()
        winner = _make_computer(
            computer_id="comp-winner",
            provider_ref="sbx-legacy",
            origin_workspace_id="ws-a",
        )
        mock_by_ref.return_value = None
        mock_create.return_value = winner
        mock_bind.return_value = None
        mock_get.return_value = winner
        row = {
            "workspace_id": "ws-a",
            "user_id": "user-1",
            "computer_id": None,
            "status": "running",
            "name": "Research",
            "sandbox_id": "sbx-legacy",
        }

        adopted = await manager._adopt_workspace_onto_computer("ws-a", workspace=row)

        assert adopted["computer_id"] == "comp-winner"
        assert mock_create.await_args.kwargs["computer_id"] != "comp-winner"
        mock_status.assert_not_awaited()

    @pytest.mark.asyncio
    @patch(f"{_SESSIONS}.get_computer_for_workspace")
    async def test_a_project_no_machine_will_take_is_refused(self, mock_get):
        """Nothing below the resolve has a second addressing mode, so a project
        the adoption cannot place raises here rather than further down."""
        manager = _make_manager()
        mock_get.return_value = None

        with patch.object(
            manager, "_adopt_workspace_onto_computer", AsyncMock(return_value=None)
        ):
            with pytest.raises(WorkspaceNotOnComputer):
                await manager.resolve_binding("ws-a")

    def test_a_live_session_settles_the_index_without_a_read(self):
        """The two synchronous route-facing accessors need a project-to-machine
        answer with no round trip, so installing the session is what records it."""
        manager = _make_manager()
        manager._put_session("comp-1", _make_session(), workspace_id="ws-a")

        assert manager._live_session_computer("ws-a") == "comp-1"


# ---------------------------------------------------------------------------
# The split-binding window
# ---------------------------------------------------------------------------


class TestSplitBinding(_Base):
    """A bind whose computer half committed and whose workspace shadow wrote no
    rows leaves the two tables naming different sandboxes, one of them already
    deleted. Neither id is trustworthy, so the handle is stale."""

    def test_disagreeing_rows_are_stale(self):
        manager = _make_manager()
        reason = manager._computer_identity_is_stale(
            _make_binding("ws-a"),
            {
                "computer_id": "comp-1",
                "computer_status": "running",
                "provider_ref": "sandbox-new",
                "sandbox_id": "sandbox-old",
            },
        )
        assert reason is not None
        assert "split" in reason
        assert "sandbox-new" in reason and "sandbox-old" in reason

    def test_agreeing_rows_are_not_stale(self):
        manager = _make_manager()
        assert (
            manager._computer_identity_is_stale(
                _make_binding("ws-a"),
                {
                    "computer_id": "comp-1",
                    "computer_status": "running",
                    "provider_ref": "sandbox-abc",
                    "sandbox_id": "sandbox-abc",
                },
            )
            is None
        )

    def test_a_computer_that_is_not_running_is_stale(self):
        manager = _make_manager()
        reason = manager._computer_identity_is_stale(
            _make_binding("ws-a"),
            {
                "computer_id": "comp-1",
                "computer_status": "stopped",
                "provider_ref": "sandbox-abc",
                "sandbox_id": "sandbox-abc",
            },
        )
        assert reason == "computer status is 'stopped'"

    def test_a_start_this_worker_owns_is_not_stale(self):
        """The record's ``pending_lazy_sync`` flag is the ownership token: the
        machine reads 'starting' because this request is starting it."""
        manager = _make_manager()
        manager._machine("comp-1").pending_lazy_sync = True

        assert (
            manager._computer_identity_is_stale(
                _make_binding("ws-a"),
                {
                    "computer_id": "comp-1",
                    "computer_status": "starting",
                    "provider_ref": "sandbox-abc",
                    "sandbox_id": "sandbox-abc",
                },
            )
            is None
        )

    def test_a_start_another_worker_owns_is_stale(self):
        manager = _make_manager()

        reason = manager._computer_identity_is_stale(
            _make_binding("ws-a"),
            {
                "computer_id": "comp-1",
                "computer_status": "starting",
                "provider_ref": "sandbox-abc",
                "sandbox_id": "sandbox-abc",
            },
        )
        assert reason == "computer status is 'starting'"

    def test_a_missing_computer_half_is_not_judged(self):
        """The identity read left-joins the machine, so a project whose computer
        row is gone arrives with nulls rather than with a disagreement."""
        manager = _make_manager()
        assert (
            manager._computer_identity_is_stale(
                _make_binding("ws-a"),
                {
                    "computer_id": None,
                    "computer_status": None,
                    "provider_ref": None,
                    "sandbox_id": "sandbox-abc",
                },
            )
            is None
        )


# ---------------------------------------------------------------------------
# The computer-scoped run gate
# ---------------------------------------------------------------------------


class TestActivityGate(_Base):
    """The sandbox belongs to the machine, so one idle project cannot decide."""

    @pytest.mark.asyncio
    @patch(f"{_LIFECYCLE}.LocalRunExecutor")
    async def test_the_gate_spans_every_project_on_the_machine(self, mock_executor_cls):
        manager = _make_manager()
        executor = MagicMock()
        executor.has_active_tasks_for_computer = AsyncMock(return_value=True)
        executor.has_active_tasks_for_workspace = AsyncMock(return_value=False)
        mock_executor_cls.get_instance.return_value = executor

        assert (
            await manager._machine_has_active_tasks("comp-1", workspace_id="ws-a")
            is True
        )
        executor.has_active_tasks_for_computer.assert_awaited_once_with(
            "comp-1", workspace_id="ws-a"
        )
        executor.has_active_tasks_for_workspace.assert_not_awaited()

    @pytest.mark.asyncio
    @patch(f"{_LIFECYCLE}.LocalRunExecutor")
    async def test_the_gate_answers_for_a_machine_no_project_named(
        self, mock_executor_cls
    ):
        """The idle sweep asks per machine, so the project is optional and the
        probe must not narrow to one when none is given."""
        manager = _make_manager()
        executor = MagicMock()
        executor.has_active_tasks_for_computer = AsyncMock(return_value=True)
        mock_executor_cls.get_instance.return_value = executor

        assert await manager._machine_has_active_tasks("comp-1") is True
        executor.has_active_tasks_for_computer.assert_awaited_once_with(
            "comp-1", workspace_id=None
        )


# ---------------------------------------------------------------------------
# Tombstoning
# ---------------------------------------------------------------------------


class TestTombstone(_Base):
    """The only place a machine ends for good.

    Two rules: nothing live left on it, and it is not the machine instant
    create hands the user's next project.
    """

    @staticmethod
    def _retiring_manager(workspace=None):
        manager = _make_manager()
        manager._teardown_machine = AsyncMock()
        manager._remove_workspace_folder = AsyncMock()
        manager._machine_decision_lock = _decision_lock(True)
        return manager, workspace or _make_workspace()

    @pytest.mark.asyncio
    @patch(f"{_MACHINES}.update_computer_status")
    @patch(f"{_MACHINES}.get_computer")
    @patch(f"{_MACHINES}.get_live_workspace_ids_for_computer")
    async def test_the_last_project_leaving_retires_the_computer(
        self, mock_live, mock_get, mock_status
    ):
        manager, workspace = self._retiring_manager()
        mock_live.return_value = []
        computer = _make_computer(is_primary=False, status="running")
        mock_get.return_value = computer
        mock_status.side_effect = [
            {**computer, "status": "stopping"},
            {**computer, "status": "deleted"},
        ]

        retired = await manager._retire_machine_if_empty("comp-1", "ws-a", workspace)

        assert retired is True
        (binding,) = manager._teardown_machine.await_args.args
        assert (binding.workspace_id, binding.computer_id) == ("ws-a", "comp-1")
        assert [item.args for item in mock_status.await_args_list] == [
            ("comp-1", "stopping"),
            ("comp-1", "deleted"),
        ]
        assert [item.kwargs for item in mock_status.await_args_list] == [
            {"expected": "running"},
            {"expected": "stopping"},
        ]
        manager._remove_workspace_folder.assert_not_awaited()

    @pytest.mark.asyncio
    @patch(f"{_MACHINES}.update_computer_status")
    @patch(f"{_MACHINES}.get_computer")
    @patch(f"{_MACHINES}.get_live_workspace_ids_for_computer")
    async def test_a_lost_retirement_claim_never_touches_the_provider(
        self, mock_live, mock_get, mock_status
    ):
        manager, workspace = self._retiring_manager()
        mock_live.return_value = []
        mock_get.return_value = _make_computer(is_primary=False, status="running")
        mock_status.return_value = None

        retired = await manager._retire_machine_if_empty("comp-1", "ws-a", workspace)

        assert retired is False
        manager._teardown_machine.assert_not_awaited()
        mock_status.assert_awaited_once_with(
            "comp-1", "stopping", expected="running"
        )

    @pytest.mark.asyncio
    @patch(f"{_MACHINES}.update_computer_status")
    @patch(f"{_MACHINES}.get_computer")
    @patch(f"{_MACHINES}.get_live_workspace_ids_for_computer")
    async def test_a_computer_with_a_live_project_is_left_alone(
        self, mock_live, mock_get, mock_status
    ):
        """The sibling may be mid-turn on that sandbox, so only the folder goes."""
        manager, workspace = self._retiring_manager()
        mock_live.return_value = ["ws-b"]
        computer = _make_computer(is_primary=False, status="running")
        mock_get.return_value = computer

        retired = await manager._retire_machine_if_empty("comp-1", "ws-a", workspace)

        assert retired is False
        manager._teardown_machine.assert_not_awaited()
        mock_status.assert_not_awaited()
        manager._remove_workspace_folder.assert_awaited_once_with(
            "ws-a", workspace, computer
        )
        self.clear_retirement_pending.assert_awaited_once_with("comp-1")

    @pytest.mark.asyncio
    @patch(f"{_MACHINES}.update_computer_status")
    @patch(f"{_MACHINES}.get_computer")
    @patch(f"{_MACHINES}.get_live_workspace_ids_for_computer")
    async def test_an_empty_primary_is_kept(self, mock_live, mock_get, mock_status):
        """Instant create hands the next project this machine, so it survives
        empty rather than costing the user a fresh sandbox and its disk."""
        manager, workspace = self._retiring_manager()
        mock_live.return_value = []
        mock_get.return_value = _make_computer(is_primary=True, status="running")

        retired = await manager._retire_machine_if_empty("comp-1", "ws-a", workspace)

        assert retired is False
        manager._teardown_machine.assert_not_awaited()
        mock_status.assert_not_awaited()
        self.clear_retirement_pending.assert_awaited_once_with("comp-1")

    @pytest.mark.asyncio
    @patch(f"{_MACHINES}.update_computer_status")
    @patch(f"{_MACHINES}.get_computer")
    @patch(f"{_MACHINES}.get_live_workspace_ids_for_computer")
    async def test_a_machine_already_retired_is_not_retired_twice(
        self, mock_live, mock_get, mock_status
    ):
        """Two deletes racing the last two projects: the loser of the key finds
        a tombstoned row and stops, rather than deleting a sandbox again."""
        manager, workspace = self._retiring_manager()
        mock_get.return_value = None

        retired = await manager._retire_machine_if_empty("comp-1", "ws-a", workspace)

        assert retired is False
        mock_live.assert_not_awaited()
        manager._teardown_machine.assert_not_awaited()
        mock_status.assert_not_awaited()

    @pytest.mark.asyncio
    @patch(f"{_MACHINES}.update_computer_status")
    @patch(f"{_MACHINES}.get_computer")
    @patch(f"{_MACHINES}.get_live_workspace_ids_for_computer")
    async def test_emptiness_is_not_decided_without_the_key(
        self, mock_live, mock_get, mock_status
    ):
        """Without the lock the read could race a sibling's tombstone, so the
        machine is left alone and only the folder goes."""
        manager, workspace = self._retiring_manager()
        manager._machine_decision_lock = _decision_lock(False)
        mock_get.return_value = _make_computer(is_primary=False, status="running")

        retired = await manager._retire_machine_if_empty("comp-1", "ws-a", workspace)

        assert retired is False
        mock_live.assert_not_awaited()
        mock_status.assert_not_awaited()
        manager._remove_workspace_folder.assert_awaited_once()

    @pytest.mark.asyncio
    @patch(f"{_MACHINES}.update_computer_status")
    @patch(f"{_MACHINES}.get_computer")
    @patch(f"{_MACHINES}.get_live_workspace_ids_for_computer")
    @patch(f"{_MACHINES}.get_retirement_pending_computers")
    async def test_cleanup_retries_a_retirement_that_lost_its_delete_lock(
        self, mock_pending, mock_live, mock_get, mock_status
    ):
        manager, _ = self._retiring_manager()
        computer = _make_computer(is_primary=False, status="running")
        mock_pending.return_value = [computer]
        mock_get.return_value = computer
        mock_live.return_value = []
        mock_status.side_effect = [
            {**computer, "status": "stopping"},
            {**computer, "status": "deleted"},
        ]

        assert await manager.reap_retirement_pending_computers() == 1

        manager._teardown_machine.assert_awaited_once()
        assert [item.kwargs for item in mock_status.await_args_list] == [
            {"expected": "running"},
            {"expected": "stopping"},
        ]

    @pytest.mark.asyncio
    @patch(f"{_MACHINES}.mark_computer_retirement_pending")
    @patch(f"{_MACHINES}.get_retirement_pending_computers")
    async def test_cleanup_defers_contended_candidates_behind_newer_work(
        self, mock_pending, mock_defer
    ):
        manager, _ = self._retiring_manager()
        manager._machine_decision_lock = _decision_lock(False)
        mock_pending.return_value = [
            _make_computer(computer_id="comp-1", is_primary=False),
            _make_computer(computer_id="comp-2", is_primary=False),
        ]

        assert await manager.reap_retirement_pending_computers() == 0

        assert [item.args[0] for item in mock_defer.await_args_list] == [
            "comp-1",
            "comp-2",
        ]

    @pytest.mark.asyncio
    @patch(f"{_MACHINES}.update_computer_status")
    @patch(f"{_MACHINES}.get_computer")
    @patch(f"{_MACHINES}.get_live_workspace_ids_for_computer")
    @patch(f"{_MACHINES}.get_retirement_pending_computers")
    async def test_cleanup_clears_a_timed_out_marker_from_a_shared_machine(
        self, mock_pending, mock_live, mock_get, mock_status
    ):
        manager, _ = self._retiring_manager()
        computer = _make_computer(
            is_primary=False,
            status="running",
            config={"retire_when_empty": True},
        )
        mock_pending.return_value = [computer]
        mock_get.return_value = computer
        mock_live.return_value = ["ws-sibling"]

        assert await manager.reap_retirement_pending_computers() == 0

        self.clear_retirement_pending.assert_awaited_once_with("comp-1")
        manager._teardown_machine.assert_not_awaited()
        mock_status.assert_not_awaited()


# ---------------------------------------------------------------------------
# Taking a project off a machine that keeps running
# ---------------------------------------------------------------------------


class TestRemovingAProjectFolder(_Base):
    """``rm -rf`` of one folder, addressed through the machine's durable ref.

    A folder still at the top level is first moved under ``_internal`` (the
    settle's script, under the folder lock), so the name is free before the
    slow removal and a workspace reusing it never shares the removed path.
    """

    _AWAY = "_internal/leftovers/ws-a"

    def _manager(self, *, exec_result=None, moved=_AWAY):
        manager = _make_manager()
        runtime = MagicMock()
        runtime.exec = AsyncMock(
            return_value=exec_result or SimpleNamespace(exit_code=0, stdout="")
        )
        manager._detached_runtime = _detached(runtime)
        manager._clear_tombstone_folder = AsyncMock(return_value=moved)
        return manager, runtime

    @pytest.mark.asyncio
    async def test_a_running_machine_loses_the_folder_after_moving_it_aside(self):
        manager, runtime = self._manager()

        removed = await manager._remove_workspace_folder(
            "ws-a",
            _make_workspace(dir_name="alpha"),
            _make_computer(status="running", provider_ref="sandbox-abc"),
        )

        assert removed is True
        manager._clear_tombstone_folder.assert_awaited_once()
        runtime.exec.assert_awaited_once_with(f"rm -rf /home/workspace/{self._AWAY}")
        self.complete_folder_cleanup.assert_awaited_once_with(
            "ws-a", computer_id="comp-1", dir_name=self._AWAY
        )

    @pytest.mark.asyncio
    async def test_a_folder_that_was_never_on_the_disk_just_hands_the_name_back(self):
        manager, runtime = self._manager(moved=None)

        assert await manager._remove_workspace_folder(
            "ws-a",
            _make_workspace(dir_name="alpha"),
            _make_computer(status="running", provider_ref="sandbox-abc"),
        )

        runtime.exec.assert_not_awaited()
        self.complete_folder_cleanup.assert_awaited_once_with(
            "ws-a", computer_id="comp-1", dir_name="alpha"
        )

    @pytest.mark.asyncio
    async def test_a_folder_already_moved_aside_is_removed_where_it_is(self):
        """A settle cleared it earlier; only the removal is left."""
        manager, runtime = self._manager()

        await manager._remove_workspace_folder(
            "ws-a",
            _make_workspace(dir_name=self._AWAY),
            _make_computer(status="running", provider_ref="sandbox-abc"),
        )

        manager._clear_tombstone_folder.assert_not_awaited()
        runtime.exec.assert_awaited_once_with(f"rm -rf /home/workspace/{self._AWAY}")

    @pytest.mark.asyncio
    async def test_no_other_nested_folder_is_ever_removed(self):
        """A row pointing inside _internal anywhere but its own leftovers is
        mid-move, and its content is a live workspace's."""
        manager, runtime = self._manager()

        assert not await manager._remove_workspace_folder(
            "ws-a",
            _make_workspace(dir_name="_internal/moving/ws-a"),
            _make_computer(status="running", provider_ref="sandbox-abc"),
        )

        runtime.exec.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_failed_command_keeps_the_durable_retry_claim(self):
        manager, _runtime = self._manager(
            exec_result=SimpleNamespace(exit_code=13, stdout="permission denied")
        )

        removed = await manager._remove_workspace_folder(
            "ws-a",
            _make_workspace(dir_name="alpha"),
            _make_computer(status="running", provider_ref="sandbox-abc"),
        )

        assert removed is False
        self.complete_folder_cleanup.assert_not_awaited()
        self.defer_folder_cleanup.assert_awaited_once_with(
            "ws-a", computer_id="comp-1", dir_name=self._AWAY
        )

    @pytest.mark.asyncio
    async def test_a_stopped_machine_is_not_woken_for_it(self):
        """The folder is only bytes the mirror holds and no live row names it."""
        manager, runtime = self._manager()

        await manager._remove_workspace_folder(
            "ws-a",
            _make_workspace(dir_name="alpha"),
            _make_computer(status="stopped", provider_ref="sandbox-abc"),
        )

        runtime.exec.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_project_owning_the_root_unlinks_nothing(self):
        """An unsplit computer's project is the root, and the root is the
        machine's own runtime rather than one project's folder."""
        manager, runtime = self._manager()

        await manager._remove_workspace_folder(
            "ws-a",
            _make_workspace(dir_name=None),
            _make_computer(status="running", provider_ref="sandbox-abc"),
        )

        runtime.exec.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_provider_failure_does_not_fail_the_delete(self):
        manager, runtime = self._manager()
        runtime.exec.side_effect = RuntimeError("toolbox down")

        assert not await manager._remove_workspace_folder(
            "ws-a",
            _make_workspace(dir_name="alpha"),
            _make_computer(status="running", provider_ref="sandbox-abc"),
        )


# ---------------------------------------------------------------------------
# Tearing a machine down
# ---------------------------------------------------------------------------


class TestTearingDownAMachine(_Base):
    """What a teardown is allowed to destroy: the machine's own sandbox only.

    ``cleanup_session`` deletes whatever this worker is attached to, which need
    not be the machine's sandbox, so a teardown that trusts the local handle
    destroys a sandbox some other live machine is serving. The machine's
    published ref is the only authorization, and the project's shadow column is
    not consulted at all.
    """

    @staticmethod
    def _tearing_manager(*, provider_ref, session_sandbox_id, computer_id="comp-1"):
        manager = _make_manager()
        manager._retire_session = AsyncMock()
        manager._detached_sandbox_teardown = AsyncMock()
        if session_sandbox_id is not None:
            session = _make_session()
            session.sandbox.sandbox_id = session_sandbox_id
            manager._machine(computer_id).session = session
        return manager

    @pytest.mark.asyncio
    @patch(f"{_PROVISIONING}.SessionManager.cleanup_session")
    async def test_the_machines_own_handle_is_cleaned_up(self, mock_cleanup):
        manager = self._tearing_manager(
            provider_ref="sandbox-abc", session_sandbox_id="sandbox-abc"
        )

        await manager._teardown_machine(_make_binding(provider_ref="sandbox-abc"))

        mock_cleanup.assert_awaited_once_with("comp-1")
        manager._retire_session.assert_not_awaited()
        manager._detached_sandbox_teardown.assert_not_awaited()

    @pytest.mark.asyncio
    @patch(f"{_PROVISIONING}.SessionManager.cleanup_session")
    async def test_a_handle_on_another_sandbox_is_evicted_intact(self, mock_cleanup):
        """Cleaning it up would delete a sandbox this machine never named."""
        manager = self._tearing_manager(
            provider_ref="sandbox-own", session_sandbox_id="sandbox-someone-else"
        )

        await manager._teardown_machine(_make_binding(provider_ref="sandbox-own"))

        mock_cleanup.assert_not_awaited()
        manager._retire_session.assert_awaited_once()
        manager._detached_sandbox_teardown.assert_awaited_once_with(
            "sandbox-own", delete=True, binding=ANY
        )

    @pytest.mark.asyncio
    @patch(f"{_PROVISIONING}.SessionManager.cleanup_session")
    async def test_a_machine_with_no_sandbox_destroys_nothing(self, mock_cleanup):
        """The regression: a rebound project's shadow still named its old
        machine's live sandbox, and the session keyed to the new machine was
        attached to it, so the teardown deleted a sandbox six live projects
        were sharing."""
        manager = self._tearing_manager(
            provider_ref=None, session_sandbox_id="sandbox-of-the-old-machine"
        )

        await manager._teardown_machine(_make_binding(provider_ref=None))

        mock_cleanup.assert_not_awaited()
        manager._retire_session.assert_awaited_once()
        manager._detached_sandbox_teardown.assert_not_awaited()

    @pytest.mark.asyncio
    @patch(f"{_PROVISIONING}.SessionManager.cleanup_session")
    async def test_no_session_here_still_reaches_the_sandbox(self, mock_cleanup):
        """Another worker may hold it, so the delete goes through the provider."""
        manager = self._tearing_manager(
            provider_ref="sandbox-own", session_sandbox_id=None
        )

        await manager._teardown_machine(_make_binding(provider_ref="sandbox-own"))

        manager._retire_session.assert_not_awaited()
        manager._detached_sandbox_teardown.assert_awaited_once_with(
            "sandbox-own", delete=True, binding=ANY
        )


# ---------------------------------------------------------------------------
# Provider addressing
# ---------------------------------------------------------------------------


class TestProviderAddressing(_Base):
    """A computer row carries the backend it was created on, so one process can
    reach more than one."""

    def test_the_provider_is_built_for_the_machines_kind_and_root(self):
        manager = _make_manager()
        binding = ComputerBinding(
            workspace_id="ws-a",
            computer_id="comp-1",
            kind="daytona",
            root_dir="/home/other",
        )

        with (
            patch("ptc_agent.core.sandbox.providers.build_provider") as mock_build,
            patch("ptc_agent.core.sandbox.providers.create_provider") as mock_create,
        ):
            manager._provider_for(binding)

        mock_build.assert_called_once_with("daytona", ANY, working_dir="/home/other")
        mock_create.assert_not_called()

    def test_an_unbound_workspace_uses_the_deployment_provider(self):
        manager = _make_manager()

        with (
            patch("ptc_agent.core.sandbox.providers.build_provider") as mock_build,
            patch("ptc_agent.core.sandbox.providers.create_provider") as mock_create,
        ):
            manager._provider_for(None)

        mock_create.assert_called_once()
        mock_build.assert_not_called()

    def test_an_unknown_kind_is_a_hard_error(self):
        manager = _make_manager()
        binding = ComputerBinding(
            workspace_id="ws-a", computer_id="comp-1", kind="nowhere"
        )

        with pytest.raises(ValueError, match="Unknown sandbox provider"):
            manager._provider_for(binding)

    def test_a_different_root_gets_a_freshly_built_filesystem_config(self):
        """``model_copy`` does not re-run ``model_post_init``, so assigning
        ``working_directory`` would leave the derived directory lists pointing
        at the old root. Only a fresh config re-derives them."""
        manager = _make_manager()
        binding = ComputerBinding(
            workspace_id="ws-a",
            computer_id="comp-1",
            kind="daytona",
            root_dir="/home/other",
        )

        core_config = manager._core_config_for(binding)

        derived = FilesystemConfig(working_directory="/home/other")
        assert core_config.filesystem.working_directory == "/home/other"
        assert core_config.filesystem.allowed_directories == (
            derived.allowed_directories
        )
        assert core_config.filesystem.denied_directories == (derived.denied_directories)
        assert "/home/workspace" not in core_config.filesystem.allowed_directories

    @pytest.mark.parametrize("allowed,denied", [(["/mnt/data"], ["/secret"]), ([], [])])
    def test_root_change_preserves_explicit_policy(self, allowed, denied):
        manager = _make_manager()
        manager.config.to_core_config.return_value.filesystem = FilesystemConfig(
            working_directory="/old", allowed_directories=allowed,
            denied_directories=denied,
        )
        result = manager._core_config_for(_make_binding(root_dir="/new"))
        assert result.filesystem.allowed_directories == allowed
        assert result.filesystem.denied_directories == denied

    def test_the_kind_reaches_the_core_config(self):
        manager = WorkspaceManager.get_instance(config=_make_real_config())
        binding = ComputerBinding(
            workspace_id="ws-a", computer_id="comp-1", kind="docker"
        )

        assert manager._core_config_for(binding).sandbox.provider == "docker"

    def test_no_binding_leaves_the_core_config_untouched(self):
        manager = _make_manager()
        core_config = manager._core_config_for(None)
        assert core_config.sandbox.provider == "daytona"
        assert core_config.filesystem.working_directory == "/home/workspace"

    @pytest.mark.asyncio
    @patch(f"{_SESSIONS}.get_computer_for_workspace")
    async def test_the_reported_kind_comes_from_the_machine(self, mock_get):
        manager = _make_manager()
        mock_get.return_value = _make_computer(kind="docker")

        assert await manager.provider_kind_for_workspace("ws-a") == "docker"

    @pytest.mark.asyncio
    @patch(f"{_SESSIONS}.get_computer_for_workspace")
    async def test_a_project_on_no_machine_has_no_kind_to_report(self, mock_get):
        """There is no deployment-wide fallback left to answer with: a project
        the adoption cannot place has no backend to name."""
        manager = _make_manager()
        mock_get.return_value = None

        with patch.object(
            manager, "_adopt_workspace_onto_computer", AsyncMock(return_value=None)
        ):
            with pytest.raises(WorkspaceNotOnComputer):
                await manager.provider_kind_for_workspace("ws-a")


# ---------------------------------------------------------------------------
# The computer-addressed surface
# ---------------------------------------------------------------------------


class TestComputerSurface(_Base):
    """The contract the computer routes resolve by name."""

    def test_every_method_is_present_and_awaitable(self):
        import inspect

        for name in COMPUTER_SURFACE:
            method = getattr(ComputerManager, name, None)
            assert method is not None, f"{name} is missing from ComputerManager"
            assert inspect.iscoroutinefunction(method), f"{name} is not async"

    def test_the_facade_resolves_the_whole_surface(self):
        manager = _make_manager()
        for name in COMPUTER_SURFACE:
            assert callable(getattr(manager, name))

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "method,delegate,rereads",
        [
            ("start_computer", "_start_machine", True),
            ("stop_computer", "_stop_machine", False),
            ("archive_computer", "_archive_machine", False),
        ],
    )
    @patch(f"{_MACHINES}.get_computer")
    async def test_each_call_drives_the_machine_and_returns_the_row(
        self, mock_get_computer, method, delegate, rereads
    ):
        """No project is borrowed to carry the operation: the computer id the
        route was given is what the delegate is addressed by. Start hands back
        nothing, so it re-reads; the other two return the row their own
        transition settled on."""
        manager = _make_manager()
        row = _make_computer(status="running")
        setattr(manager, delegate, AsyncMock(return_value=row))
        mock_get_computer.return_value = row

        result = await getattr(manager, method)("comp-1")

        assert getattr(manager, delegate).await_args.args[0] == "comp-1"
        assert result is row
        assert mock_get_computer.await_count == (1 if rereads else 0)

    @pytest.mark.asyncio
    async def test_get_session_for_computer_returns_the_session_not_the_row(self):
        """The session route needs the handle itself, so this one does not
        re-read the computer."""
        manager = _make_manager()
        session = _make_session()
        manager._start_machine = AsyncMock(return_value=session)

        result = await manager.get_session_for_computer("comp-1", user_id="user-1")

        assert result is session
        manager._start_machine.assert_awaited_once_with(
            "comp-1", user_id="user-1", on_state_observed=None
        )

    @pytest.mark.asyncio
    async def test_a_start_another_worker_owns_is_not_a_session(self):
        """``_start_machine`` returns None when it loses the claim, and a route
        that handed that back as a session would fail in the agent instead."""
        manager = _make_manager()
        manager._start_machine = AsyncMock(return_value=None)

        with pytest.raises(RuntimeError, match="being started by another worker"):
            await manager.get_session_for_computer("comp-1")

    @pytest.mark.asyncio
    @patch(f"{_MACHINES}.get_computer")
    async def test_blocking_start_waits_for_the_other_worker(self, mock_get):
        manager = _make_manager(start_wait_poll_interval=0)
        manager._start_machine = AsyncMock(return_value=None)
        starting = _make_computer(status="starting")
        running = _make_computer(status="running", provider_ref="sandbox-abc")
        mock_get.side_effect = [starting, running]

        assert await manager.start_computer("comp-1") is running
        assert mock_get.await_count == 2

    @pytest.mark.asyncio
    @patch(f"{_MACHINES}.touch_computer_starting")
    async def test_long_start_renews_its_durable_claim(self, mock_touch):
        manager = _make_manager(reap_stuck_after=0.3)
        renewed = asyncio.Event()

        async def touch(_computer_id):
            renewed.set()
            return True

        mock_touch.side_effect = touch
        async with manager._computer_start_heartbeat("comp-1"):
            await asyncio.wait_for(renewed.wait(), timeout=1)

        mock_touch.assert_awaited_with("comp-1")


# ---------------------------------------------------------------------------
# A machine with no project on it
# ---------------------------------------------------------------------------


class TestBareMachine(_Base):
    """A computer created but not yet given a project still starts and stops.

    The computer surface used to resolve a project before doing anything, so a
    machine with none raised out of the background task: nothing was published,
    the row stayed where it was, and every subscriber waited out the stream's
    cap. These lock the machine-addressed path that answers instead.
    """

    @pytest.mark.asyncio
    @patch(f"{_MACHINES}.get_computer")
    async def test_start_addresses_the_machine_itself(self, mock_get_computer):
        manager = _make_manager()
        manager._start_machine = AsyncMock()
        manager._acquire_session = AsyncMock()
        row = _make_computer(status="running")
        mock_get_computer.return_value = row

        assert await manager.start_computer("comp-1") is row

        manager._start_machine.assert_awaited_once_with("comp-1", on_start_claimed=None)
        manager._acquire_session.assert_not_awaited()

    @pytest.mark.asyncio
    @patch(f"{_MACHINE_BACKUP}.get_live_workspace_ids_for_computer")
    @patch(f"{_LIFECYCLE}.LocalRunExecutor")
    @patch(f"{_MACHINES}.update_computer_status")
    @patch(f"{_MACHINES}.get_computer")
    @patch(f"{_LIFECYCLE}.get_computer")
    async def test_a_project_that_arrived_late_is_still_mirrored(
        self,
        mock_lifecycle_get,
        mock_machines_get,
        mock_status,
        mock_executor,
        mock_live,
    ):
        """A stop can name no project at all, and the mirror is still per
        project: one created and attached meanwhile would otherwise lose its
        folder unmirrored."""
        manager = _make_manager()
        computer = _make_computer(status="running", provider_ref="sandbox-abc")
        mock_lifecycle_get.return_value = computer
        mock_machines_get.return_value = computer
        mock_status.return_value = {**computer, "status": "stopping"}
        mock_executor.get_instance.return_value = MagicMock(
            has_active_tasks_for_computer=AsyncMock(return_value=False)
        )
        mock_live.return_value = ["ws-late"]
        backup = AsyncMock()
        manager.backup_project_files = backup
        manager._detached_sandbox_teardown = AsyncMock()

        await manager._stop_machine("comp-1")

        assert [c.args[0] for c in backup.await_args_list] == ["ws-late"]
        assert backup.await_args_list[0].kwargs["expected_sandbox_id"] == "sandbox-abc"

    @pytest.mark.asyncio
    async def test_stop_addresses_the_machine_itself(self):
        manager = _make_manager()
        row = _make_computer(status="stopped")
        manager._stop_machine = AsyncMock(return_value=row)

        assert await manager.stop_computer("comp-1") is row

        manager._stop_machine.assert_awaited_once_with("comp-1")

    @pytest.mark.asyncio
    async def test_the_session_route_skips_the_per_folder_restore(self):
        manager = _make_manager()
        session = _make_session()
        manager._start_machine = AsyncMock(return_value=session)
        manager._ensure_project_attached = AsyncMock()

        result = await manager.get_session_for_computer("comp-1", user_id="user-1")

        assert result is session
        manager._start_machine.assert_awaited_once_with(
            "comp-1", user_id="user-1", on_state_observed=None
        )
        manager._ensure_project_attached.assert_not_awaited()

    @pytest.mark.asyncio
    @cm_patch("try_claim_computer_for_start")
    @patch(f"{_MACHINES}.get_computer")
    async def test_a_retirement_pending_machine_cannot_be_started(
        self, mock_get_computer, mock_claim
    ):
        manager = _make_manager()
        mock_get_computer.return_value = _make_computer(
            status="stopped", config={"retire_when_empty": True}
        )
        manager._build_machine_session = AsyncMock()

        with pytest.raises(RuntimeError, match="being retired"):
            await manager._start_machine("comp-1")

        mock_claim.assert_not_awaited()
        manager._build_machine_session.assert_not_awaited()

    @pytest.mark.asyncio
    @cm_patch("try_claim_computer_for_start")
    @patch(f"{_MACHINES}.get_computer")
    async def test_the_claimed_row_is_the_one_that_gets_built(
        self, mock_get_computer, mock_claim
    ):
        """Build from the claim's return, not the row read before it: the claim
        is the write that moved the status, so only its row carries 'starting'."""
        manager = _make_manager()
        mock_get_computer.return_value = _make_computer(status="stopped")
        claimed = _make_computer(status="starting")
        mock_claim.return_value = claimed
        session = _make_session()
        manager._build_machine_session = AsyncMock(return_value=session)

        assert await manager._start_machine("comp-1") is session

        mock_claim.assert_awaited_once_with("comp-1", from_status="stopped")
        assert manager._build_machine_session.await_args.args[0] is claimed

    @pytest.mark.asyncio
    @cm_patch("try_claim_computer_for_start")
    @patch(f"{_MACHINES}.get_computer")
    async def test_a_never_provisioned_machine_is_claimable_from_creating(
        self, mock_get_computer, mock_claim
    ):
        """046 backfilled a 'creating' row for a project that was never
        provisioned, so refusing that status would leave it unstartable."""
        manager = _make_manager()
        mock_get_computer.return_value = _make_computer(status="creating")
        mock_claim.return_value = _make_computer(status="starting")
        manager._build_machine_session = AsyncMock(return_value=_make_session())

        await manager._start_machine("comp-1")

        mock_claim.assert_awaited_once_with("comp-1", from_status="creating")

    @pytest.mark.asyncio
    @cm_patch("try_claim_computer_for_start")
    @patch(f"{_MACHINES}.get_computer")
    async def test_a_lost_claim_builds_nothing(self, mock_get_computer, mock_claim):
        manager = _make_manager()
        mock_get_computer.return_value = _make_computer(status="stopped")
        mock_claim.return_value = None
        manager._build_machine_session = AsyncMock()

        assert await manager._start_machine("comp-1") is None

        manager._build_machine_session.assert_not_awaited()

    @pytest.mark.asyncio
    @patch(f"{_MACHINES}.update_computer_status")
    @cm_patch("try_claim_computer_for_start")
    @patch(f"{_MACHINES}.get_computer")
    async def test_a_failed_start_hands_the_claim_back(
        self, mock_get_computer, mock_claim, mock_status
    ):
        """Compare-and-set on 'starting', so the next attempt is not blocked and
        a machine someone else already moved is left alone."""
        manager = _make_manager()
        mock_get_computer.return_value = _make_computer(status="stopped")
        mock_claim.return_value = _make_computer(status="starting")
        manager._build_machine_session = AsyncMock(side_effect=RuntimeError("boom"))

        with pytest.raises(RuntimeError, match="boom"):
            await manager._start_machine("comp-1")

        mock_status.assert_awaited_once_with("comp-1", "stopped", expected="starting")

    def test_a_handle_on_another_sandbox_is_not_reused(self):
        """The split-binding window: the row is authoritative about which
        sandbox is the machine's, so a cached handle on any other one is dead."""
        manager = _make_manager()
        session = _make_session()
        manager._machine("comp-1").session = session
        session.sandbox.has_failed.return_value = False

        computer = _make_computer(status="running", provider_ref="sandbox-abc")
        assert manager._machine_session_if_live(computer) is session

        stale = _make_computer(status="running", provider_ref="sandbox-xyz")
        assert manager._machine_session_if_live(stale) is None


# ---------------------------------------------------------------------------
# The workspace façade
# ---------------------------------------------------------------------------


class TestWorkspaceFacade(_Base):
    """``get_session_for_workspace`` is the name ~12 test modules and five
    metric descriptions carry. It stays, and it delegates."""

    @pytest.mark.asyncio
    async def test_it_acquires_then_attaches_this_project(self):
        manager = _make_manager()
        session = _make_session()
        manager._acquire_session = AsyncMock(return_value=session)
        manager.resolve_binding = AsyncMock(return_value=_make_binding("ws-a"))
        manager._ensure_project_attached = AsyncMock()

        result = await manager.get_session_for_workspace("ws-a", user_id="user-1")

        assert result is session
        manager._acquire_session.assert_awaited_once_with(
            "ws-a",
            user_id="user-1",
            on_state_observed=None,
            skills_signature=None,
            _attempt=0,
        )
        binding, got = manager._ensure_project_attached.await_args.args
        assert binding.workspace_id == "ws-a"
        assert got is session

    @staticmethod
    def _folder_reads(events, *folders):
        """Resolve to each folder in turn, as a settle between the reads would."""
        remaining = iter(folders)

        async def resolve(workspace_id, **_kw):
            folder = next(remaining)
            events.append(f"read {folder}")
            return _make_binding(workspace_id, dir_name=folder)

        return resolve

    @staticmethod
    def _hold(events, *, held_by_a_settle=False):
        @asynccontextmanager
        async def hold(workspace_id):
            events.append(f"hold {workspace_id}")
            if held_by_a_settle:
                raise WorkspaceFolderMoving(workspace_id)
            try:
                yield
            finally:
                events.append("release")

        return hold

    @pytest.mark.asyncio
    async def test_the_attachment_writes_to_the_folder_read_under_the_hold(self):
        """/start and a file route have no run a settle counts as busy, so only
        the hold keeps the folder where the restore and overlay write, and a
        settle elsewhere may have landed it since the read this one used."""
        manager = _make_manager()
        session = _make_session()
        events = []

        async def settle(_computer_id, _runtime, **_kw):
            events.append("settle")

        async def attach(binding, _session, *, user_id=None):
            events.append(f"attach {binding.dir_name}")

        manager._acquire_session = AsyncMock(return_value=session)
        manager.resolve_binding = self._folder_reads(events, "Research", "Macro")
        manager._settle_folders = settle
        manager._ensure_project_attached = attach
        with patch(f"{_WORKSPACE_MANAGER}.workspace_folder_in_use", self._hold(events)):
            assert await manager.get_session_for_workspace("ws-a") is session

        assert events == [
            "read Research", "settle", "hold ws-a", "read Macro", "attach Macro", "release",
        ]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("moving", ["staged", "held by a settle"])
    async def test_a_folder_a_settle_is_moving_is_refused_for_a_retry(self, moving):
        """Staged, the content may be in any of three folders; held past the wait,
        a settle is moving it now. Nothing attaches, and the caller gets the
        retryable refusal rather than a 500."""
        manager = _make_manager()
        events = []
        under_hold = "_internal/moving/ws-a" if moving == "staged" else "Macro"
        manager._acquire_session = AsyncMock(return_value=_make_session())
        manager.resolve_binding = self._folder_reads(events, "Research", under_hold)
        manager._ensure_project_attached = AsyncMock()
        hold = self._hold(events, held_by_a_settle=moving == "held by a settle")

        with (
            patch(f"{_WORKSPACE_MANAGER}.workspace_folder_in_use", hold),
            pytest.raises(SandboxTransientError, match="being moved"),
        ):
            await manager.get_session_for_workspace("ws-a")

        manager._ensure_project_attached.assert_not_awaited()
        if moving == "staged":
            assert events == ["read Research", "hold ws-a", f"read {under_hold}", "release"]
        else:
            assert events == ["read Research", "hold ws-a"]

    def test_the_facade_inherits_the_lifecycle(self):
        assert WorkspaceManager.__mro__[1] is ComputerManager


# ---------------------------------------------------------------------------
# Shutdown
# ---------------------------------------------------------------------------


class TestShutdown(_Base):
    @pytest.mark.asyncio
    @cm_patch("SessionManager")
    async def test_shutdown_clears_every_machine_keyed_cache(self, mock_session_mgr):
        """One record per machine, so one assertion covers every cache it held.

        The record carries the session, its metadata, every flag, the cooldown
        stamp and the machine lock, and shutdown drops the record whole."""
        mock_session_mgr.stop_all = AsyncMock()
        manager = _make_manager()
        manager._put_session("comp-1", _make_session(), workspace_id="ws-a")
        machine = manager._machine("comp-1")
        machine.pending_lazy_sync = True
        machine.resolve_superseded = True
        machine.last_sync_at = 1.0

        await manager.shutdown()

        assert manager._machines == {}
        assert manager._session_computer == {}


# ---------------------------------------------------------------------------
# kind authoritative (WP15)
# ---------------------------------------------------------------------------


def _make_real_config(provider="daytona", working_directory="/home/workspace"):
    """An AgentConfig stand-in whose provider settings are the real models.

    ``_provider_settings`` validates a row's overrides against the model's own
    fields, so a SimpleNamespace stand-in would not exercise it.
    """
    from ptc_agent.config.core import DaytonaConfig, DockerConfig

    config = MagicMock()
    config.sandbox = SimpleNamespace(provider=provider)
    config.filesystem = SimpleNamespace(working_directory=working_directory)

    def _core_config():
        return SimpleNamespace(
            sandbox=SimpleNamespace(
                provider=provider,
                daytona=DaytonaConfig(api_key="test-key"),
                docker=DockerConfig(),
                platform_secrets={},
            ),
            filesystem=FilesystemConfig(working_directory=working_directory),
        )

    config.to_core_config.side_effect = _core_config
    return config


class TestProviderPerRow(_Base):
    """``kind`` is authoritative: the row decides the backend, not the process."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("held", [True, False])
    @pytest.mark.parametrize("enabled, expected_minutes", [(True, 0), (False, 10)])
    async def test_always_on_restores_the_computers_own_interval(
        self, held, enabled, expected_minutes
    ):
        config = _make_real_config()
        config.sandbox.daytona = config.to_core_config().sandbox.daytona
        manager = WorkspaceManager.get_instance(config=config)
        binding = _make_binding(provider_config={"auto_stop_interval": 600})
        runtime = SimpleNamespace(
            capabilities={"autostop"}, set_autostop_interval=AsyncMock()
        )

        @asynccontextmanager
        async def detached(sandbox_id, *, binding):
            assert sandbox_id == "sandbox-abc"
            assert binding.provider_config == {"auto_stop_interval": 600}
            yield runtime

        manager._detached_runtime = detached
        await manager._apply_autostop_for_always_on(
            "sandbox-abc", enabled=enabled, runtime=runtime if held else None,
            binding=binding,
        )
        runtime.set_autostop_interval.assert_awaited_once_with(expected_minutes)

    def test_the_rows_provider_config_overrides_the_deployments_settings(self):
        manager = WorkspaceManager.get_instance(config=_make_real_config())
        binding = ComputerBinding(
            workspace_id="ws-a",
            computer_id="comp-1",
            kind="daytona",
            root_dir="/home/workspace",
            provider_config={"auto_stop_interval": 60},
        )

        with patch("ptc_agent.core.sandbox.providers.build_provider") as mock_build:
            manager._provider_for(binding)

        settings = mock_build.call_args.args[1]
        assert settings.auto_stop_interval == 60
        # The deployment's own settings are untouched by the row's override.
        assert manager.config.to_core_config().sandbox.daytona.auto_stop_interval != 60

    def test_two_machines_on_one_kind_get_their_own_settings_objects(self):
        """Providers take their config by reference, so two machines sharing one
        object is the aliasing this seam exists to prevent."""
        manager = WorkspaceManager.get_instance(config=_make_real_config())
        binding = ComputerBinding(
            workspace_id="ws-a", computer_id="comp-1", kind="daytona"
        )

        first = manager._provider_settings(binding.kind, binding.provider_config)
        second = manager._provider_settings(binding.kind, binding.provider_config)

        assert first is not second

    def test_an_unknown_setting_in_the_row_is_a_hard_error(self):
        manager = WorkspaceManager.get_instance(config=_make_real_config())

        with pytest.raises(ValueError, match="unknown settings: nonesuch"):
            manager._provider_settings("daytona", {"nonesuch": 1})

    def test_a_machine_on_another_kind_builds_that_kinds_provider(self):
        manager = WorkspaceManager.get_instance(config=_make_real_config())
        binding = ComputerBinding(
            workspace_id="ws-a", computer_id="comp-1", kind="docker"
        )

        with patch("ptc_agent.core.sandbox.providers.build_provider") as mock_build:
            manager._provider_for(binding)

        assert mock_build.call_args.args[0] == "docker"

    def test_the_error_classifier_is_one_provider_per_backend(self):
        manager = WorkspaceManager.get_instance(config=_make_real_config())
        daytona = ComputerBinding(
            workspace_id="ws-a", computer_id="comp-1", kind="daytona"
        )
        docker = ComputerBinding(
            workspace_id="ws-b", computer_id="comp-2", kind="docker"
        )

        with patch.object(manager, "_provider_for", side_effect=lambda b: MagicMock()):
            manager._is_sandbox_gone(RuntimeError("x"), daytona)
            manager._is_sandbox_gone(RuntimeError("x"), daytona)
            manager._is_sandbox_gone(RuntimeError("x"), docker)
            manager._is_sandbox_gone(RuntimeError("x"), None)

        assert set(manager._error_classifiers) == {
            ("daytona", "{}"),
            ("docker", "{}"),
            (None, "{}"),
        }


class TestSandboxConfigHash(_Base):
    """The hash decides whether a machine keeps the sandbox it has."""

    def test_a_row_that_agrees_with_the_deployment_hashes_as_it_always_did(self):
        """Inert for the current fleet: folding the machine in must not put
        every existing workspace on the migrate path."""
        manager = _make_manager()
        binding = ComputerBinding(
            workspace_id="ws-a",
            computer_id="comp-1",
            kind="daytona",
            root_dir="/home/workspace",
        )

        assert manager._compute_sandbox_config_hash(
            manager.config, binding
        ) == manager._compute_sandbox_config_hash(manager.config)

    def test_a_changed_kind_gets_a_new_hash(self):
        manager = _make_manager()
        base = ComputerBinding(
            workspace_id="ws-a",
            computer_id="comp-1",
            kind="daytona",
            root_dir="/home/workspace",
        )
        moved = ComputerBinding(
            workspace_id="ws-a",
            computer_id="comp-1",
            kind="docker",
            root_dir="/home/workspace",
        )

        assert manager._compute_sandbox_config_hash(
            manager.config, base
        ) != manager._compute_sandbox_config_hash(manager.config, moved)

    def test_a_changed_provider_config_gets_a_new_hash(self):
        manager = _make_manager()
        base = ComputerBinding(
            workspace_id="ws-a", computer_id="comp-1", kind="daytona"
        )
        tuned = ComputerBinding(
            workspace_id="ws-a",
            computer_id="comp-1",
            kind="daytona",
            provider_config={"auto_stop_interval": 60},
        )

        assert manager._compute_sandbox_config_hash(
            manager.config, base
        ) != manager._compute_sandbox_config_hash(manager.config, tuned)

    def test_the_stamp_records_the_machines_own_backend(self):
        manager = _make_manager()
        binding = ComputerBinding(
            workspace_id="ws-a",
            computer_id="comp-1",
            kind="docker",
            root_dir="/home/other",
        )

        stamp = manager._sandbox_config_stamp(binding)

        assert stamp["sandbox_provider"] == "docker"
        assert stamp["sandbox_working_dir"] == "/home/other"
        assert stamp["sandbox_config_hash"] == manager._compute_sandbox_config_hash(
            manager.config, binding
        )


# ---------------------------------------------------------------------------
# The first start of a machine that was never built
# ---------------------------------------------------------------------------


class TestTheFirstProvision(_Base):
    """Instant creation hands back a project whose machine has no sandbox, so
    the first start is where the machine is actually built.

    A NULL ``sandbox_id`` used to be a broken row and raised. It is now the
    ordinary state of a brand-new project, and the restart path has to answer
    it with a provision or nothing the user creates would ever run.
    """

    @pytest.mark.asyncio
    async def test_a_machine_with_no_sandbox_is_provisioned_not_refused(self):
        """The machine's published ref is what decides, and NULL is the
        ordinary state of a machine whose first start this is."""
        manager = _make_manager()
        workspace = {"workspace_id": "ws-a", "sandbox_id": None, "name": "Research"}
        session = _make_session()

        with patch.object(
            manager, "_provision_first_sandbox", AsyncMock(return_value=session)
        ) as provision:
            got = await manager._restart_workspace(
                _make_binding("ws-a", provider_ref=None), workspace, "user-1"
            )

        assert got is session
        provision.assert_awaited_once_with(workspace, "user-1")

    @pytest.mark.asyncio
    async def test_the_provision_runs_the_recovery_path(self):
        """Entitled tier, fresh sandbox, durable files restored: a duplicate's
        copy is waiting in the mirror, so building a bare sandbox would hand
        the user an empty workspace."""
        manager = _make_manager()
        workspace = {
            "workspace_id": "ws-a",
            "sandbox_id": None,
            "name": "Research",
            "dir_name": "research-ab12",
            "computer_id": "comp-1",
        }
        session = _make_session()

        with (
            patch.object(
                manager,
                "resolve_binding",
                AsyncMock(return_value=_make_binding("ws-a", provider_ref=None)),
            ),
            patch.object(
                manager, "_recover_sandbox", AsyncMock(return_value=session)
            ) as recover,
            patch.object(manager, "_seed_agent_md", AsyncMock()) as seed,
        ):
            assert (
                await manager._provision_first_sandbox(workspace, "user-1") is session
            )

        binding, user_id, _core_config = recover.await_args.args
        assert (binding.workspace_id, binding.computer_id) == ("ws-a", "comp-1")
        assert user_id == "user-1"
        seed.assert_awaited_once_with(session.sandbox, "Research", "research-ab12")


# ---------------------------------------------------------------------------
# Joining a machine that is already running
# ---------------------------------------------------------------------------


class TestJoiningARunningMachine(_Base):
    """A project created on a live machine names no sandbox of its own.

    That is not a split binding, and answering it as one rebuilds the machine
    out from under every sibling already working on it. The machine's ref is
    the answer, adopted onto the project rather than guessed at.
    """

    @staticmethod
    def _running_binding():
        return _make_binding("ws-new", provider_ref="sandbox-live")

    @staticmethod
    @contextmanager
    def _past_the_attach(manager):
        """Silence the cold-attach tail so only the adopt decision is exercised."""
        patches = (
            ("_apply_session_platform_secret", AsyncMock()),
            ("_apply_session_mcp", AsyncMock(return_value=None)),
            ("_sync_sandbox_assets", AsyncMock()),
            ("_reconcile_skills", AsyncMock()),
            ("_maybe_migrate_sandbox", AsyncMock(return_value=None)),
        )
        with ExitStack() as stack:
            for name, new in patches:
                stack.enter_context(patch.object(manager, name, new=new))
            yield

    @pytest.mark.asyncio
    async def test_an_unbound_project_adopts_the_machines_sandbox(self):
        manager = _make_manager()
        session = _make_session()
        session._initialized = False
        session.initialize = AsyncMock()
        # What ``initialize`` attaches to: the machine's own sandbox, so the
        # identity bind below is skipped rather than raced and lost.
        session.sandbox.sandbox_id = "sandbox-live"
        workspace = {
            "workspace_id": "ws-new",
            "sandbox_id": None,
            "status": "running",
            "computer_id": "comp-1",
        }

        with (
            patch.object(manager, "_session_handle", return_value=session),
            patch.object(manager, "_recover_sandbox", AsyncMock()) as recover,
            patch(
                f"{_LIFECYCLE}.adopt_computer_sandbox_into_workspaces",
                AsyncMock(return_value=["ws-new"]),
            ) as adopt,
            self._past_the_attach(manager),
        ):
            got, did_init = await manager._attach_running_session(
                self._running_binding(), workspace, "user-1", None, lambda _p: None
            )

        assert got is session
        assert did_init is True
        adopt.assert_awaited_once_with("comp-1")
        # The machine is NOT rebuilt, and the session attaches to its sandbox.
        recover.assert_not_awaited()
        assert session.initialize.await_args.kwargs["sandbox_id"] == "sandbox-live"

    @staticmethod
    @contextmanager
    def _rereading(workspace, *, computer_ref, workspace_ref):
        """The rows read again under the lock, the refs from one statement."""
        with (
            patch(
                f"{_LIFECYCLE}.db_get_workspace_identity",
                AsyncMock(
                    return_value={
                        "status": "running",
                        "sandbox_id": workspace_ref,
                        "provider_ref": computer_ref,
                        "computer_status": "running",
                    }
                ),
            ),
            patch(
                f"{_LIFECYCLE}.get_computer",
                AsyncMock(
                    return_value=_make_computer(
                        status="running", provider_ref=computer_ref
                    )
                ),
            ),
            patch(
                f"{_LIFECYCLE}.db_get_workspace",
                AsyncMock(return_value={**workspace, "sandbox_id": workspace_ref}),
            ),
        ):
            yield

    @pytest.mark.asyncio
    async def test_a_project_naming_another_sandbox_is_still_a_split(self):
        """Two non-null ids is the disagreement neither table can settle, and
        the losing bind's own sandbox is already deleted."""
        manager = _make_manager()
        recovered = _make_session()
        handle = _make_session()
        workspace = {
            "workspace_id": "ws-new",
            "sandbox_id": "sandbox-other",
            "status": "running",
            "computer_id": "comp-1",
        }

        with (
            patch.object(manager, "_session_handle", return_value=handle),
            patch.object(manager, "_clear_session", AsyncMock()) as clear,
            patch.object(
                manager, "_recover_sandbox", AsyncMock(return_value=recovered)
            ) as recover,
            patch(
                f"{_LIFECYCLE}.adopt_computer_sandbox_into_workspaces",
                AsyncMock(),
            ) as adopt,
            self._rereading(
                workspace, computer_ref="sandbox-live", workspace_ref="sandbox-other"
            ),
        ):
            got, did_init = await manager._attach_running_session(
                self._running_binding(), workspace, "user-1", None, lambda _p: None
            )

        assert got is recovered and did_init is True
        recover.assert_awaited_once()
        # The rebuild is fenced on the ref read now, not the one the binding
        # was resolved with.
        assert recover.await_args.args[0].provider_ref == "sandbox-live"
        clear.assert_awaited_once_with("comp-1", evict_session=handle)
        adopt.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_bind_landing_after_the_binding_was_resolved_is_not_a_split(self):
        """The binding is read before the machine lock and the row under it, so
        a recover's bind between the two reads shows the old ref beside the new
        one. Rebuilding on that made a sandbox the bind fence refused, and on
        the worker that recovered it tore down the sandbox just bound."""
        manager = _make_manager()
        session = _make_session()
        session.sandbox.sandbox_id = "sandbox-new"
        workspace = {
            "workspace_id": "ws-new",
            "sandbox_id": "sandbox-new",
            "status": "running",
            "computer_id": "comp-1",
        }

        with (
            patch.object(manager, "_session_handle", return_value=session),
            patch.object(manager, "_clear_session", AsyncMock()) as clear,
            patch.object(manager, "_recover_sandbox", AsyncMock()) as recover,
            self._rereading(
                workspace, computer_ref="sandbox-new", workspace_ref="sandbox-new"
            ),
        ):
            got, did_init = await manager._attach_running_session(
                _make_binding("ws-new", provider_ref="sandbox-old"),
                workspace,
                "user-1",
                None,
                lambda _p: None,
            )

        assert got is session and did_init is False
        recover.assert_not_awaited()
        clear.assert_not_awaited()

    def test_a_cached_handle_is_not_stale_merely_for_an_unbound_row(self):
        """The warm path retires on it once, when the row is repaired; reading
        it as a permanent split would retire every session forever."""
        manager = _make_manager()
        binding = _make_binding("ws-new", provider_ref="sandbox-live")
        assert (
            manager._computer_identity_is_stale(
                binding,
                {
                    "computer_id": "comp-1",
                    "computer_status": "running",
                    "provider_ref": "sandbox-live",
                    "sandbox_id": None,
                },
            )
            is None
        )
        assert (
            manager._computer_identity_is_stale(
                binding,
                {
                    "computer_id": "comp-1",
                    "computer_status": "running",
                    "provider_ref": "sandbox-live",
                    "sandbox_id": "sandbox-other",
                },
            )
            is not None
        )


# ---------------------------------------------------------------------------
# A warm session whose skills changed
# ---------------------------------------------------------------------------


class TestWarmSkillChange(_Base):
    """The asset upload fills only the computer's shared skills directory.

    A workspace reaches a shared skill through a link the reconciler lays, so a
    warm turn after a skill is added or switched back on must reconcile too, or
    the skill is listed that turn but cannot be read.
    """

    @staticmethod
    async def _warm_sync(manager, session, *, mcp_changed):
        session.sandbox.ensure_sandbox_ready = AsyncMock()
        session.skills_signature = "before"
        patches = (
            ("_apply_session_platform_secret", AsyncMock()),
            (
                "_apply_session_mcp",
                AsyncMock(return_value=MagicMock() if mcp_changed else None),
            ),
            ("_freeze_tool_view", MagicMock()),
            ("tool_view", MagicMock()),
            ("_servers_needing_discovery", MagicMock(return_value=[])),
            ("_kick_mcp_discovery", MagicMock()),
            ("_sync_sandbox_assets", AsyncMock()),
            ("_reconcile_skills", AsyncMock()),
        )
        with ExitStack() as stack:
            for name, new in patches:
                stack.enter_context(patch.object(manager, name, new=new))
            await manager._complete_phase2_sync(
                _make_binding("ws-a"),
                session,
                workspace_user_id="user-1",
                needs_sync=True,
                needs_deferred_sync=False,
                phase2_owner=True,
                phase2_event=None,
                mark=lambda _phase: None,
                skills_signature="after" if not mcp_changed else "before",
            )
            return manager._sync_sandbox_assets, manager._reconcile_skills

    @pytest.mark.asyncio
    async def test_changed_skills_upload_then_relink(self):
        manager = _make_manager()
        session = _make_session()

        upload, reconcile = await self._warm_sync(manager, session, mcp_changed=False)

        upload.assert_awaited_once()
        reconcile.assert_awaited_once()
        assert reconcile.await_args.kwargs["source"] == "warm_skills_changed"
        assert session.skills_signature == "after"

    @pytest.mark.asyncio
    async def test_an_mcp_only_change_does_not_reconcile_skills(self):
        manager = _make_manager()
        session = _make_session()

        upload, reconcile = await self._warm_sync(manager, session, mcp_changed=True)

        upload.assert_awaited_once()
        reconcile.assert_not_awaited()


# ---------------------------------------------------------------------------
# Folders on a shared machine
# ---------------------------------------------------------------------------


class TestEveryProjectsFolder(_Base):
    """Restore is per project even though the sandbox is per machine.

    A project joining a machine another one started gets that machine's live
    session straight back, and the restore that ran when the sandbox was built
    only ever looked at the starter's folder.
    """

    @pytest.mark.asyncio
    async def test_a_joining_project_gets_its_own_restore(self):
        manager = _make_manager()
        session = _make_session()

        with _attached(manager, session) as restore:
            await manager.get_session_for_workspace("ws-joiner")

        assert restore.await_count == 1
        binding, sandbox = restore.await_args.args
        assert binding.workspace_id == "ws-joiner"
        assert sandbox is session.sandbox
        assert binding.dir_name == "joiner-6c06"

    @pytest.mark.asyncio
    async def test_each_project_on_the_machine_is_checked(self):
        """The starter's marker says nothing about a sibling's folder."""
        manager = _make_manager()
        session = _make_session()

        with _attached(manager, session) as restore:
            await manager.get_session_for_workspace("ws-a")
            await manager.get_session_for_workspace("ws-b")

        assert [c.args[0].workspace_id for c in restore.await_args_list] == [
            "ws-a",
            "ws-b",
        ]

    @pytest.mark.asyncio
    async def test_a_folder_already_checked_is_not_read_again(self):
        """A warm acquisition detects files removed after the initial attach."""
        manager = _make_manager()
        session = _make_session()

        with _attached(manager, session) as restore:
            await manager.get_session_for_workspace("ws-a")
            await manager.get_session_for_workspace("ws-a")

        restore.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_a_rebuilt_machine_is_checked_again(self):
        """The files went with the old sandbox, so the memo has to expire."""
        manager = _make_manager()
        session = _make_session()

        with _attached(manager, session) as restore:
            await manager.get_session_for_workspace("ws-a")
            session.sandbox.sandbox_id = "sandbox-rebuilt"
            await manager.get_session_for_workspace("ws-a")

        assert restore.await_count == 2

    @pytest.mark.asyncio
    async def test_the_folder_is_made_before_the_files_land(self):
        """A machine rebuilt for a sibling carries only that sibling's folder,
        and the transfer needs somewhere to put this project's files."""
        manager = _make_manager()
        session = _make_session()
        order: list[str] = []
        session.sandbox._ensure_workspace_dirs = AsyncMock(
            side_effect=lambda *_a, **_k: order.append("folder")
        )

        with _attached(manager, session, folder="beta-6c06") as restore:
            restore.side_effect = lambda *_a, **_k: order.append("restore")
            await manager.get_session_for_workspace("ws-joiner")

        assert order == ["folder", "restore"]
        session.sandbox._ensure_workspace_dirs.assert_awaited_once_with("beta-6c06")


# ---------------------------------------------------------------------------
# Tool overlays on a shared machine
# ---------------------------------------------------------------------------


class TestEveryProjectsToolOverlay(_Base):
    """The tool overlay is per project even though the wrapper union is not.

    The union is computer-wide and the asset manifest hashes it without any
    workspace identity, so a project joining a machine whose union is already
    current never reaches the install. Its empty ``.agents/tools`` is a
    namespace portion, which leaves the union importable through the shared
    path entry with nothing left to gate it.
    """

    @staticmethod
    def _joining(*, claim_missing=True):
        session = _make_session()
        session.mcp_config_workspace_id = None
        session.sandbox.workspace_overlay_missing = AsyncMock(
            return_value=claim_missing
        )
        return session

    @pytest.mark.asyncio
    async def test_a_joining_project_gets_its_own_overlay(self):
        manager = _make_manager()
        session = self._joining()

        with _attaching(manager, session) as reached:
            await manager.get_session_for_workspace("ws-joiner", user_id="user-1")

        session.sandbox.workspace_overlay_missing.assert_any_await(
            workspace_id="ws-joiner", dir_name="joiner-6c06"
        )
        reached.sync.assert_awaited_once()
        assert reached.sync.await_args.args[0].workspace_id == "ws-joiner"
        assert reached.sync.await_args.kwargs["reusing_sandbox"] is True
        assert reached.sync.await_args.args[0].dir_name == "joiner-6c06"

    @pytest.mark.asyncio
    async def test_the_joining_projects_own_server_set_is_resolved_first(self):
        """The session's composite belongs to whichever sibling resolved last,
        and that set is what the install would write into this overlay."""
        manager = _make_manager()
        session = self._joining()

        with _attaching(manager, session) as reached:
            await manager.get_session_for_workspace("ws-joiner", user_id="user-1")

        reached.resolve.assert_awaited_once()
        binding, user_id = reached.resolve.await_args.args[:2]
        assert (binding.workspace_id, user_id) == ("ws-joiner", "user-1")
        # None rather than the row's number: a version compare cannot answer
        # for a workspace whose composite was never installed.
        assert reached.resolve.await_args.kwargs["ws_version"] is None

    @pytest.mark.asyncio
    async def test_a_project_holding_its_claim_is_left_alone(self):
        """The steady state pays one ledger read and nothing else."""
        manager = _make_manager()
        session = self._joining(claim_missing=False)

        with _attaching(manager, session) as reached:
            await manager.get_session_for_workspace("ws-joiner")

        reached.resolve.assert_not_awaited()
        reached.sync.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_sibling_set_is_never_written_into_this_overlay(self):
        """A resolve that did not land leaves a sibling's composite on the
        session, and syncing it here is worse than leaving the folder for the
        next acquire."""
        manager = _make_manager()
        session = self._joining()

        with _attaching(manager, session, resolve_lands=False) as reached:
            await manager.get_session_for_workspace("ws-joiner")

        reached.resolve.assert_awaited_once()
        reached.sync.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_the_machines_root_owner_gets_a_config_too(self):
        manager = _make_manager()
        session = self._joining()

        with _attaching(manager, session, folder=None) as reached:
            await manager.get_session_for_workspace("ws-root")

        session.sandbox.workspace_overlay_missing.assert_awaited()
        reached.sync.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_an_overlay_already_built_is_checked_on_reacquire(self):
        """A warm acquisition detects files removed after the initial attach."""
        manager = _make_manager()
        session = self._joining()

        with _attaching(manager, session) as reached:
            await manager.get_session_for_workspace("ws-joiner")
            await manager.get_session_for_workspace("ws-joiner")

        assert session.sandbox.workspace_overlay_missing.await_count == 3
        reached.sync.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_a_rebuilt_machine_is_checked_again(self):
        """The overlay went with the old sandbox, so the memo has to expire."""
        manager = _make_manager()
        session = self._joining()

        with _attaching(manager, session):
            await manager.get_session_for_workspace("ws-joiner")
            session.sandbox.sandbox_id = "sandbox-rebuilt"
            await manager.get_session_for_workspace("ws-joiner")

        assert session.sandbox.workspace_overlay_missing.await_count == 3

    @pytest.mark.asyncio
    async def test_the_files_land_before_the_overlay_is_built(self):
        """Both write into the folder, and the restore's completeness guard
        must not see a half-built tool tree as this project's own files."""
        manager = _make_manager()
        session = self._joining()
        order: list[str] = []

        with _attaching(manager, session) as reached:
            manager._maybe_restore_files.side_effect = lambda *_a, **_k: order.append(
                "restore"
            )
            reached.sync.side_effect = lambda *_a, **_k: order.append("overlay")
            await manager.get_session_for_workspace("ws-joiner")

        assert order == ["restore", "overlay"]

    @pytest.mark.asyncio
    async def test_a_failed_build_is_not_remembered_as_done(self):
        """The folder and its files are the attach's other two jobs, so a
        sandbox that cannot answer about its ledger must not cost them. The
        memo stays unset: a project left without an overlay reads the whole
        union ungated, so the next acquire has to try again."""
        manager = _make_manager()
        session = self._joining()
        session.sandbox.workspace_overlay_missing = AsyncMock(
            side_effect=RuntimeError("toolbox down")
        )

        with _attaching(manager, session) as reached:
            await manager.get_session_for_workspace("ws-joiner")
            manager._maybe_restore_files.assert_awaited_once()
            await manager.get_session_for_workspace("ws-joiner")

        reached.sync.assert_not_awaited()
        assert session.sandbox.workspace_overlay_missing.await_count == 2


# ---------------------------------------------------------------------------
# A bare start is a (re)provision: it answers to the plan, not the row
# ---------------------------------------------------------------------------


class TestStartAnswersToEntitlement(_Base):
    def _manager(self, *, tier="standard", always_on=False):
        manager = _make_manager()
        manager._entitled_tier = AsyncMock(return_value=tier)
        manager._entitled_always_on = AsyncMock(return_value=always_on)
        manager._destroy_sandbox = AsyncMock()
        manager._apply_autostop_for_always_on = AsyncMock()
        manager._sync_machine_assets = AsyncMock()
        manager._serve_livefs = AsyncMock()
        manager._link_livefs_soon = MagicMock()
        manager._record_sync = MagicMock()
        manager._put_session = MagicMock()
        manager._clear_session = AsyncMock()
        manager._retire_session = AsyncMock()
        manager._session_sandbox_id = MagicMock(return_value="sandbox-new")
        return manager

    def _session(self):
        session = _make_session()
        session.initialize = AsyncMock()
        session.sandbox.runtime = MagicMock()
        return session

    @pytest.mark.asyncio
    @patch("src.server.services.platform_secret_rollout.certify_platform_secrets")
    @patch(f"{_MACHINES}.update_computer_activity")
    @patch(f"{_MACHINES}.try_bind_computer_provider_ref")
    @patch(f"{_MACHINES}.SessionManager")
    async def test_a_lapsed_tier_rebuilds_the_sandbox_at_the_entitled_size(
        self, mock_sm, mock_bind, mock_activity, mock_certify
    ):
        manager = self._manager(tier="standard")
        session = self._session()
        mock_sm.get_session.return_value = session
        mock_bind.return_value = {"computer_id": "comp-1"}
        mock_certify.return_value = 3
        computer = _make_computer(
            status="starting", provider_ref="sandbox-old", resource_tier="large"
        )

        with patch(f"{_MACHINES}.get_computer", AsyncMock(return_value=computer)):
            await manager._build_machine_session(
                computer, user_id="user-1", on_state_observed=None
            )

        manager._destroy_sandbox.assert_awaited_once()
        assert manager._destroy_sandbox.await_args.args[0] == "sandbox-old"
        kwargs = session.initialize.await_args.kwargs
        assert "sandbox_id" not in kwargs
        assert kwargs["tier"] == "standard"
        assert kwargs["auto_stop_minutes"] is None
        assert (
            mock_bind.await_args.kwargs["expected_previous_provider_ref"]
            == "sandbox-old"
        )

    @pytest.mark.asyncio
    @patch(f"{_MACHINES}.SessionManager")
    async def test_a_lapsed_tier_does_not_replace_an_undeleted_sandbox(
        self, mock_sm
    ):
        manager = self._manager(tier="standard")
        manager._destroy_sandbox.side_effect = RuntimeError("provider unavailable")
        session = self._session()
        mock_sm.get_session.return_value = session
        computer = _make_computer(
            status="starting", provider_ref="sandbox-old", resource_tier="large"
        )

        with pytest.raises(RuntimeError, match="provider unavailable"):
            await manager._build_machine_session(
                computer, user_id="user-1", on_state_observed=None
            )

        manager._destroy_sandbox.assert_awaited_once()
        session.initialize.assert_not_awaited()

    @pytest.mark.asyncio
    @patch(f"{_MACHINES}.update_computer_activity")
    @patch(f"{_MACHINES}.update_computer_status")
    @patch(f"{_MACHINES}.SessionManager")
    async def test_a_reconnect_with_a_lapsed_always_on_regains_its_autostop(
        self, mock_sm, mock_status, mock_activity
    ):
        manager = self._manager(tier="standard", always_on=False)
        session = self._session()
        mock_sm.get_session.return_value = session
        mock_status.return_value = {"computer_id": "comp-1", "status": "running"}
        computer = _make_computer(
            status="starting", provider_ref="sandbox-new", is_always_on=False
        )

        with patch(f"{_MACHINES}.get_computer", AsyncMock(return_value=computer)):
            await manager._build_machine_session(
                computer, user_id="user-1", on_state_observed=None
            )

        assert session.initialize.await_args.kwargs["sandbox_id"] == "sandbox-new"
        manager._destroy_sandbox.assert_not_awaited()
        manager._apply_autostop_for_always_on.assert_awaited_once()
        assert (
            manager._apply_autostop_for_always_on.await_args.kwargs["enabled"] is False
        )

    @pytest.mark.asyncio
    @patch(f"{_MACHINES}.update_computer_activity")
    @patch(f"{_MACHINES}.update_computer_status")
    @patch(f"{_MACHINES}.SessionManager")
    async def test_a_reconnect_retires_a_stale_initialized_handle(
        self, mock_sm, mock_status, mock_activity
    ):
        manager = self._manager(tier="standard", always_on=True)
        stale = self._session()
        fresh = self._session()
        mock_sm.get_cached_session.return_value = stale
        mock_sm.get_session.return_value = fresh
        mock_status.return_value = {"computer_id": "comp-1", "status": "running"}
        computer = _make_computer(
            status="starting", provider_ref="sandbox-new", is_always_on=True
        )

        with patch(f"{_MACHINES}.get_computer", AsyncMock(return_value=computer)):
            await manager._build_machine_session(
                computer, user_id="user-1", on_state_observed=None
            )

        manager._retire_session.assert_awaited_once_with(
            "comp-1",
            stale,
            reason="machine start requires a fresh provider reconnect",
        )
        assert fresh.initialize.await_args.kwargs["sandbox_id"] == "sandbox-new"
        stale.initialize.assert_not_awaited()

    @pytest.mark.asyncio
    @patch(f"{_MACHINES}.update_computer_activity")
    @patch(f"{_MACHINES}.update_computer_status")
    @patch(f"{_MACHINES}.SessionManager")
    async def test_an_intact_entitlement_reconnects_untouched(
        self, mock_sm, mock_status, mock_activity
    ):
        manager = self._manager(tier="large", always_on=True)
        session = self._session()
        mock_sm.get_session.return_value = session
        mock_status.return_value = {"computer_id": "comp-1", "status": "running"}
        computer = _make_computer(
            status="starting",
            provider_ref="sandbox-new",
            resource_tier="large",
            is_always_on=True,
        )

        with patch(f"{_MACHINES}.get_computer", AsyncMock(return_value=computer)):
            await manager._build_machine_session(
                computer, user_id="user-1", on_state_observed=None
            )

        manager._destroy_sandbox.assert_not_awaited()
        manager._apply_autostop_for_always_on.assert_awaited_once()


class TestFreshBuildSettlesEachRestoreAfterTheBind(_Base):
    """A fresh build restores every project before the bind, when no row names
    the new sandbox yet, so each restore's flag clear matches nothing. The
    start repeats it once the bind lands, for every project it restored."""

    _manager = TestStartAnswersToEntitlement._manager
    _session = TestStartAnswersToEntitlement._session

    async def _build(self, manager, *, order):
        session = self._session()
        manager.resolve_binding = _resolving("proj-dir")
        manager._ensure_workspace_dirs = AsyncMock()
        manager._restore_files = AsyncMock(
            side_effect=lambda b, *_a, **_k: order.append(("restore", b.workspace_id))
        )
        manager._put_session = MagicMock(side_effect=lambda *_a: order.append(("put",)))

        async def _bind(*_a, **_k):
            order.append(("bind",))
            return {"computer_id": "comp-1"}

        computer = _make_computer(
            status="starting", provider_ref="sandbox-old", resource_tier="large"
        )
        with (
            patch(f"{_MACHINES}.SessionManager") as mock_sm,
            patch(f"{_MACHINES}.try_bind_computer_provider_ref", AsyncMock(side_effect=_bind)),
            patch(f"{_MACHINES}.update_computer_activity", AsyncMock()),
            patch(
                f"{_MACHINES}.get_live_workspace_ids_for_computer",
                AsyncMock(return_value=["ws-a", "ws-b"]),
            ),
            patch(f"{_MACHINES}.get_computer", AsyncMock(return_value=computer)),
            patch(
                "src.server.services.platform_secret_rollout.certify_platform_secrets",
                AsyncMock(return_value=1),
            ),
        ):
            mock_sm.get_cached_session.return_value = None
            mock_sm.get_session.return_value = session
            # The tier lapsed, so the start builds rather than reconnects.
            result = await manager._build_machine_session(
                computer, user_id="user-1", on_state_observed=None
            )
        return result, session

    @pytest.mark.asyncio
    async def test_every_restored_project_is_settled_after_the_bind(self):
        manager = self._manager(tier="standard")
        order: list[tuple] = []
        manager._maybe_restore_files = AsyncMock(
            side_effect=lambda b, *_a, **_k: order.append(("settle", b.workspace_id))
        )

        result, session = await self._build(manager, order=order)

        assert result is session
        assert order == [
            ("restore", "ws-a"),
            ("restore", "ws-b"),
            ("bind",),
            ("put",),
            ("settle", "ws-a"),
            ("settle", "ws-b"),
        ]
        for call in manager._maybe_restore_files.await_args_list:
            assert call.args[1] is session.sandbox

    @pytest.mark.asyncio
    async def test_a_failed_settle_is_logged_and_the_start_still_succeeds(self):
        """The flag stays up, the safe side; a bound machine is not unwound."""
        manager = self._manager(tier="standard")
        manager._maybe_restore_files = AsyncMock(
            side_effect=[RuntimeError("db blip"), True]
        )

        result, session = await self._build(manager, order=[])

        assert result is session
        assert manager._maybe_restore_files.await_count == 2
        manager._clear_session.assert_not_awaited()
        manager._retire_session.assert_not_awaited()
        manager._destroy_sandbox.assert_awaited_once()  # only the lapsed tier's


class TestReviewProviderAndCapacityRegressions(_Base):
    @pytest.mark.parametrize(
        'kind,overrides',
        [('daytona', {'base_url': 'https://owned-provider.example/api', 'api_key': 'owned-test-key'}),
         ('docker', {'image': 'owned-sandbox:test', 'memory_limit': '6g'})],
    )
    def test_live_session_config_uses_durable_provider_overrides(self, kind, overrides):
        manager = WorkspaceManager.get_instance(config=_make_real_config())
        binding = _make_binding(kind=kind, provider_config=overrides)
        core = manager._core_config_for(binding)
        effective = getattr(core.sandbox, kind)
        for name, value in overrides.items():
            assert getattr(effective, name) == value
            assert getattr(getattr(manager.config.to_core_config().sandbox, kind), name) != value
        assert getattr(manager._core_config_for(binding).sandbox, kind) is not effective

    @pytest.mark.asyncio
    @pytest.mark.parametrize('existing', [True, False])
    async def test_preparing_a_primary_does_not_consume_active_capacity(self, existing):
        manager = _make_manager()
        computer = _make_computer() if existing else None
        created = _make_computer(is_primary=True)
        with (
            patch(f'{_MACHINES}.get_primary_computer', AsyncMock(return_value=computer)),
            patch.object(
                manager, 'create_computer_for_user', AsyncMock(return_value=created)
            ) as create,
            patch('src.server.dependencies.usage_limits.enforce_computer_limit',
                  AsyncMock()) as gate,
        ):
            if existing:
                assert await manager.ensure_primary_computer('user-1') is computer
                create.assert_not_awaited()
            else:
                assert await manager.ensure_primary_computer('user-1') is created
                create.assert_awaited_once()
            gate.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_spec_uses_docker_override_for_validation_and_disk_guard(self):
        manager = WorkspaceManager.get_instance(config=_make_real_config())
        @asynccontextmanager
        async def decision_lock(_computer_id):
            yield True
        manager._machine_decision_lock = decision_lock
        computer = _make_computer(
            kind='docker', resource_tier='performance', provider_ref='container-test',
            status='stopped',
            provider_config={'resource_tiers': {
                'standard': {'cpu': 1, 'memory': 1, 'disk': 8},
                'performance': {'cpu': 2, 'memory': 2, 'disk': 10},
                'owned-tier': {'cpu': 1, 'memory': 1, 'disk': 6},
            }},
        )
        with (
            patch('src.server.services.computer_manager._spec.get_computer', AsyncMock(return_value=computer)),
            patch(
                'src.server.services.computer_manager._spec.claim_computer_spec_change',
                AsyncMock(return_value={**computer, 'spec_change': {'claim_id': 'c-1'}}),
            ),
            patch('src.server.services.computer_manager._spec.settle_computer_spec_change', AsyncMock()),
            patch('src.server.services.computer_manager._spec.heartbeat_computer_spec_change', AsyncMock(return_value=True)),
            patch('src.server.services.computer_manager._spec.publish_computer_status_change', AsyncMock()),
            patch.object(manager, '_apply_spec_change', AsyncMock()) as apply,
        ):
            await manager.set_computer_spec('comp-1', 'owned-tier')
        assert apply.await_args.kwargs['disk_guard'] == 6

    @pytest.mark.asyncio
    async def test_running_spec_change_reconnects_on_an_uncached_worker(self):
        manager = _make_manager()
        binding = _make_binding()
        session = _make_session()
        session._initialized = False
        session.sandbox = None
        session.initialize = AsyncMock()

        async def attach(*, sandbox_id):
            session._initialized = True
            session.sandbox = MagicMock(sandbox_id=sandbox_id)

        session.initialize.side_effect = attach
        manager._machine_has_active_tasks = AsyncMock(return_value=False)
        manager._session_handle = MagicMock(return_value=session)

        result = await manager._assert_machine_is_replaceable(
            binding, "sandbox-abc", "ws-a"
        )

        assert result is session
        session.initialize.assert_awaited_once_with(sandbox_id="sandbox-abc")
        assert manager._cached_session("comp-1") is session

    @pytest.mark.asyncio
    async def test_reclaim_uses_computers_standard_disk(self):
        manager = WorkspaceManager.get_instance(config=_make_real_config())
        binding = _make_binding(kind='docker', resource_tier='max', provider_config={
            'resource_tiers': {'standard': {'cpu': 1, 'memory': 1, 'disk': 8}},
        })
        with (
            patch('src.server.dependencies.usage_limits.spec_entitlement_lost', AsyncMock(return_value=True)),
            patch('src.server.services.workspace_entitlements.db_set_computer_resource_tier', AsyncMock()),
            patch.object(manager, '_assert_machine_disk_fits', AsyncMock()) as disk,
        ):
            assert await manager._entitled_tier(binding, 'user-1') == 'standard'
            # Measured on the mirror before the backup, and again after it.
            assert [c.args for c in disk.await_args_list] == [('comp-1', 8)] * 2


@pytest.mark.asyncio
async def test_overlay_failed_upload_retries_then_repairs_warm_deletion():
    manager = _make_manager()
    session = TestEveryProjectsToolOverlay._joining()
    with _attaching(manager, session) as reached:
        succeed = reached.sync.side_effect
        reached.sync.side_effect = None
        reached.sync.return_value = None
        await manager.get_session_for_workspace("ws-joiner", user_id="user-1")
        assert not manager._projects_attached
        reached.sync.side_effect = succeed
        await manager.get_session_for_workspace("ws-joiner", user_id="user-1")
        assert manager._projects_attached
        session.sandbox.workspace_overlay_missing.return_value = True
        await manager.get_session_for_workspace("ws-joiner", user_id="user-1")
        assert reached.sync.await_count == 3


@pytest.mark.asyncio
async def test_transient_restore_failure_is_retried_before_attach_is_remembered():
    manager = _make_manager()
    manager._projects_attached.clear()
    session = TestEveryProjectsToolOverlay._joining()
    with _attaching(manager, session) as reached:
        manager._maybe_restore_files.side_effect = [False, True]
        restore = manager._maybe_restore_files
        await manager.get_session_for_workspace("ws-joiner", user_id="user-1")
        assert not manager._projects_attached
        await manager.get_session_for_workspace("ws-joiner", user_id="user-1")

    assert restore.await_count == 2
    assert manager._projects_attached
    assert reached.sync.await_count == 1


@pytest.mark.asyncio
async def test_an_unfinished_skill_pass_is_retried_before_attach_is_remembered():
    """The attach pass is the only one that links a sibling's folder before
    its first turn; remembered after a pass that gave up, the folder runs
    without its shared skills until some later lifecycle event."""
    manager = _make_manager()
    manager._projects_attached.clear()
    session = TestEveryProjectsToolOverlay._joining()
    with _attaching(manager, session):
        manager._reconcile_skills.side_effect = [False, True]
        await manager.get_session_for_workspace("ws-joiner", user_id="user-1")
        assert not manager._projects_attached
        await manager.get_session_for_workspace("ws-joiner", user_id="user-1")

        assert manager._reconcile_skills.await_count == 2
    assert manager._projects_attached

@pytest.mark.asyncio
async def test_a_workspace_the_bring_up_drops_during_its_attach_is_queued_again():
    """The bring-up the restore queued runs at the attach's next await and may
    drop the workspace (its folder began a move), discarding its key. The
    attach must not put the key back, or no later acquisition queues it."""
    manager = _make_manager()
    manager._projects_attached.clear()
    session = TestEveryProjectsToolOverlay._joining()
    with _attaching(manager, session) as reached:
        restore = manager._maybe_restore_files
        synced = reached.sync.side_effect

        async def dropped_meanwhile(*args, **kwargs):
            manager._projects_attached.clear()
            return await synced(*args, **kwargs)

        reached.sync.side_effect = dropped_meanwhile
        await manager.get_session_for_workspace("ws-joiner", user_id="user-1")
        assert not manager._projects_attached
        await manager.get_session_for_workspace("ws-joiner", user_id="user-1")

    assert restore.await_count == 2
    assert manager._projects_attached

@pytest.mark.asyncio
async def test_layout_failure_is_not_downgraded_to_best_effort_asset_sync():
    from ptc_agent.core.sandbox.migration import LayoutMigrationError

    manager = _make_manager()
    binding = _make_binding(dir_name="project-ab12")
    manager._vault_snapshot = AsyncMock(return_value=(None, {}, ""))
    manager._layout_root_owner_dir = AsyncMock(return_value=binding.dir_name)
    sandbox = SimpleNamespace(sync_sandbox_assets=AsyncMock(
        side_effect=LayoutMigrationError("partial move")
    ))
    with (
        patch(f"{_PROVISIONING}.sandbox_skill_sync_params", AsyncMock(return_value={})),
        patch(f"{_PROVISIONING}.get_workspace_dir_names_for_computer", AsyncMock(return_value=(binding.dir_name,))),
    ):
        with pytest.raises(LayoutMigrationError, match="partial move"):
            await manager._sync_sandbox_assets(binding, None, sandbox)


@pytest.mark.asyncio
async def test_overlay_repair_does_not_hide_a_layout_failure():
    from ptc_agent.core.sandbox.migration import LayoutMigrationError

    manager = _make_manager()
    binding = _make_binding(dir_name="project-ab12")
    session = SimpleNamespace(sandbox=SimpleNamespace(
        workspace_overlay_missing=AsyncMock(return_value=True)
    ))
    manager._apply_session_mcp = AsyncMock()
    manager._workspace_tool_view = MagicMock(return_value=object())
    manager._sync_sandbox_assets = AsyncMock(side_effect=LayoutMigrationError("partial move"))
    with pytest.raises(LayoutMigrationError, match="partial move"):
        await manager._ensure_project_tool_overlay(binding, session, user_id="user-1")


@pytest.fixture(autouse=True)
def _local_lifecycle_capacity(monkeypatch):
    # Lifecycle fakes have no platform or database; admission is tested separately.
    monkeypatch.setattr("src.server.dependencies.usage_limits.platform_gating_active", lambda: False)


@pytest.mark.asyncio
@pytest.mark.parametrize("denied", [False, True])
async def test_capacity_admission_holds_user_lock_through_committed_claim(denied):
    from fastapi import HTTPException

    manager = _make_manager()
    connection = object()
    held = False

    @asynccontextmanager
    async def capacity_lock(user_id):
        nonlocal held
        assert user_id == "user-1"
        held = True
        try:
            yield connection
        finally:
            held = False

    async def admit(_user_id):
        assert held
        if denied:
            raise HTTPException(429, "full")

    async def claim(computer_id, **kwargs):
        assert held and kwargs["conn"] is connection
        return {"status": "starting"}

    with (
        patch("src.server.dependencies.usage_limits.platform_gating_active", return_value=True),
        patch("src.server.dependencies.usage_limits.enforce_computer_limit", side_effect=admit),
        patch(f"{_MACHINES}.computer_capacity_lock", capacity_lock),
        patch(f"{_MACHINES}.get_computer", AsyncMock(return_value=_make_computer(status="stopped"))),
        patch(f"{_MACHINES}.try_claim_computer_for_start", AsyncMock(side_effect=claim)) as claimed,
    ):
        if denied:
            with pytest.raises(HTTPException):
                await manager._claim_start_with_capacity("comp-1", from_status="stopped")
            claimed.assert_not_awaited()
        else:
            assert await manager._claim_start_with_capacity("comp-1", from_status="stopped") == {"status": "starting"}
    assert not held


@pytest.mark.asyncio
async def test_stopped_replacement_does_not_delete_after_incomplete_backup():
    manager = _make_manager()
    binding = _make_binding(provider_ref="original")
    session = SimpleNamespace(initialize=AsyncMock(), stop=AsyncMock(), sandbox=object())
    manager._sync_machine_assets = AsyncMock()
    manager._backup_machine_files_to_db = AsyncMock(side_effect=RuntimeError("one sibling unsaved"))
    manager._destroy_sandbox = AsyncMock()
    with (
        patch("src.server.services.computer_manager._spec.Session", return_value=session),
        patch("src.server.services.computer_manager._spec.try_claim_computer_for_start", AsyncMock(return_value={"status": "starting"})),
        patch("src.server.services.computer_manager._spec.update_computer_status", AsyncMock()) as status,
        patch("src.server.services.computer_manager._spec.heartbeat_computer_spec_change", AsyncMock(return_value=True)),
    ):
        with pytest.raises(RuntimeError, match="one sibling unsaved"):
            await manager._replace_stopped_sandbox(binding, "original", claim_id="c-1", disk_guard=None, origin_workspace_id=None, user_id="user-1")
    manager._destroy_sandbox.assert_not_awaited()
    session.stop.assert_awaited_once()
    assert manager._backup_machine_files_to_db.await_args.kwargs["strict"] is True
    status.assert_awaited_once_with(binding.computer_id, "stopped", expected="starting")


@pytest.mark.asyncio
@pytest.mark.parametrize("gone", [False, True])
async def test_stopped_replacement_drops_the_reading_of_the_sandbox_it_replaced(gone):
    """The row keeps naming the old sandbox until the next start, so its
    reading (the old tier's total, its fullness) would still read as the
    machine's. Dropped once the sandbox is destroyed, or found already gone."""
    from ptc_agent.core.sandbox.runtime import SandboxGoneError

    manager = _make_manager()
    binding = _make_binding(provider_ref="original")
    order = []
    session = SimpleNamespace(
        initialize=AsyncMock(side_effect=SandboxGoneError("gone") if gone else None),
        stop=AsyncMock(),
        sandbox=object(),
    )
    manager._sync_machine_assets = AsyncMock()
    manager._backup_machine_files_to_db = AsyncMock()
    manager._destroy_sandbox = AsyncMock(side_effect=lambda *_a, **_k: order.append("destroy"))
    with (
        patch("src.server.services.computer_manager._spec.Session", return_value=session),
        patch("src.server.services.computer_manager._spec.try_claim_computer_for_start", AsyncMock(return_value={"status": "starting"})),
        patch("src.server.services.computer_manager._spec.update_computer_status", AsyncMock()),
        patch("src.server.services.computer_manager._spec.heartbeat_computer_spec_change", AsyncMock(return_value=True)),
        patch(
            "src.server.services.computer_manager._spec.clear_computer_disk",
            AsyncMock(side_effect=lambda *_a, **_k: order.append("clear")),
        ) as clear,
    ):
        await manager._replace_stopped_sandbox(binding, "original", claim_id="c-1", disk_guard=None, origin_workspace_id=None, user_id="user-1")
    clear.assert_awaited_once_with(binding.computer_id, sandbox_id="original")
    assert order == (["clear"] if gone else ["destroy", "clear"])


@pytest.mark.asyncio
async def test_a_stopped_computers_backup_brings_no_mount_up():
    """Its session stops once the files are saved, and no revoke follows, so
    a token minted for it would stay live on a stopped computer."""
    # The tests above stub the sync on the shared instance.
    ComputerManager.reset_instance()
    manager = _make_manager()
    binding = _make_binding(provider_ref="original")
    sandbox = SimpleNamespace(
        sync_sandbox_assets=AsyncMock(return_value=SimpleNamespace(layout_version=4))
    )
    session = SimpleNamespace(initialize=AsyncMock(), stop=AsyncMock(), sandbox=sandbox)
    manager._root_owner_folder = AsyncMock(return_value=None)
    manager._stamp_layout_version = AsyncMock()
    manager._backup_machine_files_to_db = AsyncMock()
    manager._destroy_sandbox = AsyncMock()
    manager._serve_livefs = AsyncMock()
    manager._link_livefs_soon = MagicMock()
    with (
        patch(f"{_MACHINES}.sandbox_skill_sync_params", AsyncMock(return_value={})),
        patch(f"{_MACHINES}.get_workspace_dir_names_for_computer", AsyncMock(return_value=())),
        patch("src.server.services.computer_manager._spec.Session", return_value=session),
        patch("src.server.services.computer_manager._spec.try_claim_computer_for_start", AsyncMock(return_value={"status": "starting"})),
        patch("src.server.services.computer_manager._spec.update_computer_status", AsyncMock()),
        patch("src.server.services.computer_manager._spec.heartbeat_computer_spec_change", AsyncMock(return_value=True)),
        patch("src.server.services.computer_manager._spec.clear_computer_disk", AsyncMock()),
    ):
        await manager._replace_stopped_sandbox(binding, "original", claim_id="c-1", disk_guard=None, origin_workspace_id=None, user_id="user-1")
    sandbox.sync_sandbox_assets.assert_awaited_once()
    manager._serve_livefs.assert_not_awaited()
    manager._link_livefs_soon.assert_not_called()


@pytest.mark.asyncio
async def test_a_failed_stopped_replacement_keeps_the_reading():
    manager = _make_manager()
    binding = _make_binding(provider_ref="original")
    session = SimpleNamespace(initialize=AsyncMock(), stop=AsyncMock(), sandbox=object())
    manager._sync_machine_assets = AsyncMock()
    manager._backup_machine_files_to_db = AsyncMock(side_effect=RuntimeError("unsaved"))
    manager._destroy_sandbox = AsyncMock()
    with (
        patch("src.server.services.computer_manager._spec.Session", return_value=session),
        patch("src.server.services.computer_manager._spec.try_claim_computer_for_start", AsyncMock(return_value={"status": "starting"})),
        patch("src.server.services.computer_manager._spec.update_computer_status", AsyncMock()),
        patch("src.server.services.computer_manager._spec.clear_computer_disk", AsyncMock()) as clear,
    ):
        with pytest.raises(RuntimeError):
            await manager._replace_stopped_sandbox(binding, "original", claim_id="c-1", disk_guard=None, origin_workspace_id=None, user_id="user-1")
    clear.assert_not_awaited()


@pytest.mark.asyncio
async def test_strict_bare_restore_refuses_partial_file_result():
    manager = _make_manager()
    binding = _make_binding(dir_name="project")
    manager._project_layout = AsyncMock()
    with patch(f"{_PROVISIONING}.FilePersistenceService.restore_to_sandbox", AsyncMock(return_value={"errors": 1, "restored": 2})):
        with pytest.raises(RuntimeError, match="1 files failed to restore"):
            await manager._restore_files(binding, object(), expected_sandbox_id="old", strict=True)


@pytest.mark.asyncio
async def test_duplicate_stays_on_secondary_source_computer():
    manager = _make_manager()
    source = {"user_id": "user-1", "status": "stopped", "name": "source"}
    binding = _make_binding(computer_id="secondary")
    manager.resolve_binding = AsyncMock(return_value=binding)
    manager.ensure_primary_computer = AsyncMock()
    with (
        patch("src.server.services.workspace_entitlements.db_get_workspace", AsyncMock(return_value=source)),
        patch("src.server.services.workspace_entitlements.duplicate_workspace_on_computer", AsyncMock(return_value={"workspace_id": "copy"})) as duplicate,
        patch("src.server.services.workspace_entitlements.get_workspace_name_keys", AsyncMock(return_value={"source"})),
    ):
        await manager.duplicate_workspace("source", "user-1")
    assert duplicate.await_args.args[3] == "secondary"
    manager.ensure_primary_computer.assert_not_awaited()


@pytest.mark.asyncio
async def test_legacy_adoption_preserves_provider_and_root_stamp():
    manager = _make_manager()
    source = {
        "user_id": "user-1", "status": "stopped", "sandbox_id": "container-id",
        "config": {"sandbox_provider": "docker", "sandbox_working_dir": "/old/root"},
    }
    with (
        patch(f"{_MACHINES}.get_computer_by_provider_ref", AsyncMock(return_value=None)) as lookup,
        patch(f"{_MACHINES}.create_computer", AsyncMock(return_value=_make_computer(origin_workspace_id="source"))) as create,
        patch(f"{_MACHINES}.bind_workspace_to_computer", AsyncMock(return_value={"dir_name": "source-folder"})),
        patch(f"{_MACHINES}.get_workspace_dir_names_for_computer", AsyncMock(return_value=())),
    ):
        await manager._adopt_workspace_onto_computer("source", workspace=source)
    lookup.assert_awaited_once_with("docker", "container-id")
    assert create.await_args.kwargs["kind"] == "docker"
    assert create.await_args.kwargs["root_dir"] == "/old/root"
