"""
Tests for ComputerManager, the machine a sandbox belongs to.

Covers the insert that mints a computer and the primary every new project
joins, the machine keying that lets one sandbox serve more than one project, the split-binding window that
makes a cached handle untrustworthy, the computer-scoped activity gate, and
the computer-addressed surface the router codes against.
"""

from contextlib import ExitStack, asynccontextmanager, contextmanager
from types import SimpleNamespace
from unittest.mock import ANY, AsyncMock, MagicMock, patch

import pytest

from ptc_agent.config.core import FilesystemConfig
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
_SESSIONS = "src.server.services.computer_manager._sessions"

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

    reached = SimpleNamespace(resolve=AsyncMock(side_effect=_apply), sync=AsyncMock())
    patches = (
        ("_acquire_session", AsyncMock(return_value=session)),
        ("resolve_binding", _resolving(folder)),
        ("_ensure_workspace_dirs", AsyncMock()),
        ("_maybe_restore_files", AsyncMock()),
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
        ("_maybe_restore_files", AsyncMock()),
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

    def teardown_method(self):
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
    @patch(f"{_MACHINES}.bind_workspace_to_computer")
    @patch(f"{_MACHINES}.get_computer_by_provider_ref")
    async def test_adoption_speaks_the_insert_contract(self, mock_by_ref, mock_bind):
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
        assert adopted["dir_name"] == "research-ab12"

    @pytest.mark.asyncio
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
        mock_bind.assert_awaited_once()
        assert mock_bind.await_args.args[1] == "comp-mine"

    @pytest.mark.asyncio
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
        mock_get.return_value = _make_computer(is_primary=False, status="running")

        retired = await manager._retire_machine_if_empty("comp-1", "ws-a", workspace)

        assert retired is True
        (binding,) = manager._teardown_machine.await_args.args
        assert (binding.workspace_id, binding.computer_id) == ("ws-a", "comp-1")
        mock_status.assert_awaited_once_with("comp-1", "deleted")
        manager._remove_workspace_folder.assert_not_awaited()

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


# ---------------------------------------------------------------------------
# Taking a project off a machine that keeps running
# ---------------------------------------------------------------------------


class TestRemovingAProjectFolder(_Base):
    """``rm -rf`` of one folder, addressed through the machine's durable ref."""

    @pytest.mark.asyncio
    async def test_a_running_machine_loses_the_folder(self):
        manager = _make_manager()
        runtime = MagicMock()
        runtime.exec = AsyncMock()
        manager._detached_runtime = _detached(runtime)

        await manager._remove_workspace_folder(
            "ws-a",
            _make_workspace(dir_name="alpha-1a2b"),
            _make_computer(status="running", provider_ref="sandbox-abc"),
        )

        runtime.exec.assert_awaited_once_with("rm -rf /home/workspace/alpha-1a2b")

    @pytest.mark.asyncio
    async def test_a_stopped_machine_is_not_woken_for_it(self):
        """The folder is only bytes the mirror holds and no live row names it."""
        manager = _make_manager()
        runtime = MagicMock()
        runtime.exec = AsyncMock()
        manager._detached_runtime = _detached(runtime)

        await manager._remove_workspace_folder(
            "ws-a",
            _make_workspace(dir_name="alpha-1a2b"),
            _make_computer(status="stopped", provider_ref="sandbox-abc"),
        )

        runtime.exec.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_project_owning_the_root_unlinks_nothing(self):
        """An unsplit computer's project is the root, and the root is the
        machine's own runtime rather than one project's folder."""
        manager = _make_manager()
        runtime = MagicMock()
        runtime.exec = AsyncMock()
        manager._detached_runtime = _detached(runtime)

        await manager._remove_workspace_folder(
            "ws-a",
            _make_workspace(dir_name=None),
            _make_computer(status="running", provider_ref="sandbox-abc"),
        )

        runtime.exec.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_provider_failure_does_not_fail_the_delete(self):
        manager = _make_manager()
        runtime = MagicMock()
        runtime.exec = AsyncMock(side_effect=RuntimeError("toolbox down"))
        manager._detached_runtime = _detached(runtime)

        await manager._remove_workspace_folder(
            "ws-a",
            _make_workspace(dir_name="alpha-1a2b"),
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

    def test_the_kind_reaches_the_core_config(self):
        manager = _make_manager()
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

        manager._start_machine.assert_awaited_once_with("comp-1")
        manager._acquire_session.assert_not_awaited()

    @pytest.mark.asyncio
    @patch(f"{_PROVISIONING}.get_live_workspace_ids_for_computer")
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

    @pytest.mark.asyncio
    async def test_a_project_naming_another_sandbox_is_still_a_split(self):
        """Two non-null ids is the disagreement neither table can settle, and
        the losing bind's own sandbox is already deleted."""
        manager = _make_manager()
        recovered = _make_session()
        workspace = {
            "workspace_id": "ws-new",
            "sandbox_id": "sandbox-other",
            "status": "running",
            "computer_id": "comp-1",
        }

        with (
            patch.object(manager, "_session_handle", return_value=_make_session()),
            patch.object(manager, "_clear_session", AsyncMock()),
            patch.object(
                manager, "_recover_sandbox", AsyncMock(return_value=recovered)
            ) as recover,
            patch(
                f"{_LIFECYCLE}.adopt_computer_sandbox_into_workspaces",
                AsyncMock(),
            ) as adopt,
        ):
            got, did_init = await manager._attach_running_session(
                self._running_binding(), workspace, "user-1", None, lambda _p: None
            )

        assert got is recovered and did_init is True
        recover.assert_awaited_once()
        adopt.assert_not_awaited()

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
        """Every turn would otherwise pay a sandbox round trip for nothing."""
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

        session.sandbox.workspace_overlay_missing.assert_awaited_once_with(
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
    async def test_the_machines_root_owner_needs_no_overlay(self):
        """It reads the union directly, so there is no folder to build one in."""
        manager = _make_manager()
        session = self._joining()

        with _attaching(manager, session, folder=None) as reached:
            await manager.get_session_for_workspace("ws-root")

        session.sandbox.workspace_overlay_missing.assert_not_awaited()
        reached.sync.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_an_overlay_already_built_is_not_checked_again(self):
        """Every turn would otherwise pay a sandbox round trip for nothing."""
        manager = _make_manager()
        session = self._joining()

        with _attaching(manager, session) as reached:
            await manager.get_session_for_workspace("ws-joiner")
            await manager.get_session_for_workspace("ws-joiner")

        assert session.sandbox.workspace_overlay_missing.await_count == 1
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

        assert session.sandbox.workspace_overlay_missing.await_count == 2

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
            status="starting", provider_ref="sandbox-new", is_always_on=True
        )

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

        await manager._build_machine_session(
            computer, user_id="user-1", on_state_observed=None
        )

        manager._destroy_sandbox.assert_not_awaited()
        manager._apply_autostop_for_always_on.assert_not_awaited()
