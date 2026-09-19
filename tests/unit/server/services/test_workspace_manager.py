"""
Tests for WorkspaceManager service.

Tests workspace lifecycle: creation, session retrieval, stop, delete,
idle cleanup, singleton pattern, and background cleanup tasks.
"""

import asyncio
import logging
import time
import uuid
from contextlib import asynccontextmanager, contextmanager, ExitStack
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import ANY, AsyncMock, MagicMock, patch

import pytest

from ptc_agent.core.sandbox.runtime import SandboxGoneError
from src.server.models.computer import ComputerStatus

# Imported for its side effect: this module binds ``platform_secrets_active`` by
# value at import time, and the hot-resync tests below patch that name on its
# source module. Left to a lazy import inside the code under test, the first
# import would land while the patch is live and keep the mock for the rest of
# the session.
from src.server.services import platform_secret_rollout  # noqa: F401
from src.server.services.workspace_manager import WorkspaceManager
from tests.computer_manager_patch import cm_patch
from tests.unit.server.services.conftest import (
    _patch_identity,
    _patch_machine_bind,
)


_STUB_COMPUTER_ID = "11111111-1111-1111-1111-111111111111"

_LIFECYCLE = "src.server.services.computer_manager._lifecycle"
_MACHINES = "src.server.services.computer_manager._machines"
_PROVISIONING = "src.server.services.computer_manager._provisioning"
_ENTITLEMENTS = "src.server.services.workspace_entitlements"


_STUB_COMPUTER = {
    "computer_id": _STUB_COMPUTER_ID,
    "user_id": "user-1",
    "kind": "daytona",
    "root_dir": "/home/workspace",
    # The machine agrees with the project row ``_make_workspace`` builds: same
    # sandbox, same state. The lifecycle is the computer's, so a stub that
    # disagreed would send every acquire down the split-binding recreate.
    "provider_ref": "sandbox-abc",
    "status": "running",
    "resource_tier": "standard",
    "is_always_on": False,
    "is_primary": True,
}


@pytest.fixture(autouse=True)
def _stub_computer_minting():
    """Creating a project resolves the user's machine and reads it back.

    Both are the ``ComputerManager``'s reads; the tests in this module are about
    the project side, and the machine's own create path is covered in
    ``test_computer_manager.py``.
    """
    with (
        patch(
            f"{_MACHINES}.get_primary_computer",
            new=AsyncMock(return_value=dict(_STUB_COMPUTER)),
        ),
        patch(
            f"{_MACHINES}.create_computer",
            new=AsyncMock(return_value=dict(_STUB_COMPUTER)),
        ),
        patch(
            "src.server.services.computer_manager._sessions.get_computer_for_workspace",
            new=AsyncMock(return_value=dict(_STUB_COMPUTER)),
        ),
        # Phase 2 re-reads the machine rather than trusting the binding frozen
        # at turn start, and that read is where always-on and the sandbox id
        # come from. It is the same row this fixture mints.
        patch(
            f"{_LIFECYCLE}.get_computer",
            new=AsyncMock(return_value=dict(_STUB_COMPUTER)),
        ),
    ):
        yield


@pytest.fixture(autouse=True)
def _stub_project_folder():
    """Which folder a project owns on its machine is a row read of its own.

    Every file, restore and layout call resolves it, and these tests run with
    no pool, where the read raises rather than answering "the whole machine".
    """
    with patch(
        f"{_PROVISIONING}.db_get_workspace_dir_name",
        new=AsyncMock(return_value="test-ab12"),
    ):
        yield


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_config():
    """Create a minimal mock AgentConfig."""
    config = MagicMock()
    config.to_core_config.return_value = MagicMock()
    # The core config has to agree with the computer the autouse fixture mints,
    # or ``_core_config_for`` rewrites provider/root and drags these tests into
    # the platform-secret path. An empty catalog keeps that path skipped.
    core_config = config.to_core_config.return_value
    core_config.sandbox.provider = "daytona"
    core_config.sandbox.platform_secrets = {}
    core_config.filesystem.working_directory = "/home/workspace"
    config.daytona = MagicMock(api_key="test-key", base_url="https://daytona.test")
    config.sandbox = MagicMock(provider="daytona")
    config.filesystem = MagicMock(working_directory="/home/workspace")
    config.skills = MagicMock(enabled=False)
    return config


def _make_config_with_tiers():
    """Config whose daytona block exposes real resource tiers + auto-stop.

    ``set_workspace_spec`` reads ``resource_tiers`` (a real dict of tiers with a
    ``.disk`` attr) to validate the tier and compute downgrade-vs-upgrade, which
    the bare ``_make_config()`` MagicMock can't provide.
    """
    config = _make_config()
    config.sandbox = MagicMock(provider="daytona")
    config.sandbox.daytona = MagicMock(
        resource_tiers={
            "standard": SimpleNamespace(cpu=1, memory=1, disk=3),
            "performance": SimpleNamespace(cpu=2, memory=4, disk=5),
            "max": SimpleNamespace(cpu=4, memory=8, disk=10),
        },
        default_tier="standard",
        auto_stop_interval=3600,
    )
    return config


def _make_workspace(
    workspace_id=None,
    user_id="user-1",
    status="running",
    sandbox_id="sandbox-abc",
    **overrides,
):
    now = datetime.now(timezone.utc)
    data = {
        "workspace_id": workspace_id or str(uuid.uuid4()),
        "user_id": user_id,
        "name": "Test Workspace",
        "description": None,
        "sandbox_id": sandbox_id,
        "status": status,
        "mode": "ptc",
        "sort_order": 0,
        "created_at": now,
        "updated_at": now,
        "last_activity_at": now,
    }
    data.update(overrides)
    return data


def _make_computer(computer_id=None, user_id="user-1", status="running", **overrides):
    now = datetime.now(timezone.utc)
    data = {
        "computer_id": computer_id or str(uuid.uuid4()),
        "user_id": user_id,
        "name": "Test Computer",
        "kind": "daytona",
        "root_dir": "/home/workspace",
        "status": status,
        "resource_tier": "standard",
        "is_always_on": False,
        "is_primary": True,
        "provider_ref": "sandbox-abc",
        "created_at": now,
        "updated_at": now,
        "last_activity_at": now,
    }
    data.update(overrides)
    return data


def _decision_lock(acquired: bool):
    """Stand in for the Postgres advisory key, yielding whether it was taken."""

    @asynccontextmanager
    async def lock(_computer_id):
        yield acquired

    return lock


def _binding(workspace_id, computer=None, **overrides):
    """The machine handle every lifecycle method now takes in place of an id."""
    row = {**(computer or _STUB_COMPUTER), **overrides}
    return WorkspaceManager._binding_from_computer(str(workspace_id), row)


# ``get_live_workspace_ids_for_computer`` is bound in three modules, and
# ``cm_patch`` only knows about the two inside the computer_manager package.
_LIVE_IDS_MODULES = (_MACHINES, _PROVISIONING, _ENTITLEMENTS)


@contextmanager
def _patch_live_ids(*workspace_ids, side_effect=None):
    """Stub the machine's live-project list in every module that reads it."""
    mock = AsyncMock(return_value=list(workspace_ids))
    if side_effect is not None:
        mock.side_effect = side_effect
    with ExitStack() as stack:
        for module in _LIVE_IDS_MODULES:
            stack.enter_context(
                patch(f"{module}.get_live_workspace_ids_for_computer", new=mock)
            )
        yield mock


def _patch_ws_status(mock=None):
    """``update_workspace_status`` is bound only in the lifecycle module now."""
    return patch(
        f"{_LIFECYCLE}.update_workspace_status",
        new=mock if mock is not None else AsyncMock(),
    )


def _claimed_computer(*workspace_ids, **overrides):
    """What the machine's start CAS returns: the row plus the shadows it reached."""
    return {
        **_STUB_COMPUTER,
        "status": "starting",
        "shadowed_workspace_ids": [str(w) for w in workspace_ids],
        **overrides,
    }


def _patch_resolve(computer):
    """Point the project-to-machine resolve at one row, over the module stub."""
    return patch(
        "src.server.services.computer_manager._sessions.get_computer_for_workspace",
        new=AsyncMock(return_value=dict(computer)),
    )


def _patch_machine_activity(active=False):
    """The machine-wide activity gate fails closed, so it must be answered."""
    executor = MagicMock()
    executor.get_instance.return_value.has_active_tasks_for_computer = AsyncMock(
        return_value=active
    )
    return patch(f"{_LIFECYCLE}.LocalRunExecutor", new=executor)


def _make_mock_session(initialized=True, has_sandbox=True):
    session = MagicMock()
    session._initialized = initialized
    session.sandbox = MagicMock() if has_sandbox else None
    if has_sandbox:
        session.sandbox.sandbox_id = "sandbox-abc"
        session.sandbox.is_ready = MagicMock(return_value=True)
        session.sandbox.ensure_sandbox_ready = AsyncMock()
        session.sandbox.sync_sandbox_assets = AsyncMock()
    session.initialize = AsyncMock()
    session.initialize_lazy = AsyncMock()
    session.stop = AsyncMock()
    session.cleanup = AsyncMock()
    return session


# ---------------------------------------------------------------------------
# Singleton
# ---------------------------------------------------------------------------


class TestSingleton:
    """Test WorkspaceManager singleton pattern."""

    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    def test_get_instance_requires_config_on_first_call(self):
        with pytest.raises(ValueError, match="config is required"):
            WorkspaceManager.get_instance()

    def test_get_instance_creates_singleton(self):
        config = _make_config()
        instance = WorkspaceManager.get_instance(config=config)
        assert instance is not None
        assert isinstance(instance, WorkspaceManager)

    def test_get_instance_returns_same_instance(self):
        config = _make_config()
        first = WorkspaceManager.get_instance(config=config)
        second = WorkspaceManager.get_instance()
        assert first is second

    def test_reset_instance_clears_singleton(self):
        config = _make_config()
        WorkspaceManager.get_instance(config=config)
        WorkspaceManager.reset_instance()
        with pytest.raises(ValueError, match="config is required"):
            WorkspaceManager.get_instance()


# ---------------------------------------------------------------------------
# Init and stats
# ---------------------------------------------------------------------------


class TestInitAndStats:
    """Test initialization and statistics."""

    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    def test_init_sets_defaults(self):
        config = _make_config()
        wm = WorkspaceManager(config, idle_timeout=600, cleanup_interval=60)
        assert wm.idle_timeout == 600
        assert wm.cleanup_interval == 60
        assert wm._machines == {}
        assert wm._shutdown is False

    def test_get_stats_empty(self):
        config = _make_config()
        wm = WorkspaceManager(config)
        stats = wm.get_stats()
        assert stats["cached_sessions"] == 0
        assert stats["cached_computer_ids"] == []
        assert stats["idle_timeout"] == 1800

    def test_get_stats_with_sessions(self):
        """The cache is keyed by machine: two projects sharing one computer are
        one cached session, not two."""
        config = _make_config()
        wm = WorkspaceManager(config)
        wm._machine("computer-1").session = _make_mock_session()
        wm._machine("computer-2").session = _make_mock_session()
        stats = wm.get_stats()
        assert stats["cached_sessions"] == 2
        assert set(stats["cached_computer_ids"]) == {"computer-1", "computer-2"}


# ---------------------------------------------------------------------------
# create_workspace
# ---------------------------------------------------------------------------


class TestCreateWorkspace:
    """Creation returns a bound project and builds nothing.

    The machine is brought up by the first turn, exactly as it is for a project
    that has been sitting stopped, so a create that awaits a sandbox is the
    regression these tests exist to catch.
    """

    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    @staticmethod
    def _created(ws_id, **overrides):
        return _make_workspace(
            workspace_id=ws_id,
            status="stopped",
            sandbox_id=None,
            computer_id=_STUB_COMPUTER_ID,
            dir_name="test-ab12",
            **overrides,
        )

    @pytest.mark.asyncio
    @patch(
        "src.server.services.workspace_manager.create_workspace_on_computer",
        new_callable=AsyncMock,
    )
    @cm_patch("SessionManager")
    async def test_create_returns_a_bound_project_without_provisioning(
        self, mock_sm, mock_insert
    ):
        ws_id = str(uuid.uuid4())
        mock_insert.return_value = self._created(ws_id)

        wm = WorkspaceManager(_make_config())
        result = await wm.create_workspace(
            user_id="user-1", name="Test", description="desc"
        )

        assert result["computer_id"] == _STUB_COMPUTER_ID
        assert result["dir_name"] == "test-ab12"
        # Nothing was built: no session asked for, none cached, no sandbox.
        mock_sm.get_session.assert_not_called()
        assert wm._cached_session(ws_id) is None
        assert wm._machines == {}

    @pytest.mark.asyncio
    @patch(
        "src.server.services.workspace_manager.create_workspace_on_computer",
        new_callable=AsyncMock,
    )
    async def test_the_project_is_inserted_on_the_users_machine(self, mock_insert):
        ws_id = str(uuid.uuid4())
        mock_insert.return_value = self._created(ws_id)

        wm = WorkspaceManager(_make_config())
        await wm.create_workspace(user_id="user-1", name="Test")

        args, kwargs = mock_insert.call_args
        assert args[0] == "user-1"
        assert args[2] == _STUB_COMPUTER_ID

    @pytest.mark.asyncio
    @patch(
        "src.server.services.workspace_manager.create_workspace_on_computer",
        new_callable=AsyncMock,
    )
    async def test_the_new_project_is_inserted_already_bound(self, mock_insert):
        """Creation resolves the machine, and an insert that landed bound means
        that resolve is a read rather than the adoption a 046 row needs."""
        ws_id = str(uuid.uuid4())
        mock_insert.return_value = self._created(ws_id)

        wm = WorkspaceManager(_make_config())
        with patch.object(
            wm, "_adopt_workspace_onto_computer", new_callable=AsyncMock
        ) as adopt:
            await wm.create_workspace(user_id="user-1", name="Test")
            binding = await wm.resolve_binding(ws_id)

        assert binding.computer_id == _STUB_COMPUTER_ID
        adopt.assert_not_awaited()

    @pytest.mark.asyncio
    @patch(
        "src.server.services.computer_manager._machines.create_computer",
        new_callable=AsyncMock,
    )
    @patch(
        "src.server.services.computer_manager._machines.get_primary_computer",
        new_callable=AsyncMock,
    )
    @patch(
        "src.server.services.workspace_manager.create_workspace_on_computer",
        new_callable=AsyncMock,
    )
    async def test_a_user_with_no_machine_gets_their_primary_minted(
        self, mock_insert, mock_primary, mock_create_computer
    ):
        mock_primary.return_value = None
        mock_create_computer.return_value = dict(_STUB_COMPUTER)
        mock_insert.return_value = self._created(str(uuid.uuid4()))

        wm = WorkspaceManager(_make_config())
        await wm.create_workspace(
            user_id="user-1", name="Test", resource_tier="performance"
        )

        kwargs = mock_create_computer.call_args.kwargs
        assert kwargs["is_primary"] is True
        assert kwargs["resource_tier"] == "performance"
        assert kwargs["kind"] == "daytona"
        # Nothing is being built, so the machine is born stopped and the first
        # start claims it the way it claims any stopped machine.
        assert kwargs["status"] == "stopped"

    @pytest.mark.asyncio
    @patch(
        "src.server.services.computer_manager._machines.create_computer",
        new_callable=AsyncMock,
    )
    @patch(
        "src.server.services.computer_manager._machines.get_primary_computer",
        new_callable=AsyncMock,
    )
    @patch(
        "src.server.services.workspace_manager.create_workspace_on_computer",
        new_callable=AsyncMock,
    )
    async def test_a_second_project_joins_the_machine_it_already_has(
        self, mock_insert, mock_primary, mock_create_computer
    ):
        mock_primary.return_value = dict(_STUB_COMPUTER)
        mock_insert.return_value = self._created(str(uuid.uuid4()))

        wm = WorkspaceManager(_make_config())
        await wm.create_workspace(user_id="user-1", name="Second")

        mock_create_computer.assert_not_awaited()
        assert mock_insert.call_args[0][2] == _STUB_COMPUTER_ID

    @pytest.mark.asyncio
    @patch(
        "src.server.services.computer_manager._machines.update_computer_status",
        new_callable=AsyncMock,
    )
    @patch(
        "src.server.services.computer_manager._machines.create_computer",
        new_callable=AsyncMock,
    )
    @patch(
        "src.server.services.computer_manager._machines.get_primary_computer",
        new_callable=AsyncMock,
    )
    @patch(
        "src.server.services.workspace_manager.create_workspace_on_computer",
        new_callable=AsyncMock,
    )
    async def test_losing_the_primary_race_tombstones_the_loser(
        self, mock_insert, mock_primary, mock_create_computer, mock_status
    ):
        """Two creates at once must not leave a spare machine to hand out later."""
        winner = {**_STUB_COMPUTER, "computer_id": str(uuid.uuid4())}
        loser_id = str(uuid.uuid4())
        mock_primary.side_effect = [None, winner]
        mock_create_computer.return_value = {
            **_STUB_COMPUTER,
            "computer_id": loser_id,
            "is_primary": False,
        }
        mock_insert.return_value = self._created(str(uuid.uuid4()))

        wm = WorkspaceManager(_make_config())
        await wm.create_workspace(user_id="user-1", name="Test")

        mock_status.assert_awaited_once_with(loser_id, "deleted")
        assert mock_insert.call_args[0][2] == winner["computer_id"]

    @pytest.mark.asyncio
    @patch(
        "src.server.services.workspace_manager.create_workspace_on_computer",
        new_callable=AsyncMock,
    )
    async def test_a_machine_that_vanished_mid_create_is_reported(self, mock_insert):
        """Returning None here means the insert wrote nothing, so answering 201
        would hand the caller a workspace id that names no row."""
        mock_insert.return_value = None

        wm = WorkspaceManager(_make_config())
        with pytest.raises(RuntimeError, match=_STUB_COMPUTER_ID):
            await wm.create_workspace(user_id="user-1", name="Test")


# ---------------------------------------------------------------------------
# stop_workspace
# ---------------------------------------------------------------------------


class TestStopWorkspace:
    """Test workspace stopping."""

    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    @pytest.mark.asyncio
    @cm_patch("db_get_workspace", new_callable=AsyncMock)
    @patch(f"{_PROVISIONING}.FilePersistenceService")
    async def test_stop_running_workspace(self, mock_file_svc, mock_db_get):
        ws_id = str(uuid.uuid4())
        mock_db_get.return_value = _make_workspace(workspace_id=ws_id, status="stopped")
        mock_file_svc.sync_to_db = AsyncMock(return_value={"synced": 1, "errors": 0})

        config = _make_config()
        wm = WorkspaceManager(config)
        mock_session = _make_mock_session()
        wm._machine(_STUB_COMPUTER_ID).session = mock_session
        wm._machine(_STUB_COMPUTER_ID).last_sync_at = time.monotonic()

        with (
            _patch_machine_activity(),
            _patch_live_ids(ws_id),
            patch(f"{_LIFECYCLE}.get_computer", AsyncMock(return_value=_STUB_COMPUTER)),
            patch(f"{_MACHINES}.get_computer", AsyncMock(return_value=_STUB_COMPUTER)),
            patch(
                f"{_MACHINES}.update_computer_status",
                AsyncMock(return_value=dict(_STUB_COMPUTER)),
            ),
        ):
            result = await wm.stop_workspace(ws_id)

        assert result["status"] == "stopped"
        mock_session.stop.assert_awaited_once()
        assert wm._cached_session(_STUB_COMPUTER_ID) is None
        assert wm._machine(_STUB_COMPUTER_ID).last_sync_at is None

    @pytest.mark.asyncio
    async def test_stop_a_machine_that_is_gone_raises(self):
        """The machine is the lifecycle authority, so its absence is the error
        a stop reports; the project row is only the handle that asked."""
        config = _make_config()
        wm = WorkspaceManager(config)

        with (
            _patch_machine_activity(),
            patch(f"{_MACHINES}.get_computer", AsyncMock(return_value=None)),
        ):
            with pytest.raises(ValueError, match="not found"):
                await wm.stop_workspace(str(uuid.uuid4()))

    @pytest.mark.asyncio
    @cm_patch("db_get_workspace", new_callable=AsyncMock)
    async def test_stopping_an_already_stopped_machine_is_not_an_error(
        self, mock_db_get
    ):
        """A settled machine has nothing to stop, and saying so is the answer.

        Raising here made every idle sweep over a machine a peer had already
        stopped count as a failure, which walked the reaper into its
        three-strike backoff over work that did not need doing.
        """
        ws_id = str(uuid.uuid4())
        stopped = {**_STUB_COMPUTER, "status": "stopped"}
        mock_db_get.return_value = _make_workspace(workspace_id=ws_id, status="stopped")
        wm = WorkspaceManager(_make_config())

        with (
            _patch_machine_activity(),
            patch(f"{_MACHINES}.get_computer", AsyncMock(return_value=stopped)),
            patch(
                f"{_MACHINES}.update_computer_status", new_callable=AsyncMock
            ) as mock_status,
            _patch_resolve(stopped),
        ):
            result = await wm.stop_workspace(ws_id)

        assert result["status"] == "stopped"
        mock_status.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_stop_a_machine_mid_transition_raises(self):
        """A state no stop can leave is the refusal that survived the merge of
        the pre-check into the claim."""
        starting = {**_STUB_COMPUTER, "status": "starting"}
        wm = WorkspaceManager(_make_config())

        with (
            _patch_machine_activity(),
            patch(f"{_MACHINES}.get_computer", AsyncMock(return_value=starting)),
            _patch_resolve(starting),
        ):
            with pytest.raises(RuntimeError, match="Cannot stop"):
                await wm.stop_workspace(str(uuid.uuid4()))


# ---------------------------------------------------------------------------
# _identity_is_stale
# ---------------------------------------------------------------------------


class TestIdentityIsStale:
    """The predicate guarding every cached-session return.

    Postgres owns the workspace↔sandbox binding; this worker's cache is only a
    handle. Getting this wrong in either direction is expensive: too permissive
    and a deleted sandbox keeps serving 404s, too strict and a live workspace
    retires its session on every single request.
    """

    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    @staticmethod
    def _check(identity, *, local_id="sandbox-abc", ready=True, owns_lazy_init=False):
        wm = WorkspaceManager(_make_config())
        session = _make_mock_session()
        session.sandbox.sandbox_id = local_id
        session.sandbox.is_ready = MagicMock(return_value=ready)
        if owns_lazy_init:
            wm._machine(_STUB_COMPUTER_ID).pending_lazy_sync = True
        return wm._identity_is_stale(_binding("ws-1"), session, identity)

    def test_agreeing_running_row_is_served(self):
        assert self._check({"status": "running", "sandbox_id": "sandbox-abc"}) is None

    @pytest.mark.asyncio
    async def test_a_flash_project_never_reaches_the_predicate(self):
        """Flash is not a serving status here any more; it is not a status this
        predicate can see. A flash project has no machine, so resolving its
        binding refuses it before any session is looked up.
        """
        from src.server.services.computer_manager._types import (
            WorkspaceNotOnComputer,
        )

        wm = WorkspaceManager(_make_config())
        flash = _make_workspace(status="flash", sandbox_id=None)
        with (
            patch(
                "src.server.services.computer_manager._sessions.get_computer_for_workspace",
                AsyncMock(return_value=None),
            ),
            patch(f"{_MACHINES}.db_get_workspace", AsyncMock(return_value=flash)),
        ):
            with pytest.raises(WorkspaceNotOnComputer):
                await wm.resolve_binding(str(flash["workspace_id"]))

    def test_missing_row_is_stale(self):
        assert "row is gone" in self._check(None)

    def test_identity_moved_is_stale(self):
        reason = self._check({"status": "running", "sandbox_id": "sandbox-new"})
        assert "sandbox identity moved" in reason

    @pytest.mark.parametrize("status", ["deleted", "error", "stopped", "stopping"])
    def test_non_serving_status_is_stale_even_when_ids_agree(self, status):
        """A stop leaves ``sandbox_id`` untouched, so the ids still agree while
        the sandbox they name is being torn down — status is the only signal."""
        reason = self._check({"status": status, "sandbox_id": "sandbox-abc"})
        assert repr(status) in reason

    def test_starting_without_owning_the_lazy_init_is_stale(self):
        """'starting' on a worker that owns no lazy init means someone else
        claimed this workspace for replacement — our handle names a doomed
        sandbox. Readiness is irrelevant; ownership is the whole signal."""
        reason = self._check({"status": "starting", "sandbox_id": "sandbox-abc"})
        assert "'starting'" in reason

    @pytest.mark.parametrize("ready", [False, True])
    def test_starting_while_owning_the_lazy_init_is_served(self, ready):
        """The worker running the lazy init sees its own claim, and keeps
        seeing it after its sandbox goes ready.

        Phase 2 runs outside the lock, so the owner's sandbox is ready for the
        whole sync while the row is still 'starting'. Retiring there drops the
        record's ``pending_lazy_sync`` flag, which gates both the promotion
        and its revert, leaving the row wedged in 'starting' until the reaper.
        """
        assert (
            self._check(
                {"status": "starting", "sandbox_id": "sandbox-abc"},
                ready=ready,
                owns_lazy_init=True,
            )
            is None
        )

    @pytest.mark.parametrize(
        "db_id,local_id", [("sandbox-abc", None), (None, "sandbox-abc"), (None, None)]
    )
    def test_half_known_binding_is_stale(self, db_id, local_id):
        """A one-sided binding is itself an inconsistency. Treating it as
        "can't tell, assume fine" is how a deleted sandbox goes on serving
        indefinitely."""
        reason = self._check(
            {"status": "running", "sandbox_id": db_id}, local_id=local_id
        )
        if db_id == local_id:
            assert reason is None
        else:
            assert "sandbox identity moved" in reason

    def test_initialized_session_without_a_sandbox_is_stale(self):
        """``SessionManager`` outlives the machine record's session, so an
        initialized session with no sandbox can be handed back for a bound
        workspace."""
        wm = WorkspaceManager(_make_config())
        session = _make_mock_session(has_sandbox=False)
        reason = wm._identity_is_stale(
            _binding("ws-1"),
            session,
            {"status": "running", "sandbox_id": "sandbox-abc"},
        )
        assert "sandbox identity moved" in reason


# ---------------------------------------------------------------------------
# backup_project_files strict mode
# ---------------------------------------------------------------------------


def _patch_backup_identity(sandbox_id="sandbox-abc"):
    """Stub the durable-identity read ``backup_project_files`` validates against."""
    return cm_patch(
        "db_get_workspace_identity",
        AsyncMock(return_value={"status": "running", "sandbox_id": sandbox_id}),
    )


class TestBackupFilesStrict:
    """strict=True turns the best-effort backup into a hard precondition for
    callers about to destroy the sandbox (spec-change recreate)."""

    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    @staticmethod
    def _backup(wm, workspace_id, **kwargs):
        """The mirror is per project, but the session it reads is the machine's."""
        return wm.backup_project_files(
            workspace_id, computer_id=_STUB_COMPUTER_ID, **kwargs
        )

    @pytest.mark.asyncio
    async def test_strict_raises_without_session(self):
        wm = WorkspaceManager(_make_config())
        ws_id = str(uuid.uuid4())

        with pytest.raises(RuntimeError, match="No attached session"):
            await self._backup(wm, ws_id, strict=True)

    @pytest.mark.asyncio
    @patch(f"{_PROVISIONING}.FilePersistenceService")
    async def test_strict_raises_on_sync_failure(self, mock_file_svc):
        wm = WorkspaceManager(_make_config())
        ws_id = str(uuid.uuid4())
        wm._machine(_STUB_COMPUTER_ID).session = _make_mock_session()
        mock_file_svc.sync_to_db = AsyncMock(side_effect=OSError("disk detached"))

        with _patch_backup_identity():
            with pytest.raises(RuntimeError, match="aborting before sandbox teardown"):
                await self._backup(wm, ws_id, strict=True)

    @pytest.mark.asyncio
    @patch(f"{_PROVISIONING}.FilePersistenceService")
    async def test_default_stays_best_effort(self, mock_file_svc):
        wm = WorkspaceManager(_make_config())
        ws_id = str(uuid.uuid4())
        wm._machine(_STUB_COMPUTER_ID).session = _make_mock_session()
        mock_file_svc.sync_to_db = AsyncMock(side_effect=OSError("disk detached"))

        with _patch_backup_identity():
            await self._backup(wm, ws_id)  # warns, does not raise

    @pytest.mark.asyncio
    @patch(f"{_PROVISIONING}.FilePersistenceService")
    async def test_strict_aborts_when_sync_leaves_files_unsaved(self, mock_file_svc):
        """``sync_to_db`` counts per-file failures instead of raising, so a clean
        return is not proof the backup is complete. The strict caller is about to
        delete the sandbox, which makes a nonzero count unrecoverable data loss.
        """
        wm = WorkspaceManager(_make_config())
        ws_id = str(uuid.uuid4())
        wm._machine(_STUB_COMPUTER_ID).session = _make_mock_session()
        mock_file_svc.sync_to_db = AsyncMock(return_value={"synced": 3, "errors": 2})

        with _patch_backup_identity():
            with pytest.raises(RuntimeError, match="left 2 file\\(s\\) unsaved"):
                await self._backup(wm, ws_id, strict=True)

    @pytest.mark.asyncio
    @patch(f"{_PROVISIONING}.FilePersistenceService")
    async def test_refuses_a_session_bound_to_a_superseded_sandbox(self, mock_file_svc):
        """``sync_to_db`` OVERWRITES the workspace's durable file copy, so running
        it from a stale handle destroys the good copy as well as missing the live
        files. The check is unconditional: making it opt-in is what left two of
        five call sites unprotected.
        """
        wm = WorkspaceManager(_make_config())
        ws_id = str(uuid.uuid4())
        wm._machine(
            _STUB_COMPUTER_ID
        ).session = _make_mock_session()  # on 'sandbox-abc'
        mock_file_svc.sync_to_db = AsyncMock(return_value={"synced": 1, "errors": 0})

        with _patch_backup_identity(sandbox_id="sandbox-REPLACED"):
            await self._backup(wm, ws_id)  # best-effort: warns, no raise
            mock_file_svc.sync_to_db.assert_not_awaited()

            with pytest.raises(RuntimeError, match="stale session"):
                await self._backup(wm, ws_id, strict=True)
            mock_file_svc.sync_to_db.assert_not_awaited()

    @pytest.mark.asyncio
    @patch(f"{_PROVISIONING}.FilePersistenceService")
    async def test_a_caller_naming_only_the_project_reaches_the_machine(
        self, mock_file_svc
    ):
        """The post-turn mirror holds no binding, and the session it has to read
        is the machine's. Demanding the machine id of every caller made that one
        fail on its own signature, so no turn's files were mirrored until stop.
        """
        wm = WorkspaceManager(_make_config())
        ws_id = str(uuid.uuid4())
        wm._machine(_STUB_COMPUTER_ID).session = _make_mock_session()
        wm.resolve_binding = AsyncMock(
            return_value=SimpleNamespace(computer_id=_STUB_COMPUTER_ID)
        )
        mock_file_svc.sync_to_db = AsyncMock(return_value={"synced": 1, "errors": 0})

        with _patch_backup_identity():
            await wm.backup_project_files(ws_id)

        wm.resolve_binding.assert_awaited_once_with(ws_id)
        mock_file_svc.sync_to_db.assert_awaited_once()

    @pytest.mark.asyncio
    @patch(f"{_PROVISIONING}.FilePersistenceService")
    async def test_caller_supplied_identity_skips_the_read(self, mock_file_svc):
        """``expected_sandbox_id`` is an optimization for callers holding the row,
        not the contract - the guard runs either way."""
        wm = WorkspaceManager(_make_config())
        ws_id = str(uuid.uuid4())
        wm._machine(_STUB_COMPUTER_ID).session = _make_mock_session()
        mock_file_svc.sync_to_db = AsyncMock(return_value={"synced": 1, "errors": 0})

        identity = AsyncMock()
        with cm_patch("db_get_workspace_identity", identity):
            await self._backup(wm, ws_id, expected_sandbox_id="sandbox-abc")

        identity.assert_not_awaited()
        mock_file_svc.sync_to_db.assert_awaited_once()

    @pytest.mark.asyncio
    @patch(f"{_PROVISIONING}.FilePersistenceService")
    async def test_the_machine_wide_mirror_covers_every_sibling(self, mock_file_svc):
        """One runtime ends for all of them, so the project that asked is not the
        unit of data loss; each sibling is fenced by the machine's durable ref."""
        wm = WorkspaceManager(_make_config())
        asked, sibling = str(uuid.uuid4()), str(uuid.uuid4())
        wm._machine(_STUB_COMPUTER_ID).session = _make_mock_session()
        mock_file_svc.sync_to_db = AsyncMock(return_value={"synced": 1, "errors": 0})

        with _patch_live_ids(asked, sibling):
            await wm._backup_machine_files_to_db(
                _STUB_COMPUTER_ID,
                workspace_id=asked,
                expected_sandbox_id="sandbox-abc",
            )

        assert [c.args[0] for c in mock_file_svc.sync_to_db.await_args_list] == [
            asked,
            sibling,
        ]

    @pytest.mark.asyncio
    @patch(f"{_PROVISIONING}.FilePersistenceService")
    async def test_a_lost_sibling_list_refuses_a_strict_teardown(self, mock_file_svc):
        """Without the list the unmirrored set is unknown, so "no failures" is
        not the same as "everything is saved"."""
        wm = WorkspaceManager(_make_config())
        asked = str(uuid.uuid4())
        wm._machine(_STUB_COMPUTER_ID).session = _make_mock_session()
        mock_file_svc.sync_to_db = AsyncMock(return_value={"synced": 1, "errors": 0})

        with _patch_live_ids(side_effect=RuntimeError("no pool")):
            with pytest.raises(RuntimeError, match="unmirrored"):
                await wm._backup_machine_files_to_db(
                    _STUB_COMPUTER_ID,
                    workspace_id=asked,
                    expected_sandbox_id="sandbox-abc",
                    strict=True,
                )


# ---------------------------------------------------------------------------
# delete_workspace
# ---------------------------------------------------------------------------


class TestDeleteWorkspace:
    """Test workspace deletion."""

    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    @pytest.mark.asyncio
    @patch(
        "src.server.services.workspace_manager.db_delete_workspace",
        new_callable=AsyncMock,
    )
    @cm_patch("db_get_workspace", new_callable=AsyncMock)
    @patch(f"{_PROVISIONING}.FilePersistenceService")
    async def test_delete_workspace_success(
        self, mock_file_svc, mock_db_get, mock_db_delete
    ):
        """The project is mirrored, tombstoned and unlinked; the machine under
        it is the user's primary, so its session and sandbox stay."""
        ws_id = str(uuid.uuid4())
        mock_db_get.return_value = _make_workspace(
            workspace_id=ws_id, status="running", dir_name="test-ab12"
        )
        mock_file_svc.sync_to_db = AsyncMock(return_value={"synced": 1, "errors": 0})

        config = _make_config()
        wm = WorkspaceManager(config)
        mock_session = _make_mock_session()
        wm._machine(_STUB_COMPUTER_ID).session = mock_session
        wm._session_computer[ws_id] = _STUB_COMPUTER_ID
        wm._machine_decision_lock = _decision_lock(True)
        wm._remove_workspace_folder = AsyncMock()
        wm._teardown_machine = AsyncMock()

        with (
            patch(f"{_MACHINES}.get_computer", AsyncMock(return_value=_STUB_COMPUTER)),
            _patch_live_ids(),
        ):
            result = await wm.delete_workspace(ws_id)

        assert result is True
        mock_db_delete.assert_awaited_once_with(ws_id)
        wm._remove_workspace_folder.assert_awaited_once()
        wm._teardown_machine.assert_not_awaited()
        # The session belongs to the computer and outlives the project; only the
        # project's own index entry goes.
        assert wm._cached_session(_STUB_COMPUTER_ID) is mock_session
        assert ws_id not in wm._session_computer

    @pytest.mark.asyncio
    @cm_patch("db_get_workspace", new_callable=AsyncMock)
    async def test_delete_workspace_not_found_raises(self, mock_db_get):
        mock_db_get.return_value = None
        config = _make_config()
        wm = WorkspaceManager(config)

        with pytest.raises(ValueError, match="not found"):
            await wm.delete_workspace("nonexistent")


class TestDeleteOnASharedMachine:
    """Deleting a project takes the project, never the machine under a sibling.

    The sandbox is the computer's and a sibling may be mid-turn on it, so the
    only delete that ends a machine is the one that leaves nothing on it.
    """

    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    @staticmethod
    def _manager():
        wm = WorkspaceManager(_make_config())
        wm._teardown_machine = AsyncMock()
        wm._remove_workspace_folder = AsyncMock()
        wm.backup_project_files = AsyncMock()
        wm._machine_decision_lock = _decision_lock(True)
        return wm

    @pytest.mark.asyncio
    @patch(
        "src.server.services.workspace_manager.db_delete_workspace",
        new_callable=AsyncMock,
    )
    @patch(f"{_MACHINES}.update_computer_status", new_callable=AsyncMock)
    @patch(f"{_MACHINES}.get_computer", new_callable=AsyncMock)
    @cm_patch("db_get_workspace", new_callable=AsyncMock)
    async def test_a_sibling_keeps_the_sandbox(
        self, mock_db_get, mock_get_computer, mock_status, mock_db_delete
    ):
        computer = _make_computer(computer_id=_STUB_COMPUTER_ID)
        computer_id = _STUB_COMPUTER_ID
        ws_id = str(uuid.uuid4())
        mock_db_get.return_value = _make_workspace(
            workspace_id=ws_id, computer_id=computer_id, dir_name="alpha-1a2b"
        )
        mock_get_computer.return_value = computer

        wm = self._manager()
        lock = wm._machine_lock(computer_id)

        with _patch_live_ids("sibling-ws"):
            assert await wm.delete_workspace(ws_id) is True

        wm._teardown_machine.assert_not_awaited()
        mock_status.assert_not_awaited()
        wm._remove_workspace_folder.assert_awaited_once()
        wm.backup_project_files.assert_awaited_once()
        # The lock is the machine's, so a sibling's next call must find the
        # same object rather than build a second one beside the holder.
        assert wm._machine_lock(computer_id) is lock

    @pytest.mark.asyncio
    @patch(
        "src.server.services.workspace_manager.db_delete_workspace",
        new_callable=AsyncMock,
    )
    @patch(f"{_MACHINES}.update_computer_status", new_callable=AsyncMock)
    @patch(f"{_MACHINES}.get_computer", new_callable=AsyncMock)
    @cm_patch("db_get_workspace", new_callable=AsyncMock)
    async def test_the_last_project_on_a_non_primary_ends_the_machine(
        self, mock_db_get, mock_get_computer, mock_status, mock_db_delete
    ):
        computer = _make_computer(computer_id=_STUB_COMPUTER_ID, is_primary=False)
        computer_id = _STUB_COMPUTER_ID
        ws_id = str(uuid.uuid4())
        workspace = _make_workspace(
            workspace_id=ws_id, computer_id=computer_id, dir_name="alpha-1a2b"
        )
        mock_db_get.return_value = workspace
        mock_get_computer.return_value = computer

        wm = self._manager()

        with _patch_live_ids():
            assert await wm.delete_workspace(ws_id) is True

        wm._teardown_machine.assert_awaited_once_with(_binding(ws_id, computer))
        mock_status.assert_awaited_once_with(computer_id, "deleted")
        # Nothing is unlinked from a disk that has just been destroyed.
        wm._remove_workspace_folder.assert_not_awaited()
        assert wm._machine_if_known(computer_id) is None

    @pytest.mark.asyncio
    @patch(
        "src.server.services.workspace_manager.db_delete_workspace",
        new_callable=AsyncMock,
    )
    @patch(f"{_MACHINES}.update_computer_status", new_callable=AsyncMock)
    @patch(f"{_MACHINES}.get_computer", new_callable=AsyncMock)
    @cm_patch("db_get_workspace", new_callable=AsyncMock)
    async def test_the_last_project_on_the_primary_keeps_the_machine(
        self, mock_db_get, mock_get_computer, mock_status, mock_db_delete
    ):
        computer = _make_computer(computer_id=_STUB_COMPUTER_ID, is_primary=True)
        ws_id = str(uuid.uuid4())
        mock_db_get.return_value = _make_workspace(
            workspace_id=ws_id, computer_id=_STUB_COMPUTER_ID, dir_name="alpha-1a2b"
        )
        mock_get_computer.return_value = computer

        wm = self._manager()

        with _patch_live_ids():
            assert await wm.delete_workspace(ws_id) is True

        wm._teardown_machine.assert_not_awaited()
        mock_status.assert_not_awaited()
        wm._remove_workspace_folder.assert_awaited_once()

    @pytest.mark.asyncio
    @patch(
        "src.server.services.workspace_manager.db_delete_workspace",
        new_callable=AsyncMock,
    )
    @patch(f"{_MACHINES}.update_computer_status", new_callable=AsyncMock)
    @patch(f"{_MACHINES}.get_computer", new_callable=AsyncMock)
    @cm_patch("db_get_workspace", new_callable=AsyncMock)
    async def test_two_deletes_of_the_last_two_retire_the_machine_once(
        self, mock_db_get, mock_get_computer, mock_status, mock_db_delete
    ):
        """Emptiness is read after the row is tombstoned and under the machine's
        key, so the pair cannot both read the other as live and both walk away,
        and the tombstone's own fence stops the loser retiring it twice."""
        computer = _make_computer(computer_id=_STUB_COMPUTER_ID, is_primary=False)
        computer_id = _STUB_COMPUTER_ID
        first, second = str(uuid.uuid4()), str(uuid.uuid4())
        rows = {
            first: _make_workspace(
                workspace_id=first, computer_id=computer_id, dir_name="alpha-1a2b"
            ),
            second: _make_workspace(
                workspace_id=second, computer_id=computer_id, dir_name="beta-3c4d"
            ),
        }
        live = [first, second]

        mock_db_get.side_effect = lambda ws_id, *a, **k: rows[ws_id]
        mock_db_delete.side_effect = lambda ws_id, *a, **k: live.remove(ws_id) or True
        mock_get_computer.side_effect = (
            lambda *a, **k: computer if mock_status.await_count == 0 else None
        )

        wm = self._manager()

        # Both rows are gone by the time either decision runs, which is what
        # the tombstone-before-the-key ordering guarantees.
        with _patch_live_ids(side_effect=lambda *a, **k: list(live)):
            await asyncio.gather(
                wm.delete_workspace(first), wm.delete_workspace(second)
            )

        assert wm._teardown_machine.await_count == 1
        mock_status.assert_awaited_once_with(computer_id, "deleted")


# ---------------------------------------------------------------------------
# cleanup_idle_workspaces
# ---------------------------------------------------------------------------


class TestCleanupIdle:
    """The idle sweep walks machines, because the sandbox is the machine's.

    One idle project on a busy computer is not a reason to take the computer
    away, and a computer carrying five idle projects is stopped once rather
    than five times. These stub the stop; what a lagging project row is allowed
    to decide is in ``TestTheIdleSweepAndALaggingShadow`` below.
    """

    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    @pytest.fixture(autouse=True)
    def _quiet_durable_probes(self):
        """The reaper's activity guard also reads the run ledgers, now over the
        whole machine; keep both quiet so these tests exercise only the
        idle-timeout mechanics."""
        with (
            patch(
                "src.server.database.runs.lifecycle.computer_has_active_run",
                new=AsyncMock(return_value=False),
            ),
            patch(
                "src.server.database.runs.subagent_runs.count_open_runs_for_computer",
                new=AsyncMock(return_value=0),
            ),
        ):
            yield

    @pytest.mark.asyncio
    @patch(
        "src.server.services.workspace_manager.get_computers_by_status",
        new_callable=AsyncMock,
    )
    async def test_an_idle_machine_is_stopped_once(self, mock_computers):
        """The machine is addressed directly, so the count of projects on it
        cannot turn into a count of stops that would fight each other."""
        computer_id = str(uuid.uuid4())
        mock_computers.return_value = [
            _make_computer(
                computer_id=computer_id,
                last_activity_at=datetime.now(timezone.utc) - timedelta(hours=2),
            )
        ]

        wm = WorkspaceManager(_make_config(), idle_timeout=1800)
        with patch.object(wm, "_stop_machine", new_callable=AsyncMock) as mock_stop:
            count = await wm.cleanup_idle_workspaces()

        assert count == 1
        mock_stop.assert_awaited_once_with(computer_id)
        mock_computers.assert_awaited_once_with("running", limit=1000)

    @pytest.mark.asyncio
    @patch(
        "src.server.services.workspace_manager.get_computers_by_status",
        new_callable=AsyncMock,
    )
    async def test_a_recently_used_machine_is_left_alone(self, mock_computers):
        """The stamp is the computer's, so any project's turn refreshes it."""
        mock_computers.return_value = [
            _make_computer(last_activity_at=datetime.now(timezone.utc))
        ]

        wm = WorkspaceManager(_make_config(), idle_timeout=1800)
        with patch.object(wm, "_stop_machine", new_callable=AsyncMock) as mock_stop:
            count = await wm.cleanup_idle_workspaces()

        assert count == 0
        mock_stop.assert_not_awaited()

    @pytest.mark.asyncio
    @patch(
        "src.server.services.workspace_manager.get_computers_by_status",
        new_callable=AsyncMock,
    )
    async def test_a_never_used_machine_is_left_alone(self, mock_computers):
        mock_computers.return_value = [_make_computer(last_activity_at=None)]

        wm = WorkspaceManager(_make_config(), idle_timeout=1800)
        with patch.object(wm, "_stop_machine", new_callable=AsyncMock) as mock_stop:
            count = await wm.cleanup_idle_workspaces()

        assert count == 0
        mock_stop.assert_not_awaited()

    @pytest.mark.asyncio
    @patch(
        "src.server.services.workspace_manager.get_computers_by_status",
        new_callable=AsyncMock,
    )
    async def test_a_turn_anywhere_on_the_machine_holds_it_open(self, mock_computers):
        """Stopping it takes every project down, so one busy project is enough
        to veto even when the machine's own stamp has gone quiet."""
        computer_id = str(uuid.uuid4())
        mock_computers.return_value = [
            _make_computer(
                computer_id=computer_id,
                last_activity_at=datetime.now(timezone.utc) - timedelta(hours=2),
            )
        ]

        wm = WorkspaceManager(_make_config(), idle_timeout=1800)
        with (
            patch.object(
                wm, "_machine_has_active_tasks", new=AsyncMock(return_value=True)
            ) as mock_active,
            patch.object(wm, "_stop_machine", new_callable=AsyncMock) as mock_stop,
        ):
            count = await wm.cleanup_idle_workspaces()

        assert count == 0
        mock_stop.assert_not_awaited()
        # Asked about the machine, not about a project borrowed as its handle.
        mock_active.assert_awaited_once_with(computer_id)

    # --- always-on entitlement reconciliation (bundled into the idle sweep) ---

    @pytest.mark.asyncio
    @patch(
        "src.server.dependencies.usage_limits.always_on_entitlement_lost",
        new_callable=AsyncMock,
    )
    @patch(
        "src.server.services.workspace_manager.get_computers_by_status",
        new_callable=AsyncMock,
    )
    async def test_always_on_entitled_stays_exempt(self, mock_computers, mock_lost):
        """A long-idle always-on machine whose owner is still entitled is
        neither disabled nor reaped — the idle exemption holds."""
        mock_computers.return_value = [
            _make_computer(
                is_always_on=True,
                last_activity_at=datetime.now(timezone.utc) - timedelta(hours=2),
            )
        ]
        mock_lost.return_value = False

        wm = WorkspaceManager(_make_config(), idle_timeout=1800)
        with (
            patch.object(wm, "_stop_machine", new_callable=AsyncMock) as mock_stop,
            patch.object(
                wm, "set_computer_always_on", new_callable=AsyncMock
            ) as mock_disable,
        ):
            count = await wm.cleanup_idle_workspaces()

        assert count == 0
        mock_stop.assert_not_awaited()
        mock_disable.assert_not_awaited()

    @pytest.mark.asyncio
    @patch(
        "src.server.dependencies.usage_limits.always_on_entitlement_lost",
        new_callable=AsyncMock,
    )
    @patch(
        "src.server.services.workspace_manager.get_computers_by_status",
        new_callable=AsyncMock,
    )
    async def test_always_on_lost_idle_disables_the_machine_and_reaps_it(
        self, mock_computers, mock_lost
    ):
        """Always-on is the machine's flag, so losing it clears it on the
        computer and the now-normal machine is reaped in the same tick."""
        computer_id = str(uuid.uuid4())
        mock_computers.return_value = [
            _make_computer(
                computer_id=computer_id,
                is_always_on=True,
                last_activity_at=datetime.now(timezone.utc) - timedelta(hours=2),
            )
        ]
        mock_lost.return_value = True

        wm = WorkspaceManager(_make_config(), idle_timeout=1800)
        with (
            patch.object(wm, "_stop_machine", new_callable=AsyncMock) as mock_stop,
            patch.object(
                wm, "set_computer_always_on", new_callable=AsyncMock
            ) as mock_disable,
        ):
            count = await wm.cleanup_idle_workspaces()

        mock_disable.assert_awaited_once_with(computer_id, False)
        mock_stop.assert_awaited_once_with(computer_id)
        assert count == 1

    @pytest.mark.asyncio
    @patch(
        "src.server.dependencies.usage_limits.always_on_entitlement_lost",
        new_callable=AsyncMock,
    )
    @patch(
        "src.server.services.workspace_manager.get_computers_by_status",
        new_callable=AsyncMock,
    )
    async def test_always_on_lost_in_use_disables_without_stopping(
        self, mock_computers, mock_lost
    ):
        """Clearing the flag must not yank a machine out from under a user
        mid-use; it idle-stops on a later tick like any other."""
        computer_id = str(uuid.uuid4())
        mock_computers.return_value = [
            _make_computer(
                computer_id=computer_id,
                is_always_on=True,
                last_activity_at=datetime.now(timezone.utc),
            )
        ]
        mock_lost.return_value = True

        wm = WorkspaceManager(_make_config(), idle_timeout=1800)
        with (
            patch.object(wm, "_stop_machine", new_callable=AsyncMock) as mock_stop,
            patch.object(
                wm, "set_computer_always_on", new_callable=AsyncMock
            ) as mock_disable,
        ):
            count = await wm.cleanup_idle_workspaces()

        mock_disable.assert_awaited_once_with(computer_id, False)
        mock_stop.assert_not_awaited()
        assert count == 0

    @pytest.mark.asyncio
    @patch(
        "src.server.dependencies.usage_limits.always_on_entitlement_lost",
        new_callable=AsyncMock,
    )
    @patch(
        "src.server.services.workspace_manager.get_computers_by_status",
        new_callable=AsyncMock,
    )
    async def test_the_entitlement_is_validated_once_per_owner(
        self, mock_computers, mock_lost
    ):
        """Two always-on machines for one user trigger a single platform
        validate, not one per machine."""
        mock_computers.return_value = [
            _make_computer(
                user_id="user-1",
                is_always_on=True,
                last_activity_at=datetime.now(timezone.utc) - timedelta(hours=2),
            ),
            _make_computer(
                user_id="user-1",
                is_always_on=True,
                last_activity_at=datetime.now(timezone.utc) - timedelta(hours=2),
            ),
        ]
        mock_lost.return_value = True

        wm = WorkspaceManager(_make_config(), idle_timeout=1800)
        with (
            patch.object(wm, "_stop_machine", new_callable=AsyncMock),
            patch.object(
                wm, "set_computer_always_on", new_callable=AsyncMock
            ) as mock_disable,
        ):
            await wm.cleanup_idle_workspaces()

        mock_lost.assert_awaited_once_with("user-1")
        assert mock_disable.await_count == 2


class TestTheIdleSweepAndALaggingShadow:
    """A project row that lags its computer must not be what decides the stop.

    Reproduced live on wt3: one row left at 'stopped' under a running computer
    (a fenced shadow write, or a rebind that moved the binding without the
    lifecycle columns) made the sweep raise every five minutes, forever. The
    machine was never stopped and kept billing, and the project could not start
    either, because the start claim wants the machine 'stopped'.
    """

    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    @pytest.mark.asyncio
    @patch(
        "src.server.services.workspace_manager.get_computers_by_status",
        new_callable=AsyncMock,
    )
    async def test_a_lagging_project_row_does_not_refuse_the_machines_stop(
        self, mock_computers
    ):
        computer_id = str(uuid.uuid4())
        ws_id = str(uuid.uuid4())
        computer = _make_computer(
            computer_id=computer_id,
            last_activity_at=datetime.now(timezone.utc) - timedelta(hours=2),
        )
        mock_computers.return_value = [computer]
        # Present, and deliberately disagreeing with its machine: the stop must
        # read the computer and never take this row's word for the state.
        lagging = _make_workspace(
            workspace_id=ws_id, status="stopped", computer_id=computer_id
        )

        transitions = []

        async def _move(cid, status, *, expected=None, **_kwargs):
            transitions.append((cid, status, expected))
            return {**computer, "status": status, "shadowed_workspace_ids": [ws_id]}

        wm = WorkspaceManager(_make_config(), idle_timeout=1800)
        with (
            cm_patch("db_get_workspace", AsyncMock(return_value=lagging)),
            _patch_live_ids(ws_id),
            patch(f"{_LIFECYCLE}.get_computer", AsyncMock(return_value=computer)),
            patch(f"{_MACHINES}.get_computer", AsyncMock(return_value=computer)),
            patch(
                f"{_MACHINES}.update_computer_status",
                AsyncMock(side_effect=_move),
            ),
            patch.object(
                wm, "_machine_has_active_tasks", AsyncMock(return_value=False)
            ),
            patch.object(wm, "backup_project_files", AsyncMock()),
            patch.object(
                wm, "_detached_sandbox_teardown", AsyncMock()
            ) as mock_teardown,
        ):
            count = await wm.cleanup_idle_workspaces()

        assert count == 1
        # Both halves enter through the computer, and both are compare-and-set,
        # so a peer that moved the machine wins instead of being stomped.
        assert transitions == [
            (computer_id, "stopping", "running"),
            (computer_id, "stopping", "stopping"),
            (computer_id, "stopped", "stopping"),
        ]
        mock_teardown.assert_awaited_once()
        assert wm._machine(computer_id).idle_reap_failures == 0

    @pytest.mark.asyncio
    @patch(
        "src.server.services.workspace_manager.get_computers_by_status",
        new_callable=AsyncMock,
    )
    async def test_a_machine_it_cannot_stop_is_parked_rather_than_retried_forever(
        self, mock_computers, caplog
    ):
        """Whatever the cause, the same error every cycle for the life of the
        process is not a signal; one warning naming it is."""
        computer_id = str(uuid.uuid4())
        mock_computers.return_value = [
            _make_computer(
                computer_id=computer_id,
                last_activity_at=datetime.now(timezone.utc) - timedelta(hours=2),
            )
        ]

        wm = WorkspaceManager(_make_config(), idle_timeout=1800)
        stop = AsyncMock(side_effect=RuntimeError("the provider is down"))
        idle = patch.object(
            wm, "_machine_has_active_tasks", AsyncMock(return_value=False)
        )
        with (
            idle,
            patch.object(wm, "_stop_machine", stop),
            caplog.at_level(
                logging.WARNING, logger="src.server.services.workspace_manager"
            ),
        ):
            for _ in range(6):
                assert await wm.cleanup_idle_workspaces() == 0

        assert stop.await_count == 3
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1
        assert "the provider is down" in warnings[0].message

        # The hour is a park, not a retirement: once it passes the sweep tries
        # again, so a machine stuck behind a provider outage still gets stopped.
        wm._machine(computer_id).idle_reap_backoff_until = 0.0
        with idle, patch.object(wm, "_stop_machine", stop):
            await wm.cleanup_idle_workspaces()
        assert stop.await_count == 4

    @pytest.mark.asyncio
    @patch(
        "src.server.services.workspace_manager.get_computers_by_status",
        new_callable=AsyncMock,
    )
    async def test_the_sweeps_stop_mirrors_every_project_on_the_machine(
        self, mock_computers
    ):
        """The sweep names no project, and the stop ends the runtime for all of
        them, so every one is mirrored before the sandbox goes."""
        computer_id = str(uuid.uuid4())
        oldest, newer = str(uuid.uuid4()), str(uuid.uuid4())
        computer = _make_computer(
            computer_id=computer_id,
            last_activity_at=datetime.now(timezone.utc) - timedelta(hours=2),
        )
        mock_computers.return_value = [computer]

        wm = WorkspaceManager(_make_config(), idle_timeout=1800)
        backup = AsyncMock()
        with (
            cm_patch(
                "db_get_workspace",
                AsyncMock(
                    return_value=_make_workspace(
                        workspace_id=oldest, computer_id=computer_id
                    )
                ),
            ),
            _patch_live_ids(oldest, newer),
            patch(f"{_LIFECYCLE}.get_computer", AsyncMock(return_value=computer)),
            patch(f"{_MACHINES}.get_computer", AsyncMock(return_value=computer)),
            patch(
                f"{_MACHINES}.update_computer_status",
                AsyncMock(return_value={**computer, "status": "stopping"}),
            ),
            patch.object(
                wm, "_machine_has_active_tasks", AsyncMock(return_value=False)
            ),
            patch.object(wm, "backup_project_files", backup),
            patch.object(wm, "_detached_sandbox_teardown", AsyncMock()),
        ):
            count = await wm.cleanup_idle_workspaces()

        assert count == 1
        assert [c.args[0] for c in backup.await_args_list] == [oldest, newer]


class TestReapStuckStarting:
    """reap_stuck_starting_workspaces reverts computers wedged in 'starting' past
    the reap_stuck_after window, but never reaps a start THIS worker is still
    running (its record holds the pending_lazy_sync flag) and leaves fresh
    rows alone.
    """

    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    @pytest.mark.asyncio
    @patch(
        "src.server.services.workspace_manager.update_computer_status",
        new_callable=AsyncMock,
    )
    @patch(
        "src.server.services.workspace_manager.get_computers_by_status",
        new_callable=AsyncMock,
    )
    async def test_reaps_a_stale_starting_machine(self, mock_computers, mock_status):
        """A row wedged past the threshold with NO local membership is the
        cross-process case (a crashed/recycled worker left it 'starting'): no
        in-process owner will ever recover it, so the reaper reverts it. The
        revert is fenced on 'starting' so a start that finished meanwhile wins.
        """
        manager = WorkspaceManager.get_instance(config=_make_config())
        computer_id = str(uuid.uuid4())
        mock_computers.return_value = [
            _make_computer(
                computer_id=computer_id,
                status="starting",
                updated_at=datetime.now(timezone.utc)
                - timedelta(seconds=manager.reap_stuck_after + 1),
            )
        ]

        reverted = await manager.reap_stuck_starting_workspaces()

        assert reverted == 1
        mock_computers.assert_awaited_once_with("starting", limit=1000)
        mock_status.assert_awaited_once_with(
            computer_id, "stopped", expected="starting"
        )

    @pytest.mark.asyncio
    @patch(
        "src.server.services.workspace_manager.update_computer_status",
        new_callable=AsyncMock,
    )
    @patch(
        "src.server.services.workspace_manager.get_computers_by_status",
        new_callable=AsyncMock,
    )
    async def test_leaves_an_in_flight_lazy_owner_past_threshold(
        self, mock_computers, mock_status
    ):
        """Even PAST the threshold, a machine this worker is still starting (it
        holds the record's pending_lazy_sync flag) must NOT be reaped. The
        owner will promote on success or revert on failure. Reaping would clear
        the flag and no-op that promotion, stranding a ready session behind a
        'stopped' row.
        """
        manager = WorkspaceManager.get_instance(config=_make_config())
        computer_id = str(uuid.uuid4())
        mock_computers.return_value = [
            _make_computer(
                computer_id=computer_id,
                status="starting",
                updated_at=datetime.now(timezone.utc)
                - timedelta(seconds=manager.reap_stuck_after + 1),
            )
        ]
        manager._machine(computer_id).pending_lazy_sync = True

        reverted = await manager.reap_stuck_starting_workspaces()

        assert reverted == 0
        mock_status.assert_not_awaited()
        # The flag is preserved so the owner's later promotion still fires.
        assert manager._pending_lazy_start(computer_id)

    @pytest.mark.asyncio
    @patch(
        "src.server.services.workspace_manager.update_computer_status",
        new_callable=AsyncMock,
    )
    @patch(
        "src.server.services.workspace_manager.get_computers_by_status",
        new_callable=AsyncMock,
    )
    async def test_leaves_a_fresh_starting_machine(self, mock_computers, mock_status):
        """A start still within the wait window must NOT be reaped — that would
        yank a legitimately in-flight cold restore out from under its owner."""
        manager = WorkspaceManager.get_instance(config=_make_config())
        mock_computers.return_value = [
            _make_computer(
                status="starting",
                updated_at=datetime.now(timezone.utc) - timedelta(seconds=10),
            )
        ]

        reverted = await manager.reap_stuck_starting_workspaces()

        assert reverted == 0
        mock_status.assert_not_awaited()

    @pytest.mark.asyncio
    @patch(
        "src.server.services.workspace_manager.update_computer_status",
        new_callable=AsyncMock,
    )
    @patch(
        "src.server.services.workspace_manager.get_computers_by_status",
        new_callable=AsyncMock,
    )
    async def test_leaves_a_slow_but_legit_restore(self, mock_computers, mock_status):
        """A row older than start_wait_timeout but younger than reap_stuck_after
        is below the reap threshold and must NOT be reaped — even with no local
        membership (e.g. a cross-process start that is slow but not yet wedged).
        This isolates the threshold boundary from the in-process owner guard."""
        manager = WorkspaceManager.get_instance(config=_make_config())
        age = (manager.start_wait_timeout + manager.reap_stuck_after) / 2
        mock_computers.return_value = [
            _make_computer(
                status="starting",
                updated_at=datetime.now(timezone.utc) - timedelta(seconds=age),
            )
        ]

        reverted = await manager.reap_stuck_starting_workspaces()

        assert reverted == 0
        mock_status.assert_not_awaited()


# ---------------------------------------------------------------------------
# shutdown
# ---------------------------------------------------------------------------


class TestShutdown:
    """Test workspace manager shutdown."""

    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    @pytest.mark.asyncio
    async def test_shutdown_clears_state(self):
        config = _make_config()
        wm = WorkspaceManager(config)
        machine = wm._machine("ws-1")
        machine.session = _make_mock_session()
        machine.pending_lazy_sync = True
        machine.last_sync_at = time.monotonic()

        await wm.shutdown()

        # The record carried the session, the flag, the cooldown stamp and the
        # machine lock, and shutdown drops the record whole.
        assert wm._machines == {}
        assert wm._shutdown is True

    @pytest.mark.asyncio
    async def test_shutdown_cancels_cleanup_task(self):
        config = _make_config()
        wm = WorkspaceManager(config, cleanup_interval=1)

        # Start cleanup task
        await wm.start_cleanup_task()
        assert wm._cleanup_task is not None

        # Shutdown
        await wm.shutdown()
        assert wm._cleanup_task is None


# ---------------------------------------------------------------------------
# Sync cooldown
# ---------------------------------------------------------------------------


class TestSyncCooldown:
    """Test sync cooldown logic."""

    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    def test_sync_cooldown_no_previous_sync(self):
        config = _make_config()
        wm = WorkspaceManager(config)
        assert wm._sync_cooldown_ok("ws-1") is False

    def test_sync_cooldown_recent_sync(self):
        config = _make_config()
        wm = WorkspaceManager(config)
        wm._record_sync("ws-1")
        assert wm._sync_cooldown_ok("ws-1") is True

    def test_sync_cooldown_expired(self):
        config = _make_config()
        wm = WorkspaceManager(config)
        # Set sync time to well past the cooldown
        wm._machine("ws-1").last_sync_at = (
            time.monotonic() - wm._SYNC_COOLDOWN_SECONDS - 10
        )
        assert wm._sync_cooldown_ok("ws-1") is False


# ---------------------------------------------------------------------------
# _seed_agent_md
# ---------------------------------------------------------------------------


class TestSeedAgentMd:
    """Test agent.md seeding."""

    @staticmethod
    def _bare_sandbox():
        """A sandbox with nothing in it yet: the seed's only writing case."""
        sandbox = AsyncMock()
        sandbox.aread_file_text = AsyncMock(return_value=None)
        sandbox.awrite_file_text = AsyncMock(return_value=True)
        return sandbox

    @pytest.mark.asyncio
    async def test_seed_agent_md_writes_to_sandbox(self):
        sandbox = self._bare_sandbox()

        await WorkspaceManager._seed_agent_md(sandbox, "My Workspace")

        sandbox.awrite_file_text.assert_awaited_once()
        call_args = sandbox.awrite_file_text.call_args
        assert call_args[0][0] == "agent.md"
        assert "## Thread Index" in call_args[0][1]

    @pytest.mark.asyncio
    async def test_an_existing_agent_md_is_never_overwritten(self):
        """A project reaching its first sandbox may arrive with restored files
        (a duplicate's copy, or a machine rebuilt under it). The seed is a
        starting point, not a reset, and clobbering one loses the user's file.
        """
        sandbox = self._bare_sandbox()
        sandbox.aread_file_text = AsyncMock(return_value="# their notes")

        await WorkspaceManager._seed_agent_md(sandbox, "My Workspace")

        sandbox.awrite_file_text.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_the_template_does_not_name_the_workspace(self):
        # The row is the only place the name lives, and the prompt injects it
        # from there each turn. A copy written here could only go stale, which
        # is the whole bug the front-matter block used to cause.
        sandbox = self._bare_sandbox()

        await WorkspaceManager._seed_agent_md(sandbox, "My Workspace")

        content = sandbox.awrite_file_text.call_args[0][1]
        assert "My Workspace" not in content
        assert not content.startswith("---")

    @pytest.mark.asyncio
    async def test_seed_agent_md_none_sandbox_noop(self):
        # Should not raise when sandbox is None
        await WorkspaceManager._seed_agent_md(None, "Name")

    @pytest.mark.asyncio
    async def test_seed_agent_md_handles_write_failure(self):
        sandbox = self._bare_sandbox()
        sandbox.awrite_file_text = AsyncMock(side_effect=Exception("write failed"))

        # Should not raise
        await WorkspaceManager._seed_agent_md(sandbox, "Name")


# ---------------------------------------------------------------------------
# SandboxGoneError
# ---------------------------------------------------------------------------


class TestSandboxGoneError:
    """Test SandboxGoneError exception class."""

    def test_attributes_and_message(self):
        err = SandboxGoneError("sandbox-123", "not found: 404")
        assert err.sandbox_id == "sandbox-123"
        assert "sandbox-123" in str(err)
        assert "not found: 404" in str(err)

    def test_is_runtime_error(self):
        err = SandboxGoneError("sandbox-123")
        assert isinstance(err, RuntimeError)

    def test_empty_message(self):
        err = SandboxGoneError("sandbox-123")
        assert str(err) == "Sandbox sandbox-123 is gone"


# ---------------------------------------------------------------------------
# PTCSandbox.has_failed() state matrix
# ---------------------------------------------------------------------------


class TestHasFailed:
    """Test PTCSandbox.has_failed() distinguishes 'init failed' from 'still initializing'."""

    def test_no_lazy_init(self):
        """Non-lazy sandbox: _ready_event is None → has_failed() returns False."""
        sandbox = MagicMock()
        sandbox._ready_event = None
        sandbox._init_error = None
        # Call the real has_failed logic
        from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

        result = PTCSandbox.has_failed(sandbox)
        assert result is False

    def test_still_initializing(self):
        """Lazy init in progress: event not set → has_failed() returns False."""
        sandbox = MagicMock()
        sandbox._ready_event = asyncio.Event()
        sandbox._init_error = None
        from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

        result = PTCSandbox.has_failed(sandbox)
        assert result is False

    def test_success(self):
        """Lazy init succeeded: event set, no error → has_failed() returns False."""
        sandbox = MagicMock()
        sandbox._ready_event = asyncio.Event()
        sandbox._ready_event.set()
        sandbox._init_error = None
        from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

        result = PTCSandbox.has_failed(sandbox)
        assert result is False

    def test_with_error(self):
        """Lazy init failed: event set + error → has_failed() returns True."""
        sandbox = MagicMock()
        sandbox._ready_event = asyncio.Event()
        sandbox._ready_event.set()
        sandbox._init_error = SandboxGoneError("sb-1", "not found")
        from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

        result = PTCSandbox.has_failed(sandbox)
        assert result is True


# ---------------------------------------------------------------------------
# has_ready_session
# ---------------------------------------------------------------------------


class TestHasReadySession:
    """Test WorkspaceManager.has_ready_session() quick pre-check."""

    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    def test_has_ready_session_no_cache(self):
        """A project this worker holds no session for returns False."""
        config = _make_config()
        wm = WorkspaceManager(config)
        assert wm.has_ready_session("ws-nonexistent") is False

    def test_has_ready_session_ready(self):
        """Initialized session with ready sandbox returns True."""
        config = _make_config()
        wm = WorkspaceManager(config)
        session = _make_mock_session(initialized=True, has_sandbox=True)
        session.sandbox.is_ready = MagicMock(return_value=True)
        wm._machine(_STUB_COMPUTER_ID).session = session
        wm._session_computer["ws-1"] = _STUB_COMPUTER_ID
        assert wm.has_ready_session("ws-1") is True

    def test_has_ready_session_not_ready(self):
        """Initialized session with non-ready sandbox returns False."""
        config = _make_config()
        wm = WorkspaceManager(config)
        session = _make_mock_session(initialized=True, has_sandbox=True)
        session.sandbox.is_ready = MagicMock(return_value=False)
        wm._machine(_STUB_COMPUTER_ID).session = session
        wm._session_computer["ws-1"] = _STUB_COMPUTER_ID
        assert wm.has_ready_session("ws-1") is False

    def test_a_project_this_worker_has_never_served_reports_no_session(self):
        """The check is synchronous and must not resolve the machine to answer.

        A sibling's live session on the same computer is not this project's:
        answering off the machine would report ready for a project whose folder
        and tool overlay this worker has never prepared.
        """
        wm = WorkspaceManager(_make_config())
        wm._machine(_STUB_COMPUTER_ID).session = _make_mock_session()
        assert wm.has_ready_session("ws-never-served") is False


# ---------------------------------------------------------------------------
# Sandbox recovery — Gap 1 & Gap 2 fixes
# ---------------------------------------------------------------------------


class TestSandboxRecovery:
    """Test sandbox recovery when lazy init fails with sandbox-gone error."""

    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    def _make_manager(self):
        config = _make_config()
        manager = WorkspaceManager.get_instance(config=config)
        # Recovery re-provisions through the real restore path; with no DB pool
        # the completeness guard cannot be raised and provisioning aborts on
        # purpose. These tests are about the recovery spine, not the restore.
        manager._restore_files = AsyncMock()
        manager._maybe_restore_files = AsyncMock()
        return manager

    def _make_failed_session(self, error=None):
        """Create a session whose sandbox has a failed lazy init."""
        session = _make_mock_session()
        session.sandbox.is_ready = MagicMock(return_value=False)
        session.sandbox.has_failed = MagicMock(return_value=True)
        session.sandbox.init_error = error or SandboxGoneError("sb-old", "not found")
        return session

    def _make_initializing_session(self):
        """Create a session whose sandbox is still lazy-initializing."""
        session = _make_mock_session()
        session.sandbox.is_ready = MagicMock(return_value=False)
        session.sandbox.has_failed = MagicMock(return_value=False)
        return session

    @pytest.mark.asyncio
    @cm_patch("db_get_workspace")
    @cm_patch("SessionManager")
    @patch(f"{_LIFECYCLE}.update_workspace_status", new_callable=AsyncMock)
    @cm_patch("update_workspace_activity")
    async def test_cache_hit_failed_lazy_sandbox_gone_recovers(
        self, mock_activity, mock_status, mock_session_mgr, mock_get_ws
    ):
        """Gap 1: cached session with SandboxGoneError → _recover_sandbox called."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        workspace = _make_workspace(workspace_id=ws_id, status="running")
        mock_get_ws.return_value = workspace

        # Place broken session in cache
        broken_session = self._make_failed_session()
        manager._machine(_STUB_COMPUTER_ID).session = broken_session

        # Mock recovery: SessionManager.get_session returns a new working session
        new_session = _make_mock_session()
        new_session.sandbox.sandbox_id = "sb-new"
        mock_session_mgr.get_session.return_value = new_session
        mock_session_mgr.cleanup_session = AsyncMock()

        with (
            _patch_identity(workspace),
            _patch_machine_bind(workspace, computer_id=_STUB_COMPUTER_ID),
        ):
            result = await manager.get_session_for_workspace(ws_id, user_id="user-1")

        # Broken session should be proactively cleaned up (MCP + provider)
        mock_session_mgr.cleanup_session.assert_awaited_with(_STUB_COMPUTER_ID)
        # Recovery creates a new session
        new_session.initialize.assert_called_once()
        # Status updated
        assert result is not None

    @pytest.mark.asyncio
    @cm_patch("db_get_workspace")
    @cm_patch("SessionManager")
    @patch(f"{_LIFECYCLE}.update_workspace_status", new_callable=AsyncMock)
    @cm_patch("update_workspace_activity")
    async def test_cache_hit_failed_lazy_other_error_clears(
        self, mock_activity, mock_status, mock_session_mgr, mock_get_ws
    ):
        """Gap 1: cached session with non-SandboxGoneError → clears session, falls through."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        workspace = _make_workspace(workspace_id=ws_id, status="running")
        mock_get_ws.return_value = workspace

        # Broken session with a non-SandboxGoneError
        broken_session = self._make_failed_session(
            error=RuntimeError("network timeout")
        )
        manager._machine(_STUB_COMPUTER_ID).session = broken_session

        # Fall-through: SessionManager.get_session returns a new session for reconnect
        new_session = _make_mock_session()
        mock_session_mgr.get_session.return_value = new_session
        mock_session_mgr.cleanup_session = AsyncMock()

        with (
            _patch_identity(workspace),
            _patch_machine_bind(workspace, computer_id=_STUB_COMPUTER_ID),
        ):
            result = await manager.get_session_for_workspace(ws_id, user_id="user-1")

        # Broken session proactively cleaned up (MCP + provider)
        mock_session_mgr.cleanup_session.assert_awaited_with(_STUB_COMPUTER_ID)
        # Falls through to status-based handling (reconnect)
        assert result is not None

    @pytest.mark.asyncio
    @patch(f"{_LIFECYCLE}.update_workspace_status", new_callable=AsyncMock)
    @cm_patch("SessionManager")
    @cm_patch("db_get_workspace")
    async def test_recover_sandbox_failure_destroys_orphan(
        self, mock_get_ws, mock_session_mgr, mock_status
    ):
        """A failure after the new sandbox is created tears it down via
        cleanup_session, so a half-built recreate never orphans a billed sandbox."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        mock_get_ws.return_value = _make_workspace(
            workspace_id=ws_id, status="running", resource_tier="standard"
        )

        session = _make_mock_session()
        mock_session_mgr.get_session.return_value = session
        mock_session_mgr.cleanup_session = AsyncMock()

        manager._mint_sandbox_tokens = AsyncMock(return_value={})
        manager._apply_session_mcp = AsyncMock(return_value=None)
        manager._sync_sandbox_assets = AsyncMock()
        # Sandbox is created (initialize ok) but a later step fails.
        manager._restore_files = AsyncMock(side_effect=RuntimeError("restore boom"))

        with pytest.raises(RuntimeError, match="restore boom"):
            await manager._recover_sandbox(_binding(ws_id), "user-1", MagicMock())

        mock_session_mgr.cleanup_session.assert_awaited_once_with(_STUB_COMPUTER_ID)
        # Broken session not left in the cache.
        assert manager._cached_session(_STUB_COMPUTER_ID) is None

    @pytest.mark.asyncio
    @cm_patch("db_get_workspace")
    async def test_cache_hit_still_initializing_returns(self, mock_get_ws):
        """Sandbox still initializing → returns session immediately, no recovery."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        workspace = _make_workspace(workspace_id=ws_id, status="running")
        mock_get_ws.return_value = workspace

        session = self._make_initializing_session()
        manager._machine(_STUB_COMPUTER_ID).session = session

        with _patch_identity(workspace):
            result = await manager.get_session_for_workspace(ws_id, user_id="user-1")

        # Same session returned, no recovery triggered
        assert result is session

    @pytest.mark.asyncio
    @cm_patch("db_get_workspace")
    @cm_patch("SessionManager")
    @patch(f"{_LIFECYCLE}.update_workspace_status", new_callable=AsyncMock)
    @cm_patch("update_workspace_activity")
    async def test_phase2_sandbox_gone_recovers(
        self, mock_activity, mock_status, mock_session_mgr, mock_get_ws
    ):
        """Gap 2: ensure_sandbox_ready raises SandboxGoneError → recovery in Phase 2."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        workspace = _make_workspace(workspace_id=ws_id, status="running")
        mock_get_ws.return_value = workspace

        # Ready session but ensure_sandbox_ready fails (sandbox gone after cooldown)
        session = _make_mock_session()
        session.sandbox.ensure_sandbox_ready = AsyncMock(
            side_effect=SandboxGoneError("sb-old", "not found")
        )
        manager._machine(_STUB_COMPUTER_ID).session = session
        # Force sync by clearing cooldown
        manager._machine(_STUB_COMPUTER_ID).last_sync_at = None

        # Mock recovery
        new_session = _make_mock_session()
        new_session.sandbox.sandbox_id = "sb-new"
        mock_session_mgr.get_session.return_value = new_session
        mock_session_mgr.cleanup_session = AsyncMock()

        with (
            _patch_identity(workspace),
            _patch_machine_bind(workspace, computer_id=_STUB_COMPUTER_ID),
        ):
            result = await manager.get_session_for_workspace(ws_id, user_id="user-1")

        # Recovery triggered
        mock_session_mgr.cleanup_session.assert_awaited_with(_STUB_COMPUTER_ID)
        new_session.initialize.assert_called_once()
        assert result is not None

    @pytest.mark.asyncio
    @cm_patch("db_get_workspace")
    @cm_patch("SessionManager")
    async def test_phase2_concurrent_recovery_skips(
        self, mock_session_mgr, mock_get_ws
    ):
        """Gap 2: SandboxGoneError but session already recovered → uses existing."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        workspace = _make_workspace(workspace_id=ws_id, status="running")
        mock_get_ws.return_value = workspace

        # Session with sandbox-gone error in Phase 2
        broken_session = _make_mock_session()
        broken_session.sandbox.ensure_sandbox_ready = AsyncMock(
            side_effect=SandboxGoneError("sb-old", "not found")
        )
        manager._machine(_STUB_COMPUTER_ID).session = broken_session
        manager._machine(_STUB_COMPUTER_ID).last_sync_at = None

        # Simulate concurrent recovery: when we re-acquire the lock,
        # another request has already placed a working session in the cache.
        already_recovered = _make_mock_session()
        already_recovered.sandbox.is_ready = MagicMock(return_value=True)

        original_acquire = manager._acquire_machine_lock

        @asynccontextmanager
        async def mock_acquire(wid, timeout=60.0):
            # Before yielding the lock, simulate concurrent recovery
            manager._machine(_STUB_COMPUTER_ID).session = already_recovered
            async with original_acquire(wid, timeout=timeout):
                yield

        manager._acquire_machine_lock = mock_acquire

        with _patch_identity(workspace):
            result = await manager.get_session_for_workspace(ws_id, user_id="user-1")

        # Should return the already-recovered session, not create a new one
        assert result is already_recovered

    @pytest.mark.asyncio
    @cm_patch("db_get_workspace")
    async def test_phase2_other_error_logs_warning(self, mock_get_ws):
        """Phase 2: non-SandboxGoneError → logs warning, returns session."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        workspace = _make_workspace(workspace_id=ws_id, status="running")
        mock_get_ws.return_value = workspace

        session = _make_mock_session()
        session.sandbox.ensure_sandbox_ready = AsyncMock(
            side_effect=RuntimeError("network blip")
        )
        manager._machine(_STUB_COMPUTER_ID).session = session
        manager._machine(_STUB_COMPUTER_ID).last_sync_at = None

        with _patch_identity(workspace):
            result = await manager.get_session_for_workspace(ws_id, user_id="user-1")

        # Same session returned (broken, but we don't know it's sandbox-gone)
        assert result is session

    @pytest.mark.asyncio
    @cm_patch("db_get_workspace")
    @cm_patch("SessionManager")
    @patch(f"{_LIFECYCLE}.update_workspace_status", new_callable=AsyncMock)
    @cm_patch("update_workspace_activity")
    async def test_running_reconnect_sandbox_gone_recovers(
        self, mock_activity, mock_status, mock_session_mgr, mock_get_ws
    ):
        """Existing path: status=running, initialize raises SandboxGoneError → recovery."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        workspace = _make_workspace(workspace_id=ws_id, status="running")
        mock_get_ws.return_value = workspace

        # First session fails to initialize (sandbox gone)
        failing_session = _make_mock_session(initialized=False)
        failing_session.initialize = AsyncMock(
            side_effect=SandboxGoneError("sb-old", "not found")
        )

        # Recovery session
        recovered_session = _make_mock_session()
        recovered_session.sandbox.sandbox_id = "sb-new"

        mock_session_mgr.get_session.side_effect = [failing_session, recovered_session]
        mock_session_mgr.cleanup_session = AsyncMock()

        with _patch_machine_bind(workspace, computer_id=_STUB_COMPUTER_ID):
            result = await manager.get_session_for_workspace(ws_id, user_id="user-1")

        # Recovery triggered
        mock_session_mgr.cleanup_session.assert_awaited_with(_STUB_COMPUTER_ID)
        recovered_session.initialize.assert_called_once()
        assert result is not None

    @pytest.mark.asyncio
    @cm_patch("db_get_workspace")
    @cm_patch("SessionManager")
    @patch(f"{_LIFECYCLE}.update_workspace_status", new_callable=AsyncMock)
    @cm_patch("update_workspace_activity")
    @patch(f"{_LIFECYCLE}.try_claim_computer_for_start", new_callable=AsyncMock)
    async def test_stopped_workspace_lazy_init_sandbox_gone_recovers(
        self, mock_claim, mock_activity, mock_status, mock_session_mgr, mock_get_ws
    ):
        """REGRESSION: First request to a stopped workspace whose sandbox is deleted.

        Previously, _restart_workspace(lazy_init=True) returned a session
        with a pending background reconnect. The reconnect failed with
        SandboxGoneError but the error only surfaced when the chat handler
        called _wait_ready(). Now, the stopped path falls through to Phase 2
        which waits for lazy init and handles SandboxGoneError.
        """
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        workspace = _make_workspace(workspace_id=ws_id, status="stopped")
        mock_get_ws.return_value = workspace
        # Cross-worker claim succeeds — this worker wins the start mutex.
        mock_claim.return_value = _claimed_computer(ws_id)

        # _restart_workspace returns a session whose sandbox will fail in Phase 2
        lazy_session = _make_mock_session()
        lazy_session.sandbox.ensure_sandbox_ready = AsyncMock(
            side_effect=SandboxGoneError("sb-old", "not found")
        )

        # Recovery session
        recovered_session = _make_mock_session()
        recovered_session.sandbox.sandbox_id = "sb-new"

        # First call: _restart_workspace gets lazy_session
        # Second call: _recover_sandbox gets recovered_session
        mock_session_mgr.get_session.side_effect = [lazy_session, recovered_session]
        mock_session_mgr.cleanup_session = AsyncMock()

        # Patch _restart_workspace to return the lazy session directly
        # (simulates the real lazy init path)
        async def mock_restart(
            binding, workspace, user_id=None, lazy_init=True, on_state_observed=None
        ):
            session = lazy_session
            manager._machine(_STUB_COMPUTER_ID).session = session
            manager._machine(_STUB_COMPUTER_ID).pending_lazy_sync = True
            return session

        with (
            patch.object(manager, "_restart_workspace", side_effect=mock_restart),
            _patch_identity(workspace),
            _patch_machine_bind(workspace, computer_id=_STUB_COMPUTER_ID),
        ):
            result = await manager.get_session_for_workspace(ws_id, user_id="user-1")

        # Phase 2 caught SandboxGoneError and triggered recovery
        mock_session_mgr.cleanup_session.assert_awaited_with(_STUB_COMPUTER_ID)
        assert result is not None

    @pytest.mark.asyncio
    @cm_patch("SessionManager")
    @patch(f"{_LIFECYCLE}.update_workspace_status", new_callable=AsyncMock)
    @cm_patch("update_workspace_activity")
    async def test_restart_workspace_stamps_activity_after_status(
        self, mock_activity, mock_status, mock_session_mgr
    ):
        """REGRESSION: _restart_workspace must await update_workspace_activity
        after flipping status to 'running'. Without this, an idle sweep firing
        during the sandbox restore reads a stale last_activity_at and stops the
        workspace mid-request, surfacing to the user as
        'Session for workspace ... is not properly initialized'.
        Mirrors _recover_sandbox.
        """
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        workspace = _make_workspace(workspace_id=ws_id, status="stopped")

        session = _make_mock_session()
        mock_session_mgr.get_session.return_value = session

        # Patch non-focus internals so execution reaches the final
        # status + activity block on the happy reconnect path.
        manager._sync_sandbox_assets = AsyncMock()
        manager._maybe_restore_files = AsyncMock()
        manager._maybe_migrate_sandbox = AsyncMock(return_value=None)

        # Record relative order of the two awaited writes.
        call_order: list[str] = []

        async def record_status(**kwargs):
            call_order.append("status")

        async def record_activity(workspace_id):
            call_order.append("activity")

        mock_status.side_effect = record_status
        mock_activity.side_effect = record_activity

        result = await manager._restart_workspace(
            _binding(ws_id), workspace, user_id="user-1", lazy_init=False
        )

        assert result is session

        mock_status.assert_awaited_once()
        status_kwargs = mock_status.await_args.kwargs
        assert status_kwargs["status"] == "running"
        assert status_kwargs["workspace_id"] == ws_id
        mock_activity.assert_awaited_once_with(ws_id)

        # Ordering must match _recover_sandbox: status flip first, then
        # activity stamp. Reversing the order would leave a larger window
        # where cleanup_idle_workspaces could stop the workspace.
        assert call_order == ["status", "activity"]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("always_on", [True, False])
    @cm_patch("SessionManager")
    @patch(f"{_LIFECYCLE}.update_workspace_status", new_callable=AsyncMock)
    @cm_patch("update_workspace_activity")
    async def test_restart_reseeds_autostop_in_both_directions(
        self, mock_activity, mock_status, mock_session_mgr, always_on
    ):
        """REGRESSION: a plain reconnect must reseed the live auto-stop interval
        from the current is_always_on flag in BOTH directions. Re-asserting only
        the enable direction left a workspace whose always-on was disabled while
        stopped pinned at interval 0 (never auto-stops) until a full recreate —
        the disable healed only on recreate, not on the cheaper reconnect.
        """
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        workspace = _make_workspace(
            workspace_id=ws_id, status="stopped", is_always_on=always_on
        )

        session = _make_mock_session()
        mock_session_mgr.get_session.return_value = session

        # Patch non-focus internals so execution reaches the reconnect reseed.
        manager._sync_sandbox_assets = AsyncMock()
        manager._maybe_restore_files = AsyncMock()
        manager._maybe_migrate_sandbox = AsyncMock(return_value=None)
        manager._apply_autostop_for_always_on = AsyncMock()

        await manager._restart_workspace(
            _binding(ws_id, is_always_on=always_on),
            workspace,
            user_id="user-1",
            lazy_init=False,
        )

        # enabled tracks the flag exactly — True keeps interval 0, False
        # restores the default so the sandbox can auto-stop again. The non-lazy
        # reconnect reuses the live runtime, so it's passed through as ``runtime``.
        manager._apply_autostop_for_always_on.assert_awaited_once_with(
            "sandbox-abc", enabled=always_on, runtime=ANY, binding=ANY
        )


# ---------------------------------------------------------------------------
# on_state_observed forwarding — pin the kwarg threads through every
# session init branch so a silent typo in any one call site fails CI.
# ---------------------------------------------------------------------------


class TestOnStateObservedForwarding:
    """Lock in that on_state_observed is passed to session.initialize /
    initialize_lazy at every call site in workspace_manager.py. A typo
    or missing kwarg in any branch would silently drop the archived
    refinement event on the chat SSE stream."""

    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    def _make_manager(self):
        manager = WorkspaceManager.get_instance(config=_make_config())
        manager._sync_sandbox_assets = AsyncMock()
        manager._maybe_migrate_sandbox = AsyncMock(return_value=None)
        return manager

    @pytest.mark.asyncio
    @cm_patch("db_get_workspace")
    @cm_patch("SessionManager")
    @cm_patch("update_workspace_activity")
    async def test_running_path_forwards_callback_to_initialize(
        self, mock_activity, mock_session_mgr, mock_get_ws
    ):
        """status=running + no cache → session.initialize(..., on_state_observed=sentinel)."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        mock_get_ws.return_value = _make_workspace(workspace_id=ws_id, status="running")
        session = _make_mock_session(initialized=False)
        mock_session_mgr.get_session.return_value = session

        def sentinel(_s: str) -> None:
            return None

        await manager.get_session_for_workspace(
            ws_id, user_id="user-1", on_state_observed=sentinel
        )

        session.initialize.assert_awaited_once()
        assert session.initialize.await_args.kwargs.get("on_state_observed") is sentinel

    @pytest.mark.asyncio
    @cm_patch("db_get_workspace")
    @cm_patch("SessionManager")
    @cm_patch("update_workspace_activity")
    @patch(f"{_LIFECYCLE}.update_workspace_status", new_callable=AsyncMock)
    @patch(f"{_LIFECYCLE}.try_claim_computer_for_start", new_callable=AsyncMock)
    async def test_stopped_path_forwards_callback_to_initialize_lazy(
        self, mock_claim, mock_status, mock_activity, mock_session_mgr, mock_get_ws
    ):
        """status=stopped + matching config hash → _restart_workspace keeps
        lazy_init=True → session.initialize_lazy(..., on_state_observed=sentinel)."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        # Make config hash match so _restart_workspace keeps lazy_init=True.
        manager._compute_sandbox_config_hash = MagicMock(return_value="matching-hash")
        workspace = _make_workspace(
            workspace_id=ws_id,
            status="stopped",
            config={"sandbox_config_hash": "matching-hash"},
        )
        mock_get_ws.return_value = workspace
        mock_claim.return_value = _claimed_computer(ws_id)
        session = _make_mock_session(initialized=False)
        # Simulate lazy init leaving sandbox ready so Phase 2 doesn't retry.
        session.sandbox.is_ready = MagicMock(return_value=True)
        session.sandbox.has_failed = MagicMock(return_value=False)
        mock_session_mgr.get_session.return_value = session

        observed: list[str] = []

        def sentinel(s: str) -> None:
            observed.append(s)

        await manager.get_session_for_workspace(
            ws_id, user_id="user-1", on_state_observed=sentinel
        )

        session.initialize_lazy.assert_awaited_once()
        forwarded = session.initialize_lazy.await_args.kwargs.get("on_state_observed")
        # The stopped (claim-owner) path wraps the caller's callback so it can
        # also broadcast the archived hint cross-worker — the wrapper must still
        # invoke the original observer.
        assert forwarded is not None
        forwarded("stopped")
        assert observed == ["stopped"]
        # Lazy path must not have touched the eager initialize.
        session.initialize.assert_not_awaited()

    @pytest.mark.asyncio
    @cm_patch("db_get_workspace")
    @cm_patch("SessionManager")
    @cm_patch("update_workspace_activity")
    @patch(f"{_LIFECYCLE}.update_workspace_status", new_callable=AsyncMock)
    @patch(f"{_LIFECYCLE}.try_claim_computer_for_start", new_callable=AsyncMock)
    async def test_restart_forced_non_lazy_forwards_callback_to_initialize(
        self, mock_claim, mock_status, mock_activity, mock_session_mgr, mock_get_ws
    ):
        """Config hash mismatch inside _restart_workspace forces lazy_init=False
        → session.initialize(..., on_state_observed=sentinel) instead of initialize_lazy."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        manager._compute_sandbox_config_hash = MagicMock(return_value="new-hash")
        workspace = _make_workspace(
            workspace_id=ws_id,
            status="stopped",
            config={"sandbox_config_hash": "old-hash"},
        )
        mock_get_ws.return_value = workspace
        mock_claim.return_value = _claimed_computer(ws_id)
        session = _make_mock_session(initialized=False)
        session.sandbox.is_ready = MagicMock(return_value=True)
        session.sandbox.has_failed = MagicMock(return_value=False)
        mock_session_mgr.get_session.return_value = session

        observed: list[str] = []

        def sentinel(s: str) -> None:
            observed.append(s)

        await manager.get_session_for_workspace(
            ws_id, user_id="user-1", on_state_observed=sentinel
        )

        session.initialize.assert_awaited_once()
        forwarded = session.initialize.await_args.kwargs.get("on_state_observed")
        # Claim-owner path wraps the caller's callback; the wrapper must still
        # invoke the original observer even on the forced non-lazy branch.
        assert forwarded is not None
        forwarded("stopped")
        assert observed == ["stopped"]
        session.initialize_lazy.assert_not_awaited()

    @pytest.mark.asyncio
    @cm_patch("db_get_workspace")
    @cm_patch("SessionManager")
    @cm_patch("update_workspace_activity")
    async def test_warm_cached_session_does_not_call_any_initialize(
        self, mock_activity, mock_session_mgr, mock_get_ws
    ):
        """Initialized cached session → no initialize / initialize_lazy call
        even when on_state_observed is passed. The callback simply has no
        path to fire on the warm hit and must not leak into any init path."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        workspace = _make_workspace(workspace_id=ws_id, status="running")
        mock_get_ws.return_value = workspace
        cached = _make_mock_session(initialized=True)
        cached.sandbox.is_ready = MagicMock(return_value=True)
        cached.sandbox.has_failed = MagicMock(return_value=False)
        manager._machine(_STUB_COMPUTER_ID).session = cached

        def sentinel(_s: str) -> None:
            return None

        with _patch_identity(workspace):
            await manager.get_session_for_workspace(
                ws_id, user_id="user-1", on_state_observed=sentinel
            )

        cached.initialize.assert_not_awaited()
        cached.initialize_lazy.assert_not_awaited()


# ---------------------------------------------------------------------------
# Phase 2 error narrowing + _clear_session helper (Fix 2)
# ---------------------------------------------------------------------------

from ptc_agent.core.sandbox.runtime import SandboxTransientError  # noqa: E402


class TestPhase2ErrorNarrowing:
    """Phase 2 distinguishes a failed lazy init (has_failed() == True,
    clear + re-raise) from a post-init transient (has_failed() == False).
    For an UNPROMOTED lazy start, a post-init transient reverts the row to
    'stopped' and re-raises so the caller can't return a sandbox behind a
    'stopped' row (split-brain). Generic Exception keeps the legacy
    best-effort-retry behavior — regression-guarded here."""

    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    def _make_manager(self):
        return WorkspaceManager.get_instance(config=_make_config())

    @pytest.mark.asyncio
    @cm_patch("db_get_workspace")
    @cm_patch("SessionManager")
    @patch(f"{_LIFECYCLE}.update_workspace_status", new_callable=AsyncMock)
    @cm_patch("update_workspace_activity")
    async def test_phase2_transient_init_failure_clears_and_raises(
        self, mock_activity, mock_status, mock_session_mgr, mock_get_ws
    ):
        """Phase 2 SandboxTransientError + has_failed() True → _clear_session
        is called and the error propagates for handle_workflow_error to catch."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        workspace = _make_workspace(workspace_id=ws_id, status="running")
        mock_get_ws.return_value = workspace

        session = _make_mock_session()
        session.sandbox.ensure_sandbox_ready = AsyncMock(
            side_effect=SandboxTransientError("transport failed after retries")
        )
        session.sandbox.has_failed = MagicMock(return_value=True)
        manager._machine(_STUB_COMPUTER_ID).session = session
        manager._machine(_STUB_COMPUTER_ID).last_sync_at = None
        mock_session_mgr.cleanup_session = AsyncMock()

        with pytest.raises(SandboxTransientError), _patch_identity(workspace):
            await manager.get_session_for_workspace(ws_id, user_id="user-1")

        mock_session_mgr.cleanup_session.assert_awaited_with(_STUB_COMPUTER_ID)
        assert manager._cached_session(_STUB_COMPUTER_ID) is None

    @pytest.mark.asyncio
    @cm_patch("db_get_workspace")
    @cm_patch("SessionManager")
    @patch(f"{_LIFECYCLE}.update_workspace_status", new_callable=AsyncMock)
    @cm_patch("update_workspace_activity")
    async def test_phase2_transient_post_init_lazy_reverts_and_raises(
        self, mock_activity, mock_status, mock_session_mgr, mock_get_ws
    ):
        """A post-init transient (e.g. asset sync) on an UNPROMOTED lazy start
        reverts the row to 'stopped' and re-raises. has_failed() == False, so
        the sandbox is healthy — but returning the session here would hand back
        a sandbox the DB says is 'stopped', letting another worker spawn a
        second one (split-brain). The discriminator is the record's
        pending_lazy_sync flag; the deferred-sync asset step is reached only on
        that path."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        workspace = _make_workspace(workspace_id=ws_id, status="running")
        mock_get_ws.return_value = workspace

        session = _make_mock_session()
        session.sandbox.has_failed = MagicMock(return_value=False)
        session.sandbox.ensure_sandbox_ready = AsyncMock()

        # Post-init transient: raise from a later sync step via patched method.
        manager._sync_sandbox_assets = AsyncMock(
            side_effect=SandboxTransientError("sync blip")
        )
        manager._maybe_restore_files = AsyncMock()
        # An unpromoted lazy start: this worker owns the row's transition.
        manager._machine(_STUB_COMPUTER_ID).pending_lazy_sync = True
        manager._machine(_STUB_COMPUTER_ID).session = session
        manager._machine(_STUB_COMPUTER_ID).last_sync_at = None
        mock_session_mgr.cleanup_session = AsyncMock()

        with pytest.raises(SandboxTransientError), _patch_identity(workspace):
            await manager.get_session_for_workspace(ws_id, user_id="user-1")

        # Row reverted so cross-worker losers re-claim immediately instead of
        # the caller returning a sandbox behind a 'stopped' row.
        mock_status.assert_any_await(workspace_id=ws_id, status="stopped")
        assert not manager._pending_lazy_start(_STUB_COMPUTER_ID)
        # has_failed() was False — the healthy session is left cached (not
        # cleared); the next request re-claims against the reverted row.
        mock_session_mgr.cleanup_session.assert_not_awaited()

    @pytest.mark.asyncio
    @cm_patch("db_get_workspace")
    @cm_patch("SessionManager")
    async def test_phase2_generic_exception_not_cleared(
        self, mock_session_mgr, mock_get_ws
    ):
        """REGRESSION: plain Exception in Phase 2 keeps the legacy
        'log and retry next request' behavior. Do not broaden the clear."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        workspace = _make_workspace(workspace_id=ws_id, status="running")
        mock_get_ws.return_value = workspace

        session = _make_mock_session()
        session.sandbox.ensure_sandbox_ready = AsyncMock(
            side_effect=RuntimeError("some non-sandbox runtime error")
        )
        manager._machine(_STUB_COMPUTER_ID).session = session
        manager._machine(_STUB_COMPUTER_ID).last_sync_at = None
        mock_session_mgr.cleanup_session = AsyncMock()

        with _patch_identity(workspace):
            result = await manager.get_session_for_workspace(ws_id, user_id="user-1")

        assert result is session
        mock_session_mgr.cleanup_session.assert_not_awaited()


class TestClearSessionHelper:
    """WorkspaceManager._clear_session proactively awaits cleanup_session
    (closes MCP + provider) and clears local caches. Must be resilient when
    cleanup_session raises and idempotent when the machine is not tracked.

    The session and every cache beside it belong to the computer, so this takes
    a computer id: clearing per project would drop a handle a sibling is using.
    """

    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    @pytest.mark.asyncio
    @cm_patch("SessionManager")
    async def test_clear_session_happy_path(self, mock_sm):
        """Awaits cleanup_session, then clears the machine record's session
        and its pending_lazy_sync flag."""
        config = _make_config()
        manager = WorkspaceManager.get_instance(config=config)
        manager._machine(_STUB_COMPUTER_ID).session = _make_mock_session()
        manager._machine(_STUB_COMPUTER_ID).pending_lazy_sync = True
        mock_sm.cleanup_session = AsyncMock()

        await manager._clear_session(_STUB_COMPUTER_ID)

        mock_sm.cleanup_session.assert_awaited_once_with(_STUB_COMPUTER_ID)
        assert manager._cached_session(_STUB_COMPUTER_ID) is None
        assert not manager._pending_lazy_start(_STUB_COMPUTER_ID)

    @pytest.mark.asyncio
    @cm_patch("SessionManager")
    async def test_clear_session_idempotent_when_absent(self, mock_sm):
        """Workspace not tracked — no KeyError; cleanup still attempted."""
        config = _make_config()
        manager = WorkspaceManager.get_instance(config=config)
        mock_sm.cleanup_session = AsyncMock()

        await manager._clear_session(_STUB_COMPUTER_ID)  # must not raise

        mock_sm.cleanup_session.assert_awaited_once_with(_STUB_COMPUTER_ID)

    @pytest.mark.asyncio
    @cm_patch("SessionManager")
    async def test_clear_session_survives_cleanup_exception(self, mock_sm):
        """If cleanup_session raises, local caches still clear — the caller
        must not see the exception bleed out of this helper."""
        config = _make_config()
        manager = WorkspaceManager.get_instance(config=config)
        manager._machine(_STUB_COMPUTER_ID).session = _make_mock_session()
        manager._machine(_STUB_COMPUTER_ID).pending_lazy_sync = True
        mock_sm.cleanup_session = AsyncMock(side_effect=RuntimeError("MCP stuck"))

        await manager._clear_session(_STUB_COMPUTER_ID)  # must swallow

        assert manager._cached_session(_STUB_COMPUTER_ID) is None
        assert not manager._pending_lazy_start(_STUB_COMPUTER_ID)


# ---------------------------------------------------------------------------
# Intermediate "starting" status (Fix 1)
# ---------------------------------------------------------------------------


class TestIntermediateStartingStatus:
    """Lazy restart: status transitions stopped → starting → running.
    The activity stamp moves with the running promotion (not the starting
    flip) — cleanup_idle_workspaces only queries status=running, so rows
    in "starting" are immune to the idle sweep regardless."""

    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    def _make_manager(self):
        manager = WorkspaceManager.get_instance(config=_make_config())
        manager._sync_sandbox_assets = AsyncMock()
        manager._maybe_restore_files = AsyncMock()
        manager._maybe_migrate_sandbox = AsyncMock(return_value=None)
        # Stable config hash so lazy_init is not force-flipped to False
        # by the config-migration guard in _restart_workspace.
        manager._compute_sandbox_config_hash = MagicMock(return_value="stable")
        return manager

    @staticmethod
    def _lazy_workspace(workspace_id, status="stopped"):
        return _make_workspace(
            workspace_id=workspace_id,
            status=status,
            config={"sandbox_config_hash": "stable"},
        )

    @pytest.mark.asyncio
    @cm_patch("SessionManager")
    @patch(f"{_LIFECYCLE}.update_workspace_status", new_callable=AsyncMock)
    @cm_patch("update_workspace_activity")
    async def test_lazy_restart_sets_starting_without_activity_stamp(
        self, mock_activity, mock_status, mock_session_mgr
    ):
        """lazy_init=True flips status → "starting" and does NOT stamp
        activity (sweep never sees "starting")."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        workspace = self._lazy_workspace(ws_id, status="stopped")

        session = _make_mock_session()
        mock_session_mgr.get_session.return_value = session

        await manager._restart_workspace(
            _binding(ws_id), workspace, user_id="user-1", lazy_init=True
        )

        mock_status.assert_awaited_once()
        kwargs = mock_status.await_args.kwargs
        assert kwargs["status"] == "starting"
        assert kwargs["workspace_id"] == ws_id
        mock_activity.assert_not_awaited()

    @pytest.mark.asyncio
    @cm_patch("db_get_workspace")
    @cm_patch("SessionManager")
    @patch(f"{_LIFECYCLE}.update_workspace_status", new_callable=AsyncMock)
    @cm_patch("update_workspace_activity")
    @patch(f"{_LIFECYCLE}.try_claim_computer_for_start", new_callable=AsyncMock)
    async def test_phase2_success_promotes_starting_to_running_and_stamps(
        self, mock_claim, mock_activity, mock_status, mock_session_mgr, mock_get_ws
    ):
        """When Phase 2 finishes the deferred sync, DB is promoted to
        running AND activity is stamped in that order (mirrors PR #152's
        invariant for the lazy path)."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        workspace = self._lazy_workspace(ws_id, status="stopped")
        mock_get_ws.return_value = workspace
        mock_claim.return_value = _claimed_computer(ws_id)

        lazy_session = _make_mock_session()
        lazy_session.sandbox.ensure_sandbox_ready = AsyncMock()
        mock_session_mgr.get_session.return_value = lazy_session

        call_order: list[tuple[str, dict]] = []

        async def record_status(**kwargs):
            call_order.append(("status", kwargs))

        async def record_activity(workspace_id):
            call_order.append(("activity", {"workspace_id": workspace_id}))

        mock_status.side_effect = record_status
        mock_activity.side_effect = record_activity

        await manager.get_session_for_workspace(ws_id, user_id="user-1")

        # Expected sequence:
        #   1. _restart_workspace: status=starting (no activity stamp yet)
        #   2. Phase 2: status=running, then activity
        names = [c[0] for c in call_order]
        assert names == ["status", "status", "activity"], names
        assert call_order[0][1]["status"] == "starting"
        assert call_order[1][1]["status"] == "running"
        assert call_order[2][1]["workspace_id"] == ws_id

    @pytest.mark.asyncio
    @cm_patch("db_get_workspace")
    @cm_patch("SessionManager")
    @patch(f"{_LIFECYCLE}.update_workspace_status", new_callable=AsyncMock)
    @cm_patch("update_workspace_activity")
    @patch(f"{_LIFECYCLE}.try_claim_computer_for_start", new_callable=AsyncMock)
    async def test_phase2_failure_reverts_status_to_stopped(
        self, mock_claim, mock_activity, mock_status, mock_session_mgr, mock_get_ws
    ):
        """Claim winner fails in Phase 2 → row is reverted "starting" → "stopped"
        (never promoted to running), so cross-worker losers can re-claim
        immediately instead of waiting out the full start_wait_timeout. The
        original exception still propagates."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        workspace = self._lazy_workspace(ws_id, status="stopped")
        mock_get_ws.return_value = workspace
        mock_claim.return_value = _claimed_computer(ws_id)

        lazy_session = _make_mock_session()
        lazy_session.sandbox.ensure_sandbox_ready = AsyncMock(
            side_effect=SandboxTransientError("exhausted retries")
        )
        lazy_session.sandbox.has_failed = MagicMock(return_value=True)
        mock_session_mgr.get_session.return_value = lazy_session
        mock_session_mgr.cleanup_session = AsyncMock()

        with pytest.raises(SandboxTransientError):
            await manager.get_session_for_workspace(ws_id, user_id="user-1")

        status_calls = [c.kwargs for c in mock_status.await_args_list]
        # 'starting' from the claim/restart, then 'stopped' from the Phase 2
        # failure revert — and crucially never 'running'.
        assert {c["status"] for c in status_calls} == {"starting", "stopped"}
        assert status_calls[-1]["status"] == "stopped"
        # Pending-sync marker cleared so the workspace isn't wedged.
        assert not manager._pending_lazy_start(_STUB_COMPUTER_ID)

    @pytest.mark.asyncio
    @cm_patch("db_get_workspace")
    @cm_patch("SessionManager")
    @patch(f"{_LIFECYCLE}.update_workspace_status", new_callable=AsyncMock)
    @cm_patch("update_workspace_activity")
    @patch(f"{_LIFECYCLE}.try_claim_computer_for_start", new_callable=AsyncMock)
    async def test_phase2_generic_failure_reverts_status_to_stopped(
        self, mock_claim, mock_activity, mock_status, mock_session_mgr, mock_get_ws
    ):
        """A generic Phase 2 failure on an unpromoted lazy start is the most
        dangerous path: it must revert the row to 'stopped' so losers re-claim
        immediately, AND re-raise rather than return the session — returning it
        would hand the agent a sandbox that never finished asset/file sync while
        the DB says 'stopped'."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        workspace = self._lazy_workspace(ws_id, status="stopped")
        mock_get_ws.return_value = workspace
        mock_claim.return_value = _claimed_computer(ws_id)

        lazy_session = _make_mock_session()
        lazy_session.sandbox.ensure_sandbox_ready = AsyncMock(
            side_effect=RuntimeError("daytona hiccup")
        )
        mock_session_mgr.get_session.return_value = lazy_session

        # Unpromoted lazy start: the failure is surfaced, not swallowed.
        with pytest.raises(RuntimeError, match="daytona hiccup"):
            await manager.get_session_for_workspace(ws_id, user_id="user-1")

        status_calls = [c.kwargs for c in mock_status.await_args_list]
        assert {c["status"] for c in status_calls} == {"starting", "stopped"}
        assert status_calls[-1]["status"] == "stopped"
        assert not manager._pending_lazy_start(_STUB_COMPUTER_ID)

    @pytest.mark.asyncio
    @cm_patch("db_get_workspace")
    @cm_patch("SessionManager")
    @patch(f"{_LIFECYCLE}.update_workspace_status", new_callable=AsyncMock)
    @cm_patch("update_workspace_activity")
    @patch(f"{_LIFECYCLE}.try_claim_computer_for_start", new_callable=AsyncMock)
    async def test_phase2_cancelled_reverts_status_to_stopped(
        self, mock_claim, mock_activity, mock_status, mock_session_mgr, mock_get_ws
    ):
        """A client disconnect / shutdown cancels Phase 2. CancelledError is a
        BaseException, so without an explicit handler it would bypass every
        revert and wedge the row in 'starting' forever. It must revert to
        'stopped' AND re-raise to preserve cancellation semantics."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        workspace = self._lazy_workspace(ws_id, status="stopped")
        mock_get_ws.return_value = workspace
        mock_claim.return_value = _claimed_computer(ws_id)

        lazy_session = _make_mock_session()
        lazy_session.sandbox.ensure_sandbox_ready = AsyncMock(
            side_effect=asyncio.CancelledError()
        )
        mock_session_mgr.get_session.return_value = lazy_session

        with pytest.raises(asyncio.CancelledError):
            await manager.get_session_for_workspace(ws_id, user_id="user-1")

        status_calls = [c.kwargs for c in mock_status.await_args_list]
        assert status_calls[-1]["status"] == "stopped"
        assert not manager._pending_lazy_start(_STUB_COMPUTER_ID)

    @pytest.mark.asyncio
    @patch(
        "src.server.services.computer_manager._lifecycle.publish_status_change",
        new_callable=AsyncMock,
    )
    @cm_patch("db_get_workspace")
    @cm_patch("SessionManager")
    @patch(f"{_LIFECYCLE}.update_workspace_status", new_callable=AsyncMock)
    @cm_patch("update_workspace_activity")
    @patch(f"{_LIFECYCLE}.try_claim_computer_for_start", new_callable=AsyncMock)
    async def test_claim_owner_broadcasts_archived_state(
        self,
        mock_claim,
        mock_activity,
        mock_status,
        mock_session_mgr,
        mock_get_ws,
        mock_publish,
    ):
        """When the pre-start sandbox state is 'archived', the claim owner
        publishes it on the status channel so cross-worker consumers (the
        /events SSE, a losing worker's chat spinner) can show the slow-restore
        copy regardless of who owns the start."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        workspace = self._lazy_workspace(ws_id, status="stopped")
        mock_get_ws.return_value = workspace
        mock_claim.return_value = _claimed_computer(ws_id)

        session = _make_mock_session(initialized=False)
        session.sandbox.is_ready = MagicMock(return_value=True)
        session.sandbox.has_failed = MagicMock(return_value=False)

        async def fake_init_lazy(*args, on_state_observed=None, **kwargs):
            if on_state_observed is not None:
                on_state_observed("archived")

        session.initialize_lazy = AsyncMock(side_effect=fake_init_lazy)
        mock_session_mgr.get_session.return_value = session

        await manager.get_session_for_workspace(ws_id, user_id="user-1")

        archived = [
            c
            for c in mock_publish.call_args_list
            if (c.kwargs.get("extra") or {}).get("sandbox_state") == "archived"
        ]
        assert archived, "claim owner did not broadcast archived sandbox_state"
        assert archived[0].args[1] == "starting"

    @pytest.mark.asyncio
    @cm_patch("db_get_workspace")
    @cm_patch("SessionManager")
    @patch(f"{_LIFECYCLE}.update_workspace_status", new_callable=AsyncMock)
    @cm_patch("update_workspace_activity")
    async def test_status_starting_waits_for_other_worker_to_finish(
        self, mock_activity, mock_status, mock_session_mgr, mock_get_ws
    ):
        """Cross-worker safety: a request landing on a workspace already in
        'starting' MUST NOT restart (would double-start the sandbox in another
        worker). It waits for the in-flight start to flip status to 'running',
        then attaches to that session via the running path.

        Replaces the prior "re-enter restart flow" behavior, which was unsafe
        under multi-worker deployments. See ``try_claim_computer_for_start``
        and ``_wait_for_start_completion``.
        """
        manager = self._make_manager()
        # Tighten polling so the test does not depend on default 300s/0.5s.
        manager.start_wait_timeout = 5.0
        manager.start_wait_poll_interval = 0.01
        ws_id = str(uuid.uuid4())
        starting_ws = self._lazy_workspace(ws_id, status="starting")
        running_ws = self._lazy_workspace(ws_id, status="running")
        # Phase 1 sees 'starting'; the wait helper reads the row once to pick
        # the machine's status channel, then its first re-read sees 'running'.
        mock_get_ws.side_effect = [starting_ws, starting_ws, running_ws]

        session = _make_mock_session(initialized=True)
        session.sandbox.is_ready = MagicMock(return_value=True)
        session.sandbox.has_failed = MagicMock(return_value=False)
        mock_session_mgr.get_session.return_value = session

        result = await manager.get_session_for_workspace(ws_id, user_id="user-1")

        # Did NOT call initialize_lazy — we did not restart.
        session.initialize_lazy.assert_not_awaited()
        # Returned a usable session attached via the running path.
        assert result is session


# ---------------------------------------------------------------------------
# Status-tuple parametrization for DB-fallback routing (Fix 1 consumers)
# ---------------------------------------------------------------------------


class TestStatusRoutesToDbFallback:
    """Smoke check that the consumer modules route non-live workspaces to the
    DB fallback instead of waking a sandbox. ``workspace_files`` (authenticated)
    decides through ``served_from_mirror``; ``public``
    (unauthenticated) uses the stronger ``status == "running"`` +
    ``has_ready_session`` no-wake gate. A regression in either reproduces the
    503 storm from the original incident (or, for public, a denial-of-wallet)."""

    @pytest.mark.parametrize("status", ["stopped", "stopping", "starting"])
    def test_workspace_files_route_to_the_mirror(self, status):
        from src.server.app.workspace_files import crud
        from src.server.models.workspace import served_from_mirror
        import inspect

        assert served_from_mirror(status)
        assert not served_from_mirror("running")
        # The authenticated routes decide through the one shared predicate.
        source = inspect.getsource(crud)
        assert 'served_from_mirror(workspace.get("status"))' in source
        assert '"stopped", "stopping", "starting"' not in source

    def test_public_routes_never_acquire_a_session(self):
        """The unauthenticated shared file routes must read only a warm
        in-memory session, never ``get_session_for_workspace`` which would
        attach/restart a Daytona sandbox for a share-token request
        (denial-of-wallet)."""
        from src.server.app import share_files
        import inspect

        source = inspect.getsource(share_files)
        # Every read goes through the one no-wake helper.
        assert "get_session_for_workspace" not in source
        assert source.count("warm_sandbox(") >= 2

    def test_get_session_if_ready_declines_a_handle_bound_elsewhere(self):
        """The no-wake accessor must refuse a session whose sandbox has moved."""
        config = _make_config()
        wm = WorkspaceManager(config)
        # The handle is the machine's; the project names which machine it is on.
        wm._machine(
            _STUB_COMPUTER_ID
        ).session = _make_mock_session()  # on 'sandbox-abc'
        wm._session_computer["ws-1"] = _STUB_COMPUTER_ID

        assert (
            wm.get_session_if_ready("ws-1", expected_sandbox_id="sandbox-abc")
            is not None
        )
        # The machine was rebound to a replacement sandbox.
        assert (
            wm.get_session_if_ready("ws-1", expected_sandbox_id="sandbox-xyz") is None
        )
        # A half-known binding is an inconsistency, not a licence to serve.
        assert wm.get_session_if_ready("ws-1", expected_sandbox_id=None) is None

    def test_the_warm_handle_is_fenced_on_the_rows_sandbox_id(self):
        """The share routes do no DB round-trip of their own, so the row they
        already hold is what fences the cached handle.

        Without it a replaced sandbox keeps answering share links with a false
        "File not found", and a row whose sandbox auto-stopped serves nothing
        while reading 'running'.
        """
        from src.server.app.workspace_files.serve import warm_sandbox

        wm = WorkspaceManager.get_instance(config=_make_config())
        session = _make_mock_session()  # on 'sandbox-abc'
        wm._machine(_STUB_COMPUTER_ID).session = session
        wm._session_computer["ws-1"] = _STUB_COMPUTER_ID
        try:
            running = {"status": "running", "sandbox_id": "sandbox-abc"}
            assert warm_sandbox(running, "ws-1") is session.sandbox
            # Replaced sandbox, same row status.
            assert warm_sandbox({**running, "sandbox_id": "sb-new"}, "ws-1") is None
            # Not running: nothing warm to serve from.
            assert warm_sandbox({**running, "status": "stopped"}, "ws-1") is None
        finally:
            WorkspaceManager.reset_instance()


# ---------------------------------------------------------------------------
# Multi-worker start mutex — cross-process race protection
# ---------------------------------------------------------------------------


class TestMultiWorkerStartMutex:
    """``try_claim_computer_for_start`` atomically flips the machine from the
    state the caller saw to 'starting'; only the winner restarts. Losers wait
    via ``_wait_for_start_completion`` and attach via the running path.

    The claim moved to the computer because the sandbox is the computer's: two
    projects on one machine racing project-row claims would both win and start
    the same sandbox twice."""

    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    def _make_manager(self):
        manager = WorkspaceManager.get_instance(config=_make_config())
        manager.start_wait_timeout = 5.0
        manager.start_wait_poll_interval = 0.01
        manager._sync_sandbox_assets = AsyncMock()
        manager._maybe_restore_files = AsyncMock()
        manager._maybe_migrate_sandbox = AsyncMock(return_value=None)
        return manager

    @pytest.mark.asyncio
    @cm_patch("db_get_workspace")
    @cm_patch("SessionManager")
    @patch(f"{_LIFECYCLE}.update_workspace_status", new_callable=AsyncMock)
    @cm_patch("update_workspace_activity")
    @patch(f"{_LIFECYCLE}.try_claim_computer_for_start", new_callable=AsyncMock)
    async def test_winner_proceeds_with_restart(
        self, mock_claim, mock_activity, mock_status, mock_session_mgr, mock_get_ws
    ):
        """Worker that wins the claim restarts the sandbox normally."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        # Provide a matching config hash so _restart_workspace keeps lazy_init=True.
        manager._compute_sandbox_config_hash = MagicMock(return_value="match")
        workspace = _make_workspace(
            workspace_id=ws_id,
            status="stopped",
            config={"sandbox_config_hash": "match"},
        )
        mock_get_ws.return_value = workspace
        # Claim succeeds — we own the start.
        mock_claim.return_value = _claimed_computer(ws_id)

        session = _make_mock_session()
        session.sandbox.ensure_sandbox_ready = AsyncMock()
        mock_session_mgr.get_session.return_value = session

        await manager.get_session_for_workspace(ws_id, user_id="user-1")

        mock_claim.assert_awaited_once_with(_STUB_COMPUTER_ID, from_status="stopped")
        session.initialize_lazy.assert_awaited_once()

    @pytest.mark.asyncio
    @cm_patch("db_get_workspace")
    @cm_patch("SessionManager")
    @patch(f"{_LIFECYCLE}.update_workspace_status", new_callable=AsyncMock)
    @cm_patch("update_workspace_activity")
    @patch(f"{_LIFECYCLE}.try_claim_computer_for_start", new_callable=AsyncMock)
    async def test_loser_waits_then_attaches_via_running_path(
        self, mock_claim, mock_activity, mock_status, mock_session_mgr, mock_get_ws
    ):
        """Worker that loses the claim does NOT restart — it waits for the
        winner to finish, then attaches to the now-running session."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        stopped_ws = _make_workspace(workspace_id=ws_id, status="stopped")
        running_ws = _make_workspace(workspace_id=ws_id, status="running")
        # Phase 1 DB read sees 'stopped'; the wait helper then reads the row
        # once to pick its channel, and its first poll sees 'running' (winner
        # finished).
        mock_get_ws.side_effect = [stopped_ws, stopped_ws, running_ws]
        # Claim returns None — another worker already claimed.
        mock_claim.return_value = None

        session = _make_mock_session(initialized=True)
        session.sandbox.is_ready = MagicMock(return_value=True)
        session.sandbox.has_failed = MagicMock(return_value=False)
        mock_session_mgr.get_session.return_value = session

        result = await manager.get_session_for_workspace(ws_id, user_id="user-1")

        mock_claim.assert_awaited_once_with(_STUB_COMPUTER_ID, from_status="stopped")
        # Critical: did not restart — no double-start across workers.
        session.initialize_lazy.assert_not_awaited()
        assert result is session

    @pytest.mark.asyncio
    @cm_patch("db_get_workspace")
    @cm_patch("SessionManager")
    @patch(f"{_LIFECYCLE}.update_workspace_status", new_callable=AsyncMock)
    @cm_patch("update_workspace_activity")
    @patch(f"{_LIFECYCLE}.try_claim_computer_for_start", new_callable=AsyncMock)
    async def test_loser_raises_when_winner_errors(
        self, mock_claim, mock_activity, mock_status, mock_session_mgr, mock_get_ws
    ):
        """If the winning worker's start fails (status → 'error'), waiting
        losers surface a RuntimeError rather than hanging or silent success."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        stopped_ws = _make_workspace(workspace_id=ws_id, status="stopped")
        error_ws = _make_workspace(workspace_id=ws_id, status="error")
        # Phase 1 read, the wait helper's channel read, then the poll.
        mock_get_ws.side_effect = [stopped_ws, stopped_ws, error_ws]
        mock_claim.return_value = None

        with pytest.raises(RuntimeError, match="failed to start"):
            await manager.get_session_for_workspace(ws_id, user_id="user-1")

    @pytest.mark.asyncio
    @cm_patch("db_get_workspace")
    async def test_wait_helper_times_out_when_stuck(self, mock_get_ws):
        """If status sits in 'starting' past the timeout (winner died mid-
        start), the wait helper raises rather than waiting forever."""
        manager = self._make_manager()
        manager.start_wait_timeout = 0.1
        manager.start_wait_poll_interval = 0.02
        ws_id = str(uuid.uuid4())
        starting_ws = _make_workspace(workspace_id=ws_id, status="starting")
        mock_get_ws.return_value = starting_ws

        with pytest.raises(RuntimeError, match="stuck in 'starting'"):
            await manager._wait_for_start_completion(ws_id)

    @pytest.mark.asyncio
    @cm_patch("db_get_workspace")
    async def test_wait_helper_raises_on_deletion(self, mock_get_ws):
        """Workspace deleted while waiting → ValueError (caller must not
        keep polling a row that no longer exists)."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        mock_get_ws.return_value = None

        with pytest.raises(ValueError, match="not found"):
            await manager._wait_for_start_completion(ws_id)

    @pytest.mark.asyncio
    @cm_patch("db_get_workspace")
    @cm_patch("SessionManager")
    @patch(f"{_LIFECYCLE}.update_workspace_status", new_callable=AsyncMock)
    @cm_patch("update_workspace_activity")
    @patch(f"{_LIFECYCLE}.try_claim_computer_for_start", new_callable=AsyncMock)
    async def test_start_wait_does_not_hold_workspace_lock(
        self, mock_claim, mock_activity, mock_status, mock_session_mgr, mock_get_ws
    ):
        """Regression: the cross-worker start wait must run OUTSIDE the per-
        workspace lock. Otherwise a 60-300s archived cold-start head-of-line
        blocks every other op on that workspace (stop/delete/another get)
        behind the 60s lock-acquire ceiling."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        starting_ws = _make_workspace(workspace_id=ws_id, status="starting")
        running_ws = _make_workspace(workspace_id=ws_id, status="running")
        mock_get_ws.side_effect = [starting_ws, running_ws]
        mock_claim.return_value = None  # 'starting' arrival skips the claim

        session = _make_mock_session(initialized=True)
        session.sandbox.is_ready = MagicMock(return_value=True)
        session.sandbox.has_failed = MagicMock(return_value=False)
        mock_session_mgr.get_session.return_value = session

        # Gate the wait so we can probe the lock while the caller is parked in it.
        release = asyncio.Event()

        async def _blocking_wait(workspace_id, *a, **k):
            await release.wait()
            return running_ws

        manager._wait_for_start_completion = AsyncMock(side_effect=_blocking_wait)

        waiter = asyncio.create_task(
            manager.get_session_for_workspace(ws_id, user_id="user-1")
        )
        await asyncio.sleep(0.05)  # let the waiter reach the gated wait
        assert not waiter.done()

        # The per-workspace lock MUST be free while the waiter waits. Short
        # timeout so a regression (wait-inside-lock) fails fast instead of
        # hanging the full 60s lock-acquire ceiling.
        async def _probe():
            async with manager._observed_lock(_STUB_COMPUTER_ID, "probe"):
                return True

        assert await asyncio.wait_for(_probe(), timeout=2.0) is True

        release.set()
        result = await waiter
        assert result is session

    @pytest.mark.asyncio
    @cm_patch("db_get_workspace")
    @cm_patch("SessionManager")
    @patch(f"{_LIFECYCLE}.update_workspace_status", new_callable=AsyncMock)
    @cm_patch("update_workspace_activity")
    @patch(f"{_LIFECYCLE}.try_claim_computer_for_start", new_callable=AsyncMock)
    async def test_loser_retries_once_when_owner_reverts_to_stopped(
        self, mock_claim, mock_activity, mock_status, mock_session_mgr, mock_get_ws
    ):
        """If the winner fails and reverts 'starting'→'stopped', the waiting
        loser retries the start once and becomes the owner (restart runs)."""
        manager = self._make_manager()
        manager._compute_sandbox_config_hash = MagicMock(return_value="match")
        ws_id = str(uuid.uuid4())
        stopped_ws = _make_workspace(
            workspace_id=ws_id,
            status="stopped",
            config={"sandbox_config_hash": "match"},
        )
        mock_get_ws.return_value = stopped_ws  # both Phase 1 reads see 'stopped'
        # First claim loses; the post-revert retry wins.
        mock_claim.side_effect = [None, _claimed_computer(ws_id)]
        # Owner failed and reverted the row back to 'stopped'.
        manager._wait_for_start_completion = AsyncMock(return_value=stopped_ws)

        session = _make_mock_session()
        session.sandbox.ensure_sandbox_ready = AsyncMock()
        mock_session_mgr.get_session.return_value = session

        await manager.get_session_for_workspace(ws_id, user_id="user-1")

        assert mock_claim.await_count == 2
        session.initialize_lazy.assert_awaited_once()


# ---------------------------------------------------------------------------
# _entitled_tier — lazy spec reclaim at (re)provision time. Keeps the persisted
# tier unless the platform confirms the owner's entitlement lapsed, in which
# case it persists back to standard — but keeps the elevated size when the check
# is inconclusive (OSS / no user) or the backed-up files won't fit standard.
# ---------------------------------------------------------------------------


class TestEntitledTier:
    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    def _make_manager(self):
        return WorkspaceManager.get_instance(config=_make_config_with_tiers())

    _LOST = "src.server.dependencies.usage_limits.spec_entitlement_lost"
    _SET_TIER = (
        "src.server.services.workspace_entitlements.db_set_computer_resource_tier"
    )

    @pytest.mark.asyncio
    async def test_standard_tier_short_circuits(self):
        """standard tier is never reclaimed — no entitlement check."""
        manager = self._make_manager()
        binding = _binding("ws-1", resource_tier="standard")

        with patch(self._LOST, new_callable=AsyncMock) as mock_lost:
            assert await manager._entitled_tier(binding, "user-1") == "standard"
        mock_lost.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_no_user_keeps_elevated_tier(self):
        """No owner to reconcile against → keep the elevated tier, no check."""
        manager = self._make_manager()
        binding = _binding("ws-1", resource_tier="max")

        with patch(self._LOST, new_callable=AsyncMock) as mock_lost:
            assert await manager._entitled_tier(binding, None) == "max"
        mock_lost.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_still_entitled_keeps_tier_no_write(self):
        """Entitlement held → keep the elevated tier, nothing persisted."""
        manager = self._make_manager()
        binding = _binding("ws-1", resource_tier="performance")

        with (
            patch(self._LOST, new_callable=AsyncMock, return_value=False),
            patch(self._SET_TIER) as mock_set_tier,
        ):
            assert await manager._entitled_tier(binding, "user-1") == "performance"
        mock_set_tier.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_lost_and_disk_fits_reclaims_to_standard(self):
        """Lost entitlement + files fit standard → persist and return standard."""
        manager = self._make_manager()
        binding = _binding("ws-1", resource_tier="max")
        manager._assert_machine_disk_fits = AsyncMock()

        with (
            patch(self._LOST, new_callable=AsyncMock, return_value=True),
            patch(self._SET_TIER) as mock_set_tier,
        ):
            assert await manager._entitled_tier(binding, "user-1") == "standard"
        # The tier is the machine's, so the reclaim writes the computer row.
        mock_set_tier.assert_awaited_once_with(_STUB_COMPUTER_ID, "standard")

    @pytest.mark.asyncio
    async def test_lost_but_files_overflow_keeps_size(self):
        """Lost entitlement but files exceed the standard disk → keep the size
        (data safety over enforcement), nothing persisted."""
        manager = self._make_manager()
        binding = _binding("ws-1", resource_tier="max")
        manager._assert_machine_disk_fits = AsyncMock(
            side_effect=RuntimeError("Cannot downgrade")
        )

        with (
            patch(self._LOST, new_callable=AsyncMock, return_value=True),
            patch(self._SET_TIER) as mock_set_tier,
        ):
            assert await manager._entitled_tier(binding, "user-1") == "max"
        mock_set_tier.assert_not_awaited()


# ---------------------------------------------------------------------------
# _entitled_always_on — lazy always-on reclaim at (re)provision time. Mirrors
# _entitled_tier: keeps the persisted flag unless the platform confirms the
# always-on entitlement lapsed, in which case it clears the flag and returns
# False so the sandbox comes back auto-stop-enabled. The idle reaper only walks
# running rows, so this is what reconciles a workspace stopped when its plan
# lapsed. Fail-safe: keeps always-on when the check is inconclusive (OSS / no
# user).
# ---------------------------------------------------------------------------


class TestEntitledAlwaysOn:
    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    def _make_manager(self):
        return WorkspaceManager.get_instance(config=_make_config_with_tiers())

    _LOST = "src.server.dependencies.usage_limits.always_on_entitlement_lost"
    _SET_AO = "src.server.services.workspace_entitlements.db_set_computer_always_on"

    @pytest.mark.asyncio
    async def test_not_always_on_short_circuits(self):
        """A machine that isn't always-on is never checked."""
        manager = self._make_manager()
        binding = _binding("ws-1", is_always_on=False)

        with patch(self._LOST, new_callable=AsyncMock) as mock_lost:
            assert await manager._entitled_always_on(binding, "user-1") is False
        mock_lost.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_no_user_keeps_flag(self):
        """No owner to reconcile against → keep always-on, no check."""
        manager = self._make_manager()
        binding = _binding("ws-1", is_always_on=True)

        with patch(self._LOST, new_callable=AsyncMock) as mock_lost:
            assert await manager._entitled_always_on(binding, None) is True
        mock_lost.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_still_entitled_keeps_flag_no_write(self):
        """Entitlement held → keep always-on, nothing persisted."""
        manager = self._make_manager()
        binding = _binding("ws-1", is_always_on=True)

        with (
            patch(self._LOST, new_callable=AsyncMock, return_value=False),
            patch(self._SET_AO) as mock_set_ao,
        ):
            assert await manager._entitled_always_on(binding, "user-1") is True
        mock_set_ao.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_lost_reclaims_and_clears_flag(self):
        """Lost entitlement → clear the flag and return False (auto-stop back on)."""
        manager = self._make_manager()
        binding = _binding("ws-1", is_always_on=True)

        with (
            patch(self._LOST, new_callable=AsyncMock, return_value=True),
            patch(self._SET_AO, new_callable=AsyncMock) as mock_set_ao,
        ):
            assert await manager._entitled_always_on(binding, "user-1") is False
        mock_set_ao.assert_awaited_once_with(_STUB_COMPUTER_ID, False)


# ---------------------------------------------------------------------------
# Lazy spec reclaim at (re)provision — Phase-2 arm (_maybe_reclaim_lazy_tier)
# plus the _recover_sandbox seam that rebuilds at the reclaimed tier. The
# reclaim deliberately runs OUTSIDE the per-workspace lock (Phase 1 must stay
# fast); the wiring tests assert lock freedom directly.
# ---------------------------------------------------------------------------


class TestMaybeReclaimLazyTier:
    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    def _make_manager(self):
        return WorkspaceManager.get_instance(config=_make_config())

    @staticmethod
    def _elevated(**overrides):
        return {
            **_STUB_COMPUTER,
            "resource_tier": "max",
            "provider_ref": "sandbox-1",
            "status": "starting",
            **overrides,
        }

    @pytest.mark.asyncio
    @patch(f"{_ENTITLEMENTS}.get_computer")
    async def test_lapsed_tier_destroys_and_recovers(self, mock_get_computer):
        """Lapsed entitlement → destroy the reconnected sandbox, clear the
        session (identity-guarded), and recover at the reclaimed tier."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        computer = self._elevated()
        mock_get_computer.return_value = computer
        session = _make_mock_session()
        manager._machine(_STUB_COMPUTER_ID).session = session

        manager._entitled_tier = AsyncMock(return_value="standard")
        manager._destroy_sandbox = AsyncMock()
        manager._clear_session = AsyncMock()
        recovered = _make_mock_session()
        manager._recover_sandbox = AsyncMock(return_value=recovered)

        binding = _binding(ws_id, computer)
        result = await manager._maybe_reclaim_lazy_tier(binding, "user-1", session)

        assert result is recovered
        manager._destroy_sandbox.assert_awaited_once_with("sandbox-1", binding=binding)
        # Sandbox, session and tier all hang off the machine, so every arm of
        # the reclaim is keyed by the computer rather than the project.
        manager._clear_session.assert_awaited_once_with(
            _STUB_COMPUTER_ID, evict_session=session
        )
        manager._recover_sandbox.assert_awaited_once_with(binding, "user-1", ANY)
        assert not manager._pending_lazy_start(_STUB_COMPUTER_ID)

    @pytest.mark.asyncio
    @patch(f"{_ENTITLEMENTS}.get_computer")
    async def test_still_entitled_returns_none(self, mock_get_computer):
        """Still entitled → None (proceed on the existing sandbox), no teardown."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        mock_get_computer.return_value = self._elevated()
        session = _make_mock_session()
        manager._entitled_tier = AsyncMock(return_value="max")
        manager._destroy_sandbox = AsyncMock()
        manager._recover_sandbox = AsyncMock()

        assert (
            await manager._maybe_reclaim_lazy_tier(
                _binding(ws_id, self._elevated()), "user-1", session
            )
            is None
        )
        manager._destroy_sandbox.assert_not_awaited()
        manager._recover_sandbox.assert_not_awaited()

    @pytest.mark.asyncio
    @patch(f"{_ENTITLEMENTS}.get_computer")
    async def test_standard_tier_skips_entitlement_check(self, mock_get_computer):
        """standard tier → no platform round-trip at all."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        mock_get_computer.return_value = self._elevated(resource_tier="standard")
        session = _make_mock_session()
        manager._entitled_tier = AsyncMock()

        assert (
            await manager._maybe_reclaim_lazy_tier(_binding(ws_id), "user-1", session)
            is None
        )
        manager._entitled_tier.assert_not_awaited()

    @pytest.mark.asyncio
    @patch(f"{_ENTITLEMENTS}.get_computer")
    async def test_a_machine_deleted_under_the_reclaim_is_left_alone(
        self, mock_get_computer
    ):
        """The binding was frozen at turn start, so the row is re-read; a gone
        machine has nothing left to resize."""
        manager = self._make_manager()
        mock_get_computer.return_value = None
        manager._entitled_tier = AsyncMock()
        manager._destroy_sandbox = AsyncMock()

        assert (
            await manager._maybe_reclaim_lazy_tier(
                _binding(str(uuid.uuid4()), self._elevated()),
                "user-1",
                _make_mock_session(),
            )
            is None
        )
        manager._entitled_tier.assert_not_awaited()
        manager._destroy_sandbox.assert_not_awaited()

    @pytest.mark.asyncio
    @patch(f"{_ENTITLEMENTS}.get_computer")
    async def test_destroy_failure_still_recovers(self, mock_get_computer):
        """Destroying the outsized sandbox is best-effort — a failure logs and
        still proceeds to recover (Daytona reclaims the orphan later)."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        mock_get_computer.return_value = self._elevated()
        session = _make_mock_session()
        manager._entitled_tier = AsyncMock(return_value="standard")
        manager._destroy_sandbox = AsyncMock(side_effect=RuntimeError("destroy boom"))
        manager._clear_session = AsyncMock()
        recovered = _make_mock_session()
        manager._recover_sandbox = AsyncMock(return_value=recovered)

        result = await manager._maybe_reclaim_lazy_tier(
            _binding(ws_id, self._elevated()), "user-1", session
        )

        assert result is recovered
        manager._recover_sandbox.assert_awaited_once()

    @pytest.mark.asyncio
    @patch(f"{_ENTITLEMENTS}.get_computer")
    async def test_recovery_failure_rearms_pending_and_raises(self, mock_get_computer):
        """A failed recovery re-arms the record's pending_lazy_sync flag so
        Phase 2's generic handler reverts the claim row to 'stopped' and
        re-raises, instead of tolerating the failure as a warm re-sync hiccup."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        mock_get_computer.return_value = self._elevated()
        session = _make_mock_session()
        manager._entitled_tier = AsyncMock(return_value="standard")
        manager._destroy_sandbox = AsyncMock()
        manager._clear_session = AsyncMock()
        manager._recover_sandbox = AsyncMock(side_effect=RuntimeError("recover boom"))

        with pytest.raises(RuntimeError, match="recover boom"):
            await manager._maybe_reclaim_lazy_tier(
                _binding(ws_id, self._elevated()), "user-1", session
            )
        assert manager._pending_lazy_start(_STUB_COMPUTER_ID)


class TestPhase2TierReclaim:
    """Wiring: the reclaim runs in Phase 2 of get_session_for_workspace —
    after the lazy restart, with the machine lock released."""

    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    def _make_manager(self):
        manager = WorkspaceManager.get_instance(config=_make_config())
        manager._sync_sandbox_assets = AsyncMock()
        manager._maybe_restore_files = AsyncMock()
        manager._maybe_migrate_sandbox = AsyncMock(return_value=None)
        manager._apply_session_mcp = AsyncMock(return_value=None)
        manager._apply_autostop_for_always_on = AsyncMock()
        manager._reconcile_skills = AsyncMock()
        manager._ensure_project_attached = AsyncMock()
        manager._compute_sandbox_config_hash = MagicMock(return_value="stable")
        return manager

    _ELEVATED_COMPUTER = {
        **_STUB_COMPUTER,
        "status": "stopped",
        "resource_tier": "max",
        "provider_ref": "sandbox-1",
    }

    @staticmethod
    def _elevated_workspace(workspace_id, status="stopped"):
        return _make_workspace(
            workspace_id=workspace_id,
            status=status,
            sandbox_id="sandbox-1",
            resource_tier="max",
            config={"sandbox_config_hash": "stable"},
        )

    @pytest.mark.asyncio
    @patch(f"{_ENTITLEMENTS}.get_computer", new_callable=AsyncMock)
    @patch(f"{_LIFECYCLE}.get_computer", new_callable=AsyncMock)
    @cm_patch("db_get_workspace")
    @cm_patch("SessionManager")
    @cm_patch("update_workspace_activity")
    @patch(f"{_LIFECYCLE}.update_workspace_status", new_callable=AsyncMock)
    @patch(f"{_LIFECYCLE}.try_claim_computer_for_start", new_callable=AsyncMock)
    async def test_lapsed_tier_reclaims_off_the_lock(
        self,
        mock_claim,
        mock_status,
        mock_activity,
        mock_session_mgr,
        mock_get_ws,
        mock_get_computer,
        mock_ent_get_computer,
    ):
        """Lapsed entitlement on a lazy restart → recovery runs with the
        machine lock FREE (Phase 2), and the caller gets the recovered
        session; the Phase-1 reconnect itself stayed lazy."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        workspace = self._elevated_workspace(ws_id)
        mock_get_ws.return_value = workspace
        mock_claim.return_value = dict(self._ELEVATED_COMPUTER)
        mock_get_computer.return_value = dict(self._ELEVATED_COMPUTER)
        mock_ent_get_computer.return_value = dict(self._ELEVATED_COMPUTER)
        mock_session_mgr.cleanup_session = AsyncMock()

        lazy_session = _make_mock_session()
        mock_session_mgr.get_session.return_value = lazy_session

        manager._entitled_tier = AsyncMock(return_value="standard")
        manager._destroy_sandbox = AsyncMock()
        recovered = _make_mock_session()

        lock_was_held: list[bool] = []

        async def recover_probe(binding, user_id, core_config):
            lock = manager._machine_lock(binding.computer_id)
            lock_was_held.append(lock.locked())
            return recovered

        manager._recover_sandbox = AsyncMock(side_effect=recover_probe)

        with _patch_resolve(self._ELEVATED_COMPUTER):
            result = await manager.get_session_for_workspace(ws_id, user_id="user-1")

        assert result is recovered
        lazy_session.initialize_lazy.assert_awaited_once()
        manager._destroy_sandbox.assert_awaited_once_with("sandbox-1", binding=ANY)
        assert lock_was_held == [False]

    @pytest.mark.asyncio
    @patch(f"{_ENTITLEMENTS}.get_computer", new_callable=AsyncMock)
    @patch(f"{_LIFECYCLE}.get_computer", new_callable=AsyncMock)
    @cm_patch("db_get_workspace")
    @cm_patch("SessionManager")
    @cm_patch("update_workspace_activity")
    @patch(f"{_LIFECYCLE}.update_workspace_status", new_callable=AsyncMock)
    @patch(f"{_LIFECYCLE}.try_claim_computer_for_start", new_callable=AsyncMock)
    async def test_entitled_tier_promotes_normally(
        self,
        mock_claim,
        mock_status,
        mock_activity,
        mock_session_mgr,
        mock_get_ws,
        mock_get_computer,
        mock_ent_get_computer,
    ):
        """Still entitled → no destroy/recover; the lazy start promotes to
        'running' exactly as before the reclaim existed."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        mock_get_ws.return_value = self._elevated_workspace(ws_id)
        mock_claim.return_value = dict(self._ELEVATED_COMPUTER)
        mock_get_computer.return_value = dict(self._ELEVATED_COMPUTER)
        mock_ent_get_computer.return_value = dict(self._ELEVATED_COMPUTER)

        lazy_session = _make_mock_session()
        mock_session_mgr.get_session.return_value = lazy_session

        manager._entitled_tier = AsyncMock(return_value="max")
        manager._destroy_sandbox = AsyncMock()
        manager._recover_sandbox = AsyncMock()

        with _patch_resolve(self._ELEVATED_COMPUTER):
            result = await manager.get_session_for_workspace(ws_id, user_id="user-1")

        assert result is lazy_session
        manager._destroy_sandbox.assert_not_awaited()
        manager._recover_sandbox.assert_not_awaited()
        statuses = [c.kwargs.get("status") for c in mock_status.await_args_list]
        assert "running" in statuses

    @pytest.mark.asyncio
    @patch(f"{_ENTITLEMENTS}.get_computer", new_callable=AsyncMock)
    @patch(f"{_LIFECYCLE}.get_computer", new_callable=AsyncMock)
    @cm_patch("db_get_workspace")
    @cm_patch("SessionManager")
    @cm_patch("update_workspace_activity")
    @patch(f"{_LIFECYCLE}.update_workspace_status", new_callable=AsyncMock)
    @patch(f"{_LIFECYCLE}.try_claim_computer_for_start", new_callable=AsyncMock)
    async def test_reclaim_recovery_failure_reverts_to_stopped(
        self,
        mock_claim,
        mock_status,
        mock_activity,
        mock_session_mgr,
        mock_get_ws,
        mock_get_computer,
        mock_ent_get_computer,
    ):
        """A failed reclaim recovery reverts the claim row to 'stopped' (via the
        re-armed pending marker) and surfaces the failure to the caller."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        mock_get_ws.return_value = self._elevated_workspace(ws_id)
        mock_claim.return_value = dict(self._ELEVATED_COMPUTER)
        mock_get_computer.return_value = dict(self._ELEVATED_COMPUTER)
        mock_ent_get_computer.return_value = dict(self._ELEVATED_COMPUTER)
        mock_session_mgr.cleanup_session = AsyncMock()

        lazy_session = _make_mock_session()
        mock_session_mgr.get_session.return_value = lazy_session

        manager._entitled_tier = AsyncMock(return_value="standard")
        manager._destroy_sandbox = AsyncMock()
        manager._recover_sandbox = AsyncMock(side_effect=RuntimeError("recover boom"))

        with _patch_resolve(self._ELEVATED_COMPUTER):
            with pytest.raises(RuntimeError, match="recover boom"):
                await manager.get_session_for_workspace(ws_id, user_id="user-1")

        statuses = [c.kwargs.get("status") for c in mock_status.await_args_list]
        assert statuses[-1] == "stopped"


class TestRecoverSandboxEntitledTier:
    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    def _make_manager(self):
        return WorkspaceManager.get_instance(config=_make_config())

    @pytest.mark.asyncio
    @cm_patch("update_workspace_activity")
    @cm_patch("SessionManager")
    @cm_patch("db_get_workspace")
    async def test_provisions_at_entitled_tier(
        self, mock_get_ws, mock_session_mgr, mock_activity
    ):
        """_recover_sandbox sizes the fresh sandbox to the tier _entitled_tier
        returns, not the raw persisted tier (a lapsed 'max' rebuilds at standard)."""
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        workspace = _make_workspace(
            workspace_id=ws_id, status="stopped", resource_tier="max"
        )
        mock_get_ws.return_value = workspace
        session = _make_mock_session()
        mock_session_mgr.get_session.return_value = session

        manager._entitled_tier = AsyncMock(return_value="standard")
        manager._mint_sandbox_tokens = AsyncMock(return_value={})
        manager._apply_session_mcp = AsyncMock(return_value=None)
        manager._sync_sandbox_assets = AsyncMock()
        manager._restore_files = AsyncMock()

        with _patch_machine_bind(workspace, computer_id=_STUB_COMPUTER_ID):
            result = await manager._recover_sandbox(
                _binding(ws_id, resource_tier="max"), "user-1", MagicMock()
            )

        assert result is session
        manager._entitled_tier.assert_awaited_once()
        assert session.initialize.await_args.kwargs["tier"] == "standard"


class TestRecoverSandboxOwnerBackfill:
    """Recovery is reachable from callers that hold no user_id (the cached-session
    fast path returns before the slow path's DB correction). Provisioning without
    an owner resolves that owner's MCP/OAuth tier as empty, so the row supplies
    it here — above every recovery call site."""

    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    def _make_manager(self):
        return WorkspaceManager.get_instance(config=_make_config())

    async def _recover(self, manager, ws_id, user_id):
        session = _make_mock_session()
        manager._entitled_tier = AsyncMock(return_value="standard")
        manager._entitled_always_on = AsyncMock(return_value=False)
        manager._provision_sandbox_session = AsyncMock(return_value=(session, {}))
        await manager._recover_sandbox(_binding(ws_id), user_id, MagicMock())
        return session

    @pytest.mark.asyncio
    @cm_patch("update_workspace_activity")
    @cm_patch("db_get_workspace")
    async def test_a_caller_without_a_user_id_recovers_as_the_row_owner(
        self, mock_get_ws, mock_activity
    ):
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        mock_get_ws.return_value = _make_workspace(
            workspace_id=ws_id, user_id="user-9", status="running"
        )

        await self._recover(manager, ws_id, None)

        assert manager._provision_sandbox_session.await_args.args[1] == "user-9"
        assert manager._entitled_tier.await_args.args[1] == "user-9"
        assert manager._entitled_always_on.await_args.args[1] == "user-9"

    @pytest.mark.asyncio
    @cm_patch("update_workspace_activity")
    @cm_patch("db_get_workspace")
    async def test_an_explicit_caller_identity_is_not_overwritten(
        self, mock_get_ws, mock_activity
    ):
        manager = self._make_manager()
        ws_id = str(uuid.uuid4())
        mock_get_ws.return_value = _make_workspace(
            workspace_id=ws_id, user_id="user-9", status="running"
        )

        await self._recover(manager, ws_id, "user-1")

        assert manager._provision_sandbox_session.await_args.args[1] == "user-1"


# ---------------------------------------------------------------------------
# set_workspace_spec — tier change recreates the sandbox (hosted Daytona can't
# resize a snapshot sandbox). Persist-then-revert on failure; guards against
# tearing the sandbox out from under a live turn or a too-small disk.
# ---------------------------------------------------------------------------


class TestSetWorkspaceSpec:
    """The tier is the machine's, so the project route is a thin address.

    Every branch reads, writes and reverts the computer row; the workspace id
    only names who asked, for the mirror and the error copy.
    """

    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    def _make_manager(self):
        return WorkspaceManager.get_instance(config=_make_config_with_tiers())

    @staticmethod
    def _computer(**overrides):
        return {**_STUB_COMPUTER, "provider_ref": "sb-1", **overrides}

    @pytest.mark.asyncio
    @patch(f"{_ENTITLEMENTS}.get_computer", new_callable=AsyncMock)
    async def test_unknown_tier_raises_before_db(self, mock_get_computer):
        """An unknown tier is rejected before the machine row is read."""
        manager = self._make_manager()
        with pytest.raises(ValueError, match="Unknown resource tier"):
            await manager.set_workspace_spec("ws-1", "titanium")
        mock_get_computer.assert_not_called()

    @pytest.mark.asyncio
    @patch(f"{_ENTITLEMENTS}.db_get_workspace", new_callable=AsyncMock)
    @patch(f"{_ENTITLEMENTS}.db_set_computer_resource_tier", new_callable=AsyncMock)
    @patch(f"{_ENTITLEMENTS}.get_computer", new_callable=AsyncMock)
    async def test_never_started_persists_tier_only(
        self, mock_get_computer, mock_set_tier, mock_get_ws
    ):
        """No sandbox yet → just persist the tier; no recreate."""
        manager = self._make_manager()
        mock_get_computer.return_value = self._computer(
            status="creating", provider_ref=None
        )
        mock_get_ws.return_value = _make_workspace(workspace_id="ws-1")
        manager._recover_sandbox = AsyncMock()

        await manager.set_workspace_spec("ws-1", "performance")

        mock_set_tier.assert_awaited_once_with(_STUB_COMPUTER_ID, "performance")
        manager._recover_sandbox.assert_not_awaited()

    @pytest.mark.asyncio
    @patch(f"{_ENTITLEMENTS}.db_get_workspace", new_callable=AsyncMock)
    @patch(f"{_ENTITLEMENTS}.db_set_computer_resource_tier", new_callable=AsyncMock)
    @patch(f"{_ENTITLEMENTS}.get_computer", new_callable=AsyncMock)
    async def test_already_at_tier_is_noop(
        self, mock_get_computer, mock_set_tier, mock_get_ws
    ):
        """Same tier + live sandbox → early return, nothing persisted."""
        manager = self._make_manager()
        mock_get_computer.return_value = self._computer(resource_tier="max")
        workspace = _make_workspace(workspace_id="ws-1")
        mock_get_ws.return_value = workspace

        result = await manager.set_workspace_spec("ws-1", "max")

        # The project route answers with the project row it was addressed by.
        assert result is workspace
        mock_set_tier.assert_not_awaited()

    @pytest.mark.asyncio
    @patch(f"{_ENTITLEMENTS}.SessionManager")
    @cm_patch("SessionManager")
    @patch(f"{_ENTITLEMENTS}.db_get_workspace", new_callable=AsyncMock)
    @patch(f"{_ENTITLEMENTS}.update_computer_status", new_callable=AsyncMock)
    @patch(f"{_ENTITLEMENTS}.db_set_computer_resource_tier", new_callable=AsyncMock)
    @patch(f"{_ENTITLEMENTS}.get_computer", new_callable=AsyncMock)
    @patch(
        f"{_ENTITLEMENTS}.try_claim_computer_for_start",
        new_callable=AsyncMock,
    )
    async def test_running_recreate_failure_marks_stopped_and_reverts_tier(
        self,
        mock_claim,
        mock_get_computer,
        mock_set_tier,
        mock_status,
        mock_get_ws,
        mock_cm_session_mgr,
        mock_ent_session_mgr,
    ):
        """A failed recreate flips the row to stopped (so the next start
        self-heals via claim -> restart -> SandboxGone -> recover) AND reverts
        the persisted tier."""
        manager = self._make_manager()
        mock_get_computer.return_value = self._computer(resource_tier="standard")
        mock_get_ws.return_value = _make_workspace(workspace_id="ws-1")
        mock_claim.return_value = self._computer(status="starting")
        mock_cm_session_mgr.cleanup_session = AsyncMock()
        manager._machine(_STUB_COMPUTER_ID).session = MagicMock(sandbox=MagicMock())
        manager._backup_machine_files_to_db = AsyncMock()
        manager._destroy_sandbox = AsyncMock()
        manager._recover_sandbox = AsyncMock(
            side_effect=RuntimeError("snapshot build failed")
        )

        with _patch_machine_activity():
            with pytest.raises(RuntimeError, match="snapshot build failed"):
                await manager.set_workspace_spec("ws-1", "max", user_id="user-1")

        # Row marked stopped (not terminal 'error') so the next start recovers.
        mock_status.assert_awaited_once_with(_STUB_COMPUTER_ID, ComputerStatus.STOPPED)
        # Tier persisted to the target first, then reverted on the outer except.
        assert mock_set_tier.await_args_list[0].args == (_STUB_COMPUTER_ID, "max")
        assert mock_set_tier.await_args_list[-1].args == (
            _STUB_COMPUTER_ID,
            "standard",
        )

    @pytest.mark.asyncio
    @patch(f"{_ENTITLEMENTS}.db_get_workspace", new_callable=AsyncMock)
    @patch(f"{_ENTITLEMENTS}.db_set_computer_resource_tier", new_callable=AsyncMock)
    @patch(f"{_ENTITLEMENTS}.get_computer", new_callable=AsyncMock)
    async def test_running_without_attached_session_refuses_and_reverts(
        self, mock_get_computer, mock_set_tier, mock_get_ws
    ):
        """Running on another replica (no local session): refuse before teardown —
        the backup would silently no-op and destroy the only copy of the files."""
        manager = self._make_manager()
        mock_get_computer.return_value = self._computer(resource_tier="standard")
        mock_get_ws.return_value = _make_workspace(workspace_id="ws-1")
        manager._destroy_sandbox = AsyncMock()
        manager._recover_sandbox = AsyncMock()

        with _patch_machine_activity():
            with pytest.raises(RuntimeError, match="not attached"):
                await manager.set_workspace_spec("ws-1", "max", user_id="user-1")

        manager._destroy_sandbox.assert_not_awaited()
        manager._recover_sandbox.assert_not_awaited()
        assert mock_set_tier.await_args_list[-1].args == (
            _STUB_COMPUTER_ID,
            "standard",
        )

    @pytest.mark.asyncio
    @patch(f"{_ENTITLEMENTS}.db_get_workspace", new_callable=AsyncMock)
    @patch(f"{_ENTITLEMENTS}.db_set_computer_resource_tier", new_callable=AsyncMock)
    @patch(f"{_ENTITLEMENTS}.get_computer", new_callable=AsyncMock)
    async def test_a_sibling_turn_on_the_machine_refuses_and_reverts(
        self, mock_get_computer, mock_set_tier, mock_get_ws
    ):
        """An in-flight agent turn blocks the recreate (would abort execute_code).

        The gate is machine-wide: the sandbox being rebuilt is shared, so a turn
        on any project on it is enough to refuse, not only one on the project
        the request named.
        """
        manager = self._make_manager()
        mock_get_computer.return_value = self._computer(resource_tier="standard")
        mock_get_ws.return_value = _make_workspace(workspace_id="ws-1")
        manager._backup_machine_files_to_db = AsyncMock()
        manager._recover_sandbox = AsyncMock()

        with _patch_machine_activity(active=True):
            with pytest.raises(RuntimeError, match="agent turn is running"):
                await manager.set_workspace_spec("ws-1", "max", user_id="user-1")

        # Refused before any teardown; tier reverted to the original.
        manager._backup_machine_files_to_db.assert_not_awaited()
        manager._recover_sandbox.assert_not_awaited()
        assert mock_set_tier.await_args_list[-1].args == (
            _STUB_COMPUTER_ID,
            "standard",
        )

    @pytest.mark.asyncio
    @patch(f"{_ENTITLEMENTS}.db_get_workspace", new_callable=AsyncMock)
    @patch(f"{_ENTITLEMENTS}.db_set_computer_resource_tier", new_callable=AsyncMock)
    @patch(f"{_ENTITLEMENTS}.get_computer", new_callable=AsyncMock)
    async def test_stopped_destroys_sandbox_for_recreate_on_next_start(
        self, mock_get_computer, mock_set_tier, mock_get_ws
    ):
        """A stopped sandbox is destroyed so the next start rebuilds it at the new tier."""
        manager = self._make_manager()
        mock_get_computer.return_value = self._computer(
            status="stopped", resource_tier="standard"
        )
        mock_get_ws.return_value = _make_workspace(workspace_id="ws-1")
        manager._destroy_sandbox = AsyncMock()

        await manager.set_workspace_spec("ws-1", "max", user_id="user-1")

        manager._destroy_sandbox.assert_awaited_once_with("sb-1", binding=ANY)
        # Upgrade succeeds → tier stays at the target, never reverted.
        assert mock_set_tier.await_args_list[-1].args == (_STUB_COMPUTER_ID, "max")

    @pytest.mark.asyncio
    @patch(f"{_ENTITLEMENTS}.get_workspace_total_size", new_callable=AsyncMock)
    @patch(f"{_ENTITLEMENTS}.db_get_workspace", new_callable=AsyncMock)
    @patch(f"{_ENTITLEMENTS}.db_set_computer_resource_tier", new_callable=AsyncMock)
    @patch(f"{_ENTITLEMENTS}.get_computer", new_callable=AsyncMock)
    async def test_downgrade_rejected_when_the_machines_files_exceed_the_disk(
        self, mock_get_computer, mock_set_tier, mock_get_ws, mock_size
    ):
        """A downgrade whose backed-up files overflow the smaller disk is refused.

        The disk is the machine's, so the guard sums every project on it: asking
        only about the one that requested the change is how a shared machine
        passes a downgrade its combined files cannot fit.
        """
        manager = self._make_manager()
        # max (10 GiB) → standard (3 GiB); 4 GiB per project won't fit either way.
        mock_get_computer.return_value = self._computer(
            status="stopped", resource_tier="max"
        )
        mock_get_ws.return_value = _make_workspace(workspace_id="ws-1")
        mock_size.return_value = 4 * 1024**3
        manager._destroy_sandbox = AsyncMock()

        with _patch_live_ids("ws-1", "sibling-ws") as mock_live:
            with pytest.raises(RuntimeError, match="Cannot downgrade"):
                await manager.set_workspace_spec("ws-1", "standard", user_id="user-1")

        mock_live.assert_awaited_with(_STUB_COMPUTER_ID)
        assert mock_size.await_count == 2
        # Refused before teardown; tier reverted to the original.
        manager._destroy_sandbox.assert_not_awaited()
        assert mock_set_tier.await_args_list[-1].args == (_STUB_COMPUTER_ID, "max")

    def _running_machine(self, manager):
        """A live machine this worker holds the session for, with the tier
        change's teardown steps stubbed out around the recreate."""
        manager._machine(_STUB_COMPUTER_ID).session = MagicMock(sandbox=MagicMock())
        manager._backup_machine_files_to_db = AsyncMock()
        manager._destroy_sandbox = AsyncMock()
        manager._recover_sandbox = AsyncMock()
        manager._workspace_folder = AsyncMock(return_value="oldest-1a2b")

    @pytest.mark.asyncio
    @patch(f"{_ENTITLEMENTS}.SessionManager")
    @cm_patch("SessionManager")
    @patch(f"{_ENTITLEMENTS}.db_get_workspace", new_callable=AsyncMock)
    @patch(f"{_ENTITLEMENTS}.db_set_computer_resource_tier", new_callable=AsyncMock)
    @patch(f"{_ENTITLEMENTS}.get_computer", new_callable=AsyncMock)
    @patch(
        f"{_ENTITLEMENTS}.try_claim_computer_for_start",
        new_callable=AsyncMock,
    )
    async def test_the_recreate_is_sized_from_the_tier_just_persisted(
        self,
        mock_claim,
        mock_get_computer,
        mock_set_tier,
        mock_get_ws,
        mock_cm_session_mgr,
        mock_ent_session_mgr,
    ):
        """The recreate sizes from the binding, which is built after the write.

        Built from the row the branch was decided on, it would carry the tier
        the change is moving away from, and hosted Daytona bakes that size into
        the snapshot it creates from.
        """
        manager = self._make_manager()
        mock_get_computer.return_value = self._computer(resource_tier="standard")
        mock_get_ws.return_value = _make_workspace(workspace_id="ws-1")
        mock_claim.return_value = self._computer(status="starting")
        mock_cm_session_mgr.cleanup_session = AsyncMock()
        self._running_machine(manager)

        with _patch_machine_activity():
            await manager.set_workspace_spec("ws-1", "max", user_id="user-1")

        assert manager._recover_sandbox.await_args.args[0].resource_tier == "max"

    @pytest.mark.asyncio
    @patch(f"{_ENTITLEMENTS}.SessionManager")
    @cm_patch("SessionManager")
    @patch(f"{_ENTITLEMENTS}.db_get_workspace", new_callable=AsyncMock)
    @patch(f"{_ENTITLEMENTS}.db_set_computer_resource_tier", new_callable=AsyncMock)
    @patch(f"{_ENTITLEMENTS}.get_computer", new_callable=AsyncMock)
    @patch(
        f"{_ENTITLEMENTS}.try_claim_computer_for_start",
        new_callable=AsyncMock,
    )
    async def test_a_machine_addressed_change_acts_through_its_own_project(
        self,
        mock_claim,
        mock_get_computer,
        mock_set_tier,
        mock_get_ws,
        mock_cm_session_mgr,
        mock_ent_session_mgr,
    ):
        """The machine route names no project, and the recreate needs one.

        Binding a project is the write that publishes the new sandbox ref, and
        its folder is where the new sandbox is laid out; a recreate that named
        neither deleted the sandbox and bound nothing back.
        """
        manager = self._make_manager()
        mock_get_computer.return_value = self._computer(resource_tier="standard")
        mock_get_ws.return_value = _make_workspace(workspace_id="ws-oldest")
        mock_claim.return_value = self._computer(status="starting")
        mock_cm_session_mgr.cleanup_session = AsyncMock()
        self._running_machine(manager)

        with _patch_machine_activity(), _patch_live_ids("ws-oldest", "ws-younger"):
            await manager.set_computer_spec(_STUB_COMPUTER_ID, "max", user_id="user-1")

        binding = manager._recover_sandbox.await_args.args[0]
        assert binding.workspace_id == "ws-oldest"
        assert binding.dir_name == "oldest-1a2b"

    @pytest.mark.asyncio
    @patch(f"{_ENTITLEMENTS}.db_get_workspace", new_callable=AsyncMock)
    @patch(f"{_ENTITLEMENTS}.db_set_computer_resource_tier", new_callable=AsyncMock)
    @patch(f"{_ENTITLEMENTS}.get_computer", new_callable=AsyncMock)
    async def test_a_machine_with_no_live_project_keeps_its_sandbox(
        self, mock_get_computer, mock_set_tier, mock_get_ws
    ):
        """Nothing to rebuild for, so the running sandbox is left alone."""
        manager = self._make_manager()
        mock_get_computer.return_value = self._computer(resource_tier="standard")
        mock_get_ws.return_value = _make_workspace(workspace_id="ws-1")
        self._running_machine(manager)

        with _patch_machine_activity(), _patch_live_ids():
            with pytest.raises(RuntimeError, match="no live project"):
                await manager.set_computer_spec(
                    _STUB_COMPUTER_ID, "max", user_id="user-1"
                )

        manager._backup_machine_files_to_db.assert_not_awaited()
        manager._recover_sandbox.assert_not_awaited()
        assert mock_set_tier.await_args_list[-1].args == (
            _STUB_COMPUTER_ID,
            "standard",
        )


# ---------------------------------------------------------------------------
# duplicate_workspace — copy files + carried tier into a fresh "<name> (copy)";
# re-check the spec entitlement (a duplicate is a new allocation); eager
# sandbox create; mark the new row error on create failure.
# ---------------------------------------------------------------------------


class TestDuplicateWorkspace:
    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    def _make_manager(self):
        return WorkspaceManager.get_instance(config=_make_config())

    def _install_create_path(self, manager, session):
        """Stub the eager-create collaborators so the lock body runs clean."""
        manager._mint_sandbox_tokens = AsyncMock(return_value={})
        manager._apply_session_mcp = AsyncMock()
        manager._sync_sandbox_assets = AsyncMock()
        manager._restore_files = AsyncMock()
        manager._record_sync = MagicMock()
        manager._update_workspace_config_fields = AsyncMock()
        manager._sandbox_config_stamp = MagicMock(return_value={})

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_entitlements.db_get_workspace")
    async def test_missing_source_rejected(self, mock_get_ws):
        manager = self._make_manager()
        mock_get_ws.return_value = None
        with pytest.raises(ValueError, match="not found"):
            await manager.duplicate_workspace("ws-x", "user-1")

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_entitlements.db_get_workspace")
    async def test_not_owned_rejected(self, mock_get_ws):
        manager = self._make_manager()
        mock_get_ws.return_value = _make_workspace(user_id="someone-else")
        with pytest.raises(ValueError, match="not found"):
            await manager.duplicate_workspace("ws-x", "user-1")

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_entitlements.db_get_workspace")
    async def test_flash_source_rejected(self, mock_get_ws):
        manager = self._make_manager()
        mock_get_ws.return_value = _make_workspace(status="flash", user_id="user-1")
        with pytest.raises(ValueError, match="flash"):
            await manager.duplicate_workspace("ws-x", "user-1")

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_entitlements.copy_workspace_files")
    @patch(
        "src.server.services.workspace_manager.create_workspace_on_computer",
        new_callable=AsyncMock,
    )
    @cm_patch("SessionManager")
    @patch("src.server.services.workspace_entitlements.db_get_workspace")
    async def test_the_copy_joins_the_source_machine_and_builds_nothing(
        self, mock_get_ws, mock_session_mgr, mock_insert, mock_copy
    ):
        """Its sandbox is the machine's first start, which restores the files,
        so a duplicate returns as fast as an ordinary create."""
        manager = self._make_manager()
        source = _make_workspace(
            status="stopped",
            user_id="user-1",
            config={
                "custom": "keep-me",
                "sandbox_config_hash": "abc",
                "sandbox_provider": "daytona",
                "sandbox_working_dir": "/home/workspace",
            },
        )
        mock_get_ws.return_value = source
        new_id = str(uuid.uuid4())
        mock_insert.return_value = _make_workspace(
            workspace_id=new_id,
            status="stopped",
            sandbox_id=None,
            computer_id=_STUB_COMPUTER_ID,
            dir_name="test-workspace-copy-ab12",
        )

        result = await manager.duplicate_workspace(source["workspace_id"], "user-1")

        assert result["computer_id"] == _STUB_COMPUTER_ID
        mock_copy.assert_awaited_once_with(source["workspace_id"], new_id)
        mock_session_mgr.get_session.assert_not_called()
        # Sandbox-identity stamps stripped; unrelated config keys preserved.
        assert mock_insert.call_args.kwargs["config"] == {"custom": "keep-me"}
        assert mock_insert.call_args[0][1] == "Test Workspace (copy)"

    @pytest.mark.asyncio
    @patch("src.server.services.workspace_entitlements.copy_workspace_files")
    @patch(
        "src.server.services.workspace_manager.create_workspace_on_computer",
        new_callable=AsyncMock,
    )
    @patch("src.server.services.workspace_entitlements.db_get_workspace")
    async def test_the_tier_is_not_carried_because_it_is_the_machines(
        self, mock_get_ws, mock_insert, mock_copy
    ):
        """An elevated source and its copy share one computer, so writing a tier
        onto the copy could only disagree with the machine it runs on."""
        manager = self._make_manager()
        source = _make_workspace(
            status="stopped", user_id="user-1", resource_tier="max"
        )
        mock_get_ws.return_value = source
        mock_insert.return_value = _make_workspace(
            workspace_id=str(uuid.uuid4()),
            status="stopped",
            sandbox_id=None,
            computer_id=_STUB_COMPUTER_ID,
            # What the insert shadows off the machine, not off the source.
            resource_tier=_STUB_COMPUTER["resource_tier"],
        )

        result = await manager.duplicate_workspace(source["workspace_id"], "user-1")

        assert "resource_tier" not in mock_insert.call_args.kwargs
        # The row the insert returns shadows its machine, not the source.
        assert result["resource_tier"] == _STUB_COMPUTER["resource_tier"]

    @pytest.mark.asyncio
    @patch(f"{_ENTITLEMENTS}.db_delete_workspace", new_callable=AsyncMock)
    @patch("src.server.services.workspace_entitlements.copy_workspace_files")
    @patch(
        "src.server.services.workspace_manager.create_workspace_on_computer",
        new_callable=AsyncMock,
    )
    @patch("src.server.services.workspace_entitlements.db_get_workspace")
    async def test_a_failed_file_copy_tombstones_the_new_row(
        self, mock_get_ws, mock_insert, mock_copy, mock_delete
    ):
        """A half-copied file set must not be left for the next start to
        restore as if it were the whole workspace.

        The row is tombstoned rather than stamped 'error': status is the
        machine's and mirrors across it, so an 'error' on the copy would move
        the source machine and every sibling on it into a state whose only exit
        is delete and recreate.
        """
        manager = self._make_manager()
        mock_get_ws.return_value = _make_workspace(status="stopped", user_id="user-1")
        new_id = str(uuid.uuid4())
        mock_insert.return_value = _make_workspace(
            workspace_id=new_id,
            status="stopped",
            sandbox_id=None,
            computer_id=_STUB_COMPUTER_ID,
        )
        mock_copy.side_effect = RuntimeError("copy boom")

        with pytest.raises(RuntimeError, match="copy boom"):
            await manager.duplicate_workspace("ws-x", "user-1")

        mock_delete.assert_awaited_once_with(new_id)


# ---------------------------------------------------------------------------
# Platform-secret wiring
# ---------------------------------------------------------------------------


class TestPlatformSecretWiring:
    """Wiring between the platform-secret hooks and session lifecycle."""

    def setup_method(self):
        WorkspaceManager.reset_instance()

    def teardown_method(self):
        WorkspaceManager.reset_instance()

    @pytest.mark.asyncio
    async def test_retire_session_if_present_leaves_the_sandbox_alone(self):
        # Retirement drops both caches without touching the sandbox — the
        # primitive the sweeper needs after restarting a sandbox in place, and
        # the one a stale-identity re-attach uses. A project names the machine
        # whose session is retired; the caches are keyed by that machine.
        manager = WorkspaceManager.get_instance(config=_make_config())
        session = _make_mock_session()
        manager._machine(_STUB_COMPUTER_ID).session = session
        manager._machine(_STUB_COMPUTER_ID).pending_lazy_sync = True
        manager._machine(_STUB_COMPUTER_ID).last_sync_at = time.monotonic()

        with (
            patch(
                "src.server.services.computer_manager._sessions.SessionManager.cleanup_session",
                AsyncMock(),
            ) as cleanup,
            patch(
                "src.server.services.computer_manager._sessions.SessionManager.get_cached_session",
                MagicMock(return_value=session),
            ),
            patch(
                "src.server.services.computer_manager._sessions.SessionManager.remove_session",
                MagicMock(),
            ) as remove,
        ):
            assert (
                await manager.retire_session_if_present("ws-retire", reason="test")
                is True
            )
            # Both caches dropped, so the next get_session builds a fresh
            # Session instead of handing back this one with _initialized=True.
            remove.assert_called_once_with(_STUB_COMPUTER_ID)
            assert manager._cached_session(_STUB_COMPUTER_ID) is None
            assert not manager._pending_lazy_start(_STUB_COMPUTER_ID)
            assert manager._machine(_STUB_COMPUTER_ID).last_sync_at is None
            # The sandbox is NOT destroyed.
            cleanup.assert_not_awaited()
            session.cleanup.assert_not_awaited()

            assert (
                await manager.retire_session_if_present("ws-retire", reason="test")
                is False
            )

    @pytest.mark.asyncio
    async def test_hot_resync_failure_propagates_without_evicting_session(self):
        # The resync is non-destructive and re-checked on every slow-path
        # acquisition, so a failure must NOT evict the session caches (retry
        # is structural) and must NOT touch the lazy-start lifecycle state.
        manager = WorkspaceManager.get_instance(config=_make_config())
        session = _make_mock_session()
        session.sandbox.runtime = MagicMock()
        session.platform_secret_version = None
        manager._machine(_STUB_COMPUTER_ID).session = session
        manager._machine(_STUB_COMPUTER_ID).pending_lazy_sync = True
        manager._machine(_STUB_COMPUTER_ID).pending_tier_recheck = True

        with (
            patch(
                "ptc_agent.core.sandbox.platform_secrets.platform_secrets_active",
                return_value=True,
            ),
            patch(
                "src.server.services.platform_secret_rollout."
                "resync_computer_platform_secret",
                AsyncMock(side_effect=RuntimeError("remount failed")),
            ),
            patch(
                "src.server.services.computer_manager._sessions.SessionManager.cleanup_session",
                AsyncMock(),
            ) as cleanup,
        ):
            with pytest.raises(RuntimeError, match="remount failed"):
                await manager._apply_session_platform_secret(
                    _binding("ws-1"), session, ws_version=1
                )

        cleanup.assert_not_awaited()
        assert manager._cached_session(_STUB_COMPUTER_ID) is session
        assert manager._pending_lazy_start(_STUB_COMPUTER_ID)
        assert manager._machine(_STUB_COMPUTER_ID).pending_tier_recheck is True
        assert session.platform_secret_version is None

    @pytest.mark.asyncio
    async def test_hot_resync_stamps_session_with_applied_generation(self):
        """The mount is the machine's, so the rollout is addressed by computer.

        One sandbox serves every project on it, so resyncing per project would
        remount the same filesystem once per sibling and compare each of their
        shadow generations against one applied generation.
        """
        manager = WorkspaceManager.get_instance(config=_make_config())
        session = _make_mock_session()
        session.sandbox.runtime = MagicMock()
        session.platform_secret_version = None

        resync = AsyncMock(return_value=3)
        with (
            patch(
                "ptc_agent.core.sandbox.platform_secrets.platform_secrets_active",
                return_value=True,
            ),
            patch(
                "src.server.services.platform_secret_rollout."
                "resync_computer_platform_secret",
                resync,
            ),
        ):
            await manager._apply_session_platform_secret(
                _binding("ws-1"), session, ws_version=2
            )

        assert session.platform_secret_version == 3
        kwargs = resync.await_args.kwargs
        assert kwargs["computer_id"] == _STUB_COMPUTER_ID
        assert kwargs["sandbox_id"] == "sandbox-abc"
        assert kwargs["db_version"] == 2
        assert kwargs["applied_generation"] is None

    @pytest.mark.asyncio
    async def test_provision_binds_through_the_cas_in_every_deployment(self):
        """The binding CAS is the only writer, platform Secrets or not.

        It used to run only when platform Secrets were active; everywhere else
        fell through to a last-writer-wins ``update_workspace_status``, so two
        concurrent provisions both "won" and one sandbox was left billed with
        nothing pointing at it.
        """
        manager = WorkspaceManager.get_instance(config=_make_config())
        session = _make_mock_session()
        session.sandbox.runtime = MagicMock()
        workspace = _make_workspace()
        workspace_id = workspace["workspace_id"]

        status = AsyncMock(return_value=workspace)
        with (
            patch.object(manager, "_mint_sandbox_tokens", AsyncMock(return_value={})),
            cm_patch("SessionManager") as session_mgr,
            patch.object(manager, "_apply_session_mcp", AsyncMock(return_value=None)),
            patch.object(manager, "_sync_sandbox_assets", AsyncMock()),
            patch.object(manager, "_reconcile_skills", AsyncMock()),
            patch.object(manager, "_maybe_restore_files", AsyncMock()),
            _patch_machine_bind(workspace, computer_id=_STUB_COMPUTER_ID) as bind,
            _patch_ws_status(status),
        ):
            session_mgr.get_session.return_value = session
            result_session, record = await manager._provision_sandbox_session(
                _binding(workspace_id),
                "user-1",
                ws_version=None,
                kick_discovery=False,
                post_init=AsyncMock(),
                expected_previous_sandbox_id="sb-old",
            )

        assert result_session is session
        # The CAS writes the machine; the record handed back is the project row
        # re-read after it, which is what names the bound sandbox.
        assert record["workspace_id"] == workspace_id
        # 0 is the "never certified — may hold plaintext env" sentinel from
        # migration 021, which is exactly what a no-catalog deployment should
        # stamp. It must be written, not left to COALESCE onto the previous
        # sandbox's generation.
        bind.assert_awaited_once_with(
            _STUB_COMPUTER_ID,
            provider_ref="sandbox-abc",
            expected_previous_provider_ref="sb-old",
            platform_secret_version=0,
        )
        status.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_provision_losing_the_identity_race_does_not_publish(self):
        """A lost CAS must unwind, not fall back to an unguarded write.

        Publishing anyway is how two workers end up believing they own the
        machine; the loser's sandbox is deleted by the caller's unwind.
        """
        from src.server.database.workspace import SandboxIdentityLostError

        manager = WorkspaceManager.get_instance(config=_make_config())
        session = _make_mock_session()
        session.sandbox.runtime = MagicMock()
        workspace_id = str(uuid.uuid4())

        with (
            patch.object(manager, "_mint_sandbox_tokens", AsyncMock(return_value={})),
            cm_patch("SessionManager") as session_mgr,
            patch.object(manager, "_apply_session_mcp", AsyncMock(return_value=None)),
            patch.object(manager, "_sync_sandbox_assets", AsyncMock()),
            patch.object(manager, "_reconcile_skills", AsyncMock()),
            patch.object(manager, "_maybe_restore_files", AsyncMock()),
            _patch_machine_bind(None, computer_id=_STUB_COMPUTER_ID),
        ):
            session_mgr.get_session.return_value = session
            with pytest.raises(SandboxIdentityLostError):
                await manager._provision_sandbox_session(
                    _binding(workspace_id),
                    "user-1",
                    ws_version=None,
                    kick_discovery=False,
                    post_init=AsyncMock(),
                )

        assert manager._cached_session(_STUB_COMPUTER_ID) is None


class TestRestoreGuard:
    """A restore that could not raise the completeness flag aborts provisioning.

    Every other restore failure is a warning: the flag it raised first keeps the
    next backup from pruning. With no flag there is nothing keeping it, so the
    sandbox must not be bound.
    """

    @pytest.mark.asyncio
    async def test_a_missing_guard_propagates_out_of_the_restore_wrapper(self):
        from src.server.services.persistence.file import RestoreGuardUnavailable

        manager = WorkspaceManager.get_instance(config=_make_config())
        with patch(
            "src.server.services.computer_manager._provisioning.FilePersistenceService.restore_to_sandbox",
            AsyncMock(side_effect=RestoreGuardUnavailable("ws-1")),
        ):
            with pytest.raises(RestoreGuardUnavailable):
                await manager._restore_files(
                    _binding("ws-1"), MagicMock(), expected_sandbox_id="sb-old"
                )

    @pytest.mark.asyncio
    async def test_any_other_restore_failure_is_still_a_warning(self):
        manager = WorkspaceManager.get_instance(config=_make_config())
        with patch(
            "src.server.services.computer_manager._provisioning.FilePersistenceService.restore_to_sandbox",
            AsyncMock(side_effect=RuntimeError("transfer boom")),
        ):
            await manager._restore_files(
                _binding("ws-1"), MagicMock(), expected_sandbox_id="sb-old"
            )

    @pytest.mark.asyncio
    async def test_a_lost_identity_propagates_too(self):
        from src.server.services.persistence.file import RestoreIdentityLost

        manager = WorkspaceManager.get_instance(config=_make_config())
        with patch(
            "src.server.services.computer_manager._provisioning.FilePersistenceService.restore_to_sandbox",
            AsyncMock(side_effect=RestoreIdentityLost("ws-1")),
        ) as restore_call:
            with pytest.raises(RestoreIdentityLost):
                await manager._restore_files(
                    _binding("ws-1"), MagicMock(), expected_sandbox_id="sb-old"
                )
        assert restore_call.await_args.kwargs["expected_sandbox_id"] == "sb-old"

    @pytest.mark.asyncio
    async def test_the_restore_is_scoped_to_the_projects_own_folder(self):
        """A machine root holds every sibling's folder, so a restore that named
        it would scan and overwrite across projects."""
        manager = WorkspaceManager.get_instance(config=_make_config())
        with patch(
            "src.server.services.computer_manager._provisioning.FilePersistenceService.restore_to_sandbox",
            AsyncMock(return_value={"restored": 1, "errors": 0}),
        ) as restore_call:
            await manager._restore_files(
                _binding("ws-1"), MagicMock(), expected_sandbox_id="sb-old"
            )

        layout = restore_call.await_args.kwargs["layout"]
        assert layout.dir_name == "test-ab12"

    @pytest.mark.asyncio
    @patch(f"{_LIFECYCLE}.update_workspace_status", new_callable=AsyncMock)
    @cm_patch("SessionManager")
    @cm_patch("db_get_workspace")
    async def test_recovery_restores_against_the_sandbox_the_row_still_names(
        self, mock_get_ws, mock_session_mgr, mock_status
    ):
        """The restore's flag and the bind's CAS must expect the same previous
        sandbox, or a late provisioner could flag a row another has bound.

        Both now read it off the machine: the sandbox is the computer's, so the
        project shadow a lagging row carries is not what either fences on.
        """
        manager = WorkspaceManager.get_instance(config=_make_config())
        ws_id = str(uuid.uuid4())
        mock_get_ws.return_value = _make_workspace(workspace_id=ws_id, status="running")
        session = _make_mock_session()
        mock_session_mgr.get_session.return_value = session
        mock_session_mgr.cleanup_session = AsyncMock()
        manager._mint_sandbox_tokens = AsyncMock(return_value={})
        manager._apply_session_mcp = AsyncMock(return_value=None)
        manager._sync_sandbox_assets = AsyncMock()
        # Stop at the restore: the rest of the spine needs a live pool.
        manager._restore_files = AsyncMock(side_effect=RuntimeError("stop here"))

        with pytest.raises(RuntimeError, match="stop here"):
            await manager._recover_sandbox(
                _binding(ws_id, provider_ref="sb-previous"), "user-1", MagicMock()
            )

        assert (
            manager._restore_files.await_args.kwargs["expected_sandbox_id"]
            == "sb-previous"
        )

    @pytest.mark.asyncio
    async def test_provision_reconciles_the_flag_after_winning_the_bind(self):
        """post_init restores into a sandbox the row does not yet name, so its
        clear cannot land. The reconcile runs once the row names this sandbox,
        and after the session is published."""
        manager = WorkspaceManager.get_instance(config=_make_config())
        session = _make_mock_session()
        session.sandbox.runtime = MagicMock()
        workspace = _make_workspace()
        workspace_id = workspace["workspace_id"]
        order: list[str] = []

        async def _bind(*_a, **_k):
            order.append("bind")
            return {"computer_id": _STUB_COMPUTER_ID, "provider_ref": "sandbox-abc"}

        async def _reconcile(*_a, **_k):
            order.append("reconcile")

        with (
            patch.object(manager, "_mint_sandbox_tokens", AsyncMock(return_value={})),
            cm_patch("SessionManager") as session_mgr,
            patch.object(manager, "_apply_session_mcp", AsyncMock(return_value=None)),
            patch.object(manager, "_sync_sandbox_assets", AsyncMock()),
            patch.object(manager, "_reconcile_skills", AsyncMock()),
            cm_patch("try_bind_computer_provider_ref", _bind),
            cm_patch("db_get_workspace", AsyncMock(return_value=workspace)),
            patch.object(manager, "_maybe_restore_files", _reconcile),
        ):
            session_mgr.get_session.return_value = session
            await manager._provision_sandbox_session(
                _binding(workspace_id),
                "user-1",
                ws_version=None,
                kick_discovery=False,
                post_init=AsyncMock(),
            )

        assert order == ["bind", "reconcile"]


class TestSupersededResolve:
    """A resolve overtaken by a newer config preserves the prior workspace view."""

    @pytest.mark.asyncio
    async def test_the_prior_workspace_view_is_kept(self):
        from src.server.services.egress.session_binding import RelayBind

        manager = WorkspaceManager(_make_config())
        registry = MagicMock()
        session = SimpleNamespace(
            sandbox=MagicMock(mcp_registry=registry),
            computer_id=_STUB_COMPUTER_ID,
            config=SimpleNamespace(mcp=SimpleNamespace(servers=[])),
            mcp_registry=registry,
            _builtin_mcp_registry=registry,
            mcp_config_version=1,
            mcp_config_workspace_id="ws-1",
            mcp_tool_summary="old summary",
            direct_mcp_tools={},
            egress_binding=None,
            mcp_settled_servers=set(),
        )

        async def _install(
            sess,
            resolved,
            *,
            user_id=None,
            workspace_id=None,
            egress_binding=None,
        ):
            sess.direct_mcp_tools = {"moomoo": object()}
            sess.mcp_tool_summary = "installed summary"

        with (
            patch(
                "src.server.services.mcp_config.resolve_mcp_config",
                AsyncMock(return_value=SimpleNamespace(version=2, servers=[])),
            ),
            patch.object(
                manager, "_install_session_composite", AsyncMock(side_effect=_install)
            ),
            patch(
                "src.server.services.computer_manager._mcp.sync_egress_relay",
                AsyncMock(return_value=RelayBind.SUPERSEDED),
            ),
        ):
            out = await manager._apply_session_mcp(
                _binding("ws-1"), "user-1", session, ws_version=2
            )

        assert out is None
        assert session.direct_mcp_tools == {}
        assert session.mcp_tool_summary == "old summary"
        assert session.mcp_config_version == 1
        assert manager._machine(_STUB_COMPUTER_ID).resolve_superseded == {"ws-1"}
