"""Disk levels and the df/du parsing the stored reading depends on."""

from datetime import datetime, timezone

import pytest

from src.server.services.computer_disk import (
    CRITICAL_FREE_BYTES,
    NOTICE_FREE_BYTES,
    WARNING_FREE_BYTES,
    disk_command,
    disk_from_row,
    disk_level,
    parse_df,
    parse_du,
    storage_breakdown,
)


@pytest.mark.parametrize(
    "free,level",
    [
        (NOTICE_FREE_BYTES, "healthy"),
        (NOTICE_FREE_BYTES - 1, "notice"),
        (WARNING_FREE_BYTES - 1, "warning"),
        (CRITICAL_FREE_BYTES - 1, "critical"),
        (0, "critical"),
    ],
)
def test_level_is_set_by_free_bytes(free, level):
    assert disk_level(free) == level


def test_parse_df_reads_the_numbers_line():
    out = (
        "Filesystem     1024-blocks  Used Available Capacity Mounted on\n"
        "overlay           3145728 94532   3051196       4% /\n"
    )
    reading = parse_df(out)
    assert (reading.total_bytes, reading.used_bytes, reading.free_bytes) == (
        3221225472,
        96800768,
        3124424704,
    )
    assert parse_df("df: /nope: No such file or directory\n") is None


def test_parse_du_keeps_direct_children_only():
    out = (
        "810548\t/home/workspace/research-a1b2/\n"
        "12\t/home/workspace/notes-c3d4/\n"
        "garbage line\n"
    )
    assert parse_du(out, "/home/workspace") == {
        "research-a1b2": 810548 * 1024,
        "notes-c3d4": 12 * 1024,
    }


def test_breakdown_measures_allocated_blocks_like_df():
    """The folders must reconcile with df's used: apparent size (``du -b``)
    lets a sparse file exceed it and clamp 'other' to zero."""
    command, _ = disk_command("/home/workspace", breakdown=True)
    assert "du -skx " in command
    assert "--apparent-size" not in command and "du -sb" not in command


def test_row_without_a_reading_has_no_disk():
    assert disk_from_row({"disk_total_bytes": None}) is None
    disk = disk_from_row(
        {
            "disk_total_bytes": 100,
            "disk_used_bytes": 90,
            "disk_free_bytes": 10,
            "disk_measured_at": datetime.now(timezone.utc),
        }
    )
    assert disk.level == "critical"


def test_a_reading_of_a_replaced_sandbox_is_no_reading():
    """A spec change or recovery swaps the sandbox without measuring it: the
    old sandbox's fullness must neither show nor hold off the next reading."""
    from src.server.services.computer_disk import measured_within

    row = {
        "provider_ref": "sb-new",
        "disk_sandbox_ref": "sb-old",
        "disk_total_bytes": 100,
        "disk_used_bytes": 99,
        "disk_free_bytes": 1,
        "disk_measured_at": datetime.now(timezone.utc),
    }
    assert disk_from_row(row) is None
    assert not measured_within(row, 3600)
    assert disk_from_row({**row, "disk_sandbox_ref": "sb-new"}) is not None


def test_breakdown_attributes_folders_and_leaves_the_rest_as_other():
    rows, other = storage_breakdown(
        {"disk_used_bytes": 1000},
        [
            {"workspace_id": "w1", "name": "A", "dir_name": "a"},
            {"workspace_id": "w2", "name": "B", "dir_name": "b"},
            {"workspace_id": "w3", "name": "C", "dir_name": None},
        ],
        {"a": 100, "b": 600, "_internal": 50},
    )
    assert [r.workspace_id for r in rows] == ["w2", "w1"]
    assert other == 300


@pytest.mark.asyncio
async def test_an_active_turn_reaches_the_client_as_a_coded_409():
    from fastapi import HTTPException

    from src.server.app.computers import _computer_action_errors
    from src.server.services.computer_errors import MachineBusyError

    with pytest.raises(HTTPException) as busy:
        async with _computer_action_errors("set spec for", "c1"):
            raise MachineBusyError("turn running")
    assert busy.value.status_code == 409
    assert busy.value.detail == {
        "code": "turn_active",
        "message": "turn running",
        "files": [],
    }


# ---------------------------------------------------------------------------
# ComputerManager.refresh_computer_disk: the one path that takes a reading.
# ---------------------------------------------------------------------------

_PROVIDERS = "src.server.services.computer_manager._providers"


@pytest.fixture(autouse=True)
def _reset_manager():
    """_disk_manager installs a mock-configured singleton; never leak it."""
    yield
    from src.server.services.computer_manager import ComputerManager

    ComputerManager.reset_instance()


def _disk_manager():
    from unittest.mock import MagicMock

    from src.server.services.computer_manager import ComputerManager

    ComputerManager.reset_instance()
    manager = ComputerManager.get_instance(config=MagicMock())
    manager._cached_session = MagicMock(return_value=None)
    manager._binding_from_computer = MagicMock(return_value=None)
    return manager


def _running(**overrides):
    return {
        "computer_id": "comp-1",
        "kind": "daytona",
        "status": "running",
        "provider_ref": "sb-1",
        "disk_sandbox_ref": "sb-1",
        "root_dir": "/home/workspace",
        **overrides,
    }


@pytest.mark.asyncio
async def test_the_refresh_timeout_covers_a_slow_connect():
    """A stop waits on this under the machine lock, so a hung provider connect
    must end at the timeout like a hung exec would."""
    import asyncio
    from contextlib import asynccontextmanager
    from unittest.mock import AsyncMock, patch

    manager = _disk_manager()

    @asynccontextmanager
    async def hung(_sandbox_id, *, binding=None):
        await asyncio.sleep(3600)
        yield None

    manager._detached_runtime = hung
    with patch(f"{_PROVIDERS}.record_computer_disk", AsyncMock()) as record:
        row, sizes = await asyncio.wait_for(
            manager.refresh_computer_disk(_running(), sandbox_id="sb-1", timeout=0.05),
            timeout=5,
        )
    assert sizes is None and row["computer_id"] == "comp-1"
    record.assert_not_awaited()


@pytest.mark.asyncio
async def test_only_a_running_machine_is_measured_unless_a_sandbox_is_named():
    from unittest.mock import AsyncMock

    manager = _disk_manager()
    manager._measure_disk = AsyncMock(return_value=({}, {}))

    assert await manager.refresh_computer_disk(_running(status="stopped")) == (
        _running(status="stopped"),
        None,
    )
    manager._measure_disk.assert_not_awaited()

    await manager.refresh_computer_disk(_running(status="stopping"), sandbox_id="sb-9")
    manager._measure_disk.assert_awaited_once()
    assert manager._measure_disk.await_args.args[1] == "sb-9"


@pytest.mark.asyncio
async def test_a_recent_reading_is_not_retaken():
    from unittest.mock import AsyncMock

    manager = _disk_manager()
    manager._measure_disk = AsyncMock(return_value=({}, {}))
    fresh = _running(disk_measured_at=datetime.now(timezone.utc))

    _, sizes = await manager.refresh_computer_disk(fresh, min_age_s=60)
    assert sizes is None
    manager._measure_disk.assert_not_awaited()

    await manager.refresh_computer_disk(fresh)
    manager._measure_disk.assert_awaited_once()


class _FakeCache:
    """The slice of RedisCacheClient the breakdown reuse touches, in memory."""

    def __init__(self):
        self.store = {}

    async def get(self, key):
        return self.store.get(key)

    async def set(self, key, value, ttl=None):
        self.store[key] = value
        return True


@pytest.mark.asyncio
async def test_a_recent_breakdown_is_served_again_without_an_exec():
    """The storage panel asks on every open and a breakdown is a du over the
    whole root, so one taken within the window answers again, from a store
    every worker reads, beside the row's own reading."""
    from unittest.mock import AsyncMock, MagicMock, patch

    manager = _disk_manager()
    runtime = MagicMock()
    runtime.exec = AsyncMock(
        return_value=MagicMock(
            stdout=(
                "Filesystem 1024-blocks Used Available Capacity Mounted on\n"
                "overlay 1000 400 600 40% /\n---du---\n"
                "300\t/home/workspace/alpha/\n100\t/home/workspace/beta/\n"
            )
        )
    )
    cache = _FakeCache()
    with (
        patch(f"{_PROVIDERS}.get_cache_client", return_value=cache),
        patch(f"{_PROVIDERS}.record_computer_disk", AsyncMock(return_value=_running())),
    ):
        _, sizes = await manager._read_and_record_disk(
            _running(), runtime, True, sandbox_id="sb-1"
        )
        assert sizes == {"alpha": 300 * 1024, "beta": 100 * 1024}

        manager._measure_disk = AsyncMock()
        row, again = await manager.refresh_computer_disk(
            _running(), breakdown=True, breakdown_max_age_s=60
        )
        assert again == sizes and row["computer_id"] == "comp-1"
        manager._measure_disk.assert_not_awaited()

        # Not for a rebuilt machine: the folders it measured are gone with the
        # sandbox they were on.
        await manager.refresh_computer_disk(
            _running(provider_ref="sb-2"), breakdown=True, breakdown_max_age_s=60
        )
        manager._measure_disk.assert_awaited_once()

        # And not past the window, nor for a caller that did not ask to reuse.
        manager._measure_disk.reset_mock()
        await manager.refresh_computer_disk(
            _running(), breakdown=True, breakdown_max_age_s=0
        )
        await manager.refresh_computer_disk(_running(), breakdown=True)
        assert manager._measure_disk.await_count == 2


@pytest.mark.parametrize(
    ("now_ref", "sizes"),
    [("sb-2", None), ("sb-1", {"alpha": 300 * 1024})],
)
@pytest.mark.asyncio
async def test_a_lost_write_answers_the_row_as_it_now_stands(now_ref, sizes):
    """The du can outlast a spec change. Lost to a replacement, the folders
    are the old sandbox's and nothing is answered as measured; lost to a
    newer reading of the same sandbox, they still hold. Either way the row is
    re-read, never the one the exec started from."""
    from unittest.mock import AsyncMock, MagicMock, patch

    manager = _disk_manager()
    runtime = MagicMock()
    runtime.exec = AsyncMock(
        return_value=MagicMock(
            stdout=(
                "Filesystem 1024-blocks Used Available Capacity Mounted on\n"
                "overlay 1000 400 600 40% /\n---du---\n300\t/home/workspace/alpha/\n"
            )
        )
    )
    current = _running(provider_ref=now_ref, disk_sandbox_ref=now_ref)
    with (
        patch(f"{_PROVIDERS}.get_cache_client", return_value=_FakeCache()),
        patch(f"{_PROVIDERS}.record_computer_disk", AsyncMock(return_value=None)),
        patch(f"{_PROVIDERS}.get_computer", AsyncMock(return_value=current)),
    ):
        row, got = await manager._read_and_record_disk(
            _running(), runtime, True, sandbox_id="sb-1"
        )
    assert row is current
    assert got == sizes


@pytest.mark.asyncio
async def test_a_failed_reading_never_raises():
    from unittest.mock import AsyncMock, patch

    manager = _disk_manager()
    with patch(f"{_PROVIDERS}.get_computer", AsyncMock(side_effect=OSError("db down"))):
        assert await manager.refresh_computer_disk("comp-1") == (None, None)


def test_a_local_computer_without_a_quota_does_not_measure():
    from unittest.mock import MagicMock

    manager = _disk_manager()
    manager._provider_settings = MagicMock(
        return_value=MagicMock(storage_quota_enabled=False)
    )
    assert manager.measures_disk({"kind": "daytona"}) is True
    assert manager.measures_disk({"kind": "docker"}) is False
    manager._provider_settings.side_effect = ValueError("bad override")
    assert manager.measures_disk({"kind": "docker"}) is False


@pytest.mark.parametrize(
    ("free", "told"),
    [
        (WARNING_FREE_BYTES - 1, None),
        (CRITICAL_FREE_BYTES, None),
        (CRITICAL_FREE_BYTES - 1, (CRITICAL_FREE_BYTES - 1) // 2**20),
        (0, 0),
    ],
)
def test_the_agent_is_told_only_at_the_critical_level(free, told):
    from src.server.services.computer_disk import low_disk_free_mb

    row = {
        "disk_total_bytes": 3 * 2**30,
        "disk_used_bytes": 1,
        "disk_free_bytes": free,
        "disk_measured_at": datetime.now(timezone.utc),
    }
    assert low_disk_free_mb(row) == told
    assert low_disk_free_mb(None) is None


def test_only_a_current_reading_counts_as_known():
    from src.server.services.computer_disk import disk_is_known

    row = {
        "disk_total_bytes": 3 * 2**30,
        "disk_used_bytes": 1,
        "disk_free_bytes": 2**30,
        "disk_measured_at": datetime.now(timezone.utc),
        "disk_sandbox_ref": "sb-1",
        "provider_ref": "sb-1",
    }
    assert disk_is_known(row) is True
    assert disk_is_known({**row, "provider_ref": "sb-2"}) is False
    assert disk_is_known({**row, "disk_measured_at": None}) is False
    assert disk_is_known(None) is False
