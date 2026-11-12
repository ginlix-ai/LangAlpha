"""A Daytona create given up on leaves no sandbox running unheld.

Daytona answers a create only once the sandbox runs and keeps building one the
caller stopped waiting for (seen live: a create held past its timeout came up a
minute later with no computer naming it), and refuses to delete one while it
is mid-transition (409). So every create carries a name of our own, and a
create that fails, however it fails, looks the name up and deletes what it
finds, or hands the name to the abandoned-sandbox sweep.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
from daytona import CreateSandboxFromSnapshotParams
from daytona.common.errors import (
    DaytonaError,
    DaytonaNotFoundError,
    DaytonaTimeoutError,
)

from ptc_agent.config.core import DaytonaConfig
from ptc_agent.core.sandbox import abandoned
from ptc_agent.core.sandbox.providers.daytona import DaytonaProvider


def _provider(client) -> DaytonaProvider:
    provider = DaytonaProvider.__new__(DaytonaProvider)
    provider._config = DaytonaConfig(api_key="test-key")
    provider._working_dir = "/home/workspace"
    provider._client = client
    return provider


def _absent() -> DaytonaNotFoundError:
    return DaytonaNotFoundError(
        "Failed to get sandbox: not found", status_code=404, code="NOT_FOUND"
    )


def _busy() -> DaytonaError:
    return DaytonaError(
        "Failed to delete sandbox: Sandbox state change in progress", status_code=409
    )


@pytest.fixture
def reports(monkeypatch):
    seen: list[tuple[str, str, str]] = []

    async def sink(kind, ref, reason):
        seen.append((kind, ref, reason))

    abandoned.set_abandoned_sink(sink)
    yield seen
    abandoned.set_abandoned_sink(None)


def _params() -> CreateSandboxFromSnapshotParams:
    return CreateSandboxFromSnapshotParams(snapshot="snap")


@pytest.mark.asyncio
async def test_every_create_carries_a_name_of_its_own_and_the_configured_wait():
    client = AsyncMock()
    client.create = AsyncMock(return_value=MagicMock(id="sb-1"))
    provider = _provider(client)
    first, second = _params(), _params()

    await provider._create_named(first)
    await provider._create_named(second)

    assert first.name and second.name and first.name != second.name
    assert client.create.await_args.kwargs["timeout"] == 180


@pytest.mark.asyncio
async def test_a_timed_out_create_deletes_the_sandbox_its_name_finds(reports):
    found = MagicMock(id="sb-late")
    client = AsyncMock()
    client.create = AsyncMock(
        side_effect=DaytonaTimeoutError("Failed to create sandbox: ")
    )
    client.get = AsyncMock(return_value=found)
    provider = _provider(client)
    params = _params()

    with pytest.raises(DaytonaTimeoutError):
        await provider._create_named(params)

    client.get.assert_awaited_once_with(params.name)
    client.delete.assert_awaited_once_with(found)
    assert reports == []


@pytest.mark.asyncio
async def test_a_sandbox_daytona_will_not_delete_yet_goes_to_the_sweep(reports):
    client = AsyncMock()
    client.create = AsyncMock(
        side_effect=DaytonaTimeoutError("Failed to create sandbox: ")
    )
    client.get = AsyncMock(return_value=MagicMock(id="sb-creating"))
    client.delete = AsyncMock(side_effect=_busy())
    params = _params()

    with pytest.raises(DaytonaTimeoutError):
        await _provider(client)._create_named(params)

    assert [(kind, ref) for kind, ref, _ in reports] == [("daytona", "sb-creating")]
    assert "refused to delete" in reports[0][2]


@pytest.mark.asyncio
async def test_a_name_nothing_answers_to_yet_is_still_reported(reports):
    """The API can take the create after the lookup; the sweep drops the name
    once it has stayed absent."""
    client = AsyncMock()
    client.create = AsyncMock(
        side_effect=DaytonaTimeoutError("Failed to create sandbox: ")
    )
    client.get = AsyncMock(side_effect=_absent())
    params = _params()

    with pytest.raises(DaytonaTimeoutError):
        await _provider(client)._create_named(params)

    client.delete.assert_not_awaited()
    assert [ref for _, ref, _ in reports] == [params.name]


@pytest.mark.asyncio
async def test_a_cancelled_create_still_cleans_up_before_it_ends(reports):
    """A client that disconnects mid-create cancels it; the cleanup runs to its
    end first, since the caller closes the Daytona client right after."""
    started = asyncio.Event()
    found = MagicMock(id="sb-orphan")
    client = AsyncMock()

    async def hang(*args, **kwargs):
        started.set()
        await asyncio.Event().wait()

    client.create = AsyncMock(side_effect=hang)
    client.get = AsyncMock(return_value=found)
    task = asyncio.create_task(_provider(client)._create_named(_params()))
    await started.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    client.delete.assert_awaited_once_with(found)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "get, state, delete, expected",
    [
        (_absent(), "started", None, True),
        (None, "started", None, True),
        (None, "creating", _busy(), False),
        (None, "started", _absent(), True),
        (None, "destroyed", _busy(), True),
    ],
    ids=["absent", "deleted", "mid-transition", "gone-meanwhile", "listed-destroyed"],
)
async def test_the_sweep_hears_whether_the_sandbox_is_gone(
    get, state, delete, expected
):
    client = AsyncMock()
    client.get = AsyncMock(
        side_effect=get, return_value=MagicMock(id="sb", state=state)
    )
    client.delete = AsyncMock(side_effect=delete)

    assert await _provider(client).remove_abandoned("name-1") is expected


@pytest.mark.asyncio
async def test_a_sweep_failure_it_cannot_read_is_raised_not_called_gone():
    client = AsyncMock()
    client.get = AsyncMock(
        side_effect=DaytonaError("Failed to get sandbox: boom", status_code=500)
    )

    with pytest.raises(DaytonaError):
        await _provider(client).remove_abandoned("name-1")
