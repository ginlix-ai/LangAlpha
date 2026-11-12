"""The abandoned-sandbox sweep keeps a name until its sandbox has stayed gone.

A create the API took late can land after the provider's own lookup found
nothing, so a name is dropped only once it has been absent past
``ABSENT_FOR``; one Daytona still refuses to delete stays for the next pass.
"""

from datetime import timedelta
from unittest.mock import AsyncMock

import pytest

from src.server.services import abandoned_sandbox_sweep as sweep


def _row(age: timedelta, attempts: int = 1) -> dict:
    return {
        "kind": "daytona",
        "ref": "name-1",
        "reason": "create failed: DaytonaTimeoutError",
        "attempts": attempts,
        "age": age,
    }


@pytest.fixture
def db(monkeypatch):
    from types import SimpleNamespace

    from src.server.database import abandoned_sandboxes

    calls = SimpleNamespace(drop=AsyncMock(), note_attempt=AsyncMock())
    monkeypatch.setattr(abandoned_sandboxes, "drop", calls.drop)
    monkeypatch.setattr(abandoned_sandboxes, "note_attempt", calls.note_attempt)
    return calls


def _provider(gone=None, error=None):
    provider = AsyncMock()
    provider.remove_abandoned = AsyncMock(return_value=gone, side_effect=error)
    return provider


@pytest.mark.asyncio
async def test_a_sandbox_gone_past_the_window_drops_its_row(db):
    providers = {"daytona": _provider(gone=True)}

    settled = await sweep.AbandonedSandboxSweeper()._sweep_one(
        _row(sweep.ABSENT_FOR + timedelta(minutes=1)), providers
    )

    assert settled is True
    db.drop.assert_awaited_once_with("daytona", "name-1")
    db.note_attempt.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_name_absent_only_briefly_is_kept_for_a_late_create(db):
    providers = {"daytona": _provider(gone=True)}

    settled = await sweep.AbandonedSandboxSweeper()._sweep_one(
        _row(timedelta(minutes=5)), providers
    )

    assert settled is False
    db.note_attempt.assert_awaited_once_with("daytona", "name-1", None)
    db.drop.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_sandbox_daytona_will_not_delete_yet_stays_for_the_next_pass(db):
    providers = {"daytona": _provider(gone=False)}

    settled = await sweep.AbandonedSandboxSweeper()._sweep_one(
        _row(timedelta(hours=2)), providers
    )

    assert settled is False
    db.drop.assert_not_awaited()
    assert "refused" in db.note_attempt.await_args.args[2]


@pytest.mark.asyncio
async def test_a_provider_failure_is_noted_and_the_row_kept(db):
    providers = {"daytona": _provider(error=RuntimeError("Daytona answered 500"))}

    settled = await sweep.AbandonedSandboxSweeper()._sweep_one(
        _row(timedelta(hours=2)), providers
    )

    assert settled is False
    db.drop.assert_not_awaited()
    assert "Daytona answered 500" in db.note_attempt.await_args.args[2]


@pytest.mark.asyncio
async def test_a_provider_that_hangs_costs_one_row_its_budget(monkeypatch, db):
    """A pass must end inside the retry window, or another worker claims the
    rows it still holds."""
    import asyncio

    async def hang(ref):
        await asyncio.Event().wait()

    provider = AsyncMock()
    provider.remove_abandoned = hang
    monkeypatch.setattr(sweep, "ROW_BUDGET_S", 0.01)

    settled = await sweep.AbandonedSandboxSweeper()._sweep_one(
        _row(timedelta(hours=2)), {"daytona": provider}
    )

    assert settled is False
    assert "TimeoutError" in db.note_attempt.await_args.args[2]


@pytest.mark.asyncio
async def test_a_pass_closes_the_provider_clients_it_built(monkeypatch, db):
    from src.server.database import abandoned_sandboxes

    built = _provider(gone=False)
    monkeypatch.setattr(
        abandoned_sandboxes,
        "claim_due",
        AsyncMock(return_value=[_row(timedelta(hours=1)), _row(timedelta(hours=1))]),
    )
    monkeypatch.setattr(sweep, "_deployment_provider", lambda kind: built)

    await sweep.AbandonedSandboxSweeper().sweep_once()

    assert built.remove_abandoned.await_count == 2
    built.close.assert_awaited_once()
