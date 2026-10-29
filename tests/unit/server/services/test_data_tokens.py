"""A minted market-data token reaches the sandbox only through the row's
fence and generation, and the sweep writes each computer from one worker."""

from __future__ import annotations

import asyncio
import json
import subprocess
import sys
from contextlib import asynccontextmanager, contextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.core.sandbox._shared import REPLACE_UNLESS_NEWER_SCRIPT
from src.server.database import computer_data_tokens as db
from src.server.services import data_token_sweep, data_tokens
from src.server.services.data_token_sweep import DataTokenSweeper
from src.server.services.workspace_manager import WorkspaceManager

COMPUTER_ID = "11111111-1111-4111-8111-111111111111"
USER_ID = "user-1"
SANDBOX_ID = "sandbox-1"
EXPIRES_AT = datetime(2026, 10, 18, 13, 0, 0, 123456, tzinfo=UTC)


ISSUED_AT_MS = 1_792_310_400_123


async def _push(saved: db.Saved, *, written: bool = True):
    write = AsyncMock(return_value=written)
    revoke = AsyncMock(return_value="revoked")
    save = AsyncMock(return_value=saved)
    mark_held = AsyncMock(return_value=True)
    minted = data_tokens.Minted("tok", ISSUED_AT_MS, EXPIRES_AT)
    with (
        patch.object(data_tokens, "_mint", AsyncMock(return_value=minted)),
        patch.object(data_tokens, "_revoke_upstream", revoke),
        patch.object(db, "save", save),
        patch.object(db, "mark_held", mark_held),
    ):
        pushed = await data_tokens.push(
            COMPUTER_ID, USER_ID, SANDBOX_ID, write, generation=4
        )
    return pushed, write, revoke, mark_held, save


class TestPush:
    @pytest.mark.asyncio
    async def test_a_token_the_fence_refuses_is_dropped_without_revoking(self):
        """Revoking is per computer, so it would end the token the sandbox
        holds now, which the refused one never replaced."""
        pushed, write, revoke, mark_held, _ = await _push(db.Saved(False, None))

        assert pushed is False
        revoke.assert_not_awaited()
        write.assert_not_awaited()
        mark_held.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_the_save_is_fenced_on_the_sandbox_it_was_minted_for(self):
        _, _, _, _, save = await _push(db.Saved(True, 5))

        save.assert_awaited_once_with(COMPUTER_ID, USER_ID, SANDBOX_ID, EXPIRES_AT, 4)

    @pytest.mark.asyncio
    async def test_a_moved_generation_drops_the_token_without_revoking(self):
        pushed, write, revoke, mark_held, _ = await _push(db.Saved(True, None))

        assert pushed is False
        revoke.assert_not_awaited()
        write.assert_not_awaited()
        mark_held.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_saved_token_is_written_then_held(self):
        pushed, write, revoke, mark_held, _ = await _push(db.Saved(True, 5))

        assert pushed is True
        revoke.assert_not_awaited()
        payload = write.await_args.args[0]
        assert set(payload) == {"access_token", "iat_ms", "expires_at", "ginlix_data_url"}
        assert payload["access_token"] == "tok"
        assert payload["iat_ms"] == ISSUED_AT_MS
        assert payload["expires_at"] == "2026-10-18T13:00:00Z"
        mark_held.assert_awaited_once_with(COMPUTER_ID, 5, SANDBOX_ID)

    @pytest.mark.asyncio
    async def test_an_unconfirmed_write_is_not_held(self):
        pushed, _, _, mark_held, _ = await _push(db.Saved(True, 5), written=False)

        assert pushed is False
        mark_held.assert_not_awaited()


class TestReplaceUnlessNewer:
    """The script the sandbox runs to swap the token file in."""

    def _run(self, tmp_path, held: dict | None, issued: int):
        src, dst = tmp_path / "incoming", tmp_path / "tokens.json"
        src.write_text(json.dumps({"access_token": "new", "iat_ms": issued}))
        if held is not None:
            dst.write_text(json.dumps(held))
        done = subprocess.run(
            [sys.executable, "-c", REPLACE_UNLESS_NEWER_SCRIPT, str(src), str(dst), str(issued)],
            capture_output=True,
        )
        return done.returncode, src, json.loads(dst.read_text())

    def test_a_later_issue_replaces_even_with_an_earlier_expiry(self, tmp_path):
        held = {"access_token": "old", "iat_ms": 100, "expires_at": "2026-10-19T00:00:00Z"}

        code, src, now = self._run(tmp_path, held, issued=200)

        assert code == 0
        assert now["access_token"] == "new"
        assert not src.exists()

    def test_an_earlier_issue_is_discarded_and_reported(self, tmp_path):
        """A write that stalled across a newer one must not put back a token a
        stop may have revoked, nor be recorded as held."""
        code, src, now = self._run(tmp_path, {"access_token": "held", "iat_ms": 300}, issued=200)

        assert code != 0
        assert now["access_token"] == "held"
        assert not src.exists()

    def test_a_file_without_an_issue_time_is_replaced(self, tmp_path):
        code, _, now = self._run(tmp_path, {"access_token": "legacy"}, issued=200)

        assert code == 0
        assert now["access_token"] == "new"


class TestDue:
    def test_due_until_this_sandbox_holds_one_with_time_left(self):
        later = datetime.now(UTC) + timedelta(minutes=50)
        soon = datetime.now(UTC) + timedelta(minutes=10)

        assert data_tokens.due(db.DataTokenRow(), SANDBOX_ID)
        assert data_tokens.due(db.DataTokenRow(later, None, 3), SANDBOX_ID)
        assert data_tokens.due(db.DataTokenRow(later, "other", 3), SANDBOX_ID)
        assert data_tokens.due(db.DataTokenRow(soon, SANDBOX_ID, 3), SANDBOX_ID)
        assert not data_tokens.due(db.DataTokenRow(later, SANDBOX_ID, 3), SANDBOX_ID)


def _row() -> dict:
    return {
        "computer_id": COMPUTER_ID,
        "user_id": USER_ID,
        "provider_ref": SANDBOX_ID,
        "kind": "daytona",
        "provider_config": None,
        "root_dir": "/home/workspace",
        "generation": 0,
        "is_always_on": True,
        "last_activity": None,
    }


def _conn(acquired: bool, executed: list[str]):
    @asynccontextmanager
    async def get_db_connection():
        cur = MagicMock()
        cur.fetchone = AsyncMock(return_value=(acquired,))

        async def execute(sql, params=None):
            executed.append(sql)
            return cur

        @asynccontextmanager
        async def cursor():
            yield cur

        cur.execute = AsyncMock(side_effect=execute)
        conn = MagicMock()
        conn.execute = AsyncMock(side_effect=execute)
        conn.cursor = cursor
        yield conn

    return get_db_connection


class TestSweepClaim:
    @pytest.mark.asyncio
    async def test_a_computer_claimed_elsewhere_is_left_alone(self):
        sweeper = DataTokenSweeper()
        locked = AsyncMock()
        executed: list[str] = []

        with (
            patch("src.server.database.pool.get_db_connection", _conn(False, executed)),
            patch.object(sweeper, "_write_locked", locked),
        ):
            assert await sweeper._sweep_one(_row()) is False

        locked.assert_not_awaited()
        assert not any("pg_advisory_unlock" in sql for sql in executed)

    @pytest.mark.asyncio
    async def test_the_claim_is_released_after_the_write(self):
        sweeper = DataTokenSweeper()
        locked = AsyncMock(side_effect=RuntimeError("sandbox unreachable"))
        executed: list[str] = []

        with (
            patch("src.server.database.pool.get_db_connection", _conn(True, executed)),
            patch.object(sweeper, "_write_locked", locked),
            pytest.raises(RuntimeError),
        ):
            await sweeper._sweep_one(_row())

        locked.assert_awaited_once_with(COMPUTER_ID, _row())
        assert any("pg_try_advisory_lock" in sql for sql in executed)
        assert any("pg_advisory_unlock" in sql for sql in executed)

    @pytest.mark.asyncio
    async def test_a_row_written_since_the_listing_is_not_minted_again(self):
        sweeper = DataTokenSweeper()
        held = db.DataTokenRow(
            datetime.now(UTC) + timedelta(minutes=55), SANDBOX_ID, 2
        )
        push = AsyncMock()

        with (
            patch.object(db, "load", AsyncMock(return_value=held)),
            patch.object(data_tokens, "push", push),
        ):
            assert await sweeper._write_locked(COMPUTER_ID, _row()) is False

        push.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_sandbox_paused_during_the_mint_is_not_written(self):
        # Attaching resumes a paused E2B sandbox, which would undo the stop.
        from ptc_agent.core.sandbox.runtime import RuntimeState

        sweeper = DataTokenSweeper()
        runtime = MagicMock()
        runtime.get_state = AsyncMock(return_value=RuntimeState.RUNNING)
        runtime.refresh_state = AsyncMock(return_value=RuntimeState.STOPPED)
        runtime.fetch_working_dir = AsyncMock(return_value="/home/workspace")

        @asynccontextmanager
        async def detached(*_a, **_k):
            yield runtime

        manager = MagicMock(_detached_runtime=detached)
        publish = AsyncMock(return_value=True)
        mark_held = AsyncMock()
        minted = data_tokens.Minted("tok", ISSUED_AT_MS, EXPIRES_AT)

        with (
            patch.object(db, "load", AsyncMock(return_value=db.DataTokenRow())),
            patch(
                "src.server.services.computer_manager.ComputerManager.get_instance",
                return_value=manager,
            ),
            patch.object(data_tokens, "_mint", AsyncMock(return_value=minted)),
            patch.object(db, "save", AsyncMock(return_value=db.Saved(True, 1))),
            patch.object(db, "mark_held", mark_held),
            patch("ptc_agent.core.sandbox._shared.publish_private_file", publish),
        ):
            assert await sweeper._write_locked(COMPUTER_ID, _row()) is False

        publish.assert_not_awaited()
        mark_held.assert_not_awaited()


class TestSweepPass:
    @staticmethod
    def _sweeper_with(pages: list[list[dict]]):
        sweeper = DataTokenSweeper()
        claim = AsyncMock(side_effect=pages)
        patches = (
            patch.object(db, "claim_due", claim),
            patch.object(sweeper, "_sweep_one", AsyncMock(return_value=True)),
            patch.object(db, "claim_revocations", AsyncMock(return_value=[])),
        )
        return sweeper, claim, patches

    @pytest.mark.asyncio
    async def test_a_pass_claims_pages_until_one_comes_back_short(self):
        full = [_row()] * data_token_sweep.SWEEP_BATCH_LIMIT
        sweeper, claim, patches = self._sweeper_with([full, [_row()] * 3])

        with patches[0], patches[1], patches[2]:
            assert await sweeper.sweep_once() == data_token_sweep.SWEEP_BATCH_LIMIT + 3

        assert claim.await_count == 2
        assert claim.await_args.kwargs["retry_after"] == data_token_sweep.RETRY_AFTER

    @pytest.mark.asyncio
    async def test_a_pass_ends_after_its_page_bound(self):
        full = [_row()] * data_token_sweep.SWEEP_BATCH_LIMIT
        sweeper, claim, patches = self._sweeper_with(
            [full] * (data_token_sweep.SWEEP_MAX_PAGES + 1)
        )

        with patches[0], patches[1], patches[2]:
            await sweeper.sweep_once()

        assert claim.await_count == data_token_sweep.SWEEP_MAX_PAGES


def _revocation(*users: str) -> db.Revocation:
    return db.Revocation(COMPUTER_ID, users or (USER_ID,), 7)


async def _settle(
    revocation: db.Revocation,
    *,
    answers: list[str],
    in_service: bool = False,
    rewrite: AsyncMock | None = None,
):
    upstream = AsyncMock(side_effect=answers)
    revoked = AsyncMock(return_value=in_service)
    rewrite = rewrite or AsyncMock(return_value=True)
    with (
        patch.object(data_tokens, "_revoke_upstream", upstream),
        patch.object(db, "revoked", revoked),
        patch.object(
            DataTokenSweeper,
            "get_instance",
            return_value=SimpleNamespace(rewrite=rewrite),
        ),
    ):
        settled = await data_tokens.settle(revocation)
    return settled, upstream, revoked, rewrite


class TestSettle:
    """A revocation stays owed until the platform confirms it, and once it
    does, the platform refuses any token minted before it handled the request,
    including one a start minted after the stop recorded it."""

    @pytest.mark.asyncio
    async def test_one_the_platform_does_not_confirm_stays_owed(self):
        settled, _, revoked, rewrite = await _settle(
            _revocation(), answers=["unreachable"]
        )

        assert settled == "unreachable"
        revoked.assert_not_awaited()
        rewrite.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_partial_confirmation_settles_that_user_and_replaces_the_token(
        self,
    ):
        """The platform keys a revocation by user and computer, so the user it
        did not confirm stays owed; the one it did may own the sandbox's token,
        which the platform now refuses."""
        revocation = _revocation(USER_ID, "user-2")

        settled, upstream, revoked, rewrite = await _settle(
            revocation, answers=["revoked", "unreachable"], in_service=True
        )

        assert settled == "unreachable"
        assert [c.args for c in upstream.await_args_list] == [
            (USER_ID, COMPUTER_ID),
            ("user-2", COMPUTER_ID),
        ]
        revoked.assert_awaited_once_with(revocation, (USER_ID,))
        rewrite.assert_awaited_once_with(COMPUTER_ID)

    @pytest.mark.asyncio
    async def test_a_user_the_platform_refuses_does_not_stop_the_next(self):
        """The token's user and the computer's user are revoked on their own,
        so a request refused for one still sends the other's."""
        revocation = _revocation("user-old", USER_ID)

        settled, upstream, revoked, _ = await _settle(
            revocation, answers=["refused", "revoked"]
        )

        assert settled == "refused"
        assert upstream.await_count == 2
        revoked.assert_awaited_once_with(revocation, (USER_ID,))

    @pytest.mark.asyncio
    async def test_an_unreachable_platform_is_not_asked_for_the_next_user(self):
        settled, upstream, revoked, _ = await _settle(
            _revocation("user-old", USER_ID), answers=["unreachable", "revoked"]
        )

        assert settled == "unreachable"
        assert upstream.await_count == 1
        revoked.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_computer_in_service_is_given_a_fresh_token(self):
        revocation = _revocation()

        settled, _, revoked, rewrite = await _settle(
            revocation, answers=["revoked"], in_service=True
        )

        assert settled == "revoked"
        revoked.assert_awaited_once_with(revocation, (USER_ID,))
        rewrite.assert_awaited_once_with(COMPUTER_ID)

    @pytest.mark.asyncio
    async def test_a_stopped_computer_is_not_written(self):
        settled, _, revoked, rewrite = await _settle(_revocation(), answers=["revoked"])

        assert settled == "revoked"
        revoked.assert_awaited_once()
        rewrite.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_failed_rewrite_still_settles_it(self):
        """The confirmation is recorded first and leaves the row due, so the
        sweep writes the sandbox if the rewrite does not."""
        settled, _, _, _ = await _settle(
            _revocation(),
            answers=["revoked"],
            in_service=True,
            rewrite=AsyncMock(side_effect=RuntimeError("sandbox unreachable")),
        )

        assert settled == "revoked"

    @pytest.mark.asyncio
    async def test_a_stop_records_the_revocation_before_sending_it(self):
        recorded = AsyncMock(return_value=_revocation())
        settle = AsyncMock(return_value="unreachable")

        with (
            patch.object(data_tokens, "configured", return_value=True),
            patch.object(db, "revoke", recorded),
            patch.object(data_tokens, "settle", settle),
        ):
            await data_tokens.revoke(COMPUTER_ID)

        recorded.assert_awaited_once_with(COMPUTER_ID)
        settle.assert_awaited_once_with(_revocation())


class TestSweepSettlesRevocations:
    @pytest.mark.asyncio
    async def test_an_unreachable_platform_ends_this_part_of_the_pass(self):
        """The rest would wait on the same unreachable platform."""
        sweeper = DataTokenSweeper()
        owed = [_revocation(), _revocation(), _revocation()]
        claim = AsyncMock(return_value=owed)
        settle = AsyncMock(side_effect=["revoked", "unreachable", "revoked"])

        with (
            patch.object(db, "claim_revocations", claim),
            patch.object(data_tokens, "settle", settle),
        ):
            await sweeper._settle_revocations()

        assert settle.await_count == 2
        assert claim.await_args.kwargs["retry_after"] == data_token_sweep.REVOKE_RETRY_AFTER

    @pytest.mark.asyncio
    async def test_a_refused_revocation_does_not_hold_up_the_rest(self):
        """Claimed together, they share one stamp, so a refusal that ended the
        pass would come first again on every pass and starve the others."""
        sweeper = DataTokenSweeper()
        owed = [_revocation(), _revocation(), _revocation()]
        settle = AsyncMock(side_effect=["refused", "revoked", "revoked"])

        with (
            patch.object(db, "claim_revocations", AsyncMock(return_value=owed)),
            patch.object(data_tokens, "settle", settle),
        ):
            await sweeper._settle_revocations()

        assert settle.await_count == 3


def _session_lock(results: list[bool]):
    held = iter(results)

    @asynccontextmanager
    async def try_session_lock(_key):
        yield next(held)

    return patch("src.server.database.session_lock.try_session_lock", try_session_lock)


class TestRewriteAfterRevocation:
    @pytest.mark.asyncio
    async def test_a_computer_out_of_service_is_not_written(self):
        sweeper = DataTokenSweeper()
        locked = AsyncMock()

        with (
            patch.object(db, "load_in_service", AsyncMock(return_value=None)),
            patch.object(sweeper, "_write_locked", locked),
        ):
            assert await sweeper.rewrite(COMPUTER_ID) is False

        locked.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_computer_still_starting_is_written_whatever_its_activity(self):
        sweeper = DataTokenSweeper()
        row = {**_row(), "status": "starting", "is_always_on": False}
        locked = AsyncMock(return_value=True)
        keeps = AsyncMock(return_value=False)

        with (
            patch.object(db, "load_in_service", AsyncMock(return_value=row)),
            patch.object(sweeper, "_keeps_running", keeps),
            patch.object(sweeper, "_write_locked", locked),
            _session_lock([True]),
        ):
            assert await sweeper.rewrite(COMPUTER_ID) is True

        keeps.assert_not_awaited()
        locked.assert_awaited_once_with(COMPUTER_ID, row)

    @pytest.mark.asyncio
    async def test_an_idle_running_computer_is_left_to_lapse(self):
        sweeper = DataTokenSweeper()
        row = {**_row(), "status": "running", "is_always_on": False}
        locked = AsyncMock()

        with (
            patch.object(db, "load_in_service", AsyncMock(return_value=row)),
            patch.object(sweeper, "_keeps_running", AsyncMock(return_value=False)),
            patch.object(sweeper, "_write_locked", locked),
        ):
            assert await sweeper.rewrite(COMPUTER_ID) is False

        locked.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_it_waits_out_a_siblings_write(self):
        """That write read the row before the revocation was recorded, so its
        token may be refused already and its hold misses the row."""
        sweeper = DataTokenSweeper()
        row = {**_row(), "status": "running"}
        locked = AsyncMock(return_value=True)

        with (
            patch.object(data_token_sweep, "_LOCK_POLL_S", 0.0),
            patch.object(db, "load_in_service", AsyncMock(return_value=row)),
            patch.object(sweeper, "_keeps_running", AsyncMock(return_value=True)),
            patch.object(sweeper, "_write_locked", locked),
            _session_lock([False, False, True]),
        ):
            assert await sweeper.rewrite(COMPUTER_ID) is True

        locked.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_the_wait_is_bounded(self):
        sweeper = DataTokenSweeper()
        locked = AsyncMock()

        with (
            patch.object(data_token_sweep, "_LOCK_POLL_S", 0.01),
            patch.object(sweeper, "_write_locked", locked),
            _session_lock([False] * 1000),
        ):
            assert await sweeper._write_when_free(COMPUTER_ID, _row(), wait_s=0.05) is False

        locked.assert_not_awaited()


MINTED = data_tokens.Minted("tok", ISSUED_AT_MS, EXPIRES_AT)
DOWN = (None, "ConnectError", True)


class TestMintPatience:
    """A sandbox holding no live token fails every market-data call, so its
    mint waits out a platform restart; a refresh never holds up a turn."""

    @staticmethod
    @contextmanager
    def _tries(*outcomes, always=None, known_down=False):
        once = (
            AsyncMock(side_effect=list(outcomes))
            if outcomes
            else AsyncMock(return_value=always)
        )
        cache = SimpleNamespace(
            get=AsyncMock(return_value=True if known_down else None),
            set=AsyncMock(return_value=True),
        )
        with (
            patch.object(data_tokens, "_mint_once", once),
            patch.object(data_tokens, "MINT_PATIENCE", timedelta(seconds=0.3)),
            patch.object(data_tokens, "_MINT_BACKOFF_S", (0.02, 0.05)),
            patch("src.utils.cache.redis_cache.get_cache_client", return_value=cache),
        ):
            yield once, cache

    @pytest.mark.asyncio
    async def test_a_patient_mint_tries_until_the_platform_answers(self):
        with self._tries(DOWN, DOWN, (MINTED, "", False)) as (once, cache):
            assert await data_tokens._mint(USER_ID, COMPUTER_ID, patient=True) is MINTED

        assert once.await_count == 3
        cache.set.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_refresh_takes_one_try(self):
        with self._tries(always=DOWN) as (once, cache):
            assert await data_tokens._mint(USER_ID, COMPUTER_ID) is None

        assert once.await_count == 1
        cache.set.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_refusal_is_not_tried_again(self):
        with self._tries(always=(None, "HTTP 403", False)) as (once, cache):
            assert await data_tokens._mint(USER_ID, COMPUTER_ID, patient=True) is None

        assert once.await_count == 1
        cache.set.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_patience_is_bounded(self):
        loop = asyncio.get_running_loop()
        began = loop.time()

        with self._tries(always=DOWN) as (once, _):
            assert await data_tokens._mint(USER_ID, COMPUTER_ID, patient=True) is None

        assert loop.time() - began < 0.3
        assert once.await_count > 2
        assert all(c.args[2] <= data_tokens._TIMEOUT_S for c in once.await_args_list)

    @pytest.mark.asyncio
    async def test_a_mint_that_waited_in_vain_spares_the_next_ones_the_wait(self):
        """The platform is down rather than restarting, and a start keeps the
        token up to three times."""
        with self._tries(always=DOWN) as (_, cache):
            await data_tokens._mint(USER_ID, COMPUTER_ID, patient=True)

        cache.set.assert_awaited_once_with(
            data_tokens.platform_down_key(), True, ttl=data_tokens._PLATFORM_DOWN_TTL_S
        )

    @pytest.mark.asyncio
    async def test_a_platform_known_down_gets_one_try(self):
        with self._tries(always=DOWN, known_down=True) as (once, cache):
            assert await data_tokens._mint(USER_ID, COMPUTER_ID, patient=True) is None

        assert once.await_count == 1
        cache.set.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("status", "retryable"), [(503, True), (429, True), (403, False), (400, False)]
    )
    async def test_only_an_unavailable_platform_is_tried_again(self, status, retryable):
        resp = SimpleNamespace(is_success=False, status_code=status)
        client = SimpleNamespace(post=AsyncMock(return_value=resp))

        with (
            patch(
                "src.server.dependencies.usage_limits.get_http_client",
                AsyncMock(return_value=client),
            ),
            patch("src.server.dependencies.usage_limits.service_headers", return_value={}),
        ):
            minted, _, again = await data_tokens._mint_once(USER_ID, COMPUTER_ID, 5.0)

        assert minted is None
        assert again is retryable

    @pytest.mark.asyncio
    async def test_a_refused_connection_is_tried_again(self):
        client = SimpleNamespace(post=AsyncMock(side_effect=ConnectionRefusedError()))

        with (
            patch(
                "src.server.dependencies.usage_limits.get_http_client",
                AsyncMock(return_value=client),
            ),
            patch("src.server.dependencies.usage_limits.service_headers", return_value={}),
        ):
            minted, _, again = await data_tokens._mint_once(USER_ID, COMPUTER_ID, 5.0)

        assert minted is None
        assert again is True


class TestRevokeUpstream:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("status", "answer", "tries"),
        [
            (204, "revoked", 1),
            (422, "refused", 1),
            (403, "unreachable", 1),
            (401, "unreachable", 1),
            (503, "unreachable", 3),
            (429, "unreachable", 3),
        ],
    )
    async def test_only_a_request_specific_refusal_reads_as_refused(
        self, status, answer, tries
    ):
        """A refused service token refuses every revocation, as an outage does."""
        resp = SimpleNamespace(is_success=status < 300, status_code=status)
        client = SimpleNamespace(post=AsyncMock(return_value=resp))

        with (
            patch(
                "src.server.dependencies.usage_limits.get_http_client",
                AsyncMock(return_value=client),
            ),
            patch("src.server.dependencies.usage_limits.service_headers", return_value={}),
            patch.object(data_tokens.asyncio, "sleep", AsyncMock()),
        ):
            assert await data_tokens._revoke_upstream(USER_ID, COMPUTER_ID) == answer

        assert client.post.await_count == tries


class TestKeepIsPatientOnlyWithoutALiveToken:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("row", "patient"),
        [
            (db.DataTokenRow(), True),
            (db.DataTokenRow(datetime.now(UTC) + timedelta(minutes=50), "other", 3), True),
            (db.DataTokenRow(datetime.now(UTC) - timedelta(minutes=1), SANDBOX_ID, 3), True),
            (db.DataTokenRow(datetime.now(UTC) + timedelta(minutes=10), SANDBOX_ID, 3), False),
        ],
        ids=["none", "another-sandbox", "run-out", "refresh"],
    )
    async def test_patience_follows_what_the_sandbox_holds(self, row, patient):
        push = AsyncMock(return_value=True)

        with (
            patch.object(data_tokens, "configured", return_value=True),
            patch.object(db, "load", AsyncMock(return_value=row)),
            patch.object(data_tokens, "push", push),
        ):
            await data_tokens.keep(COMPUTER_ID, USER_ID, SANDBOX_ID, AsyncMock())

        assert push.await_args.kwargs["patient"] is patient


class TestSweepSparesIdleComputers:
    """Writing into a sandbox counts as activity at its provider, so renewing
    an idle computer's token would keep it from ever auto-stopping."""

    IDLE_LIMIT_S = 3000

    @staticmethod
    def _executor(active: bool):
        executor = MagicMock()
        executor.get_instance.return_value.has_active_tasks_for_computer = AsyncMock(
            return_value=active
        )
        return patch("src.server.services.runs.executor.LocalRunExecutor", executor)

    @classmethod
    def _manager(cls):
        manager = SimpleNamespace(_idle_timeout_for=lambda row: cls.IDLE_LIMIT_S)
        return patch(
            "src.server.services.computer_manager.ComputerManager.get_instance",
            return_value=manager,
        )

    @classmethod
    def _idle_row(cls, idle_s: float) -> dict:
        used = datetime.now(UTC) - timedelta(seconds=idle_s)
        return {**_row(), "is_always_on": False, "last_activity": used}

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("idle_s", "kept"), [(IDLE_LIMIT_S - 60, True), (IDLE_LIMIT_S + 60, False)]
    )
    async def test_recent_use_is_judged_by_the_providers_idle_limit(
        self, idle_s, kept
    ):
        sweeper = DataTokenSweeper()

        with self._manager(), self._executor(False):
            assert await sweeper._keeps_running(self._idle_row(idle_s)) is kept

    @pytest.mark.asyncio
    async def test_an_idle_computer_with_nothing_running_is_not_written(self):
        sweeper = DataTokenSweeper()
        locked = AsyncMock()
        executed: list[str] = []

        with (
            self._manager(),
            self._executor(False),
            patch("src.server.database.pool.get_db_connection", _conn(True, executed)),
            patch.object(sweeper, "_write_locked", locked),
        ):
            assert await sweeper._sweep_one(self._idle_row(4 * 3600)) is False

        locked.assert_not_awaited()
        assert executed == []

    @pytest.mark.asyncio
    async def test_an_idle_computer_with_work_in_flight_is_written(self):
        sweeper = DataTokenSweeper()
        locked = AsyncMock(return_value=True)

        with (
            self._manager(),
            self._executor(True),
            patch("src.server.database.pool.get_db_connection", _conn(True, [])),
            patch.object(sweeper, "_write_locked", locked),
        ):
            assert await sweeper._sweep_one(self._idle_row(4 * 3600)) is True


class TestTheAcquireKeepsTheToken:
    @pytest.mark.asyncio
    async def test_a_warm_acquire_writes_the_token_when_it_runs_low(self):
        """No asset sync runs on a warm acquire, and the sweep leaves an idle
        computer's token to lapse, so the acquire is what renews it."""

        @asynccontextmanager
        async def in_use(_workspace_id):
            yield

        session = SimpleNamespace(sandbox=SimpleNamespace(runtime=object()))
        binding = SimpleNamespace(computer_id=COMPUTER_ID, root_dir="/home/workspace", dir_name=None)
        manager = SimpleNamespace(
            _acquire_session=AsyncMock(return_value=session),
            resolve_binding=AsyncMock(return_value=binding),
            _keep_data_token=AsyncMock(),
            _settle_folders=AsyncMock(),
            _ensure_project_attached=AsyncMock(),
        )

        with patch("src.server.services.workspace_manager.workspace_folder_in_use", in_use):
            got = await WorkspaceManager.get_session_for_workspace(
                manager, "ws-1", user_id=USER_ID
            )

        assert got is session
        manager._keep_data_token.assert_awaited_once_with(COMPUTER_ID, USER_ID, session.sandbox)
