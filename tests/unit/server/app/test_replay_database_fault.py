"""A database fault during replay is a retryable 503 that names no host, on
the owner's route and a share's alike, wherever the route reads.

Replay ends the read on such a fault rather than show the thread as stubs, so
the endpoint is where it surfaces, and the driver's text names the database.
"""

from contextlib import ExitStack
from unittest.mock import AsyncMock, patch

import psycopg
import pytest
from psycopg_pool import PoolTimeout

from tests.unit.server.services.history.replay_builders import replay_rows

pytestmark = pytest.mark.asyncio

THREAD = "11111111-1111-1111-1111-111111111111"
_SHARE_TOKEN = "share-token-placeholder"
_DRIVER_TEXT = 'connection to server at "db.example.com" (192.0.2.7), port 5432 failed'
_FAULTS = pytest.mark.parametrize(
    "fault",
    [
        psycopg.OperationalError(_DRIVER_TEXT),
        psycopg.InterfaceError(_DRIVER_TEXT),
        PoolTimeout("couldn't get a connection after 30.00 sec"),
    ],
    ids=["operational", "interface", "pool"],
)
_REPLAY = "src.server.services.history.replay.read_replay_page"
# Each route's reads in order, by what they answer when they do not fail.
_OWNER_READS = {
    "rows": "src.server.app.threads.messaging.get_replay_thread_data",
    "replay": _REPLAY,
}
_SHARE_READS = {
    "lookup": "src.server.app.share_access.get_thread_by_share_token",
    "rows": "src.server.app.public.get_replay_thread_data",
    "replay": _REPLAY,
}


def _answers():
    thread = {"conversation_thread_id": THREAD, "latest_checkpoint_id": "cp-tip"}
    return {
        "lookup": {"conversation_thread_id": THREAD, "share_permissions": {}},
        "rows": replay_rows(thread, [], [], owner_id="test-user-123"),
    }


def _reads(stack: ExitStack, reads: dict[str, str], failing: str, fault: Exception):
    answers = _answers()
    for name, target in reads.items():
        mock = (
            AsyncMock(side_effect=fault)
            if name == failing
            else AsyncMock(return_value=answers.get(name))
        )
        stack.enter_context(patch(target, new=mock))


def _assert_retryable(resp):
    assert resp.status_code == 503
    assert resp.headers["retry-after"] == "2"
    assert "example.com" not in resp.text
    assert "192.0.2.7" not in resp.text


@_FAULTS
@pytest.mark.parametrize("failing", list(_OWNER_READS))
async def test_the_owner_replay_answers_a_database_fault_with_503(
    threads_client, failing, fault
):
    with ExitStack() as stack:
        _reads(stack, _OWNER_READS, failing, fault)
        resp = await threads_client.get(f"/api/v1/threads/{THREAD}/messages/replay")

    _assert_retryable(resp)


@_FAULTS
@pytest.mark.parametrize("failing", list(_SHARE_READS))
async def test_a_shared_replay_answers_a_database_fault_with_503(
    public_client, failing, fault
):
    with ExitStack() as stack:
        _reads(stack, _SHARE_READS, failing, fault)
        resp = await public_client.get(f"/api/v1/public/shared/{_SHARE_TOKEN}/replay")

    _assert_retryable(resp)


async def test_a_server_error_is_logged_by_its_type_and_sqlstate(
    threads_client, caplog
):
    # A server's error quotes what it refused, which is the user's content.
    fault = psycopg.errors.QueryCanceled("canceling statement near 'a held row'")
    with ExitStack() as stack:
        _reads(stack, _OWNER_READS, "replay", fault)
        resp = await threads_client.get(f"/api/v1/threads/{THREAD}/messages/replay")

    assert resp.status_code == 503
    assert "QueryCanceled (sqlstate 57014)" in caplog.text
    assert "a held row" not in caplog.text
