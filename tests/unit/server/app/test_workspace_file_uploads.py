"""The routes of an upload a browser sends to object storage in parts.

Starting one decides the path, never the client: small files and sandboxes
that cannot pull from the store keep the one-request path and its cap. Each
later step reads the upload's row, which any worker may hold, so the row's
state decides what a step may do.
"""

from __future__ import annotations

import math
from contextlib import ExitStack, asynccontextmanager, contextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest
from httpx import ASGITransport, AsyncClient

from src.server.app.workspace_files import router as files_router
from src.server.app.workspace_files import uploads
from src.server.app.workspace_files._shared import MAX_UPLOAD_BYTES
from src.server.app.workspace_files.uploads import (
    ALREADY_SAVING,
    DIRECT_UPLOAD_MIN_BYTES,
    MAX_DIRECT_UPLOAD_BYTES,
    MAX_OPEN_UPLOADS_PER_USER,
    PARTS_MISSING,
    TOO_MANY_UPLOADS,
    UPLOAD_ENDED,
    UPLOAD_PART_BYTES,
)
from src.server.database.blob_keys import INLINE_MAX_BYTES, upload_key
from src.server.services.computer_disk import CRITICAL_FREE_BYTES
from src.server.services.workspace_upload_pull import STORE_UNREACHABLE, Assembly, not_enough_disk
from tests.conftest import create_test_app

pytestmark = pytest.mark.asyncio

USER = "test-user-123"  # who create_test_app authenticates as
WS = "ws-uploads"
UPLOAD_ID = "00000000-0000-4000-8000-000000000001"
STORE_ID = "store-upload-1"
WORK_DIR = "/home/u/Research"
BASE = f"/api/v1/workspaces/{WS}/files/uploads"
MiB = 1024 * 1024
EXPIRES = datetime(2030, 1, 1, tzinfo=UTC)

_UPLOADS = "src.server.app.workspace_files.uploads"


async def _call(method: str, path: str, **kwargs):
    app = create_test_app(files_router)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        return await client.request(method, f"{BASE}{path}", **kwargs)


# --- starting --------------------------------------------------------------------


def _created(**row):
    return {**row, "expires_at": EXPIRES}


@contextmanager
def _computer(*, direct=True, scan_cap=None, free=None, store_id=STORE_ID):
    """A running workspace whose sandbox the begin route acquires and inspects."""
    sandbox = SimpleNamespace(sandbox_id="sb-1")
    m = SimpleNamespace(
        owned=AsyncMock(return_value={"workspace_id": WS, "user_id": USER, "status": "running"}),
        acquired=MagicMock(),
        open=MagicMock(return_value=store_id),
        abort=MagicMock(return_value=True),
        create=AsyncMock(side_effect=_created),
    )

    @asynccontextmanager
    async def acquire(*_args):
        m.acquired()
        yield sandbox, {"workspace_id": WS, "dir_name": "Research"}

    with ExitStack() as stack:
        for name, value in {
            "owned_workspace_on_computer": m.owned,
            "acquire_sandbox_to_change": acquire,
            "direct_available": lambda _sandbox: direct,
            "scan_cap_bytes": lambda _sandbox, *, blobs_on: scan_cap,
            "owner_work_dir": lambda _workspace: WORK_DIR,
            "previous_dir_names_of": lambda _workspace: (),
            "contained_target": AsyncMock(side_effect=lambda _sb, path, wd, _prev: f"{wd}/{path}"),
            "free_disk_bytes": AsyncMock(return_value=free),
            "open_multipart_upload": m.open,
            "abort_multipart_upload": m.abort,
        }.items():
            stack.enter_context(patch(f"{_UPLOADS}.{name}", value))
        stack.enter_context(patch.object(uploads.db, "create_upload", m.create))
        yield m


def _begin(size: int, path: str = "data/big.bin"):
    return _call("POST", "", json={"path": path, "size": size})


async def test_a_file_past_the_largest_upload_is_refused_before_the_workspace_is_read():
    with _computer() as m:
        resp = await _begin(MAX_DIRECT_UPLOAD_BYTES + 1)
    assert resp.status_code == 413
    m.owned.assert_not_awaited()
    m.acquired.assert_not_called()


async def test_a_small_file_takes_the_one_request_path_without_waking_the_sandbox():
    with _computer() as m:
        resp = await _begin(DIRECT_UPLOAD_MIN_BYTES - 1)
    assert (resp.status_code, resp.json()) == (200, {"mode": "relay"})
    m.acquired.assert_not_called()


@pytest.mark.parametrize(
    "scan_cap, size, status",
    [
        (None, MAX_UPLOAD_BYTES, 200),
        (None, MAX_UPLOAD_BYTES + 1, 413),
        # No store: the relay keeps the file inline, under a lower cap.
        (INLINE_MAX_BYTES, INLINE_MAX_BYTES + 1, 413),
    ],
    ids=["at-relay-cap", "past-relay-cap", "past-inline-cap"],
)
async def test_a_sandbox_that_cannot_pull_keeps_the_relay_and_its_cap(scan_cap, size, status):
    with _computer(direct=False, scan_cap=scan_cap) as m:
        resp = await _begin(size)
    assert resp.status_code == status
    if status == 200:
        assert resp.json() == {"mode": "relay"}
    m.open.assert_not_called()


@pytest.mark.parametrize(
    "size", [DIRECT_UPLOAD_MIN_BYTES, 100 * MiB + 1, MAX_DIRECT_UPLOAD_BYTES]
)
async def test_a_direct_upload_answers_its_parts_layout(size):
    with _computer() as m:
        resp = await _begin(size)

    assert resp.status_code == 200
    body = resp.json()
    upload_id = body.pop("upload_id")
    assert UUID(upload_id)
    assert body == {
        "mode": "direct",
        "path": "data/big.bin",
        "part_size": UPLOAD_PART_BYTES,
        "part_count": math.ceil(size / UPLOAD_PART_BYTES),
        "expires_at": EXPIRES.isoformat(),
    }
    key = upload_key(USER, upload_id)
    assert m.open.call_args.args == (key,)
    created = m.create.call_args.kwargs
    assert (created["object_key"], created["store_upload_id"]) == (key, STORE_ID)
    assert created["max_open"] == MAX_OPEN_UPLOADS_PER_USER


async def test_the_disk_reading_goes_where_the_row_is_written():
    # The sum of what is already due on the computer is taken there, under the
    # user's lock, so two starts at once cannot both fit.
    with _computer(free=900 * MiB) as m:
        resp = await _begin(64 * MiB)
    assert resp.status_code == 200
    created = m.create.call_args.kwargs
    assert (created["free_bytes"], created["keep_free_bytes"]) == (900 * MiB, CRITICAL_FREE_BYTES)


async def test_no_room_beside_the_uploads_due_on_the_computer_is_a_507():
    with _computer(free=900 * MiB) as m:
        m.create.side_effect = uploads.db.NotEnoughRoom(40 * MiB)
        resp = await _begin(64 * MiB)

    assert (resp.status_code, resp.json()) == (507, {"detail": not_enough_disk(40 * MiB)})
    m.abort.assert_called_once_with(m.open.call_args.args[0], STORE_ID)


async def test_an_unreadable_disk_does_not_refuse():
    with _computer(free=None) as m:
        resp = await _begin(64 * MiB)
    assert resp.json()["mode"] == "direct"
    assert m.create.call_args.kwargs["free_bytes"] is None


async def test_past_the_open_upload_cap_the_store_upload_is_aborted():
    with _computer() as m:
        m.create.side_effect = uploads.db.TooManyOpenUploads()
        resp = await _begin(64 * MiB)

    assert (resp.status_code, resp.json()) == (429, {"detail": TOO_MANY_UPLOADS})
    m.abort.assert_called_once_with(m.open.call_args.args[0], STORE_ID)


async def test_an_upload_the_row_failed_to_record_is_aborted_too():
    with _computer() as m:
        m.create.side_effect = RuntimeError("database gone")
        with pytest.raises(RuntimeError):
            await _begin(64 * MiB)
    m.abort.assert_called_once_with(m.open.call_args.args[0], STORE_ID)


async def test_a_store_that_cannot_open_the_upload_is_a_503():
    with _computer(store_id=None) as m:
        resp = await _begin(64 * MiB)
    assert (resp.status_code, resp.json()) == (503, {"detail": STORE_UNREACHABLE})
    m.create.assert_not_awaited()


# --- the upload's row ---------------------------------------------------------------


def _upload(state: str = "uploading", **overrides) -> dict:
    return {
        "upload_id": UPLOAD_ID,
        "user_id": USER,
        "workspace_id": WS,
        "path": "data/big.bin",
        "size": 2 * UPLOAD_PART_BYTES + 5,
        "part_size": UPLOAD_PART_BYTES,
        "part_count": 3,
        "sha256": None,
        "object_key": upload_key(USER, UPLOAD_ID),
        "store_upload_id": STORE_ID,
        "sandbox_id": None,
        "state": state,
        "error": None,
        "attempts": 0,
        "expires_at": datetime.now(UTC) + timedelta(hours=1),
        **overrides,
    }


def _status_of(row: dict) -> dict:
    return {k: row[k] for k in ("upload_id", "state", "path", "size", "error")}


def _db(**fns):
    stack = ExitStack()
    for name, value in fns.items():
        stack.enter_context(patch.object(uploads.db, name, value))
    return stack


def _sign():
    return MagicMock(
        side_effect=lambda _key, _store_id, *, part_number, size, expires_in, for_browser: (
            f"https://store.invalid/part/{part_number}"
        )
    )


async def test_parts_are_signed_for_the_browser_at_their_offsets():
    row, sign = _upload(), _sign()
    with (
        _db(get_upload=AsyncMock(return_value=row), extend_upload=AsyncMock(return_value=row)),
        patch(f"{_UPLOADS}.sign_upload_part", sign),
    ):
        resp = await _call("POST", f"/{UPLOAD_ID}/parts", json={"part_numbers": [3, 1, 3, 2]})

    assert resp.status_code == 200
    part = UPLOAD_PART_BYTES
    assert [(p["part_number"], p["offset"], p["size"]) for p in resp.json()["parts"]] == [
        (1, 0, part),
        (2, part, part),
        (3, 2 * part, 5),
    ]
    assert {c.kwargs["for_browser"] for c in sign.call_args_list} == {True}
    assert [c.kwargs["size"] for c in sign.call_args_list] == [part, part, 5]


@pytest.mark.parametrize("numbers", [[0], [4], [1, 4]])
async def test_a_part_outside_the_upload_is_422(numbers):
    get, extend = AsyncMock(return_value=_upload()), AsyncMock()
    with _db(get_upload=get, extend_upload=extend):
        resp = await _call("POST", f"/{UPLOAD_ID}/parts", json={"part_numbers": numbers})
    assert resp.status_code == 422
    # Refused by the route against the row, not by the body's schema.
    get.assert_awaited_once()
    extend.assert_not_awaited()


async def test_parts_for_an_ended_upload_are_409():
    sign = _sign()
    with (
        _db(get_upload=AsyncMock(return_value=_upload()), extend_upload=AsyncMock(return_value=None)),
        patch(f"{_UPLOADS}.sign_upload_part", sign),
    ):
        resp = await _call("POST", f"/{UPLOAD_ID}/parts", json={"part_numbers": [1]})
    assert (resp.status_code, resp.json()) == (409, {"detail": UPLOAD_ENDED})
    sign.assert_not_called()


async def test_a_part_the_store_cannot_sign_is_503():
    row = _upload()
    with (
        _db(get_upload=AsyncMock(return_value=row), extend_upload=AsyncMock(return_value=row)),
        patch(f"{_UPLOADS}.sign_upload_part", MagicMock(return_value=None)),
    ):
        resp = await _call("POST", f"/{UPLOAD_ID}/parts", json={"part_numbers": [1]})
    assert (resp.status_code, resp.json()) == (503, {"detail": STORE_UNREACHABLE})


async def test_completing_with_parts_missing_names_them():
    assemble = AsyncMock(return_value=(_upload(), Assembly(missing=[2, 3])))
    with (
        _db(
            get_upload=AsyncMock(return_value=_upload()),
            claim_complete=AsyncMock(return_value=_upload("completing")),
        ),
        patch(f"{_UPLOADS}.assemble_and_pull", assemble),
    ):
        resp = await _call("POST", f"/{UPLOAD_ID}/complete")

    assert resp.status_code == 409
    assert resp.json() == {"detail": {"message": PARTS_MISSING, "missing_parts": [2, 3]}}


@pytest.mark.parametrize("state", ["completing", "pulling", "done", "failed", "aborted"])
async def test_completing_again_answers_where_it_stands_without_assembling(state):
    row = _upload(state)
    claim, assemble = AsyncMock(), AsyncMock()
    with (
        _db(get_upload=AsyncMock(return_value=row), claim_complete=claim),
        patch(f"{_UPLOADS}.assemble_and_pull", assemble),
    ):
        resp = await _call("POST", f"/{UPLOAD_ID}/complete")

    assert (resp.status_code, resp.json()) == (202, _status_of(row))
    claim.assert_not_awaited()
    assemble.assert_not_awaited()


async def test_an_uploading_row_past_its_expiry_reads_as_aborted():
    row = _upload(expires_at=datetime.now(UTC) - timedelta(seconds=1))
    with _db(get_upload=AsyncMock(return_value=row)):
        resp = await _call("GET", f"/{UPLOAD_ID}")
    assert resp.json()["state"] == "aborted"


async def test_cancelling_an_upload_still_sending_aborts_and_cleans_it():
    ended = _upload("aborted")
    end, clean = AsyncMock(return_value=ended), AsyncMock()
    with (
        _db(get_upload=AsyncMock(return_value=_upload()), end_upload=end),
        patch(f"{_UPLOADS}.clean_up", clean),
    ):
        resp = await _call("DELETE", f"/{UPLOAD_ID}")

    assert resp.status_code == 204
    end.assert_awaited_once_with(UPLOAD_ID, from_states=("uploading",), state="aborted")
    clean.assert_awaited_once_with(ended)


@pytest.mark.parametrize("state", ["completing", "pulling"])
async def test_an_upload_already_assembled_cannot_be_cancelled(state):
    end, clean = AsyncMock(), AsyncMock()
    with (
        _db(get_upload=AsyncMock(return_value=_upload(state)), end_upload=end),
        patch(f"{_UPLOADS}.clean_up", clean),
    ):
        resp = await _call("DELETE", f"/{UPLOAD_ID}")

    assert (resp.status_code, resp.json()) == (409, {"detail": ALREADY_SAVING})
    end.assert_not_awaited()
    clean.assert_not_awaited()


STEPS = [
    ("GET", f"/{UPLOAD_ID}", {}),
    ("POST", f"/{UPLOAD_ID}/parts", {"json": {"part_numbers": [1]}}),
    ("POST", f"/{UPLOAD_ID}/complete", {}),
    ("DELETE", f"/{UPLOAD_ID}", {}),
]


@pytest.mark.parametrize("method, path, kwargs", STEPS, ids=[f"{m} {p}" for m, p, _ in STEPS])
@pytest.mark.parametrize(
    "row",
    [None, _upload(user_id="someone-else"), _upload(workspace_id="ws-other")],
    ids=["missing", "another-users", "another-workspaces"],
)
async def test_an_upload_not_this_users_in_this_workspace_is_404(method, path, kwargs, row):
    touched = AsyncMock()
    with _db(
        get_upload=AsyncMock(return_value=row),
        extend_upload=touched,
        claim_complete=touched,
        end_upload=touched,
    ):
        resp = await _call(method, path, **kwargs)
    assert resp.status_code == 404
    touched.assert_not_awaited()


# --- which sandbox may take the direct path -------------------------------------------


def _sandbox(provider: str) -> SimpleNamespace:
    return SimpleNamespace(config=SimpleNamespace(sandbox=SimpleNamespace(provider=provider)))


@pytest.mark.parametrize(
    "browser_uploads, scan_cap, provider, available",
    [
        (True, None, "daytona", True),
        (False, None, "daytona", False),
        # A bounded scan cap means the sandbox does not pull from the store itself.
        (True, MAX_UPLOAD_BYTES, "daytona", False),
        # A machine its owner brought keeps the one-request path.
        (True, None, "ssh", False),
    ],
)
async def test_direct_needs_browser_uploads_and_a_sandbox_that_pulls(
    browser_uploads, scan_cap, provider, available
):
    with (
        patch(f"{_UPLOADS}.browser_uploads_enabled", lambda: browser_uploads),
        patch(f"{_UPLOADS}.scan_cap_bytes", lambda _sandbox, *, blobs_on: scan_cap),
    ):
        assert uploads.direct_available(_sandbox(provider)) is available
