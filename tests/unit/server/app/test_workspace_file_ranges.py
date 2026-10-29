"""Range on the owner download route: the header it honors, and ranged reads
of a stopped workspace's mirror.

The live sandbox's ranged reads run the real shell probe, so they sit with the
other containment tests in ``test_file_serving_containment.py``.
"""

from __future__ import annotations

import hashlib
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from src.server.app.workspace_files._shared import requested_byte_range
from src.server.app.workspace_files.crud import (
    RANGE_MAX_BYTES,
    download_workspace_file,
)
from src.server.database.workspace_file_blobs import BlobFetchError

_CRUD = "src.server.app.workspace_files.crud"
_RANGED = "src.server.services.persistence.resolve.fetch_blob_range"
_WHOLE = "src.server.services.persistence.resolve.fetch_blob"
_SERVE_MAX = "src.server.services.persistence.resolve.SERVE_MAX_BYTES"
_STORAGE = "src.server.database.workspace_file_blobs._storage_get_bytes_range"

WS_ID = "ws-ranges-1"
OWNER = "user-ranges-1"
PDF = bytes(range(256)) * 4
DIGEST = hashlib.sha256(PDF).hexdigest()
VAULT_SECRET = "vault-secret-value-1234"


# --- the Range header --------------------------------------------------------


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        ("bytes=0-99", (0, 99)),
        ("bytes=5-5", (5, 5)),
        ("Bytes=5-9", (5, 9)),
        (" bytes=5-9 ", (5, 9)),
    ],
)
def test_one_closed_byte_range_is_honored(header, expected) -> None:
    assert requested_byte_range({"range": header}) == expected


@pytest.mark.parametrize(
    "header",
    [
        None,
        "",
        # A client resuming a download would keep a capped answer as the rest.
        "bytes=100-",
        "bytes = 5 - 9",
        "bytes=0-1,4-5",
        "bytes=-500",
        "items=0-9",
        "bytes=9-5",
        "bytes=a-b",
        # Past what the shell's integers hold, and past any file.
        "bytes=0-" + "9" * 19,
        # A digit to Python's int(), not to the shell.
        "bytes=١-٢",
    ],
)
def test_every_other_range_is_ignored(header) -> None:
    """The file then goes whole, as it would without the header."""
    headers = {} if header is None else {"range": header}
    assert requested_byte_range(headers) is None


@pytest.mark.parametrize(
    "precondition",
    [
        "if-range",
        "if-match",
        "if-none-match",
        "if-modified-since",
        "if-unmodified-since",
    ],
)
def test_a_conditional_range_is_ignored(precondition) -> None:
    """A browser resuming a download sends If-Range with the whole answer's
    tag, which a range never carries, so it gets the whole file back."""
    headers = {"range": "bytes=0-9", precondition: '"abc"'}
    assert requested_byte_range(headers) is None


def test_a_long_header_is_rejected_without_backtracking() -> None:
    started = time.perf_counter()
    assert requested_byte_range({"range": "bytes=" + " " * 100_000 + "0-1"}) is None
    assert time.perf_counter() - started < 0.1


# --- ranged reads of the mirror ---------------------------------------------


def _row(**fields) -> dict:
    return {
        "file_path": "report.pdf",
        "file_name": "report.pdf",
        "mime_type": "application/pdf",
        "content_hash": DIGEST,
        "file_size": len(PDF),
        **fields,
    }


def _stored(data: bytes) -> AsyncMock:
    """Object storage holding ``data``, answering any range of it."""
    return AsyncMock(
        side_effect=lambda _user, _key, start, n, **_: data[start : start + n]
    )


async def _download(row: dict, range_header: str | None = None):
    """The route's answer for a stopped workspace, and the vault lookup."""
    request = MagicMock()
    request.headers = {"range": range_header} if range_header else {}
    workspace = {"workspace_id": WS_ID, "user_id": OWNER, "status": "stopped"}
    vault = AsyncMock(return_value={"API_KEY": VAULT_SECRET})
    with (
        patch(
            "src.server.services.workspace_files.db_get_workspace",
            AsyncMock(return_value=workspace),
        ),
        patch(f"{_CRUD}.owner_work_dir", return_value="/home/workspace/reports"),
        patch(
            f"{_CRUD}.FilePersistenceService.get_file_content",
            AsyncMock(return_value=row),
        ),
        patch(f"{_CRUD}.get_vault_secrets_for_redaction", vault),
    ):
        response = await download_workspace_file(
            WS_ID, OWNER, request, path="report.pdf", attachment=False
        )
    return response, vault


@pytest.mark.asyncio
async def test_a_ranged_mirror_download_reads_only_the_slice() -> None:
    with patch(_RANGED, _stored(PDF)) as ranged, patch(_WHOLE, AsyncMock()) as whole:
        response, vault = await _download(_row(blob_sha256=DIGEST), "bytes=100-199")
    assert response.status_code == 206
    assert response.body == PDF[100:200]
    assert response.headers["content-range"] == "bytes 100-199/1024"
    assert response.headers["accept-ranges"] == "bytes"
    assert response.headers["cache-control"] == "private, no-store, no-transform"
    # The file's own digest, so the tag holds across a re-sync of the same bytes.
    assert response.headers["etag"] == f'"{DIGEST}"'
    # The slice, and the file's first bytes, which rule out text.
    assert sorted(c.args for c in ranged.await_args_list) == [
        (OWNER, DIGEST, 0, len(PDF)),
        (OWNER, DIGEST, 100, 100),
    ]
    whole.assert_not_awaited()
    # A ranged answer is never redacted, so it never reads the vault.
    vault.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("header", "cap", "content_range"),
    [
        ("bytes=1000-2000", RANGE_MAX_BYTES, "bytes 1000-1023/1024"),
        ("bytes=0-1023", 300, "bytes 0-299/1024"),
        ("bytes=10-5000", 300, "bytes 10-309/1024"),
    ],
)
async def test_a_mirror_range_stops_at_the_end_and_at_the_cap(
    header, cap, content_range
) -> None:
    with patch(f"{_CRUD}.RANGE_MAX_BYTES", cap), patch(_RANGED, _stored(PDF)):
        response, _ = await _download(_row(blob_sha256=DIGEST), header)
    first, last = (int(n) for n in content_range[6:].split("/")[0].split("-"))
    assert response.headers["content-range"] == content_range
    assert response.body == PDF[first : last + 1]


@pytest.mark.asyncio
async def test_a_mirror_range_past_the_end_is_416_with_the_size() -> None:
    with patch(_RANGED, _stored(PDF)) as ranged:
        with pytest.raises(HTTPException) as exc:
            await _download(_row(blob_sha256=DIGEST), "bytes=1024-2000")
    assert exc.value.status_code == 416
    assert exc.value.headers == {"Content-Range": "bytes */1024"}
    assert all(c.args[2] == 0 for c in ranged.await_args_list)


@pytest.mark.asyncio
async def test_an_inline_row_goes_whole_once_rather_than_a_range_at_a_time() -> None:
    """Its bytes come with the row, so every slice would load all of them."""
    with patch(_RANGED, AsyncMock()) as ranged:
        response, _ = await _download(_row(content_binary=PDF), "bytes=0-9")
    assert response.status_code == 200
    assert response.body == PDF
    assert "content-range" not in response.headers
    ranged.assert_not_awaited()


@pytest.mark.asyncio
async def test_storage_down_on_a_ranged_read_is_503() -> None:
    """The owner is told to retry, not that the file is gone."""
    with patch(_RANGED, AsyncMock(side_effect=BlobFetchError("storage down"))):
        with pytest.raises(HTTPException) as exc:
            await _download(_row(blob_sha256=DIGEST), "bytes=0-9")
    assert exc.value.status_code == 503


@pytest.mark.asyncio
async def test_a_mirror_slice_that_is_the_whole_file_must_match_its_digest() -> None:
    """The first bytes read to rule out text are the whole of a small file, so
    a mis-keyed object fails as loudly as it does on a whole read."""
    tampered = PDF[:-1] + b"\x00"
    with patch(
        _STORAGE, side_effect=lambda _key, start, n: tampered[start : start + n]
    ):
        with pytest.raises(HTTPException) as exc:
            await _download(_row(blob_sha256=DIGEST), "bytes=100-199")
    assert exc.value.status_code == 503
    assert exc.value.detail == "File content temporarily unavailable"


@pytest.mark.asyncio
async def test_only_an_unranged_mirror_read_is_bound_by_the_serving_limit() -> None:
    """Nothing holds the whole file for a range, so a file too large to open
    whole still opens a range at a time."""
    row = _row(blob_sha256=DIGEST)
    with patch(_SERVE_MAX, 10), patch(_RANGED, _stored(PDF)):
        response, _ = await _download(row, "bytes=0-9")
        assert response.status_code == 206
        with pytest.raises(HTTPException) as exc:
            await _download(row)
    assert exc.value.status_code == 413


@pytest.mark.asyncio
async def test_a_text_row_goes_whole_and_redacted_despite_a_range() -> None:
    text = f"key={VAULT_SECRET}\n"
    row = _row(
        file_name="notes.txt",
        mime_type="text/plain",
        content_text=text,
        file_size=len(text),
    )
    response, vault = await _download(row, "bytes=0-3")
    assert response.status_code == 200
    assert "content-range" not in response.headers
    assert response.body.startswith(b"key=")
    assert VAULT_SECRET.encode() not in response.body
    vault.assert_awaited_once()


@pytest.mark.asyncio
async def test_text_under_a_binary_type_goes_whole_and_redacted() -> None:
    """The whole answer redacts any body that decodes as UTF-8, whatever its
    type, so a range of one would carry what that answer hides."""
    text = f"API_KEY={VAULT_SECRET}\n".encode()
    digest = hashlib.sha256(text).hexdigest()
    row = _row(
        file_name="settings.yaml",
        mime_type="application/yaml",
        content_hash=digest,
        blob_sha256=digest,
        file_size=len(text),
    )
    with (
        patch(_RANGED, _stored(text)),
        patch(_WHOLE, AsyncMock(return_value=text)),
    ):
        response, vault = await _download(row, "bytes=0-9")
    assert response.status_code == 200
    assert "content-range" not in response.headers
    assert response.body.startswith(b"API_KEY=")
    assert VAULT_SECRET.encode() not in response.body
    vault.assert_awaited_once()


def test_a_ranged_download_is_described_to_script_from_another_origin() -> None:
    """The reader reads Content-Range and ETag by script, and a browser shows
    script from another origin only these past its safelisted few; without
    them every large PDF would quietly download whole."""
    from fastapi.middleware.cors import CORSMiddleware

    from src.server.app.setup import app

    cors = next(m for m in app.user_middleware if m.cls is CORSMiddleware)
    exposed = set(cors.kwargs["expose_headers"])
    assert {"Content-Range", "Accept-Ranges", "ETag"} <= exposed
