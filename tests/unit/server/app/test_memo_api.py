"""Unit tests for the memo HTTP API handlers.

Calls the FastAPI handler functions directly (same style as
test_workspace_files_routing.py) so we don't have to spin up a TestClient.
Uses InMemoryStore for the store and stubs LLMService + memo_binary_storage
so external IO never happens.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import io
from hashlib import sha256 as _sha256
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException, UploadFile
from langgraph.store.memory import InMemoryStore

from ptc_agent.agent.backends import lock_for_namespace
from src.server.app import memo as memo_mod
from src.server.app import setup as setup_mod
from src.server.app.memo import (
    MemoWriteRequest,
    delete_user_memo,
    download_user_memo,
    list_user_memos,
    read_user_memo,
    regenerate_user_memo_metadata,
    upload_user_memo,
    write_user_memo,
)

NAMESPACE = ("user_abc", "memos")
MEMORY_NAMESPACE = ("user_abc", "memory")


def _upload(filename: str, content: bytes, content_type: str) -> UploadFile:
    return UploadFile(
        filename=filename,
        file=io.BytesIO(content),
        headers={"content-type": content_type},
    )


@pytest.fixture
def store(monkeypatch):
    s = InMemoryStore()
    monkeypatch.setattr(setup_mod, "store", s, raising=False)
    # Stub the LLM service so background tasks don't try to hit it.
    llm = MagicMock()
    llm.complete = AsyncMock(side_effect=RuntimeError("no LLM in tests"))
    monkeypatch.setattr(setup_mod, "llm_service", llm, raising=False)
    # Sandbox-source uploads now run an ownership check via db_get_workspace.
    # By default return a workspace owned by user_abc so existing tests pass;
    # specific tests can override with their own monkeypatch.
    async def _fake_get_workspace(workspace_id: str) -> dict:
        return {"id": workspace_id, "user_id": "user_abc"}
    monkeypatch.setattr(memo_mod, "db_get_workspace", _fake_get_workspace, raising=True)
    return s


# --- Upload ---------------------------------------------------------------


@pytest.mark.asyncio
async def test_upload_markdown_lands_pending(store):
    body = b"# Q1 Thesis\n\nPlenty of content here to pass the minimum-content threshold easily."
    resp = await upload_user_memo(
        user_id="user_abc",
        file=_upload("Q1 2026 Thesis.md", body, "text/markdown"),
    )
    assert resp.key == "q1-2026-thesis.md"
    assert resp.metadata_status == "pending"
    item = await store.aget(NAMESPACE, "q1-2026-thesis.md")
    assert item.value["original_filename"] == "Q1 2026 Thesis.md"
    assert item.value["content"].startswith("# Q1 Thesis")
    assert item.value["sha256"]


@pytest.mark.asyncio
async def test_upload_rejects_unsupported_mime(store):
    with pytest.raises(HTTPException) as exc:
        await upload_user_memo(
            user_id="user_abc",
            file=_upload("x.bin", b"\x00\x01\x02", "application/octet-stream"),
        )
    assert exc.value.status_code == 415


@pytest.mark.asyncio
async def test_upload_falls_back_to_extension_when_mime_is_octet_stream(store):
    """Safari + drag-and-drop reports application/octet-stream for valid memos."""
    body = b"# Doc\n\n" + b"x" * 80
    resp = await upload_user_memo(
        user_id="user_abc",
        file=_upload("notes.md", body, "application/octet-stream"),
    )
    assert resp.metadata_status == "pending"
    item = await store.aget(NAMESPACE, "notes.md")
    assert item.value["mime_type"] == "text/markdown"


@pytest.mark.asyncio
async def test_upload_falls_back_to_extension_when_mime_is_empty(store):
    """Some browsers omit Content-Type entirely on drag-drop."""
    body = b"# Doc\n\n" + b"x" * 80
    resp = await upload_user_memo(
        user_id="user_abc",
        file=_upload("notes.md", body, ""),
    )
    assert resp.metadata_status == "pending"
    item = await store.aget(NAMESPACE, "notes.md")
    assert item.value["mime_type"] == "text/markdown"


@pytest.mark.asyncio
async def test_upload_rejects_undecodable_text(store):
    with pytest.raises(HTTPException) as exc:
        await upload_user_memo(
            user_id="user_abc",
            file=_upload("bad.md", b"\xff\xfe" * 30, "text/markdown"),
        )
    assert exc.value.status_code == 422


@pytest.mark.asyncio
async def test_upload_with_sandbox_source_replaces_on_second_add(store):
    body_v1 = b"# Doc v1\n\nFirst snapshot of the sandbox file with enough chars to pass."
    body_v2 = b"# Doc v2\n\nSecond snapshot replaces the first under the same key."

    first = await upload_user_memo(
        user_id="user_abc",
        file=_upload("doc.md", body_v1, "text/markdown"),
        source_kind="sandbox",
        source_workspace_id="ws-1",
        source_path="results/doc.md",
    )
    assert first.replaced is False
    first_key = first.key

    second = await upload_user_memo(
        user_id="user_abc",
        file=_upload("doc.md", body_v2, "text/markdown"),
        source_kind="sandbox",
        source_workspace_id="ws-1",
        source_path="results/doc.md",
    )
    assert second.replaced is True
    assert second.key == first_key

    item = await store.aget(NAMESPACE, first_key)
    assert item.value["content"].startswith("# Doc v2")
    assert item.value["source_kind"] == "sandbox"
    assert item.value["source_workspace_id"] == "ws-1"
    assert item.value["source_path"] == "results/doc.md"


@pytest.mark.asyncio
async def test_upload_with_different_sandbox_source_creates_new(store):
    body = b"# Doc\n\nMore than fifty characters of payload for the upload to succeed."
    first = await upload_user_memo(
        user_id="user_abc",
        file=_upload("doc.md", body, "text/markdown"),
        source_kind="sandbox",
        source_workspace_id="ws-1",
        source_path="results/doc.md",
    )
    second = await upload_user_memo(
        user_id="user_abc",
        file=_upload("doc.md", body, "text/markdown"),
        source_kind="sandbox",
        source_workspace_id="ws-1",
        source_path="archive/doc.md",
    )
    assert first.replaced is False
    assert second.replaced is False
    assert first.key != second.key


@pytest.mark.asyncio
async def test_upload_without_source_keeps_legacy_collision_suffix(store):
    body = b"new body with lots of characters to meet minimum size easily here."
    await upload_user_memo(
        user_id="user_abc",
        file=_upload("Notes.md", body, "text/markdown"),
    )
    resp = await upload_user_memo(
        user_id="user_abc",
        file=_upload("Notes.md", body, "text/markdown"),
    )
    # No source params → falls through to slug-suffix path, distinct keys.
    assert resp.replaced is False
    assert resp.key == "notes-2.md"


@pytest.mark.asyncio
async def test_upload_sandbox_source_rejects_unowned_workspace(store, monkeypatch):
    """Sandbox-source upload must 403 when caller does not own the workspace."""
    async def _foreign(workspace_id: str) -> dict:
        return {"id": workspace_id, "user_id": "someone_else"}
    monkeypatch.setattr(memo_mod, "db_get_workspace", _foreign, raising=True)

    body = b"x" * 80
    with pytest.raises(HTTPException) as exc:
        await upload_user_memo(
            user_id="user_abc",
            file=_upload("doc.md", body, "text/markdown"),
            source_kind="sandbox",
            source_workspace_id="ws-unowned",
            source_path="results/doc.md",
        )
    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_upload_sandbox_source_rejects_missing_workspace(store, monkeypatch):
    """Sandbox-source upload must 404 when the workspace doesn't exist."""
    async def _missing(workspace_id: str) -> None:
        return None
    monkeypatch.setattr(memo_mod, "db_get_workspace", _missing, raising=True)

    body = b"x" * 80
    with pytest.raises(HTTPException) as exc:
        await upload_user_memo(
            user_id="user_abc",
            file=_upload("doc.md", body, "text/markdown"),
            source_kind="sandbox",
            source_workspace_id="ws-ghost",
            source_path="results/doc.md",
        )
    assert exc.value.status_code == 404


@pytest.mark.asyncio
async def test_upload_rejects_oversized_source_path(store):
    """source_path larger than the cap returns 400 before any store work."""
    body = b"x" * 80
    with pytest.raises(HTTPException) as exc:
        await upload_user_memo(
            user_id="user_abc",
            file=_upload("doc.md", body, "text/markdown"),
            source_kind="sandbox",
            source_workspace_id="ws-1",
            source_path="x" * 2000,
        )
    assert exc.value.status_code == 400


@pytest.mark.asyncio
async def test_upload_aborts_chunked_read_at_limit(store):
    """Chunked read must reject a too-large file mid-stream, not after full buffer.

    Build a file 2x the cap, much larger than ``_UPLOAD_READ_CHUNK``, so the
    cap fires before the full body is consumed.
    """
    from ptc_agent.agent.memo.schema import MEMO_MAX_UPLOAD_BYTES

    # File contains 2x the cap — large enough that the chunked limit fires
    # before EOF.
    big_body = b"y" * (MEMO_MAX_UPLOAD_BYTES * 2)
    with pytest.raises(HTTPException) as exc:
        await upload_user_memo(
            user_id="user_abc",
            file=_upload("huge.md", big_body, "text/markdown"),
        )
    assert exc.value.status_code == 413


@pytest.mark.asyncio
async def test_upload_rejects_invalid_source_kind(store):
    body = b"body content with enough characters to pass the minimum threshold."
    with pytest.raises(HTTPException) as exc:
        await upload_user_memo(
            user_id="user_abc",
            file=_upload("x.md", body, "text/markdown"),
            source_kind="not-a-real-kind",
        )
    assert exc.value.status_code == 400


@pytest.mark.asyncio
async def test_upload_slug_collision_appends_suffix(store):
    await store.aput(
        NAMESPACE,
        "q1-thesis.md",
        {"content": "existing", "encoding": "utf-8"},
    )
    resp = await upload_user_memo(
        user_id="user_abc",
        file=_upload(
            "Q1 Thesis.md",
            b"new body with lots of characters to meet minimum size.",
            "text/markdown",
        ),
    )
    assert resp.key == "q1-thesis-2.md"


@pytest.mark.asyncio
async def test_upload_pdf_uses_base64_when_not_configured(store):
    pdf_bytes = b"%PDF-1.4 fake"
    with (
        patch.object(
            memo_mod,
            "extract_pdf_text",
            AsyncMock(
                return_value="Extracted text content spanning more than fifty chars easily here."
            ),
        ),
        patch.object(
            memo_mod.memo_binary_storage, "is_configured", return_value=False
        ),
    ):
        resp = await upload_user_memo(
            user_id="user_abc",
            file=_upload("q.pdf", pdf_bytes, "application/pdf"),
        )
    assert resp.metadata_status == "pending"
    item = await store.aget(NAMESPACE, "q.pdf")
    assert item.value["binary_ref"] is None
    assert item.value["original_bytes_b64"]
    assert base64.b64decode(item.value["original_bytes_b64"]) == pdf_bytes


@pytest.mark.asyncio
async def test_upload_pdf_uses_object_storage_when_configured(store):
    pdf_bytes = b"%PDF-1.4 other"
    ref = {
        "storage": "r2",
        "key": "memo/user_abc/q2.pdf",
        "content_type": "application/pdf",
    }
    with (
        patch.object(
            memo_mod,
            "extract_pdf_text",
            AsyncMock(return_value="Extracted content well over fifty chars worth here yes."),
        ),
        patch.object(
            memo_mod.memo_binary_storage, "is_configured", return_value=True
        ),
        patch.object(
            memo_mod.memo_binary_storage,
            "store_binary",
            AsyncMock(return_value=ref),
        ),
    ):
        await upload_user_memo(
            user_id="user_abc",
            file=_upload("q2.pdf", pdf_bytes, "application/pdf"),
        )
    item = await store.aget(NAMESPACE, "q2.pdf")
    assert item.value["binary_ref"] == ref
    assert item.value["original_bytes_b64"] is None


@pytest.mark.asyncio
async def test_upload_pdf_r2_failure_returns_500(store):
    from src.server.services.memo_binary_storage import MemoBinaryUploadError

    with (
        patch.object(
            memo_mod,
            "extract_pdf_text",
            AsyncMock(return_value="Extracted content well over fifty chars worth here."),
        ),
        patch.object(
            memo_mod.memo_binary_storage, "is_configured", return_value=True
        ),
        patch.object(
            memo_mod.memo_binary_storage,
            "store_binary",
            AsyncMock(side_effect=MemoBinaryUploadError("R2 down")),
        ),
    ):
        with pytest.raises(HTTPException) as exc:
            await upload_user_memo(
                user_id="user_abc",
                file=_upload("bad.pdf", b"%PDF-1.4 bad", "application/pdf"),
            )
    assert exc.value.status_code == 500
    # No store write on failure.
    assert await store.aget(NAMESPACE, "bad.pdf") is None


@pytest.mark.asyncio
async def test_upload_pdf_empty_extraction_returns_422(store):
    from ptc_agent.agent.memo.pdf import MemoPdfExtractionError

    with patch.object(
        memo_mod,
        "extract_pdf_text",
        AsyncMock(side_effect=MemoPdfExtractionError("scan")),
    ):
        with pytest.raises(HTTPException) as exc:
            await upload_user_memo(
                user_id="user_abc",
                file=_upload("scan.pdf", b"%PDF-1.4 scan", "application/pdf"),
            )
    assert exc.value.status_code == 422
    assert await store.aget(NAMESPACE, "scan.pdf") is None


# --- Write ----------------------------------------------------------------


async def _seed(store, key: str, **overrides) -> None:
    base = {
        "content": "original",
        "encoding": "utf-8",
        "mime_type": "text/markdown",
        "original_filename": "note.md",
        "key": key,
        "size_bytes": 8,
        "sha256": _sha256(b"original").hexdigest(),
        "description": "keep",
        "summary": "",
        "metadata_status": "ready",
        "metadata_error": None,
        "binary_ref": None,
        "original_bytes_b64": None,
        "created_at": "2026-04-24T10:00:00Z",
        "modified_at": "2026-04-24T10:00:00Z",
    }
    base.update(overrides)
    await store.aput(NAMESPACE, key, base)


@pytest.mark.asyncio
async def test_write_flips_to_pending_on_change(store):
    await _seed(store, "note.md")
    resp = await write_user_memo(
        user_id="user_abc",
        body=MemoWriteRequest(key="note.md", content="totally new body"),
    )
    assert resp.metadata_status == "pending"
    item = await store.aget(NAMESPACE, "note.md")
    assert item.value["content"] == "totally new body"
    assert item.value["metadata_status"] == "pending"


@pytest.mark.asyncio
async def test_write_identical_content_is_noop(store):
    await _seed(store, "note.md")
    resp = await write_user_memo(
        user_id="user_abc",
        body=MemoWriteRequest(key="note.md", content="original"),
    )
    assert resp.metadata_status == "ready"
    item = await store.aget(NAMESPACE, "note.md")
    assert item.value["description"] == "keep"
    assert item.value["metadata_status"] == "ready"


@pytest.mark.asyncio
async def test_write_rejects_binary_backed(store):
    await _seed(
        store,
        "q.pdf",
        mime_type="application/pdf",
        binary_ref={"storage": "r2", "key": "k", "content_type": "application/pdf"},
    )
    with pytest.raises(HTTPException) as exc:
        await write_user_memo(
            user_id="user_abc",
            body=MemoWriteRequest(key="q.pdf", content="new"),
        )
    assert exc.value.status_code == 400


@pytest.mark.asyncio
async def test_write_missing_returns_404(store):
    with pytest.raises(HTTPException) as exc:
        await write_user_memo(
            user_id="user_abc",
            body=MemoWriteRequest(key="ghost.md", content="x"),
        )
    assert exc.value.status_code == 404


# --- List + Read ----------------------------------------------------------


@pytest.mark.asyncio
async def test_list_excludes_memo_md(store):
    await _seed(store, "a.md", description="alpha")
    await store.aput(NAMESPACE, "memo.md", {"content": "# Memos", "encoding": "utf-8"})

    resp = await list_user_memos(user_id="user_abc")
    keys = [e.key for e in resp.entries]
    assert "a.md" in keys
    assert "memo.md" not in keys


@pytest.mark.asyncio
async def test_read_returns_full_metadata(store):
    await _seed(
        store,
        "a.md",
        content="body",
        description="alpha",
        summary="longer summary",
    )
    resp = await read_user_memo(user_id="user_abc", key="a.md")
    assert resp.content == "body"
    assert resp.description == "alpha"
    assert resp.summary == "longer summary"


@pytest.mark.asyncio
async def test_read_missing_returns_404(store):
    with pytest.raises(HTTPException) as exc:
        await read_user_memo(user_id="user_abc", key="missing.md")
    assert exc.value.status_code == 404


@pytest.mark.asyncio
async def test_read_rejects_invalid_key(store):
    with pytest.raises(HTTPException) as exc:
        await read_user_memo(user_id="user_abc", key="../escape.md")
    assert exc.value.status_code == 400


# --- Download -------------------------------------------------------------


@pytest.mark.asyncio
async def test_download_base64_backed(store):
    content = b"fake pdf bytes"
    await _seed(
        store,
        "q.pdf",
        mime_type="application/pdf",
        original_bytes_b64=base64.b64encode(content).decode("ascii"),
    )
    resp = await download_user_memo(user_id="user_abc", key="q.pdf")
    assert resp.body == content


@pytest.mark.asyncio
async def test_download_text_memo_streams_content(store):
    await _seed(store, "note.md", content="hello", mime_type="text/markdown")
    resp = await download_user_memo(user_id="user_abc", key="note.md")
    assert resp.body == b"hello"


@pytest.mark.asyncio
async def test_download_binary_ref_via_storage(store):
    ref = {
        "storage": "r2",
        "key": "memo/user_abc/q.pdf",
        "content_type": "application/pdf",
    }
    await _seed(store, "q.pdf", mime_type="application/pdf", binary_ref=ref)
    with patch.object(
        memo_mod.memo_binary_storage,
        "fetch_binary",
        AsyncMock(return_value=b"via-r2"),
    ) as fetch:
        resp = await download_user_memo(user_id="user_abc", key="q.pdf")
    assert resp.body == b"via-r2"
    fetch.assert_awaited_once_with(ref)


# --- Delete + Regenerate --------------------------------------------------


@pytest.mark.asyncio
async def test_delete_removes_and_rebuilds(store):
    await _seed(store, "a.md", description="alpha")
    await delete_user_memo(user_id="user_abc", key="a.md")
    assert await store.aget(NAMESPACE, "a.md") is None
    memo_md = await store.aget(NAMESPACE, "memo.md")
    assert memo_md is not None
    assert "a.md" not in memo_md.value["content"]


@pytest.mark.asyncio
async def test_delete_missing_returns_404(store):
    with pytest.raises(HTTPException) as exc:
        await delete_user_memo(user_id="user_abc", key="ghost.md")
    assert exc.value.status_code == 404


@pytest.mark.asyncio
async def test_regenerate_flips_to_pending(store):
    await _seed(
        store,
        "a.md",
        description="old",
        metadata_status="failed",
        metadata_error="prev LLM fail",
    )
    resp = await regenerate_user_memo_metadata(user_id="user_abc", key="a.md")
    assert resp.metadata_status == "pending"
    item = await store.aget(NAMESPACE, "a.md")
    assert item.value["metadata_status"] == "pending"
    assert item.value["metadata_error"] is None


# --- Regression: postgres prefix-leak ---------------------------------------
#
# AsyncPostgresStore implements asearch's namespace prefix as a SQL
# ``LIKE 'user_id.memo%'`` after joining the tuple with ``"."``. The naive
# memo namespace ``(user_id, "memo")`` therefore matched every row under
# ``(user_id, "memory")`` because the strings share a common prefix. The fix
# is to use the plural ``"memos"`` so neither namespace string is a prefix
# of the other. This regression reproduces the postgres semantics with a
# fake store so the test fails if anyone reverts the rename.


class _StringPrefixStore:
    """Store stand-in that mirrors AsyncPostgresStore's SQL prefix semantics.

    Real ``InMemoryStore`` does tuple-prefix matching, so it doesn't catch
    this bug. Here ``asearch(prefix)`` returns every row whose period-joined
    namespace starts with the period-joined prefix — which is what postgres
    actually does, and which is what allowed memory rows to leak into memo
    queries when the namespaces shared a string prefix.
    """

    def __init__(self) -> None:
        self._rows: dict[tuple[tuple[str, ...], str], dict] = {}

    @staticmethod
    def _to_text(ns: tuple[str, ...]) -> str:
        return ".".join(ns)

    async def aput(self, namespace, key, value):
        self._rows[(namespace, key)] = value

    async def aget(self, namespace, key):
        value = self._rows.get((namespace, key))
        if value is None:
            return None
        item = MagicMock()
        item.namespace = namespace
        item.key = key
        item.value = value
        return item

    async def asearch(self, namespace_prefix, *, query=None, filter=None,
                      limit=10, offset=0, refresh_ttl=None):
        prefix_text = self._to_text(namespace_prefix)
        out = []
        for (ns, key), value in self._rows.items():
            if self._to_text(ns).startswith(prefix_text):
                item = MagicMock()
                item.namespace = ns
                item.key = key
                item.value = value
                out.append(item)
        return out[offset : offset + limit]

    async def adelete(self, namespace, key):
        self._rows.pop((namespace, key), None)


@pytest.mark.asyncio
async def test_memo_namespace_does_not_leak_memory_rows_under_postgres_semantics(
    monkeypatch,
):
    """Regression for the prod bug where memory.md showed up in the memo panel.

    Reproduces the bug only if someone reverts ``"memos"`` back to ``"memo"``
    or otherwise picks a memo namespace that is a string prefix of the memory
    namespace.
    """
    fake = _StringPrefixStore()
    monkeypatch.setattr(setup_mod, "store", fake, raising=False)
    monkeypatch.setattr(setup_mod, "llm_service", None, raising=False)

    # Seed two rows the agent's memory tier writes — these MUST NOT show up
    # under the memo router.
    memory_ns = ("user_abc", "memory")
    await fake.aput(memory_ns, "memory.md", {"content": "agent notes"})
    await fake.aput(
        memory_ns,
        "feedback_visualization_preference.md",
        {"content": "user prefers bar charts"},
    )

    resp = await list_user_memos(user_id="user_abc")
    keys = {entry.key for entry in resp.entries}
    assert "memory.md" not in keys, (
        "memo namespace must not pick up memory rows via postgres prefix match"
    )
    assert "feedback_visualization_preference.md" not in keys
    assert keys == set()


# --- Coverage gap fills (added by ship-workflow audit) ---------------------
#
# These tests exercise paths that were uncovered before:
# - Upload >5 MB raw is rejected with 413
# - Extracted PDF text >1 MB is rejected with 413 (and no row created)
# - Write content too large is rejected with 413
# - Download Content-Disposition handles unicode (CJK/emoji) per RFC 6266
# - Delete cancels in-flight metadata generation task


@pytest.mark.asyncio
async def test_upload_rejects_oversized_raw_bytes(store):
    """Raw upload >5 MB → 413 before any extraction or store write."""
    from ptc_agent.agent.memo.schema import MEMO_MAX_UPLOAD_BYTES

    oversized = b"x" * (MEMO_MAX_UPLOAD_BYTES + 1)
    with pytest.raises(HTTPException) as exc:
        await upload_user_memo(
            user_id="user_abc",
            file=_upload("huge.md", oversized, "text/markdown"),
        )
    assert exc.value.status_code == 413
    # Nothing landed in the store.
    listing = await list_user_memos(user_id="user_abc")
    assert listing.entries == []


@pytest.mark.asyncio
async def test_upload_rejects_extracted_content_too_large(store):
    """PDF extraction yields >1 MB of text → 413 with no store row."""
    from ptc_agent.agent.memo.schema import MEMO_MAX_CONTENT_BYTES

    huge_text = "a" * (MEMO_MAX_CONTENT_BYTES + 1)
    with patch.object(
        memo_mod,
        "extract_pdf_text",
        AsyncMock(return_value=huge_text),
    ):
        with pytest.raises(HTTPException) as exc:
            await upload_user_memo(
                user_id="user_abc",
                file=_upload("big.pdf", b"%PDF-1.4 fake", "application/pdf"),
            )
    assert exc.value.status_code == 413
    assert await store.aget(NAMESPACE, "big.pdf") is None


@pytest.mark.asyncio
async def test_write_rejects_content_too_large(store):
    """write_user_memo content >1 MB → 413 and the existing row stays intact."""
    from ptc_agent.agent.memo.schema import MEMO_MAX_CONTENT_BYTES

    await _seed(store, "note.md", content="original")
    bigger = "y" * (MEMO_MAX_CONTENT_BYTES + 1)
    with pytest.raises(HTTPException) as exc:
        await write_user_memo(
            user_id="user_abc",
            body=MemoWriteRequest(key="note.md", content=bigger),
        )
    assert exc.value.status_code == 413
    # Original content unchanged.
    item = await store.aget(NAMESPACE, "note.md")
    assert item.value["content"] == "original"


def test_content_disposition_strips_quotes_and_newlines():
    """Header injection defense: quotes, CR, LF must be removed.

    Without this, an upload with ``original_filename = 'x";evil="y'`` could
    forge a second header parameter and confuse some downloaders.
    """
    from src.server.app.memo import _content_disposition

    cd = _content_disposition('weird"name\nwith.md')
    # No raw quote or newline in the ASCII fallback portion.
    assert '"weird' in cd
    # The filename token is ASCII-safe — no embedded quote/CR/LF.
    # Slice between the first and second " of the ASCII filename token.
    first_q = cd.index('"')
    second_q = cd.index('"', first_q + 1)
    ascii_token = cd[first_q + 1 : second_q]
    assert '"' not in ascii_token
    assert "\n" not in ascii_token
    assert "\r" not in ascii_token


def test_content_disposition_unicode_filename_is_latin1_safe():
    """CJK / emoji filenames must produce a header Starlette can latin-1 encode.

    Without ``re.ASCII``, ``\\w`` keeps unicode word chars, so the ASCII
    fallback parameter would contain CJK and Starlette would 500 the
    download endpoint. Regression for that bug.
    """
    from src.server.app.memo import _content_disposition

    cd = _content_disposition("备忘录.pdf")
    # Whole header must be latin-1 encodable (what Starlette will try).
    cd.encode("latin-1")
    # Original CJK survives via filename* per RFC 6266.
    assert "filename*=UTF-8''" in cd


@pytest.mark.asyncio
async def test_delete_cancels_pending_metadata_task(store, monkeypatch):
    """Delete must cancel any in-flight metadata task before the row vanishes.

    Otherwise a post-LLM ``_merge_metadata`` aput could resurrect the deleted
    key. We register a never-completing task in the registry, call delete,
    and assert the task was cancelled.
    """
    await _seed(store, "ghost.md", description="old")

    namespace = ("user_abc", "memos")
    # Register a fake long-running metadata task in the module-level registry
    # so delete_user_memo's _cancel_pending_metadata has something to find.
    async def _never_resolves() -> None:
        await asyncio.sleep(60)

    pending_task = asyncio.create_task(_never_resolves(), name="memo-metadata-ghost.md")
    memo_mod._METADATA_TASKS[(namespace, "ghost.md")] = pending_task
    try:
        await delete_user_memo(user_id="user_abc", key="ghost.md")
    finally:
        # Make sure we don't leak the task even if the assertion below fails.
        if not pending_task.done():
            pending_task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await pending_task

    assert pending_task.cancelled() or pending_task.done()
    # And the registry entry was cleared so a follow-up delete is a no-op.
    assert (namespace, "ghost.md") not in memo_mod._METADATA_TASKS


@pytest.mark.asyncio
async def test_delete_acquires_namespace_lock(store):
    """Delete must hold the namespace lock so a concurrent upload's aput
    after _find_by_source can't write a fresh value over a row we just
    deleted (silent resurrection). The contract is enforced by acquiring
    the same lock the upload handler uses.
    """
    await _seed(store, "to-delete.md", description="bye")

    # Hold the registry-shared lock externally; the WeakValueDictionary
    # keeps the same instance as long as we hold a reference.
    lock = lock_for_namespace(NAMESPACE)
    async with lock:
        task = asyncio.create_task(
            delete_user_memo(user_id="user_abc", key="to-delete.md"),
        )
        # Yield to give the task a chance to run; if delete forgot the
        # lock, it would complete before we release.
        for _ in range(20):
            await asyncio.sleep(0)
        assert not task.done(), (
            "delete_user_memo did not block on the namespace lock"
        )

    # Lock released — delete should now proceed.
    await asyncio.wait_for(task, timeout=2.0)
    assert await store.aget(NAMESPACE, "to-delete.md") is None
