"""Unit tests for ``ImageCaptureMiddleware``.

Confirms sandbox image markdown in model responses is rewritten to
content-addressed storage URLs before the message enters state, and that
every failure path (storage disabled, no sandbox, download/upload failure,
unexpected exception) degrades to leaving the sandbox path in place.
"""

from __future__ import annotations

import hashlib
from pathlib import PurePosixPath

import pytest
from langchain.agents.middleware.types import ModelResponse
from langchain_core.messages import AIMessage, ToolMessage

import ptc_agent.agent.middleware.image_capture as ic
from ptc_agent.agent.middleware.image_capture import (
    ImageCaptureMiddleware,
    image_storage_key,
    is_sandbox_image_path,
)

PNG = b"png-bytes"
PNG_SHA16 = hashlib.sha256(PNG).hexdigest()[:16]


class _FakeSandbox:
    """Stands in for the sandbox, mirroring ``core.sandbox.files.normalize_path``.

    The allowed-roots passthrough is the part that matters: an absolute path
    under an allowed root is a real location, not a virtual one, so it must not
    be re-rooted under the work dir. A fake that always prepends the work dir
    silently agrees with a caller that folds the path first.
    """

    working_dir = "/home/workspace"
    allowed_directories = ("/home/workspace", "/tmp")

    def __init__(self, files: dict[str, bytes] | None = None):
        self.files = files if files is not None else {}
        self.downloads: list[str] = []

    def normalize_path(self, path: str) -> str:
        if path in ("", ".", "/"):
            return self.working_dir
        path = path.strip()
        for allowed in self.allowed_directories:
            if path.startswith(allowed):
                return str(PurePosixPath(path))
        if path.startswith("/"):
            return str(PurePosixPath(f"{self.working_dir}{path}"))
        return str(PurePosixPath(f"{self.working_dir}/{path}"))

    async def adownload_file_bytes(self, filepath: str) -> bytes | None:
        self.downloads.append(filepath)
        return self.files.get(filepath)


class _FakeSession:
    def __init__(self, sandbox):
        self.sandbox = sandbox


@pytest.fixture
def storage(monkeypatch):
    """Enable fake storage; record upload calls. Returns the call list."""
    uploads: list[tuple[str, bytes, str | None]] = []

    def upload_bytes(key, data, content_type=None):
        uploads.append((key, data, content_type))
        return True

    monkeypatch.setattr(ic, "is_storage_enabled", lambda: True)
    monkeypatch.setattr(ic, "upload_bytes", upload_bytes)
    monkeypatch.setattr(ic, "get_public_url", lambda key: f"https://cdn.test/{key}")
    return uploads


def _mw(sandbox) -> ImageCaptureMiddleware:
    return ImageCaptureMiddleware(session=_FakeSession(sandbox))


def _response(*messages) -> ModelResponse:
    return ModelResponse(result=list(messages))


async def _run(mw, response):
    async def handler(request):
        return response

    return await mw.awrap_model_call(None, handler)


@pytest.mark.asyncio
async def test_rewrites_sandbox_image_to_storage_url(storage):
    sandbox = _FakeSandbox({"/home/workspace/work/charts/rev.png": PNG})
    msg = AIMessage(content="Result: ![rev](work/charts/rev.png) done", id="m1")

    out = await _run(_mw(sandbox), _response(msg))

    assert out.result[0].content == (
        f"Result: ![rev](https://cdn.test/response-images/{PNG_SHA16}/rev.png) done"
    )
    assert out.result[0].id == "m1"  # id preserved → same message upserted
    assert storage[0][0] == f"response-images/{PNG_SHA16}/rev.png"
    assert storage[0][2] == "image/png"


@pytest.mark.asyncio
async def test_rewrites_text_blocks_only(storage):
    sandbox = _FakeSandbox({"/home/workspace/a.png": PNG})
    msg = AIMessage(
        content=[
            {"type": "text", "text": "see ![a](a.png)"},
            {"type": "tool_use", "id": "t1", "name": "x", "input": {}},
        ],
        id="m1",
    )

    out = await _run(_mw(sandbox), _response(msg))

    blocks = out.result[0].content
    assert f"https://cdn.test/response-images/{PNG_SHA16}/a.png" in blocks[0]["text"]
    assert blocks[1] == {"type": "tool_use", "id": "t1", "name": "x", "input": {}}


@pytest.mark.asyncio
async def test_noop_without_sandbox_images(storage):
    sandbox = _FakeSandbox()
    msg = AIMessage(content="external ![x](https://example.com/x.png) and text")

    out = await _run(_mw(sandbox), _response(msg))

    assert out.result[0] is msg
    assert not storage
    assert not sandbox.downloads


@pytest.mark.asyncio
async def test_noop_when_storage_disabled(monkeypatch):
    monkeypatch.setattr(ic, "is_storage_enabled", lambda: False)
    sandbox = _FakeSandbox({"/home/workspace/a.png": PNG})
    msg = AIMessage(content="![a](a.png)")

    out = await _run(_mw(sandbox), _response(msg))

    assert out.result[0] is msg
    assert not sandbox.downloads


@pytest.mark.asyncio
async def test_noop_without_sandbox(storage):
    out = await _run(
        ImageCaptureMiddleware(session=_FakeSession(None)),
        _response(AIMessage(content="![a](a.png)")),
    )
    assert out.result[0].content == "![a](a.png)"
    assert not storage


@pytest.mark.asyncio
async def test_download_failure_leaves_path(storage):
    sandbox = _FakeSandbox()  # file missing → download returns None
    msg = AIMessage(content="![a](work/a.png)")

    out = await _run(_mw(sandbox), _response(msg))

    assert out.result[0].content == "![a](work/a.png)"
    assert not storage


@pytest.mark.asyncio
async def test_upload_failure_leaves_path(monkeypatch):
    monkeypatch.setattr(ic, "is_storage_enabled", lambda: True)
    monkeypatch.setattr(ic, "upload_bytes", lambda *a, **k: False)
    monkeypatch.setattr(ic, "get_public_url", lambda key: f"https://cdn.test/{key}")
    sandbox = _FakeSandbox({"/home/workspace/a.png": PNG})
    msg = AIMessage(content="![a](a.png)")

    out = await _run(_mw(sandbox), _response(msg))

    assert out.result[0].content == "![a](a.png)"


@pytest.mark.asyncio
async def test_capture_exception_returns_response_unchanged(monkeypatch, storage):
    sandbox = _FakeSandbox({"/home/workspace/a.png": PNG})

    def boom(*a, **k):
        raise RuntimeError("scan failed")

    monkeypatch.setattr(ic, "_collect_sandbox_paths", boom)
    response = _response(AIMessage(content="![a](a.png)"))

    out = await _run(_mw(sandbox), response)

    assert out is response


@pytest.mark.asyncio
async def test_multiple_images_and_non_ai_messages(storage):
    other = b"jpg-bytes"
    other_sha16 = hashlib.sha256(other).hexdigest()[:16]
    sandbox = _FakeSandbox(
        {
            "/home/workspace/work/a.png": PNG,
            "/home/workspace/work/b.jpg": other,
        }
    )
    ai = AIMessage(content="![a](work/a.png) ![b](work/b.jpg) ![a2](work/a.png)")
    tool = ToolMessage(content="![a](work/a.png)", tool_call_id="t1")

    out = await _run(_mw(sandbox), _response(ai, tool))

    content = out.result[0].content
    assert content.count(f"https://cdn.test/response-images/{PNG_SHA16}/a.png") == 2
    assert f"https://cdn.test/response-images/{other_sha16}/b.jpg" in content
    assert len(storage) == 2  # dedup: a.png downloaded/uploaded once
    assert out.result[1] is tool  # non-AIMessage untouched


@pytest.mark.asyncio
async def test_absolute_work_dir_path_normalized(storage):
    sandbox = _FakeSandbox({"/home/workspace/work/a.png": PNG})
    msg = AIMessage(content="![a](/home/workspace/work/a.png)")

    out = await _run(_mw(sandbox), _response(msg))

    assert (
        f"https://cdn.test/response-images/{PNG_SHA16}/a.png" in out.result[0].content
    )


def test_is_sandbox_image_path():
    assert is_sandbox_image_path("work/a.png")
    assert is_sandbox_image_path("/home/workspace/work/a.png")
    assert not is_sandbox_image_path("https://example.com/a.png")
    assert not is_sandbox_image_path("data:image/png;base64,xxx")
    assert not is_sandbox_image_path("work/notes.txt")


@pytest.mark.asyncio
async def test_an_image_under_an_allowed_root_outside_the_work_dir_is_captured(storage):
    # /tmp is an allowed_directories default and matplotlib's savefig target, so
    # this is the ordinary case, not an exotic one. Folding the path to a
    # work-dir-relative form before normalize_path re-rooted it under the work
    # dir: the download missed, and the markdown silently kept a sandbox path
    # the browser cannot load.
    sandbox = _FakeSandbox({"/tmp/chart.png": PNG})
    msg = AIMessage(content="![chart](/tmp/chart.png)")

    out = await _run(_mw(sandbox), _response(msg))

    assert sandbox.downloads == ["/tmp/chart.png"]
    assert (
        f"https://cdn.test/response-images/{PNG_SHA16}/chart.png"
        in out.result[0].content
    )


@pytest.mark.asyncio
async def test_a_path_that_repeats_the_work_dir_keeps_its_segments(storage):
    # A directory that mirrors the work dir must not have its name spliced out
    # of the middle: ".../mirror/home/workspace/c.png" once collapsed to
    # "mirrorc.png" under a str.replace that stripped every occurrence.
    path = "/home/workspace/mirror/home/workspace/c.png"
    sandbox = _FakeSandbox({path: PNG})

    out = await _run(_mw(sandbox), _response(AIMessage(content=f"![c]({path})")))

    assert sandbox.downloads == [path]
    assert f"https://cdn.test/response-images/{PNG_SHA16}/c.png" in out.result[0].content


def test_image_storage_key_thread_scoping():
    sha = "ab" * 32
    assert image_storage_key("t1", sha, "a.png") == f"response-images/t1/{sha[:16]}/a.png"
    assert image_storage_key("", sha, "a.png") == f"response-images/{sha[:16]}/a.png"
