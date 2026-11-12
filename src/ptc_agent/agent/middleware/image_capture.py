"""Capture sandbox images to durable storage, rewriting paths pre-checkpoint.

``ImageCaptureMiddleware`` rewrites sandbox image markdown in the model's
response to storage URLs *before* the message enters state, so checkpointed
truth carries durable URLs and replay needs no sandbox or rewrite map. Keys
are content-addressed (sha256 of the bytes), so any re-capture of the same
image, including the late capture when the turn settles, converges on the
identical URL with no coordination.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import mimetypes
import re
from collections.abc import Awaitable, Callable
from dataclasses import replace
from typing import Any

from ptc_agent.core.project_context import ProjectContext

from langchain.agents.middleware.types import (
    AgentMiddleware,
    ModelRequest,
    ModelResponse,
)
from langchain_core.messages import AIMessage
from langgraph.config import get_config

from src.utils.storage import get_public_url, is_storage_enabled, upload_bytes

logger = logging.getLogger(__name__)

IMAGE_EXTS = {"png", "jpg", "jpeg", "gif", "svg", "webp", "bmp"}
IMAGE_MD_RE = re.compile(r"!\[([^\]]*)\]\(([^)]+)\)")
# An embed or a plain link: a reply often lists the file beside its picture.
_IMAGE_REF_RE = re.compile(r"(!?)\[([^\]]*)\]\(([^)]+)\)")

# Capture rides each model call, and the late capture rides the run's
# finalize. Both are bounded so a hung sandbox or storage backend can't stall
# the turn: a path the model-time capture misses is retried late, at the same
# content-addressed keys.
CAPTURE_TIMEOUT_SECONDS = 30.0

# Where a message keeps the URL each sandbox image it names was captured to,
# by the path as written. The text keeps the path: a URL in the agent's own
# history is one it copies, and a copied URL names some earlier capture, or
# one it made up, instead of the file. Readers show the URL in its place: the
# live view from the run's row, a replay from the row or from here.
CAPTURED_IMAGES_KEY = "captured_images"


def recorded_images(message: Any) -> dict[str, str]:
    """The captures an AI message recorded, by path; none for any other."""
    if not isinstance(message, AIMessage):
        return {}
    captured = (message.response_metadata or {}).get(CAPTURED_IMAGES_KEY)
    if not isinstance(captured, dict):
        return {}
    return {str(k): str(v) for k, v in captured.items() if k and v}


def resolve_image_paths(text: str, url_map: dict[str, str]) -> str:
    """``text`` with each embed of and link to a captured path pointing at its
    capture, for a reader with no sandbox to resolve the path against."""
    if not url_map:
        return text

    def replacer(match: re.Match) -> str:
        bang, label, path = match.groups()
        url = url_map.get(path)
        return f"{bang}[{label}]({url})" if url else match.group(0)

    return _IMAGE_REF_RE.sub(replacer, text)


def is_sandbox_image_path(path: str) -> bool:
    """Check if path is a sandbox-relative image (not an external URL)."""
    if path.startswith(("http://", "https://", "//", "data:")):
        return False
    basename = path.rsplit("/", 1)[-1]
    ext = basename.rsplit(".", 1)[-1].lower() if "." in basename else ""
    return ext in IMAGE_EXTS


def image_storage_key(thread_id: str, sha_hex: str, basename: str) -> str:
    """Content-addressed storage key — deterministic, so re-captures converge."""
    prefix = f"response-images/{thread_id}" if thread_id else "response-images"
    return f"{prefix}/{sha_hex[:16]}/{basename}"


async def capture_sandbox_images(
    sandbox: Any,
    paths: set[str],
    thread_id: str = "",
    *,
    project: ProjectContext | None = None,
) -> dict[str, str]:
    """Download sandbox image paths, upload content-addressed, return path→URL.

    Per-path captures run concurrently; each failure is logged and simply
    absent from the returned map (callers leave unmapped paths untouched).

    Paths go to ``normalize_path`` exactly as the agent wrote them: it already
    resolves every spelling, and it is the only thing that knows an absolute
    path may name an allowed root outside the work dir (``/tmp``, matplotlib's
    default savefig target). Folding to a work-dir-relative form first would
    re-root those under the work dir and the image would vanish.
    """

    async def _capture_one(path: str) -> tuple[str, str] | None:
        try:
            normalized = (
                sandbox.normalize_path(path, project=project)
                if project is not None
                else sandbox.normalize_path(path)
            )
            content = await sandbox.adownload_file_bytes(normalized)
            if not content:
                return None
            basename = path.rsplit("/", 1)[-1]
            key = image_storage_key(
                thread_id, hashlib.sha256(content).hexdigest(), basename
            )
            content_type, _ = mimetypes.guess_type(basename)
            # upload_bytes is sync (boto3) — run in thread to avoid blocking
            if await asyncio.to_thread(upload_bytes, key, content, content_type):
                logger.info(f"[IMAGE_CAPTURE] Uploaded {path} → {key}")
                return path, get_public_url(key)
        except Exception as e:
            logger.warning(f"[IMAGE_CAPTURE] Failed to capture {path}: {e}")
        return None

    results = await asyncio.gather(*(_capture_one(path) for path in paths))
    return dict(r for r in results if r is not None)


def collect_sandbox_paths(messages: list[Any]) -> set[str]:
    """The sandbox image paths the AI messages' text names."""
    paths: set[str] = set()
    for text in _iter_text_segments(messages):
        for match in IMAGE_MD_RE.finditer(text):
            if is_sandbox_image_path(match.group(2)):
                paths.add(match.group(2))
    return paths


def _iter_text_segments(messages: list[Any]):
    for message in messages:
        if not isinstance(message, AIMessage):
            continue
        if isinstance(message.content, str):
            yield message.content
        elif isinstance(message.content, list):
            for block in message.content:
                if isinstance(block, str):
                    yield block
                elif isinstance(block, dict) and block.get("type") == "text":
                    yield block.get("text") or ""


class ImageCaptureMiddleware(AgentMiddleware):
    """Capture the sandbox images a model response names, as the file is when
    the response is written, and record each one's URL on its message.

    Runs on the final response only (place outside retry/fallback middleware);
    no-op when storage is disabled or the session has no sandbox. A failure
    records nothing, and the run's finalize captures what is left.
    """

    def __init__(self, *, session: Any) -> None:
        self._session = session

    async def awrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], Awaitable[ModelResponse]],
    ) -> ModelResponse:
        response = await handler(request)
        try:
            result = await asyncio.wait_for(
                self._arecord(response.result), CAPTURE_TIMEOUT_SECONDS
            )
        except TimeoutError:
            logger.warning(
                "[ImageCapture] capture exceeded %ss; left to the run's end",
                CAPTURE_TIMEOUT_SECONDS,
            )
            return response
        except Exception:
            logger.warning(
                "[ImageCapture] capture failed; left to the run's end",
                exc_info=True,
            )
            return response
        if result is None:
            return response
        return replace(response, result=result)

    async def _arecord(self, messages: list[Any]) -> list[Any] | None:
        """The messages with their captures recorded, or None when nothing
        was captured."""
        sandbox = getattr(self._session, "sandbox", None)
        if sandbox is None or not is_storage_enabled():
            return None
        paths = collect_sandbox_paths(messages)
        if not paths:
            return None

        url_map = await capture_sandbox_images(sandbox, paths, self._thread_id())
        if not url_map:
            return None

        recorded: list[Any] = []
        for message in messages:
            if isinstance(message, AIMessage):
                captured = {
                    path: url_map[path]
                    for path in collect_sandbox_paths([message])
                    if path in url_map
                }
                if captured:
                    recorded.append(message.model_copy(update={
                        "response_metadata": {
                            **message.response_metadata,
                            CAPTURED_IMAGES_KEY: captured,
                        },
                    }))
                    continue
            recorded.append(message)
        return recorded

    @staticmethod
    def _thread_id() -> str:
        try:
            return (get_config().get("configurable") or {}).get("thread_id") or ""
        except Exception:
            return ""
