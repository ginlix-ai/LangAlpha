"""Capture sandbox images to durable storage, rewriting paths pre-checkpoint.

``ImageCaptureMiddleware`` rewrites sandbox image markdown in the model's
response to storage URLs *before* the message enters state, so checkpointed
truth carries durable URLs and replay needs no sandbox or rewrite map. Keys
are content-addressed (sha256 of the bytes), so any re-capture of the same
image — including the post-stream sse_events safety net — converges on the
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

# Capture rides the live turn-completion path (paths are rewritten pre-commit so
# checkpointed truth carries durable URLs). Bound it so a hung sandbox or storage
# backend can't stall the turn — on timeout the paths are left in place and the
# idempotent post-stream capture (same content-addressed keys) recovers them.
_CAPTURE_TIMEOUT_SECONDS = 30.0


def is_sandbox_image_path(path: str, work_dir: str = "/home/workspace") -> bool:
    """Check if path is a sandbox-relative image (not an external URL)."""
    if path.startswith(("http://", "https://", "//", "data:")):
        return False
    work_dir_prefix = work_dir.rstrip("/") + "/"
    normalized = (
        path.replace(work_dir_prefix, "") if path.startswith(work_dir_prefix) else path
    )
    ext = normalized.rsplit(".", 1)[-1].lower() if "." in normalized else ""
    return ext in IMAGE_EXTS


def image_storage_key(thread_id: str, sha_hex: str, basename: str) -> str:
    """Content-addressed storage key — deterministic, so re-captures converge."""
    prefix = f"response-images/{thread_id}" if thread_id else "response-images"
    return f"{prefix}/{sha_hex[:16]}/{basename}"


async def capture_sandbox_images(
    sandbox: Any, paths: set[str], thread_id: str = ""
) -> dict[str, str]:
    """Download sandbox image paths, upload content-addressed, return path→URL.

    Per-path captures run concurrently; each failure is logged and simply
    absent from the returned map (callers leave unmapped paths untouched).
    """
    work_dir_prefix = sandbox.working_dir.rstrip("/") + "/"

    async def _capture_one(path: str) -> tuple[str, str] | None:
        try:
            normalized = (
                path.replace(work_dir_prefix, "")
                if path.startswith(work_dir_prefix)
                else path
            )
            content = await sandbox.adownload_file_bytes(
                sandbox.normalize_path(normalized)
            )
            if not content:
                return None
            basename = normalized.rsplit("/", 1)[-1]
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


def rewrite_image_paths(text: str, url_map: dict[str, str]) -> str:
    """Rewrite markdown image paths present in *url_map* to their URLs."""

    def replacer(match: re.Match) -> str:
        alt, path = match.group(1), match.group(2)
        if path in url_map:
            return f"![{alt}]({url_map[path]})"
        return match.group(0)

    return IMAGE_MD_RE.sub(replacer, text)


def _collect_sandbox_paths(messages: list[Any], work_dir: str) -> set[str]:
    paths: set[str] = set()
    for text in _iter_text_segments(messages):
        for match in IMAGE_MD_RE.finditer(text):
            if is_sandbox_image_path(match.group(2), work_dir):
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


def _rewrite_content(content: Any, url_map: dict[str, str]) -> tuple[Any, bool]:
    if isinstance(content, str):
        rewritten = rewrite_image_paths(content, url_map)
        return rewritten, rewritten != content
    if isinstance(content, list):
        new_blocks: list[Any] = []
        changed = False
        for block in content:
            if isinstance(block, str):
                rewritten = rewrite_image_paths(block, url_map)
                changed = changed or rewritten != block
                new_blocks.append(rewritten)
            elif isinstance(block, dict) and block.get("type") == "text":
                text = block.get("text") or ""
                rewritten = rewrite_image_paths(text, url_map)
                if rewritten != text:
                    changed = True
                    new_blocks.append({**block, "text": rewritten})
                else:
                    new_blocks.append(block)
            else:
                new_blocks.append(block)
        return (new_blocks, True) if changed else (content, False)
    return content, False


class ImageCaptureMiddleware(AgentMiddleware):
    """Rewrite sandbox image paths to storage URLs in model responses.

    Runs on the final response only (place outside retry/fallback middleware);
    no-op when storage is disabled or the session has no sandbox. Failures
    leave the sandbox path in place — the post-stream capture safety net
    (content-addressed to the same keys) covers them.
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
                self._arewrite(response.result), _CAPTURE_TIMEOUT_SECONDS
            )
        except TimeoutError:
            logger.warning(
                "[ImageCapture] capture exceeded %ss; sandbox paths left in place",
                _CAPTURE_TIMEOUT_SECONDS,
            )
            return response
        except Exception:
            logger.warning(
                "[ImageCapture] capture failed; sandbox paths left in place",
                exc_info=True,
            )
            return response
        if result is None:
            return response
        return replace(response, result=result)

    async def _arewrite(self, messages: list[Any]) -> list[Any] | None:
        """Return rewritten messages, or None when nothing needs rewriting."""
        sandbox = getattr(self._session, "sandbox", None)
        if sandbox is None or not is_storage_enabled():
            return None
        paths = _collect_sandbox_paths(messages, sandbox.working_dir)
        if not paths:
            return None

        url_map = await capture_sandbox_images(sandbox, paths, self._thread_id())
        if not url_map:
            return None

        rewritten: list[Any] = []
        for message in messages:
            if isinstance(message, AIMessage):
                new_content, changed = _rewrite_content(message.content, url_map)
                if changed:
                    rewritten.append(message.model_copy(update={"content": new_content}))
                    continue
            rewritten.append(message)
        return rewritten

    @staticmethod
    def _thread_id() -> str:
        try:
            return (get_config().get("configurable") or {}).get("thread_id") or ""
        except Exception:
            return ""
