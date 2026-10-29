"""Write-side half of multimodal support: getting an image or PDF into history.

Intercepts a Read of an image/PDF path or URL, fetches the bytes, and hands back
the tool result carrying them as content blocks, so the attachment stays bound to
the call that produced it. A PDF is attached a window of pages at a time.

Whether the model about to be called can still read those blocks is a separate
question answered on every later turn, in ``multimodal_strip``.

Supported formats:
- Images: PNG, JPG, JPEG, GIF, WebP
- Documents: PDF
"""

import asyncio
import base64
import io
import logging
import weakref
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import httpx
from langchain.agents.middleware import AgentMiddleware
from langchain_core.messages import ToolMessage

from ptc_agent.agent.middleware.file_operations.image_fit import (
    UnreadableImage,
    UnsupportedImageFormat,
    identify_image,
)
from ptc_agent.agent.tools.computer_reach import (
    COMPUTER_ARG,
    ComputerReach,
    ComputerUnreachable,
    ran_elsewhere,
)
from ptc_agent.agent.tools.file_ops import is_memo_text_path
from ptc_agent.core.project_context import project_bound
from ptc_agent.core.sandbox.files import FileOverCap
from src.tools.web.inhouse.guard import (
    BlockedAddressError,
    OverCap,
    UnexpectedEncoding,
    UnexpectedStatus,
    fetch_capped,
)

logger = logging.getLogger(__name__)

# Supported image extensions
IMAGE_EXTENSIONS = frozenset({".png", ".jpg", ".jpeg", ".gif", ".webp"})

# Supported document extensions
DOCUMENT_EXTENSIONS = frozenset({".pdf"})

# Combined visual extensions
VISUAL_EXTENSIONS = IMAGE_EXTENSIONS | DOCUMENT_EXTENSIONS

# The most pages one Read attaches, and how many it attaches unasked. A
# provider reads each page as its text and an image of it, a few thousand
# tokens, so a whole report could fill the context window in one Read, and
# every later call would carry it. A window bounds what a Read costs whatever
# the document's length, and stays under every target's page ceiling, which
# the read side still checks.
PDF_PAGE_WINDOW = 20

# Raw bytes, deliberately below every provider's published ceiling: those are
# quoted on the base64 string, which is 4/3 larger, so 5 MiB here is ~6.7 MiB
# on the wire. Both fetch paths write the block into graph state via the tool
# result, so an oversized block is not a one-turn error: it is checkpointed
# and replayed on every later turn, and the modality strip cannot save a
# target that legitimately has the image modality. Refuse before encoding
# rather than brick the thread.
MAX_CONTENT_BYTES = 5 * 1024 * 1024


class _UnusableContent(Exception):
    """Why bytes can't be injected. The text reaches the agent, so it reads as a
    predicate ("not a readable PDF") that slots into the caller's message."""


class _TooLarge(Exception):
    """Content over a byte cap, which the refusal names."""

    def __init__(self, limit: int) -> None:
        super().__init__(limit)
        self.limit = limit


class _PastTheEnd(Exception):
    """A page window that starts after the PDF's last page."""

    def __init__(self, total: int) -> None:
        super().__init__(total)
        self.total = total


def _detect_mime_type(content: bytes) -> tuple[str, int | None]:
    """MIME type and page count, raising `_UnusableContent` if nothing accepts the bytes.

    The name can't decide this: a URL may carry no extension at all, and a
    sandbox path is only as accurate as whatever wrote the file. Both callers
    checkpoint what they inject, so anything a provider would reject has to be
    caught here — otherwise it replays its 400 on every later turn instead of
    failing once. Hence structural verification, not just a magic-byte peek.

    The page count is returned so it can be stamped on the block: the read side
    needs it to judge the PDF against the target's own ceiling, and re-deriving
    it there would mean decoding and reparsing every PDF in history per call.
    """
    if content.startswith(b"%PDF"):
        import pypdf

        try:
            pages = len(pypdf.PdfReader(io.BytesIO(content)).pages)
        except Exception:
            raise _UnusableContent("not a readable PDF") from None
        return "application/pdf", pages

    try:
        return identify_image(content), None
    except UnsupportedImageFormat as exc:
        raise _UnusableContent(str(exc)) from None
    except UnreadableImage:
        raise _UnusableContent("not a readable image or PDF") from None


def _page_window(offset: Any, limit: Any) -> tuple[int, int]:
    """(pages to skip, pages to attach) from Read's ``offset`` and ``limit``.

    The model sizes ``limit`` in lines unless it means pages, so anything past
    the window, or no usable number at all, is the window.
    """

    def _int(value: Any) -> int | None:
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    skip = max(_int(offset) or 0, 0)
    count = _int(limit)
    if count is None or not 0 < count <= PDF_PAGE_WINDOW:
        count = PDF_PAGE_WINDOW
    return skip, count


def _pdf_window(content: bytes, reader: Any, skip: int, count: int) -> bytes:
    """The PDF made of ``count`` pages after the first ``skip``, as its own
    document. The bytes as they are when the window is all of it, so a short
    PDF reaches the provider exactly as it was written."""
    if skip == 0 and count >= len(reader.pages):
        return content
    import pypdf

    try:
        writer = pypdf.PdfWriter()
        for page in reader.pages[skip : skip + count]:
            writer.add_page(page)
        out = io.BytesIO()
        writer.write(out)
    except Exception:
        raise _UnusableContent("not a readable PDF") from None
    return out.getvalue()


def _prepare(
    content: bytes, skip: int, count: int, source: str, max_bytes: int
) -> tuple[bytes, str, int | None, str | None]:
    """(bytes to attach, MIME type, pages attached, marker), the marker None
    unless pages remain after the window.

    A PDF window over ``max_bytes`` is halved until it fits rather than
    refused: the marker gives the offset after the pages actually attached, so
    a shorter window loses nothing, where a refusal would leave a document of
    large scanned pages unreadable at the default window.
    """
    mime_type, total = _detect_mime_type(content)
    if not total:
        # An image, or a PDF whose page tree reads as empty, which a provider
        # may still read: no window to cut, so it goes whole under the cap a
        # window has, its page count unknown.
        if len(content) > max_bytes:
            raise _TooLarge(max_bytes)
        return content, mime_type, None, None
    if skip >= total:
        raise _PastTheEnd(total)
    import pypdf

    try:
        reader = pypdf.PdfReader(io.BytesIO(content))
    except Exception:
        raise _UnusableContent("not a readable PDF") from None
    count = min(count, total - skip)
    window = _pdf_window(content, reader, skip, count)
    while len(window) > max_bytes:
        if count == 1:
            raise _TooLarge(max_bytes)
        count //= 2
        window = _pdf_window(content, reader, skip, count)
    last = skip + count
    marker = None
    if last < total:
        # 1-based for the model, and the next offset is the pages read so far.
        pages = f"Page {last}" if count == 1 else f"Pages {skip + 1}-{last}"
        marker = (
            f"[{pages} of {total}. Read(file_path='{source}', "
            f"offset={last}) for the next pages.]"
        )
    return window, mime_type, count, marker


#: Reads loading an attachment at once on one event loop. A PDF source may be
#: 64 MiB, held with a copy of it and the window cut from it, so a parallel
#: batch of Reads is bounded here, from the fetch until the window is attached.
#: The sandbox's own download slots cover neither a URL nor the cutting.
_LOADS_AT_ONCE = 4
_loads: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Semaphore] = (
    weakref.WeakKeyDictionary()
)


def _load_slot() -> asyncio.Semaphore:
    loop = asyncio.get_running_loop()
    slot = _loads.get(loop)
    if slot is None:
        slot = _loads[loop] = asyncio.Semaphore(_LOADS_AT_ONCE)
    return slot


def _is_visual_request(file_path: str) -> bool:
    """Whether a Read of this path or URL should carry an attachment back."""
    # A URL could be either an image or a PDF; only the bytes will say.
    if file_path.startswith(("http://", "https://")):
        return True

    # A memo PDF is served as extracted text and has no sandbox-FS copy to
    # fetch, so intercepting it would trade real content for a not-found error.
    if is_memo_text_path(file_path):
        return False

    return Path(file_path).suffix.lower() in VISUAL_EXTENSIONS


def _error(tool_call_id: str, text: str) -> ToolMessage:
    """A refusal the agent can read. The ``ERROR:`` prefix is the tool convention."""
    return ToolMessage(
        content=f"ERROR: {text}", tool_call_id=tool_call_id, status="error"
    )


def build_content_blocks(
    b64_string: str,
    source: str,
    mime_type: str,
    pages: int | None = None,
    marker: str | None = None,
) -> list[dict[str, Any]]:
    """Content blocks for verified bytes, labelled and ready to ride on a result.

    ``pages`` is stamped on the PDF block for the read-side ceiling check. Every
    provider converter drops keys it doesn't recognise, so it stays a local
    annotation and never reaches the wire. ``marker`` joins the PDF's label,
    keeping the one-text-plus-attachment shape every route was probed against.

    ``mime_type`` comes from ``_detect_mime_type``, which admits PDFs and the
    four image formats ``identify_image`` accepts and nothing else.
    """
    if mime_type == "application/pdf":
        # LangChain's file block, which converts to Anthropic's document format.
        filename = Path(source).name
        block: dict[str, Any] = {
            "type": "file",
            "base64": b64_string,
            "mime_type": mime_type,
            "filename": filename,
        }
        if pages is not None:
            block["pages"] = pages
        label = f"[Viewing PDF: {filename}]" + (f"\n{marker}" if marker else "")
        return [{"type": "text", "text": label}, block]

    return [
        {"type": "text", "text": "[Viewing image]"},
        {
            "type": "image_url",
            "image_url": {"url": f"data:{mime_type};base64,{b64_string}"},
        },
    ]


def attach_to_tool_result(
    tool_result: ToolMessage,
    content_blocks: list[dict[str, Any]],
) -> ToolMessage:
    """Carry the attachment on the tool result rather than a message after it.

    A separate carrier ``HumanMessage`` landed between the results of a parallel
    batch. That cost two things: a read-time reorder to satisfy Anthropic's rule
    that tool_result blocks open a user turn, and a compaction cutoff that could
    stop on the carrier and strand the rest of the group behind a summarized
    parent. On the tool result the attachment cannot be separated from the call
    that produced it, so neither is reachable.
    """
    ack = tool_result.text
    blocks = list(content_blocks)

    if ack and blocks and blocks[0].get("type") == "text":
        # Folded into the existing label instead of prepended as a second text
        # block, so the result keeps the one-text-plus-attachment shape every
        # route was probed against.
        blocks[0] = {**blocks[0], "text": f"{ack}\n{blocks[0].get('text', '')}"}
    elif ack:
        blocks.insert(0, {"type": "text", "text": ack})

    return tool_result.model_copy(update={"content": blocks})


class MultimodalMiddleware(AgentMiddleware):
    """Intercept Read for images/PDFs and attach the bytes to the tool result.

    Content is always downloaded and base64-encoded rather than passed by URL:
    several providers cannot fetch an external URL at all, and the ones that can
    cannot reach a private bucket.
    """

    # Must track the name file_ops.py registers (``@tool("Read")``) — this is
    # what awrap_tool_call matches on, and a mismatch silently turns the whole
    # middleware into dead code rather than failing anywhere visible.
    TOOL_NAME = "Read"

    MAX_CONTENT_BYTES = MAX_CONTENT_BYTES

    # A PDF is fetched whole but attached a window at a time, and only the
    # window reaches the checkpoint, so MAX_CONTENT_BYTES bounds the window and
    # this bounds the source: what a model-chosen file may cost the backend to
    # hold and parse.
    MAX_PDF_SOURCE_BYTES = 64 * 1024 * 1024

    def __init__(self, *, sandbox: Any, computers: ComputerReach | None = None) -> None:
        """
        Args:
            sandbox: PTCSandbox the Read tool is backed by, used to fetch the
                raw bytes of a path the tool only acknowledged.
            computers: The user's other computers, for a Read that names one
                (the Chief of Staff's); its bytes are fetched from there.
        """
        super().__init__()
        self.sandbox = sandbox
        self._computers = computers

    async def awrap_tool_call(
        self,
        request: Any,
        handler: Callable[[Any], Awaitable[Any]],
    ) -> Any:
        tool_call = request.tool_call
        if tool_call.get("name") != self.TOOL_NAME:
            return await handler(request)

        tool_call_id = tool_call.get("id", "unknown")
        args = tool_call.get("args", {})
        file_path = args.get("file_path", "")
        if not _is_visual_request(file_path):
            return await handler(request)
        window = _page_window(args.get("offset"), args.get("limit"))

        # Injected unconditionally, without asking whether the model can view it.
        # This middleware is a single instance shared with every subagent, so the
        # only model it could ask about is the configured one — and gating on that
        # withheld the blocks from a subagent running its own vision model, which
        # the read-side strip cannot undo because it only ever removes blocks. The
        # cost is that a text-only turn still checkpoints content it will strip on
        # every read; that is the accepted price of never silently losing a read.
        logger.debug(f"[MULTIMODAL] Intercepting Read for visual content: {file_path}")

        result = await handler(request)

        # The tool already failed; there are no bytes to go looking for.
        if isinstance(result.content, str) and result.content.startswith("ERROR:"):
            return result

        async with _load_slot():
            if file_path.startswith(("http://", "https://")):
                return await self._handle_url_content(file_path, result, tool_call_id, window)
            return await self._handle_sandbox_content(
                file_path,
                result,
                tool_call_id,
                window,
                computer=args[COMPUTER_ARG].strip() if ran_elsewhere(args) else None,
            )

    def _too_large(self, source: str, tool_call_id: str, limit: int) -> ToolMessage:
        """Refuse an oversized block with a bare error result.

        The point of refusing is that nothing oversized reaches the checkpoint,
        so the refusal carries no content blocks at all.
        """
        limit_mb = limit // (1024 * 1024)
        logger.warning(f"[MULTIMODAL] Content exceeds {limit} bytes, refusing: {source}")
        return _error(
            tool_call_id,
            f"Content is larger than the {limit_mb}MB limit and was not loaded: {source}",
        )

    def _cap_for(self, head: bytes) -> int:
        """The byte cap for a body that starts with ``head``.

        The cap follows the bytes, the way the content is typed: a URL need not
        end in .pdf, and a sandbox name is only as accurate as its writer.
        Checked as the bytes arrive, since a Content-Length header or a file
        size is only a claim until they do.
        """
        return self.MAX_PDF_SOURCE_BYTES if head.startswith(b"%PDF") else self.MAX_CONTENT_BYTES

    async def _attach_bytes(
        self,
        content_bytes: bytes,
        source: str,
        tool_result: ToolMessage,
        tool_call_id: str,
        window: tuple[int, int] = (0, PDF_PAGE_WINDOW),
    ) -> ToolMessage:
        """Type, verify and encode fetched bytes onto the tool result, a PDF
        cut to the ``window`` of pages (skip, count) asked for.

        Typed from the bytes rather than the name: a signed or redirected URL can
        end without an extension, and a sandbox extension is only as accurate as
        whatever wrote the file — a truncated chart named .png is checkpointed
        before any provider sees it.
        """
        try:
            # Off-loop: pypdf walks the xref of up to MAX_PDF_SOURCE_BYTES of
            # model-chosen content, which is milliseconds on a real document
            # but ~100ms on one padded with junk after %%EOF, and copies the
            # window's pages out, again for each halving.
            content_bytes, mime_type, pages, marker = await asyncio.to_thread(
                _prepare, content_bytes, *window, source, self.MAX_CONTENT_BYTES
            )
        except _TooLarge as exc:
            return self._too_large(source, tool_call_id, exc.limit)
        except _UnusableContent as exc:
            logger.warning(f"[MULTIMODAL] Refusing content ({exc}): {source}")
            return _error(tool_call_id, f"Content is {exc}: {source}")
        except _PastTheEnd as exc:
            return _error(
                tool_call_id,
                f"offset={window[0]} is past the last page of this {exc.total}-page "
                f"PDF: {source}",
            )

        b64_string = base64.b64encode(content_bytes).decode("utf-8")
        logger.info(
            f"[MULTIMODAL] Attaching content to the tool result: {source} "
            f"({len(content_bytes)} bytes, {mime_type})"
        )
        return attach_to_tool_result(
            tool_result, build_content_blocks(b64_string, source, mime_type, pages, marker)
        )

    async def _handle_url_content(
        self,
        url: str,
        tool_result: ToolMessage,
        tool_call_id: str,
        window: tuple[int, int] = (0, PDF_PAGE_WINDOW),
    ) -> ToolMessage:
        """Download a URL and attach its bytes, or answer with why it could not."""
        try:
            # The URL is model-chosen and this fetch runs in the backend process
            # — not the sandbox — so it inherits the backend's reach into the
            # private network. fetch_capped re-checks the resolved address on
            # every hop, which is what keeps a public page from redirecting the
            # fetch at 169.254.169.254.
            content_bytes = await fetch_capped(url, self._cap_for)

            if not content_bytes:
                logger.warning(f"[MULTIMODAL] Empty content from URL: {url}")
                return _error(tool_call_id, f"Empty content from URL: {url}")

            return await self._attach_bytes(
                content_bytes, url, tool_result, tool_call_id, window
            )

        except UnexpectedStatus as exc:
            logger.warning(
                f"[MULTIMODAL] Failed to download content: {url} "
                f"(status {exc.status_code})"
            )
            return _error(
                tool_call_id,
                f"Could not download content (HTTP {exc.status_code}): {url}",
            )
        except OverCap as exc:
            return self._too_large(url, tool_call_id, exc.limit)
        except UnexpectedEncoding as exc:
            return _error(
                tool_call_id,
                f"Could not read content sent {exc.encoding}-compressed; only uncompressed content is read: {url}",
            )
        except httpx.TimeoutException:
            logger.warning(f"[MULTIMODAL] Timeout downloading content: {url}")
            return _error(tool_call_id, f"Timeout downloading content: {url}")
        except BlockedAddressError as e:
            # The message names the resolved address ("Blocked private/reserved
            # address 10.1.2.3 for host x"). The URL is model-chosen, so echoing
            # that back turns Read into a DNS-to-IP probe steerable by anything
            # the agent reads. Keep the detail in the log, not in the transcript.
            logger.warning(f"[MULTIMODAL] Blocked address for {url}: {e}")
            return _error(tool_call_id, f"This address is not allowed: {url}")
        except httpx.RequestError as e:
            logger.warning(f"[MULTIMODAL] Network error downloading content {url}: {e}")
            return _error(tool_call_id, f"Network error downloading content: {e}")
        except Exception as e:
            logger.warning(f"[MULTIMODAL] Unexpected error handling URL {url}: {e}")
            return _error(tool_call_id, f"Failed to load content: {e}")

    async def _handle_sandbox_content(
        self,
        file_path: str,
        tool_result: ToolMessage,
        tool_call_id: str,
        window: tuple[int, int] = (0, PDF_PAGE_WINDOW),
        *,
        computer: str | None = None,
    ) -> ToolMessage:
        """Fetch a sandbox file's bytes and attach them, or answer with why not."""
        sandbox, project = self.sandbox, None
        if computer and self._computers is not None:
            # Reached already by the tool's own call, so this reads the same
            # session back; one gone since answers as that call would have.
            try:
                target = await self._computers.reach(computer)
            except ComputerUnreachable as e:
                return _error(tool_call_id, str(e))
            if target is not None:
                sandbox, project = target.sandbox, target.project
        try:
            # The Read tool reaches the sandbox through SandboxBackend, which
            # normalizes first; this middleware holds the sandbox itself, whose
            # download does not. Without this, a virtual path the tool resolved
            # fine ("/work/task/charts/c.png") misses here and replaces the
            # tool's success with a not-found error.
            with project_bound(project):
                path = sandbox.normalize_path(file_path)
            file_bytes = await sandbox.adownload_file_bytes_capped(path, self._cap_for)
            if not file_bytes:
                logger.warning(f"[MULTIMODAL] Failed to download file: {file_path}")
                return _error(tool_call_id, f"Could not read file: {file_path}")

            return await self._attach_bytes(
                file_bytes, file_path, tool_result, tool_call_id, window
            )

        except (_TooLarge, FileOverCap) as exc:
            return self._too_large(file_path, tool_call_id, exc.limit)
        except (OSError, ValueError) as e:
            logger.warning(f"[MULTIMODAL] Error loading sandbox file {file_path}: {e}")
            return _error(tool_call_id, f"Failed to load file: {e}")


__all__ = ["MultimodalMiddleware", "attach_to_tool_result", "build_content_blocks"]
