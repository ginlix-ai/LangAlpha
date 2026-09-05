"""Multimodal middleware for injecting images and PDFs into LLM conversations.

This middleware intercepts Read tool calls for image/PDF paths and URLs,
downloading the content and injecting it as a HumanMessage content block
for multimodal models.

Architecture:
- Intercepts Read tool calls that match image/PDF patterns
- Downloads content from sandbox or URL
- Injects as HumanMessage using LangGraph's Command pattern
- Passes through non-visual Read calls unchanged

Supported formats:
- Images: PNG, JPG, JPEG, GIF, WebP
- Documents: PDF
"""

import asyncio
import base64
import io
import logging
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import httpx
from langchain.agents.middleware import AgentMiddleware
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langgraph.types import Command
from PIL import Image

from ptc_agent.agent.middleware._utils import append_to_system_message
from ptc_agent.agent.tools.file_ops import is_memo_text_path
from src.llms.llm import get_input_modalities, get_max_pdf_pages
from src.tools.web.inhouse.guard import BlockedAddressError, GuardedAsyncTransport

logger = logging.getLogger(__name__)

# Supported image extensions
IMAGE_EXTENSIONS = frozenset({".png", ".jpg", ".jpeg", ".gif", ".webp"})

# Supported document extensions
DOCUMENT_EXTENSIONS = frozenset({".pdf"})

# Combined visual extensions
VISUAL_EXTENSIONS = IMAGE_EXTENSIONS | DOCUMENT_EXTENSIONS

# PIL reports a format name; providers speak MIME. A format outside this map is
# one no provider accepts, so it is refused rather than given a near-miss guess.
_PIL_FORMAT_TO_MIME = {
    "PNG": "image/png",
    "JPEG": "image/jpeg",
    "GIF": "image/gif",
    "WEBP": "image/webp",
}

# The widest per-request page ceiling any provider we ship publishes (Gemini's).
# Deliberately not the tightest: injection cannot know which model will consume
# the block — the middleware instance is shared with every subagent — so a cap
# picked for the strictest target would refuse documents the actual target
# accepts. What each target can really take is enforced on the read side, where
# the model is known; this bound only rejects what nobody would accept.
MAX_PDF_PAGES = 1000


class _UnusableContent(Exception):
    """Why bytes can't be injected. The text reaches the agent, so it reads as a
    predicate ("not a readable PDF") that slots into the caller's message."""


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
        if pages > MAX_PDF_PAGES:
            raise _UnusableContent(
                f"a {pages}-page PDF, over the {MAX_PDF_PAGES}-page limit"
            )
        return "application/pdf", pages

    try:
        img = Image.open(io.BytesIO(content))
        img.verify()
    except Exception:
        raise _UnusableContent("not a readable image or PDF") from None
    mime_type = _PIL_FORMAT_TO_MIME.get(img.format or "")
    if mime_type is None:
        raise _UnusableContent(f"in {img.format} format, which no provider accepts")
    return mime_type, None


def _strip_unsupported_content_blocks(
    messages: list,
    has_image: bool,
    has_pdf: bool,
    max_pdf_pages: int | None = None,
) -> list:
    """Replace unsupported image/file content blocks with text placeholders.

    ``max_pdf_pages`` is the target's own page ceiling (None = unbounded). A PDF
    over it is dropped here rather than refused at injection, because the ceiling
    is a property of the model that reads the block, not of the tool call that
    created it — the same document is fine on a 1M-context route and rejected on
    a 200K one, and only this side knows which one is being called.

    Returns the original list if no changes needed (avoids unnecessary copies).
    Does not mutate the original messages (checkpoint integrity).
    """
    modified = False
    result = []

    for msg in messages:
        content = msg.content if hasattr(msg, "content") else None
        if not isinstance(content, list):
            result.append(msg)
            continue

        new_blocks = []
        msg_modified = False
        for block in content:
            if not isinstance(block, dict):
                new_blocks.append(block)
                continue

            block_type = block.get("type", "")

            # Check image_url blocks
            if block_type == "image_url" and not has_image:
                new_blocks.append({
                    "type": "text",
                    "text": "[Image attached in prior turn — not visible to current model]"
                })
                msg_modified = True
                continue

            # Any block typed "image" — covers the Anthropic-native `source`
            # shape and the langchain v1 `base64` one. Matched on the type alone
            # rather than a key, so a shape that drifts on a lib bump still gets
            # stripped: this is the only thing standing between a mid-thread
            # switch and a 400, and failing closed here only costs a placeholder.
            if block_type == "image" and not has_image:
                new_blocks.append({
                    "type": "text",
                    "text": "[Image attached in prior turn — not visible to current model]"
                })
                msg_modified = True
                continue

            # Any block typed "file", matched on the type alone for the same
            # reason as "image" above: `pdf` is the only file-modality flag the
            # manifest carries, so a file block we can't classify — mime absent,
            # null, or non-PDF — is one a model without it has no way to accept.
            if block_type == "file" and not has_pdf:
                new_blocks.append({
                    "type": "text",
                    "text": "[PDF attached in prior turn — not visible to current model]"
                })
                msg_modified = True
                continue

            # Over the target's page ceiling. Unstamped blocks pass: they predate
            # the stamp, and re-deriving the count would mean decoding every PDF
            # in history on every call. They keep the old failure mode; nothing
            # that already worked starts failing here.
            if (
                block_type == "file"
                and max_pdf_pages is not None
                and isinstance(block.get("pages"), int)
                and block["pages"] > max_pdf_pages
            ):
                new_blocks.append({
                    "type": "text",
                    "text": (
                        f"[PDF attached in prior turn — {block['pages']} pages, over "
                        f"the current model's {max_pdf_pages}-page limit]"
                    ),
                })
                msg_modified = True
                continue

            new_blocks.append(block)

        if msg_modified:
            modified = True
            # Create a copy of the message with new content
            if hasattr(msg, "model_copy"):
                new_msg = msg.model_copy(update={"content": new_blocks})
            else:
                new_msg = type(msg)(content=new_blocks, **{
                    k: v for k, v in (msg.__dict__ if hasattr(msg, "__dict__") else {}).items()
                    if k != "content"
                })
            result.append(new_msg)
        else:
            result.append(msg)

    return result if modified else messages


# One note for both strip reasons: the placeholder already states which one
# applied, next to the block it replaced, so branching the reminder would only
# duplicate that where the model is less likely to be reading.
_UNSUPPORTED_NOTE = (
    "\n\n<system-reminder>"
    "Content shown as a bracketed placeholder is not visible to the current model; "
    "the placeholder states why. Be transparent with the user about the limitation: "
    "an unsupported input type means switching to a model that accepts it, and a "
    "page-limit rejection means splitting the document or moving to a model with a "
    "longer context. Work in best effort to answer their query."
    "</system-reminder>"
)


def _tool_results_first(messages: list) -> list:
    """Order each tool-result run so its ToolMessages precede injected content.

    A visual Read resolves to a Command carrying ``[ToolMessage, HumanMessage]``,
    and a parallel batch interleaves that with the other calls' results —
    ``tool_result, text, image, tool_result``. Anthropic requires a user turn's
    tool_result blocks to come before any other content and 400s otherwise, and
    since the Command is checkpointed the bad order would replay every turn.
    Repaired here rather than at injection time because only this side sees the
    whole batch; the checkpoint keeps the original order and heals on read.
    """
    out: list = []
    run: list = []
    in_run = False
    changed = False

    def flush() -> None:
        nonlocal changed
        if not run:
            return
        needs, seen_other = False, False
        for msg in run:
            if isinstance(msg, ToolMessage):
                if seen_other:
                    needs = True
                    break
            else:
                seen_other = True
        if needs:
            changed = True
            out.extend(m for m in run if isinstance(m, ToolMessage))
            out.extend(m for m in run if not isinstance(m, ToolMessage))
        else:
            out.extend(run)
        run.clear()

    for msg in messages:
        if isinstance(msg, AIMessage):
            flush()
            in_run = bool(getattr(msg, "tool_calls", None))
            out.append(msg)
        elif in_run:
            run.append(msg)
        else:
            out.append(msg)
    flush()

    return out if changed else messages


def _is_visual_request(file_path: str) -> bool:
    """Check if the file_path is a visual file (image or PDF - URL or file extension).

    Args:
        file_path: Path or URL to check.

    Returns:
        True if this is a visual file request, False otherwise.
    """
    # Check for URLs (could be image or PDF)
    if file_path.startswith(("http://", "https://")):
        return True

    # A memo PDF is served as extracted text and has no sandbox-FS copy to
    # fetch, so intercepting it would trade real content for a not-found error.
    if is_memo_text_path(file_path):
        return False

    # Check for visual file extensions (images + documents)
    suffix = Path(file_path).suffix.lower()
    return suffix in VISUAL_EXTENSIONS


class MultimodalMiddleware(AgentMiddleware):
    """Middleware that intercepts Read for images/PDFs and injects them as HumanMessage.

    When Read is called with an image/PDF path or URL, this middleware:
    1. Executes the tool to get the acknowledgment message
    2. Downloads the content (from URL or sandbox)
    3. Converts to base64 for universal LLM provider compatibility
    4. Returns a Command that injects both:
       - The ToolMessage (for tool call completion)
       - A HumanMessage with the base64 content (for multimodal model processing)

    Non-visual Read calls pass through unchanged.

    Independently of that, it strips image/PDF blocks out of history when the
    current model lacks the modality — the half that keeps a mid-thread switch
    to a text-only model from replaying an earlier turn's blocks. That half
    needs no sandbox, so an agent with no Read tool wires it with
    ``sandbox=None`` to get the strip alone.

    Note: Content is always downloaded and converted to base64 because many LLM
    providers (like Anthropic) cannot fetch external URLs directly.

    Supported formats:
    - Images: PNG, JPG, JPEG, GIF, WebP (using image_url content block)
    - Documents: PDF (using LangChain's file content block)

    Attributes:
        sandbox: PTCSandbox instance for reading files from sandbox paths
    """

    # Must track the name file_ops.py registers (``@tool("Read")``) — this is
    # what awrap_tool_call matches on, and a mismatch silently turns the whole
    # injection half into dead code rather than failing anywhere visible.
    TOOL_NAME = "Read"

    # Raw bytes, deliberately below every provider's published ceiling: those are
    # quoted on the base64 string, which is 4/3 larger, so 5 MiB here is ~6.7 MiB
    # on the wire. Both injection paths return a Command that writes the block
    # into graph state, so an oversized block is not a one-turn error: it is
    # checkpointed and replayed on every later turn, and the modality strip
    # cannot save a target that legitimately has the image modality. Refuse
    # before encoding rather than brick the thread.
    MAX_CONTENT_BYTES = 5 * 1024 * 1024

    def __init__(
        self,
        *,
        sandbox: Any | None = None,
        model_name: str | None = None,
        custom_modalities: list[str] | None = None,
    ) -> None:
        """Initialize the MultimodalMiddleware.

        Args:
            sandbox: PTCSandbox instance for reading files from sandbox paths.
                    Required for local file support.
            model_name: LLM model name for capability checking (from models.json).
                       Used to determine which input modalities the model supports.
            custom_modalities: Override modalities for custom models (from user
                              preferences). When set, bypasses the models.json lookup.
        """
        super().__init__()
        self.sandbox = sandbox
        self.model_name = model_name
        self.custom_modalities = custom_modalities

    def wrap_tool_call(
        self,
        request: Any,
        handler: Callable[[Any], Any],
    ) -> Any:
        """Synchronous wrapper - delegates to async implementation.

        Note: File handling requires async, so this sync wrapper is limited.
        For production use, prefer async execution via awrap_tool_call.
        """
        tool_call = request.tool_call
        tool_name = tool_call.get("name")

        # Pass through non-target tools
        if tool_name != self.TOOL_NAME:
            return handler(request)

        # Check if this is a visual request
        tool_args = tool_call.get("args", {})
        file_path = tool_args.get("file_path", "")

        if not _is_visual_request(file_path):
            return handler(request)

        # For sync execution, just run the tool without content injection
        logger.warning(
            "[MULTIMODAL] Sync execution detected. Visual content will not be injected. "
            "Use async execution for full functionality."
        )
        return handler(request)

    def _too_large_message(self, source: str, tool_call_id: str) -> ToolMessage:
        """Refuse an oversized block as a plain ToolMessage, never a Command.

        A Command would write the block into graph state; the point of refusing
        is that nothing oversized reaches the checkpoint.
        """
        limit_mb = self.MAX_CONTENT_BYTES // (1024 * 1024)
        logger.warning(
            f"[MULTIMODAL] Content exceeds {self.MAX_CONTENT_BYTES} bytes, refusing: {source}"
        )
        return ToolMessage(
            content=(
                f"ERROR: Content is larger than the {limit_mb}MB limit and was "
                f"not loaded: {source}"
            ),
            tool_call_id=tool_call_id,
        )

    def _resolve_target(self, request: Any) -> tuple[str | None, list[str]]:
        """Name and modalities of the model this request will actually reach.

        Read off ``request.model`` rather than the configured name so a
        resilience fallback — or a subagent running its own model — is judged on
        the client in hand. The name comes back too because the PDF page ceiling
        is per-model and has to be looked up against the same target.
        """
        metadata = getattr(request.model, "metadata", None)
        stamped = metadata.get("manifest_model") if isinstance(metadata, dict) else None

        if not isinstance(stamped, str) or not stamped:
            # Unattributable: the client skipped LLM.get_llm(), which today means
            # a subagent whose config names a bare model string for deepagents to
            # resolve via init_chat_model. Falling back to the configured name
            # would lend a vision parent's modalities to a text-only target and
            # replay exactly the blocks this strip exists to remove, so an
            # unstamped client is judged text-only — the same fail-closed reading
            # LLM.get_llm() documents when it writes the stamp.
            return None, ["text"]
        # ``custom_modalities`` is a per-model override from user preferences, so
        # it describes only the model it was configured for — a fallback is a
        # different model and has to be judged on its own manifest entry.
        overrides = self.custom_modalities if stamped == self.model_name else None
        return stamped, get_input_modalities(stamped, custom_modalities=overrides)

    async def awrap_model_call(self, request, handler):
        """Strip unsupported content blocks from historical messages for text-only models.

        When a user switches from a vision model to a text-only model mid-thread,
        the checkpoint contains image/PDF content blocks that would cause 400 errors.
        This method replaces those blocks with text placeholders.
        """
        # Runs for every target, not just modality-limited ones: the interleaving
        # only arises where injection succeeded, which is a model that can see it.
        messages = _tool_results_first(request.messages)
        overrides: dict[str, Any] = {}

        stamped, modalities = self._resolve_target(request)
        has_image = "image" in modalities
        has_pdf = "pdf" in modalities
        # Looked up even for a fully multimodal target: a model that accepts PDFs
        # can still be handed one past its page ceiling, which is the judgement
        # the tool side deliberately no longer attempts.
        max_pdf_pages = get_max_pdf_pages(stamped) if stamped and has_pdf else None

        sanitized = _strip_unsupported_content_blocks(
            messages, has_image, has_pdf, max_pdf_pages
        )
        if sanitized is not messages:
            messages = sanitized
            # Told to the model that actually can't see the file, on the call
            # where that is true. The tool-time note this replaces judged the
            # configured model and froze that verdict into the transcript,
            # which a subagent on its own model then inherited wrongly.
            overrides["system_message"] = append_to_system_message(
                request.system_message, _UNSUPPORTED_NOTE
            )

        if messages is not request.messages:
            overrides["messages"] = messages
        return await handler(request.override(**overrides) if overrides else request)

    async def awrap_tool_call(
        self,
        request: Any,
        handler: Callable[[Any], Awaitable[Any]],
    ) -> Any:
        """Async wrapper that intercepts Read for images/PDFs and injects as HumanMessage.

        Args:
            request: Tool call request containing tool_call dict with name, args, id
            handler: Next handler in middleware chain

        Returns:
            Command with updated messages (ToolMessage + HumanMessage with content),
            or the original result if not a visual request or on error
        """
        tool_call = request.tool_call
        tool_name = tool_call.get("name")

        # Pass through non-target tools
        if tool_name != self.TOOL_NAME:
            return await handler(request)

        tool_call_id = tool_call.get("id", "unknown")
        tool_args = tool_call.get("args", {})
        file_path = tool_args.get("file_path", "")

        # Pass through non-visual requests
        if not _is_visual_request(file_path):
            return await handler(request)

        # Injected unconditionally, without asking whether the model can view it.
        # This middleware is a single instance shared with every subagent, so the
        # only model it could ask about is the configured one — and gating on that
        # withheld the blocks from a subagent running its own vision model, which
        # awrap_model_call cannot undo because it only ever removes blocks. The
        # cost is that a text-only turn still checkpoints content it will strip on
        # every read; that is the accepted price of never silently losing a read.
        logger.debug(f"[MULTIMODAL] Intercepting Read for visual content: {file_path}")

        # Execute the tool to get the acknowledgment message
        result = await handler(request)

        # Check if the tool returned an error
        result_content = result.content if hasattr(result, "content") else str(result)
        if result_content.startswith("ERROR:"):
            return result

        # Handle URL content (images or PDFs)
        if file_path.startswith(("http://", "https://")):
            return await self._handle_url_content(file_path, result, tool_call_id)

        # Handle local sandbox files
        return await self._handle_sandbox_content(file_path, result, tool_call_id)

    def _build_content_blocks(
        self,
        b64_string: str,
        file_path: str,
        mime_type: str,
        pages: int | None = None,
    ) -> list[dict[str, Any]]:
        """Build appropriate content blocks based on file type.

        ``pages`` is stamped on the PDF block for the read-side ceiling check.
        Every provider converter drops keys it doesn't recognise, so it stays a
        local annotation and never reaches the wire.

        Args:
            b64_string: Base64-encoded file content
            file_path: Original file path (for display name)
            mime_type: MIME type of the content
            pages: PDF page count, or None for images

        Returns:
            List of content block dicts for HumanMessage
        """
        if mime_type.startswith("image/"):
            # Image: use data URI with image_url block
            data_uri = f"data:{mime_type};base64,{b64_string}"
            return [
                {"type": "text", "text": "[Viewing image]"},
                {"type": "image_url", "image_url": {"url": data_uri}},
            ]
        elif mime_type == "application/pdf":
            # PDF: use LangChain's file content block format (converts to Anthropic's document format)
            filename = Path(file_path).name
            block = {
                "type": "file",
                "base64": b64_string,
                "mime_type": mime_type,
                "filename": filename,
            }
            if pages is not None:
                block["pages"] = pages
            return [
                {"type": "text", "text": f"[Viewing PDF: {filename}]"},
                block,
            ]
        return []

    async def _handle_url_content(
        self,
        url: str,
        tool_result: Any,
        tool_call_id: str,
    ) -> Any:
        """Handle URL content (image or PDF) by downloading and injecting as base64 HumanMessage.

        Downloads the content and converts to base64 to ensure compatibility with
        all LLM providers. Many providers (like Anthropic) cannot fetch external
        URLs directly, especially from private S3 buckets.

        Args:
            url: Content URL to download
            tool_result: Original tool result (acknowledgment message)
            tool_call_id: Tool call ID for error messages

        Returns:
            Command with ToolMessage + HumanMessage, or ToolMessage with error
        """
        try:
            # Download the content. The URL is model-chosen and this fetch runs
            # in the backend process — not the sandbox — so it inherits the
            # backend's reach into the private network. GuardedAsyncTransport
            # re-checks the resolved address on every hop, which is what keeps a
            # public page from redirecting the fetch at 169.254.169.254.
            async with httpx.AsyncClient(
                transport=GuardedAsyncTransport(), timeout=30.0
            ) as client:
                async with client.stream("GET", url, follow_redirects=True) as response:
                    if response.status_code != 200:
                        logger.warning(
                            f"[MULTIMODAL] Failed to download content: {url} (status {response.status_code})"
                        )
                        return ToolMessage(
                            content=f"ERROR: Could not download content (HTTP {response.status_code}): {url}",
                            tool_call_id=tool_call_id,
                        )

                    buffer = bytearray()
                    async for chunk in response.aiter_bytes():
                        buffer.extend(chunk)
                        if len(buffer) > self.MAX_CONTENT_BYTES:
                            return self._too_large_message(url, tool_call_id)
                    content_bytes = bytes(buffer)

                if not content_bytes:
                    logger.warning(f"[MULTIMODAL] Empty content from URL: {url}")
                    return ToolMessage(
                        content=f"ERROR: Empty content from URL: {url}",
                        tool_call_id=tool_call_id,
                    )

            # Typed from the bytes, not the URL: a signed or redirected URL can
            # end without an extension, and branching on that would send a PDF
            # down the image path and reject it as corrupt.
            try:
                # Off-loop: pypdf walks the xref of up to MAX_CONTENT_BYTES of
                # model-chosen content, which is milliseconds on a real document
                # but ~100ms on one padded with junk after %%EOF.
                mime_type, pages = await asyncio.to_thread(
                    _detect_mime_type, content_bytes
                )
            except _UnusableContent as exc:
                logger.warning(f"[MULTIMODAL] Refusing content from URL ({exc}): {url}")
                return ToolMessage(
                    content=f"ERROR: Content is {exc}: {url}",
                    tool_call_id=tool_call_id,
                )

            # Encode as base64
            b64_string = base64.b64encode(content_bytes).decode("utf-8")

            # Build content blocks based on type
            content_blocks = self._build_content_blocks(
                b64_string, url, mime_type, pages
            )
            if not content_blocks:
                return ToolMessage(
                    content=f"ERROR: Unsupported content type: {mime_type}",
                    tool_call_id=tool_call_id,
                )

            human_message = HumanMessage(content=content_blocks)  # type: ignore[arg-type]

            logger.info(
                f"[MULTIMODAL] Injecting URL content as base64 HumanMessage: {url} "
                f"({len(content_bytes)} bytes, {mime_type})"
            )

            return Command(
                update={
                    "messages": [
                        tool_result,
                        human_message,
                    ]
                }
            )

        except httpx.TimeoutException:
            logger.warning(f"[MULTIMODAL] Timeout downloading content: {url}")
            return ToolMessage(
                content=f"ERROR: Timeout downloading content: {url}",
                tool_call_id=tool_call_id,
            )
        except BlockedAddressError as e:
            # The message names the resolved address ("Blocked private/reserved
            # address 10.1.2.3 for host x"). The URL is model-chosen, so echoing
            # that back turns Read into a DNS-to-IP probe steerable by anything
            # the agent reads. Keep the detail in the log, not in the transcript.
            logger.warning(f"[MULTIMODAL] Blocked address for {url}: {e}")
            return ToolMessage(
                content=f"ERROR: This address is not allowed: {url}",
                tool_call_id=tool_call_id,
            )
        except httpx.RequestError as e:
            logger.warning(f"[MULTIMODAL] Network error downloading content {url}: {e}")
            return ToolMessage(
                content=f"ERROR: Network error downloading content: {e}",
                tool_call_id=tool_call_id,
            )
        except Exception as e:
            logger.warning(f"[MULTIMODAL] Unexpected error handling URL content {url}: {e}")
            return ToolMessage(
                content=f"ERROR: Failed to load content: {e}",
                tool_call_id=tool_call_id,
            )

    async def _handle_sandbox_content(
        self,
        file_path: str,
        tool_result: Any,
        tool_call_id: str,
    ) -> Any:
        """Handle sandbox file (image or PDF) by downloading and injecting as base64 HumanMessage.

        Args:
            file_path: Sandbox path to file
            tool_result: Original tool result (acknowledgment message)
            tool_call_id: Tool call ID for error messages

        Returns:
            Command with ToolMessage + HumanMessage, or ToolMessage with error
        """
        if not self.sandbox:
            logger.warning("[MULTIMODAL] No sandbox available for local file reading")
            return ToolMessage(
                content=f"ERROR: Cannot read local file without sandbox: {file_path}",
                tool_call_id=tool_call_id,
            )

        try:
            # The Read tool reaches the sandbox through SandboxBackend, which
            # normalizes first; this middleware holds the sandbox itself, whose
            # adownload_file_bytes does not. Without this, a virtual path the
            # tool resolved fine ("/work/task/charts/c.png") misses here and replaces
            # the tool's success with a not-found error.
            file_bytes = await self.sandbox.adownload_file_bytes(
                self.sandbox.normalize_path(file_path)
            )
            if not file_bytes:
                logger.warning(f"[MULTIMODAL] Failed to download file: {file_path}")
                return ToolMessage(
                    content=f"ERROR: Could not read file: {file_path}",
                    tool_call_id=tool_call_id,
                )

            if len(file_bytes) > self.MAX_CONTENT_BYTES:
                return self._too_large_message(file_path, tool_call_id)

            # Typed from the bytes for the same reason the URL path is: the
            # extension is whatever wrote the file claimed, and a truncated
            # chart named .png is checkpointed before any provider sees it.
            try:
                mime_type, pages = await asyncio.to_thread(
                    _detect_mime_type, file_bytes
                )
            except _UnusableContent as exc:
                logger.warning(f"[MULTIMODAL] Refusing file ({exc}): {file_path}")
                return ToolMessage(
                    content=f"ERROR: File is {exc}: {file_path}",
                    tool_call_id=tool_call_id,
                )

            # Encode as base64
            b64_string = base64.b64encode(file_bytes).decode("utf-8")

            # Build content blocks based on type
            content_blocks = self._build_content_blocks(
                b64_string, file_path, mime_type, pages
            )
            if not content_blocks:
                return ToolMessage(
                    content=f"ERROR: Unsupported content type: {mime_type}",
                    tool_call_id=tool_call_id,
                )

            human_message = HumanMessage(content=content_blocks)  # type: ignore[arg-type]

            logger.info(
                f"[MULTIMODAL] Injecting sandbox file as HumanMessage: {file_path} "
                f"({len(file_bytes)} bytes, {mime_type})"
            )

            return Command(
                update={
                    "messages": [
                        tool_result,
                        human_message,
                    ]
                }
            )

        except (OSError, ValueError) as e:
            logger.warning(f"[MULTIMODAL] Error loading sandbox file {file_path}: {e}")
            return ToolMessage(
                content=f"ERROR: Failed to load file: {e}",
                tool_call_id=tool_call_id,
            )


__all__ = ["MultimodalMiddleware"]
