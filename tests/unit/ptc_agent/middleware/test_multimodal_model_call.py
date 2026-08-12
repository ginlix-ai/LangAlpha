import io
import types

import httpx
import pypdf
import pytest
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langgraph.types import Command
from PIL import Image

from ptc_agent.agent.middleware.file_operations import multimodal
from ptc_agent.agent.middleware.file_operations.multimodal import (
    MultimodalMiddleware,
    _is_visual_request,
    _strip_unsupported_content_blocks,
)
from src.llms.llm import LLM, get_input_modalities


def _system_text(request) -> str:
    """The system message as flat text, whether it is a string or block list."""
    content = request.system_message.content
    if isinstance(content, list):
        return "".join(
            b.get("text", "") for b in content if isinstance(b, dict)
        )
    return str(content)


def _pdf_bytes(pages: int = 1) -> bytes:
    """A structurally valid PDF. Generated rather than committed as a blob so the
    page count is a parameter — that is what the provider ceiling is measured in."""
    writer = pypdf.PdfWriter()
    for _ in range(pages):
        writer.add_blank_page(width=72, height=72)
    buf = io.BytesIO()
    writer.write(buf)
    return buf.getvalue()


def _png_bytes() -> bytes:
    buf = io.BytesIO()
    Image.new("RGB", (2, 2), "red").save(buf, format="PNG")
    return buf.getvalue()


class _Sandbox:
    """Stands in for PTCSandbox — which normalizes on its own, not on download."""

    def __init__(self, content: bytes = b"", *, work_dir: str = "/home/workspace"):
        self._content = content
        self._work_dir = work_dir
        self.downloaded: str | None = None

    def normalize_path(self, path: str) -> str:
        if path.startswith((self._work_dir, "/tmp")):
            return path
        return f"{self._work_dir}/{path.lstrip('/')}"

    async def adownload_file_bytes(self, path: str) -> bytes:
        self.downloaded = path
        return self._content


class TestStripUnsupportedContentBlocks:
    def test_vision_model_passes_through(self):
        """Vision model (has_image=True, has_pdf=True): messages returned unchanged."""
        msgs = [
            HumanMessage(content=[
                {"type": "text", "text": "Look at this"},
                {"type": "image_url", "image_url": {"url": "data:image/png;base64,abc"}},
            ]),
            AIMessage(content="I see an image"),
        ]
        result = _strip_unsupported_content_blocks(msgs, has_image=True, has_pdf=True)
        assert result is msgs  # exact same object, no copy

    def test_text_only_strips_image_blocks(self):
        msgs = [
            HumanMessage(content=[
                {"type": "text", "text": "Look at this"},
                {"type": "image_url", "image_url": {"url": "data:image/png;base64,abc"}},
            ]),
        ]
        result = _strip_unsupported_content_blocks(msgs, has_image=False, has_pdf=False)
        assert result is not msgs
        content = result[0].content
        assert len(content) == 2
        assert content[0] == {"type": "text", "text": "Look at this"}
        assert content[1]["type"] == "text"
        assert "not visible" in content[1]["text"]

    def test_text_only_strips_pdf_blocks(self):
        msgs = [
            HumanMessage(content=[
                {"type": "file", "base64": "abc", "mime_type": "application/pdf", "filename": "doc.pdf"},
            ]),
        ]
        result = _strip_unsupported_content_blocks(msgs, has_image=False, has_pdf=False)
        content = result[0].content
        assert content[0]["type"] == "text"
        assert "PDF" in content[0]["text"]

    def test_text_only_strips_anthropic_native_image_blocks(self):
        """Regression: an Anthropic-lineage turn leaves ``type: image`` blocks,
        not ``image_url``. Missing that shape let the most common vision→text-only
        switch replay raw blocks and 400."""
        msgs = [
            HumanMessage(content=[
                {"type": "text", "text": "Look at this"},
                {"type": "image", "source": {
                    "type": "base64", "media_type": "image/png", "data": "abc",
                }},
            ]),
        ]
        result = _strip_unsupported_content_blocks(msgs, has_image=False, has_pdf=False)
        assert result is not msgs
        content = result[0].content
        assert content[0] == {"type": "text", "text": "Look at this"}
        assert content[1]["type"] == "text"
        assert "not visible" in content[1]["text"]
        # The checkpoint keeps the original block — the strip is read-side only.
        assert msgs[0].content[1]["type"] == "image"

    def test_text_only_strips_langchain_v1_image_blocks(self):
        """The v1 shape keys the payload on `base64` instead of `source`; matching
        on the block type alone keeps either from slipping through."""
        msgs = [
            HumanMessage(content=[
                {"type": "image", "base64": "abc", "mime_type": "image/png"},
            ]),
        ]
        result = _strip_unsupported_content_blocks(msgs, has_image=False, has_pdf=False)
        assert result is not msgs
        assert result[0].content[0]["type"] == "text"

    @pytest.mark.parametrize(
        "block",
        [
            {"type": "file", "base64": "abc", "mime_type": None},
            {"type": "file", "base64": "abc"},
            {"type": "file", "base64": "abc", "mime_type": "image/png"},
        ],
        ids=["null-mime", "no-mime-key", "non-pdf-mime"],
    )
    def test_unclassifiable_file_blocks_fail_closed(self, block):
        """`pdf` is the only file-modality flag we have, so a file block we can't
        classify is one a model without it cannot accept. Matching on mime left
        these through — and on a null mime the old `.startswith` raised outright."""
        msgs = [HumanMessage(content=[block])]
        result = _strip_unsupported_content_blocks(msgs, has_image=False, has_pdf=False)
        assert result is not msgs
        assert result[0].content[0]["type"] == "text"

    def test_vision_model_keeps_anthropic_native_image_blocks(self):
        msgs = [
            HumanMessage(content=[
                {"type": "image", "source": {
                    "type": "base64", "media_type": "image/png", "data": "abc",
                }},
            ]),
        ]
        result = _strip_unsupported_content_blocks(msgs, has_image=True, has_pdf=False)
        assert result is msgs

    def test_mixed_content_preserves_text_blocks(self):
        msgs = [
            HumanMessage(content=[
                {"type": "text", "text": "Look at this chart"},
                {"type": "image_url", "image_url": {"url": "data:image/png;base64,abc"}},
            ]),
        ]
        result = _strip_unsupported_content_blocks(msgs, has_image=False, has_pdf=True)
        content = result[0].content
        assert any(b.get("text") == "Look at this chart" for b in content)

    def test_string_content_unchanged(self):
        msgs = [HumanMessage(content="hello"), AIMessage(content="hi")]
        result = _strip_unsupported_content_blocks(msgs, has_image=False, has_pdf=False)
        assert result is msgs  # no list content, no changes
        assert result[0].content == "hello"

    def test_image_supported_pdf_not(self):
        msgs = [
            HumanMessage(content=[
                {"type": "image_url", "image_url": {"url": "data:image/png;base64,abc"}},
            ]),
            HumanMessage(content=[
                {"type": "file", "base64": "xyz", "mime_type": "application/pdf", "filename": "doc.pdf"},
            ]),
        ]
        result = _strip_unsupported_content_blocks(msgs, has_image=True, has_pdf=False)
        # Image preserved
        assert result[0].content[0]["type"] == "image_url"
        # PDF stripped
        assert result[1].content[0]["type"] == "text"
        assert "PDF" in result[1].content[0]["text"]

    def test_original_messages_not_mutated(self):
        original_content = [
            {"type": "text", "text": "test"},
            {"type": "image_url", "image_url": {"url": "data:image/png;base64,abc"}},
        ]
        msgs = [HumanMessage(content=original_content.copy())]
        _strip_unsupported_content_blocks(msgs, has_image=False, has_pdf=False)
        # Original message content should be unchanged
        assert msgs[0].content[1]["type"] == "image_url"

    def test_no_visual_blocks_passes_through(self):
        """Messages with only text blocks pass through unchanged."""
        msgs = [
            HumanMessage(content=[{"type": "text", "text": "hello"}]),
            AIMessage(content="response"),
        ]
        result = _strip_unsupported_content_blocks(msgs, has_image=False, has_pdf=False)
        assert result is msgs


def _request(manifest_model):
    """A model-call request carrying only what _resolve_target reads."""
    metadata = {"manifest_model": manifest_model} if manifest_model else {}
    return types.SimpleNamespace(model=types.SimpleNamespace(metadata=metadata))


class TestResolveModalities:
    """The middleware runs inside ModelResilienceMiddleware, so the client on the
    request is the one that will actually be called — including after a fallback.
    """

    def test_reads_the_stamped_model_not_the_configured_one(self):
        mw = MultimodalMiddleware(model_name="gpt-5.5")
        # Configured for a vision model, but resilience substituted a text-only
        # client; judging on the configured name would replay image blocks at it.
        assert mw._resolve_target(_request("deepseek-v4-pro"))[1] == ["text"]

    def test_custom_modalities_apply_only_to_the_configured_model(self):
        mw = MultimodalMiddleware(model_name="my-custom-vlm", custom_modalities=["text", "image"])
        assert mw._resolve_target(_request("my-custom-vlm"))[1] == ["text", "image"]
        # A fallback is a different model — the override must not follow it over.
        assert mw._resolve_target(_request("deepseek-v4-pro"))[1] == ["text"]

    def test_an_unstamped_client_is_text_only_even_under_a_vision_parent(self):
        """Regression: a bare-string subagent resolves via ``init_chat_model`` and
        carries no stamp. Lending it the configured model's modalities let a
        vision parent replay image blocks into a text-only subagent — the exact
        400 this strip exists to prevent."""
        mw = MultimodalMiddleware(model_name="claude-sonnet-4-6")
        assert "image" in get_input_modalities("claude-sonnet-4-6")  # parent sees images
        assert mw._resolve_target(_request(None))[1] == ["text"]

    def test_a_client_with_no_metadata_at_all_is_text_only(self):
        mw = MultimodalMiddleware(model_name="claude-sonnet-4-6")
        no_metadata = types.SimpleNamespace(model=types.SimpleNamespace())
        assert mw._resolve_target(no_metadata)[1] == ["text"]

    def test_no_model_name_at_all_is_text_only(self):
        """Fail closed: over-stripping costs a placeholder, under-stripping a 400."""
        mw = MultimodalMiddleware()
        assert mw._resolve_target(_request(None))[1] == ["text"]

    def test_a_custom_modalities_override_does_not_survive_an_unstamped_client(self):
        """The override describes the configured model; an unattributable client
        is not that model."""
        mw = MultimodalMiddleware(model_name="my-custom-vlm", custom_modalities=["text", "image"])
        assert mw._resolve_target(_request(None))[1] == ["text"]


class _ModelCallRequest:
    """Minimal stand-in for the model-call request: a stamped client, a history,
    and the ``override`` the middleware must go through to replace messages."""

    def __init__(self, manifest_model, messages, system_message=None):
        self.model = types.SimpleNamespace(metadata={"manifest_model": manifest_model})
        self.messages = messages
        self.system_message = system_message

    def override(self, **kwargs):
        clone = _ModelCallRequest.__new__(_ModelCallRequest)
        clone.model = self.model
        clone.messages = kwargs.get("messages", self.messages)
        clone.system_message = kwargs.get("system_message", self.system_message)
        return clone


class TestAwrapModelCall:
    """The seam itself: capability resolution and the strip are covered above,
    but nothing exercised the method that joins them and calls ``override``."""

    @staticmethod
    def _image_history():
        return [
            HumanMessage(content=[
                {"type": "text", "text": "Look at this"},
                {"type": "image_url", "image_url": {"url": "data:image/png;base64,abc"}},
            ]),
        ]

    @pytest.mark.asyncio
    async def test_a_vision_target_is_handed_the_request_untouched(self):
        seen = {}

        async def handler(request):
            seen["request"] = request
            return "ok"

        request = _ModelCallRequest("claude-sonnet-4-6", self._image_history())
        assert await MultimodalMiddleware().awrap_model_call(request, handler) == "ok"
        assert seen["request"] is request, "vision target must not be cloned or stripped"

    @pytest.mark.asyncio
    async def test_a_text_only_target_is_handed_stripped_messages(self):
        seen = {}

        async def handler(request):
            seen["request"] = request
            return "ok"

        history = self._image_history()
        request = _ModelCallRequest("deepseek-v4-pro", history)
        await MultimodalMiddleware().awrap_model_call(request, handler)

        forwarded = seen["request"]
        assert forwarded is not request, "must forward an override, not the original"
        assert forwarded.messages[0].content[1]["type"] == "text"
        # Read-side only: the checkpoint's copy still holds the real block.
        assert history[0].content[1]["type"] == "image_url"

    @pytest.mark.asyncio
    async def test_a_text_only_target_with_nothing_to_strip_is_not_cloned(self):
        """No visual blocks means no override — the request passes through as-is."""
        seen = {}

        async def handler(request):
            seen["request"] = request
            return "ok"

        request = _ModelCallRequest("deepseek-v4-pro", [HumanMessage(content="plain text")])
        await MultimodalMiddleware().awrap_model_call(request, handler)
        assert seen["request"] is request


class TestManifestModelStampRoundTrip:
    """Producer and consumer of ``manifest_model`` live in different modules and
    agree only by string. Both sides are asserted against one real client so a
    rename on either cannot pass."""

    def test_the_key_the_producer_writes_is_the_key_the_middleware_reads(self):
        client = LLM("claude-sonnet-4-6", api_key="unused-offline").get_llm()
        assert client.metadata["manifest_model"] == "claude-sonnet-4-6"

        # Configured for a text-only model, handed a vision client: the stamp is
        # what must win, which only works if both sides name the same key.
        mw = MultimodalMiddleware(model_name="deepseek-v4-pro")
        _, modalities = mw._resolve_target(types.SimpleNamespace(model=client))
        assert "image" in modalities

    def test_the_stamp_is_the_manifest_key_not_the_provider_model_id(self):
        """``get_input_modalities`` looks up models.json keys; the API model id
        would silently resolve to text-only for every renamed model."""
        client = LLM("claude-sonnet-4-6", api_key="unused-offline").get_llm()
        stamped = client.metadata["manifest_model"]
        assert get_input_modalities(stamped) != ["text"]


class TestVisualRequestRouting:
    def test_memo_pdf_is_not_intercepted(self):
        """Read serves memo PDFs as extracted text; they have no sandbox-FS copy,
        so intercepting would swap real content for a not-found error."""
        assert not _is_visual_request(".agents/user/memo/report.pdf")
        assert not _is_visual_request("/.agents/user/memo/report.pdf")
        assert not _is_visual_request("./.agents/user/memo/report.pdf")

    def test_workspace_pdf_is_intercepted(self):
        assert _is_visual_request("results/report.pdf")

    def test_tool_name_matches_the_registered_read_tool(self):
        """A drift here silently disables the whole injection half — it fails by
        matching nothing, not by raising, which is how it went unnoticed before."""
        from ptc_agent.agent.tools.file_ops import create_filesystem_tools

        # The factory only closes over the backend; nothing touches it until a
        # tool is actually invoked, so a bare stub is enough to read the names.
        names = {t.name for t in create_filesystem_tools(types.SimpleNamespace())}
        assert MultimodalMiddleware.TOOL_NAME in names


class TestUnsupportedModalityIsJudgedReadSide:
    """Which model can see the file is decided where the model is known.

    One middleware instance is shared with every subagent, so the configured name
    is not the consuming model. Deciding at tool-call time withheld the block from
    a subagent running its own vision model — and the read side only ever removes
    blocks, so nothing downstream could recover a block never created.
    """

    @staticmethod
    def _read(file_path):
        return types.SimpleNamespace(
            tool_call={"name": "Read", "args": {"file_path": file_path}, "id": "tc-1"}
        )

    @staticmethod
    async def _handler(_request):
        return ToolMessage(content="ok", tool_call_id="tc-1")

    @pytest.mark.asyncio
    async def test_a_text_only_configured_model_still_injects(self):
        """The block has to exist for a vision subagent sharing this instance."""
        mw = MultimodalMiddleware(
            sandbox=_Sandbox(_png_bytes()), model_name="stub", custom_modalities=["text"]
        )

        result = await mw.awrap_tool_call(self._read("chart.png"), self._handler)
        assert isinstance(result, Command)
        assert result.update["messages"][-1].content[-1]["type"] == "image_url"

    @pytest.mark.parametrize(
        "file_path",
        ["chart.png", "report.pdf", "https://example.com/asset"],
        ids=["image-ext", "pdf-ext", "url-without-extension"],
    )
    @pytest.mark.asyncio
    async def test_no_verdict_is_frozen_into_the_transcript(self, file_path):
        """The extensionless URL is the interesting one: it could resolve to
        either kind, and the old gate judged all three off the configured name."""
        mw = MultimodalMiddleware(model_name="stub", custom_modalities=["text"])
        result = await mw.awrap_tool_call(self._read(file_path), self._handler)
        assert "does not support" not in str(result.content)

    @pytest.mark.asyncio
    async def test_the_note_reaches_the_model_that_actually_cannot_see(self):
        seen = {}

        async def handler(request):
            seen["request"] = request
            return "ok"

        request = _ModelCallRequest("deepseek-v4-pro", [
            HumanMessage(content=[
                {"type": "text", "text": "Look at this"},
                {"type": "image_url", "image_url": {"url": "data:image/png;base64,abc"}},
            ]),
        ])
        await MultimodalMiddleware().awrap_model_call(request, handler)

        assert multimodal._UNSUPPORTED_NOTE in _system_text(seen["request"])


class TestContentSizeCap:
    @pytest.mark.asyncio
    async def test_oversized_download_is_aborted(self, monkeypatch):
        """The body is streamed so the cap can fire mid-transfer — a Content-Length
        check alone is a header a server can simply lie about."""
        mw = MultimodalMiddleware()
        monkeypatch.setattr(mw, "MAX_CONTENT_BYTES", 1024)

        def handler(request):
            return httpx.Response(200, content=b"x" * 4096)

        monkeypatch.setattr(
            multimodal, "GuardedAsyncTransport", lambda: httpx.MockTransport(handler)
        )

        result = await mw._handle_url_content(
            "https://example.com/big.png", AIMessage(content="ok"), "call-1"
        )
        assert "larger than" in result.content
        assert result.tool_call_id == "call-1"

    @pytest.mark.asyncio
    async def test_oversized_sandbox_file_is_refused(self, monkeypatch):
        """The sandbox path was uncapped while the URL path was not. Both write
        into graph state, so an oversized block there is checkpointed and
        replayed on every later turn — a permanent 400, not a one-turn error."""
        mw = MultimodalMiddleware(sandbox=_Sandbox(b"x" * 4096))
        monkeypatch.setattr(mw, "MAX_CONTENT_BYTES", 1024)

        result = await mw._handle_sandbox_content(
            "/home/workspace/big.png", AIMessage(content="ok"), "call-1"
        )
        assert "larger than" in result.content
        assert result.tool_call_id == "call-1"

    @pytest.mark.asyncio
    async def test_a_refusal_never_reaches_graph_state(self, monkeypatch):
        """A Command would write the block into the checkpoint; refusing has to
        stay a plain ToolMessage or the cap accomplishes nothing."""
        mw = MultimodalMiddleware(sandbox=_Sandbox(b"x" * 4096))
        monkeypatch.setattr(mw, "MAX_CONTENT_BYTES", 1024)

        result = await mw._handle_sandbox_content(
            "/home/workspace/big.png", AIMessage(content="ok"), "call-1"
        )
        assert isinstance(result, ToolMessage)
        assert not isinstance(result, Command)

    @pytest.mark.asyncio
    async def test_a_blocked_address_does_not_name_the_resolved_ip(self, monkeypatch):
        """The URL is model-chosen, so echoing the guard's message back — it names
        the resolved address — turns Read into a DNS-to-IP probe steerable by
        anything the agent reads."""
        from src.tools.web.inhouse.guard import BlockedAddressError

        class _Blocking(httpx.AsyncBaseTransport):
            async def handle_async_request(self, request):
                raise BlockedAddressError(
                    "Blocked private/reserved address 10.11.12.13 for host 'x.corp'",
                    request=request,
                )

        mw = MultimodalMiddleware()
        monkeypatch.setattr(multimodal, "GuardedAsyncTransport", _Blocking)

        result = await mw._handle_url_content(
            "http://x.corp/a.png", AIMessage(content="ok"), "call-1"
        )
        assert "10.11.12.13" not in result.content
        assert "x.corp" in result.content  # the URL the model already knows
        assert "not allowed" in result.content

    def test_the_cap_matches_the_binding_provider_limit(self):
        """5MB is Anthropic's per-image ceiling, which binds before any
        per-request limit. Raising this re-opens the checkpoint brick."""
        assert MultimodalMiddleware.MAX_CONTENT_BYTES == 5 * 1024 * 1024


class TestSandboxPathNormalization:
    @pytest.mark.asyncio
    async def test_a_virtual_path_is_normalized_before_download(self):
        """The Read tool normalizes through SandboxBackend, this middleware holds
        the sandbox itself. Skipping it makes the middleware miss a file the tool
        just read and overwrite that success with a not-found error."""
        sandbox = _Sandbox(_png_bytes())
        mw = MultimodalMiddleware(sandbox=sandbox)

        result = await mw._handle_sandbox_content(
            "/results/chart.png", AIMessage(content="ok"), "call-1"
        )
        assert sandbox.downloaded == "/home/workspace/results/chart.png"
        assert isinstance(result, Command)

    @pytest.mark.asyncio
    async def test_an_already_absolute_path_is_left_alone(self):
        sandbox = _Sandbox(_png_bytes())
        mw = MultimodalMiddleware(sandbox=sandbox)

        await mw._handle_sandbox_content(
            "/home/workspace/work/t/charts/fig.png", AIMessage(content="ok"), "call-1"
        )
        assert sandbox.downloaded == "/home/workspace/work/t/charts/fig.png"


class TestContentIsTypedByItsBytes:
    """The extension is whatever wrote the file claimed, and a URL may carry none
    at all. Both injection paths checkpoint what they build, so a wrong call here
    is replayed on every later turn instead of failing once.
    """

    @pytest.mark.asyncio
    async def test_a_corrupt_sandbox_image_is_refused(self):
        """The URL path already ran PIL over its bytes; the sandbox path trusted
        the extension, so a truncated chart reached graph state unexamined."""
        mw = MultimodalMiddleware(sandbox=_Sandbox(_png_bytes()[:20]))

        result = await mw._handle_sandbox_content(
            "chart.png", AIMessage(content="ok"), "call-1"
        )
        assert isinstance(result, ToolMessage)
        assert not isinstance(result, Command)
        assert "not a readable image or PDF" in result.content

    @pytest.mark.asyncio
    async def test_a_sandbox_pdf_named_png_is_read_as_a_pdf(self):
        mw = MultimodalMiddleware(sandbox=_Sandbox(_pdf_bytes()))

        result = await mw._handle_sandbox_content(
            "report.png", AIMessage(content="ok"), "call-1"
        )
        blocks = result.update["messages"][-1].content
        assert [b["type"] for b in blocks] == ["text", "file"]
        assert blocks[-1]["mime_type"] == "application/pdf"

    @pytest.mark.asyncio
    async def test_an_extensionless_url_serving_a_pdf_is_not_judged_as_an_image(
        self, monkeypatch
    ):
        """Signed and redirected URLs routinely end without a suffix; branching on
        it sent every one of them down the image path."""
        mw = MultimodalMiddleware()
        monkeypatch.setattr(
            multimodal,
            "GuardedAsyncTransport",
            lambda: httpx.MockTransport(
                lambda request: httpx.Response(200, content=_pdf_bytes())
            ),
        )

        result = await mw._handle_url_content(
            "https://files.example.com/d/abc123", AIMessage(content="ok"), "call-1"
        )
        assert isinstance(result, Command)
        assert result.update["messages"][-1].content[-1]["mime_type"] == "application/pdf"

    @pytest.mark.asyncio
    async def test_a_format_no_provider_accepts_is_refused_not_relabeled(
        self, monkeypatch
    ):
        """The old URL path defaulted an unmapped PIL format to image/png, which
        ships a BMP under a PNG mime and earns a 400 on every replay."""
        buf = io.BytesIO()
        Image.new("RGB", (2, 2), "blue").save(buf, format="BMP")
        mw = MultimodalMiddleware()
        monkeypatch.setattr(
            multimodal,
            "GuardedAsyncTransport",
            lambda: httpx.MockTransport(
                lambda request: httpx.Response(200, content=buf.getvalue())
            ),
        )

        result = await mw._handle_url_content(
            "https://example.com/a.bmp", AIMessage(content="ok"), "call-1"
        )
        assert isinstance(result, ToolMessage)
        assert not isinstance(result, Command)
        assert "BMP" in result.content


def _batch(*after_ai):
    """A turn whose assistant message issued two parallel tool calls."""
    return [
        HumanMessage(content="look at the chart and list the files"),
        AIMessage(content="", tool_calls=[
            {"name": "Read", "args": {"file_path": "chart.png"}, "id": "toolu_A",
             "type": "tool_call"},
            {"name": "bash", "args": {"command": "ls"}, "id": "toolu_B",
             "type": "tool_call"},
        ]),
        *after_ai,
    ]


def _media():
    return HumanMessage(content=[
        {"type": "text", "text": "[Viewing image]"},
        {"type": "image_url", "image_url": {"url": "data:image/png;base64,abc"}},
    ])


class TestToolResultsPrecedeInjectedMedia:
    """Injection returns a Command carrying [ToolMessage, HumanMessage], so a
    visual Read that is not last in a parallel batch interleaves its media between
    two tool results. Anthropic requires a user turn's tool_result blocks to come
    before any other content; the raw order earns a 400, and the Command is
    checkpointed, so the 400 repeats on every replay.
    """

    @pytest.mark.asyncio
    async def test_media_between_two_results_is_moved_after_both(self):
        seen = {}

        async def handler(request):
            seen["request"] = request
            return "ok"

        history = _batch(
            ToolMessage(content="Read image", tool_call_id="toolu_A"),
            _media(),
            ToolMessage(content="file1", tool_call_id="toolu_B"),
        )
        request = _ModelCallRequest("claude-sonnet-4-6", history)
        await MultimodalMiddleware().awrap_model_call(request, handler)

        kinds = [type(m).__name__ for m in seen["request"].messages]
        assert kinds == ["HumanMessage", "AIMessage", "ToolMessage", "ToolMessage",
                         "HumanMessage"]
        # Read-side only: the checkpoint's copy keeps the order it was written in.
        assert [type(m).__name__ for m in history][2:] == ["ToolMessage", "HumanMessage",
                                                            "ToolMessage"]

    def test_the_repair_produces_a_payload_anthropic_accepts(self):
        """The contract this exists for, asserted where it is actually enforced —
        block order inside the converted user turn, not message order."""
        from langchain_anthropic.chat_models import _format_messages

        broken = _batch(
            ToolMessage(content="Read image", tool_call_id="toolu_A"),
            _media(),
            ToolMessage(content="file1", tool_call_id="toolu_B"),
        )
        _, before = _format_messages(broken)
        assert [b["type"] for b in before[-1]["content"]] == [
            "tool_result", "text", "image", "tool_result"
        ], "precondition: the raw order interleaves a tool_result after content"

        _, after = _format_messages(multimodal._tool_results_first(broken))
        kinds = [b["type"] for b in after[-1]["content"]]
        assert kinds.index("text") > max(
            i for i, k in enumerate(kinds) if k == "tool_result"
        ), "every tool_result must precede any other block"

    @pytest.mark.parametrize(
        "tail",
        [
            pytest.param(
                [ToolMessage(content="a", tool_call_id="toolu_A"),
                 ToolMessage(content="b", tool_call_id="toolu_B"), _media()],
                id="media-already-last",
            ),
            pytest.param(
                [ToolMessage(content="a", tool_call_id="toolu_A")],
                id="single-result",
            ),
            pytest.param([], id="no-results-yet"),
        ],
    )
    def test_an_already_valid_turn_is_returned_unchanged(self, tail):
        """Identity, not equality — an unnecessary copy would defeat the caller's
        `is not` check and clone every request in the process."""
        messages = _batch(*tail)
        assert multimodal._tool_results_first(messages) is messages

    def test_a_turn_with_no_tool_calls_is_untouched(self):
        messages = [
            HumanMessage(content="hi"),
            AIMessage(content="hello"),
            HumanMessage(content="thanks"),
        ]
        assert multimodal._tool_results_first(messages) is messages

    def test_relative_order_inside_each_group_is_preserved(self):
        """Two injected reads in one batch must stay in the order they ran."""
        first, second = _media(), _media()
        messages = _batch(
            ToolMessage(content="a", tool_call_id="toolu_A"),
            first,
            ToolMessage(content="b", tool_call_id="toolu_B"),
            second,
        )
        tail = multimodal._tool_results_first(messages)[2:]
        assert [m.content for m in tail[:2]] == ["a", "b"]
        assert tail[2] is first and tail[3] is second

    def test_an_earlier_healthy_batch_is_left_alone(self):
        """Only the offending run is rewritten; earlier turns keep their shape."""
        good_media = _media()
        messages = [
            *_batch(ToolMessage(content="a", tool_call_id="toolu_A"), good_media),
            AIMessage(content="", tool_calls=[
                {"name": "Read", "args": {"file_path": "x.png"}, "id": "toolu_C",
                 "type": "tool_call"},
                {"name": "bash", "args": {"command": "ls"}, "id": "toolu_D",
                 "type": "tool_call"},
            ]),
            ToolMessage(content="c", tool_call_id="toolu_C"),
            _media(),
            ToolMessage(content="d", tool_call_id="toolu_D"),
        ]
        result = multimodal._tool_results_first(messages)
        assert result[2].content == "a" and result[3] is good_media
        assert [type(m).__name__ for m in result[5:]] == [
            "ToolMessage", "ToolMessage", "HumanMessage"
        ]


class TestPDFsAreVerifiedNotSniffed:
    """A `%PDF` prefix is four bytes of claim. Everything past it — the xref, the
    page tree, the page count — is what a provider actually validates, and the
    Command that carries the block is written to the checkpoint before any
    provider sees it.
    """

    @pytest.mark.asyncio
    async def test_a_header_only_pdf_never_reaches_graph_state(self):
        """The bytes that pass a prefix check and nothing else."""
        mw = MultimodalMiddleware(sandbox=_Sandbox(b"%PDF-1.4 body"))

        result = await mw._handle_sandbox_content(
            "report.pdf", AIMessage(content="ok"), "call-1"
        )
        assert isinstance(result, ToolMessage)
        assert not isinstance(result, Command)
        assert "not a readable PDF" in result.content

    @pytest.mark.asyncio
    async def test_a_truncated_pdf_is_refused(self):
        """The realistic shape: an interrupted download or a half-written report,
        whose head is a genuine PDF."""
        whole = _pdf_bytes()
        mw = MultimodalMiddleware(sandbox=_Sandbox(whole[: len(whole) * 6 // 10]))

        result = await mw._handle_sandbox_content(
            "report.pdf", AIMessage(content="ok"), "call-1"
        )
        assert isinstance(result, ToolMessage)
        assert "not a readable PDF" in result.content

    @pytest.mark.asyncio
    async def test_a_pdf_over_the_page_ceiling_is_refused_with_its_count(
        self, monkeypatch
    ):
        """Page count is the ceiling that binds: this fixture is a few KB, so no
        byte cap would catch it. The message names the count so the agent can
        split the document rather than retry the same read. The cap is patched
        down because what is under test is that it is enforced, not its value."""
        monkeypatch.setattr(multimodal, "MAX_PDF_PAGES", 3)
        mw = MultimodalMiddleware(sandbox=_Sandbox(_pdf_bytes(4)))

        result = await mw._handle_sandbox_content(
            "long.pdf", AIMessage(content="ok"), "call-1"
        )
        assert isinstance(result, ToolMessage)
        assert not isinstance(result, Command)
        assert "4-page" in result.content

    @pytest.mark.asyncio
    async def test_a_pdf_at_the_page_ceiling_is_accepted(self, monkeypatch):
        """The limit is inclusive — off-by-one here silently rejects a legal doc."""
        monkeypatch.setattr(multimodal, "MAX_PDF_PAGES", 3)
        mw = MultimodalMiddleware(sandbox=_Sandbox(_pdf_bytes(3)))

        result = await mw._handle_sandbox_content(
            "long.pdf", AIMessage(content="ok"), "call-1"
        )
        assert isinstance(result, Command)

    @pytest.mark.asyncio
    async def test_the_injection_cap_is_the_widest_ceiling_not_the_tightest(self):
        """The case that motivated splitting the cap in two: a 150-page filing is
        legal on a 1M-context route, and injection cannot know it isn't headed
        there, so refusing it at tool time would be a guess against the user."""
        mw = MultimodalMiddleware(sandbox=_Sandbox(_pdf_bytes(150)))

        result = await mw._handle_sandbox_content(
            "filing.pdf", AIMessage(content="ok"), "call-1"
        )
        assert isinstance(result, Command)
        assert result.update["messages"][-1].content[-1]["pages"] == 150

    @pytest.mark.asyncio
    async def test_trailing_bytes_do_not_condemn_an_otherwise_readable_pdf(self):
        """Verification has to reject damage, not tidiness. Real PDFs routinely
        carry junk after the final %%EOF, and providers accept them."""
        mw = MultimodalMiddleware(sandbox=_Sandbox(_pdf_bytes() + b"\n<!-- appended -->"))

        result = await mw._handle_sandbox_content(
            "report.pdf", AIMessage(content="ok"), "call-1"
        )
        assert isinstance(result, Command)
        assert result.update["messages"][-1].content[-1]["mime_type"] == "application/pdf"


def _pdf_block(pages):
    return HumanMessage(content=[
        {"type": "text", "text": "[Viewing PDF: filing.pdf]"},
        {"type": "file", "base64": "abc", "mime_type": "application/pdf",
         "filename": "filing.pdf", "pages": pages},
    ])


async def _blocks_reaching(model, message):
    """The content blocks the middleware actually hands the target."""
    seen = {}

    async def handler(request):
        seen["request"] = request
        return "ok"

    await MultimodalMiddleware().awrap_model_call(
        _ModelCallRequest(model, [message]), handler
    )
    return seen["request"].messages[0].content, seen["request"]


class TestPDFPageCeilingIsPerTarget:
    """The page ceiling belongs to the model that reads the block, not to the
    tool call that made it: Anthropic publishes 600 at a 1M context and 100
    below it, so one global cap is wrong for someone whichever value it takes."""

    @pytest.mark.asyncio
    async def test_a_long_pdf_survives_to_a_1m_context_route(self):
        blocks, _ = await _blocks_reaching("claude-sonnet-5", _pdf_block(300))
        assert [b["type"] for b in blocks] == ["text", "file"]

    @pytest.mark.asyncio
    async def test_the_same_pdf_is_stripped_for_a_200k_route(self):
        blocks, request = await _blocks_reaching("claude-sonnet-4-6", _pdf_block(300))
        assert [b["type"] for b in blocks] == ["text", "text"]
        assert "300 pages" in blocks[1]["text"]
        assert multimodal._UNSUPPORTED_NOTE in _system_text(request)

    @pytest.mark.asyncio
    async def test_a_pdf_inside_the_200k_ceiling_still_reaches_it(self):
        blocks, _ = await _blocks_reaching("claude-sonnet-4-6", _pdf_block(80))
        assert [b["type"] for b in blocks] == ["text", "file"]

    @pytest.mark.asyncio
    async def test_a_provider_documenting_no_page_limit_keeps_the_block(self):
        blocks, _ = await _blocks_reaching("gpt-5.5", _pdf_block(900))
        assert [b["type"] for b in blocks] == ["text", "file"]

    @pytest.mark.asyncio
    async def test_an_unstamped_block_is_left_alone(self):
        """Blocks written before the stamp existed. Re-deriving the count would
        mean decoding every PDF in history per call; leaving them keeps the old
        behaviour rather than regressing threads that already work."""
        legacy = HumanMessage(content=[
            {"type": "text", "text": "[Viewing PDF: old.pdf]"},
            {"type": "file", "base64": "abc", "mime_type": "application/pdf",
             "filename": "old.pdf"},
        ])
        blocks, _ = await _blocks_reaching("claude-sonnet-4-6", legacy)
        assert [b["type"] for b in blocks] == ["text", "file"]


class TestPageStampStaysLocal:
    def test_the_stamp_never_reaches_the_wire(self):
        """The count rides on the block so the read side can judge it. That is
        only safe because every provider converter drops keys it doesn't know —
        if one passed it through, it would be an unknown field in the payload."""
        from langchain_anthropic.chat_models import _format_messages

        _, payload = _format_messages([_pdf_block(300)])
        document = [b for b in payload[0]["content"] if b["type"] == "document"][0]
        assert "pages" not in document
        assert "pages" not in document["source"]
