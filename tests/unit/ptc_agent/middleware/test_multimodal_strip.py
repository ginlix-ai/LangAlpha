"""The read side: what the model about to be called can still see.

Its counterpart, the Read interception that puts an attachment into history in
the first place, is covered in ``test_multimodal_read``.
"""

import base64
import io
import types

import pypdf
import pytest
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage
from PIL import Image

from ptc_agent.agent.middleware._message_utils import order_tool_results_first
from ptc_agent.agent.middleware.file_operations.multimodal_strip import (
    _IF_NOT,
    PLACEHOLDER_GUIDANCE,
    MultimodalStripMiddleware,
    placeholder_guidance,
    remove_attachments,
    strip_unsupported_content_blocks,
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


def _read_result(call_id: str, path: str, block: dict | None = None) -> list:
    """A Read call and its result carrying an attachment."""
    block = block or {
        "type": "file", "base64": "JVBERi0=", "mime_type": "application/pdf",
        "filename": "x.pdf", "pages": 3,
    }
    return [
        AIMessage(content="", tool_calls=[
            {"name": "Read", "args": {"file_path": path}, "id": call_id}
        ]),
        ToolMessage(
            content=[{"type": "text", "text": f"Loading: {path}"}, block],
            tool_call_id=call_id,
            id=f"t-{call_id}",
        ),
    ]


class TestRemoveAttachments:
    """What stands in for an attachment taken out to fit the context window."""

    def test_a_pdf_read_names_its_path_and_pages(self):
        msgs = _read_result("c1", "docs/a.pdf")
        (_, result) = remove_attachments(msgs, {"t-c1"})
        assert result.content[1]["text"] == (
            "[docs/a.pdf (3 pages) was removed from the conversation to fit the context "
            "window. Read it again by page range if you need it.]"
        )
        assert result.content[0] == msgs[1].content[0]

    def test_a_one_page_pdf_is_one_page(self):
        msgs = _read_result("c1", "docs/a.pdf")
        (_, result) = remove_attachments(msgs, {"t-c1"}, pages=lambda block: 1)
        assert result.content[1]["text"].startswith("[docs/a.pdf (1 page) was removed")

    def test_an_image_read_names_its_path(self):
        image = {"type": "image_url", "image_url": {"url": "data:image/png;base64,abc"}}
        (_, result) = remove_attachments(_read_result("c1", "charts/c.png", image), {"t-c1"})
        assert result.content[1]["text"] == (
            "[Image charts/c.png was removed from the conversation to fit the context window.]"
        )

    def test_an_upload_names_what_it_carries(self):
        upload = HumanMessage(id="h1", content=[
            {"type": "text", "text": "[Attached PDF: q3.pdf (saved to work/uploads/q3.pdf)]"},
            {"type": "file", "base64": "JVBERi0=", "mime_type": "application/pdf",
             "filename": "q3.pdf"},
            {"type": "image_url", "image_url": {"url": "data:image/png;base64,abc"}},
        ])
        (result,) = remove_attachments([upload], {"h1"}, pages=lambda block: None)
        assert [b["text"] for b in result.content[1:]] == [
            "[q3.pdf was removed from the conversation to fit the context window. "
            "Read it again by page range if you need it.]",
            "[Image was removed from the conversation to fit the context window.]",
        ]

    def test_an_unsaved_upload_asks_for_it_again_instead_of_a_read(self):
        upload = HumanMessage(id="h1", content=[
            {"type": "file", "base64": "JVBERi0=", "mime_type": "application/pdf",
             "filename": "q3.pdf", "unsaved": True},
        ])
        (result,) = remove_attachments([upload], {"h1"}, pages=lambda block: 12)
        assert result.content == [{"type": "text", "text": (
            "[q3.pdf (12 pages) was removed from the conversation to fit the context "
            "window, and no copy was saved. Ask the user to attach it again if you need it.]"
        )}]

    def test_an_image_uploaded_with_no_saved_copy_asks_for_it_again(self):
        upload = HumanMessage(id="h1", content=[
            {"type": "text", "text": "[Attached image: chart.png]"},
            {"type": "image_url", "image_url": {"url": "data:image/png;base64,abc"}},
            {"type": "text", "text": "[Attached image: kept.png (saved to work/uploads/kept.png)]"},
            {"type": "image_url", "image_url": {"url": "data:image/png;base64,def"}},
        ])
        (result,) = remove_attachments([upload], {"h1"})
        assert [b["text"] for b in result.content] == [
            "[Attached image: chart.png]",
            "[Image chart.png was removed from the conversation to fit the context window, "
            "and no copy was saved. Ask the user to attach it again if you need it.]",
            "[Attached image: kept.png (saved to work/uploads/kept.png)]",
            "[Image was removed from the conversation to fit the context window.]",
        ]

    @pytest.mark.parametrize("path", [None, "work/uploads/chart.png"], ids=["unsaved", "saved"])
    def test_the_label_the_server_writes_is_the_one_read_back(self, path):
        from src.server.models.additional_context import MultimodalContext
        from src.server.utils.multimodal_context import inject_multimodal_context

        image = MultimodalContext(
            type="image", data="data:image/png;base64,abc", description="chart.png"
        )
        (written,) = inject_multimodal_context(
            [{"role": "user", "content": "what is this?"}], [image], file_paths=[path]
        )
        upload = HumanMessage(id="h1", content=written["content"])
        (result,) = remove_attachments([upload], {"h1"})
        asked = "Ask the user to attach it again" in result.content[1]["text"]
        assert asked is (path is None)

    def test_messages_not_named_are_the_same_objects(self):
        msgs = _read_result("c1", "docs/a.pdf")
        assert remove_attachments(msgs, {"other"}) is msgs
        assert remove_attachments(msgs, set()) is msgs


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
        result = strip_unsupported_content_blocks(msgs, has_image=True, has_pdf=True)
        assert result is msgs  # exact same object, no copy

    def test_the_guidance_rides_on_the_first_placeholder_only(self):
        msgs = [
            HumanMessage(content=[
                {"type": "text", "text": "first"},
                {"type": "image_url", "image_url": {"url": "data:image/png;base64,abc"}},
            ]),
            HumanMessage(content=[
                {"type": "text", "text": "second"},
                {"type": "image_url", "image_url": {"url": "data:image/png;base64,def"}},
                {"type": "image_url", "image_url": {"url": "data:image/png;base64,ghi"}},
            ]),
        ]
        result = strip_unsupported_content_blocks(
            msgs, has_image=False, has_pdf=False, guidance="Ask for a description."
        )
        placeholders = [
            b["text"] for m in result for b in m.content if "not visible" in b["text"]
        ]
        assert len(placeholders) == 3
        assert "Ask for a description." in placeholders[0]
        assert all("Ask for a description" not in p for p in placeholders[1:])

    def test_a_url_read_is_not_said_to_be_in_the_workspace(self):
        # The server fetched it; nothing wrote it into the workspace.
        url = "https://example.test/report.pdf"
        msgs = [
            *_read_result("c1", "docs/a.pdf"),
            *_read_result("c2", url),
            *_read_result("c3", "docs/b.pdf"),
        ]
        result = strip_unsupported_content_blocks(
            msgs, has_image=True, has_pdf=False, guidance=placeholder_guidance
        )
        texts = [m.content[1]["text"] for m in result if isinstance(m, ToolMessage)]

        assert texts[0].endswith(f" {PLACEHOLDER_GUIDANCE}]")
        assert texts[1] == (
            "[PDF attached in a prior turn, not visible to the current model. "
            f"The file is at {url}: download it in the sandbox and extract it yourself "
            "with Python (a PDF's text or page renders, OCR or image analysis) before "
            "giving up. If that cannot answer the question, tell the user: an unsupported "
            "input type means switching to a model that accepts it, a page-limit rejection "
            "means splitting the document or a longer-context model. Answer in best effort "
            "otherwise.]"
        )
        # Each guidance is said once, on the first block it applies to.
        assert texts[2] == "[PDF attached in a prior turn, not visible to the current model.]"

    def test_text_only_strips_image_blocks(self):
        msgs = [
            HumanMessage(content=[
                {"type": "text", "text": "Look at this"},
                {"type": "image_url", "image_url": {"url": "data:image/png;base64,abc"}},
            ]),
        ]
        result = strip_unsupported_content_blocks(msgs, has_image=False, has_pdf=False)
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
        result = strip_unsupported_content_blocks(msgs, has_image=False, has_pdf=False)
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
        result = strip_unsupported_content_blocks(msgs, has_image=False, has_pdf=False)
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
        result = strip_unsupported_content_blocks(msgs, has_image=False, has_pdf=False)
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
        result = strip_unsupported_content_blocks(msgs, has_image=False, has_pdf=False)
        assert result is not msgs
        assert result[0].content[0]["type"] == "text"

    def test_text_only_strips_anthropic_native_document_blocks(self):
        """``document`` is what Anthropic calls a PDF block, so one echoed back
        by a provider arrives under that type rather than the ``file`` we write."""
        msgs = [
            HumanMessage(content=[
                {"type": "document", "source": {
                    "type": "base64", "media_type": "application/pdf", "data": "abc",
                }},
            ]),
        ]
        result = strip_unsupported_content_blocks(msgs, has_image=False, has_pdf=False)
        assert result is not msgs
        assert result[0].content[0]["type"] == "text"
        assert "PDF" in result[0].content[0]["text"]

    def test_a_document_block_honors_the_page_ceiling(self):
        msgs = [HumanMessage(content=[{"type": "document", "base64": "abc", "pages": 900}])]
        result = strip_unsupported_content_blocks(
            msgs, has_image=False, has_pdf=True, max_pdf_pages=100
        )
        assert result is not msgs
        assert "900 pages" in result[0].content[0]["text"]

    def test_a_pdf_model_keeps_document_blocks(self):
        msgs = [HumanMessage(content=[{"type": "document", "base64": "abc"}])]
        assert strip_unsupported_content_blocks(msgs, has_image=False, has_pdf=True) is msgs

    def test_vision_model_keeps_anthropic_native_image_blocks(self):
        msgs = [
            HumanMessage(content=[
                {"type": "image", "source": {
                    "type": "base64", "media_type": "image/png", "data": "abc",
                }},
            ]),
        ]
        result = strip_unsupported_content_blocks(msgs, has_image=True, has_pdf=False)
        assert result is msgs

    def test_mixed_content_preserves_text_blocks(self):
        msgs = [
            HumanMessage(content=[
                {"type": "text", "text": "Look at this chart"},
                {"type": "image_url", "image_url": {"url": "data:image/png;base64,abc"}},
            ]),
        ]
        result = strip_unsupported_content_blocks(msgs, has_image=False, has_pdf=True)
        content = result[0].content
        assert any(b.get("text") == "Look at this chart" for b in content)

    def test_string_content_unchanged(self):
        msgs = [HumanMessage(content="hello"), AIMessage(content="hi")]
        result = strip_unsupported_content_blocks(msgs, has_image=False, has_pdf=False)
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
        result = strip_unsupported_content_blocks(msgs, has_image=True, has_pdf=False)
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
        strip_unsupported_content_blocks(msgs, has_image=False, has_pdf=False)
        # Original message content should be unchanged
        assert msgs[0].content[1]["type"] == "image_url"

    def test_no_visual_blocks_passes_through(self):
        """Messages with only text blocks pass through unchanged."""
        msgs = [
            HumanMessage(content=[{"type": "text", "text": "hello"}]),
            AIMessage(content="response"),
        ]
        result = strip_unsupported_content_blocks(msgs, has_image=False, has_pdf=False)
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
        mw = MultimodalStripMiddleware(model_name="gpt-6.1-sol")
        # Configured for a vision model, but resilience substituted a text-only
        # client; judging on the configured name would replay image blocks at it.
        assert mw._resolve_target(_request("glm-5.2"))[1] == ["text"]

    def test_custom_modalities_apply_only_to_the_configured_model(self):
        mw = MultimodalStripMiddleware(model_name="my-custom-vlm", custom_modalities=["text", "image"])
        assert mw._resolve_target(_request("my-custom-vlm"))[1] == ["text", "image"]
        # A fallback is a different model — the override must not follow it over.
        assert mw._resolve_target(_request("glm-5.2"))[1] == ["text"]

    def test_an_unstamped_client_is_text_only_even_under_a_vision_parent(self):
        """Regression: a bare-string subagent resolves via ``init_chat_model`` and
        carries no stamp. Lending it the configured model's modalities let a
        vision parent replay image blocks into a text-only subagent — the exact
        400 this strip exists to prevent."""
        mw = MultimodalStripMiddleware(model_name="claude-sonnet-5-5")
        assert "image" in get_input_modalities("claude-sonnet-5-5")  # parent sees images
        assert mw._resolve_target(_request(None))[1] == ["text"]

    def test_a_client_with_no_metadata_at_all_is_text_only(self):
        mw = MultimodalStripMiddleware(model_name="claude-sonnet-5-5")
        no_metadata = types.SimpleNamespace(model=types.SimpleNamespace())
        assert mw._resolve_target(no_metadata)[1] == ["text"]

    def test_no_model_name_at_all_is_text_only(self):
        """Fail closed: over-stripping costs a placeholder, under-stripping a 400."""
        mw = MultimodalStripMiddleware()
        assert mw._resolve_target(_request(None))[1] == ["text"]

    def test_a_custom_modalities_override_does_not_survive_an_unstamped_client(self):
        """The override describes the configured model; an unattributable client
        is not that model."""
        mw = MultimodalStripMiddleware(model_name="my-custom-vlm", custom_modalities=["text", "image"])
        assert mw._resolve_target(_request(None))[1] == ["text"]


class TestReceives:
    """What compaction counts toward its trigger: the blocks this strip
    would send a model as they are, nothing it turns into a placeholder."""

    PDF = {"type": "file", "base64": "JVBERi0=", "mime_type": "application/pdf", "pages": 20}
    IMAGE = {"type": "image", "base64": "iVBORw0KGgo=", "mime_type": "image/png"}

    def test_a_text_only_model_is_sent_neither(self):
        receives = MultimodalStripMiddleware().receives(_request("glm-5.2").model)
        assert not receives(self.PDF) and not receives(self.IMAGE)
        assert receives({"type": "text", "text": "x"})

    def test_a_pdf_model_is_sent_one_within_its_page_ceiling(self):
        receives = MultimodalStripMiddleware().receives(_request("glm-5.3-flash-cn").model)
        assert receives(self.PDF) and receives(self.IMAGE)
        assert not receives({**self.PDF, "pages": 142})  # over its 100 pages

    def test_an_override_applies_to_the_configured_model_only(self):
        mw = MultimodalStripMiddleware(model_name="glm-5.2", custom_modalities=["text", "image"])
        assert mw.receives(_request("glm-5.2").model)(self.IMAGE)
        assert not mw.receives(_request(None).model)(self.IMAGE)


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
        # A real image: a vision target now also has each image fitted to its
        # pixel limit, and bytes that do not decode are replaced.
        png = io.BytesIO()
        Image.new("RGB", (4, 4), "white").save(png, "PNG")
        url = "data:image/png;base64," + base64.b64encode(png.getvalue()).decode()
        return [
            HumanMessage(content=[
                {"type": "text", "text": "Look at this"},
                {"type": "image_url", "image_url": {"url": url}},
            ]),
        ]

    @pytest.mark.asyncio
    async def test_a_vision_target_is_handed_the_request_untouched(self):
        seen = {}

        async def handler(request):
            seen["request"] = request
            return "ok"

        request = _ModelCallRequest("claude-sonnet-5-5", self._image_history())
        response = await MultimodalStripMiddleware().awrap_model_call(request, handler)
        assert getattr(response, "model_response", response) == "ok"
        assert seen["request"] is request, "vision target must not be cloned or stripped"

    @pytest.mark.asyncio
    async def test_a_text_only_target_is_handed_stripped_messages(self):
        seen = {}

        async def handler(request):
            seen["request"] = request
            return "ok"

        history = self._image_history()
        request = _ModelCallRequest("glm-5.2", history)
        await MultimodalStripMiddleware().awrap_model_call(request, handler)

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

        request = _ModelCallRequest("glm-5.2", [HumanMessage(content="plain text")])
        await MultimodalStripMiddleware().awrap_model_call(request, handler)
        assert seen["request"] is request


class TestManifestModelStampRoundTrip:
    """Producer and consumer of ``manifest_model`` live in different modules and
    agree only by string. Both sides are asserted against one real client so a
    rename on either cannot pass."""

    def test_the_key_the_producer_writes_is_the_key_the_middleware_reads(self):
        client = LLM("claude-sonnet-5-5", api_key="unused-offline").get_llm()
        assert client.metadata["manifest_model"] == "claude-sonnet-5-5"

        # Configured for a text-only model, handed a vision client: the stamp is
        # what must win, which only works if both sides name the same key.
        mw = MultimodalStripMiddleware(model_name="glm-5.2")
        _, modalities = mw._resolve_target(types.SimpleNamespace(model=client))
        assert "image" in modalities

    def test_the_stamp_is_the_manifest_key_not_the_provider_model_id(self):
        """``get_input_modalities`` looks up models.json keys; the API model id
        would silently resolve to text-only for every renamed model."""
        client = LLM("claude-sonnet-5-5", api_key="unused-offline").get_llm()
        stamped = client.metadata["manifest_model"]
        assert get_input_modalities(stamped) != ["text"]


class TestTheGuidanceRidesInThePlaceholder:
    """Decided on the call where the strip fired, so it lands on the model that
    actually cannot see the file rather than being frozen into the transcript
    at tool time under whatever model was configured then. It rides inside the
    placeholder, next to the block it explains, and the system prompt is left
    untouched so the cached prefix does not move on that call."""

    def _request(self):
        return _ModelCallRequest("glm-5.2", [
            HumanMessage(content=[
                {"type": "text", "text": "Look at this"},
                {"type": "image_url", "image_url": {"url": "data:image/png;base64,abc"}},
            ]),
        ], system_message=SystemMessage(content=[{"type": "text", "text": "prefix"}]))

    @pytest.mark.asyncio
    async def test_the_guidance_reaches_the_model_that_actually_cannot_see(self):
        seen = {}

        async def handler(request):
            seen["request"] = request
            return "ok"

        await MultimodalStripMiddleware().awrap_model_call(self._request(), handler)

        placeholder = seen["request"].messages[0].content[1]["text"]
        assert placeholder.startswith("[Image attached in a prior turn, not visible to the current model.")
        assert PLACEHOLDER_GUIDANCE in placeholder
        assert "extract it yourself" in placeholder
        assert _system_text(seen["request"]) == "prefix"


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
    """The read-side repair for histories written before the move onto the result.

    Injection used to return a Command carrying [ToolMessage, HumanMessage], so a
    visual Read that was not last in a parallel batch interleaved its media
    between two tool results. Anthropic requires a user turn's tool_result blocks
    to come before any other content; the raw order earns a 400, and those
    messages are checkpointed, so the 400 repeats on every replay of an existing
    thread. New turns cannot produce the shape.
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
        request = _ModelCallRequest("claude-sonnet-5-5", history)
        await MultimodalStripMiddleware().awrap_model_call(request, handler)

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
        _, before = _format_messages(broken, model="claude-sonnet-4-6")
        assert [b["type"] for b in before[-1]["content"]] == [
            "tool_result", "text", "image", "tool_result"
        ], "precondition: the raw order interleaves a tool_result after content"

        _, after = _format_messages(order_tool_results_first(broken), model="claude-sonnet-4-6")
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
        assert order_tool_results_first(messages) is messages

    def test_a_turn_with_no_tool_calls_is_untouched(self):
        messages = [
            HumanMessage(content="hi"),
            AIMessage(content="hello"),
            HumanMessage(content="thanks"),
        ]
        assert order_tool_results_first(messages) is messages

    def test_relative_order_inside_each_group_is_preserved(self):
        """Two injected reads in one batch must stay in the order they ran."""
        first, second = _media(), _media()
        messages = _batch(
            ToolMessage(content="a", tool_call_id="toolu_A"),
            first,
            ToolMessage(content="b", tool_call_id="toolu_B"),
            second,
        )
        tail = order_tool_results_first(messages)[2:]
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
        result = order_tool_results_first(messages)
        assert result[2].content == "a" and result[3] is good_media
        assert [type(m).__name__ for m in result[5:]] == [
            "ToolMessage", "ToolMessage", "HumanMessage"
        ]


def _pdf_block(pages):
    return HumanMessage(content=[
        {"type": "text", "text": "[Viewing PDF: filing.pdf]"},
        {"type": "file", "base64": "abc", "mime_type": "application/pdf",
         "filename": "filing.pdf", "pages": pages},
    ])


#: Default instance: the note it carries is the one ``_blocks_reaching`` produces.
_STRIP = MultimodalStripMiddleware()


async def _blocks_reaching(model, message):
    """The content blocks the middleware actually hands the target."""
    seen = {}

    async def handler(request):
        seen["request"] = request
        return "ok"

    await _STRIP.awrap_model_call(_ModelCallRequest(model, [message]), handler)
    return seen["request"].messages[0].content, seen["request"]


class TestPDFPageCeilingIsPerTarget:
    """The page ceiling belongs to the model that reads the block, not to the
    tool call that made it: Anthropic publishes 600 at a 1M context and 100
    below it, so one global cap is wrong for someone whichever value it takes."""

    @pytest.mark.asyncio
    async def test_a_long_pdf_survives_to_a_1m_context_route(self):
        blocks, _ = await _blocks_reaching("claude-sonnet-5-5", _pdf_block(300))
        assert [b["type"] for b in blocks] == ["text", "file"]

    @pytest.fixture
    def route_200k(self, monkeypatch):
        """No shipped Anthropic model sits below a 1M context any more. A model
        missing from the manifest would also be held to 100 pages, but it would
        have no PDF modality either, so the 80-page case below only passes when
        this entry is the one being read."""
        monkeypatch.setitem(
            LLM.get_model_config().llm_config,
            "_route_200k",
            {
                "model_id": "_route_200k",
                "provider": "anthropic",
                "context": 200_000,
                "input_modalities": ["text", "image", "pdf"],
            },
        )
        return "_route_200k"

    @pytest.mark.asyncio
    async def test_the_same_pdf_is_stripped_for_a_200k_route(self, route_200k):
        blocks, request = await _blocks_reaching(route_200k, _pdf_block(300))
        assert [b["type"] for b in blocks] == ["text", "text"]
        assert "300 pages" in blocks[1]["text"]
        assert PLACEHOLDER_GUIDANCE in blocks[1]["text"]

    @pytest.mark.asyncio
    async def test_an_unsaved_upload_over_it_is_not_pointed_at_a_copy(self, route_200k):
        """A composer PDF inlined because nothing could save it: no workspace
        copy exists, so only what to tell the user follows."""
        upload = HumanMessage(content=[
            {"type": "text", "text": "[Attached PDF: filing.pdf]"},
            {"type": "file", "base64": "abc", "mime_type": "application/pdf",
             "filename": "filing.pdf", "pages": 300, "unsaved": True},
        ])
        blocks, _ = await _blocks_reaching(route_200k, upload)
        assert blocks[1]["text"] == (
            "[PDF filing.pdf (300 pages) is over the current model's 100-page limit "
            f"and was not sent. {_IF_NOT}]"
        )

    @pytest.mark.asyncio
    async def test_a_pdf_inside_the_200k_ceiling_still_reaches_it(self, route_200k):
        blocks, _ = await _blocks_reaching(route_200k, _pdf_block(80))
        assert [b["type"] for b in blocks] == ["text", "file"]

    @pytest.mark.asyncio
    async def test_a_provider_documenting_no_page_limit_keeps_the_block(self):
        blocks, _ = await _blocks_reaching("gpt-6.1-sol", _pdf_block(900))
        assert [b["type"] for b in blocks] == ["text", "file"]

    @pytest.mark.asyncio
    async def test_an_unstamped_block_is_left_alone(self, route_200k):
        """Blocks written before the stamp existed. Re-deriving the count would
        mean decoding every PDF in history per call; leaving them keeps the old
        behaviour rather than regressing threads that already work."""
        legacy = HumanMessage(content=[
            {"type": "text", "text": "[Viewing PDF: old.pdf]"},
            {"type": "file", "base64": "abc", "mime_type": "application/pdf",
             "filename": "old.pdf"},
        ])
        blocks, _ = await _blocks_reaching(route_200k, legacy)
        assert [b["type"] for b in blocks] == ["text", "file"]


class TestPageStampStaysLocal:
    def test_the_stamp_never_reaches_the_wire(self):
        """The count rides on the block so the read side can judge it. That is
        only safe because every provider converter drops keys it doesn't know —
        if one passed it through, it would be an unknown field in the payload."""
        from langchain_anthropic.chat_models import _format_messages

        _, payload = _format_messages([_pdf_block(300)], model="claude-sonnet-4-6")
        document = [b for b in payload[0]["content"] if b["type"] == "document"][0]
        assert "pages" not in document
        assert "pages" not in document["source"]

    def test_the_unsaved_mark_never_reaches_the_wire(self):
        from langchain_anthropic.chat_models import _format_messages

        message = _pdf_block(3)
        message.content[1]["unsaved"] = True
        _, payload = _format_messages([message], model="claude-sonnet-4-6")
        document = [b for b in payload[0]["content"] if b["type"] == "document"][0]
        assert "unsaved" not in document and "unsaved" not in document["source"]


class TestImageSideRule:
    """A thread keeps the side its images were first sent at: one whose first
    call already holds thinking was sent them by count, and fitting them to
    the fixed 2000px side would invalidate that thinking."""

    @staticmethod
    def _history(*, thinking: bool) -> list:
        png = io.BytesIO()
        Image.new("RGB", (2400, 1200), "white").save(png, "PNG")
        url = "data:image/png;base64," + base64.b64encode(png.getvalue()).decode()
        answer = [{"type": "thinking", "thinking": "a chart", "signature": "sig"}] if thinking else []
        return [
            HumanMessage(content=[
                {"type": "text", "text": "Look at this"},
                {"type": "image_url", "image_url": {"url": url}},
            ]),
            AIMessage(
                content=[*answer, {"type": "text", "text": "A chart."}],
                response_metadata={"model_name": "claude-sonnet-5-5"},
            ),
            HumanMessage(content="And now?"),
        ]

    @staticmethod
    async def _call(history: list, state: dict) -> tuple[int, object]:
        seen = {}

        async def handler(request):
            seen["request"] = request
            return "ok"

        request = _ModelCallRequest("claude-sonnet-5-5", history)
        request.state = state
        response = await MultimodalStripMiddleware().awrap_model_call(request, handler)
        image = next(
            block
            for block in seen["request"].messages[0].content
            if isinstance(block, dict) and block.get("type") != "text"
        )
        url = image.get("image_url", {}).get("url") or image["source"]["data"]
        data = base64.b64decode(url.split(",", 1)[-1])
        return Image.open(io.BytesIO(data)).width, response

    @pytest.mark.asyncio
    async def test_a_thread_from_before_the_fixed_side_keeps_its_images_by_count(self):
        width, response = await self._call(self._history(thinking=True), {})

        assert width == 2400
        assert response.command.update == {"image_side_rule": "count"}
        assert await self._call(self._history(thinking=True), {"image_side_rule": "count"}) == (
            2400,
            "ok",
        )

    @pytest.mark.asyncio
    async def test_a_thread_with_thinking_and_no_image_behind_it_takes_the_fixed_side(self):
        """Only an image sent ahead of thinking ties the thread to the count:
        one arriving after it fits to the fixed side from its first call, so a
        later image count never refits it."""
        history = self._history(thinking=True)
        history = [HumanMessage(content="Hi"), history[1], history[0]]

        seen = {}

        async def handler(request):
            seen["request"] = request
            return "ok"

        request = _ModelCallRequest("claude-sonnet-5-5", history)
        request.state = {}
        response = await MultimodalStripMiddleware().awrap_model_call(request, handler)

        assert response.command.update == {"image_side_rule": "fixed"}

    @pytest.mark.asyncio
    async def test_a_new_thread_fits_its_images_to_the_fixed_side(self):
        width, response = await self._call(self._history(thinking=False), {})

        assert width == 2000
        assert response.command.update == {"image_side_rule": "fixed"}
