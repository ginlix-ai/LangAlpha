"""The write side: getting an image or PDF onto the tool result that produced it.

Its counterpart, the strip that decides what the model about to be called can
still read, is covered in ``test_multimodal_strip``.
"""

import asyncio
import io
import types

import httpx
import pypdf
import pytest
from langchain_core.messages import ToolMessage
from PIL import Image

from ptc_agent.core.sandbox import files as sandbox_files
from ptc_agent.agent.middleware.file_operations.multimodal import (
    MultimodalMiddleware,
    _is_visual_request,
    attach_to_tool_result,
)
from src.tools.web.inhouse import guard


def _pdf_bytes(pages: int = 1) -> bytes:
    """A structurally valid PDF. Generated rather than committed as a blob so the
    page count is a parameter — that is what the provider ceiling is measured in."""
    writer = pypdf.PdfWriter()
    for _ in range(pages):
        writer.add_blank_page(width=72, height=72)
    buf = io.BytesIO()
    writer.write(buf)
    return buf.getvalue()


def _ack() -> ToolMessage:
    """The Read tool's own result, which is what the handlers are handed.

    Its text is what the attachment is merged onto, so it has to be a
    ToolMessage here: an AIMessage stand-in takes the construct-a-new-one
    branch and never exercises the copy the live path takes."""
    return ToolMessage(content="ok", tool_call_id="call-1")


def _png_bytes() -> bytes:
    buf = io.BytesIO()
    Image.new("RGB", (2, 2), "red").save(buf, format="PNG")
    return buf.getvalue()


class _Sandbox:
    """Stands in for PTCSandbox — which normalizes on its own, not on download.

    The capped download is the real one, over a runtime that streams
    ``content``; ``stream`` is what a test overrides.
    """

    def __init__(self, content: bytes = b"", *, work_dir: str = "/home/workspace"):
        self._content = content
        self._work_dir = work_dir
        self.downloaded: str | None = None
        self.runtime = types.SimpleNamespace(download_file_stream=self.stream)
        self._download_semaphore = asyncio.Semaphore(4)

    def normalize_path(self, path: str) -> str:
        if path.startswith((self._work_dir, "/tmp")):
            return path
        return f"{self._work_dir}/{path.lstrip('/')}"

    async def stream(self, path: str):
        self.downloaded = path
        for start in range(0, len(self._content), 1024):
            yield self._content[start : start + 1024]

    async def _wait_ready(self) -> None:
        pass

    async def _runtime_call(self, func, *args, retry_policy, **kwargs):
        return await func(*args)

    async def adownload_file_bytes_capped(self, path: str, cap):
        return await sandbox_files.adownload_file_bytes_capped(self, path, cap)


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


class TestTheWriteSideNeverJudgesTheModel:
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
        mw = MultimodalMiddleware(sandbox=_Sandbox(_png_bytes()))

        result = await mw.awrap_tool_call(self._read("chart.png"), self._handler)
        assert isinstance(result, ToolMessage)
        assert result.content[-1]["type"] == "image_url"

    @pytest.mark.parametrize(
        "file_path",
        ["chart.png", "report.pdf", "https://example.com/asset"],
        ids=["image-ext", "pdf-ext", "url-without-extension"],
    )
    @pytest.mark.asyncio
    async def test_no_verdict_is_frozen_into_the_transcript(
        self, file_path, monkeypatch
    ):
        """The extensionless URL is the interesting one: it could resolve to
        either kind, and the old gate judged all three off the configured name.

        The transport is mocked because the assertion is a negative: an
        unmocked URL case would reach the network, and every way that can fail
        also satisfies "no refusal in the content", so the case would pass
        without ever exercising the path it names.
        """
        served = _png_bytes()

        def handler(request):
            return httpx.Response(
                200, content=served, headers={"content-type": "image/png"}
            )

        monkeypatch.setattr(
            guard, "GuardedAsyncTransport", lambda: httpx.MockTransport(handler)
        )
        mw = MultimodalMiddleware(sandbox=_Sandbox(served))
        result = await mw.awrap_tool_call(self._read(file_path), self._handler)
        assert "does not support" not in str(result.content)
        # Non-vacuous: the path really ran and produced the block.
        assert result.content[-1]["type"] == "image_url"



class TestContentSizeCap:
    @pytest.mark.asyncio
    async def test_oversized_download_is_aborted(self, monkeypatch):
        """The body is streamed so the cap can fire mid-transfer — a Content-Length
        check alone is a header a server can simply lie about."""
        mw = MultimodalMiddleware(sandbox=_Sandbox())
        monkeypatch.setattr(mw, "MAX_CONTENT_BYTES", 1024)

        def handler(request):
            return httpx.Response(200, content=b"x" * 4096)

        monkeypatch.setattr(
            guard, "GuardedAsyncTransport", lambda: httpx.MockTransport(handler)
        )

        result = await mw._handle_url_content(
            "https://example.com/big.png", _ack(), "call-1"
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
            "/home/workspace/big.png", _ack(), "call-1"
        )
        assert "larger than" in result.content
        assert result.tool_call_id == "call-1"

    @pytest.mark.asyncio
    async def test_a_refusal_never_reaches_graph_state(self, monkeypatch):
        """Attaching the block would write it into the checkpoint; refusing has
        to leave the result plain text or the cap accomplishes nothing."""
        mw = MultimodalMiddleware(sandbox=_Sandbox(b"x" * 4096))
        monkeypatch.setattr(mw, "MAX_CONTENT_BYTES", 1024)

        result = await mw._handle_sandbox_content(
            "/home/workspace/big.png", _ack(), "call-1"
        )
        assert isinstance(result, ToolMessage)
        assert isinstance(result.content, str)

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

        mw = MultimodalMiddleware(sandbox=_Sandbox())
        monkeypatch.setattr(guard, "GuardedAsyncTransport", _Blocking)

        result = await mw._handle_url_content(
            "http://x.corp/a.png", _ack(), "call-1"
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
            "/results/chart.png", _ack(), "call-1"
        )
        assert sandbox.downloaded == "/home/workspace/results/chart.png"
        assert isinstance(result, ToolMessage)

    @pytest.mark.asyncio
    async def test_an_already_absolute_path_is_left_alone(self):
        sandbox = _Sandbox(_png_bytes())
        mw = MultimodalMiddleware(sandbox=sandbox)

        await mw._handle_sandbox_content(
            "/home/workspace/work/t/charts/fig.png", _ack(), "call-1"
        )
        assert sandbox.downloaded == "/home/workspace/work/t/charts/fig.png"


class _Reach:
    """The user's computers as a Chief of Staff turn reaches them: ``answer``
    is the target a name resolves to, None for the turn's own, or the error."""

    def __init__(self, answer):
        self.answer = answer

    async def reach(self, computer):
        if isinstance(self.answer, Exception):
            raise self.answer
        return self.answer


class TestAReadOnAnotherComputer:
    """The Chief of Staff's Read that names a computer attaches its bytes from
    there, never a file under the same path on the turn's own."""

    @staticmethod
    def _read(computer):
        args = {"file_path": "charts/c.png", "computer": computer}
        return types.SimpleNamespace(tool_call={"name": "Read", "args": args, "id": "tc-1"})

    @staticmethod
    async def _handler(_request):
        return ToolMessage(content="ok", tool_call_id="tc-1")

    @pytest.mark.asyncio
    async def test_its_bytes_come_from_that_computer(self):
        home = _Sandbox(_png_bytes())
        there = _Sandbox(_png_bytes(), work_dir="/Users/me/langalpha")
        target = types.SimpleNamespace(sandbox=there, project=None)
        mw = MultimodalMiddleware(sandbox=home, computers=_Reach(target))

        result = await mw.awrap_tool_call(self._read("Studio Mac"), self._handler)

        assert there.downloaded == "/Users/me/langalpha/charts/c.png"
        assert home.downloaded is None
        assert isinstance(result.content, list)

    @pytest.mark.asyncio
    async def test_the_turns_own_computer_is_read_here(self):
        home = _Sandbox(_png_bytes())
        mw = MultimodalMiddleware(sandbox=home, computers=_Reach(None))

        await mw.awrap_tool_call(self._read("Cloud computer"), self._handler)

        assert home.downloaded == "/home/workspace/charts/c.png"

    @pytest.mark.asyncio
    async def test_one_no_longer_reachable_answers_as_the_tools_own_call_would(self):
        """It once fell back to the turn's own computer, which could attach
        another file under the same path."""
        from ptc_agent.agent.tools.computer_reach import ComputerUnreachable

        home = _Sandbox(_png_bytes())
        gone = ComputerUnreachable('Computer "Studio Mac" is not reachable right now.')
        mw = MultimodalMiddleware(sandbox=home, computers=_Reach(gone))

        result = await mw.awrap_tool_call(self._read("Studio Mac"), self._handler)

        assert result.content == 'ERROR: Computer "Studio Mac" is not reachable right now.'
        assert home.downloaded is None


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
            "chart.png", _ack(), "call-1"
        )
        assert isinstance(result, ToolMessage)
        assert isinstance(result.content, str)
        assert "not a readable image or PDF" in result.content

    @pytest.mark.asyncio
    async def test_a_sandbox_pdf_named_png_is_read_as_a_pdf(self):
        mw = MultimodalMiddleware(sandbox=_Sandbox(_pdf_bytes()))

        result = await mw._handle_sandbox_content(
            "report.png", _ack(), "call-1"
        )
        blocks = result.content
        assert [b["type"] for b in blocks] == ["text", "file"]
        assert blocks[-1]["mime_type"] == "application/pdf"

    @pytest.mark.asyncio
    async def test_an_extensionless_url_serving_a_pdf_is_not_judged_as_an_image(
        self, monkeypatch
    ):
        """Signed and redirected URLs routinely end without a suffix; branching on
        it sent every one of them down the image path."""
        mw = MultimodalMiddleware(sandbox=_Sandbox())
        monkeypatch.setattr(
            guard,
            "GuardedAsyncTransport",
            lambda: httpx.MockTransport(
                lambda request: httpx.Response(200, content=_pdf_bytes())
            ),
        )

        result = await mw._handle_url_content(
            "https://files.example.com/d/abc123", _ack(), "call-1"
        )
        assert isinstance(result, ToolMessage)
        assert result.content[-1]["mime_type"] == "application/pdf"

    @pytest.mark.asyncio
    async def test_a_format_no_provider_accepts_is_refused_not_relabeled(
        self, monkeypatch
    ):
        """The old URL path defaulted an unmapped PIL format to image/png, which
        ships a BMP under a PNG mime and earns a 400 on every replay."""
        buf = io.BytesIO()
        Image.new("RGB", (2, 2), "blue").save(buf, format="BMP")
        mw = MultimodalMiddleware(sandbox=_Sandbox())
        monkeypatch.setattr(
            guard,
            "GuardedAsyncTransport",
            lambda: httpx.MockTransport(
                lambda request: httpx.Response(200, content=buf.getvalue())
            ),
        )

        result = await mw._handle_url_content(
            "https://example.com/a.bmp", _ack(), "call-1"
        )
        assert isinstance(result, ToolMessage)
        assert isinstance(result.content, str)
        assert "BMP" in result.content


class TestPDFsAreVerifiedNotSniffed:
    """A `%PDF` prefix is four bytes of claim. Everything past it — the xref, the
    page tree, the page count — is what a provider actually validates, and the
    tool result carrying the block is written to the checkpoint before any
    provider sees it.
    """

    @pytest.mark.asyncio
    async def test_a_header_only_pdf_never_reaches_graph_state(self):
        """The bytes that pass a prefix check and nothing else."""
        mw = MultimodalMiddleware(sandbox=_Sandbox(b"%PDF-1.4 body"))

        result = await mw._handle_sandbox_content(
            "report.pdf", _ack(), "call-1"
        )
        assert isinstance(result, ToolMessage)
        assert isinstance(result.content, str)
        assert "not a readable PDF" in result.content

    @pytest.mark.asyncio
    async def test_a_truncated_pdf_is_refused(self):
        """The realistic shape: an interrupted download or a half-written report,
        whose head is a genuine PDF."""
        whole = _pdf_bytes()
        mw = MultimodalMiddleware(sandbox=_Sandbox(whole[: len(whole) * 6 // 10]))

        result = await mw._handle_sandbox_content(
            "report.pdf", _ack(), "call-1"
        )
        assert isinstance(result, ToolMessage)
        assert "not a readable PDF" in result.content

    @pytest.mark.asyncio
    async def test_trailing_bytes_do_not_condemn_an_otherwise_readable_pdf(self):
        """Verification has to reject damage, not tidiness. Real PDFs routinely
        carry junk after the final %%EOF, and providers accept them."""
        mw = MultimodalMiddleware(sandbox=_Sandbox(_pdf_bytes() + b"\n<!-- appended -->"))

        result = await mw._handle_sandbox_content(
            "report.pdf", _ack(), "call-1"
        )
        assert isinstance(result, ToolMessage)
        assert result.content[-1]["mime_type"] == "application/pdf"


class TestTheAttachmentRidesOnTheToolResult:
    """The attachment is the tool result, not a message that follows it.

    A second message could be separated from the call it answers by a
    compaction cutoff, by a parallel batch's ordering, or by any later history
    surgery. Carrying the blocks on the result makes those failures
    unrepresentable: the attachment moves wherever the result moves.
    """

    _BLOCKS = [
        {"type": "text", "text": "[Viewing image]"},
        {"type": "image_url", "image_url": {"url": "data:image/png;base64,abc"}},
    ]

    def test_the_acknowledgment_merges_into_the_leading_text_block(self):
        result = attach_to_tool_result(
            ToolMessage(content="Loading image: chart.png", tool_call_id="call-1"),
            self._BLOCKS,
        )
        assert result.content[0] == {
            "type": "text",
            "text": "Loading image: chart.png\n[Viewing image]",
        }
        assert result.content[1] is self._BLOCKS[1]

    def test_the_result_keeps_its_own_identity(self):
        """Copied rather than rebuilt: name and tool_call_id are what pair the
        result with its call."""
        result = attach_to_tool_result(
            ToolMessage(content="ok", tool_call_id="call-1", name="Read"),
            self._BLOCKS,
        )
        assert result.tool_call_id == "call-1"
        assert result.name == "Read"

    def test_an_acknowledgment_with_no_text_block_to_merge_into_leads_instead(self):
        result = attach_to_tool_result(
            ToolMessage(content="ok", tool_call_id="call-1"),
            [self._BLOCKS[1]],
        )
        assert [b["type"] for b in result.content] == ["text", "image_url"]
        assert result.content[0]["text"] == "ok"

    def test_the_blocks_handed_in_are_not_mutated(self):
        """The caller built them from the file bytes; a later read reuses the
        same list."""
        blocks = [dict(b) for b in self._BLOCKS]
        before = [dict(b) for b in blocks]
        attach_to_tool_result(
            ToolMessage(content="ok", tool_call_id="call-1"), blocks
        )
        assert blocks == before


def _numbered_pdf(pages: int, *, page_bytes: int = 0) -> bytes:
    """A PDF whose page i is 100 + i points wide, so a window's pages can be
    told apart from any others. ``page_bytes`` gives each page a content
    stream of incompressible bytes, the weight of a scanned page."""
    import os

    from pypdf.generic import DecodedStreamObject, NameObject

    writer = pypdf.PdfWriter()
    for i in range(pages):
        page = writer.add_blank_page(width=100 + i, height=72)
        if page_bytes:
            stream = DecodedStreamObject()
            # A PDF comment line: valid content that draws nothing.
            stream.set_data(b"%" + os.urandom(page_bytes // 2).hex().encode() + b"\n")
            page[NameObject("/Contents")] = writer._add_object(stream)
    buf = io.BytesIO()
    writer.write(buf)
    return buf.getvalue()


class TestPdfPageWindows:
    """A Read attaches a window of a PDF's pages, so no single Read can fill
    the context window with one document."""

    @staticmethod
    async def _read(
        pdf: bytes, path: str = "docs/r.pdf", *, caps: dict | None = None, **args
    ) -> ToolMessage:
        mw = MultimodalMiddleware(sandbox=_Sandbox(pdf))
        for name, value in (caps or {}).items():
            setattr(mw, name, value)
        request = types.SimpleNamespace(
            tool_call={"name": "Read", "args": {"file_path": path, **args}, "id": "call-1"}
        )

        async def handler(_request):
            return ToolMessage(content=f"Loading document: {path}", tool_call_id="call-1")

        return await mw.awrap_tool_call(request, handler)

    @staticmethod
    def _attached(result: ToolMessage) -> list[int]:
        import base64

        reader = pypdf.PdfReader(io.BytesIO(base64.b64decode(result.content[-1]["base64"])))
        return [int(page.mediabox.width) - 100 for page in reader.pages]

    @pytest.mark.asyncio
    async def test_a_long_pdf_comes_back_as_its_first_pages_and_where_to_go_on(self):
        result = await self._read(_numbered_pdf(45))

        assert self._attached(result) == list(range(20))
        assert result.content[-1]["pages"] == 20
        assert result.content[0]["text"] == (
            "Loading document: docs/r.pdf\n[Viewing PDF: r.pdf]\n"
            "[Pages 1-20 of 45. Read(file_path='docs/r.pdf', offset=20) for the next pages.]"
        )

    @pytest.mark.asyncio
    async def test_offset_skips_pages_and_limit_counts_them(self):
        result = await self._read(_numbered_pdf(45), offset=10, limit=3)

        assert self._attached(result) == [10, 11, 12]
        assert result.content[0]["text"].endswith(
            "[Pages 11-13 of 45. Read(file_path='docs/r.pdf', offset=13) for the next pages.]"
        )

    @pytest.mark.asyncio
    async def test_the_last_window_has_no_marker(self):
        result = await self._read(_numbered_pdf(45), offset=40)

        assert self._attached(result) == [40, 41, 42, 43, 44]
        assert result.content[0]["text"] == "Loading document: docs/r.pdf\n[Viewing PDF: r.pdf]"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("limit", [2000, 0, -5, "many"])
    async def test_a_limit_past_the_window_or_unusable_is_the_window(self, limit):
        # The model sizes limit in lines unless it means pages.
        result = await self._read(_numbered_pdf(45), limit=limit)

        assert result.content[-1]["pages"] == 20

    @pytest.mark.asyncio
    async def test_a_pdf_whose_page_tree_reads_empty_goes_whole(self):
        # pypdf counts no pages where a writer left the tree empty or broken;
        # the provider may still read it, so it is not refused as past its end.
        empty = _numbered_pdf(0)
        result = await self._read(empty)

        assert result.status != "error"
        assert result.content[-1]["type"] == "file"
        assert "pages" not in result.content[-1]

        refused = await self._read(empty, caps={"MAX_CONTENT_BYTES": len(empty) - 1})
        assert refused.status == "error"
        assert "past the last page" not in refused.content

    @pytest.mark.asyncio
    async def test_an_offset_past_the_end_says_how_long_the_pdf_is(self):
        result = await self._read(_numbered_pdf(45), offset=45)

        assert result.status == "error"
        assert result.content == (
            "ERROR: offset=45 is past the last page of this 45-page PDF: docs/r.pdf"
        )

    @pytest.mark.asyncio
    async def test_a_pdf_inside_one_window_is_sent_as_written(self):
        import base64

        pdf = _numbered_pdf(5)
        result = await self._read(pdf)

        assert base64.b64decode(result.content[-1]["base64"]) == pdf
        assert result.content[-1]["pages"] == 5

    @pytest.mark.asyncio
    async def test_a_url_pdf_is_windowed_too(self, monkeypatch):
        url = "https://files.example.com/report.pdf"
        monkeypatch.setattr(
            guard,
            "GuardedAsyncTransport",
            lambda: httpx.MockTransport(
                lambda request: httpx.Response(200, content=_numbered_pdf(30))
            ),
        )

        result = await self._read(b"", path=url, offset=20)

        assert self._attached(result) == list(range(20, 30))
        assert "Read(file_path=" not in result.content[0]["text"]

    @pytest.mark.asyncio
    async def test_a_one_page_window_names_its_page(self):
        result = await self._read(_numbered_pdf(45), offset=7, limit=1)

        assert result.content[0]["text"].endswith(
            "[Page 8 of 45. Read(file_path='docs/r.pdf', offset=8) for the next pages.]"
        )


class TestPdfByteCaps:
    """A PDF source is bounded apart from the window cut from it: only the
    window reaches the checkpoint, which is what MAX_CONTENT_BYTES is for."""

    _read = staticmethod(TestPdfPageWindows._read)
    _attached = staticmethod(TestPdfPageWindows._attached)

    @pytest.mark.asyncio
    async def test_a_source_over_the_block_cap_loads_a_window_halved_to_fit(self):
        # Pages of ~2 KB under a 9 KB cap: 20 pages, then 10, then 5, then 2.
        pdf = _numbered_pdf(45, page_bytes=2048)
        assert len(pdf) > 9_000

        result = await self._read(pdf, offset=4, caps={"MAX_CONTENT_BYTES": 9_000})

        assert self._attached(result) == [4, 5]
        assert result.content[-1]["pages"] == 2
        assert result.content[0]["text"].endswith(
            "[Pages 5-6 of 45. Read(file_path='docs/r.pdf', offset=6) for the next pages.]"
        )

    @pytest.mark.asyncio
    async def test_a_single_page_over_the_block_cap_is_refused(self):
        result = await self._read(
            _numbered_pdf(3, page_bytes=8192), caps={"MAX_CONTENT_BYTES": 4096}
        )

        assert result.status == "error"
        assert result.content == (
            "ERROR: Content is larger than the 0MB limit and was not loaded: docs/r.pdf"
        )

    @pytest.mark.asyncio
    async def test_a_source_over_the_pdf_cap_is_refused_naming_it(self):
        pdf = b"%PDF-1.7\n" + b"0" * (2 * 1024 * 1024)

        result = await self._read(pdf, caps={"MAX_PDF_SOURCE_BYTES": 1024 * 1024})

        assert result.content == (
            "ERROR: Content is larger than the 1MB limit and was not loaded: docs/r.pdf"
        )

    @pytest.mark.asyncio
    async def test_the_sandbox_stream_is_closed_when_the_cap_stops_it(self):
        closed = []

        class _Closing(_Sandbox):
            async def stream(self, path):
                try:
                    while True:
                        yield b"x" * 1024
                finally:
                    closed.append(path)

        mw = MultimodalMiddleware(sandbox=_Closing())
        result = await mw._handle_sandbox_content("/home/workspace/big.png", _ack(), "call-1")

        assert result.content.startswith("ERROR: Content is larger than the 5MB limit")
        assert closed == ["/home/workspace/big.png"]

    @pytest.mark.asyncio
    async def test_parallel_reads_load_four_at_a_time(self):
        # A 64 MiB source is held with its copies while its window is cut, so
        # a parallel batch of Reads waits for a slot rather than all loading.
        loading, most, release = 0, 0, asyncio.Event()

        class _Slow(_Sandbox):
            async def stream(self, path):
                nonlocal loading, most
                loading += 1
                most = max(most, loading)
                await release.wait()
                loading -= 1
                yield _png_bytes()

        # Plenty of download slots: they are given back before the window is
        # cut, and a URL takes none, so they are not what holds this to four.
        sandbox = _Slow()
        sandbox._download_semaphore = asyncio.Semaphore(100)
        mw = MultimodalMiddleware(sandbox=sandbox)

        def read(i):
            request = types.SimpleNamespace(
                tool_call={"name": "Read", "args": {"file_path": f"c{i}.png"}, "id": f"call-{i}"}
            )

            async def handler(_request):
                return ToolMessage(content="ok", tool_call_id=f"call-{i}")

            return mw.awrap_tool_call(request, handler)

        reads = asyncio.gather(*(read(i) for i in range(7)))
        for _ in range(20):
            await asyncio.sleep(0)
        assert most == 4
        release.set()
        results = await reads
        assert most == 4
        assert all(r.content[-1]["type"] == "image_url" for r in results)

    def test_the_caps(self):
        assert MultimodalMiddleware.MAX_CONTENT_BYTES == 5 * 1024 * 1024
        assert MultimodalMiddleware.MAX_PDF_SOURCE_BYTES == 64 * 1024 * 1024
