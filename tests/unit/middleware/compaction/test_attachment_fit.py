"""A context the model refuses, or one over the threshold, is compacted, and
the attachments in what the summary keeps go until it fits."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from langchain.agents.middleware.types import ModelRequest, ModelResponse
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from ptc_agent.agent.middleware.compaction.compact import Compaction
from ptc_agent.agent.middleware.compaction.middleware import CompactionMiddleware
from ptc_agent.agent.middleware.compaction.offloading import offload_view
from ptc_agent.agent.middleware.compaction.summarize import Summary
from ptc_agent.agent.middleware.compaction.utils import (
    PDF_PAGE_TOKENS,
    build_summary_message,
    find_group_safe_cutoff,
)

pytestmark = pytest.mark.asyncio

THRESHOLD = 10_000
URL = "https://example.test/b.pdf"


def _read(call_id: str, msg_id: str, path: str) -> AIMessage:
    return AIMessage(
        "", id=msg_id, tool_calls=[{"name": "Read", "args": {"file_path": path}, "id": call_id}]
    )


def _pdf_result(call_id: str, msg_id: str, path: str, pages: int = 2) -> ToolMessage:
    return ToolMessage(
        content=[
            {"type": "text", "text": f"Loading document: {path}\n[Viewing PDF: x.pdf]"},
            {
                "type": "file",
                "base64": "JVBERi0=",
                "mime_type": "application/pdf",
                "filename": "x.pdf",
                "pages": pages,
            },
        ],
        tool_call_id=call_id,
        id=msg_id,
    )


def _history() -> list:
    return [
        HumanMessage("go", id="h0"),
        AIMessage("a1", id="a1"),
        AIMessage("a2", id="a2"),
        _read("c1", "r1", "docs/a.pdf"),
        _pdf_result("c1", "t1", "docs/a.pdf"),
        _read("c2", "r2", URL),
        _pdf_result("c2", "t2", URL),
    ]


def _windows(count: int, pages: int) -> list:
    """A turn that read ``count`` windows of ``pages`` pages at once."""
    reads = [
        {"name": "Read", "args": {"file_path": URL, "offset": i * pages}, "id": f"w{i}"}
        for i in range(count)
    ]
    return [
        HumanMessage("read it", id="h0"),
        AIMessage("", id="r0", tool_calls=reads),
        *(_pdf_result(f"w{i}", f"t{i}", URL, pages=pages) for i in range(count)),
    ]


def _middleware(
    compactions: list,
    *,
    keep: int = 4,
    receives=None,
    threshold: int = THRESHOLD,
    page_tokens=None,
) -> CompactionMiddleware:
    async def compact(messages, view, cutoff, **_):
        preserved = view[cutoff:]
        compactions.append(preserved)
        return Compaction(
            event={
                "summary_message": build_summary_message("condensed", None),
                "cutoff_index": cutoff,
                "anchor_message_id": preserved[0].id,
            },
            summary=Summary("condensed", "model", view[:cutoff]),
            original_count=len(view),
            preserved=preserved,
        )

    # Ten tokens a message, so the attachments decide every fit below.
    counter = lambda msgs: 10 * len(list(msgs))  # noqa: E731
    mw = CompactionMiddleware(
        SimpleNamespace(counter=counter, compact=compact),
        token_threshold=threshold,
        keep_tokens=0,
        self_summary=False,
        receives=receives,
        page_tokens=page_tokens,
    )
    # The newest ``keep`` messages stay, whatever their attachments weigh: what
    # is fitted below is what a summary kept, however it was sized.
    mw._cutoff = lambda view, model: (
        find_group_safe_cutoff(view, len(view) - keep) if len(view) > keep else 0
    )
    return mw


async def _call(
    mw,
    outcomes: list,
    *,
    measured: int,
    usage: tuple[int, int] = (0, 0),
    state=None,
    model=None,
):
    """Run one model call; ``outcomes`` are raised or answered in order, an
    answer reporting ``usage`` (input, output). ``state`` replaces the
    starting one. Returns (what each send held, the state update)."""
    sent: list[list] = []

    async def handler(request):
        sent.append(request.messages)
        outcome = outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        answer = AIMessage("done", id="done")
        if usage[0]:
            answer.usage_metadata = {
                "input_tokens": usage[0],
                "output_tokens": usage[1],
                "total_tokens": sum(usage),
            }
        return ModelResponse(result=[answer])

    if state is None:
        state = {"messages": _history(), "_cached_input_tokens": measured}
    request = ModelRequest(
        model=model or GenericFakeChatModel(messages=iter([])),
        messages=state["messages"],
        system_message=None,
        tool_choice=None,
        tools=[],
        response_format=None,
        state=state,
        runtime=None,
    )
    out = await mw.awrap_model_call(request, handler)
    return sent, out.command.update


def _by_id(messages: list, msg_id: str):
    return next(m for m in messages if m.id == msg_id)


def _has_pdf(message) -> bool:
    return any(b.get("type") == "file" for b in message.content)


def _overflow() -> Exception:
    return Exception("Invalid request: Your request exceeded model token limit: 262144")


def _removed(path: str) -> str:
    return (
        f"[{path} (2 pages) was removed from the conversation to fit the context "
        "window. Read it again by page range if you need it.]"
    )


async def test_over_the_threshold_the_oldest_attachments_go_until_it_fits():
    # Kept: two 2-page PDFs, over the threshold together, under it apart.
    assert 2 * 2 * PDF_PAGE_TOKENS > THRESHOLD > 2 * PDF_PAGE_TOKENS
    compactions: list = []

    (sent,), update = await _call(_middleware(compactions), ["ok"], measured=20_000)

    assert len(compactions) == 1
    older, newer = _by_id(sent, "t1"), _by_id(sent, "t2")
    assert older.content == [older.content[0], {"type": "text", "text": _removed("docs/a.pdf")}]
    assert _has_pdf(newer)
    assert update["_removed_attachment_ids"] == {"t1"}


async def test_a_refused_view_is_compacted_then_fitted_by_estimate():
    # A wording LangChain does not classify; the estimate said it would fit.
    # The summary may be what made the room, so the attachments it keeps go
    # only as far as the estimate asks.
    compactions: list = []

    sent, update = await _call(_middleware(compactions), [_overflow(), "ok"], measured=50)

    assert len(compactions) == 1
    first, compacted = sent
    assert _has_pdf(_by_id(first, "t2"))
    assert _by_id(compacted, "t1").content[-1]["text"] == _removed("docs/a.pdf")
    assert _has_pdf(_by_id(compacted, "t2"))
    assert update["_removed_attachment_ids"] == {"t1"}
    assert update["_summarization_event"] is not None


async def test_a_compacted_view_refused_in_its_turn_loses_every_kept_attachment():
    compactions: list = []

    sent, update = await _call(
        _middleware(compactions), [_overflow(), _overflow(), "ok"], measured=50
    )

    assert len(compactions) == 1
    _, fitted, bare = sent
    assert _has_pdf(_by_id(fitted, "t2"))
    assert _by_id(bare, "t1").content[-1]["text"] == _removed("docs/a.pdf")
    assert _by_id(bare, "t2").content[-1]["text"] == _removed(URL)
    assert update["_removed_attachment_ids"] == {"t1", "t2"}


async def test_a_fitted_send_that_is_refused_goes_again_without_any():
    sent, update = await _call(_middleware([]), [_overflow(), "ok"], measured=20_000)

    fitted, bare = sent
    assert _has_pdf(_by_id(fitted, "t2"))
    assert not _has_pdf(_by_id(bare, "t2"))
    assert update["_removed_attachment_ids"] == {"t1", "t2"}


async def test_a_refusal_with_nothing_left_to_remove_is_raised():
    overflow = _overflow()
    with pytest.raises(Exception) as exc_info:
        await _call(_middleware([]), [_overflow(), _overflow(), overflow], measured=50)
    assert exc_info.value is overflow


async def test_a_refused_view_with_nothing_to_remove_is_not_sent_again():
    # Too few messages to summarize and no attachment to take out: the same
    # request would only be refused the same way, so the refusal is raised
    # where a resend would have been answered.
    overflow = _overflow()
    with pytest.raises(Exception) as exc_info:
        await _call(_middleware([], keep=50, receives=_text_only), [overflow, "ok"], measured=50)
    assert exc_info.value is overflow


async def test_too_few_messages_to_summarize_still_lose_their_attachments():
    compactions: list = []

    sent, update = await _call(
        _middleware(compactions, keep=50), [_overflow(), "ok"], measured=50
    )

    assert compactions == []
    assert not _has_pdf(_by_id(sent[1], "t1")) and not _has_pdf(_by_id(sent[1], "t2"))
    assert update["_removed_attachment_ids"] == {"t1", "t2"}


async def test_another_failure_is_raised_without_compacting():
    compactions: list = []
    with pytest.raises(Exception, match="unsupported keyword"):
        await _call(
            _middleware(compactions), [Exception("tools.0: unsupported keyword")], measured=50
        )
    assert compactions == []


async def test_a_read_since_the_last_measurement_counts_toward_the_trigger():
    # The last call measured 5,000, under the threshold; the Read result
    # after it carries a 2-page PDF the measurement never saw.
    compactions: list = []

    await _call(_middleware(compactions), ["ok"], measured=5_000)

    assert len(compactions) == 1


def _text_only(model):
    # As the strip judges a model that reads no PDFs.
    return lambda block: not (isinstance(block, dict) and block.get("type") == "file")


async def test_attachments_the_target_cannot_take_do_not_count_toward_the_trigger():
    # The strip, inside, sends a placeholder in place of each, so only the
    # placeholder's few tokens reach the request.
    compactions: list = []

    await _call(_middleware(compactions, receives=_text_only), ["ok"], measured=5_000)

    assert compactions == []


async def test_a_refusal_leaves_attachments_the_target_cannot_take_to_the_strip():
    # Removing them would swap the strip's extraction guidance for advice to
    # Read them again, which the target still could not use.
    compactions: list = []

    sent, update = await _call(
        _middleware(compactions, receives=_text_only), [_overflow(), "ok"], measured=50
    )

    assert len(compactions) == 1
    assert _has_pdf(_by_id(sent[1], "t1")) and _has_pdf(_by_id(sent[1], "t2"))
    assert not update["_removed_attachment_ids"]


async def test_a_fit_the_provider_measures_over_the_threshold_takes_out_more():
    # The estimate fitted the summary's view by removing t1; the provider
    # counts that view at twice the estimate, so t2 goes too, at twice its
    # estimate, and the next call reads what is left.
    compactions: list = []
    estimate = 5 * 10 + 2 * PDF_PAGE_TOKENS

    _, update = await _call(
        _middleware(compactions), ["ok"], measured=20_000, usage=(2 * estimate, 100)
    )

    assert len(compactions) == 1
    assert update["_removed_attachment_ids"] == {"t1", "t2"}
    assert update["_cached_input_tokens"] == 2 * estimate - 2 * 2 * PDF_PAGE_TOKENS


async def test_a_summary_does_not_repeat_over_attachments_it_kept():
    compactions: list = []
    mw = _middleware(compactions)
    _, update = await _call(mw, ["ok"], measured=20_000, usage=(12_100, 100))
    history = [*_history(), AIMessage("done", id="done")]

    sent, _ = await _call(mw, ["ok"], measured=0, state={**update, "messages": history})

    assert len(compactions) == 1
    (view,) = sent
    assert not _has_pdf(_by_id(view, "t1")) and not _has_pdf(_by_id(view, "t2"))


async def test_a_fit_the_provider_measures_under_the_threshold_stands():
    _, update = await _call(_middleware([]), ["ok"], measured=20_000, usage=(6_000, 100))

    assert update["_removed_attachment_ids"] == {"t1"}
    assert update["_cached_input_tokens"] == 6_000


async def test_the_next_call_is_sent_the_view_without_them():
    _, update = await _call(_middleware([]), ["ok"], measured=20_000)
    state = {"messages": _history(), **update}

    view = await offload_view(state["messages"], state, max_length=2000, transcript=None)

    assert not _has_pdf(_by_id(view, "t1"))
    assert _has_pdf(_by_id(view, "t2"))


async def test_a_removed_pdf_with_no_stamp_is_parsed_off_the_event_loop(monkeypatch):
    # One an older build checkpointed, on a worker that has not parsed it yet.
    import base64
    import io
    import threading

    import pypdf

    from ptc_agent.agent.middleware.compaction import utils

    writer = pypdf.PdfWriter()
    for _ in range(3):
        writer.add_blank_page(width=612, height=792)
    out = io.BytesIO()
    writer.write(out)
    threads = []
    real = pypdf.PdfReader

    def reader(stream, *args, **kwargs):
        threads.append(threading.current_thread())
        return real(stream, *args, **kwargs)

    monkeypatch.setattr(pypdf, "PdfReader", reader)
    monkeypatch.setattr(utils, "_PARSED", type(utils._PARSED)())
    legacy = _pdf_result("c1", "t1", "docs/a.pdf")
    del legacy.content[1]["pages"]
    legacy.content[1]["base64"] = base64.b64encode(out.getvalue()).decode()
    state = {
        "messages": [HumanMessage("go", id="h0"), _read("c1", "r1", "docs/a.pdf"), legacy],
        "_removed_attachment_ids": {"t1"},
    }

    view = await offload_view(state["messages"], state, max_length=2000, transcript=None)

    assert "docs/a.pdf (3 pages) was removed" in _by_id(view, "t1").content[1]["text"]
    assert threads and threading.main_thread() not in threads


async def test_a_later_summary_drops_the_ones_it_summarized():
    summary = build_summary_message("condensed", None)
    history = _history()
    compaction = Compaction(
        event={"summary_message": summary, "cutoff_index": 5, "anchor_message_id": "r2"},
        summary=Summary("condensed", "model", history[:5]),
        original_count=len(history),
        preserved=history[5:],
    )

    update = compaction.update({"_removed_attachment_ids": {"t1", "t2"}})

    assert update["_removed_attachment_ids"] == {"t2"}


@pytest.mark.parametrize(
    ("manifest_model", "kept"),
    [
        # Gemini charges a page as its image, about a fifth of the default.
        ("gemini-3.1-flash-lite", 7),
        # Anthropic is counted at the default, the top of its documented range.
        ("claude-haiku-5-5", 4),
    ],
)
async def test_a_pdf_page_counts_at_what_the_target_is_charged(manifest_model, kept):
    from ptc_agent.agent.middleware.file_operations.multimodal_strip import (
        MultimodalStripMiddleware,
    )

    # Seven 20-page windows read at once: over a 300k threshold at 3,000 a
    # page, well under it at Gemini's charge.
    compactions: list = []
    mw = _middleware(
        compactions,
        keep=50,
        threshold=300_000,
        page_tokens=MultimodalStripMiddleware().pdf_page_tokens,
    )
    model = GenericFakeChatModel(messages=iter([]), metadata={"manifest_model": manifest_model})
    state = {"messages": _windows(7, 20), "_cached_input_tokens": 25_000}
    sent, _ = await _call(mw, ["ok"], measured=25_000, state=state, model=model)

    assert len(sent) == 1
    assert sum(1 for m in sent[0] if isinstance(m, ToolMessage) and _has_pdf(m)) == kept
