from __future__ import annotations

import asyncio

import pytest
from langchain_anthropic.chat_models import convert_to_anthropic_tool
from langchain_core.messages import ToolMessage
from langchain_core.tools import tool

from claude_agent_sdk import ToolResultBlock

from ptc_agent.agent.claude_code.tools_bridge import Answered, Ended, Run, ToolResults, allow_rule, to_call_result, tool_specs


@tool("Read")
def read(file_path: str, limit: int = 100) -> str:
    """Read a file \u2026 up to limit lines."""
    return ""


def test_specs_carry_the_schema_and_description_a_langalpha_turn_sends():
    (spec,) = tool_specs([read], read_only=frozenset({"Read"}))
    wire = convert_to_anthropic_tool(read)
    assert spec == {"name": wire["name"], "description": wire["description"], "input_schema": wire["input_schema"], "read_only": True}


def test_allow_rules_use_the_prefixed_form_the_cli_matches():
    assert allow_rule("Bash") == "mcp__langalpha__Bash"


def test_tool_messages_become_call_results():
    image = {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "aGk="}}
    read_image = {"type": "image_url", "image_url": {"url": "data:image/png;base64,aGk="}}
    read_pdf = {"type": "file", "base64": "JVBERi0=", "mime_type": "application/pdf"}
    result = to_call_result(ToolMessage(content=["text", {"type": "text", "text": "more"}, image, read_image, read_pdf], tool_call_id="t1"))
    assert [c.type for c in result.content] == ["text", "text", "image", "image", "resource"]
    assert result.content[3].data == "aGk=" and result.content[4].resource.mime_type == "application/pdf"
    assert result.is_error is False
    assert to_call_result(ToolMessage(content="boom", tool_call_id="t2", status="error")).is_error is True


@pytest.mark.asyncio
async def test_a_result_waits_for_the_tools_step_and_unrun_calls_fail_at_close():
    results = ToolResults()
    results.resolve(ToolMessage(content="done", tool_call_id="t1"))
    assert (await results.wait("t1", {})).content[0].text == "done"
    asked = asyncio.ensure_future(results.wait("t2", {}))
    await asyncio.sleep(0)
    results.fail_all("The turn ended before this call ran.")
    late = await asyncio.wait_for(asked, 1)
    assert late.is_error and late.content[0].text == "The turn ended before this call ran."


@pytest.mark.asyncio
async def test_a_call_that_arrives_after_close_fails_at_once():
    results = ToolResults()
    results.fail_all("The turn ended before this call ran.")
    after = await asyncio.wait_for(results.wait("t3", {}), 1)
    assert after.is_error and after.content[0].text == "The turn ended before this call ran."


@pytest.mark.asyncio
async def test_a_call_is_due_as_the_cli_first_takes_it_up():
    results = ToolResults()
    refused = ToolResultBlock(tool_use_id="t2", content="Permission to use Bash has been denied.", is_error=True)
    asked = asyncio.ensure_future(results.wait("t1", {"command": "rewritten"}))
    await asyncio.sleep(0)
    results.answered([refused])
    results.ended()
    assert await results.due("t1") == Run({"command": "rewritten"})
    assert await results.due("t2") == Answered(refused)
    results.due("t4")
    results.ended()
    assert await asyncio.wait_for(results.due("t4"), 1) == Ended()
    assert await asyncio.wait_for(results.due("t3"), 1) == Ended()
    asked.cancel()


def _ran(results: ToolResults, message: ToolMessage, given) -> ToolResultBlock | None:
    results.resolve(message)
    results.answered([ToolResultBlock(tool_use_id=message.tool_call_id, content=given)])
    return results.given_instead(message.tool_call_id, "Bash")


IMAGE = {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "aGk="}}
#: The CLI re-encodes a large image, so the one it gives Claude can differ from ours.
RESIZED = {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "cmVzaXplZA=="}}
PDF = {"type": "document", "source": {"type": "base64", "media_type": "application/pdf", "data": "JVBERi0="}}
IMAGE_NOTE = {"type": "text", "text": "[Image: source: /cfg/projects/p/s/tool-results/mcp-langalpha-blob-1793402130429-mqjsub.png]"}
PDF_NOTE = {"type": "text", "text": ("[Resource from langalpha at langalpha://t1/document] Binary content (application/pdf, 44 bytes) "
                                     "saved to /cfg/projects/p/s/tool-results/mcp-langalpha-blob-1793402134777-7xxtfb.pdf")}


@pytest.mark.asyncio
@pytest.mark.parametrize(("ours", "given"), [
    ("ran: hello", [{"type": "text", "text": "ran: hello"}]),
    ("boom", "boom"),
    ("", "(Bash completed with no output)"),
    ([{"type": "text", "text": "a chart"}, IMAGE], [{"type": "text", "text": "a chart"}, IMAGE, IMAGE_NOTE]),
    ([IMAGE], [RESIZED, IMAGE_NOTE]),
    ([{"type": "text", "text": "a pdf"}, PDF], [{"type": "text", "text": "a pdf"}, PDF_NOTE]),
    ([PDF], [PDF_NOTE]),
    ([{"type": "text", "text": "[Image: source: /x]"}, PDF], [{"type": "text", "text": "[Image: source: /x]"}, PDF_NOTE]),
    ([{"type": "text", "text": PDF_NOTE["text"]}, IMAGE], [{"type": "text", "text": PDF_NOTE["text"]}, IMAGE, IMAGE_NOTE]),
], ids=["text", "error-string", "empty", "image", "large-image", "document", "document-only", "note-like-text-beside-a-document",
        "note-like-text-beside-an-image"])
async def test_the_clis_own_wording_of_our_result_is_our_result(ours, given):
    assert _ran(ToolResults(), ToolMessage(content=ours, tool_call_id="t1"), given) is None


@pytest.mark.asyncio
@pytest.mark.parametrize(("ours", "given"), [
    ("ran: hello", "replaced by hook"),
    ("ran: hello", [{"type": "text", "text": "replaced list"}]),
    ("ran: hello", [{"type": "text", "text": "ran: hello"}, {"type": "text", "text": "appended by a hook"}]),
    ("", "replaced by hook"),
    ("z" * 600, "<persisted-output>\nOutput too large (586KB). Full output saved to: /cfg/tool-results/t1.txt"),
    ([IMAGE], "redacted by hook"),
    ([{"type": "text", "text": "a chart"}, IMAGE], [{"type": "text", "text": "a chart"}]),
    ([{"type": "text", "text": "a chart"}, IMAGE], [{"type": "text", "text": "a chart"}, {"type": "text", "text": "[image removed]"}]),
    ([{"type": "text", "text": "a chart"}, IMAGE], [{"type": "text", "text": "a chart"}, IMAGE, {"type": "text", "text": "and more"}]),
    ([{"type": "text", "text": "a chart"}, IMAGE], [{"type": "text", "text": "a chart"}, IMAGE_NOTE]),
    ([{"type": "text", "text": "a pdf"}, PDF], [{"type": "text", "text": "a pdf"}]),
    ([{"type": "text", "text": "a pdf"}, PDF], [{"type": "text", "text": "a pdf"}, {**PDF_NOTE, "text": PDF_NOTE["text"].replace("t1", "t9")}]),
], ids=["hook-text", "hook-list", "hook-appended", "hook-on-empty", "preview", "hook-on-image-only", "image-dropped", "image-dropped-for-text", "note-replaced", "image-dropped-note-kept",
        "document-dropped", "another-resource"])
async def test_other_content_than_our_result_is_the_answer_claude_was_given(ours, given):
    block = _ran(ToolResults(), ToolMessage(content=ours, tool_call_id="t1"), given)
    assert block is not None and block.content == given
