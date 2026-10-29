from __future__ import annotations

import asyncio

import pytest
from langchain_anthropic.chat_models import convert_to_anthropic_tool
from langchain_core.messages import ToolMessage
from langchain_core.tools import tool

from ptc_agent.agent.claude_code.tools_bridge import ToolResults, allow_rule, to_call_result, tool_specs


@tool("Read")
def read(file_path: str, limit: int = 100) -> str:
    """Read a file \u2026 up to limit lines."""
    return ""


def test_specs_carry_the_schema_and_description_a_langalpha_turn_sends():
    (spec,) = tool_specs([read], read_only=frozenset({"Read"}))
    wire = convert_to_anthropic_tool(read)
    assert (spec.name, spec.description, spec.input_schema) == (wire["name"], wire["description"], wire["input_schema"])
    assert spec.annotations.read_only_hint is True


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
    assert (await results.wait("t1")).content[0].text == "done"
    results._future("t2")
    results.fail_all("The turn ended before this call ran.")
    late = await results.wait("t2")
    assert late.is_error and late.content[0].text == "The turn ended before this call ran."


@pytest.mark.asyncio
async def test_a_call_that_arrives_after_close_fails_at_once():
    results = ToolResults()
    results.fail_all("The turn ended before this call ran.")
    after = await asyncio.wait_for(results.wait("t3"), 1)
    assert after.is_error and after.content[0].text == "The turn ended before this call ran."
