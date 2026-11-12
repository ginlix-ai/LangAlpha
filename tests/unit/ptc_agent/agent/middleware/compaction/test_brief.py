"""The hand-off brief's bookkeeping: which reply it has to end on, where it
cuts the thread, and when a kept one still holds."""

from __future__ import annotations

from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from ptc_agent.agent.middleware.compaction.brief import (
    BRIEF_KEY,
    BRIEF_PREFIX,
    brief_cut,
    brief_due,
    brief_from,
    handoff_mark,
    kept,
    THREAD_PREFIX,
    stored_brief,
)
from ptc_agent.agent.middleware.compaction.types import CONTEXT_SUMMARY_PREFIX
from ptc_agent.agent.middleware.compaction.utils import build_summary_message
from src.llms.claude_code import MESSAGE_ID_KEY

CALL = {"name": "Bash", "args": {}, "id": "c1", "type": "tool_call"}


def _claude(text: str, id: str) -> AIMessage:
    return AIMessage(text, id=id, response_metadata={MESSAGE_ID_KEY: "msg_" + id})


def test_a_brief_is_due_only_when_another_model_wrote_the_last_reply():
    assert brief_due([HumanMessage("a"), AIMessage("b", id="other"), HumanMessage("c")]) == "other"
    assert brief_due([HumanMessage("a"), _claude("b", "mine"), HumanMessage("c")]) is None
    assert brief_due([HumanMessage("a")]) is None


def test_a_brief_cuts_after_the_last_reply_and_the_results_of_its_calls():
    thread = [HumanMessage("a"), AIMessage("", tool_calls=[CALL], id="r"),
              ToolMessage("out", tool_call_id="c1"), HumanMessage("new")]
    assert brief_cut(thread) == 3
    assert brief_cut([HumanMessage("a")]) == 0
    # A compaction that kept no reply after it: its summary holds the reply.
    assert brief_cut([build_summary_message("all of it", source="model"), HumanMessage("new")]) == 1


def test_a_kept_brief_holds_only_while_the_thread_still_ends_on_its_reply():
    brief = {"text": "t", "through": "other", "source": "self"}
    thread = [HumanMessage("a"), AIMessage("b", id="other"), HumanMessage("c")]
    assert stored_brief({"messages": thread, BRIEF_KEY: brief}) == brief
    later = [*thread, AIMessage("d", id="other-2"), HumanMessage("e")]
    assert stored_brief({"messages": later, BRIEF_KEY: brief}) is None
    assert stored_brief({"messages": [*thread, _claude("d", "mine")], BRIEF_KEY: brief}) is None


def test_a_brief_reads_as_a_hand_off_not_as_the_models_own_compaction():
    summary = build_summary_message("SUMMARY BODY", source="self")
    assert summary.content.startswith(CONTEXT_SUMMARY_PREFIX)
    brief = brief_from(summary, through="other")
    assert brief["text"].startswith(BRIEF_PREFIX + "SUMMARY BODY")
    assert CONTEXT_SUMMARY_PREFIX not in brief["text"]
    assert (brief["through"], brief["source"]) == ("other", "self")


def test_a_thread_handed_over_as_itself_reads_as_the_conversation_not_a_summary():
    summary = build_summary_message("Human: a\nAI: b", source="thread")
    brief = brief_from(summary, through="other", from_model="deepseek-flash")
    assert brief["text"].startswith(THREAD_PREFIX + "Human: a")
    assert (brief["source"], brief["from_model"]) == ("thread", "deepseek-flash")



def test_the_mark_keeps_a_written_brief_but_not_the_thread_handed_over_as_itself():
    """The user reads the thread above the mark, and it can run to half the
    reader's context, which every later write of the channel would copy."""
    written = brief_from(build_summary_message("the brief", source="model"), through="r1", from_model="m")
    assert handoff_mark(written, to="c")["props"]["summary_text"] == "the brief"
    itself = brief_from(build_summary_message("Human: a\nAI: b", source="thread"), through="r1", from_model="m")
    assert handoff_mark(itself, to="c")["props"] == {
        "from_model": "m", "to_model": "c", "source": "thread", "summary_text": None,
    }


def test_the_checkpoint_keeps_a_brief_of_the_thread_itself_without_its_text():
    thread = {"text": THREAD_PREFIX + "all of it", "through": "r", "source": "thread", "from_model": None, "room": 9}
    written = {"text": BRIEF_PREFIX + "summary", "through": "r", "source": "model", "from_model": "m"}
    assert kept(thread) == {"through": "r", "source": "thread", "from_model": None, "room": 9}
    assert kept(written) == written


def test_a_thread_compacted_down_to_its_summary_is_due_a_brief_through_that_summary():
    summary = build_summary_message("all of it", source="model")
    assert brief_due([summary, HumanMessage("new")]) == summary.id
    assert brief_due([summary, HumanMessage("new"), _claude("on it", "c1"), HumanMessage("more")]) is None
    assert brief_due([HumanMessage("new")]) is None


def test_a_reply_kept_from_before_messages_had_ids_is_still_due_a_brief():
    legacy = [HumanMessage("a"), AIMessage("b"), HumanMessage("c")]
    edited = [HumanMessage("a"), AIMessage("b, edited"), HumanMessage("c")]
    legacy[1].id = edited[1].id = None
    assert brief_due(legacy).startswith("message-1-") and brief_due(legacy) == brief_due(list(legacy))
    assert brief_due(edited) != brief_due(legacy)
    calls = [HumanMessage("a"), AIMessage("", tool_calls=[{**CALL, "args": {"command": "ls"}}]), HumanMessage("c")]
    other_calls = [HumanMessage("a"), AIMessage("", tool_calls=[{**CALL, "args": {"command": "pwd"}}]), HumanMessage("c")]
    calls[1].id = other_calls[1].id = None
    assert brief_due(calls) != brief_due(other_calls)
    ran = [HumanMessage("a"), AIMessage("", tool_calls=[CALL]), ToolMessage("one", tool_call_id="c1"), HumanMessage("c")]
    ran_again = [HumanMessage("a"), AIMessage("", tool_calls=[CALL]), ToolMessage("two", tool_call_id="c1"), HumanMessage("c")]
    ran[1].id = ran_again[1].id = None
    assert brief_due(ran) != brief_due(ran_again)


def test_a_reply_whose_calls_ran_again_after_a_rollback_is_due_another_brief():
    ran = [HumanMessage("a"), AIMessage("", tool_calls=[CALL], id="r"),
           ToolMessage("one", tool_call_id="c1", id="t1"), HumanMessage("c")]
    ran_again = [*ran[:2], ToolMessage("two", tool_call_id="c1", id="t2"), HumanMessage("c")]
    assert brief_due(ran).startswith("r-") and brief_due(ran) == brief_due(list(ran))
    assert brief_due(ran_again) != brief_due(ran)
