"""Threads that are not Claude Code threads keep their checkpoints exactly as they are.

The ``claude_session`` channel belongs to ``ClaudeCodeState`` alone. A checkpoint
written by a main-agent graph before the runtime existed must load to the same
values, and the thread must keep running, in a process that has imported the
runtime. Two fresh interpreters stand in for a deploy before and after.
"""

import json
import pickle
import subprocess
import sys

_GRAPH = """
from langchain_core.messages import AIMessage, HumanMessage
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import START, StateGraph
from ptc_agent.agent.main_state import MainAgentState

def reply(state):
    return {"messages": [AIMessage(f"reply {len(state['messages'])}")]}

def graph(saver):
    g = StateGraph(MainAgentState)
    g.add_node("model", reply)
    g.add_edge(START, "model")
    return g.compile(checkpointer=saver)

CONFIG = {"configurable": {"thread_id": "existing"}}
"""

_BEFORE = _GRAPH + """
import pickle, sys
saver = InMemorySaver()
graph(saver).invoke({"messages": [HumanMessage("first")]}, CONFIG)
plain = {name: {k: dict(v) for k, v in getattr(saver, name).items()} for name in ("storage", "writes")}
plain["blobs"] = dict(saver.blobs)
open(sys.argv[1], "wb").write(pickle.dumps(plain))
"""

_AFTER = _GRAPH + """
import json, pickle, sys
import ptc_agent.agent.claude_code.runtime as runtime
plain = pickle.loads(open(sys.argv[1], "rb").read())
saver = InMemorySaver()
for name in ("storage", "writes"):
    for k, v in plain[name].items():
        getattr(saver, name)[k].update(v)
saver.blobs.update(plain["blobs"])
g = graph(saver)
loaded = g.get_state(CONFIG).values
after = g.invoke({"messages": [HumanMessage("second")]}, CONFIG)
print("RESULT=" + json.dumps({
    "channels": sorted(g.channels),
    "loaded": [m.content for m in loaded["messages"]],
    "after": [m.content for m in after["messages"]],
    "claude_state_has_channel": "claude_session" in runtime.ClaudeCodeState.__annotations__,
}))
"""


def test_an_existing_threads_checkpoint_loads_and_runs_unchanged_beside_the_runtime(tmp_path):
    saved = tmp_path / "checkpoint.pickle"
    subprocess.run([sys.executable, "-c", _BEFORE, str(saved)], capture_output=True, timeout=300, check=True)
    assert pickle.loads(saved.read_bytes())["storage"]
    done = subprocess.run([sys.executable, "-c", _AFTER, str(saved)], capture_output=True, text=True, timeout=300, check=True)
    (line,) = [ln for ln in done.stdout.splitlines() if ln.startswith("RESULT=")]
    got = json.loads(line.removeprefix("RESULT="))
    assert "claude_session" not in got["channels"]
    assert got["loaded"] == ["first", "reply 1"]
    assert got["after"] == ["first", "reply 1", "second", "reply 3"]
    assert got["claude_state_has_channel"] is True
