"""What a checkpointed HumanMessage is: input someone sent, or a stamp.

Replay and the transcript both open a turn (a task's run) on real input only,
so both classify here. Every stamp a writer puts on a HumanMessage must be
registered: ``plain`` is the fallback, so an unregistered stamp would open a
turn it only landed inside.
"""

from __future__ import annotations

from typing import Literal

from langchain_core.messages import AnyMessage, HumanMessage

HumanKind = Literal[
    "plain",
    "steering",
    "summarization",
    "market-watch",
    "credit-gate",
    "runtime-context",
    "orchestrator",
]

#: How mid-turn input opens: the user's, and an orchestrator's follow-up to a
#: background task.
STEERING_MARKERS = (
    "[Steering from User]\n",
    "[Follow-up Instructions from Orchestrator]\n",
)

# Written by middleware/market_watch.py: the lc_source tag and the content
# stamp prefix that mark a live-price injection.
_MARKET_WATCH_SOURCE = "market_watch"
_MARKET_WATCH_STAMP_OPEN = "<market-watch>"

# Written by src/server/utils/credit_resume_context.py: the record of
# gate-stopped background tasks injected when a credit-paused turn resumes.
_CREDIT_GATE_SOURCE = "credit_gate"

# Written by middleware/runtime_context/: the tail envelope carrier and the
# durable rows it renders.
_RUNTIME_CONTEXT_SOURCES = frozenset({"runtime_context", "runtime_update"})

# Written by middleware/background_subagent/orchestrator.py: the trigger that
# re-invokes the agent for pending steering, and background-task notices.
# Checkpoints from before the stamp carry only the message name.
ORCHESTRATOR_SOURCE = "orchestrator"
#: The steering trigger's whole text. It only routes the graph back to the
#: agent; the steering message that follows it carries the user's words.
STEERING_TRIGGER = "User sent additional instructions."


def human_kind(message: HumanMessage) -> HumanKind:
    """Classify a HumanMessage by its injection stamp; ``plain`` is real input."""
    kwargs = message.additional_kwargs or {}
    source = kwargs.get("lc_source")
    content = message.content if isinstance(message.content, str) else ""
    if source == _MARKET_WATCH_SOURCE or content.startswith(_MARKET_WATCH_STAMP_OPEN):
        # Both guards stay: the content-prefix check is the safety net for
        # any ephemeral stamp that reaches a message unstamped.
        return "market-watch"
    if source == "steering" or content.startswith(STEERING_MARKERS):
        return "steering"
    if source == "summarization":
        return "summarization"
    if source == _CREDIT_GATE_SOURCE:
        return "credit-gate"
    if source in _RUNTIME_CONTEXT_SOURCES:
        return "runtime-context"
    if source == ORCHESTRATOR_SOURCE or message.name == ORCHESTRATOR_SOURCE:
        return "orchestrator"
    return "plain"


def is_run_boundary_message(message: AnyMessage) -> bool:
    """A plain HumanMessage opens a turn, or a run in a task namespace (the
    spawn or resume input); stamped injections land mid-run and never open one."""
    return isinstance(message, HumanMessage) and human_kind(message) == "plain"
