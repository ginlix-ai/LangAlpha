"""What a checkpointed HumanMessage is: input someone sent, or a stamp.

Replay and the transcript both open a turn (a task's run) on real input only,
so both classify here. Every stamp a writer puts on a HumanMessage must be
registered: ``plain`` is the fallback, so an unregistered stamp would open a
turn it only landed inside.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType
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
    "empty-call-retry",
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

# How middleware/compaction/ opened a summary before it said which side wins a
# disagreement, as summaries already in checkpoints still do: all a summary
# written before the lc_source stamp has to be known by, and an opening
# parse_summary_message strips like the current one.
LEGACY_SUMMARY_PREFIX = (
    "[Context Summary]\n"
    "This session is being continued from a previous conversation "
    "that ran out of context. The conversation is summarized below:\n\n"
)

# Written by middleware/runtime_context/: the tail envelope carrier and the
# durable rows it renders.
_RUNTIME_CONTEXT_SOURCES = frozenset({"runtime_context", "runtime_update"})

#: Written by middleware/tool/empty_call_retry.py: the remediation hint a
#: retry sends after an answer that meant to call a tool and carried none.
EMPTY_CALL_RETRY_SOURCE = "empty_call_retry"

# Written by middleware/background_subagent/orchestrator.py: the trigger that
# re-invokes the agent for pending steering, and background-task notices.
# Checkpoints from before the stamp carry only the message name.
ORCHESTRATOR_SOURCE = "orchestrator"
#: The steering trigger's whole text for the user's steers, and every
#: trigger's text before it named the sender. It only routes the graph back to
#: the agent; the steering message that follows it carries the words.
STEERING_TRIGGER = "User sent additional instructions."
#: The trigger when all that is waiting is a market target's notice, which
#: nobody sent.
MARKET_TARGET_TRIGGER = "A market alert arrived."
#: The trigger when a wakeup's notice is waiting.
WAKE_TRIGGER = "A wakeup arrived."
#: The senders a message checker names for notices nobody sent: a market
#: target's alert, and a wakeup's notice.
MARKET_ALERT_SENDER = "market_alert"
WAKEUP_SENDER = "wakeup"
#: The trigger for each of those senders, the one that wins first when
#: several wait: a wakeup's trigger covers a market alert taken in beside it.
REENTRY_TRIGGERS: Mapping[str, str] = MappingProxyType(
    {WAKEUP_SENDER: WAKE_TRIGGER, MARKET_ALERT_SENDER: MARKET_TARGET_TRIGGER}
)
_NOTICE_TRIGGERS = frozenset(REENTRY_TRIGGERS.values())
_TRIGGER_TAIL = " sent additional instructions."
_TRIGGER_NAMES = {"user": "User", "chief_of_staff": "Chief of Staff"}


def reentry_trigger(senders: list[str]) -> str:
    """The trigger that routes the graph back to the agent for what waits.

    Notices nobody sent have no sender to name, so they take their own
    trigger; any steer among them names its sender.
    """
    if senders and all(sender in REENTRY_TRIGGERS for sender in senders):
        return next(t for s, t in REENTRY_TRIGGERS.items() if s in senders)
    return steering_trigger(senders)


def steering_trigger(senders: list[str]) -> str:
    """The trigger naming who sent the steers waiting, in the order given."""
    names = [
        _TRIGGER_NAMES.get(sender) or sender.replace("_", " ").title()
        for sender in dict.fromkeys(senders or ["user"])
    ]
    return " and ".join(names) + _TRIGGER_TAIL


def is_steering_trigger(content: object) -> bool:
    return isinstance(content, str) and (
        content.endswith(_TRIGGER_TAIL) or content in _NOTICE_TRIGGERS
    )


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
    if source == EMPTY_CALL_RETRY_SOURCE:
        return "empty-call-retry"
    return "plain"


def is_run_boundary_message(message: AnyMessage) -> bool:
    """A plain HumanMessage opens a turn, or a run in a task namespace (the
    spawn or resume input); stamped injections land mid-run and never open one."""
    return isinstance(message, HumanMessage) and human_kind(message) == "plain"


def is_summary_message(message: AnyMessage) -> bool:
    """A compaction summary, which heads the view after a compaction.

    One written before summaries were stamped is known by its opening alone,
    so the next compaction keeps it whole rather than trimming it as input.
    """
    if not isinstance(message, HumanMessage):
        return False
    content = message.content if isinstance(message.content, str) else ""
    return human_kind(message) == "summarization" or content.startswith(LEGACY_SUMMARY_PREFIX)
