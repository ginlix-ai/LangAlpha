"""The bounds every agent run shares, and the marks the loop guard reads.

LangGraph ends a run after ``recursion_limit`` supersteps, and a step count is a
poor bound on an agent's work: one round costs a superstep for each
``before_model`` hook as well as the model call and the tools, so the budget in
rounds shrank each time a hook was added, until real turns were cut off
mid-report. A turn is bounded by its wall clock and its loop guard instead, so
this only has to be a number LangGraph accepts and a turn never reaches.
"""

import re
from typing import Any

UNBOUNDED_RECURSION_LIMIT = 1_000_000_000

# Where a refused call's ToolMessage says it was refused, and by what ("repeat",
# "poll", "time_up"). On the message rather than in its words, because a
# tool's output is anyone's text: a command's stdout, an MCP tool's reply or a
# fetched page can start with the refusal's words, and a guard that skipped
# those would let that output hide its own polling loop.
REFUSAL_KEY = "refusal"

# How the repeat refusal began before refusals were tagged. Frozen on purpose:
# rows persisted then carry this text, whatever the refusal says now.
_UNTAGGED_REFUSAL_HEAD = "Not run: this exact call has returned the same result "


def is_refusal(message: Any) -> bool:
    """Whether a tool result is the harness refusing the call, not the tool's answer."""
    kwargs = getattr(message, "additional_kwargs", None) or {}
    if kwargs.get(REFUSAL_KEY):
        return True
    # A row from before the tag: a refusal was always an error, while tool
    # output that merely starts with the same words comes back a success.
    content = getattr(message, "content", None)
    return (
        getattr(message, "status", None) == "error"
        and isinstance(content, str)
        and content.startswith(_UNTAGGED_REFUSAL_HEAD)
    )


# Set on the artifact of a status read that found its work still running:
# TaskOutput on a live task, ThreadStatus listing running work. The text
# cannot say it, since a read of live work never reads the same twice
# (elapsed time, growing output). The artifact never reaches the model, and
# the checkpoint and result eviction both keep it.
PENDING_READ_KEY = "pending_read"


def pending_read(artifact: dict[str, Any] | None = None) -> dict[str, Any]:
    """``artifact`` marked as a read that found its work still running."""
    return {**(artifact or {}), PENDING_READ_KEY: True}


def is_pending_read(message: Any) -> bool:
    artifact = getattr(message, "artifact", None)
    return isinstance(artifact, dict) and artifact.get(PENDING_READ_KEY) is True


# What ``la threads output`` writes to stderr, after ``Thread <id> ``, when the
# thread is still running, with ``--json`` too; ``STILL_RUNNING`` in
# cli/src/commands/threads.ts, which a unit test holds to this. A command's job
# merges stderr into the output the tool returns, so the line reaches the tool
# even when stdout is piped elsewhere.
LA_STILL_RUNNING = "is still running; this is its text so far."
_LA_RUNNING_LINE = re.compile(rf"^Thread \S+ {re.escape(LA_STILL_RUNNING)}$", re.MULTILINE)


def read_a_running_thread(output: str) -> bool:
    """Whether a command's output shows its ``la`` read a thread still running.

    Read from the text, unlike the other marks: `la` is a program the
    command runs, so its output is all the tool has of it. Text
    that only quotes the line marks the command a poll too, which costs at
    most an early refusal after three such rounds in a row.
    """
    return _LA_RUNNING_LINE.search(output) is not None
