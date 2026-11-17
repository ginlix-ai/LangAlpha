"""What a public share of a thread may carry of its stored turns.

A shared thread is readable by anyone holding the link, so the owner-only
marks come out of every query and event before it leaves the server. The
rule cannot live in the client once the payload has already left.
"""

from __future__ import annotations

import json
import re
from typing import Any, Literal

from ptc_agent.agent.middleware.direct_mcp import METADATA_KEY
from ptc_agent.agent.middleware.order_governance import RECEIPT_KEY
from src.server.contracts.turn_kinds import NOTIFICATION_QUERY_TYPES
from src.tools.secretary._commands import HANDOFF_TOOL_NAME
from src.tools.wakeups.quiet import TOOL_NAME as WAKEUP_TOOL_NAME
from src.utils.nested import without_keys

# Five things a turn carries name the owner's brokerage account: the provenance
# record of a direct tool call, an order call's own arguments, the order
# receipt stamped on that call's artifact, the vendor's own answer to an order
# call, and the verdict the owner's resume recorded against the order it
# answered. None is rendered for a viewer, so none is sent.
#
# ``tool_call_chunks`` go whole: they are the arguments again, streamed in
# pieces that often carry no call id to match, and no replay reads them. An
# interrupt asks the owner, and no share renders or answers one.
#
# Exception text can name a provider's organization, an internal address or a
# sandbox, and scrubbing credentials from it removes none of those. A turn's
# closing ``error`` event is nothing but that text, so it goes whole; a run's
# lifecycle frame and a model fallback keep their status and lose the text.
# No share renders either.
#
# A wait for a Claude Code CLI names the owner's computer, how many agents
# they ran on it and how to raise that; no share renders it.
_DROPPED_EVENTS = frozenset({"provenance", "tool_call_chunks", "interrupt", "error", "claude_code_queue"})
_ERROR_TEXT_EVENTS = frozenset({"workflow_lifecycle", "model_fallback"})
_PRIVATE_ARTIFACT_KEYS = (RECEIPT_KEY, "provenance")
# The owner's own turn, and the resume that answers an approval names every
# order it decided by the attempt's ledger id. A viewer cannot answer one and
# must not read which of the owner's orders were approved. A report-back's
# source names the dispatched thread and its workspace.
_PRIVATE_QUERY_METADATA_KEYS = frozenset(
    {"workspace_id", "order_decisions", "report_source"}
)
# A wakeup on an order names the order and, once it settles, the fill. A
# share keeps each notice's label and kind, which name what fired, and drops
# the order wherever the notice sits: a wake turn's query metadata and the
# entry a running turn took it in by. The agent's own ``schedule_wakeup``
# calls name the order too, by its id in the arguments, in the answer's
# listing and in the rows its artifact lists as set or cancelled, so those go
# as an order call's do.
_WAKEUP_ORDER_KEYS = frozenset({"order"})
# Keys naming the owner's account wherever they sit in an event: no share
# renders them, and none is the viewer's to read.
_OWNER_ID_KEYS = frozenset({"workspace_id", "user_id"})
# The one mark a direct MCP call carries before its answer: ``direct_tool_name``
# builds every direct tool name under this prefix, its digest forms included.
_DIRECT_TOOL_PREFIX = "mcp__"
# The Chief of Staff's account tools list, create, delete and dispatch into the
# owner's workspaces and threads. Their answers are the owner's own rows as
# text, sandbox and user ids included, which no key strip reaches, and their
# arguments name the same things. A share renders none of these calls, so each
# travels as its name and id alone. Only the hand-off is still a tool; the
# others, and the retired Flash agent's, went under these names, and threads
# that called them are still shared, so they stay spelled out here.
_EARLIER_SECRETARY_TOOL_NAMES = frozenset(
    {"manage_workspaces", "ptc_agent", "agent_output", "manage_threads"}
)
# A Bash or ExecuteCode call that may run the `la` CLI goes as one too: the
# Chief of Staff manages the same rows through it now. No tool can be named
# this, so the kind never meets a real call's name.
_LA_RUN = "\x00la"
_SECRETARY_TOOL_NAMES = _EARLIER_SECRETARY_TOOL_NAMES | {HANDOFF_TOOL_NAME, _LA_RUN}
# The retired user data tools answered with raw rows until they dropped the
# owner's user_id, and threads stored before then still spell it out in the
# answer text, as a repr or JSON pair no key strip reaches. The tools are gone
# but those threads are not, so the names stay spelled out here. Another
# tool's answer is its own content, so only theirs is rewritten.
_USER_DATA_TOOL_NAMES = frozenset(
    {"get_user_data", "update_user_data", "remove_user_data"}
)
_USER_ID_PAIR = r"""(["'])user_id\1:\s*(["'])[^"'\\]*\2"""
_USER_ID_PAIR_FIRST = re.compile(_USER_ID_PAIR + r",\s*")
# The comma is matched whole or not at all: ``,?\s*`` lets a run of spaces
# start a match at each of its own positions, quadratic in a long note.
_USER_ID_PAIR_LAST = re.compile(r"(?:,\s*)?" + _USER_ID_PAIR)
# What a tool's own answer becomes on a share, by the kind of the call it
# answers.
_ANSWER_RULES: dict[str, Literal["blank", "scrub_user_id"]] = {
    **dict.fromkeys(_SECRETARY_TOOL_NAMES, "blank"),
    **dict.fromkeys(_USER_DATA_TOOL_NAMES, "scrub_user_id"),
    WAKEUP_TOOL_NAME: "blank",
}
# A command or code run that calls `la` answers with the rows the retired
# account tools did, and its own text names the same ids. The match is loose
# on purpose: the word `la` or `langalpha`, bare, escaped past an alias
# (`\la`) or ending a path, wherever a shell or a string literal could start a
# program, and ending at a redirection (`la</dev/null`) or at an escaped quote,
# as in arguments the stream could not parse (`[\"la\", ...]`). A command that
# only mentions the word loses its text on a share; a real call left out would
# publish another thread's output.
_COMMAND_ARG = {"Bash": "command", "ExecuteCode": "code"}
_RUNS_LA = re.compile(
    r"""(?:^|[\s;&|(`$"'=])(?:[^\s;&|()`$"'=]*/)?\\?(?:la|langalpha)(?=$|[\s;&|()`"'<>\\])"""
)


def _runs_la(name: Any, args: Any) -> bool:
    """Whether a Bash or ExecuteCode call may run the `la` CLI.

    Arguments the stream could not parse arrive as text, which still says
    what ran.
    """
    arg = _COMMAND_ARG.get(name) if isinstance(name, str) else None
    if arg is None:
        return False
    text = args.get(arg) if isinstance(args, dict) else args
    return isinstance(text, str) and _RUNS_LA.search(text) is not None


def _is_order_result(data: dict[str, Any]) -> bool:
    """Whether a tool result answers an order call.

    The stamp marks the call, not the receipt: a ledger write that failed
    leaves no receipt and the same answer.
    """
    artifact = data.get("artifact")
    if not isinstance(artifact, dict):
        return False
    stamp = artifact.get(METADATA_KEY)
    return (
        isinstance(stamp, dict) and isinstance(stamp.get("order"), dict)
    ) or RECEIPT_KEY in artifact


def _order_call_ids(events: list[dict[str, Any]]) -> set[str]:
    """The id of every order call in a thread, read before any event is sent.

    A call is streamed before anything names it an order, and an approval ends
    its turn, so the result that names it can sit in a later turn than the call.
    """
    found: list[Any] = []
    for item in events:
        data = item.get("data")
        if not isinstance(data, dict):
            continue
        if item.get("event") == "tool_call_result" and _is_order_result(data):
            found.append(data.get("tool_call_id"))
        elif item.get("event") == "interrupt" and isinstance(
            data.get("action_requests"), list
        ):
            found.extend(
                r.get("tool_call_id")
                for r in data["action_requests"]
                if isinstance(r, dict) and "attempt_id" in r
            )
    return {str(i) for i in found if i}


def _cleared_call_ids(events: list[dict[str, Any]]) -> set[tuple[str, str]]:
    """Every call, as message id and call id, whose answer shows it was not an order.

    Only a direct tool's answer carries the binder's stamp, so it is the one
    proof a direct call never touched an order. An answer names only its call
    id, which a provider may repeat in a later message, so it clears the last
    message before it to make that call, however many turns back: only the main
    agent holds direct tools, and it is not asked again until a message's calls
    are answered.
    """
    made_by: dict[str, Any] = {}
    found: set[tuple[str, str]] = set()
    for item in events:
        data = item.get("data")
        if not isinstance(data, dict):
            continue
        if item.get("event") == "tool_calls" and isinstance(
            data.get("tool_calls"), list
        ):
            for call in data["tool_calls"]:
                if isinstance(call, dict) and call.get("id"):
                    made_by[str(call["id"])] = data.get("id")
            continue
        if item.get("event") != "tool_call_result":
            continue
        artifact = data.get("artifact")
        if not isinstance(artifact, dict) or RECEIPT_KEY in artifact:
            continue
        stamp = artifact.get(METADATA_KEY)
        call_id = data.get("tool_call_id")
        message_id = made_by.get(str(call_id))
        # The binder stamps ``order`` null on a call that does nothing to one.
        # The stream writes "unknown" for a message that came with no id.
        if (
            call_id
            and message_id not in (None, "", "unknown")
            and isinstance(stamp, dict)
            and stamp.get("order") is None
        ):
            found.add((str(message_id), str(call_id)))
    return found


def _wakeup_args(args: Any) -> Any:
    """A ``schedule_wakeup`` call's arguments less the order each trigger names.

    The triggers stay listed, as the view reads a call that sets one as a
    change. A list sent as JSON text is read as the tool reads it, and one
    that cannot be read keeps a single empty trigger in its place.
    """
    if not isinstance(args, dict) or "triggers" not in args:
        return args
    triggers = args["triggers"]
    if isinstance(triggers, str):
        try:
            triggers = json.loads(triggers)
        except ValueError:
            triggers = None
    if not isinstance(triggers, list):
        return {**args, "triggers": [{}]}
    return {**args, "triggers": without_keys(triggers, _WAKEUP_ORDER_KEYS)}


def _without_user_id_pairs(text: str) -> str:
    """``text`` less every quoted ``user_id`` pair, with the comma it leaves."""
    return _USER_ID_PAIR_LAST.sub("", _USER_ID_PAIR_FIRST.sub("", text))


class ShareRedaction:
    """One shared thread's queries and replayed events as a viewer may read them.

    Built from every event of the thread's replay, then fed the events in
    stream order, since a call's name is known only once its ``tool_calls``
    event has passed. One instance serves one replay.
    """

    def __init__(self, events: list[dict[str, Any]]) -> None:
        self._order_calls = _order_call_ids(events)
        self._cleared_calls = _cleared_call_ids(events)
        # The latest kind each call id was made as, None once a result has
        # answered it. A result names only its call id, which a provider may
        # repeat later, so this follows the stream rather than reading the
        # thread up front.
        self._call_names: dict[str, str | None] = {}

    def query(self, q: dict[str, Any]) -> tuple[Any, Any]:
        """A query row's text and metadata, less what a viewer must not read.

        A notification turn's text is the agent's own prompt, never shown,
        and a wake's names any order it fired on; a report-back's metadata
        names the dispatched thread and its workspace.
        """
        metadata = q.get("metadata") or {}
        if isinstance(metadata, dict):
            # Attached context is client-shaped, so an owner id can sit at any
            # depth in it.
            metadata = without_keys(
                {
                    k: v
                    for k, v in metadata.items()
                    if k not in _PRIVATE_QUERY_METADATA_KEYS
                },
                _OWNER_ID_KEYS,
            )
            if "wakeup" in metadata:
                metadata["wakeup"] = without_keys(
                    metadata["wakeup"], _WAKEUP_ORDER_KEYS
                )
        content = (
            "" if q.get("type") in NOTIFICATION_QUERY_TYPES else q.get("content")
        )
        return content, metadata

    def event(self, event_type: str, data: dict[str, Any]) -> dict[str, Any] | None:
        """An event's data as a viewer may read it, or None when no share sends it.

        Returns a copy, so the stored or cached event is never changed.
        """
        if event_type in _DROPPED_EVENTS:
            return None
        # An archived workspace_status event a legacy turn replays verbatim
        # carries the workspace id at the top level, tool artifacts (chart
        # annotations) nest it, and a steering event names the user on each
        # message, so the owner's ids go at every depth. sandbox_state is
        # server-side runtime state.
        data = without_keys(data, _OWNER_ID_KEYS)
        data.pop("sandbox_state", None)
        if event_type == "steering_delivered":
            # A report a live turn took in names its source as a report
            # turn's query metadata does; a share keeps neither.
            data = without_keys(data, {"report_source"})
            if isinstance(data.get("messages"), list):
                data["messages"] = [
                    {**m, "wakeup": without_keys(m["wakeup"], _WAKEUP_ORDER_KEYS)}
                    if isinstance(m, dict) and "wakeup" in m
                    else m
                    for m in data["messages"]
                ]
        if event_type in _ERROR_TEXT_EVENTS:
            data.pop("error", None)
        if event_type == "tool_calls":
            self._tool_calls(data)
        elif event_type == "tool_call_result":
            self._tool_call_result(data)
        return data

    def _tool_calls(self, data: dict[str, Any]) -> None:
        """Note each call's kind, and empty the arguments of each possible
        order call and each secretary call, an `la` run among them.

        Emptied rather than dropped, as the stream does for arguments it cannot
        parse: the call keeps its card and its id, so its result still lands.
        """
        calls = data.get("tool_calls")
        if not isinstance(calls, list):
            return
        shown = []
        for call in calls:
            if isinstance(call, dict):
                name = call.get("name")
                kind = _LA_RUN if _runs_la(name, call.get("args")) else name
                if call.get("id"):
                    self._call_names[str(call["id"])] = (
                        kind if isinstance(kind, str) else ""
                    )
                if kind in _SECRETARY_TOOL_NAMES or self._may_be_order(
                    call, data.get("id")
                ):
                    call = {**call, "args": {}}
                elif kind == WAKEUP_TOOL_NAME:
                    call = {**call, "args": _wakeup_args(call.get("args"))}
            shown.append(call)
        data["tool_calls"] = shown

    def _may_be_order(self, call: dict[str, Any], message_id: Any) -> bool:
        """Whether a call's arguments may name an order, and so stay off a shared thread.

        Fails closed on a direct call: an order stopped, or lost with its worker,
        before the vendor answered leaves no result and no approval card to mark
        it. So a direct call keeps its arguments only once an answer clears it,
        and a stopped one that was not an order shows none either.
        """
        if call.get("id") in self._order_calls:
            return True
        name = call.get("name")
        return (
            isinstance(name, str)
            and name.startswith(_DIRECT_TOOL_PREFIX)
            and (message_id, call.get("id")) not in self._cleared_calls
        )

    def _tool_call_result(self, data: dict[str, Any]) -> None:
        """The one place that decides what a tool's answer shows."""
        artifact = data.get("artifact")
        if isinstance(artifact, dict):
            if _is_order_result(data):
                # The vendor's own answer to an order names the account and
                # the fill.
                data["content"] = ""
            if any(key in artifact for key in _PRIVATE_ARTIFACT_KEYS):
                data["artifact"] = {
                    k: v
                    for k, v in artifact.items()
                    if k not in _PRIVATE_ARTIFACT_KEYS
                }
        # An answer with no call waiting on its id comes from a tool no one can
        # name, so it shows nothing: a second answer with no call named in
        # between (the stream drops a call that repeats an id earlier in its
        # run), one whose call never reached the stored stream (a frame the
        # salvage of a lost run could not read), and one naming no call.
        call_name: str | None = None
        if call_id := data.get("tool_call_id"):
            call_name = self._call_names.get(str(call_id))
            self._call_names[str(call_id)] = None
        rule = "blank" if call_name is None else _ANSWER_RULES.get(call_name)
        # An answer no call can be named for may be a wakeup's too.
        if call_name in (None, WAKEUP_TOOL_NAME) and isinstance(data.get("artifact"), dict):
            data["artifact"] = without_keys(data["artifact"], _WAKEUP_ORDER_KEYS)
        content = data.get("content")
        if rule == "blank":
            data["content"] = ""
        elif rule == "scrub_user_id" and isinstance(content, str):
            data["content"] = _without_user_id_pairs(content)
