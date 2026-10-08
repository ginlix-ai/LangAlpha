"""What a public share of a thread may carry of its stored turns.

A shared thread is readable by anyone holding the link, so the owner-only
marks come out of every query and event before it leaves the server. The
rule cannot live in the client once the payload has already left.
"""

from __future__ import annotations

import re
from typing import Any, Literal

from ptc_agent.agent.middleware.direct_mcp import METADATA_KEY
from ptc_agent.agent.middleware.order_governance import RECEIPT_KEY
from src.tools.secretary import SECRETARY_TOOLS
from src.tools.secretary.chief_of_staff import CHIEF_OF_STAFF_TOOLS
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
_DROPPED_EVENTS = frozenset({"provenance", "tool_call_chunks", "interrupt"})
_PRIVATE_ARTIFACT_KEYS = (RECEIPT_KEY, "provenance")
# The owner's own turn, and the resume that answers an approval names every
# order it decided by the attempt's ledger id. A viewer cannot answer one and
# must not read which of the owner's orders were approved.
_PRIVATE_QUERY_METADATA_KEYS = frozenset({"workspace_id", "order_decisions"})
# Keys naming the owner's account wherever they sit in an event: no share
# renders them, and none is the viewer's to read.
_OWNER_ID_KEYS = frozenset({"workspace_id", "user_id"})
# The one mark a direct MCP call carries before its answer: ``direct_tool_name``
# builds every direct tool name under this prefix, its digest forms included.
_DIRECT_TOOL_PREFIX = "mcp__"
# The account tools of the flash agent and the Chief of Staff list, create,
# delete and dispatch into the owner's workspaces and threads. Their answers
# are the owner's own rows as text, sandbox and user ids included, which no key
# strip reaches, and their arguments name the same things. A share renders none
# of these calls, so each travels as its name and id alone.
_SECRETARY_TOOL_NAMES = frozenset(
    t.name for t in (*SECRETARY_TOOLS, *CHIEF_OF_STAFF_TOOLS)
)
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
# What a tool's own answer becomes on a share, by the name of the call it
# answers.
_ANSWER_RULES: dict[str, Literal["blank", "scrub_user_id"]] = {
    **dict.fromkeys(_SECRETARY_TOOL_NAMES, "blank"),
    **dict.fromkeys(_USER_DATA_TOOL_NAMES, "scrub_user_id"),
}


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


def _without_user_id_pairs(text: str) -> str:
    """``text`` less every quoted ``user_id`` pair, with the comma it leaves."""
    return _USER_ID_PAIR_LAST.sub("", _USER_ID_PAIR_FIRST.sub("", text))


class ShareRedaction:
    """One shared thread's queries and stored events as a viewer may read them.

    Built from every stored event of the thread, then fed the events in stream
    order, since a call's name is known only once its ``tool_calls`` event has
    passed. One instance serves one replay.
    """

    def __init__(self, stored_events: list[dict[str, Any]]) -> None:
        self._order_calls = _order_call_ids(stored_events)
        self._cleared_calls = _cleared_call_ids(stored_events)
        # The latest name each call id was made under, None once a result has
        # answered it. A result names only its call id, which a provider may
        # repeat later, so this follows the stream rather than reading the
        # thread up front.
        self._call_names: dict[str, str | None] = {}

    def query(self, q: dict[str, Any]) -> tuple[Any, Any]:
        """A query row's text and metadata, less what a viewer must not read.

        A system query's text is the agent's own prompt, never shown, and a
        report-back names the dispatched thread and its workspace.
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
        content = "" if q.get("type") == "system" else q.get("content")
        return content, metadata

    def event(self, event_type: str, data: dict[str, Any]) -> dict[str, Any] | None:
        """An event's data as a viewer may read it, or None when no share sends it.

        Returns a copy, so the stored or cached event is never changed.
        """
        if event_type in _DROPPED_EVENTS:
            return None
        # Stored workspace_status events carry the workspace id at the top
        # level, tool artifacts (chart annotations) nest it, and a steering
        # event names the user on each message, so the owner's ids go at every
        # depth. sandbox_state is server-side runtime state.
        data = without_keys(data, _OWNER_ID_KEYS)
        data.pop("sandbox_state", None)
        if event_type == "tool_calls":
            self._tool_calls(data)
        elif event_type == "tool_call_result":
            self._tool_call_result(data)
        return data

    def _tool_calls(self, data: dict[str, Any]) -> None:
        """Note each call's name, and empty the arguments of each possible
        order call and each secretary call.

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
                if call.get("id"):
                    self._call_names[str(call["id"])] = (
                        name if isinstance(name, str) else ""
                    )
                if name in _SECRETARY_TOOL_NAMES or self._may_be_order(
                    call, data.get("id")
                ):
                    call = {**call, "args": {}}
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
        content = data.get("content")
        if rule == "blank":
            data["content"] = ""
        elif rule == "scrub_user_id" and isinstance(content, str):
            data["content"] = _without_user_id_pairs(content)
