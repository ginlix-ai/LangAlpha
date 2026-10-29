"""What the window has trimmed from the head of a message list: how many runs,
which messages, as one digest, and what a summary's index says of the newest
of them.

The window trims runs from the head of the main agent's messages (see
``compaction.window``) once the server's turn slices hold them, and from then
on those slices are where the trimmed messages are read back from. A trim
checks that the slices hold exactly the messages it drops, and a read back
checks that it got them, both against this digest rather than by counting
turns or runs, which a thread can have in more than one shape.

A message is keyed by what it says and the tool calls it makes or answers,
never by its id: one saved before ids were stable loads with a different id,
or none, from one checkpoint to the next. Order counts, but not among the
answers to one batch of parallel tool calls: each is its own task's write,
and which order a load replays a step's writes in is the saver's to choose
(checkpoint-postgres 3.2 changed it), so they are hashed in call-id order.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import Any

from langchain_core.messages import AnyMessage
from typing_extensions import NotRequired, TypedDict

from ptc_agent.agent.transcript.render import split_runs, turn_map

# The index lists the newest turns by request and one line stands for the
# rest, so a long thread does not grow every summary without bound.
_INDEX_LIMIT = 20
# Each request is copied from the checkpoint at every compaction, so the
# user's instructions keep their wording here while summaries paraphrase
# them; the cap only stops a paste from riding in every summary.
_REQUEST_CHARS = 1_000
_NO_REQUEST = "(no request)"

#: How a digest is hashed: 2 takes a batch of tool answers in call-id order,
#: 1 (a window trimmed before it) in the order they loaded in.
DIGEST_VERSION = 2


def _kind(message: AnyMessage) -> str:
    # A streamed message can be stored as its chunk class.
    return message.type.removesuffix("MessageChunk").lower()


def _feed(step: Any, message: AnyMessage) -> None:
    content = message.content
    text = content if isinstance(content, str) else json.dumps(
        content, sort_keys=True, default=str
    )
    kind = _kind(message)
    calls = [call.get("id") for call in getattr(message, "tool_calls", None) or ()]
    answers = getattr(message, "tool_call_id", None)
    data = text.encode("utf-8", "surrogatepass")
    # The text's length frames it, so it is hashed as it is rather than
    # escaped into the header: a transcript render hashes every message.
    step.update(json.dumps([kind, calls, answers, len(data)], default=str).encode())
    step.update(data)


def _chain(digest: str, messages: Iterable[AnyMessage]) -> str:
    for message in messages:
        step = hashlib.blake2b(digest.encode(), digest_size=16)
        _feed(step, message)
        digest = step.hexdigest()
    return digest


def _call_ordered(messages: Iterable[AnyMessage]) -> Iterator[AnyMessage]:
    """``messages`` with each batch of tool answers in call-id order."""
    batch: list[AnyMessage] = []
    for message in messages:
        if _kind(message) == "tool":
            batch.append(message)
            continue
        yield from sorted(batch, key=_call_id)
        batch.clear()
        yield message
    yield from sorted(batch, key=_call_id)


def _call_id(message: AnyMessage) -> str:
    return getattr(message, "tool_call_id", None) or ""


def extend(digest: str, messages: Iterable[AnyMessage]) -> str:
    """``digest`` carried on over ``messages``. A list's digest is its
    head's carried on over the rest, so a trim adds to the digest the trims
    before it left instead of needing what they dropped; a head ends where a
    run starts, never inside a batch of tool answers."""
    return _chain(digest, _call_ordered(messages))


class WindowState(TypedDict):
    """``Window`` as the checkpoint keeps it: ``requests`` is its index text,
    the last line being run ``runs``'s, a list since a checkpoint keeps no
    int keys."""

    runs: int
    messages: int
    digest: str
    requests: list[str]
    #: ``DIGEST_VERSION`` the digest was hashed under; absent, 1.
    version: NotRequired[int]


@dataclass(frozen=True)
class Window:
    """What the window has trimmed from the head of the main agent's
    messages. Every numbering of turns counts on from ``runs``, so a file
    name or an index line reads the same before and after a trim."""

    #: How many transcript runs were trimmed; the list's first is ``runs + 1``.
    runs: int = 0
    #: How many messages were trimmed, and their digest.
    messages: int = 0
    digest: str = ""
    #: The index text of the newest trimmed runs, by number.
    earlier: Mapping[int, str] = field(default_factory=dict)
    #: ``DIGEST_VERSION`` the digest was hashed under.
    version: int = DIGEST_VERSION

    @classmethod
    def of(cls, state: Mapping[str, Any]) -> Window:
        kept: WindowState | None = state.get("_window")
        if not kept:
            return cls()
        runs, requests = kept["runs"], kept["requests"]
        first = runs - len(requests) + 1
        return cls(
            runs,
            kept["messages"],
            kept["digest"],
            {first + i: line for i, line in enumerate(requests)},
            kept.get("version", 1),
        )

    def update(self) -> dict[str, WindowState]:
        """The state update that keeps it."""
        return {
            "_window": WindowState(
                runs=self.runs,
                messages=self.messages,
                digest=self.digest,
                requests=list(self.earlier.values()),
                version=self.version,
            )
        }

    def lines(self, requests: Mapping[int, str]) -> dict[int, str]:
        """The index text of each turn, by number: the trimmed ones' as
        kept, and the rest's from ``requests``, the text of the user message
        that opened each (``turn_map``)."""
        return {**self.earlier, **{n: _index_request(text) for n, text in requests.items()}}

    def extend(self, messages: Sequence[AnyMessage]) -> Window:
        """This window with ``messages`` trimmed after it, which run up to
        the start of a run. Hashes every one of them."""
        _, requests = turn_map(messages, base=self.runs)
        runs = self.runs + len(split_runs(messages))
        return Window(
            runs,
            self.messages + len(messages),
            self._extend(self.digest, messages),
            newest_lines(self.lines(requests), runs),
            self.version,
        )

    def holds(self, messages: Sequence[AnyMessage]) -> bool:
        """Whether ``messages`` are exactly the ones trimmed."""
        return len(messages) == self.messages and self._extend("", messages) == self.digest

    def extend_held(
        self, messages: Sequence[AnyMessage], held: Sequence[AnyMessage]
    ) -> Window | None:
        """This window with ``messages`` trimmed after it, if ``held`` is
        exactly its messages and then those, else None, hashed under
        ``DIGEST_VERSION``.

        A window hashed under an older digest is rehashed from ``held``
        unchecked: that digest took answers in the order they loaded, which
        a later load need not repeat, and what it trimmed lives only in the
        slices already. ``messages`` are still checked in full, where its
        count puts them.
        """
        earlier, later = held[: self.messages], held[self.messages :]
        if len(earlier) != self.messages or len(later) != len(messages):
            return None
        base = self
        if self.version != DIGEST_VERSION:
            base = replace(self, digest=extend("", earlier), version=DIGEST_VERSION)
        elif not self.holds(earlier):
            return None
        head = base.extend(messages)
        return head if extend(base.digest, later) == head.digest else None

    def _extend(self, digest: str, messages: Iterable[AnyMessage]) -> str:
        if self.version == DIGEST_VERSION:
            return extend(digest, messages)
        return _chain(digest, messages)


def newest_lines(lines: Mapping[int, str], last: int) -> dict[int, str]:
    """The index text of the newest turns up to ``last`` a summary lists,
    by number."""
    start = max(1, last - _INDEX_LIMIT + 1)
    return {n: lines.get(n, _NO_REQUEST) for n in range(start, last + 1)}


def _index_request(request: str) -> str:
    """What the index says of a turn opened by ``request``."""
    return _one_line(request) if request else _NO_REQUEST


def _one_line(text: str) -> str:
    from ptc_agent.agent.middleware.skills.content import skill_blocks_as_names

    line = " ".join(skill_blocks_as_names(text).split())
    if len(line) <= _REQUEST_CHARS:
        return line
    return line[: _REQUEST_CHARS - 1].rstrip() + "…"
