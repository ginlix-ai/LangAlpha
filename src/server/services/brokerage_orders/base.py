"""The adapter contract, the vendor lookup, and the parts of a result no vendor owns.

A refusal and a transport error look the same at every vendor because neither
came from one: the relay wrote the first and the tool runtime the second. Both
are settled here so an adapter only ever reads its own envelope.
"""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, ClassVar

from src.server.services.brokerage_orders.models import (
    AttemptStatus,
    BrokerOrder,
    Failure,
    InstrumentRef,
    OrderOutcome,
)

# What the consent gate and the order gate put in a ToolMessage when a call
# is not permitted.
REFUSAL_PREFIX = "Refused:"

#: Every shape a tool result reaches us in: the mapping, the JSON text of one,
#: or the list of content blocks LangChain wraps it in -- and that list both as
#: a real list and as its JSON text. :func:`body_parts` is what reads all four.
ResultBody = str | Mapping[str, Any] | Sequence[Any] | None


class StatusReadError(Exception):
    """A status read the vendor did not answer.

    Distinct from an empty list on purpose: "the vendor has never heard of this
    order" is a fact reconciliation acts on, and a read that failed is not that
    fact. Conflating them would settle a live order as lost because a token
    expired.
    """


class ListingIncomplete(Exception):
    """A vendor that claims more orders but names no next page to read.

    Raised from :meth:`OrderAdapter.next_page`. The pages already read still
    count, so an order found in them settles its attempt; what is lost is the
    right to read absence as evidence. That is why this is not a
    :class:`StatusReadError`, which discards the read whole.
    """


@dataclass(frozen=True)
class StatusQuery:
    """One read call that reports what the vendor has done with an order."""

    tool: str
    args: dict[str, Any] = field(default_factory=dict)

    def key(self) -> tuple:
        """Identity for de-duplication, so one read serves every attempt it covers."""
        return (self.tool, tuple(sorted((k, str(v)) for k, v in self.args.items())))


@dataclass(frozen=True)
class MatchKey:
    """One order's identity as named fields, for pairing a call with a listing.

    Compared field by field rather than as one joined string, because the two
    sides do not always know the same fields. A vendor defaults a field the
    request left out and then lists its own value for it, and a book nulls a
    field the request did set; both are the same thing, one side knowing
    something the other never said. So a field absent on either side takes no
    part in the comparison, while a field both sides carry still tells two
    orders apart.

    An adapter builds a key only once it holds the fields that make two rows
    different orders rather than one, and answers None otherwise. A key
    therefore always carries those, and the comparison never rests on the
    optional few alone.
    """

    fields: tuple[tuple[str, str], ...]

    @classmethod
    def of(cls, **fields: str | None) -> MatchKey:
        """A key over the fields whose values are known, dropping the rest.

        An empty value is an absent one: a normalizer already spells "the
        vendor computes this one" and "the request never said it" the same
        blank way.
        """
        return cls(tuple(sorted((name, v) for name, v in fields.items() if v)))

    def matches(self, other: MatchKey | None) -> bool:
        """Whether the two agree on every field both of them name."""
        if other is None:
            return False
        mine, theirs = dict(self.fields), dict(other.fields)
        shared = mine.keys() & theirs.keys()
        return bool(shared) and all(mine[name] == theirs[name] for name in shared)


@dataclass(frozen=True)
class VendorOrder:
    """One order as the vendor's own list reports it.

    ``match_key`` is what pairs this row with an attempt the host never got an
    id for: the same field map an adapter derives from the request it sent, so
    the two are comparable without either side inventing a shared field.

    ``instrument`` is what the listing says the order is *for*, which is not
    always what the request could say: a brokerage that takes an opaque
    contract id and nothing else first names the symbol here. None whenever the
    listing adds nothing, which is the common case.
    """

    vendor_order_id: str
    outcome: OrderOutcome
    match_key: MatchKey | None = None
    placed_at: datetime | None = None
    instrument: InstrumentRef | None = None


class OrderAdapter(ABC):
    """One vendor's translation between its order tools and the neutral shapes."""

    vendor: ClassVar[str]

    def parse_result(
        self,
        tool: str,
        body: ResultBody,
        *,
        tool_status: str,
        request: BrokerOrder | None = None,
    ) -> OrderOutcome:
        """The vendor's answer, mapped onto the attempt lifecycle.

        The body is decoded once and everything no vendor owns is settled here,
        so :meth:`vendor_outcome` is only ever handed a call the vendor itself
        answered. ``tool_status`` is the ToolMessage status, "success" or
        "error".
        """
        envelope, text = body_parts(body)
        settled = _before_vendor(envelope, text, tool_status=tool_status, adapter=self)
        if settled is not None:
            return settled
        return self.vendor_outcome(tool, envelope, text, request)

    @abstractmethod
    def parse_request(self, tool: str, args: Mapping[str, Any]) -> BrokerOrder | None:
        """The order this call places, changes or cancels, or None if it is not one."""

    @abstractmethod
    def vendor_outcome(
        self,
        tool: str,
        envelope: dict[str, Any] | None,
        text: str,
        request: BrokerOrder | None,
    ) -> OrderOutcome:
        """This vendor's own answer, from the decoded body.

        ``envelope`` is None when the body was not JSON, which several vendors
        answer with. ``request`` is the parsed call, which is how route tokens
        the vendor never echoes reach the outcome.
        """

    @abstractmethod
    def status_query(
        self,
        order: BrokerOrder | None,
        *,
        vendor_order_id: str | None = None,
        route: Mapping[str, str] | None = None,
    ) -> StatusQuery | None:
        """The read call that reports this order's state, or None if there is none.

        Never an order tool: reconciliation reads, and a vendor with no read
        for this order's book answers None rather than a call that mutates one.
        """

    @abstractmethod
    def parse_status(self, query: StatusQuery, body: ResultBody) -> list[VendorOrder]:
        """The orders one status read returned, empty when it returned none.

        Takes the query as well as the answer because a vendor answers with
        less than it was asked: moomoo's paper list stamps a different market
        integer than the one a cancel has to send back.

        Raises :class:`StatusReadError` when the vendor did not answer at all.
        """

    def next_page(self, query: StatusQuery, body: ResultBody) -> StatusQuery | None:
        """The read that continues this one, or None when the vendor listed all it has.

        Absence is only evidence once the whole list has been read, so a vendor
        that pages has to say so here; the default is a vendor that answers in
        one page. Raises :class:`ListingIncomplete` when the vendor says it has
        more and names no page to ask for, which is neither the end of the list
        nor a read that can go on.
        """
        return None

    @abstractmethod
    def match_key(self, order: BrokerOrder | None) -> MatchKey | None:
        """The field map of an order we sent, comparable with a listed one.

        None when the request carries too little to identify itself, which is
        the honest answer: an attempt with no id and no key stays unresolved
        rather than being paired with a guess.
        """

    @abstractmethod
    def vendor_error(
        self, envelope: Mapping[str, Any] | None, text: str
    ) -> Failure | None:
        """The vendor's own refusal, when it sent one on the tool-error channel.

        A tool error is the transport's word at most brokerages, and that is
        what it stays here unless the adapter recognizes its vendor speaking:
        IBKR refuses an instruction with an error object and Robinhood with
        prose, and neither means the order never reached the venue.
        """


def adapter_for(vendor: str) -> OrderAdapter | None:
    """The adapter for a vendor key, or None where orders are not modeled yet."""
    # Imported here rather than at module scope: the vendor modules import this
    # one for the contract, so the registry has to resolve after they load.
    from src.server.services.brokerage_orders.ibkr import IbkrOrderAdapter
    from src.server.services.brokerage_orders.moomoo import MoomooOrderAdapter
    from src.server.services.brokerage_orders.robinhood import RobinhoodOrderAdapter

    match (vendor or "").strip().casefold():
        case "moomoo":
            return MoomooOrderAdapter()
        case "robinhood":
            return RobinhoodOrderAdapter()
        case "ibkr":
            return IbkrOrderAdapter()
        case _:
            return None


def _block_text(block: Any) -> str:
    if isinstance(block, str):
        return block
    if isinstance(block, Mapping) and block.get("type") == "text":
        return str(block.get("text") or "")
    return ""


def _content_blocks(
    blocks: Sequence[Any], fallback: str = ""
) -> tuple[dict[str, Any] | None, str]:
    """The envelope inside a list of LangChain content blocks, plus its text."""
    inner = "".join(_block_text(block) for block in blocks).strip()
    if not inner:
        return None, fallback
    try:
        nested = json.loads(inner)
    except (ValueError, TypeError):
        return None, inner
    return (dict(nested) if isinstance(nested, Mapping) else None), inner


def body_parts(body: ResultBody) -> tuple[dict[str, Any] | None, str]:
    """A tool result split into the JSON envelope it carries and its plain text.

    A result reaches us as a mapping, as the JSON text of one, or as the list of
    content blocks LangChain wraps it in -- and that list arrives both as a real
    list and as its JSON text, depending on whether the caller read
    ``ToolMessage.content`` or something that had already serialized it. A
    refusal reaches us as prose, and has no envelope.
    """
    if body is None:
        return None, ""
    if isinstance(body, Mapping):
        return dict(body), ""
    if isinstance(body, Sequence) and not isinstance(body, (str, bytes)):
        return _content_blocks(body)
    text = str(body).strip()
    try:
        decoded = json.loads(text)
    except (ValueError, TypeError):
        return None, text
    if isinstance(decoded, Mapping):
        return dict(decoded), text
    if isinstance(decoded, list):
        return _content_blocks(decoded, fallback=text)
    return None, text


def pre_vendor_outcome(
    body: ResultBody,
    *,
    tool_status: str,
    adapter: OrderAdapter | None = None,
) -> OrderOutcome | None:
    """The outcome for a call no vendor answered on its own terms, or None.

    For a caller holding an undecoded body and no adapter to hand it to. An
    adapter's own :meth:`OrderAdapter.parse_result` decodes once and reaches
    the same rules directly.
    """
    return _before_vendor(*body_parts(body), tool_status=tool_status, adapter=adapter)


def _before_vendor(
    envelope: dict[str, Any] | None,
    text: str,
    *,
    tool_status: str,
    adapter: OrderAdapter | None,
) -> OrderOutcome | None:
    """A policy refusal is terminal and names no vendor code. A tool error is a
    transport failure, and a retry after one is a new attempt, never this one --
    unless the adapter recognizes its own vendor in the error, which is the one
    thing that can tell a call that never left from an order turned down."""
    if text.startswith(REFUSAL_PREFIX):
        return OrderOutcome(
            status=AttemptStatus.REFUSED,
            failure=Failure(kind="policy", message=text),
            raw=envelope or {},
        )
    if (tool_status or "").strip().casefold() != "error":
        return None
    refusal = adapter.vendor_error(envelope, text) if adapter is not None else None
    if refusal is not None:
        return OrderOutcome(
            status=AttemptStatus.REJECTED_BY_VENDOR,
            # A vendor that sent an object has it whole in ``raw``; one that
            # sent prose has nothing else to say what state it named.
            raw_status=None if envelope else (text or None),
            failure=refusal,
            raw=envelope or {},
        )
    return OrderOutcome(
        status=AttemptStatus.FAILED,
        failure=Failure(kind="transport", message=text or None),
        raw=envelope or {},
    )
