"""The vendor-neutral order, the vendor's answer to it, and the JSON both persist as.

Both shapes land in a JSONB column and in a tool artifact, so ``to_json``
renders a Decimal as a string and a date as ISO text itself rather than leaving
a serializer to guess. Anything a vendor sends that these fields cannot name
survives in ``extras`` or ``raw``: a mapping gap loses detail, never the record.

The validators are the tolerance a stored row has always been read with, and
only that: a number a vendor typed three ways, an empty nested object that
means absent, an instrument whose ``kind`` we no longer keep. Everything else
is the declaration, so a side or an order type that is not one of ours is a
row we refuse to read rather than one we carry a wrong word out of.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Annotated, Any, Literal, Self

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    PlainSerializer,
    TypeAdapter,
    ValidationError,
)

from src.server.services.brokerage_capabilities import OrderAction, OrderMode
from src.server.services.brokerage_orders._coerce import (
    as_date,
    as_datetime,
    as_decimal,
    as_int,
)

class AttemptStatus(StrEnum):
    """The order attempt lifecycle, from proposal to a settled end state."""

    PROPOSED = "proposed"
    APPROVED = "approved"
    REJECTED_BY_USER = "rejected_by_user"
    REFUSED = "refused"
    SUBMITTING = "submitting"
    SUBMITTED = "submitted"
    PENDING_CONFIRM = "pending_confirm"
    WORKING = "working"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED_BY_VENDOR = "rejected_by_vendor"
    FAILED = "failed"
    UNKNOWN = "unknown"


# The states a vendor can still move an attempt out of. A row in one of them is
# neither settled nor finished, so reconciliation keeps asking and nothing may
# stamp it complete -- ``unknown`` included, because it is an answer we could
# not read rather than an end.
OPEN_STATUSES: tuple[AttemptStatus, ...] = (
    AttemptStatus.SUBMITTED,
    AttemptStatus.PENDING_CONFIRM,
    AttemptStatus.WORKING,
    AttemptStatus.PARTIALLY_FILLED,
    AttemptStatus.UNKNOWN,
)

# The states nothing may overtake. An attempt that reached one has its answer,
# so a listing that arrives afterwards describes an order that had already
# ended, and reaching one is also what dates the attempt: every writer stamps
# ``completed_at`` from this set and from nothing else.
TERMINAL_STATUSES: tuple[AttemptStatus, ...] = (
    AttemptStatus.REJECTED_BY_USER,
    AttemptStatus.REFUSED,
    AttemptStatus.FILLED,
    AttemptStatus.CANCELLED,
    AttemptStatus.REJECTED_BY_VENDOR,
    AttemptStatus.FAILED,
)

# The actions that bring a vendor order into existence. Only these own the id
# they carry: a cancel names an order it did not create, so counting its id as
# taken would hide the placement it points at from the attempt that made it.
CREATING_ACTIONS: tuple[OrderAction, ...] = (OrderAction.PLACE, OrderAction.STAGE)

# The actions whose event no tool lists. Nothing could ever read one of these
# back, so the attempt's own answer is the last word on it.
UNLISTED_ACTIONS: tuple[OrderAction, ...] = (
    OrderAction.EXERCISE,
    OrderAction.CANCEL_EXERCISE,
)


AssetClass = Literal[
    "equity", "option", "option_combo", "future", "crypto", "warrant", "other"
]
Side = Literal["buy", "sell", "sell_short", "buy_to_cover"]
OrderType = Literal[
    "market",
    "limit",
    "stop",
    "stop_limit",
    "market_if_touched",
    "limit_if_touched",
    "auction",
    "auction_limit",
]
# The last three are IBKR's ``OVT``, ``OND`` and ``OPG``: an order that lives
# through the overnight session, one that lives through it and the day after,
# and one that waits for the opening auction.
TimeInForce = Literal[
    "day", "gtc", "overnight", "overnight_next_day", "at_the_open"
]
Session = Literal["rth", "rth_plus_ext", "overnight", "all_day"]
OptionRight = Literal["C", "P"]


def _empty_as_absent(value: Any) -> Any:
    """An empty nested object read as absent, which is how it was always stored.

    ``{}`` reaches these fields only from a hand-written row: the codec drops a
    null rather than writing one, so nothing we emit round-trips through it.
    """
    return value or None


def _string(value: Any) -> Any:
    return "" if value is None else str(value)


def _optional_string(value: Any) -> Any:
    return None if value is None else str(value)


def _amount(value: Any) -> Any:
    return as_decimal(value) or Decimal(0)


#: A Decimal a vendor may have typed as a string, an int or a float.
Number = Annotated[Decimal | None, BeforeValidator(as_decimal)]
Text = Annotated[str, BeforeValidator(_string)]
OptionalText = Annotated[str | None, BeforeValidator(_optional_string)]
#: Serialized by hand because Pydantic writes UTC as ``Z`` and every row in the
#: column was written with ``isoformat``.
Timestamp = Annotated[
    datetime | None,
    BeforeValidator(as_datetime),
    PlainSerializer(datetime.isoformat, return_type=str, when_used="json-unless-none"),
]
JsonDict = Annotated[dict[str, Any], BeforeValidator(lambda v: dict(v or {}))]
Route = Annotated[
    dict[str, str],
    BeforeValidator(lambda v: {str(k): str(x) for k, x in (v or {}).items()}),
]


class _Value(BaseModel):
    """A frozen value object that persists whole, its nulls included.

    A null here is part of the shape rather than an absence: ``code: null``
    beside a message says the vendor named no code.
    """

    model_config = ConfigDict(frozen=True)

    def to_json(self) -> dict[str, Any]:
        return self.model_dump(mode="json")

    @classmethod
    def from_json(cls, data: Mapping[str, Any]) -> Self:
        return cls.model_validate(data)


class _Record(BaseModel):
    """A record that persists with its null fields left out.

    Only at the top level: a null inside an instrument or a fee belongs to the
    shape that names it, so the drop is not recursive.
    """

    def to_json(self) -> dict[str, Any]:
        return {k: v for k, v in self.model_dump(mode="json").items() if v is not None}

    @classmethod
    def from_json(cls, data: Mapping[str, Any]) -> Self:
        return cls.model_validate(data)


class Money(_Value):
    amount: Annotated[Decimal, BeforeValidator(_amount)] = Decimal(0)
    currency: Text = ""


class Failure(_Value):
    kind: Text = "unknown"
    code: OptionalText = None
    message: OptionalText = None


class EquityRef(_Value):
    kind: Literal["equity"] = "equity"
    symbol: Text = ""
    venue: OptionalText = None


class OptionRef(_Value):
    kind: Literal["option"] = "option"
    underlying: Text = ""
    expiration: Annotated[date | None, BeforeValidator(as_date)] = None
    strike: Number = None
    right: OptionRight | None = None
    multiplier: Annotated[int | None, BeforeValidator(as_int)] = None
    vendor_instrument_id: OptionalText = None


class ComboRef(_Value):
    """Both carriers, because moomoo sends legs and Robinhood sends a name."""

    kind: Literal["combo"] = "combo"
    legs: Annotated[
        tuple[dict[str, Any], ...] | None, BeforeValidator(_empty_as_absent)
    ] = None
    strategy: OptionalText = None


class FutureRef(_Value):
    kind: Literal["future"] = "future"
    symbol: Text = ""
    venue: OptionalText = None
    contract_month: OptionalText = None


class CryptoRef(_Value):
    kind: Literal["crypto"] = "crypto"
    pair: Text = ""


class OpaqueRef(_Value):
    """An instrument addressed only by a vendor string, such as an HK warrant."""

    kind: Literal["opaque"] = "opaque"
    raw_code: Text = ""


InstrumentRef = Annotated[
    EquityRef | OptionRef | ComboRef | FutureRef | CryptoRef | OpaqueRef,
    Field(discriminator="kind"),
]

_INSTRUMENT: TypeAdapter[InstrumentRef] = TypeAdapter(InstrumentRef)


def instrument_to_json(ref: InstrumentRef | None) -> dict[str, Any] | None:
    return None if ref is None else ref.model_dump(mode="json")


def instrument_from_json(data: Mapping[str, Any] | None) -> InstrumentRef | None:
    """The instrument a row names, or None when nothing here names that kind.

    Tolerant on purpose: a row written by a build that knew a kind this one
    does not still describes an order, and losing the instrument is a smaller
    loss than refusing to read the order at all.
    """
    if not data:
        return None
    try:
        return _INSTRUMENT.validate_python(data)
    except ValidationError:
        return None


class BrokerOrder(_Record):
    """One order as every vendor's request parses into it.

    Everything past the account is optional because a cancel, a replace and a
    confirm are order records too, and each knows only a slice of this.
    """

    vendor: Text = ""
    account_ref: Text = ""
    mode: OrderMode = OrderMode.LIVE
    #: The vendor's id for the order this call acts on, when it acts on one:
    #: what a cancel, a replace, a confirm or an unstage names. A placement
    #: creates an id rather than naming one, and leaves this None.
    target_ref: OptionalText = None
    asset_class: Annotated[AssetClass, BeforeValidator(lambda v: v or "other")] = "other"
    instrument: Annotated[
        InstrumentRef | None, BeforeValidator(instrument_from_json)
    ] = None
    side: Side | None = None
    qty: Number = None
    notional: Annotated[Money | None, BeforeValidator(_empty_as_absent)] = None
    order_type: OrderType | None = None
    limit_price: Number = None
    stop_price: Number = None
    time_in_force: TimeInForce | None = None
    session: Session | None = None
    currency: OptionalText = None
    note: OptionalText = None
    extras: JsonDict = Field(default_factory=dict)
    raw: JsonDict = Field(default_factory=dict)


class OrderOutcome(_Record):
    """What the vendor answered, mapped onto the attempt lifecycle."""

    status: AttemptStatus = AttemptStatus.UNKNOWN
    raw_status: OptionalText = None
    vendor_order_id: OptionalText = None
    route: Route = Field(default_factory=dict)
    filled_qty: Number = None
    avg_fill_price: Number = None
    fees: Annotated[Money | None, BeforeValidator(_empty_as_absent)] = None
    confirm_token: OptionalText = None
    #: A vendor link the user has to open to act on this order, when the vendor
    #: answers with one: IBKR stages an instruction and hands back the page a
    #: human releases it on.
    action_url: OptionalText = None
    failure: Annotated[Failure | None, BeforeValidator(_empty_as_absent)] = None
    created_at: Timestamp = None
    updated_at: Timestamp = None
    raw: JsonDict = Field(default_factory=dict)
