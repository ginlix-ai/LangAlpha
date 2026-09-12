"""Vendor adapters that turn an order tool call and its answer into one shape.

A tool call at a brokerage is the only record an order has, and every vendor
spells it differently. These adapters are the seam: the ledger, the receipt and
the orders surface read ``BrokerOrder`` and ``OrderOutcome`` and never a
vendor's field names.
"""

from src.server.services.brokerage_orders.base import (
    REFUSAL_PREFIX,
    ListingIncomplete,
    MatchKey,
    OrderAdapter,
    StatusQuery,
    StatusReadError,
    VendorOrder,
    adapter_for,
    body_parts,
    pre_vendor_outcome,
)
from src.server.services.brokerage_orders.ibkr import IbkrOrderAdapter
from src.server.services.brokerage_orders.models import (
    OPEN_STATUSES,
    TERMINAL_STATUSES,
    AssetClass,
    AttemptStatus,
    BrokerOrder,
    ComboRef,
    CryptoRef,
    EquityRef,
    Failure,
    FutureRef,
    InstrumentRef,
    Money,
    OpaqueRef,
    OptionRef,
    OrderMode,
    OrderOutcome,
    OrderType,
    Session,
    Side,
    TimeInForce,
    instrument_from_json,
    instrument_to_json,
)
from src.server.services.brokerage_orders.moomoo import MoomooOrderAdapter
from src.server.services.brokerage_orders.robinhood import RobinhoodOrderAdapter

__all__ = [
    "OPEN_STATUSES",
    "REFUSAL_PREFIX",
    "TERMINAL_STATUSES",
    "AssetClass",
    "AttemptStatus",
    "BrokerOrder",
    "ComboRef",
    "CryptoRef",
    "EquityRef",
    "Failure",
    "FutureRef",
    "IbkrOrderAdapter",
    "InstrumentRef",
    "ListingIncomplete",
    "MatchKey",
    "Money",
    "MoomooOrderAdapter",
    "OpaqueRef",
    "OptionRef",
    "OrderAdapter",
    "OrderMode",
    "OrderOutcome",
    "OrderType",
    "RobinhoodOrderAdapter",
    "Session",
    "Side",
    "StatusQuery",
    "StatusReadError",
    "TimeInForce",
    "VendorOrder",
    "adapter_for",
    "body_parts",
    "instrument_from_json",
    "instrument_to_json",
    "pre_vendor_outcome",
]
