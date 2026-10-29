"""The user's trading permission: whether their real-money orders stop for approval.

One level per user, above every brokerage connection's own approval switches.
The server enforces exactly one thing off it, whether a live order or a staged
instruction stops for the user's approval; how the agent should behave at the
level reaches it as prompt context, where the user's own words in a conversation
can override it. Nothing said in a conversation lifts the approval step, because
the agent reads text it did not get from the user. Paper orders spend nothing
and keep their own per-connection switch whatever the level.

The two levels that skip approval hold only while the agreement the user
accepted is the current one. A stale acceptance reads as the default, so
raising ``TRADING_AGREEMENT_VERSION`` puts every such user back to approving
each order until they agree again.
"""

from __future__ import annotations

from enum import Enum


class TradingPermission(str, Enum):
    NO_TRADING = "no_trading"
    APPROVE_EACH = "approve_each"
    PLAN_FIRST = "plan_first"
    AUTONOMOUS = "autonomous"

    @property
    def asks(self) -> bool:
        """Live orders and staged instructions stop for the user at this level."""
        return self in (TradingPermission.NO_TRADING, TradingPermission.APPROVE_EACH)

    @property
    def needs_agreement(self) -> bool:
        return not self.asks


DEFAULT_TRADING_PERMISSION = TradingPermission.APPROVE_EACH
TRADING_AGREEMENT_VERSION = 1


def effective_permission(
    level: object, agreement_version: object
) -> TradingPermission:
    """The level a stored row grants today.

    Anything unreadable, and a level whose agreement is not the current one,
    answers the default: the failure mode of this reading is an order that asks.
    """
    try:
        parsed = TradingPermission(level)
    except ValueError:
        return DEFAULT_TRADING_PERMISSION
    if parsed.needs_agreement and agreement_version != TRADING_AGREEMENT_VERSION:
        return DEFAULT_TRADING_PERMISSION
    return parsed
