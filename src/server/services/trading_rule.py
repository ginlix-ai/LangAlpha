"""What the agent is told about trading, derived from the user's settings.

Read off the binding plans the resolve makes for the servers the workspace is
configured with, never off the tools a turn managed to bind: a lapsed grant, a
connection waiting on re-auth or order tools the turn budget dropped change no
setting, and a rule that followed them would tell the model mid-thread that it
changed. The same plans stamp each order tool's approval, so the rule and the
interrupt read one resolution.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from src.config.env import EGRESS_RELAY_SECRET, PUBLIC_APP_URL
from src.server.services.tool_binding import REAL_MONEY_MODES
from src.server.services.trading_permission import TradingPermission

if TYPE_CHECKING:
    from src.server.services.mcp_config import ResolvedMCP


#: Where the user changes the level, for the agent to send them; the web app's
#: ``TRADING_PERMISSION_HREF`` names the same page. Absolute, since the reply
#: may be read in a channel rather than in the app.
TRADING_SETTINGS_URL = f"{PUBLIC_APP_URL}/settings?tab=preferences#trading-permission"


@dataclass(frozen=True)
class TradingRule:
    level: TradingPermission
    #: Servers whose live or staged order tool still asks at a level that
    #: skips approval, whose rule would otherwise say those orders go straight
    #: out. Named by server, the name in the model's own tool names.
    asks_on: tuple[str, ...] = ()


def trading_rule(resolved: ResolvedMCP | None) -> TradingRule | None:
    """The rule, or None when no configured server can place a live order or
    stage an instruction as a direct tool.

    Without the relay secret nothing on this deployment binds directly, so
    there is no order the rule could be about.
    """
    if resolved is None or not EGRESS_RELAY_SECRET:
        return None
    level: TradingPermission | None = None
    asking: set[str] = set()
    for server, plan in resolved.binding_plans_by_name.items():
        for tool, answer in plan.by_tool.items():
            order = answer.order
            if order is None or order.mode not in REAL_MONEY_MODES:
                continue
            if tool not in plan.direct:
                continue
            # Every row comes from one catalog read, which joins the same
            # level on, so the plans cannot name two.
            level = plan.trading
            if order.approval:
                asking.add(server)
    if level is None:
        return None
    return TradingRule(level, () if level.asks else tuple(sorted(asking)))


def with_trading_rule(
    user_profile: dict[str, Any] | None, rule: TradingRule | None
) -> dict[str, Any] | None:
    """The profile the agent is built with, naming the rule when there is one;
    a copy, since the cached profile is shared.

    Named even when the profile read did not answer, since this turn's order
    tools are stamped from the same plans either way; a profile holding only
    the rule reads downstream as that unanswered read.
    """
    if rule is None:
        return user_profile
    profile = {
        **(user_profile or {}),
        "trading_permission": rule.level.value,
        "trading_settings_url": TRADING_SETTINGS_URL,
    }
    if rule.asks_on:
        profile["trading_asks_on"] = list(rule.asks_on)
    return profile
