"""A bot's tool surface: the whole set unless it narrowed it.

``tools_allow`` is a whitelist (empty means "everything"), ``tools_deny`` a
blacklist applied after it. The filter runs on the built main-agent tools, so
a name it removes is invisible to the model rather than refused at call time.
"""

from __future__ import annotations

from typing import Any, Iterable, Optional


def tool_names(tools: Iterable[Any]) -> list[str]:
    return [str(getattr(tool, "name", "") or "") for tool in tools]


def narrow_tools(tools: list[Any], bot: Optional[dict]) -> list[Any]:
    """Apply a bot's allow/deny lists to the main agent's tools.

    A bot with no lists keeps the whole set; a bot with an allow list keeps
    only those names; deny always wins over allow, and names the build never
    bound are ignored (a plugin may have been uninstalled since).
    """
    if not bot:
        return tools
    allow = {str(name) for name in (bot.get("tools_allow") or []) if name}
    deny = {str(name) for name in (bot.get("tools_deny") or []) if name}
    if not allow and not deny:
        return tools
    kept = []
    for tool in tools:
        name = str(getattr(tool, "name", "") or "")
        if allow and name not in allow:
            continue
        if name in deny:
            continue
        kept.append(tool)
    return kept
