"""The model that writes a compaction summary, and the window it declares.

The summary runs on the compaction model, by default the model the turn runs.
A model picked for compaction can have a far smaller window than the turn's
model whose history it reads, so ``summarize`` sizes each summary call to the
model it goes to.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ptc_agent.config.agent import AgentConfig


def resolve_compaction_client(config: AgentConfig) -> Any | None:
    """The compaction role's pre-resolved client, or None to build the named
    model. A client handed to the config (``AgentConfig.create``) has no name
    to build, so with no compaction model named the summary runs on a copy of
    it. A server turn always names one (``select_model``), so its main client,
    at the turn's own level, is never borrowed."""
    named = bool(config.llm and config.llm.compaction)
    return config.client_for_role("compaction", fallback_to_main=not named)


def max_input_tokens(model: Any) -> int | None:
    """The input window ``model``'s profile declares, or None without one."""
    profile = getattr(model, "profile", None)
    if not isinstance(profile, Mapping):
        return None
    limit = profile.get("max_input_tokens")
    return limit if isinstance(limit, int) else None
