"""The model that writes a compaction summary, and the window it declares.

The summary runs on the compaction model, by default the Background model,
whose window can be far smaller than that of the turn's model whose history it
reads, so ``summarize`` sizes each summary call to the model it goes to.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ptc_agent.config.agent import AgentConfig


def resolve_compaction_client(config: AgentConfig) -> Any | None:
    """Return the compaction LLM client (role-resolved or main-copy), or None.

    With a dedicated compaction model, use the pre-resolved role client
    (credentialed users) or None (platform users keep the cheap name-based
    model). Without one, fall back to a copy of the main client.
    """
    has_compaction_model = bool(config.llm and config.llm.compaction_name)
    return config.client_for_role("compaction", fallback_to_main=not has_compaction_model)


def max_input_tokens(model: Any) -> int | None:
    """The input window ``model``'s profile declares, or None without one."""
    profile = getattr(model, "profile", None)
    if not isinstance(profile, Mapping):
        return None
    limit = profile.get("max_input_tokens")
    return limit if isinstance(limit, int) else None
