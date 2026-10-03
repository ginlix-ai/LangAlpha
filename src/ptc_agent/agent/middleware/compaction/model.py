"""The model that writes a compaction summary, and how much history it can read.

The summary runs on the compaction model, by default the Background model,
whose window can be far smaller than that of the turn's model whose history it
reads, so what one summary call carries is sized to the compaction model.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ptc_agent.config.agent import AgentConfig

# tiktoken counts low against other vendors' tokenizers, and the summary prompt
# and its answer share the window, so history gets at most this share of it.
_SUMMARY_CONTEXT_SHARE = 0.7


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


def summary_trim_budget(model: Any, token_threshold: int) -> int:
    """How many tokens of history one summary call may carry.

    A budget sized only to the turn's threshold overflows a compaction model
    with a smaller window, and the compaction fails.
    """
    budget = token_threshold + 50_000
    limit = max_input_tokens(model)
    if limit is not None and limit > 0:
        budget = min(budget, int(limit * _SUMMARY_CONTEXT_SHARE))
    return budget
