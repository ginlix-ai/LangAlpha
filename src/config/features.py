"""Feature-flag registry: code-declared catalog, deployment overrides, user opt-ins.

Each feature carries two orthogonal controls:

- ``enabled`` — the kill switch. ``false`` turns the feature off for everyone
  and hides every surface; the gate is irrelevant.
- ``gate`` — the access model while enabled:
    * ``none``    — available to everyone
    * ``opt_in``  — off until the user opts in (Settings toggle)
    * ``opt_out`` — on unless the user opts out (Settings toggle)
    * ``plan``    — requires a platform plan whose access tier meets the
      feature's ``min_tier`` (OSS/self-hosted deployments bypass plan gates,
      matching the search-provider tier-gating convention)

Three definition layers, resolved in order:

1. **Catalog** (this module): every feature the codebase knows about, with its
   hard defaults and the frontend-facing label/description. A feature must be
   declared here to exist anywhere else.
2. **Deployment posture** (``config.yaml`` ``features:`` section): per-deploy
   override of ``enabled`` / ``gate`` / ``min_tier``; unset fields inherit
   the catalog.
3. **User override** (``user_preferences.other_preference.feature_overrides``,
   server-side): honored only for ``opt_in``/``opt_out`` gates.

Server code resolves per-user via ``src.server.services.features``; this
module stays user-agnostic so the agent library can consult defaults.
"""

from dataclasses import dataclass
from enum import StrEnum
from typing import Mapping, NamedTuple

from src.config import settings
from src.config.settings import get_infrastructure_config


class FeatureGate(StrEnum):
    NONE = "none"
    OPT_IN = "opt_in"
    OPT_OUT = "opt_out"
    PLAN = "plan"


# Gates whose features accept a per-user override (the Settings toggle).
USER_CONTROLLABLE_GATES = frozenset({FeatureGate.OPT_IN, FeatureGate.OPT_OUT})


@dataclass(frozen=True)
class FeatureSpec:
    """A code-declared feature and its hard defaults."""

    key: str
    label: str
    description: str
    enabled: bool = False
    gate: FeatureGate = FeatureGate.NONE
    min_tier: int | None = None  # plan gate only
    tradeoffs: str | None = None  # honest cost of opting in, shown in Settings


FEATURES: dict[str, FeatureSpec] = {
    "wakeups": FeatureSpec(
        key="wakeups",
        label="Wakeups",
        description=(
            "Lets the agent pick a thread back up by itself to carry on a task: "
            "at a time, every few minutes, when a price moves, or when an order "
            "it placed fills. Pending wakeups show in the chat tray and the "
            "Status panel, where you can cancel any of them."
        ),
        enabled=True,
        gate=FeatureGate.OPT_OUT,
        tradeoffs=(
            "A wakeup that fires while the thread is idle starts a turn by "
            "itself, which costs as much as any turn; checking every 5 minutes "
            "can take up to 24 turns over two hours. Turning this off cancels "
            "every pending wakeup."
        ),
    ),
    "market_watch": FeatureSpec(
        key="market_watch",
        label="Market watch",
        description=(
            "Adds a Watch toggle to chat: keep a per-thread list of symbols "
            "whose live quotes stream into the conversation while you work, "
            "so the agent reasons from current intraday prices instead of "
            "stale lookups."
        ),
        enabled=True,
        gate=FeatureGate.OPT_IN,
        tradeoffs=(
            "Quotes are re-injected into the model's context as they update, "
            "which can occasionally distract the agent from its main task and "
            "invalidate the prompt cache — expect slightly slower, costlier "
            "turns while a watch list is active."
        ),
    ),
    "site_crawl": FeatureSpec(
        key="site_crawl",
        label="Site crawling (experimental)",
        description=(
            "Gives the agent WebMap and WebCrawl tools: list a site's URLs, "
            "then crawl whole sections into workspace files for multi-page "
            "research (docs sites, filing archives)."
        ),
        enabled=True,
        gate=FeatureGate.OPT_IN,
        tradeoffs=(
            "Crawling bills per delivered page up to the call's page limit, "
            "so a broad crawl costs more than targeted fetches. Experimental — "
            "behavior and pricing may change."
        ),
    ),
    "todo_write": FeatureSpec(
        key="todo_write",
        label="Task checklist (legacy)",
        description=(
            "Gives the agent a TodoWrite tool to keep a checklist of its steps "
            "on long tasks, shown as a live todo list in chat."
        ),
        enabled=True,
        gate=FeatureGate.OPT_IN,
        tradeoffs=(
            "Each checklist update is an extra tool call, which adds a little "
            "time and cost to long turns. Legacy, and may be removed in a "
            "future release."
        ),
    ),
    "scratchpad": FeatureSpec(
        key="scratchpad",
        label="Scratchpad and working notes",
        description=(
            "Gives the agent a scratchpad folder in each thread for its "
            "intermediate files, and has it keep running notes there: key "
            "findings, decisions and why, and corrections you give mid-task. "
            "The notes carry a long task through after older messages in the "
            "thread are summarized."
        ),
        enabled=True,
        gate=FeatureGate.OPT_IN,
        tradeoffs=(
            "The agent spends a few extra tool calls per task keeping its "
            "notes. The scratchpad and its notes are deleted when you archive "
            "the thread, so keep anything you want to last in the workspace's "
            "own files."
        ),
    ),
    "provider_compaction": FeatureSpec(
        key="provider_compaction",
        label="Provider compaction",
        description=(
            "When a long thread is compacted, Claude and OpenAI models "
            "summarize it with their provider's own compaction rather than "
            "ours, which keeps more of what they knew: Claude keeps its "
            "reasoning in the turns it keeps. To turn it off for one model "
            "only, use Per-model settings on the Model tab."
        ),
        enabled=True,
        gate=FeatureGate.OPT_OUT,
        tradeoffs=(
            "OpenAI's summary is encrypted, so only OpenAI models can read it "
            "and the compaction card shows a plain record of the conversation "
            "instead. Switch such a thread to another provider's model and it "
            "gets that record, which leaves out the earlier tool results."
        ),
    ),
    # Off in every deployment until an operator turns it on; then each user
    # opts in. The system switch lists the Claude Code models
    # (``src.llms.claude_code.runtime_available``) and a user's opt-in lets
    # their turns run on one (``resolve_llm_config``).
    "claude_code_runtime": FeatureSpec(
        key="claude_code_runtime",
        label="Claude Code runtime (preview)",
        description=(
            "Runs your Claude threads through Claude Code on your own Claude "
            "plan instead of an API key. It needs a setup token, a separate "
            "credential you make for LangAlpha that never touches your own "
            "Claude Code sign-in. Add it in Settings under Model access."
        ),
        enabled=False,
        gate=FeatureGate.OPT_IN,
    ),
    "a_share_pack": FeatureSpec(
        key="a_share_pack",
        label="A-share market pack",
        description=(
            "Switches the dashboard news widget to the Chinese-language CN "
            "market feed (Chinese locales only)."
        ),
        enabled=True,
        gate=FeatureGate.OPT_IN,
    ),
}


class SystemFlag(NamedTuple):
    enabled: bool
    gate: FeatureGate
    min_tier: int | None


def get_feature_specs() -> dict[str, FeatureSpec]:
    return dict(FEATURES)


def system_flag(key: str) -> SystemFlag:
    """Deployment-level state of a feature: catalog defaults overlaid with the
    config.yaml ``features:`` entry (unset fields inherit the catalog)."""
    spec = FEATURES[key]
    override = get_infrastructure_config().features.get(key)
    enabled = spec.enabled
    gate = spec.gate
    min_tier = spec.min_tier
    if override is not None:
        if override.enabled is not None:
            enabled = override.enabled
        if override.gate is not None:
            gate = FeatureGate(override.gate)
        if override.min_tier is not None:
            min_tier = override.min_tier
    return SystemFlag(enabled=enabled, gate=gate, min_tier=min_tier)


def is_feature_enabled_system(key: str) -> bool:
    """Kill-switch check: is the feature on at all in this deployment?

    Gate-agnostic — an ``opt_in`` feature nobody enabled yet is still "on"
    here. Used where no user context exists but the feature merely needs to
    exist (skill registry listings, sandbox skill sync).
    """
    return system_flag(key).enabled


def has_plan_gated_features() -> bool:
    """Whether any cataloged feature currently resolves to a plan gate —
    lets callers skip the platform tier fetch entirely when none does."""
    return any(system_flag(key).gate is FeatureGate.PLAN for key in FEATURES)


def effective_flags(
    user_overrides: Mapping[str, object] | None,
    *,
    access_tier: int | None = None,
    platform_mode: bool | None = None,
) -> dict[str, bool]:
    """Resolve every cataloged feature for one user.

    ``user_overrides`` is the raw ``feature_overrides`` mapping from the
    user's preferences; non-bool values and overrides on non-opt gates are
    ignored rather than errored so a stale or hand-edited row can't break
    turns. Plan gates fail closed in platform mode when ``access_tier`` is
    unknown or below ``min_tier``; non-platform deployments bypass them.
    """
    if platform_mode is None:
        platform_mode = settings.HOST_MODE == "platform"

    resolved: dict[str, bool] = {}
    for key in FEATURES:
        flag = system_flag(key)
        if not flag.enabled:
            resolved[key] = False
            continue
        override = (user_overrides or {}).get(key)
        override = override if isinstance(override, bool) else None
        if flag.gate is FeatureGate.NONE:
            resolved[key] = True
        elif flag.gate is FeatureGate.OPT_IN:
            resolved[key] = override is True
        elif flag.gate is FeatureGate.OPT_OUT:
            resolved[key] = override is not False
        else:  # FeatureGate.PLAN
            resolved[key] = not platform_mode or (
                access_tier is not None
                and flag.min_tier is not None
                and access_tier >= flag.min_tier
            )
    return resolved


def default_feature_enabled(key: str) -> bool:
    """No-user-context effective default (opt_in and platform plan gates
    resolve False). Fallback for entry points that skip per-user resolution."""
    return effective_flags(None).get(key, False)
