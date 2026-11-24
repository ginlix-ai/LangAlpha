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
    * ``grant``   — off unless granted to the user: an override the server
      or a migration writes, which the user can neither see nor set

Three definition layers, resolved in order (with an organization tier between
the last two, when the auth service places the user in one; see
``effective_flags``):

1. **Catalog** (this module): every feature the codebase knows about, with its
   hard defaults and the frontend-facing copy (label, description and what
   Experiments shows a user deciding), in English; a web locale translates
   them under ``settings.featureCatalog.<key>`` (``web/src/locales``) and
   falls back to this copy. A feature must be
   declared here to exist anywhere else.
2. **Deployment posture** (``config.yaml`` ``features:`` section): per-deploy
   override of ``enabled`` / ``gate`` / ``min_tier``; unset fields inherit
   the catalog.
3. **User override** (``user_preferences.other_preference.feature_overrides``,
   server-side): honored only for ``opt_in``/``opt_out``/``grant`` gates,
   and set by the user only for the first two.

Server code resolves per-user via ``src.server.services.features``; this
module stays user-agnostic so the agent library can consult defaults.
"""

from dataclasses import dataclass
from enum import StrEnum
from typing import Literal, Mapping, NamedTuple

from src.config import settings
from src.config.settings import get_infrastructure_config


class FeatureGate(StrEnum):
    NONE = "none"
    OPT_IN = "opt_in"
    OPT_OUT = "opt_out"
    PLAN = "plan"
    GRANT = "grant"


# Gates whose features accept a per-user override (the Settings toggle).
USER_CONTROLLABLE_GATES = frozenset({FeatureGate.OPT_IN, FeatureGate.OPT_OUT})


class FeatureCategory(StrEnum):
    """The group a feature is listed under in Experiments, by what it changes
    for the user. The web app names and orders the groups."""

    CHAT = "chat"
    TOOLS = "tools"
    AUTONOMY = "autonomy"
    LONG_THREADS = "long_threads"
    DASHBOARD = "dashboard"


@dataclass(frozen=True)
class FeatureGuide:
    """What Experiments shows a user deciding whether to keep a feature.

    ``on_if`` and ``off_if`` finish "Turn it on if" and "Leave it off if" (or
    "Keep it on if" and "Turn it off if" for an opt-out feature), so each
    starts in lower case. ``cost`` is the price at a glance, shown beside the
    default; ``good_to_know`` holds what is true either way. ``caution`` is
    what switching it loses, which the row shows beside its switch, since the
    switch toggles without opening the rest.
    """

    on_if: str
    off_if: str
    cost: str | None = None
    good_to_know: str | None = None
    caution: str | None = None


@dataclass(frozen=True)
class FeatureSpec:
    """A code-declared feature and its hard defaults. Within a category,
    features are listed in catalog order."""

    key: str
    label: str
    description: str
    enabled: bool = False
    gate: FeatureGate = FeatureGate.NONE
    min_tier: int | None = None  # plan gate only
    category: FeatureCategory | None = None
    #: Still changing: behavior and pricing may move under it.
    preview: bool = False
    guide: FeatureGuide | None = None
    #: Where the user turns it on, when that is somewhere other than
    #: Experiments: Claude Code's opt-in is made by connecting it under Model access.
    home: Literal["model_access"] | None = None
    #: Features this one depends on: it resolves off wherever any of them
    #: does, so a child can never outlive the switch that opens its parent.
    requires: tuple[str, ...] = ()


FEATURES: dict[str, FeatureSpec] = {
    "agent_plan": FeatureSpec(
        key="agent_plan",
        label="Show the agent's plan",
        description=(
            "On longer tasks, the agent shows its plan at the top of its work "
            "in the chat and marks which step it is on."
        ),
        enabled=True,
        gate=FeatureGate.OPT_OUT,
        category=FeatureCategory.CHAT,
        guide=FeatureGuide(
            on_if=(
                "you want to see where a long task is heading and catch a wrong "
                "turn early."
            ),
            off_if="you would rather the chat show only the work and the answer.",
            cost="Almost no extra time or cost",
            good_to_know=(
                "Plan updates travel with calls the agent is already making. Quick "
                "answers show no plan."
            ),
        ),
    ),
    "market_watch": FeatureSpec(
        key="market_watch",
        label="Market watch",
        description=(
            "Adds a Market watch toggle to chat: keep a list of symbols for a "
            "thread, and their live quotes stream into the conversation while "
            "you work."
        ),
        enabled=True,
        gate=FeatureGate.OPT_IN,
        category=FeatureCategory.CHAT,
        guide=FeatureGuide(
            on_if=(
                "you trade intraday and want the agent working from the current "
                "price, not one it looked up earlier."
            ),
            off_if=(
                "your research runs on filings and fundamentals, where live quotes "
                "only add noise."
            ),
            cost="Slower, costlier turns while a watch list is on",
            good_to_know=(
                "Each quote update is added to what the model reads. That can pull "
                "the agent off its main task, and the model re-reads more of the "
                "thread on every turn, which is where the time and cost go."
            ),
        ),
    ),
    # The image and the wrapper are on every cloud computer either way: off, the
    # agent is not offered the skill and no one is shown the browser, which keeps
    # it unused rather than out of reach.
    "browser": FeatureSpec(
        key="browser",
        label="Browser",
        description=(
            "Gives the agent a Chrome on your computers for pages that only "
            "show what it needs after clicking or typing. Watch it live in the "
            "file panel's Browser tab, and take control to use the page "
            "yourself."
        ),
        enabled=True,
        gate=FeatureGate.OPT_IN,
        category=FeatureCategory.TOOLS,
        preview=True,
        guide=FeatureGuide(
            on_if=(
                "the sources you need sit behind search forms, filters, sign-ins "
                "or pages that load as you scroll."
            ),
            off_if=(
                "the pages you research read fine as plain text, or your computer "
                "is busy with heavy work."
            ),
            cost="Uses the computer's memory and CPU",
            good_to_know=(
                "It is slower than reading a page directly, and closes after 20 "
                "minutes unused. Sign-ins made in it are kept on the computer "
                "unless you turn that off for the computer."
            ),
        ),
    ),
    "site_crawl": FeatureSpec(
        key="site_crawl",
        label="Site crawling",
        description=(
            "Gives the agent WebMap and WebCrawl tools: list a site's URLs, "
            "then crawl whole sections into workspace files."
        ),
        enabled=True,
        gate=FeatureGate.OPT_IN,
        category=FeatureCategory.TOOLS,
        preview=True,
        guide=FeatureGuide(
            on_if=(
                "you research across many pages of one site, like a documentation "
                "site or a company's filing archive."
            ),
            off_if=(
                "you mostly need a few specific pages, which the agent's regular "
                "web fetch covers for less."
            ),
            cost="Billed per page",
            good_to_know=(
                "A crawl bills each page it delivers, up to the call's page limit, "
                "so a broad crawl costs more than targeted fetches. Behavior and "
                "pricing may change."
            ),
        ),
    ),
    "drive": FeatureSpec(
        key="drive",
        label="Drive",
        description=(
            "A folder of your own files that you and your agent both read and "
            "edit, from any workspace. Earlier versions of text and other small "
            "files are kept, and deleted files wait in the trash for up to 14 days."
        ),
        enabled=True,
        gate=FeatureGate.OPT_IN,
        category=FeatureCategory.TOOLS,
        preview=True,
        guide=FeatureGuide(
            on_if=(
                "you keep notes, reference files or templates you want the agent "
                "to use across all your workspaces."
            ),
            off_if="your files belong to one workspace each.",
            good_to_know=(
                "The agent may change these files at any time. Each change shows "
                "who made it, and an earlier version can be restored while it is kept."
            ),
        ),
    ),
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
        category=FeatureCategory.AUTONOMY,
        guide=FeatureGuide(
            on_if=(
                'you hand off tasks that wait on something, like "tell me if NVDA '
                'breaks 150" or "check the fill after the open".'
            ),
            off_if=(
                "you want every turn, and what it costs, to start with a message "
                "from you."
            ),
            cost="Each wakeup can start a turn",
            good_to_know=(
                "A wakeup that fires while the thread is idle costs as much as any "
                "turn: checking every 5 minutes can take up to 24 turns over two "
                "hours."
            ),
            caution="Turning this off cancels every pending wakeup.",
        ),
    ),
    "order_wakeups": FeatureSpec(
        key="order_wakeups",
        label="Order wakeups",
        description=(
            "When the agent places an order that does not fill at once, it sets "
            "a wakeup that brings it back to the thread when the order fills, "
            "partly fills or ends unfilled."
        ),
        enabled=True,
        gate=FeatureGate.OPT_OUT,
        category=FeatureCategory.AUTONOMY,
        guide=FeatureGuide(
            on_if=(
                "you let the agent place orders and want it to follow through, "
                "like setting the exit once the entry fills."
            ),
            off_if=(
                "you watch your orders yourself and would rather not pay for a "
                "turn on each fill."
            ),
            cost="Each fill can start a turn",
            good_to_know=(
                "Turned off, the agent hears how an order ends only if it sets a "
                "wakeup itself or you ask."
            ),
        ),
        requires=("wakeups",),
    ),
    "scratchpad": FeatureSpec(
        key="scratchpad",
        label="Scratchpad and working notes",
        description=(
            "Gives the agent a scratchpad folder in each thread for its "
            "intermediate files, and has it keep running notes there: key "
            "findings, decisions and why, and corrections you give mid-task."
        ),
        enabled=True,
        gate=FeatureGate.OPT_OUT,
        category=FeatureCategory.LONG_THREADS,
        guide=FeatureGuide(
            on_if=(
                "you give the agent long tasks: the notes carry them through after "
                "older messages in the thread are summarized."
            ),
            off_if=(
                "your threads are short and you would rather skip the calls the "
                "notes take."
            ),
            cost="A few extra tool calls per task",
            good_to_know=(
                "Archiving a thread deletes its scratchpad and notes, so keep "
                "anything you want to last in the workspace's own files."
            ),
        ),
    ),
    "provider_compaction": FeatureSpec(
        key="provider_compaction",
        label="Provider compaction",
        description=(
            "When a long thread is compacted, Claude and OpenAI models "
            "summarize it with their provider's own compaction rather than "
            "ours, which keeps more of what they knew: Claude keeps its "
            "reasoning in the turns it keeps."
        ),
        enabled=True,
        gate=FeatureGate.OPT_OUT,
        category=FeatureCategory.LONG_THREADS,
        guide=FeatureGuide(
            on_if="you keep each thread with one provider's models.",
            off_if=(
                "you often switch a thread between providers. OpenAI's summary is "
                "encrypted, so another provider's model gets only a plain record "
                "of the conversation, without the earlier tool results."
            ),
            good_to_know=(
                "To turn it off for one model only, use Per-model settings on the "
                "Model tab."
            ),
        ),
    ),
    # Each user opts in. For one who has, a completed turn arms its thread's
    # summary while the thread is idle on a warm cache
    # (``services.idle_compaction``), on routes whose cache entries live an
    # hour only, Claude Code's among them, whose CLI compacts the session;
    # "Summarize long threads after a break" (``Tuning.cold_compaction``)
    # turned off still keeps it from summarizing.
    "idle_compaction": FeatureSpec(
        key="idle_compaction",
        label="Idle compaction",
        description=(
            "Summarizes a long Claude thread while it sits idle, before its "
            "hour-long prompt cache expires, so the next message neither waits "
            "for a summary nor sends the whole conversation again. Applies "
            "where the cache lasts an hour: Claude Code mode, and the Claude "
            "sign-in (legacy)."
        ),
        enabled=True,
        gate=FeatureGate.OPT_IN,
        category=FeatureCategory.LONG_THREADS,
        guide=FeatureGuide(
            on_if=(
                "you use Claude Code mode or the Claude sign-in and come back to "
                "long threads after a break."
            ),
            off_if=(
                "you often leave long threads for good: the summary runs whether "
                "or not you come back."
            ),
            cost="About a turn per summary",
            good_to_know=(
                "Each summary reads the thread from the warm cache and writes the "
                "summary, which costs about what a turn does."
            ),
        ),
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
        category=FeatureCategory.DASHBOARD,
    ),
    # On for everyone. The models are listed only where the backend can run
    # them (``src.llms.claude_code.rows_available``: the ``claude-code`` extra
    # and the at-rest key), and a user's turns run on one once they connect a
    # setup token under Model access. A deployment that wants a per-user
    # switch sets the gate to ``opt_in``.
    "claude_code_runtime": FeatureSpec(
        key="claude_code_runtime",
        label="Claude Code mode",
        description=(
            "Runs your Claude threads through Claude Code on your own Claude "
            "plan. It signs in with a setup token made for LangAlpha. Claude "
            "Code runs on your own computers, and where this "
            "server offers it, also on cloud computers, with your setup token. "
            "Choose where it runs in Settings under Model access."
        ),
        enabled=True,
        gate=FeatureGate.NONE,
        home="model_access",
    ),
    # Who may start a new Claude sign-in (the ``claude-oauth`` provider),
    # which Claude Code replaces: only the users migration 121 granted it to,
    # every one with a Claude sign-in stored, who keep reconnecting it. A
    # sign-in already stored keeps working either way. A deployment still
    # offering it to everyone sets the gate to ``none``.
    "claude_oauth": FeatureSpec(
        key="claude_oauth",
        label="Claude sign-in (legacy)",
        description=(
            "Uses a Claude Pro or Max plan through a sign-in to your Claude "
            "account, with LangAlpha calling Claude directly. Claude Code mode "
            "replaces it."
        ),
        enabled=True,
        gate=FeatureGate.GRANT,
    ),
    # Off in every deployment until one turns it on. Even then it resolves on
    # only for an account the auth service places in an organization that
    # allows it (``src.server.services.account.account_for``).
    "teams": FeatureSpec(
        key="teams",
        label="Organizations",
        description=(
            "Lets the organization your account belongs to decide which "
            "features its members have."
        ),
        enabled=False,
        gate=FeatureGate.GRANT,
    ),
    # Off in every deployment until one turns it on, and then on only for a
    # member whose organization grants it through its ``features`` block.
    "org_connectors": FeatureSpec(
        key="org_connectors",
        label="Organization connectors",
        description=(
            "Gives you the connectors your organization sets up, ready in "
            "every workspace without connecting each one yourself."
        ),
        enabled=False,
        gate=FeatureGate.GRANT,
        requires=("teams",),
    ),
    # Off in every deployment until one turns it on, and then on only for a
    # member whose organization grants it through its ``features`` block.
    "org_library": FeatureSpec(
        key="org_library",
        label="Organization library",
        description=(
            "Lets members publish threads, files and skills to a library "
            "every member of the organization can open."
        ),
        enabled=False,
        gate=FeatureGate.GRANT,
        requires=("teams",),
    ),
    # Off in every deployment until one turns it on, and then on only for a
    # member whose organization grants it through its ``features`` block.
    "shared_workspaces": FeatureSpec(
        key="shared_workspaces",
        label="Shared workspaces",
        description=(
            "Lets members of the organization work in one workspace together, "
            "each running their own turns and seeing each other's threads and "
            "files."
        ),
        enabled=False,
        gate=FeatureGate.GRANT,
        requires=("teams",),
    ),
    # On wherever shared workspaces are, and then only for a member whose
    # organization grants it through its ``features`` block; a workspace's
    # managers can still keep it out of theirs (``SharedRow.member_context``).
    "shared_workspaces_member_context": FeatureSpec(
        key="shared_workspaces_member_context",
        label="Your own context in shared workspaces",
        description=(
            "On your own messages in a shared workspace, the agent follows your "
            "answer preferences and can read your personal memory, without "
            "changing it."
        ),
        enabled=True,
        gate=FeatureGate.GRANT,
        requires=("shared_workspaces",),
    ),
}


def _check_requires(catalog: Mapping[str, FeatureSpec]) -> None:
    """Each parent must be declared earlier in the catalog, which rules out a
    cycle and lets one pass in catalog order resolve every child after its
    parents."""
    seen: set[str] = set()
    for key, spec in catalog.items():
        for parent in spec.requires:
            if parent not in seen:
                raise AssertionError(
                    f"feature {key!r} requires {parent!r}, which is not "
                    "declared before it"
                )
        seen.add(key)


_check_requires(FEATURES)


class SystemFlag(NamedTuple):
    enabled: bool
    gate: FeatureGate
    min_tier: int | None


def get_feature_specs() -> dict[str, FeatureSpec]:
    return dict(FEATURES)


def system_flag(key: str) -> SystemFlag:
    """Deployment-level state of a feature: catalog defaults overlaid with the
    config.yaml ``features:`` entry (unset fields inherit the catalog). A
    feature whose parent is switched off is switched off with it."""
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
    enabled = enabled and all(system_flag(parent).enabled for parent in spec.requires)
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


def org_value(org_features: Mapping[str, object] | None, key: str) -> bool | None:
    """The organization's setting for ``key``, or None where it sets none."""
    value = (org_features or {}).get(key)
    return value if isinstance(value, bool) else None


def effective_flags(
    user_overrides: Mapping[str, object] | None,
    *,
    access_tier: int | None = None,
    platform_mode: bool | None = None,
    org_features: Mapping[str, object] | None = None,
    account_unknown: bool = False,
    forced: Mapping[str, bool] | None = None,
) -> dict[str, bool]:
    """Resolve every cataloged feature for one user.

    ``user_overrides`` is the raw ``feature_overrides`` mapping from the
    user's preferences; non-bool values and overrides on non-opt gates are
    ignored rather than errored so a stale or hand-edited row can't break
    turns. Plan gates fail closed in platform mode when ``access_tier`` is
    unknown or below ``min_tier``; non-platform deployments bypass them.

    ``org_features`` is the organization tier between the deployment and the
    user: False turns a feature off whatever the user chose, True grants a
    ``grant`` feature. ``account_unknown`` (the auth service could not say
    which organization the user is in) resolves every feature off, as the
    deployment's kill switch does one feature: any of them might be one the
    organization turned off.

    ``forced`` settles a feature whatever its gate (``teams``, which only the
    account decides), before a feature that requires another is turned off
    with it, so a forced parent still carries its children.
    """
    if platform_mode is None:
        platform_mode = settings.HOST_MODE == "platform"

    resolved: dict[str, bool] = {}
    for key in FEATURES:
        flag = system_flag(key)
        if not flag.enabled or account_unknown:
            resolved[key] = False
            continue
        org = org_value(org_features, key)
        if org is False:
            resolved[key] = False
            continue
        override = (user_overrides or {}).get(key)
        override = override if isinstance(override, bool) else None
        if flag.gate is FeatureGate.NONE:
            resolved[key] = True
        elif flag.gate is FeatureGate.OPT_IN:
            resolved[key] = override is True
        elif flag.gate is FeatureGate.GRANT:
            resolved[key] = org is True or override is True
        elif flag.gate is FeatureGate.OPT_OUT:
            resolved[key] = override is not False
        else:  # FeatureGate.PLAN
            resolved[key] = not platform_mode or (
                access_tier is not None
                and flag.min_tier is not None
                and access_tier >= flag.min_tier
            )
    for key, value in (forced or {}).items():
        if key in resolved:
            resolved[key] = value
    for key, spec in FEATURES.items():
        resolved[key] = resolved[key] and all(resolved[p] for p in spec.requires)
    return resolved


def default_feature_enabled(key: str) -> bool:
    """No-user-context effective default (opt_in and platform plan gates
    resolve False). Fallback for entry points that skip per-user resolution."""
    return effective_flags(None).get(key, False)
