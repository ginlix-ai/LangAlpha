"""
Skill registry for dynamic tool loading.

This module defines the registry of available skills. A skill loads when the
agent reads its SKILL.md or a turn requests it; any tools it gates are
pre-registered but hidden until then.
"""

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any, Callable, Literal

from src.config.features import is_feature_enabled_system
from src.config.settings import get_workflow_orchestration_config


@dataclass
class SkillDefinition:
    """Definition of a loadable skill.

    Attributes:
        name: Unique skill identifier
        description: Human-readable description of what the skill does
        tool_names: Names of the tools this skill hides until it loads. The
            agent build registers each tool (RunWorkflow is a per-thread
            factory tool that can't be instantiated at import time); the skill
            only controls its visibility.
        switched_tools: Names of tools the agent binds without loading this
            skill but drops when the skill is switched off. Never hidden
            until the skill loads; listed with the skill so the Plugins page
            shows what its switch removes.
        skill_md_path: Where the agent reaches SKILL.md, not where the repo
            keeps it. A sandbox-relative suffix, matched against the reads
            the agent makes under ``.agents/skills/``, which the sync
            flattens every source into. A shipped skill's files live in the
            bundle that declares it; that path never appears here.
        hidden: Activated only programmatically (via additionalContext), so
            it stays out of listings, the manifest and the sandbox.
        system_gate: Deployment kill switch for skills owned by a config section
            rather than the feature catalog. False drops the skill everywhere.
        source_dir: Absolute host directory containing ``<name>/SKILL.md`` for
            user-tier skills. It lets the loader read the body without any
            ``skill_dirs`` search. None for platform skills.
        origin: Whose content this is. "user" entries get trust framing in the
            manifest and the user-tier wire shape in the API.
        requires: Names of registry skills this one reads at runtime. A
            required skill survives a disable of its own for as long as one
            skill declaring it is not itself disabled (see
            ``resolve_disabled_skills``).
    """

    name: str
    description: str
    tool_names: tuple[str, ...] = ()
    switched_tools: tuple[str, ...] = ()
    skill_md_path: str | None = None
    hidden: bool = False
    command: str | None = None
    # Feature-flag key (src/config/features.py) owning this skill; the skill
    # drops out of every accessor while the feature's system default is off.
    # Per-user resolution happens at agent build (SkillsMiddleware injection).
    feature: str | None = None
    system_gate: Callable[[], bool] | None = None
    source_dir: str | None = None
    origin: Literal["platform", "user"] = "platform"
    requires: tuple[str, ...] = ()

    def listed_tool_names(self) -> list[str]:
        """Every tool this skill's switch governs, for listings."""
        return [*self.tool_names, *self.switched_tools]


def _run_workflow_enabled() -> bool:
    return get_workflow_orchestration_config().enabled


def _is_enabled(
    skill: SkillDefinition, feature_resolver: Callable[[str], bool] | None = None
) -> bool:
    """Availability gate: skills whose deployment switch or owning feature is
    off drop out of every accessor (listings, lookups, sandbox sync).

    ``feature_resolver`` defaults to the system gate, the no-user-context
    default these accessors run under. The agent build injects a per-user
    resolver (via ``get_skill_registry``) so a user's opt-in/out is honored
    when skills are assembled for that build. ``system_gate`` is a deployment
    kill switch, so no resolver overrides it.
    """
    if skill.system_gate is not None and not skill.system_gate():
        return False
    if skill.feature is None:
        return True
    resolve = feature_resolver or is_feature_enabled_system
    return resolve(skill.feature)


# Skills no subagent gets: the chart guide covers annotation tools that only
# the main agent binds, so a subagent's manifest, its preloads and the names a
# subagent setting may prefer all leave it out.
SUBAGENT_WITHHELD_SKILLS: frozenset[str] = frozenset({"chart-annotation"})

# Registry of all available skills
# Skills are pre-registered at agent creation but tools are hidden until loaded
SKILL_REGISTRY: dict[str, SkillDefinition] = {
    "onboarding": SkillDefinition(
        name="onboarding",
        # Keep in sync with the `description:` in
        # plugins/langalpha_service/skills/onboarding/SKILL.md frontmatter.
        description="First run after the user saves their investing profile: import holdings and watchlists from the brokerage they connected or the screenshots they attached, then propose three first tasks (one on their portfolio or watchlist, one from their profile, one on today's market) and do the one they pick.",
        # Guidance-only: the agent saves what it learns to the profile files
        # under .agents/user/profile/, so the skill binds no tools of its own.
        # The web's kickoff names the action and may carry the day's market
        # brief as a directive. This loads alone: when the user picks the
        # health check, the body sends the agent to portfolio-diagnosis's
        # SKILL.md.
        skill_md_path="skills/onboarding/SKILL.md",
        hidden=True,
    ),
    "chart-annotation": SkillDefinition(
        name="chart-annotation",
        # Keep this text in sync with the `description:` in
        # plugins/langalpha_deliverables/skills/chart-annotation/SKILL.md
        # frontmatter, both are live (this one drives PTC discovery; the
        # frontmatter drives the sandbox skill manifest), so they must not
        # drift.
        description=(
            "Extra guidance for draw_chart_annotation, which works without "
            "loading this. Covers choosing between similar variants, aligning "
            "times to bars, several charts in one turn, and answering a chart "
            "selection the user sends."
        ),
        switched_tools=("draw_chart_annotation", "manage_chart_annotations"),
        skill_md_path="skills/chart-annotation/SKILL.md",
        # Guidance-only: every chat in the app can open the chart a drawing
        # lands on (MarketView's, or the chart tab a chat card opens), so the
        # main agent binds both annotation tools without loading this, and
        # drops them when this skill is switched off.
        # MarketView still injects it each turn, because its per-turn
        # instruction carries the active chart's symbol and timeframe.
        command="chart-annotation",
    ),
    "market-watch": SkillDefinition(
        name="market-watch",
        # Keep in sync with the `description:` in
        # plugins/langalpha_research/skills/market-watch/SKILL.md
        # frontmatter (this drives PTC discovery; the frontmatter drives the
        # sandbox skill manifest).
        description=(
            "Track live prices for the tickers central to the current task. "
            "Registers symbols for an ambient real-time price feed, keeps "
            "analysis current with the newest quotes, and re-checks staleness "
            "before stating prices."
        ),
        # Guidance-only skill: the watch_market tool (subsystem c) and the
        # <market-watch> stamping middleware (subsystem b) are registered
        # whenever the market_watch feature resolves enabled. This skill
        # just tells the agent how to use them. Activated by the frontend
        # Watch toggle via additional_context.
        skill_md_path="skills/market-watch/SKILL.md",
        command="market-watch",
        feature="market_watch",
        requires=("research-conventions",),
    ),
    "run-workflow": SkillDefinition(
        name="run-workflow",
        # Keep in sync with the `description:` in
        # plugins/langalpha_service/skills/run-workflow/SKILL.md
        # frontmatter (locked by a unit test). RunWorkflow itself is a per-thread
        # factory tool registered in agent.py, so it's gated by name here.
        description=(
            "Orchestrate parallel subagent pipelines from a JavaScript workflow "
            "script. Fan out work across many items (tickers, filings, findings) "
            "then synthesize, or run a saved workflow by name. "
            "Unlocks the RunWorkflow tool."
        ),
        tool_names=("RunWorkflow",),
        skill_md_path="skills/run-workflow/SKILL.md",
        system_gate=_run_workflow_enabled,
    ),
    "langalpha-doc": SkillDefinition(
        name="langalpha-doc",
        # Keep in sync with the `description:` in
        # plugins/langalpha_service/skills/langalpha-doc/SKILL.md
        # frontmatter (locked by a unit test).
        description=(
            "How the platform under you works: the computer and its "
            "workspaces, files and what survives a restart, conversation "
            "transcripts, saved tool results, memory, chat apps, and adding "
            "skills, MCP servers, brokerages or plugins. Read it when the "
            "user points back at an earlier conversation, when something is "
            "missing or behaves unexpectedly, before adding any of those, "
            "and when the user asks how the platform works and you don't "
            "know."
        ),
        # No `command`: the prompt points into its reference files, and a
        # user has no reason to invoke it.
        skill_md_path="skills/langalpha-doc/SKILL.md",
    ),
    "pdf": SkillDefinition(
        name="pdf",
        description="Read, fill and build PDFs: inspect structure and fonts, extract text and tables with pdfplumber and poppler, fill and flatten AcroForms with pypdf, create with reportlab, merge, split and encrypt with qpdf, and verify by rendering the page and looking at it",
        skill_md_path="skills/pdf/SKILL.md",
    ),
    "docx": SkillDefinition(
        name="docx",
        description="Word documents a human will review and edit: build with python-docx, edit an existing file in place with tracked changes and comment threads, render, validate",
        skill_md_path="skills/docx/SKILL.md",
    ),
    "pptx": SkillDefinition(
        name="pptx",
        description="PowerPoint decks built with pptxgenjs and native charts, existing decks edited in place with python-pptx, then rendered to images and audited for overflow, overlap, bounds and typography before delivery",
        skill_md_path="skills/pptx/SKILL.md",
    ),
    "xlsx": SkillDefinition(
        name="xlsx",
        description="Excel workbooks with live formulas: build or edit .xlsx models with openpyxl, recalculate with IronCalc or LibreOffice, audit conventions, profile messy uploads, render for review",
        skill_md_path="skills/xlsx/SKILL.md",
    ),
    "research-conventions": SkillDefinition(
        name="research-conventions",
        # Keep in sync with the `description:` in
        # plugins/langalpha_research/skills/research-conventions/SKILL.md
        # frontmatter (this drives PTC discovery; the frontmatter drives the
        # sandbox skill manifest).
        description=(
            "The evidence, judgement, intake and market-data rules every "
            "research deliverable follows. Read before the first deliverable "
            "of a research task; when a number has no source; when two "
            "sources disagree; when a figure may be stale; when deciding what "
            "to ask the user; before any valuation, thesis or recommendation."
        ),
        # No `command`: the shared layer is read by the research skills that
        # point into it, never invoked as a slash command of its own.
        skill_md_path="skills/research-conventions/SKILL.md",
    ),
    "comps-analysis": SkillDefinition(
        name="comps-analysis",
        description="Comparable company analysis: peer set, operating metrics, valuation multiples, statistics and an implied value. Triggers on comps, trading comparables, how does it trade against peers, peer benchmarking, what multiple should it get.",
        skill_md_path="skills/comps-analysis/SKILL.md",
        command="comps-analysis",
        requires=("research-conventions",),
    ),
    "dcf-model": SkillDefinition(
        name="dcf-model",
        description="Build a DCF valuation in Excel: FCF projections, WACC, terminal value, scenarios, sensitivity grids, reverse DCF. Triggers on build a DCF, what is it worth, intrinsic value, fair value, price target from cash flows.",
        skill_md_path="skills/dcf-model/SKILL.md",
        command="dcf-model",
        requires=("research-conventions",),
    ),
    "earnings-preview": SkillDefinition(
        name="earnings-preview",
        description="Pre-print setup for a company about to report: the expectation bar, EPS-quality watch, call questions, scenarios and the reaction framework. Triggers on earnings preview, what to watch for [company] earnings, pre-earnings setup, preview Q[N].",
        skill_md_path="skills/earnings-preview/SKILL.md",
        command="earnings-preview",
        requires=("research-conventions",),
    ),
    "company-profile": SkillDefinition(
        name="company-profile",
        description="One-page company profile slide, four quadrants of overview, financial summary, share price chart and key facts. Also the compact profile paragraph when it has to sit inside a memo, a deck or a chat reply. Triggers on one-page profile, tear sheet, company snapshot, profile slide, quick profile of [company].",
        skill_md_path="skills/company-profile/SKILL.md",
        command="company-profile",
        requires=("research-conventions",),
    ),
    "idea-generation": SkillDefinition(
        name="idea-generation",
        description="Find long and short candidates across a universe when no name is on the table yet: mandate, universe validation, archetype screens, thematic sweep, triage, idea cards, idea log. Triggers on idea generation, stock screen, find ideas, what looks interesting, screen for, new ideas, pitch me something.",
        skill_md_path="skills/idea-generation/SKILL.md",
        command="idea-generation",
        requires=("research-conventions",),
    ),
    "check-model": SkillDefinition(
        name="check-model",
        description="Audit a model somebody already built, and report on it without editing it: structure, formulas, integrity identities, source tie-out and reasonableness, ending in a routed issue log. Triggers on check my formulas, QA this spreadsheet, audit model, model review, something is off in my model, why does my balance sheet not balance.",
        skill_md_path="skills/check-model/SKILL.md",
        command="check-model",
        requires=("research-conventions",),
    ),
    "morning-note": SkillDefinition(
        name="morning-note",
        description="Daily research briefing on overnight news, pre-market movers, earnings and macro events. Triggers on morning note, morning meeting, what happened overnight, morning call prep, daily note, trade idea for the open.",
        skill_md_path="skills/morning-note/SKILL.md",
        command="morning-note",
        requires=("research-conventions",),
    ),
    "catalyst-calendar": SkillDefinition(
        name="catalyst-calendar",
        description="Dated events ranked by what they can change: earnings, regulatory decisions, flow events, macro releases, with prep owners and a weekly preview. Triggers on catalyst calendar, upcoming events, what is coming up, earnings calendar, event calendar, catalyst tracker, what should I prepare for.",
        skill_md_path="skills/catalyst-calendar/SKILL.md",
        command="catalyst-calendar",
        requires=("research-conventions",),
    ),
    "check-deck": SkillDefinition(
        name="check-deck",
        description="QC an investment deck (a .pptx) before it circulates: number consistency, chart and narrative alignment, source coverage, language, then a circulation verdict. Triggers on check this deck, deck QC, review my presentation, is this ready to send, proofread the pitch book.",
        skill_md_path="skills/check-deck/SKILL.md",
        command="check-deck",
        requires=("research-conventions",),
    ),
    "thesis-tracker": SkillDefinition(
        name="thesis-tracker",
        description="Keep a live thesis honest: pillar status, evidence ledger, monitoring triggers, drift detection. Triggers on thesis tracker, thesis update, is the thesis still intact, post-earnings thesis check, portfolio thesis review, re-underwrite.",
        skill_md_path="skills/thesis-tracker/SKILL.md",
        command="thesis-tracker",
        requires=("research-conventions",),
    ),
    "portfolio-diagnosis": SkillDefinition(
        name="portfolio-diagnosis",
        # Keep in sync with the `description:` in
        # plugins/langalpha_research/skills/portfolio-diagnosis/SKILL.md
        # frontmatter (locked by a unit test).
        description=(
            "Health check of the user's own portfolio against their investing "
            "profile: concentration, exposure by sector, region and asset "
            "class, cash, currency mix, and risk fit (volatility, beta, "
            "drawdown), ending in a few next steps. Triggers on diagnose my "
            "portfolio, portfolio review, portfolio health check, am I too "
            "concentrated, how risky is my portfolio."
        ),
        skill_md_path="skills/portfolio-diagnosis/SKILL.md",
        # The figures come from one execute_code call. Onboarding reads it by
        # path for the health check; the command reaches it any time after.
        command="portfolio-diagnosis",
    ),
    "model-update": SkillDefinition(
        name="model-update",
        description="Refresh an existing financial model after a print, a guidance change, a consensus move, a filing, a KPI release or a capital-structure change. Triggers on update the model, roll it forward, the new quarter is out, revise estimates, refresh the price target.",
        skill_md_path="skills/model-update/SKILL.md",
        command="model-update",
        requires=("research-conventions",),
    ),
    "impact-analysis": SkillDefinition(
        name="impact-analysis",
        description="Translate a macro, policy, rate, commodity, geopolitical or industry shock into equity exposure through a named transmission channel. Triggers on what does X mean for, impact of, exposure to, who benefits from, tariff, rate cut, oil shock, new regulation.",
        skill_md_path="skills/impact-analysis/SKILL.md",
        command="impact-analysis",
        requires=("research-conventions",),
    ),
    "trade-pitch": SkillDefinition(
        name="trade-pitch",
        description="Turn finished analysis on one named security into a position: variant view, falsifiable evidence, scenario tree, expression, risk and monitoring. Triggers on pitch this name, should I buy, a long idea or short idea on a named stock, position, trade recommendation, risk reward.",
        skill_md_path="skills/trade-pitch/SKILL.md",
        command="trade-pitch",
        requires=("research-conventions",),
    ),
    "3-statements": SkillDefinition(
        name="3-statements",
        description="Build or repair an integrated three-statement model: linked IS, BS and CF, supporting schedules, scenarios and a Checks sheet. Triggers on three-statement model, build me a model, link the statements, populate this template, make my balance sheet balance.",
        skill_md_path="skills/3-statements/SKILL.md",
        command="3-statement-model",
        requires=("research-conventions",),
    ),
    "earnings-analysis": SkillDefinition(
        name="earnings-analysis",
        description="Post-print earnings update for a covered name: beat/miss decomposition, EPS quality, transcript debate map, estimate revisions, thesis impact. Also the call-only ask that wants the transcript Q&A and the debate map alone. Triggers on earnings update, post-earnings report, analyze quarterly results, Q[N] update, what management said on the call.",
        skill_md_path="skills/earnings-analysis/SKILL.md",
        command="earnings-analysis",
        requires=("research-conventions",),
    ),
    "sector-overview": SkillDefinition(
        name="sector-overview",
        description="Sector and industry landscape report, built on the sector's own archetypes, metrics and valuation lenses. Triggers on sector overview, industry landscape, industry primer, sector deep dive, market map.",
        skill_md_path="skills/sector-overview/SKILL.md",
        command="sector-overview",
        requires=("research-conventions",),
    ),
    "competitive-analysis": SkillDefinition(
        name="competitive-analysis",
        description="Competitive landscape analysis: positioning, scorecards, moat assessment, market share trends. Triggers on competitive analysis, competitive landscape, competitor benchmarking, moat assessment, market share, who are the competitors.",
        skill_md_path="skills/competitive-analysis/SKILL.md",
        command="competitive-analysis",
        requires=("research-conventions",),
    ),
    "inline-widget": SkillDefinition(
        name="inline-widget",
        description="Inline HTML widgets: charts, dashboards, data tables rendered directly in the chat via ShowWidget",
        skill_md_path="skills/inline-widget/SKILL.md",
    ),
    "interactive-dashboard": SkillDefinition(
        name="interactive-dashboard",
        description="Interactive web dashboards: stock trackers, sector heatmaps, portfolio monitors, served via preview URL",
        skill_md_path="skills/interactive-dashboard/SKILL.md",
        command="dashboard",
    ),
    "initiating-coverage": SkillDefinition(
        name="initiating-coverage",
        description="First-time coverage of a company, or a refresh of coverage already published, run one task per request by default: company research, financial model, valuation, charts, then a 30 to 50 page report with a model. Triggers on initiation report, initiate coverage, initiating coverage, full equity research report, refresh the initiation.",
        skill_md_path="skills/initiating-coverage/SKILL.md",
        command="initiating-coverage",
        requires=("research-conventions",),
    ),
    "web-scraping": SkillDefinition(
        name="web-scraping",
        description="Web scraping: scrape_page / scrape_pages MCP tools for fetching pages as markdown, HTML, or text (fast HTTP, browser rendering, anti-bot stealth), plus the direct Scrapling Python API for selectors, sessions, and spiders",
        skill_md_path="skills/web-scraping/SKILL.md",
        command="web-scraping",
    ),
    "html-report": SkillDefinition(
        name="html-report",
        description="Self-contained styled HTML reports written to the task directory: PDF-exportable research documents with inline data, charts, and theme-aware CSS",
        skill_md_path="skills/html-report/SKILL.md",
        command="html-report",
    ),
    "ui-design": SkillDefinition(
        name="ui-design",
        description="Design-quality reference for financial-research visual output: typography, color, composition, and avoiding generic AI aesthetics",
        skill_md_path="skills/ui-design/SKILL.md",
    ),
}

# Built-ins only the retired Flash agent loaded. Threads it started still name
# them, and no build has a SKILL.md for them.
RETIRED_SKILLS = frozenset({"secretary", "automation"})


def build_user_skill_definitions(
    specs: Sequence[Any],
    *,
    source_dir: str | None,
    workspace_source_dir: str | None = None,
) -> dict[str, SkillDefinition]:
    """Build registry entries for a user's uploaded skills.

    ``specs`` is duck-typed (needs ``.name``/``.description``, optional
    ``.command``/``.workspace_scoped``) so the server's spec dataclass never
    has to be imported here. Every entry is docs-only (no tools) and carries
    ``skill_md_path="skills/<name>/SKILL.md"`` so auto-load on ``Read`` of
    ``.agents/skills/<name>/SKILL.md`` matches with no extra code.

    The two tiers read from different host dirs (see
    ``load_user_skill_bundle``); a spec whose tier has no dir is skipped
    rather than pointed at the other one's.
    """
    out: dict[str, SkillDefinition] = {}
    for spec in specs:
        root = (
            workspace_source_dir
            if getattr(spec, "workspace_scoped", False)
            else source_dir
        )
        if root is None:
            continue
        out[spec.name] = SkillDefinition(
            name=spec.name,
            description=spec.description,
            skill_md_path=f"skills/{spec.name}/SKILL.md",
            command=getattr(spec, "command", None) or spec.name,
            origin="user",
            source_dir=root,
        )
    return out


def resolve_disabled_skills(disabled: Iterable[str]) -> frozenset[str]:
    """Drop from *disabled* every skill another, undisabled skill declares
    in ``requires``.

    A shared layer like ``research-conventions`` carries no command of its
    own and is reached only through the skills that mandate reading it, so
    switching it off while one of them is on would point that skill's first
    mandated read at a file the sandbox sync never uploads. The dependent's
    own switch is the one that takes the layer away.
    """
    blocked = frozenset(disabled)
    if not blocked:
        return blocked
    return blocked - {
        required
        for name, skill in SKILL_REGISTRY.items()
        if name not in blocked
        for required in skill.requires
    }


def build_effective_skill_registry(
    *,
    feature_resolver: Callable[[str], bool] | None = None,
    disabled_skills: Iterable[str] = (),
    user_skills: Sequence[Any] = (),
    user_skill_dir: str | None = None,
    workspace_skill_dir: str | None = None,
) -> dict[str, SkillDefinition]:
    """Assemble the per-build registry every agent surface consumes.

    Feature gating, minus the user's builtin disables (as
    ``resolve_disabled_skills`` reads them), plus the user's uploaded
    skills. The one assembly path shared by the agent build, the subagent
    compiler and the server's per-turn checks, so a gate applied in one
    can't be missed in another.
    """
    registry = get_skill_registry(feature_resolver=feature_resolver)
    for name in resolve_disabled_skills(disabled_skills):
        registry.pop(name, None)
    registry.update(
        build_user_skill_definitions(
            user_skills,
            source_dir=user_skill_dir,
            workspace_source_dir=workspace_skill_dir,
        )
    )
    return registry


def get_skill_registry(
    *,
    feature_resolver: Callable[[str], bool] | None = None,
) -> dict[str, SkillDefinition]:
    """Get the enabled skills.

    Args:
        feature_resolver: Optional per-user feature gate. Defaults to the
              system gate; the agent build passes the build's own
              ``feature_enabled`` so opt-in/out drops the skill for that user.

    Returns:
        Dict of skill name to SkillDefinition for enabled skills
    """
    return {
        name: skill
        for name, skill in SKILL_REGISTRY.items()
        if _is_enabled(skill, feature_resolver)
    }


def skills_outside_sandbox() -> frozenset[str]:
    """Built-in skills the sandbox does not carry, even when their directory
    is on disk.

    One gated off (``_is_enabled``) has nothing to offer there, and a hidden
    one reaches the agent only as a body injected from the host. The sandbox
    sync leaves these out and a summary does not send the agent back to them.
    A name the registry does not know (a user's skill) is never in it.
    """
    return frozenset(
        name
        for name, skill in SKILL_REGISTRY.items()
        if skill.hidden or not _is_enabled(skill)
    )


def get_skill(skill_name: str) -> SkillDefinition | None:
    """Get an enabled skill definition by name, or None."""
    skill = SKILL_REGISTRY.get(skill_name)
    if skill is None or not _is_enabled(skill):
        return None
    return skill


def get_command_to_skill_map() -> dict[str, str]:
    """Map slash command names to skill names.

    Returns a dict where keys are command strings (e.g. "3-statement-model")
    and values are skill names (e.g. "3-statements"). Only includes enabled
    skills that have a non-None command field.
    """
    return {
        skill.command: name
        for name, skill in SKILL_REGISTRY.items()
        if skill.command and _is_enabled(skill)
    }


def list_skills() -> list[dict[str, Any]]:
    """List available skills with their metadata.

    Hidden skills are excluded from listings (they can only be activated
    programmatically via additionalContext).

    Returns:
        List of skill info dicts with name, description, and tool count
    """
    return [
        {
            "name": skill.name,
            "description": skill.description,
            "tool_count": len(skill.listed_tool_names()),
            "tools": skill.listed_tool_names(),
            "command": skill.command,
        }
        for skill in SKILL_REGISTRY.values()
        if _is_enabled(skill) and not skill.hidden
    ]
