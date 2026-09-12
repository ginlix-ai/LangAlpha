"""
Skill registry for dynamic tool loading.

This module defines the registry of available skills that can be dynamically
loaded by the agent via the load_skill mechanism. Each skill contains a set
of tools that are pre-registered but hidden until the skill is loaded.
"""

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any, Callable, Literal

from src.config.features import is_feature_enabled_system
from src.config.settings import get_workflow_orchestration_config
from src.tools.automation import AUTOMATION_TOOLS
from src.tools.chart_annotation import CHART_ANNOTATION_TOOLS
from src.tools.onboarding import ONBOARDING_TOOLS
from src.tools.user_profile import USER_PROFILE_TOOLS

# Type alias for agent modes that can use skills
SkillMode = Literal["ptc", "flash"]


@dataclass
class SkillDefinition:
    """Definition of a loadable skill.

    Attributes:
        name: Unique skill identifier
        description: Human-readable description of what the skill does
        tools: List of LangChain tools included in this skill
        tool_names: Names of externally-registered tools gated by this skill,
            for per-thread factory tools that can't be instantiated at import
            time (e.g. RunWorkflow). The tool object is registered by the agent
            factory; the skill only controls its visibility.
        skill_md_path: Where the agent reaches SKILL.md, not where the repo
            keeps it. A sandbox-relative suffix, matched against the reads
            the agent makes under ``.agents/skills/``, which the sync
            flattens every source into. A shipped skill's files live in the
            bundle that declares it; that path never appears here.
        exposure: Which agent mode(s) can use this skill ("ptc", "flash", or "both")
        system_gate: Deployment kill switch for skills owned by a config section
            rather than the feature catalog. False drops the skill everywhere.
        source_dir: Absolute host directory containing ``<name>/SKILL.md`` for
            user-tier skills. It lets the loader read the body without any
            ``skill_dirs`` search. None for platform skills.
        origin: Whose content this is. "user" entries get trust framing in the
            manifest and the user-tier wire shape in the API.
    """

    name: str
    description: str
    tools: list[Any]
    tool_names: tuple[str, ...] = ()
    skill_md_path: str | None = None
    exposure: Literal["ptc", "flash", "both", "hidden"] = "ptc"
    command: str | None = None
    # Feature-flag key (src/config/features.py) owning this skill; the skill
    # drops out of every accessor while the feature's system default is off.
    # Per-user resolution happens at agent build (SkillsMiddleware injection).
    feature: str | None = None
    system_gate: Callable[[], bool] | None = None
    source_dir: str | None = None
    origin: Literal["platform", "user"] = "platform"

    def get_tool_names(self) -> list[str]:
        """Get list of tool names in this skill (including externally-registered ones)."""
        return [getattr(t, "name", str(t)) for t in self.tools] + list(self.tool_names)

    def format_tool_descriptions(self, max_desc_len: int = 200) -> str:
        """Format tool descriptions for display.

        Args:
            max_desc_len: Maximum length for each tool's description text.

        Returns:
            Formatted string with one line per tool.
        """
        lines = []
        for t in self.tools:
            name = getattr(t, "name", str(t))
            desc = getattr(t, "description", "No description")
            if len(desc) > max_desc_len:
                desc = desc[:max_desc_len] + "..."
            lines.append(f"  - **{name}**: {desc}")
        return "\n".join(lines)


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


def _matches_mode(skill: SkillDefinition, mode: SkillMode | None) -> bool:
    """Check if a skill matches the given agent mode.

    Returns True if mode is None (no filter), skill.exposure matches,
    or skill is hidden (hidden skills match any mode for explicit lookup
    but are excluded from listings by callers).
    """
    if mode is None:
        return True
    if skill.exposure == "hidden":
        return True  # Available in all modes, excluded from listings separately
    return skill.exposure == mode or skill.exposure == "both"


# Registry of all available skills
# Skills are pre-registered at agent creation but tools are hidden until loaded
SKILL_REGISTRY: dict[str, SkillDefinition] = {
    "user-profile": SkillDefinition(
        name="user-profile",
        description="Manage user profile including watchlists, portfolio, and preferences.",
        tools=USER_PROFILE_TOOLS,
        skill_md_path="skills/user-profile/SKILL.md",
        # Flash-only: PTC edits user data via .agents/user/profile/*.json through
        # the UserDataBackend filesystem surface, so the structured skill tools
        # are redundant there. Flash has no filesystem, so it keeps them.
        exposure="flash",
    ),
    "onboarding": SkillDefinition(
        name="onboarding",
        description="First-time user onboarding to set up investment profile, watchlists, portfolio, and preferences.",
        tools=ONBOARDING_TOOLS,
        skill_md_path="skills/onboarding/SKILL.md",
        exposure="hidden",
    ),
    "chart-annotation": SkillDefinition(
        name="chart-annotation",
        # Keep this text in sync with the `description:` in
        # plugins/langalpha_deliverables/skills/chart-annotation/SKILL.md
        # frontmatter, both are live (this one drives PTC discovery; the
        # frontmatter drives the sandbox/Flash skill manifest), so they must
        # not drift.
        description=(
            "Draw price lines, trendlines, zones, and event markers directly on a "
            "stock's price chart. Reach for it whenever you'd otherwise describe a "
            "level, pattern, or event in prose. Renders live on MarketView and as a "
            "clickable preview card in any other chat."
        ),
        tools=CHART_ANNOTATION_TOOLS,
        skill_md_path="skills/chart-annotation/SKILL.md",
        # Discoverable in both modes so the agent can self-load it whenever the
        # user asks to annotate, including from the standalone chat page, where
        # the result renders as a preview card. MarketView also injects it
        # proactively (with the active symbol) for turn-1 availability.
        exposure="both",
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
        tools=[],
        # Guidance-only skill: the watch_market tool (subsystem c) and the
        # <market-watch> stamping middleware (subsystem b) are registered
        # whenever the market_watch feature resolves enabled. This skill
        # just tells the agent how to use them. Activated by the frontend
        # Watch toggle via additional_context.
        skill_md_path="skills/market-watch/SKILL.md",
        exposure="ptc",
        command="market-watch",
        feature="market_watch",
    ),
    "secretary": SkillDefinition(
        name="secretary",
        description="Workspace and research management. Dispatch analyses, monitor running agents, manage workspaces and threads.",
        tools=[],
        skill_md_path="skills/secretary/SKILL.md",
        exposure="flash",
        command="secretary",
    ),
    "automation": SkillDefinition(
        name="automation",
        description="Create and manage scheduled and price-triggered automations.",
        tools=AUTOMATION_TOOLS,
        skill_md_path="skills/automation/SKILL.md",
        exposure="both",
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
        tools=[],
        tool_names=("RunWorkflow",),
        skill_md_path="skills/run-workflow/SKILL.md",
        exposure="ptc",
        system_gate=_run_workflow_enabled,
    ),
    "pdf": SkillDefinition(
        name="pdf",
        description="Read, fill and build PDFs: inspect structure and fonts, extract text and tables with pdfplumber and poppler, fill and flatten AcroForms with pypdf, create with reportlab, merge, split and encrypt with qpdf, and verify by rendering the page and looking at it",
        tools=[],
        skill_md_path="skills/pdf/SKILL.md",
        exposure="ptc",
    ),
    "docx": SkillDefinition(
        name="docx",
        description="Word documents a human will review and edit: build with python-docx, edit an existing file in place with tracked changes and comment threads, render, validate",
        tools=[],
        skill_md_path="skills/docx/SKILL.md",
        exposure="ptc",
    ),
    "pptx": SkillDefinition(
        name="pptx",
        description="PowerPoint decks built with pptxgenjs and native charts, existing decks edited in place with python-pptx, then rendered to images and audited for overflow, overlap, bounds and typography before delivery",
        tools=[],
        skill_md_path="skills/pptx/SKILL.md",
        exposure="ptc",
    ),
    "xlsx": SkillDefinition(
        name="xlsx",
        description="Excel workbooks with live formulas: build or edit .xlsx models with openpyxl, recalculate with IronCalc or LibreOffice, audit conventions, profile messy uploads, render for review",
        tools=[],
        skill_md_path="skills/xlsx/SKILL.md",
        exposure="ptc",
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
        tools=[],
        # No `command`: the shared layer is read by the research skills that
        # point into it, never invoked as a slash command of its own.
        skill_md_path="skills/research-conventions/SKILL.md",
        exposure="ptc",
    ),
    "comps-analysis": SkillDefinition(
        name="comps-analysis",
        description="Comparable company analysis: peer set, operating metrics, valuation multiples, statistics and an implied value. Triggers on comps, trading comparables, how does it trade against peers, peer benchmarking, what multiple should it get.",
        tools=[],
        skill_md_path="skills/comps-analysis/SKILL.md",
        exposure="ptc",
        command="comps-analysis",
    ),
    "dcf-model": SkillDefinition(
        name="dcf-model",
        description="Build a DCF valuation in Excel: FCF projections, WACC, terminal value, scenarios, sensitivity grids, reverse DCF. Triggers on build a DCF, what is it worth, intrinsic value, fair value, price target from cash flows.",
        tools=[],
        skill_md_path="skills/dcf-model/SKILL.md",
        exposure="ptc",
        command="dcf-model",
    ),
    "earnings-preview": SkillDefinition(
        name="earnings-preview",
        description="Pre-print setup for a company about to report: the expectation bar, EPS-quality watch, call questions, scenarios and the reaction framework. Triggers on earnings preview, what to watch for [company] earnings, pre-earnings setup, preview Q[N].",
        tools=[],
        skill_md_path="skills/earnings-preview/SKILL.md",
        exposure="ptc",
        command="earnings-preview",
    ),
    "company-profile": SkillDefinition(
        name="company-profile",
        description="One-page company profile slide, four quadrants of overview, financial summary, share price chart and key facts. Also the compact profile paragraph when it has to sit inside a memo, a deck or a chat reply. Triggers on one-page profile, tear sheet, company snapshot, profile slide, quick profile of [company].",
        tools=[],
        skill_md_path="skills/company-profile/SKILL.md",
        exposure="ptc",
        command="company-profile",
    ),
    "idea-generation": SkillDefinition(
        name="idea-generation",
        description="Find long and short candidates across a universe when no name is on the table yet: mandate, universe validation, archetype screens, thematic sweep, triage, idea cards, idea log. Triggers on idea generation, stock screen, find ideas, what looks interesting, screen for, new ideas, pitch me something.",
        tools=[],
        skill_md_path="skills/idea-generation/SKILL.md",
        exposure="ptc",
        command="idea-generation",
    ),
    "check-model": SkillDefinition(
        name="check-model",
        description="Audit a model somebody already built, and report on it without editing it: structure, formulas, integrity identities, source tie-out and reasonableness, ending in a routed issue log. Triggers on check my formulas, QA this spreadsheet, audit model, model review, something is off in my model, why does my balance sheet not balance.",
        tools=[],
        skill_md_path="skills/check-model/SKILL.md",
        exposure="ptc",
        command="check-model",
    ),
    "morning-note": SkillDefinition(
        name="morning-note",
        description="Daily research briefing on overnight news, pre-market movers, earnings and macro events. Triggers on morning note, morning meeting, what happened overnight, morning call prep, daily note, trade idea for the open.",
        tools=[],
        skill_md_path="skills/morning-note/SKILL.md",
        exposure="ptc",
        command="morning-note",
    ),
    "catalyst-calendar": SkillDefinition(
        name="catalyst-calendar",
        description="Dated events ranked by what they can change: earnings, regulatory decisions, flow events, macro releases, with prep owners and a weekly preview. Triggers on catalyst calendar, upcoming events, what is coming up, earnings calendar, event calendar, catalyst tracker, what should I prepare for.",
        tools=[],
        skill_md_path="skills/catalyst-calendar/SKILL.md",
        exposure="ptc",
        command="catalyst-calendar",
    ),
    "check-deck": SkillDefinition(
        name="check-deck",
        description="QC an investment deck (a .pptx) before it circulates: number consistency, chart and narrative alignment, source coverage, language, then a circulation verdict. Triggers on check this deck, deck QC, review my presentation, is this ready to send, proofread the pitch book.",
        tools=[],
        skill_md_path="skills/check-deck/SKILL.md",
        exposure="ptc",
        command="check-deck",
    ),
    "thesis-tracker": SkillDefinition(
        name="thesis-tracker",
        description="Keep a live thesis honest: pillar status, evidence ledger, monitoring triggers, drift detection. Triggers on thesis tracker, thesis update, is the thesis still intact, post-earnings thesis check, portfolio thesis review, re-underwrite.",
        tools=[],
        skill_md_path="skills/thesis-tracker/SKILL.md",
        exposure="ptc",
        command="thesis-tracker",
    ),
    "model-update": SkillDefinition(
        name="model-update",
        description="Refresh an existing financial model after a print, a guidance change, a consensus move, a filing, a KPI release or a capital-structure change. Triggers on update the model, roll it forward, the new quarter is out, revise estimates, refresh the price target.",
        tools=[],
        skill_md_path="skills/model-update/SKILL.md",
        exposure="ptc",
        command="model-update",
    ),
    "impact-analysis": SkillDefinition(
        name="impact-analysis",
        description="Translate a macro, policy, rate, commodity, geopolitical or industry shock into equity exposure through a named transmission channel. Triggers on what does X mean for, impact of, exposure to, who benefits from, tariff, rate cut, oil shock, new regulation.",
        tools=[],
        skill_md_path="skills/impact-analysis/SKILL.md",
        exposure="ptc",
        command="impact-analysis",
    ),
    "trade-pitch": SkillDefinition(
        name="trade-pitch",
        description="Turn finished analysis on one named security into a position: variant view, falsifiable evidence, scenario tree, expression, risk and monitoring. Triggers on pitch this name, should I buy, a long idea or short idea on a named stock, position, trade recommendation, risk reward.",
        tools=[],
        skill_md_path="skills/trade-pitch/SKILL.md",
        exposure="ptc",
        command="trade-pitch",
    ),
    "3-statements": SkillDefinition(
        name="3-statements",
        description="Build or repair an integrated three-statement model: linked IS, BS and CF, supporting schedules, scenarios and a Checks sheet. Triggers on three-statement model, build me a model, link the statements, populate this template, make my balance sheet balance.",
        tools=[],
        skill_md_path="skills/3-statements/SKILL.md",
        exposure="ptc",
        command="3-statement-model",
    ),
    "earnings-analysis": SkillDefinition(
        name="earnings-analysis",
        description="Post-print earnings update for a covered name: beat/miss decomposition, EPS quality, transcript debate map, estimate revisions, thesis impact. Also the call-only ask that wants the transcript Q&A and the debate map alone. Triggers on earnings update, post-earnings report, analyze quarterly results, Q[N] update, what management said on the call.",
        tools=[],
        skill_md_path="skills/earnings-analysis/SKILL.md",
        exposure="ptc",
        command="earnings-analysis",
    ),
    "sector-overview": SkillDefinition(
        name="sector-overview",
        description="Sector and industry landscape report, built on the sector's own archetypes, metrics and valuation lenses. Triggers on sector overview, industry landscape, industry primer, sector deep dive, market map.",
        tools=[],
        skill_md_path="skills/sector-overview/SKILL.md",
        exposure="ptc",
        command="sector-overview",
    ),
    "competitive-analysis": SkillDefinition(
        name="competitive-analysis",
        description="Competitive landscape analysis: positioning, scorecards, moat assessment, market share trends. Triggers on competitive analysis, competitive landscape, competitor benchmarking, moat assessment, market share, who are the competitors.",
        tools=[],
        skill_md_path="skills/competitive-analysis/SKILL.md",
        exposure="ptc",
        command="competitive-analysis",
    ),
    "self-improve": SkillDefinition(
        name="self-improve",
        description="Report issues and propose fixes to improve your own capabilities when you encounter errors or limitations",
        tools=[],
        skill_md_path="skills/self-improve/SKILL.md",
        exposure="ptc",
        command="report-issue",
    ),
    "inline-widget": SkillDefinition(
        name="inline-widget",
        description="Inline HTML widgets: charts, dashboards, data tables rendered directly in the chat via ShowWidget",
        tools=[],
        skill_md_path="skills/inline-widget/SKILL.md",
        exposure="ptc",
    ),
    "interactive-dashboard": SkillDefinition(
        name="interactive-dashboard",
        description="Interactive web dashboards: stock trackers, sector heatmaps, portfolio monitors, served via preview URL",
        tools=[],
        skill_md_path="skills/interactive-dashboard/SKILL.md",
        exposure="ptc",
        command="dashboard",
    ),
    "initiating-coverage": SkillDefinition(
        name="initiating-coverage",
        description="First-time coverage of a company, or a refresh of coverage already published, run one task per request by default: company research, financial model, valuation, charts, then a 30 to 50 page report with a model. Triggers on initiation report, initiate coverage, initiating coverage, full equity research report, refresh the initiation.",
        tools=[],
        skill_md_path="skills/initiating-coverage/SKILL.md",
        exposure="ptc",
        command="initiating-coverage",
    ),
    "web-scraping": SkillDefinition(
        name="web-scraping",
        description="Web scraping: scrape_page / scrape_pages MCP tools for fetching pages as markdown, HTML, or text (fast HTTP, browser rendering, anti-bot stealth), plus the direct Scrapling Python API for selectors, sessions, and spiders",
        tools=[],
        skill_md_path="skills/web-scraping/SKILL.md",
        exposure="ptc",
        command="web-scraping",
    ),
    "html-report": SkillDefinition(
        name="html-report",
        description="Self-contained styled HTML reports written to the task directory: PDF-exportable research documents with inline data, charts, and theme-aware CSS",
        tools=[],
        skill_md_path="skills/html-report/SKILL.md",
        exposure="ptc",
        command="html-report",
    ),
    "ui-design": SkillDefinition(
        name="ui-design",
        description="Design-quality reference for financial-research visual output: typography, color, composition, and avoiding generic AI aesthetics",
        tools=[],
        skill_md_path="skills/ui-design/SKILL.md",
        exposure="ptc",
    ),
}


def build_user_skill_definitions(
    specs: Sequence[Any],
    *,
    source_dir: str | None,
    workspace_source_dir: str | None = None,
) -> dict[str, SkillDefinition]:
    """Build registry entries for a user's uploaded skills.

    ``specs`` is duck-typed (needs ``.name``/``.description``, optional
    ``.command``/``.workspace_scoped``) so the server's spec dataclass never
    has to be imported here. Every entry is docs-only (no tools), exposed in
    both modes, and carries ``skill_md_path="skills/<name>/SKILL.md"`` so PTC
    auto-load on ``Read`` of ``.agents/skills/<name>/SKILL.md`` matches with no
    extra code.

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
            tools=[],
            skill_md_path=f"skills/{spec.name}/SKILL.md",
            exposure="both",
            command=getattr(spec, "command", None) or spec.name,
            origin="user",
            source_dir=root,
        )
    return out


def build_effective_skill_registry(
    mode: SkillMode | None,
    *,
    feature_resolver: Callable[[str], bool] | None = None,
    disabled_skills: Iterable[str] = (),
    user_skills: Sequence[Any] = (),
    user_skill_dir: str | None = None,
    workspace_skill_dir: str | None = None,
) -> dict[str, SkillDefinition]:
    """Assemble the per-build registry every agent surface consumes.

    Mode + feature gating, minus the user's builtin disables, plus the user's
    uploaded skills. The one assembly path shared by the PTC build, the Flash
    build, and the subagent compiler, so a gate applied in one can't be missed
    in another.
    """
    registry = get_skill_registry(mode, feature_resolver=feature_resolver)
    for name in disabled_skills:
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
    mode: SkillMode | None = None,
    *,
    feature_resolver: Callable[[str], bool] | None = None,
) -> dict[str, SkillDefinition]:
    """Get the skill registry filtered by agent mode.

    Args:
        mode: Optional agent mode filter. None applies no mode filter.
              Feature-gated skills are excluded when disabled regardless.
        feature_resolver: Optional per-user feature gate. Defaults to the
              system gate; the agent build passes the build's own
              ``feature_enabled`` so opt-in/out drops the skill for that user.

    Returns:
        Dict of skill name to SkillDefinition for matching skills
    """
    return {
        name: skill
        for name, skill in SKILL_REGISTRY.items()
        if _is_enabled(skill, feature_resolver) and _matches_mode(skill, mode)
    }


def get_sandbox_skill_names() -> set[str]:
    """Get names of skills that should be synced to sandbox.

    Returns skills with exposure "ptc" or "both", NOT "flash" (flash-only
    skills are never accessed in sandboxes).

    Returns:
        Set of skill names for sandbox upload
    """
    return {
        name
        for name, skill in SKILL_REGISTRY.items()
        if _is_enabled(skill) and skill.exposure in ("ptc", "both")
    }


def get_skill(skill_name: str, mode: SkillMode | None = None) -> SkillDefinition | None:
    """Get a skill definition by name, optionally validating exposure mode.

    Args:
        skill_name: Name of the skill to retrieve
        mode: Optional agent mode. If provided, only returns the skill if
              its exposure matches the mode.

    Returns:
        SkillDefinition if found and mode-compatible, None otherwise
    """
    skill = SKILL_REGISTRY.get(skill_name)
    if skill is None or not _is_enabled(skill):
        return None
    if mode is not None and not _matches_mode(skill, mode):
        return None
    return skill


def get_all_skill_tools(mode: SkillMode | None = None) -> list[Any]:
    """Get all tools from registered skills, optionally filtered by mode.

    Used during agent creation to pre-register all tools with ToolNode.

    Args:
        mode: Optional agent mode filter. None returns tools from all skills.

    Returns:
        Flat list of all tools from matching skills
    """
    all_tools = []
    for skill in SKILL_REGISTRY.values():
        if _is_enabled(skill) and _matches_mode(skill, mode):
            all_tools.extend(skill.tools)
    return all_tools


def get_all_skill_tool_names(mode: SkillMode | None = None) -> set[str]:
    """Get names of all tools from registered skills, optionally filtered by mode.

    Used by middleware to identify which tools belong to skills.

    Args:
        mode: Optional agent mode filter. None returns tool names from all skills.

    Returns:
        Set of tool names
    """
    names = set()
    for skill in SKILL_REGISTRY.values():
        if _is_enabled(skill) and _matches_mode(skill, mode):
            names.update(skill.get_tool_names())
    return names


def get_command_to_skill_map(mode: SkillMode | None = None) -> dict[str, str]:
    """Map slash command names to skill names, filtered by mode.

    Returns a dict where keys are command strings (e.g. "3-statement-model")
    and values are skill names (e.g. "3-statements"). Only includes skills
    that have a non-None command field.

    Args:
        mode: Optional agent mode filter. None returns all skills with commands.

    Returns:
        Dict mapping command name to skill name
    """
    return {
        skill.command: name
        for name, skill in SKILL_REGISTRY.items()
        if skill.command and _is_enabled(skill) and _matches_mode(skill, mode)
    }


def list_skills(mode: SkillMode | None = None) -> list[dict[str, Any]]:
    """List available skills with their metadata, optionally filtered by mode.

    Hidden skills are excluded from listings (they can only be activated
    programmatically via additionalContext).

    Args:
        mode: Optional agent mode filter. None returns all non-hidden skills.

    Returns:
        List of skill info dicts with name, description, and tool count
    """
    return [
        {
            "name": skill.name,
            "description": skill.description,
            "tool_count": len(skill.get_tool_names()),
            "tools": skill.get_tool_names(),
            "command": skill.command,
        }
        for skill in SKILL_REGISTRY.values()
        if _is_enabled(skill) and _matches_mode(skill, mode) and skill.exposure != "hidden"
    ]
