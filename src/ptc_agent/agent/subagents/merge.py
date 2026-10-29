"""One user's subagents: the deployment's base with the user's own rows on top.

The base is the built-ins with the deployment's YAML over them, field by
field as an overlay reads, then the subagents the user's plugins ship. A user's row
then either overrides a base field by field, storing only what the user
changed so later improvements to the base still reach them, or is a subagent
of their own. Pure: rows come in, finished definitions go out, and nothing
here reads storage or the turn.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from typing import Any

from ptc_agent.agent.roles import ALL_ROLES, AgentRole
from ptc_agent.agent.subagents.definition import CODE_SECTIONS, SubagentDefinition
from ptc_agent.agent.subagents.registry import SubagentRegistry
from ptc_agent.config.agent import SubagentsConfig

#: The tool words a user switches, and the tool sets they bind. A user's
#: ``tools`` replaces only these; every other set a base binds stays.
TOOL_WORDS: dict[str, str] = {
    "web": "web_search",
    "market_data": "finance",
    "code": "execute_code",
    "shell": "bash",
}
SWITCHABLE_TOOL_SETS = frozenset(TOOL_WORDS.values())

# A custom subagent binds its sets in general-purpose's order, so one with
# every word on is handed general-purpose's tool list.
_CUSTOM_TOOL_ORDER = ("execute_code", "filesystem", "bash", "finance", "web_search")

#: What a custom subagent saved without ``tools`` binds: the Analyst preset.
DEFAULT_CUSTOM_TOOLS = ("web", "market_data", "code", "shell")

BROKERAGE_ACCESS = ("off", "on")


@dataclass(frozen=True)
class BaseSubagent:
    """A subagent the user did not write, keyed the way an override names it.

    ``key`` is ``builtin:<name>``, ``deployment:<name>`` or
    ``plugin:<plugin name>/<agent>``. An override applies only to the base it
    was written against, so a name that changes hands (a YAML definition
    taking a plugin subagent's name, a plugin removed) orphans it instead of
    silently reshaping a different subagent. A YAML definition with a
    built-in's name only changes that built-in, which keeps its key.
    """

    key: str
    definition: SubagentDefinition


@dataclass(frozen=True)
class ResolvedSubagents:
    """The finished roster one turn compiles, and what the merge set aside.

    ``definitions`` covers every offered base and every custom subagent,
    including ones switched off, so a reader can show them; ``enabled`` is the
    roster in order. ``orphaned`` names overrides the turn could not apply,
    their base gone or not offered, and ``shadowed`` bases a custom subagent of
    the same name hides.
    """

    definitions: Mapping[str, SubagentDefinition]
    enabled: tuple[str, ...]
    orphaned: tuple[str, ...] = ()
    shadowed: tuple[str, ...] = ()

    def for_role(self, role: AgentRole) -> list[SubagentDefinition]:
        """The enabled definitions ``role`` compiles, in order.

        ``enabled`` is one list for every role, so a subagent ``role`` is not
        listed for is left out rather than refused.
        """
        return [d for name in self.enabled if role in (d := self.definitions[name]).roles]


def base_key(definition: SubagentDefinition) -> str:
    """How an override names this base."""
    if definition.source == "builtin":
        return f"builtin:{definition.name}"
    return f"deployment:{definition.name}"


def plan_gated(defn: SubagentDefinition, base: BaseSubagent | None) -> bool:
    """Whether the user's plan gates ``defn``'s pinned model: one the user
    chose, a plugin's included since the user installed it. A deployment's
    own pin is the operator's call, and gating it would change the turn of a
    user who never touched their subagents."""
    return (
        defn.source in ("custom", "plugin")
        or base is None
        or base.definition.model != defn.model
    )


def base_subagents(subagents: SubagentsConfig) -> dict[str, BaseSubagent]:
    """Every base the deployment defines, offered or not."""
    registry = SubagentRegistry(user_definitions=subagents.definitions or None)
    return {
        name: BaseSubagent(base_key(defn), defn)
        for name, defn in registry.list_all().items()
    }


def tool_words(tool_sets: Iterable[str]) -> list[str]:
    """The switchable words a tool-set list binds, in ``TOOL_WORDS`` order."""
    bound = set(tool_sets)
    return [word for word, tool_set in TOOL_WORDS.items() if tool_set in bound]


def tools_for_words(
    words: Iterable[str], base_tools: Sequence[str] | None = None
) -> list[str]:
    """``base_tools`` with its switchable sets replaced by ``words``.

    The base's order is kept for every set that stays, so naming the sets the
    base already binds hands back its list unchanged; a newly bound set goes
    last. Without a base this is a custom subagent's list, which always binds
    ``filesystem``. Unknown words are skipped: the writers validate them, and
    a stale row must not take a turn down.
    """
    wanted = {TOOL_WORDS[w] for w in words if w in TOOL_WORDS}
    if base_tools is None:
        return [t for t in _CUSTOM_TOOL_ORDER if t == "filesystem" or t in wanted]
    kept = [t for t in base_tools if t not in SWITCHABLE_TOOL_SETS or t in wanted]
    added = [t for t in _CUSTOM_TOOL_ORDER if t in wanted and t not in kept]
    return kept + added


def _inherit(value: Any) -> str | None:
    """A stored model, effort or search value; ``inherit`` and null are unset."""
    if value is None or value == "inherit":
        return None
    return str(value)


def _switch(value: Any) -> bool:
    return value is True or value == "on"


def _brokerages(value: Any, current: str) -> str:
    return value if value in BROKERAGE_ACCESS else current


def _common_changes(fields: Mapping[str, Any], defn: SubagentDefinition) -> dict[str, Any]:
    """The editable fields any row may set, as definition attributes.

    A key that is absent keeps the base's value; one present with null (or
    ``inherit``) is the user choosing Inherit, which for a model or an effort
    means the thread's, whatever the base pins.
    """
    changes: dict[str, Any] = {}
    if "model" in fields:
        changes["model"] = _inherit(fields["model"])
    if "effort" in fields:
        changes["reasoning_effort"] = _inherit(fields["effort"])
    if "search_provider" in fields:
        changes["search_provider"] = _inherit(fields["search_provider"])
    if "search_depth" in fields:
        changes["search_depth"] = _inherit(fields["search_depth"])
    if "connectors" in fields:
        changes["connectors"] = _switch(fields["connectors"])
    if "brokerages" in fields:
        changes["brokerages"] = _brokerages(fields["brokerages"], defn.brokerages)
    if "memory" in fields:
        changes["memory"] = _switch(fields["memory"])
    if isinstance(fields.get("preferred_connectors"), list):
        changes["preferred_connectors"] = [str(c) for c in fields["preferred_connectors"]]
    # Preloading is what a preferred skill means: every subagent already sees
    # the whole skill list, so only putting a SKILL.md in the prompt changes
    # what it reaches for.
    if isinstance(fields.get("preferred_skills"), list):
        changes["preload_skills"] = [str(s) for s in fields["preferred_skills"]]
    return changes


def apply_override(
    defn: SubagentDefinition, fields: Mapping[str, Any]
) -> SubagentDefinition:
    """``defn`` with the fields a user changed on it.

    ``dataclasses.replace`` keeps everything the user cannot edit, ``roles``
    and ``role_prompt_template`` above all. The code sections move only when
    the user's tools flip ``code`` against the base: otherwise the base's own
    choices stand, so data-prep keeps its visualizations section off.
    """
    changes = _common_changes(fields, defn)
    if isinstance(fields.get("tools"), list):
        tools = tools_for_words(fields["tools"], defn.tools)
        changes["tools"] = tools
        # A base the deployment already narrowed stays narrowed.
        changes["tools_narrowed"] = defn.tools_narrowed or bool(set(defn.tools) - set(tools))
        has_code = "execute_code" in tools
        if has_code != ("execute_code" in defn.tools):
            changes["sections"] = {
                **defn.sections,
                **{section: has_code for section in CODE_SECTIONS},
            }
    return replace(defn, **changes) if changes else defn


def custom_definition(row: Mapping[str, Any]) -> SubagentDefinition:
    """A user's own subagent as a definition.

    The instructions are the role section under the shared subagent frame,
    passed as text: the frame substitutes it as a value, so nothing a user or
    the agent writes is ever rendered as a template.
    """
    fields = row.get("fields") or {}
    words = fields.get("tools")
    tools = tools_for_words(words if isinstance(words, list) else DEFAULT_CUSTOM_TOOLS)
    has_code = "execute_code" in tools
    roles = frozenset(r for r in (row.get("roles") or ()) if r in ALL_ROLES)
    base = SubagentDefinition(
        name=row["name"],
        description=str(fields.get("description") or ""),
        role_prompt=row.get("instructions") or "",
        tools=tools,
        # The shared tool guide is written for the default set.
        tools_narrowed=bool(set(tools_for_words(DEFAULT_CUSTOM_TOOLS)) - set(tools)),
        sections={section: has_code for section in CODE_SECTIONS},
        roles=roles or ALL_ROLES,
        # A subagent the user writes reads memory unless they switch it off.
        memory=True,
        source="custom",
    )
    changes = _common_changes(fields, base)
    return replace(base, **changes) if changes else base


def plugin_definition(row: Mapping[str, Any]) -> SubagentDefinition:
    """A plugin's subagent as a base, built the way a custom one is.

    ``row`` is shaped like a custom row (``name``, ``fields``, ``instructions``,
    ``roles``), so a package's instructions are passed as text under the
    frame too: nothing a package ships is rendered as a template.
    """
    # A package's instructions are written for the tools its file names.
    return replace(custom_definition(row), source="plugin", tools_narrowed=False)


def add_plugin_bases(
    bases: Mapping[str, BaseSubagent],
    offered: Sequence[str],
    plugin_bases: Iterable[tuple[BaseSubagent, bool]],
) -> tuple[dict[str, BaseSubagent], tuple[str, ...]]:
    """The deployment's bases and offered names with the plugins' after them.

    Each plugin base comes with whether its plugin is on; one that is off
    stays a base, so the user's changes to it wait for it rather than show
    as a removed base. A name the deployment defines, offered or not, stays the
    deployment's, and between plugins the first keeps it. Install refuses
    both, so this settles only a deploy that later takes a plugin's name.
    """
    out = dict(bases)
    names = list(dict.fromkeys(offered))
    for base, on in plugin_bases:
        name = base.definition.name
        if name in out:
            continue
        out[name] = base
        if on:
            names.append(name)
    return out, tuple(names)


def _enabled(fields: Mapping[str, Any]) -> bool:
    return fields.get("enabled", True) is not False


def merge_subagents(
    bases: Mapping[str, BaseSubagent],
    offered: Sequence[str],
    rows: Iterable[Mapping[str, Any]],
) -> ResolvedSubagents:
    """The user's roster: the offered bases with their overrides, then customs.

    ``offered`` is the deployment's ``subagents.enabled``: a base the operator
    left out is not in the roster, the pane or the folder. A custom subagent
    whose name a base later took wins for this user, and the base is reported
    as shadowed. Rows are at most one per name, which the table enforces.
    """
    overrides: dict[str, Mapping[str, Any]] = {}
    customs: dict[str, Mapping[str, Any]] = {}
    for row in rows:
        # Per-workspace rows are reserved for later; v1 resolves the user tier.
        if row.get("workspace_id") is not None:
            continue
        if row.get("kind") == "custom":
            customs[row["name"]] = row
        elif row.get("kind") == "override":
            overrides[row["name"]] = row

    definitions: dict[str, SubagentDefinition] = {}
    enabled: list[str] = []
    shadowed: list[str] = []
    applied: set[str] = set()
    for name in dict.fromkeys(offered):
        base = bases.get(name)
        if base is None:
            continue
        if name in customs:
            shadowed.append(name)
            continue
        defn = base.definition
        on = True
        row = overrides.get(name)
        if row is not None and row.get("base_key") == base.key:
            fields = row.get("fields") or {}
            defn = apply_override(defn, fields)
            on = _enabled(fields)
            applied.add(name)
        definitions[name] = defn
        if on:
            enabled.append(name)

    for name, row in customs.items():
        definitions[name] = custom_definition(row)
        if _enabled(row.get("fields") or {}):
            enabled.append(name)

    return ResolvedSubagents(
        definitions=definitions,
        enabled=tuple(enabled),
        orphaned=tuple(name for name in overrides if name not in applied),
        shadowed=tuple(shadowed),
    )


def deployment_roster(subagents: SubagentsConfig) -> ResolvedSubagents:
    """The roster with no user rows: what a build no user resolved compiles."""
    return merge_subagents(base_subagents(subagents), subagents.enabled, ())


def turn_roster(subagents: SubagentsConfig) -> ResolvedSubagents:
    """The roster a build compiles: the turn's merge, else the deployment's."""
    resolved = subagents.resolved
    return resolved if resolved is not None else deployment_roster(subagents)


def narrow_to_request(
    resolved: ResolvedSubagents, requested: Sequence[str] | None
) -> tuple[ResolvedSubagents, list[str]]:
    """The roster a request named, and the names it named that are not on it.

    The request keeps its own order, as it always set the roster's. A name
    off the roster (deleted, renamed, switched off, never offered) is dropped
    rather than failing the turn, since a stored request outlives the
    subagents it named. An empty or absent request is the whole roster.
    """
    if not requested:
        return resolved, []
    on = set(resolved.enabled)
    names = list(dict.fromkeys(requested))
    dropped = [n for n in names if n not in on]
    return replace(resolved, enabled=tuple(n for n in names if n in on)), dropped
