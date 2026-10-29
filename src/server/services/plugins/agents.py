"""A package's ``agents/`` folder: one subagent per ``agents/<name>.md``.

The file is the one a user's Subagents folder shows (frontmatter between two
``---`` lines, then the instructions), so a subagent copied out of that folder
ships as is. A Claude Code agent file reads too: its tool names map onto the
tool words and its ``skills`` onto the preferred skills. What has no
counterpart here is ignored with a diagnostic rather than refusing the file,
since one option this client lacks should not cost the user the subagent.

Pure, like the rest of validation: a bad file drops that subagent only, as a
bad skill drops that skill.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field, replace
from typing import Any, ClassVar

from src.server.models.plugin import Diagnostic

AGENTS_DIR = "agents/"


def is_agent_file(path: str) -> bool:
    """Whether install reads ``path`` as a subagent: ``agents/<name>.md``."""
    rest = path.removeprefix(AGENTS_DIR)
    return rest != path and "/" not in rest and rest.endswith(".md")


# Claude Code's tool names, as the tool words they mean here. Its file tools
# need no word, since every subagent reads and writes files, and nor do Bash's
# companions, which reach only a shell Bash started.
_WORDLESS_TOOLS = frozenset({
    "Read", "Write", "Edit", "MultiEdit", "Glob", "Grep", "LS",
    "NotebookRead", "NotebookEdit", "BashOutput", "KillShell", "KillBash",
})
_CLAUDE_TOOLS: dict[str, tuple[str, ...]] = {
    "Bash": ("code", "shell"),
    "WebSearch": ("web",),
    "WebFetch": ("web",),
}
_MODEL_ALIASES = frozenset({"sonnet", "opus", "haiku"})

#: The cap a SKILL.md has: past the instructions' own, it bounds what the
#: frontmatter can carry into the read and the stored file.
AGENT_FILE_MAX_BYTES = 64 * 1024

# The subagent file's keys a package sets, besides the ones read below.
_VALUE_KEYS = (
    "effort",
    "search_provider",
    "search_depth",
    "connectors",
    "brokerages",
    "preferred_connectors",
    "preferred_skills",
    "available_to",
)
# Keys the folder's file carries that say nothing to an install.
_QUIET_KEYS = frozenset({"name", "source", "enabled"})
_NUL_REASON = "the file holds a NUL character (\\x00), which can't be saved"
_SURROGATE_REASON = "the file holds an unpaired surrogate (\\ud800 to \\udfff), which can't be saved"


@dataclass
class AgentPlan:
    """One ``agents/<name>.md``, read into a base subagent's row or skipped."""

    # How this plan reports itself (ComponentResult.of). The file name is the
    # subagent's name, so nothing renames it.
    kind: ClassVar[str] = "subagent"
    renamed: ClassVar[bool] = False

    name: str
    #: The file as shipped, which export writes back and update compares.
    text: str = ""
    #: A custom subagent's row (``user_subagents``): ``fields`` holds only the
    #: keys the file sets, so one it leaves out takes a custom subagent's default.
    fields: dict[str, Any] = field(default_factory=dict)
    instructions: str = ""
    roles: list[str] | None = None
    skip_code: str | None = None
    skip_reason: str | None = None
    diagnostics: list[Diagnostic] = field(default_factory=list)

    @property
    def key(self) -> str:
        return self.name

    @property
    def row(self) -> dict[str, Any]:
        """The plan as ``plugin_subagents`` stores it and ``plugin_definition`` reads it."""
        return {
            "name": self.name, "fields": self.fields,
            "instructions": self.instructions, "roles": self.roles,
        }


def _diag(target: str, code: str, message: str, level: str = "warning") -> Diagnostic:
    return Diagnostic(level=level, scope="subagent", target=target, code=code, message=message)


def _skip(plan: AgentPlan, code: str, reason: str) -> AgentPlan:
    plan.skip_code = code
    plan.skip_reason = reason
    plan.diagnostics.append(_diag(plan.name, code, reason))
    return plan


def _items(value: Any) -> list[str] | None:
    """A list, or Claude Code's comma-separated string, as names; None if neither."""
    if isinstance(value, str):
        value = value.split(",")
    if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
        return None
    return list(dict.fromkeys(v.strip() for v in value if v.strip()))


def _tools(value: Any, plan: AgentPlan) -> tuple[list[str] | None, list[str]]:
    """The tool words ``value`` binds (None for the default), and the servers
    its ``mcp__`` names reach."""
    from ptc_agent.agent.subagents.merge import TOOL_WORDS

    if value is None:
        return None, []
    items = _items(value)
    if items is None:
        plan.diagnostics.append(_diag(
            plan.name, "tools_ignored",
            "tools must be a list of names, so it gets the Analyst tools",
        ))
        return None, []
    words: set[str] = set()
    servers: list[str] = []
    ignored: list[str] = []
    for item in items:
        if item in TOOL_WORDS:
            words.add(item)
        elif item in _CLAUDE_TOOLS:
            words.update(_CLAUDE_TOOLS[item])
        elif item.startswith("mcp__"):
            words.add("code")
            server = item.removeprefix("mcp__").split("__", 1)[0]
            if server:
                servers.append(server)
        elif item not in _WORDLESS_TOOLS:
            ignored.append(item)
    if ignored:
        plan.diagnostics.append(_diag(
            plan.name, "tools_ignored",
            f"tools with no match here were left out: {', '.join(ignored)}",
        ))
    return [w for w in TOOL_WORDS if w in words], servers


def _model(value: Any, plan: AgentPlan) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        plan.diagnostics.append(_diag(
            plan.name, "model_ignored", "model must be a model id, so it uses the thread's model",
        ))
        return None
    value = value.strip()
    if value.lower() == "inherit":
        return None
    if value.lower() in _MODEL_ALIASES:
        plan.diagnostics.append(_diag(
            plan.name, "model_alias",
            f"model: {value} names a model family, not a model, so it uses the thread's model",
            level="info",
        ))
        return None
    return value


def _search_errors(fields: dict[str, Any]) -> list[str]:
    """Drop from ``fields`` the search settings this deployment can't run, saying why."""
    from src.tools.web.manifest import (
        CAPABILITY_SEARCH,
        providers_with_capability,
        search_levels,
    )

    notes: list[str] = []
    provider = fields.get("search_provider")
    if provider is not None and provider not in providers_with_capability(CAPABILITY_SEARCH):
        notes.append(f"search_provider {provider} isn't offered here, so it uses the user's")
        del fields["search_provider"]
        provider = None
    depth = fields.get("search_depth")
    if depth is not None and depth not in search_levels(provider):
        notes.append(f"search_depth {depth} isn't offered here, so it uses the user's")
        del fields["search_depth"]
    return notes


def _read(loaded: Mapping[str, Any], body: str, plan: AgentPlan) -> None:
    """Fill the plan's row from the frontmatter and body, or skip it."""
    from src.llms.reasoning import REASONING_LEVELS
    from src.server.services.subagents.rules import (
        DESCRIPTION_MAX,
        INHERIT,
        INSTRUCTIONS_MAX_BYTES,
        normalize_value,
    )

    description = loaded.get("description")
    if isinstance(description, (int, float)) and not isinstance(description, bool):
        description = str(description)
    if not isinstance(description, str) or not description.strip():
        _skip(plan, "missing_description", "a subagent needs a description saying when to use it")
        return
    description = " ".join(description.split())
    if len(description) > DESCRIPTION_MAX:
        _skip(plan, "description_too_long", f"the description is over {DESCRIPTION_MAX} characters")
        return
    instructions = body.strip()
    if len(instructions.encode("utf-8")) > INSTRUCTIONS_MAX_BYTES:
        _skip(plan, "instructions_too_long", f"the instructions are over {INSTRUCTIONS_MAX_BYTES // 1024} KB")
        return
    plan.instructions = instructions

    # No ``memory``: other agent formats spend that key on something else
    # (Claude Code's memory scopes), so a package's subagent takes the default
    # a custom one has, on.
    fields: dict[str, Any] = {"description": description}
    tools, servers = _tools(loaded.get("tools"), plan)
    if tools is not None:
        fields["tools"] = tools
    model = _model(loaded.get("model"), plan)
    if model is not None:
        fields["model"] = model
    for key in _VALUE_KEYS:
        if loaded.get(key) is None:
            continue
        try:
            value = normalize_value(key, loaded[key])
        except ValueError as exc:
            plan.diagnostics.append(_diag(plan.name, f"{key}_ignored", f"{key}: {exc}"))
            continue
        if value == INHERIT:
            continue
        if key == "available_to":
            plan.roles = value
        else:
            fields[key] = value
    effort = fields.get("effort")
    if effort is not None and effort not in REASONING_LEVELS:
        del fields["effort"]
        plan.diagnostics.append(_diag(
            plan.name, "effort_ignored", f"effort {effort} isn't a level, so it uses the thread's",
        ))
    plan.diagnostics += [_diag(plan.name, "search_ignored", note) for note in _search_errors(fields)]

    # Claude Code's ``skills`` preloads each one, which is what a preferred
    # skill does here.
    skills = _items(loaded.get("skills")) if loaded.get("skills") is not None else []
    if skills is None:
        plan.diagnostics.append(_diag(plan.name, "skills_ignored", "skills must be a list of skill names"))
        skills = []
    if skills:
        fields["preferred_skills"] = list(dict.fromkeys([*fields.get("preferred_skills", []), *skills]))
    if servers:
        fields["preferred_connectors"] = list(dict.fromkeys([*fields.get("preferred_connectors", []), *servers]))
    plan.fields = fields

    ignored = [str(k) for k in loaded if k not in (*_VALUE_KEYS, *_QUIET_KEYS, "description", "tools", "model", "skills")]
    if ignored:
        plan.diagnostics.append(_diag(
            plan.name, "keys_ignored",
            f"keys with no match here were ignored: {', '.join(ignored)}",
            level="info",
        ))
    if loaded.get("enabled") is False:
        plan.diagnostics.append(_diag(
            plan.name, "enabled_ignored",
            "enabled: false is ignored; a plugin's subagents arrive on, and the user turns one off",
            level="info",
        ))
    written = loaded.get("name")
    if written is not None and written != plan.name:
        plan.diagnostics.append(_diag(
            plan.name, "name_ignored",
            f"the file is {plan.name}.md, so the subagent is named {plan.name}, not {written}",
            level="info",
        ))


def read_agent(name: str, raw: bytes) -> AgentPlan:
    """One file as a plan; ``name`` is its file name without ``.md``."""
    from src.server.services.subagents.frontmatter import (
        FrontmatterFault,
        load_frontmatter,
        unsavable_char,
    )
    from src.server.services.subagents.rules import NAME_RE, RESERVED_NAMES

    plan = AgentPlan(name=name)
    if not NAME_RE.fullmatch(name):
        return _skip(
            plan, "invalid_name",
            "a subagent's file name is its name: 1 to 48 lowercase letters, digits or -, "
            "starting with a letter or digit",
        )
    if name in RESERVED_NAMES:
        return _skip(plan, "reserved_name", f"{name} is reserved for the agent itself")
    if len(raw) > AGENT_FILE_MAX_BYTES:
        return _skip(plan, "file_too_large", f"the file is over {AGENT_FILE_MAX_BYTES // 1024} KB")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return _skip(plan, "unreadable", "the file is not UTF-8 text")
    # Refused here, as a file: stored, it would fail the insert after the
    # plugin's other components had landed, and every update after it.
    if "\x00" in text:
        return _skip(plan, "unreadable", _NUL_REASON)
    plan.text = text
    try:
        loaded, body = load_frontmatter(text)
    except FrontmatterFault as fault:
        return _skip(plan, "parse_error", fault.message)
    unsavable = unsavable_char(loaded)
    if unsavable is not None:
        return _skip(plan, "unreadable", _NUL_REASON if unsavable == "\x00" else _SURROGATE_REASON)
    _read(loaded, body, plan)
    return plan


def collect_agents(files: Mapping[str, bytes]) -> tuple[list[AgentPlan], list[Diagnostic]]:
    """Every ``agents/<name>.md`` as a plan, by name.

    Only files directly under ``agents/`` are subagents; anything else there
    is reported and left out.
    """
    diagnostics: list[Diagnostic] = []
    plans: list[AgentPlan] = []
    for path in sorted(files):
        if not path.startswith(AGENTS_DIR):
            continue
        if not is_agent_file(path):
            diagnostics.append(_diag(
                path, "loose_file", "only agents/<name>.md files are subagents, so this one was left out",
            ))
            continue
        plans.append(read_agent(path.removeprefix(AGENTS_DIR).removesuffix(".md"), files[path]))
    return plans, diagnostics


def link_connectors(
    plans: Iterable[AgentPlan], names: Mapping[str, str], plugin: str
) -> list[AgentPlan]:
    """The plans with preferred connectors that name a package server pointed
    at the row the plugin holds for it.

    ``names`` maps a server's ``mcp.json`` key to that row's name, which
    install may have chosen differently. Claude Code names a plugin's own
    server ``plugin_<plugin>_<server>`` in its tool names, which an agent's
    ``tools`` uses. A server with no row of the plugin's keeps the name the
    file gives, so one that never landed reads as missing instead of standing
    for a server of the user's that holds its name.
    """
    own = f"plugin_{plugin}_"

    def linked(connector: str) -> str:
        if connector in names:
            return names[connector]
        server = connector.removeprefix(own)
        return names.get(server, connector) if connector.startswith(own) else connector

    return [
        replace(plan, fields={
            **plan.fields,
            "preferred_connectors": list(dict.fromkeys(
                linked(c) for c in plan.fields["preferred_connectors"]
            )),
        })
        if "preferred_connectors" in plan.fields else plan
        for plan in plans
    ]
