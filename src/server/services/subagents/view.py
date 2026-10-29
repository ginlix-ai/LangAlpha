"""Each subagent as the pane and the agent's file show it.

The effective values come from the same merge a turn runs, so what the pane
shows is what the next turn compiles. Saved values are shown as saved, never
as a turn's fallback: a model the user can't run stays visible with a problem
beside it, because "absent here" is not "retired".

The version a writer sends back hashes the effective editable values and the
base the user's changes sit on: its values and its role body rendered at a
fixed guidance level (never its template path, which a template edit leaves
unchanged). A base that moves between a read and a write is then a conflict
rather than a save that silently pins values the user never touched.
"""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any

from ptc_agent.agent.subagents.definition import SubagentDefinition
from ptc_agent.agent.subagents.merge import (
    BaseSubagent,
    apply_override,
    merge_subagents,
    plan_gated,
    tool_words,
)

from .rules import NAME_RE, RESERVED_NAMES, ROLE_ORDER, WRITABLE_KEYS, description_matches

if TYPE_CHECKING:
    from .context import SubagentContext

logger = logging.getLogger(__name__)

#: The guidance level a base's body renders at, so the file and the hash don't
#: depend on which model a turn runs. The fuller one, so the user reads all of it.
BODY_GUIDANCE = "detailed"


def agent_file(name: str) -> str | None:
    """The subagent's file under the user's folder; None for a name no file can carry."""
    from ptc_agent.core.paths import SandboxLayout

    if name in RESERVED_NAMES or not NAME_RE.fullmatch(name):
        return None
    return f"{SandboxLayout.USER_AGENTS_DIR}/{name}.md"


def render_base_body(defn: SubagentDefinition) -> str:
    """A base's role section, the read-only body of its file.

    Its template gets every variable a turn passes, at the compiler's stand-in
    values and a fixed guidance level, so one that reads the thread renders
    here as in a turn rather than leaving the file and any copy without a
    body, and the body changes only when the template or the definition does.
    """
    from ptc_agent.agent.prompts import get_loader
    from ptc_agent.agent.subagents.compiler import subagent_template_vars

    if defn.custom_prompt is not None:
        return defn.custom_prompt.strip()
    variables = subagent_template_vars(defn, guidance=BODY_GUIDANCE)
    # In the compiler's order, so a definition setting several shows the one a turn runs.
    if defn.custom_prompt_template:
        template = defn.custom_prompt_template
    elif defn.role_prompt:
        return defn.role_prompt.strip()
    elif defn.role_prompt_template:
        template = f"subagents/{defn.role_prompt_template}"
        # The base prompt hands its role template this one as well.
        variables["tools_narrowed"] = defn.tools_narrowed
    else:
        return ""
    try:
        return get_loader().render(template, **variables).strip()
    except Exception as exc:  # a missing file or a syntax error, which fail a turn too
        logger.warning(f"Could not render subagent body for {defn.name!r}: {exc}")
        return ""


def definition_values(defn: SubagentDefinition, *, enabled: bool) -> dict[str, Any]:
    """The editable values a definition carries, keyed as the pane and file spell them."""
    return {
        "enabled": enabled,
        "model": defn.model,
        "effort": defn.reasoning_effort,
        "tools": tool_words(defn.tools),
        "search_provider": defn.search_provider,
        "search_depth": defn.search_depth,
        "connectors": defn.connectors,
        "brokerages": defn.brokerages,
        "memory": defn.memory,
        "preferred_connectors": list(defn.preferred_connectors),
        "preferred_skills": list(defn.preload_skills),
    }


def version_of(payload: Mapping[str, Any]) -> str:
    """sha256 over a canonical JSON of what a version covers."""
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _source_of_key(key: str | None) -> str:
    prefix = (key or "").split(":", 1)[0]
    return prefix if prefix in ("builtin", "deployment", "plugin") else "deployment"


def _plugin_of_key(key: str | None) -> dict[str, Any] | None:
    if not key or not key.startswith("plugin:"):
        return None
    plugin = key.split(":", 1)[1].split("/", 1)[0]
    return {"name": plugin, "display_name": plugin, "installed": True}


def _roles(roles: Iterable[str]) -> list[str]:
    held = set(roles)
    return [r for r in ROLE_ORDER if r in held]


def _provenance(row: Mapping[str, Any] | None) -> dict[str, Any]:
    return {
        "updated_by": row.get("updated_by") if row else None,
        "updated_thread_id": row.get("updated_thread_id") if row else None,
        "updated_thread_title": None,
        "updated_at": row.get("updated_at") if row else None,
    }


def _user_fields(row: Mapping[str, Any] | None) -> list[str]:
    if row is None:
        return []
    keys = set(row.get("fields") or {})
    if row.get("kind") == "custom":
        if row.get("instructions") is not None:
            keys.add("instructions")
        if row.get("roles") is not None:
            keys.add("available_to")
    return [k for k in WRITABLE_KEYS if k in keys]


def _finish(view: dict[str, Any], extra: Mapping[str, Any] | None = None) -> dict[str, Any]:
    view["file"] = agent_file(view["name"])
    view["version"] = version_of({
        "name": view["name"],
        "kind": view["kind"],
        "source": view["source"],
        "values": {k: view[k] for k in WRITABLE_KEYS},
        "base": view["base"],
        **(extra or {}),
    })
    return view


def _custom_view(defn: SubagentDefinition, row: Mapping[str, Any], *, enabled: bool) -> dict[str, Any]:
    return _finish({
        "name": defn.name,
        "kind": "custom",
        "source": "custom",
        "plugin": None,
        "edited": False,
        "user_fields": _user_fields(row),
        "description": defn.description,
        "instructions": row.get("instructions") or "",
        **definition_values(defn, enabled=enabled),
        "available_to": _roles(defn.roles),
        "base": None,
        "problems": [],
        **_provenance(row),
    })


def _base_view(
    defn: SubagentDefinition,
    base: BaseSubagent,
    row: Mapping[str, Any] | None,
    stale: Mapping[str, Any] | None,
    *,
    enabled: bool,
) -> dict[str, Any]:
    """A base with the user's override, if one applies.

    ``stale`` is a row written against a base this name no longer has (a
    deployment definition now takes a plugin subagent's name); it is shown as base
    removed on this view, and a save starts over from the current base.
    """
    body = render_base_body(base.definition)
    base_values = definition_values(base.definition, enabled=True)
    edited = row is not None and bool(row.get("fields"))
    problems = [{"code": "base_removed", "detail": stale.get("base_key")}] if stale else []
    return _finish(
        {
            "name": defn.name,
            "kind": "base",
            "source": _source_of_key(base.key),
            "plugin": _plugin_of_key(base.key),
            "edited": edited,
            "user_fields": _user_fields(row),
            "description": defn.description,
            "instructions": body,
            **definition_values(defn, enabled=enabled),
            "available_to": _roles(defn.roles),
            "base": base_values,
            "problems": problems,
            **_provenance(row or stale),
        },
        {"base_key": base.key, "stale": dict(stale.get("fields") or {}) if stale else None},
    )


def is_orphan(view: Mapping[str, Any]) -> bool:
    """Settings of a base that is gone, which have no file: only the pane deletes them."""
    return view["kind"] == "base" and view["base"] is None


def _orphan_view(row: Mapping[str, Any], installed_plugins: frozenset[str]) -> dict[str, Any]:
    """An override whose base is gone: listed so the user can delete it, nothing else."""
    plugin = _plugin_of_key(row.get("base_key"))
    if plugin:
        plugin["installed"] = plugin["name"] in installed_plugins
    blank = SubagentDefinition(name=row["name"], description="", tools=[], source="deployment")
    defn = apply_override(blank, row.get("fields") or {})
    fields = row.get("fields") or {}
    return _finish(
        {
            "name": row["name"],
            "kind": "base",
            "source": _source_of_key(row.get("base_key")),
            "plugin": plugin,
            "edited": True,
            "user_fields": _user_fields(row),
            "description": "",
            "instructions": "",
            **definition_values(defn, enabled=fields.get("enabled", True) is not False),
            "available_to": list(ROLE_ORDER),
            "base": None,
            "problems": [{"code": "base_removed", "detail": row.get("base_key")}],
            **_provenance(row),
        },
        {"base_key": row.get("base_key"), "orphan": dict(fields)},
    ) | {"file": None}


async def _model_unavailable(
    ctx: SubagentContext, defn: SubagentDefinition, base: BaseSubagent | None
) -> bool:
    """The per-turn availability passes, asked ahead of the turn.

    Name and plan as the merge's first pass checks them (a deployment's own
    pin is never plan-gated, a plugin's is), then the key a custom model
    needs, which the turn finds only once it builds clients.
    """
    if not ctx.model_known(defn.model):
        return True
    if plan_gated(defn, base) and not await ctx.model_on_plan(defn.model):
        return True
    return not await ctx.model_has_key(defn.model)


async def _search_problems(ctx: SubagentContext, defn: SubagentDefinition) -> list[dict[str, Any]]:
    """Where the subagent's search pair falls back at run time; only while ``web`` is on."""
    if "web_search" not in defn.tools or not (defn.search_provider or defn.search_depth):
        return []
    try:
        gate = await ctx.gate_search(defn.search_provider, defn.search_depth)
    except Exception as exc:
        logger.warning(f"Could not gate search for subagent {defn.name!r}: {exc}")
        return []
    problems: list[dict[str, Any]] = []
    if gate.provider_fallback == "not_on_plan":
        problems.append({"code": "search_not_on_plan", "detail": defn.search_provider})
    elif gate.provider_fallback == "unknown":
        problems.append({"code": "search_unavailable", "detail": defn.search_provider})
    if gate.depth_fallback == "not_offered":
        problems.append({"code": "depth_not_offered", "detail": gate.provider})
    elif gate.depth_fallback == "not_on_plan":
        problems.append({"code": "search_not_on_plan", "detail": defn.search_depth})
    return problems


# A view with the definition it shows and its base (None for a custom one).
_Pair = tuple[dict[str, Any], SubagentDefinition, BaseSubagent | None]


def _views(
    bases: Mapping[str, BaseSubagent], offered: Iterable[str], rows: Iterable[Mapping[str, Any]]
) -> tuple[list[dict[str, Any]], list[_Pair]]:
    rows = [r for r in rows if r.get("workspace_id") is None]
    # The bases hold every installed plugin's subagents, switched on or off,
    # and bundles. A plugin that ships none any more can only have left an
    # override behind through an older build's uninstall, so calling it not
    # installed at worst drops a link.
    installed_plugins = frozenset(
        p["name"] for b in bases.values() if (p := _plugin_of_key(b.key))
    )
    by_name = {r["name"]: r for r in rows}
    resolved = merge_subagents(bases, tuple(offered), rows)
    on = set(resolved.enabled)

    views: list[dict[str, Any]] = []
    pairs: list[_Pair] = []
    for name, defn in resolved.definitions.items():
        row = by_name.get(name)
        if defn.source == "custom":
            view = _custom_view(defn, row or {}, enabled=name in on)
            if name in resolved.shadowed:
                view["problems"].append({
                    "code": "name_collision",
                    "detail": _source_of_key(bases[name].key),
                })
            pairs.append((view, defn, None))
        else:
            base = bases[name]
            applies = row is not None and row.get("base_key") == base.key
            view = _base_view(
                defn, base, row if applies else None,
                row if row is not None and not applies else None,
                enabled=name in on,
            )
            pairs.append((view, defn, base))
        views.append(view)
    for name in resolved.orphaned:
        row = by_name[name]
        # A base still defined but not offered (left out by the operator, or
        # its plugin off) keeps the user's changes for when it is back.
        dormant = name in bases and bases[name].key == row.get("base_key")
        if name not in resolved.definitions and not dormant:
            views.append(_orphan_view(row, installed_plugins))
    return views, pairs


def value_views(
    bases: Mapping[str, BaseSubagent], offered: Iterable[str], rows: Iterable[Mapping[str, Any]]
) -> list[dict[str, Any]]:
    """``build_views`` without problems, for a reader that has no user context."""
    return _views(bases, offered, rows)[0]


def matching_descriptions(views: Mapping[str, Mapping[str, Any]], name: str) -> list[str]:
    """The subagents that are on whose description reads the same as ``name``'s.

    A base's description is read-only, so only one of the user's own that is
    on is asked to say how it differs.
    """
    view = views[name]
    if view["kind"] != "custom" or not view["enabled"]:
        return []
    others = [
        (v["name"], v["description"], v["available_to"])
        for v in views.values()
        if v["name"] != name and v["enabled"] and not is_orphan(v)
    ]
    return description_matches(view["description"], view["available_to"], others)


async def build_views(ctx: SubagentContext, rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Every subagent the user has, in roster order, then orphaned overrides.

    The offered bases come first in the deployment's order, then the user's
    own; each carries its effective values, its base's, its problems and its
    version. Thread titles are left for ``attach_titles``.
    """
    views, pairs = _views(ctx.bases, ctx.offered, rows)
    by_name = {v["name"]: v for v in views}
    for view, defn, base in pairs:
        found = view["problems"]
        if defn.model and await _model_unavailable(ctx, defn, base):
            found.append({"code": "model_unavailable", "detail": defn.model})
        elif (
            defn.model and defn.reasoning_effort
            and defn.reasoning_effort not in ctx.effort_ladder(defn.model)
        ):
            # A save holds a level to the model's ladder, so this is the
            # deployment's own file or a ladder changed since; the client
            # steps the level down.
            found.append({"code": "effort_not_offered", "detail": defn.model})
        found += [
            {"code": "skill_missing", "detail": s}
            for s in defn.preload_skills if s not in ctx.skills
        ]
        found += [
            {"code": "connector_missing", "detail": c}
            for c in defn.preferred_connectors if c not in ctx.connector_names
        ]
        found += await _search_problems(ctx, defn)
        found += [
            {"code": "description_matches", "detail": other}
            for other in matching_descriptions(by_name, defn.name)
        ]
    return views


async def attach_titles(user_id: str, views: Iterable[dict[str, Any]]) -> None:
    """Fill ``updated_thread_title`` from the threads the views name, in one query."""
    from src.server.database.conversation.threads_read import get_thread_labels

    views = list(views)
    ids = list({v["updated_thread_id"] for v in views if v.get("updated_thread_id")})
    if not ids:
        return
    titles = {label["thread_id"]: label["title"] for label in await get_thread_labels(ids, user_id)}
    for view in views:
        thread = view.get("updated_thread_id")
        if thread:
            view["updated_thread_title"] = titles.get(thread)
