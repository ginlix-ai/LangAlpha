"""Saving and removing a user's subagents, for the pane and the agent's files alike.

Both writers send the version they read. A save is checked before the
per-user lock (``prepare_save``), since validation reads keys and plans over
other connections, then made under it (``apply_save``) only while the
subagent stands at the version it was checked against: a pane edit racing an
agent edit is a conflict rather than a silent overwrite, and so is a base that
changed underneath (a deploy, a template edit). The caller holds the lock and
the transaction, so the file route makes a save in its own and reads the file
it hands back there.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any, Literal, NamedTuple

from ptc_agent.agent.subagents.merge import BaseSubagent

from .context import SubagentContext, load_context, roster
from .rules import (
    EDITABLE_KEYS,
    INHERIT,
    INHERITABLE_KEYS,
    ROLE_ORDER,
    FieldError,
    SubagentInvalid,
    custom_text_errors,
    validate_fields,
    validate_new_name,
)
from .view import attach_titles, build_views, definition_values, is_orphan, value_views

logger = logging.getLogger(__name__)


class VersionConflict(Exception):
    """The subagent changed since the writer read it; read again and retry."""


class SubagentNotFound(Exception):
    """No subagent of that name."""


class Subagents(NamedTuple):
    """The user's subagents as one read found them: the bases and offered
    names their views merge, and the views and rows by name."""

    bases: dict[str, BaseSubagent]
    offered: tuple[str, ...]
    views: dict[str, dict[str, Any]]
    rows: dict[str, dict[str, Any]]

    @classmethod
    def of(
        cls, bases: dict[str, BaseSubagent], offered: tuple[str, ...], rows: Iterable[Mapping[str, Any]]
    ) -> Subagents:
        by_name = {r["name"]: r for r in rows}
        views = {v["name"]: v for v in value_views(bases, offered, by_name.values())}
        return cls(bases, offered, views, by_name)

    def version(self, name: str) -> str | None:
        """The version of ``name``'s view; None where it has none."""
        view = self.views.get(name)
        return view["version"] if view else None

    def saved(self, name: str, row: Mapping[str, Any] | None) -> Subagents:
        """These subagents once ``name``'s row is ``row``, or is gone for None."""
        rows = {**self.rows, name: row}
        return Subagents.of(self.bases, self.offered, [r for r in rows.values() if r is not None])


async def read_subagents(user_id: str, *, conn=None) -> Subagents:
    """The user's subagents without problems or titles: what their files show
    and their versions, which need no user context.

    ``conn`` reads them under a writer's lock. Uninstall takes the same lock
    to drop the overrides of a plugin's subagents, so a save that loses the
    race finds the base gone (a conflict) rather than writing an override
    that outlives the plugin.
    """
    from src.server.database.user_subagents import list_user_subagents

    bases, offered = await roster(user_id, conn=conn)
    return Subagents.of(bases, offered, await list_user_subagents(user_id, conn=conn))


async def list_subagents(
    user_id: str, *, ctx: SubagentContext | None = None
) -> tuple[SubagentContext, list[dict[str, Any]]]:
    """Every subagent view, with problems and thread titles."""
    from src.server.database.user_subagents import list_user_subagents

    if ctx is None:
        ctx, rows = await asyncio.gather(load_context(user_id), list_user_subagents(user_id))
    else:
        rows = await list_user_subagents(user_id)
    views = await build_views(ctx, rows)
    await attach_titles(user_id, views)
    return ctx, views


def _stored_effective(key: str, value: Any) -> Any:
    """What a stored field value means, for comparing it with the base's."""
    if key in INHERITABLE_KEYS:
        return None if value in (None, INHERIT) else value
    if key in ("connectors", "memory"):
        return value is True or value == "on"
    return value


def _override_fields(
    base: BaseSubagent, row: Mapping[str, Any] | None, patch: Mapping[str, Any]
) -> dict[str, Any]:
    """The override's fields after ``patch``: only what differs from the base.

    A row written against another base (``base_key`` moved) is started over.
    An explicit ``inherit`` is stored as null, which the merge reads as the
    thread's value whatever the base pins.
    """
    applies = row is not None and row.get("base_key") == base.key
    fields = dict(row.get("fields") or {}) if applies else {}
    for key, value in patch.items():
        if value is None:
            fields.pop(key, None)
        else:
            fields[key] = None if value == INHERIT else value
    base_values = definition_values(base.definition, enabled=True)
    return {k: v for k, v in fields.items() if _stored_effective(k, v) != base_values[k]}


def _custom_parts(
    fields: Mapping[str, Any], instructions: str, roles: list[str], patch: Mapping[str, Any]
) -> tuple[dict[str, Any], str, list[str]]:
    """A custom subagent's row after ``patch``."""
    out = dict(fields)
    for key, value in patch.items():
        if key == "instructions":
            instructions = value or ""
        elif key == "available_to":
            roles = list(value) if value else list(ROLE_ORDER)
        elif value is None or (key in INHERITABLE_KEYS and value == INHERIT):
            out.pop(key, None)
        else:
            out[key] = value
    return out, instructions, roles


def _seed(view: Mapping[str, Any]) -> tuple[dict[str, Any], str, list[str]]:
    """A copy's starting row: every effective value of ``view``.

    The same copy ``cp`` makes in the folder, so the pane's Start from and the
    agent produce the same subagent.
    """
    fields = {
        k: view[k] for k in EDITABLE_KEYS
        if not (k in INHERITABLE_KEYS and view[k] is None)
    }
    fields["description"] = view["description"]
    return fields, view["instructions"], list(view["available_to"])


async def _runnable_seed(ctx: SubagentContext, view: Mapping[str, Any]) -> tuple[dict[str, Any], str, list[str]]:
    """``_seed`` less a model pin the copy would not run where ``view`` does.

    A deployment base runs its own pin whatever the plan, but a copy is the
    user's own subagent and held to the plan like any model they pick, so it
    follows the thread instead, which is what the turn would run. The effort
    stays, as the turn keeps it on that fallback.
    """
    fields, instructions, roles = _seed(view)
    model = fields.get("model")
    runs = not any(p["code"] == "model_unavailable" for p in view["problems"])
    if model and runs and not await ctx.model_on_plan(model):
        del fields["model"]
    return fields, instructions, roles


def _blank_custom(name: str) -> dict[str, Any]:
    from ptc_agent.agent.subagents.merge import custom_definition

    return definition_values(custom_definition({"name": name, "fields": {}}), enabled=True)


@dataclass(frozen=True)
class PreparedSave:
    """A save checked against the subagents an unlocked read found, for
    ``apply_save`` to make while they stand as it found them."""

    user_id: str
    name: str
    # The version of the subagent it was checked against; None creates.
    version: str | None
    kind: Literal["custom", "override"]
    patch: dict[str, Any]
    # A copy's starting row (``start_from``).
    seed: tuple[dict[str, Any], str, list[str]] | None = None


async def prepare_save(
    ctx: SubagentContext,
    found: Subagents,
    name: str,
    fields: Mapping[str, Any],
    *,
    version: str | None,
    start_from: str | None = None,
) -> PreparedSave:
    """Check a save that creates a custom subagent, or changes one the user
    has, base or custom, against ``found``, read before the lock.

    ``version`` is the one the writer read, required for an existing subagent;
    None creates a custom one, optionally copied from ``start_from``. On a base,
    only differences from it are stored and null clears a key back to it.

    Raises ``SubagentInvalid`` (field errors) and ``VersionConflict``.
    """
    views = found.views
    target = views.get(name)
    creating = version is None
    if creating and target is not None:
        if target["kind"] == "base":
            raise SubagentInvalid([FieldError("name", (
                f"{name} holds the settings of a built-in subagent that was removed. "
                "Delete them, or pick another name."
                if is_orphan(target)
                else f"{name} is a built-in subagent. Pick another name."
            ))])
        raise VersionConflict()
    if not creating and (target is None or target["version"] != version):
        raise VersionConflict()
    if not creating and start_from is not None:
        raise SubagentInvalid([FieldError("start_from", "Only when creating a subagent.")])
    if target is not None and is_orphan(target):
        raise SubagentInvalid([FieldError(
            "name", "Its built-in subagent was removed, so its settings can only be deleted."
        )])

    errors: list[FieldError] = []
    seed: tuple[dict[str, Any], str, list[str]] | None = None
    if creating:
        errors += validate_new_name(
            name,
            base_names=found.bases,
            custom_count=sum(1 for v in views.values() if v["kind"] == "custom"),
        )
        if start_from is not None:
            source = views.get(start_from)
            if source is None or is_orphan(source):
                raise SubagentInvalid(errors + [FieldError(
                    "start_from", f"No subagent named {start_from} to start from.",
                    [n for n, v in views.items() if not is_orphan(v)],
                )])
            seed = await _runnable_seed(ctx, source)
            current = {**seed[0], "instructions": seed[1], "available_to": seed[2]}
        else:
            current = {**_blank_custom(name), "description": "", "instructions": "",
                       "available_to": list(ROLE_ORDER)}
        kind, base_values = "custom", None
    else:
        current = dict(target)
        kind = "custom" if target["kind"] == "custom" else "override"
        base_values = target["base"]

    try:
        patch = await validate_fields(ctx, fields, kind=kind, current=current, base=base_values)
    except SubagentInvalid as exc:
        errors += exc.errors
        patch = {}
    if kind == "custom":
        # The text as sent when another field refused the save and left no
        # patch, so a description that was sent is never reported missing.
        final = {
            key: (patch[key] or "") if key in patch
            else fields[key] if isinstance(fields.get(key), str)
            else current.get(key)
            for key in ("description", "instructions")
        }
        refused = {e.field for e in errors}
        errors += [
            e for e in custom_text_errors(final, fields) if e.field not in refused
        ]
    if errors:
        raise SubagentInvalid(errors)
    return PreparedSave(ctx.user_id, name, version, kind, patch, seed)


async def apply_save(
    prepared: PreparedSave,
    found: Subagents,
    conn,
    *,
    updated_by: str,
    thread_id: str | None = None,
) -> dict[str, Any] | None:
    """Make ``prepared`` on ``conn``, whose transaction holds the user's lock
    and read ``found`` under it: the row it leaves, None for a base reset to
    its own values.

    Raises ``VersionConflict`` when the subagent moved since it was checked,
    and ``SubagentInvalid`` when a new one's name or the cap was taken since.
    """
    from src.server.database.user_subagents import delete_user_subagent, upsert_user_subagent

    name, user_id = prepared.name, prepared.user_id
    if found.version(name) != prepared.version:
        raise VersionConflict()
    row = found.rows.get(name)
    if prepared.version is None:
        errors = validate_new_name(
            name,
            base_names=found.bases,
            custom_count=sum(1 for r in found.rows.values() if r.get("kind") == "custom"),
        )
        if errors:
            raise SubagentInvalid(errors)
        start = prepared.seed or ({}, "", list(ROLE_ORDER))
    elif prepared.kind == "custom":
        start = (row.get("fields") or {}, row.get("instructions") or "", row.get("roles") or list(ROLE_ORDER))
    else:
        base = found.bases[name]
        new_fields = _override_fields(base, row, prepared.patch)
        if not new_fields:
            # Nothing left that differs from the base: a row would only mark
            # it edited, so the save is a reset.
            if row is not None:
                await delete_user_subagent(user_id, name, conn=conn)
            return None
        return await upsert_user_subagent(
            user_id, name, kind="override", base_key=base.key, fields=new_fields,
            updated_by=updated_by, updated_thread_id=thread_id, conn=conn,
        )
    new_fields, instructions, roles = _custom_parts(*start, prepared.patch)
    return await upsert_user_subagent(
        user_id, name, kind="custom", fields=new_fields, instructions=instructions, roles=roles,
        updated_by=updated_by, updated_thread_id=thread_id, conn=conn,
    )


async def save_subagent(
    user_id: str,
    name: str,
    fields: Mapping[str, Any],
    *,
    version: str | None,
    start_from: str | None = None,
) -> dict[str, Any]:
    """The pane's save (``prepare_save`` says what it takes): the view it
    leaves, with problems and thread titles.

    Raises ``SubagentInvalid`` (field errors), ``VersionConflict`` and
    ``SubagentsUnavailable``.
    """
    from src.server.database.pool import get_db_connection
    from src.server.database.user_subagents import list_user_subagents, lock_user_subagents

    ctx, rows = await asyncio.gather(load_context(user_id), list_user_subagents(user_id))
    prepared = await prepare_save(
        ctx, Subagents.of(ctx.bases, ctx.offered, rows), name, fields,
        version=version, start_from=start_from,
    )
    async with get_db_connection() as conn, conn.transaction():
        await lock_user_subagents(user_id, conn=conn)
        await apply_save(prepared, await read_subagents(user_id, conn=conn), conn, updated_by="user")
    _, after = await list_subagents(user_id, ctx=ctx)
    saved = next((v for v in after if v["name"] == name), None)
    if saved is None:
        # Removed by another writer between the commit and this read.
        raise VersionConflict()
    return saved


async def delete_subagent(user_id: str, name: str, *, version: str) -> bool:
    """Delete a custom subagent, or reset a base by removing the user's override.

    False when there was nothing to remove (a base never edited). Raises
    ``SubagentNotFound``, ``VersionConflict`` and ``SubagentsUnavailable``.
    """
    from src.server.database.pool import get_db_connection
    from src.server.database.user_subagents import delete_user_subagent, lock_user_subagents

    async with get_db_connection() as conn, conn.transaction():
        await lock_user_subagents(user_id, conn=conn)
        found = await read_subagents(user_id, conn=conn)
        target = found.views.get(name)
        if target is None:
            # A row with no view is an override its base keeps while not
            # offered (its plugin off): the page this came from is stale.
            if name in found.rows:
                raise VersionConflict()
            raise SubagentNotFound(name)
        if target["version"] != version:
            raise VersionConflict()
        return await delete_user_subagent(user_id, name, conn=conn)
