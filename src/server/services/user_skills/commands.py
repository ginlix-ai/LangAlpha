"""Canonical slash-trigger policy for the skill tiers.

A skill's EFFECTIVE trigger is its ``command`` alias when set, else its name.
Availability layers bottom-up: reserved names (platform skills and their
default commands), every row's effective trigger, and the user's platform
command renames. The DB layer re-checks row-vs-row collisions under the
per-user advisory lock (the race-safe truth); the helpers here own the
platform-tier checks and the seed policy shared by the upload and
sandbox-import paths. Conflicts raise ``ValueError`` with the user-facing
message — the routers map it to 409.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from src.server.database.user_skills import (
    list_all_user_skills,
    list_user_skills,
    user_trigger_guard,
)
from src.server.services.features import (
    get_skill_command_overrides,
    set_skill_command_override,
)
from src.server.services.user_skills.validate import (
    ValidatedSkill,
    reserved_skill_names,
)


def effective_trigger(row: dict[str, Any]) -> str:
    """The trigger a row answers to: its alias when set, else its name."""
    return row.get("command") or row["name"]


def taken_triggers(
    rows: Iterable[dict[str, Any]],
    overrides: Mapping[str, str] | None = None,
    *,
    exclude: tuple[str, str | None] | None = None,
) -> set[str]:
    """Every trigger in use among ``rows``, plus builtin-override aliases.

    A row's NAME stays reserved even when an alias hides it — clearing an
    alias (which skips collision checks by design) must always land back on
    a free trigger. ``exclude`` names one ``(name, workspace_id)`` row to
    skip — the row being edited or replaced.
    """
    taken: set[str] = set()
    for r in rows:
        if exclude is not None and (
            r["name"] == exclude[0] and r.get("workspace_id") == exclude[1]
        ):
            continue
        taken.add(r["name"])
        taken.add(effective_trigger(r))
    if overrides:
        taken |= set(overrides.values())
    return taken


def free_seed(
    validated: ValidatedSkill,
    rows: Iterable[dict[str, Any]],
    overrides: Mapping[str, str] | None,
    *,
    exclude: tuple[str, str | None] | None = None,
) -> str | None:
    """Frontmatter ``command:`` seeds the alias column once, and only when
    nothing can already answer to it — reserved, any visible row's effective
    trigger, or a builtin's override. Silent on conflict: the skill still
    installs, triggered by its name."""
    seed = validated.command
    if not seed or seed == validated.name or seed in reserved_skill_names():
        return None
    if seed in taken_triggers(rows, overrides, exclude=exclude):
        return None
    return seed


async def upload_seed(
    user_id: str, validated: ValidatedSkill, workspace_id: str | None
) -> str | None:
    """The upload path's seed: the visible rows are the user tier plus, on a
    scoped upload, that workspace's. Replaces never re-seed (the upsert skips
    the column), so excluding the same-scope row only covers that no-op case."""
    if not validated.command:
        return None
    rows = await list_user_skills(user_id)
    if workspace_id is not None:
        rows += await list_user_skills(user_id, workspace_id=workspace_id)
    return free_seed(
        validated,
        rows,
        await get_skill_command_overrides(user_id),
        exclude=(validated.name, workspace_id),
    )


async def ensure_free_of_platform(
    user_id: str,
    command: str,
    *,
    overrides: Mapping[str, str] | None = None,
) -> None:
    """Row aliases must stay clear of the platform tier: builtin names and
    default commands (reserved), plus any alias the user gave a builtin.

    A caller checking a set of names passes the override map it already read,
    so a fan-out costs one preferences read rather than one per skill.
    """
    if command in reserved_skill_names():
        raise ValueError(f"/{command} is reserved by a built-in skill or command")
    if overrides is None:
        overrides = await get_skill_command_overrides(user_id)
    if command in set(overrides.values()):
        raise ValueError(f"/{command} is already in use by another skill")


async def set_platform_alias(
    user_id: str,
    skill_name: str,
    default_command: str | None,
    command: str | None,
) -> dict[str, str]:
    """Rename a builtin's trigger via preferences; returns the new override
    map. The alias must not collide with anything the slash menu can show:
    other reserved names (the skill's own name and default command mean
    "back to default"), any row's effective trigger in any scope, or another
    builtin's alias."""
    if command is not None and command in (skill_name, default_command):
        command = None
    # The guard holds the same per-user lock the row writers take for their
    # trigger checks, so neither tier can read the other pre-commit and
    # conclude a trigger is free while the other is writing it.
    async with user_trigger_guard(user_id):
        if command is not None:
            if command in reserved_skill_names() - {skill_name, default_command}:
                raise ValueError(
                    f"/{command} is reserved by a built-in skill or command"
                )
            rows = await list_all_user_skills(user_id)
            overrides = await get_skill_command_overrides(user_id)
            others = {n: c for n, c in overrides.items() if n != skill_name}
            if command in taken_triggers(rows, others):
                raise ValueError(f"/{command} is already in use by another skill")
        return await set_skill_command_override(user_id, skill_name, command)
