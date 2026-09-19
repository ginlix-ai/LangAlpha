"""Status fences, the computers column list, and the derivations that must match them.

Nothing here imports the project, so an operator script can hold exactly the
predicates the app writes without reaching ``src.config``, whose ``load_dotenv()``
would retarget a mutating job at whatever ``.env`` is on disk.
"""

from __future__ import annotations

import hashlib
import re
from typing import Optional

# Share the bind fence across entry points. A whitelist could reject future
# transitional states and make losing binders destroy valid provisions.
FENCE_BINDABLE = "status NOT IN ('deleted', 'stopping', 'stopped')"

# Tombstones retain provider_ref; prevent a racing reaper from reviving a billed machine.
FENCE_NOT_DELETED = "status <> 'deleted'"

# A flash workspace has no computer, so a machine-scoped write must not reach
# it and overwrite the 'flash' marker with the machine's own status.
FENCE_LIVE_WORKSPACE = "status NOT IN ('deleted', 'flash')"

# One literal column list keeps every dict_row result shape consistent.
COMPUTER_COLS = (
    "computer_id, user_id, kind, provider_ref, name, is_primary, status, "
    "resource_tier, is_always_on, platform_secret_version, mcp_config_version, "
    "root_dir, layout_version, origin_workspace_id, provider_config, artifacts, "
    "last_activity_at, stopped_at, created_at, updated_at, config"
)

# migration 046's dir_name column width. The separator and the hex suffix come
# out of the slug's budget, so widening the suffix cannot overflow the column.
DIR_NAME_MAX = 64


def workspace_dir_name(
    name: Optional[str], workspace_id: str, *, hex_chars: int = 4
) -> str:
    """Match workspace.py's _DIR_NAME_SQL before a row exists; widen hex_chars on a taken folder.

    The slug pays for the suffix, so a re-slug cannot overflow dir_name and turn
    a folder collision into a truncation error the UniqueViolation loop misses.
    """
    slug_cap = DIR_NAME_MAX - 1 - hex_chars
    slug = re.sub(r"[^a-z0-9]+", "-", (name or "").lower())[:slug_cap].strip("-")
    digest = hashlib.md5(str(workspace_id).encode("utf-8")).hexdigest()
    return f"{slug or 'workspace'}-{digest[:hex_chars]}"


def advisory_key(domain: str, *parts: str) -> int:
    """sha256("domain|part|part")[:8] as a signed bigint for pg advisory locks."""
    digest = hashlib.sha256("|".join((domain, *parts)).encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big", signed=True)


# The one shadow tail: which projects the machine write reached, for pub/sub.
SHADOWED_IDS = (
    "COALESCE((SELECT array_agg(workspace_id) FROM shadow), ARRAY[]::uuid[])"
    " AS shadowed_workspace_ids"
)


def shadowed_write(
    *,
    authority: str,
    computer_set: str,
    workspace_set: str,
    computer_returning: str,
    workspace_returning: str,
    select: str,
    computer_fence: str = FENCE_NOT_DELETED,
    workspace_fence: str = FENCE_LIVE_WORKSPACE,
    computer_guard: str = "",
    workspace_guard: str = "",
    fan_out: bool = False,
    now: str = "NOW()",
) -> str:
    """Build the one statement shape that writes a computer and its workspace shadows.

    comp always updates first and shadow always reads comp's output, through a
    join even where no column is wanted, because that dependency is what forces
    both writers to take the row locks in the same order. Guards are whole
    ``AND ...`` clauses; fences are the module's predicates, applied under the
    alias so PostgreSQL rechecks them inside the UPDATE.
    """
    comp_fence = f"\n                  AND c.{computer_fence}" if computer_fence else ""
    ws_fence = f"\n                  AND w.{workspace_fence}" if workspace_fence else ""

    if authority == "computer":
        return f"""
            WITH comp AS (
                UPDATE computers c
                SET {computer_set},
                    updated_at = {now}
                WHERE c.computer_id = %(computer_id)s{comp_fence}{computer_guard}
                RETURNING {computer_returning}
            ),
            shadow AS (
                UPDATE workspaces w
                SET {workspace_set},
                    updated_at = {now}
                FROM comp
                WHERE w.computer_id = comp.computer_id{ws_fence}{workspace_guard}
                RETURNING {workspace_returning}
            )
            {select}
        """
    if authority == "workspace":
        scope = "w.workspace_id = %(workspace_id)s"
        join = ""
        if fan_out:
            # A machine transition reaches every project on it, so no sibling can
            # go on reporting the machine's old status.
            scope = f"{scope}\n                        OR w.computer_id = comp.mirrored_computer_id"
            join = "\n                LEFT JOIN comp ON TRUE"
        return f"""
            WITH comp AS (
                UPDATE computers c
                SET {computer_set},
                    updated_at = {now}
                FROM workspaces w
                WHERE w.workspace_id = %(workspace_id)s
                  AND c.computer_id = w.computer_id{ws_fence}{comp_fence}{computer_guard}
                RETURNING {computer_returning}
            ),
            shadow AS (
                UPDATE workspaces w
                SET {workspace_set},
                    updated_at = {now}
                FROM (SELECT count(*) AS mirrored FROM comp) m{join}
                WHERE (
                        {scope}
                      ){ws_fence}{workspace_guard}
                RETURNING {workspace_returning}
            )
            {select}
        """
    raise ValueError(f"Unknown shadowed_write authority: {authority!r}")
