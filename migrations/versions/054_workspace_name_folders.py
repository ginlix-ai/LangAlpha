"""Migration 054: one workspace per name per user, and folders named after them.

A workspace's folder on its computer is now its name, so two live workspaces
of one user may not share a name. ``name_key`` is the folded folder spelling
two names must not share, unique per user over live non-Flash rows (a Flash
workspace has no folder). ``previous_dir_names`` keeps the folders a
workspace was renamed out of, newest first, so an old path still resolves.
``dir_name`` widens to 255 bytes because it is the name now, not a slug.

The backfill runs in Python with a frozen copy of the name rules, because the
fold is Unicode-aware and Postgres ``lower`` is not ``casefold``. Per user,
oldest first: a name over 80 characters is cut to 80, a name with no folder
spelling at all becomes "Workspace", and a later duplicate, or a name reserved
as a folder, takes the first free " (2)", " (3)" suffix. Folders do not move
here; each follows its name at the computer's next acquisition.

Tombstones now hold their folder name until their cleanup removes the folder,
and completing cleanup clears ``dir_name``. A tombstone from before that rule
still holds ``dir_name`` after its cleanup ran, which would keep the name
taken on that computer forever, so it is marked for cleanup again: the next
pass finds the folder gone and hands the name back. Without a computer there
is no folder to find, so ``dir_name`` is cleared directly.

The previous build keeps serving through the deploy and writes rows this
revision did not key: one it creates has no ``name_key``, one it renames keeps
its old name's key, and one it renames while the backfill runs is left
unkeyed. The index cannot refuse those writes, and until a follow-up migration
re-keys them once the previous build has drained (numbering duplicates as
``plan_names`` does), such a row is invisible to the name index under its
current name.

Rollback: ``alembic stamp 053`` first, because the previous build's
``upgrade head`` cannot start from a revision it does not ship, then redeploy
it; it reads none of the new columns and binds each workspace through
``dir_name`` wherever that points. Upgrading again reruns the backfill from
what it reads, clearing the keys the first run wrote. ``downgrade`` drops the
index and both columns and leaves ``dir_name`` wide (narrowing it would fail
on any longer folder) and the renamed duplicates renamed (names are user data).
Once a settle has moved folders, the build rolled back to must quote folder
paths in its shell commands: one that does not splits a folder named
``Q3 Earnings`` into two paths, and breaks on a name with a quote.
"""

from __future__ import annotations

import logging
import re
import unicodedata

from alembic import op
from sqlalchemy import text

revision = "054"
down_revision = "053"
branch_labels = None
depends_on = None

_NAME_INDEX = "idx_workspaces_user_name_key"

# Frozen copy of src/server/database/workspace_names.py as of this revision.
_NAME_MAX_CHARS = 80
_FOLDER_MAX_BYTES = 255
_RESERVED = frozenset({"_internal", "mcp_servers", ".agents", ".system", "tools", "code"})
_UNSAFE = re.compile(r'[/\\:$`"\x00-\x1f\x7f]')
_FALLBACK_NAME = "Workspace"


def _fit_bytes(value: str, limit: int) -> str:
    encoded = value.encode("utf-8")
    if len(encoded) <= limit:
        return value
    return encoded[:limit].decode("utf-8", errors="ignore").rstrip()


def _unfitted(name: str) -> str:
    folder = unicodedata.normalize("NFC", name or "")
    return _UNSAFE.sub("-", " ".join(folder.split())).lstrip(".- ")


def _spelling(name: str) -> str:
    return _fit_bytes(_unfitted(name), _FOLDER_MAX_BYTES)


def _folder(name: str) -> str | None:
    """The folder spelling, or None when the name has no usable one."""
    folder = _spelling(name)
    if not folder or folder.casefold() in _RESERVED:
        return None
    return folder


def _key(name: str) -> str | None:
    folder = _folder(name)
    return folder.casefold() if folder else None


def _suffixed(name: str, suffix: str) -> str:
    base = name.strip()
    while base and (
        len(base) + len(suffix) > _NAME_MAX_CHARS
        or len(_unfitted(base + suffix).encode("utf-8")) > _FOLDER_MAX_BYTES
    ):
        base = base[:-1].rstrip()
    return f"{base}{suffix}" if base else suffix.strip()


def plan_names(rows) -> list[tuple[str, str, str]]:
    """``(workspace_id, name, name_key)`` for every row, given rows ordered
    oldest first within each user as ``(workspace_id, user_id, name)``.

    Every valid name that is first to its key keeps it before any other row is
    suffixed, so a suffix never lands on a name the user typed. A name reserved
    as a folder is numbered like a duplicate ("Code (2)"), which the app would
    accept, so it stays recognizable; only a name with no folder spelling at
    all becomes "Workspace".
    """
    taken: dict[str, set[str]] = {}
    fitted = []
    for workspace_id, user_id, name in rows:
        keys = taken.setdefault(str(user_id), set())
        name = (name or "").strip()
        if len(name) > _NAME_MAX_CHARS:
            name = name[:_NAME_MAX_CHARS].rstrip()
        key = _key(name)
        keeps = key is not None and key not in keys
        if keeps:
            keys.add(key)
        elif not _spelling(name):
            name = _FALLBACK_NAME
        fitted.append((str(workspace_id), str(user_id), name, keeps))
    planned = []
    for workspace_id, user_id, name, keeps in fitted:
        candidate, n = name, 2
        while not keeps and (
            _key(candidate) is None or _key(candidate) in taken[user_id]
        ):
            candidate = _suffixed(name, f" ({n})")
            n += 1
        taken[user_id].add(_key(candidate))
        planned.append((workspace_id, candidate, _key(candidate)))
    return planned


# Guarded on the name that was read: the previous build keeps serving and may
# rename a workspace between the read and this write, and its name wins.
_BACKFILL_SQL = """
    UPDATE workspaces w
    SET name = p.name, name_key = p.name_key
    FROM unnest(
        CAST(:ids AS uuid[]), CAST(:read AS text[]),
        CAST(:names AS text[]), CAST(:keys AS text[])
    ) AS p(workspace_id, read_name, name, name_key)
    WHERE w.workspace_id = p.workspace_id AND w.name = p.read_name
    RETURNING w.workspace_id
"""


def upgrade() -> None:
    # Own transaction: ALTER TABLE's ACCESS EXCLUSIVE would otherwise hold
    # every read of workspaces through the backfill.
    with op.get_context().autocommit_block():
        op.execute("SET lock_timeout = '5s'")
        op.execute(
            """
            ALTER TABLE workspaces
                ALTER COLUMN dir_name TYPE VARCHAR(255),
                ADD COLUMN IF NOT EXISTS name_key TEXT,
                ADD COLUMN IF NOT EXISTS previous_dir_names TEXT[] NOT NULL DEFAULT '{}'
            """
        )
        op.execute("RESET lock_timeout")

    bind = op.get_bind()
    bind.execute(text("SET LOCAL lock_timeout = '5s'"))
    rows = bind.execute(
        text(
            """
            SELECT workspace_id, user_id, name FROM workspaces
            WHERE status NOT IN ('deleted', 'flash')
            ORDER BY user_id, created_at, workspace_id
            """
        )
    ).fetchall()
    read = {str(r[0]): r[2] for r in rows}
    planned = plan_names(rows)

    # Read and planned first: switching the trigger holds every writer until
    # commit. The trigger would stamp every backfilled row as just edited and
    # float long-idle workspaces to the top of the gallery.
    bind.execute(text("ALTER TABLE workspaces DISABLE TRIGGER trg_workspaces_updated_at"))
    bind.execute(
        text(
            """
            UPDATE workspaces
            SET config = jsonb_set(
                    COALESCE(config, '{}'::jsonb), '{folder_cleanup_pending}', 'true'::jsonb
                )
            WHERE status = 'deleted' AND computer_id IS NOT NULL AND dir_name IS NOT NULL
              AND NOT COALESCE(config, '{}'::jsonb) @> '{"folder_cleanup_pending": true}'
            """
        )
    )
    bind.execute(
        text(
            "UPDATE workspaces SET dir_name = NULL "
            "WHERE status = 'deleted' AND computer_id IS NULL AND dir_name IS NOT NULL"
        )
    )
    # Rerun after the documented rollback, the first run's keys are still here
    # and the name index may still enforce them, so a stale key (the previous
    # build renames without touching it) would collide with a new one.
    bind.execute(text("UPDATE workspaces SET name_key = NULL WHERE name_key IS NOT NULL"))
    written: set[str] = set()
    if planned:
        result = bind.execute(
            text(_BACKFILL_SQL),
            {
                "ids": [i for i, _n, _k in planned],
                "read": [read[i] for i, _n, _k in planned],
                "names": [n for _i, n, _k in planned],
                "keys": [k for _i, _n, k in planned],
            },
        )
        written = {str(r[0]) for r in result.fetchall()}
    bind.execute(text("ALTER TABLE workspaces ENABLE TRIGGER trg_workspaces_updated_at"))
    renamed = sum(1 for i, n, _k in planned if i in written and n != read[i])
    logging.getLogger("alembic.runtime.migration").info(
        f"054: keyed {len(written)} of {len(planned)} workspace(s), renamed {renamed}"
    )

    # workspaces is written on every turn: build without blocking writers.
    # Drop-before-create repairs an INVALID index an interrupted concurrent
    # build left, instead of adopting it.
    with op.get_context().autocommit_block():
        op.execute("SET lock_timeout = 0")
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {_NAME_INDEX}")
        op.execute(
            f"""
            CREATE UNIQUE INDEX CONCURRENTLY {_NAME_INDEX}
            ON workspaces (user_id, name_key)
            WHERE status NOT IN ('deleted', 'flash')
            """
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("SET lock_timeout = 0")
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {_NAME_INDEX}")
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(
        """
        ALTER TABLE workspaces
            DROP COLUMN IF EXISTS previous_dir_names,
            DROP COLUMN IF EXISTS name_key
        """
    )
