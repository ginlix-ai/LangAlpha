"""Migration 097: one live computer per name per user.

A computer's name alone says which of the user's computers it means, so two
live computers of one user may no longer share one. Names compare as the
index folds them, trimmed, each run of spaces read as one and lower-cased, so
"Lab box" and " lab  box " are the same name.

Existing duplicates are renamed first. Per user and name the oldest computer
keeps it, and each later one takes the first free " 2", " 3" numbering of its
name, as the app numbers a default name; whether a number is free is asked of
the database, so it folds exactly as the index will. The renames and the index
build share one transaction under a lock that holds writers to ``computers``
but not readers, so no duplicate can land between them. ``migrations/env.py``
runs every pending revision in that one transaction, so the lock lasts until
the whole upgrade commits, not just through this revision: every write to
``computers``, a start's or a stop's status included, waits until then. The
previous build, still serving during the deploy, has a create or rename onto a
taken name refused from then on rather than written.

Rollback: ``alembic downgrade 096``. It drops the index; the renamed computers
keep their new names, which are user data by now.

Revision ID: 097
Revises: 096
"""

from __future__ import annotations

import logging

from alembic import op
from sqlalchemy import text

revision = "097"
down_revision = "096"
branch_labels = None
depends_on = None

_INDEX = "idx_computers_user_name"
_NAME_MAX = 255
# Frozen copies, as of this revision, of the app's fence and of the fold and
# numbering in src/server/database/computer_names.py.
_LIVE = "status <> 'deleted'"
_KEY = r"lower(btrim(regexp_replace(name, '\s+', ' ', 'g')))"

# Every live computer that is not the oldest to its name, oldest first. A
# blank name has nothing to number, so it is numbered as "Computer".
_DUPLICATES = f"""
    SELECT computer_id, user_id, base
    FROM (
        SELECT computer_id, user_id, created_at,
               COALESCE(NULLIF(btrim(name), ''), 'Computer') AS base,
               row_number() OVER (
                   PARTITION BY user_id, {_KEY}
                   ORDER BY created_at, computer_id
               ) AS place
        FROM computers
        WHERE {_LIVE}
    ) ranked
    WHERE place > 1
    ORDER BY user_id, created_at, computer_id
"""

_TAKEN = f"""
    SELECT EXISTS (
        SELECT 1 FROM computers
        WHERE user_id = :user_id AND {_LIVE}
          AND {_KEY} = lower(btrim(regexp_replace(:name, '\\s+', ' ', 'g')))
    )
"""


def _numbered(base: str, number: int) -> str:
    suffix = f" {number}"
    return f"{base[: _NAME_MAX - len(suffix)].rstrip()}{suffix}"


def upgrade() -> None:
    bind = op.get_bind()
    bind.execute(text("SET LOCAL lock_timeout = '5s'"))
    bind.execute(text("LOCK TABLE computers IN SHARE ROW EXCLUSIVE MODE"))

    log = logging.getLogger("alembic.runtime.migration")
    duplicates = bind.execute(text(_DUPLICATES)).fetchall()
    for computer_id, user_id, base in duplicates:
        number = 2
        while bind.execute(
            text(_TAKEN), {"user_id": user_id, "name": _numbered(base, number)}
        ).scalar():
            number += 1
        name = _numbered(base, number)
        bind.execute(
            text("UPDATE computers SET name = :name WHERE computer_id = :computer_id"),
            {"name": name, "computer_id": computer_id},
        )
        log.info(f"097: renamed computer {computer_id} of {user_id} to {name!r}")
    log.info(f"097: renamed {len(duplicates)} computer(s) sharing a name")

    op.execute(
        f"""
        CREATE UNIQUE INDEX IF NOT EXISTS {_INDEX}
        ON computers (user_id, {_KEY})
        WHERE {_LIVE}
        """
    )


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(f"DROP INDEX IF EXISTS {_INDEX}")
