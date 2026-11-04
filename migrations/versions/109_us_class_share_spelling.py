"""Migration 109: store a US class share dashed (``BRK-B``).

A US class share has two spellings, ``BRK.B`` (the exchange, Massive) and
``BRK-B`` (Yahoo, FMP). Both are accepted on input; ``display_spelling`` now
stores the dashed one. Rows written before can hold the dotted one, which
leaves one listing under two keys: a holding re-added as ``BRK-B`` would sit
beside the ``BRK.B`` one instead of merging into it, and a chart drawn on
``BRK.B`` would not show on ``BRK-B``.

Each table 063 respells is respelled here by 063's own statements, so a
collision merges by its rules: the oldest row survives, and a holding's
quantities sum with its cost averaged only within one currency. Watch targets
and price wakeups are respelled too, so each shows the listing as it is now
stored. A thread armed on both spellings for one market and kind keeps its
newest target, and the older ones are cancelled, as arming the newest would
have done. Only an A, B or C class after a dot reads as a
class share; any other dotted symbol is left as stored.

Rollback: ``alembic downgrade 108``; it changes nothing. The previous build
resolves ``BRK-B`` as the US listing too, so the respelled rows stay readable,
and rows merged on the way up stay merged. A row the previous build writes in
the dotted spelling, before or after a rollback, stays dotted; since the
downgrade changes nothing, ``alembic downgrade 108`` then ``alembic upgrade 109``
respells it.

Revision ID: 109
Revises: 108
"""

import importlib.util
from pathlib import Path

from alembic import op

revision = "109"
down_revision = "108"
branch_labels = None
depends_on = None


def _class_share(col: str) -> str:
    """SQL for *col* with a dotted US class share dashed, as ``display_spelling``
    writes it. Any other symbol is left as stored."""
    return (
        rf"CASE WHEN upper({col}) ~ '^[A-Z]+\.[ABC]$' THEN"
        rf" regexp_replace(upper({col}), '\.([ABC])$', '-\1') ELSE {col} END"
    )


def _respell_063():
    spec = importlib.util.spec_from_file_location(
        "migration_063", Path(__file__).with_name("063_shanghai_display_spelling.py")
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module._respell


def upgrade() -> None:
    _respell_063()(_class_share, r"^[A-Z]+-[ABC]$")
    target = _class_share("symbol")
    op.execute(rf"""
        WITH ranked AS (
            SELECT target_id,
                   row_number() OVER (
                       PARTITION BY conversation_thread_id, {target}, market, kind
                       ORDER BY created_at DESC, target_id DESC
                   ) AS rn
            FROM market_watch_targets
            WHERE status = 'armed' AND {target} ~ '^[A-Z]+-[ABC]$'
        )
        UPDATE market_watch_targets t
        SET status = 'cancelled', updated_at = NOW()
        FROM ranked r
        WHERE t.target_id = r.target_id AND r.rn > 1 AND t.status = 'armed'
    """)
    for table in ("market_watch_targets", "wakeups"):
        op.execute(f"UPDATE {table} SET symbol = {target} WHERE symbol <> {target}")


def downgrade() -> None:
    pass
