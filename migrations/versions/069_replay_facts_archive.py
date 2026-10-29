"""Migration 069: keep the archived stream of turns their checkpoints cannot cover.

A turn settled while live streams were archived in ``sse_events`` had what
only that archive kept resolved into its row's legacy facts
(``replay_facts.legacy``), against the turn's projection. A turn its
checkpoints cannot cover had no projection to resolve them against, so its
archive is all it shows, and replay no longer reads the column. This copies
that archive into ``replay_facts.archive``, which the turn's stub replays.
A row with legacy facts (version 3) needs no copy, and one that already holds
it is skipped, so a rerun copies nothing.

Rollback: ``downgrade`` removes the key; the previous build never reads it,
and ``sse_events`` still holds every archive.
"""

from alembic import op

revision = "069"
down_revision = "068"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ``sse_events IS NOT NULL`` reads only the null bitmap, so the scan
    # detoasts an archive only for the rows it copies.
    op.execute(
        """
        UPDATE conversation_responses
        SET replay_facts = COALESCE(replay_facts, '{}'::jsonb)
            || jsonb_build_object('archive', sse_events)
        WHERE sse_events IS NOT NULL
          AND (replay_facts #>> '{legacy,v}') IS DISTINCT FROM '3'
          AND NOT COALESCE(replay_facts ? 'archive', false)
        """
    )


def downgrade() -> None:
    op.execute(
        """
        UPDATE conversation_responses
        SET replay_facts = replay_facts - 'archive'
        WHERE replay_facts ? 'archive'
        """
    )
