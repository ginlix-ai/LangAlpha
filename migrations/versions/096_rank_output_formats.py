"""Migration 096: the answer's form becomes one ranked ``output_format``.

How an answer with substance is handed back was two keys of
``agent_preference``: ``deliverable`` (``text`` or ``visual``), from the
onboarding form, and ``output_format`` (``html`` for a report, ``markdown`` or
absent for none chosen), from Settings. One question in two places let them
disagree. ``output_format`` is now a list of ``visual``, ``markdown`` and
``text``, most preferred first, and ``deliverable`` is gone.

A report is the visual answer's long form, so ``html`` and ``visual`` both
become ``visual`` first; ``text`` keeps the chat first. A stored ``markdown``
meant nothing was chosen and becomes no ranking. A value outside those
vocabularies is the user's own words and is left as it is.
``upgraded_agent_preference`` reads an unmigrated profile the same way, for
the profiles frozen in threads before this ran.

Rollback is a redeploy of the previous build after ``alembic downgrade 095``,
which writes the first-ranked format back as the old keys. It is lossy: a
report and a visual answer were two values before and are one now, so every
``visual`` comes back as ``deliverable: visual``.

Revision ID: 096
Revises: 095
"""

from alembic import op

revision = "096"
down_revision = "095"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        UPDATE user_preferences
        SET agent_preference =
            CASE
                WHEN agent_preference->>'deliverable' IN ('text', 'visual')
                    THEN agent_preference - 'output_format' - 'deliverable'
                ELSE agent_preference - 'output_format'
            END
            || CASE
                WHEN agent_preference->>'output_format' = 'html'
                    OR agent_preference->>'deliverable' = 'visual'
                    THEN '{"output_format": ["visual", "markdown", "text"]}'::jsonb
                WHEN agent_preference->>'deliverable' = 'text'
                    THEN '{"output_format": ["text", "markdown", "visual"]}'::jsonb
                ELSE '{}'::jsonb
            END
        WHERE COALESCE(agent_preference->>'output_format', 'markdown') IN ('html', 'markdown')
          AND (
              agent_preference->>'output_format' IN ('html', 'markdown')
              OR agent_preference->>'deliverable' IN ('text', 'visual')
          )
        """
    )


def downgrade() -> None:
    op.execute(
        """
        UPDATE user_preferences
        SET agent_preference = (agent_preference - 'output_format')
            || CASE agent_preference->'output_format'->>0
                WHEN 'visual' THEN '{"deliverable": "visual"}'::jsonb
                WHEN 'text' THEN '{"deliverable": "text"}'::jsonb
                ELSE '{}'::jsonb
            END
        WHERE jsonb_typeof(agent_preference->'output_format') = 'array'
        """
    )
