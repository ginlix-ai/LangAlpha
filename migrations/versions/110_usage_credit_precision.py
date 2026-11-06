"""Migration 110: a turn's usage row takes any credit total.

``conversation_usages`` stored ``token_credits``, ``infrastructure_credits``
and ``total_credits`` as NUMERIC(10,6), so a row of 10,000 credits or more
(10 USD of model spend at the default rate) failed to insert with ``numeric
field overflow``. The turn's row is inserted inside its finalize transaction,
so a long turn on a costly model could not finalize: it ended as an error and
neither its usage nor its subagents' was recorded. The three columns become
unconstrained NUMERIC, like ``in_flight_credits``, which carries the same
figure while the turn runs. Dropping the precision needs no table rewrite.

Rollback: ``alembic downgrade 109`` narrows the columns back to
NUMERIC(10,6), rewriting the table and rounding each value to six places, and
fails if any row holds 10,000 credits or more.
"""

from alembic import op

revision = "110"
down_revision = "109"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(
        "ALTER TABLE conversation_usages "
        "ALTER COLUMN token_credits TYPE NUMERIC, "
        "ALTER COLUMN infrastructure_credits TYPE NUMERIC, "
        "ALTER COLUMN total_credits TYPE NUMERIC"
    )


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(
        "ALTER TABLE conversation_usages "
        "ALTER COLUMN token_credits TYPE NUMERIC(10, 6), "
        "ALTER COLUMN infrastructure_credits TYPE NUMERIC(10, 6), "
        "ALTER COLUMN total_credits TYPE NUMERIC(10, 6)"
    )
