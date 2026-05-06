"""Add 'executing' to automations.status CHECK constraint.

PriceMonitor._try_trigger marks an automation 'executing' before dispatch so
get_active_price_automations() (and the idx_automations_next_run partial index)
exclude it during in-flight execution. The original CHECK constraint in
001_initial_schema didn't include this transient value, so every price trigger
fails with a constraint violation and the dispatch never reaches the executor.

Revision ID: 010
"""

from alembic import op


revision = "010"
down_revision = "009"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE automations
            DROP CONSTRAINT IF EXISTS automations_status_check
    """)
    op.execute("""
        ALTER TABLE automations
            ADD CONSTRAINT automations_status_check
            CHECK (status IN ('active', 'paused', 'completed', 'disabled', 'executing'))
    """)


def downgrade() -> None:
    # Coerce any in-flight rows back to 'active' so the older constraint is satisfied.
    op.execute("""
        UPDATE automations SET status = 'active' WHERE status = 'executing'
    """)
    op.execute("""
        ALTER TABLE automations
            DROP CONSTRAINT IF EXISTS automations_status_check
    """)
    op.execute("""
        ALTER TABLE automations
            ADD CONSTRAINT automations_status_check
            CHECK (status IN ('active', 'paused', 'completed', 'disabled'))
    """)
