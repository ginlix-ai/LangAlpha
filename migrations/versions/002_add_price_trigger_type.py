"""Add 'price' to automation trigger_type CHECK constraint.

Revision ID: 002
"""

from alembic import op


revision = "002"
down_revision = "001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Drop and re-create the trigger_type CHECK constraint to include 'price'
    op.execute("""
        ALTER TABLE automations
            DROP CONSTRAINT IF EXISTS automations_trigger_type_check
    """)
    op.execute("""
        ALTER TABLE automations
            ADD CONSTRAINT automations_trigger_type_check
            CHECK (trigger_type IN ('cron', 'once', 'price'))
    """)


def downgrade() -> None:
    # Convert any price-triggered automations before narrowing the constraint
    op.execute("""
        UPDATE automations
        SET trigger_type = 'once', status = 'completed', trigger_config = NULL
        WHERE trigger_type = 'price'
    """)
    op.execute("""
        ALTER TABLE automations
            DROP CONSTRAINT IF EXISTS automations_trigger_type_check
    """)
    op.execute("""
        ALTER TABLE automations
            ADD CONSTRAINT automations_trigger_type_check
            CHECK (trigger_type IN ('cron', 'once'))
    """)
