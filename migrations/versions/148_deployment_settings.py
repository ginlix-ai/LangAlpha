"""Migration 148: what the setup step of a self-hosted deployment saves.

Vendor keys and the sandbox choice, one row per name, each value encrypted at
rest with the same key as ``user_api_keys``. The rows reach running code as
environment values, so nothing downstream learns where a key came from. Only
``HOST_MODE=oss`` reads or writes this table.
"""

from alembic import op

revision = "148"
down_revision = "147"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("""
        CREATE TABLE IF NOT EXISTS deployment_settings (
            name TEXT PRIMARY KEY,
            value BYTEA NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("DROP TABLE IF EXISTS deployment_settings")
