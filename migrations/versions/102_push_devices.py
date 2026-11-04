"""Migration 102: the phones a user's push notifications go to.

One row per app install, keyed by its APNs device token, which is unique per
install. A token registered again by another user moves to that user, and its
per-kind switches are reset, so a phone shared by two sign-ins never pushes
the account that signed out. ``bundle_id`` is the APNs topic and
``environment`` which APNs host the token belongs to (a development build gets
a sandbox token). ``preferences`` holds only the kinds switched off; a kind it
does not name is on. Rows go with their user.

Rollback: ``alembic downgrade 101``. The previous build never reads the table,
so it can also run beside it; downgrading drops every registered device, and
the apps register again on their next start.

Revision ID: 102
Revises: 101
"""

from alembic import op

revision = "102"
down_revision = "101"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("""
        CREATE TABLE IF NOT EXISTS push_devices (
            push_device_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id VARCHAR(255) NOT NULL
                REFERENCES users(user_id) ON DELETE CASCADE,
            token TEXT NOT NULL UNIQUE,
            platform VARCHAR(16) NOT NULL,
            bundle_id VARCHAR(255) NOT NULL,
            environment VARCHAR(16) NOT NULL
                CHECK (environment IN ('sandbox', 'production')),
            device_name TEXT,
            app_version TEXT,
            preferences JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            last_sent_at TIMESTAMPTZ
        )
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_push_devices_user
        ON push_devices (user_id)
    """)


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("DROP TABLE IF EXISTS push_devices")
