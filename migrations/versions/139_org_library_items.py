"""Migration 139: the organization library's items.

An item points at a thread's or a file's link that its publisher keeps on,
in the organization the publisher's account is in (``org_id``, as the auth
service names it at publishing). Members open the item through that link,
so the item stores no content: turning the link off takes the item out of
the listing, and deleting the thread or the file's record takes the item
with it.

``publisher_id`` has no foreign key, as ``activity_events.actor_id`` has
none: an item published by someone later suspended or removed stays in the
library. One item per thread and per file, so publishing again edits it.

A new table, so nothing the previous build reads changes. Rollback: roll the
code back first, then ``alembic downgrade 138`` drops the table and every
item in it.
"""

from alembic import op

revision = "139"
down_revision = "138"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS org_library_items (
            item_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            org_id TEXT NOT NULL,
            kind TEXT NOT NULL,
            title TEXT NOT NULL,
            summary TEXT NOT NULL DEFAULT '',
            publisher_id VARCHAR(255) NOT NULL,
            thread_id UUID
                REFERENCES conversation_threads(conversation_thread_id) ON DELETE CASCADE,
            artifact_id UUID
                REFERENCES share_links(artifact_id) ON DELETE CASCADE,
            published_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT org_library_items_kind CHECK (
                (kind = 'thread' AND thread_id IS NOT NULL AND artifact_id IS NULL)
                OR (kind = 'file' AND artifact_id IS NOT NULL AND thread_id IS NULL)
            )
        )
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_org_library_items_org
        ON org_library_items (org_id, published_at DESC, item_id DESC)
    """)
    op.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_org_library_items_thread
        ON org_library_items (thread_id) WHERE thread_id IS NOT NULL
    """)
    op.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_org_library_items_artifact
        ON org_library_items (artifact_id) WHERE artifact_id IS NOT NULL
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS org_library_items")
