"""Migration 140: skills in the organization library.

A skill item holds its own snapshot of the publisher's skill (``content_hash``
and the archive, in object storage under the organization's prefix or inline
as ``user_skills`` keeps one), so it stays installable whatever later happens
to the publisher's copy. Publishing the same skill again replaces the
snapshot: one item per publisher and skill name in an organization.

``user_skills.org_library_item_id`` names the item a member's installed copy
came from, with no foreign key: the copy outlives the item. Comparing the
copy's ``content_hash`` with the item's is how a member learns an update is
available.

Online-safe: every column is nullable with no default (catalog-only), the
widened check on ``org_library_items`` is added NOT VALID and validated in
an autocommit block, and every row there is a thread or file item that
already meets it. Rollback: roll the code back first, then ``alembic
downgrade 139``; it deletes skill items, since the narrower check cannot
hold them, and drops the columns.
"""

from alembic import op

revision = "140"
down_revision = "139"
branch_labels = None
depends_on = None

_CHECK = "org_library_items_kind"


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("""
        ALTER TABLE org_library_items
            ADD COLUMN IF NOT EXISTS skill_name VARCHAR(64),
            ADD COLUMN IF NOT EXISTS content_hash VARCHAR(71),
            ADD COLUMN IF NOT EXISTS archive_key TEXT,
            ADD COLUMN IF NOT EXISTS archive_blob BYTEA,
            ADD COLUMN IF NOT EXISTS archive_bytes BIGINT
    """)
    op.execute(f"""
        ALTER TABLE org_library_items
            DROP CONSTRAINT IF EXISTS {_CHECK},
            ADD CONSTRAINT {_CHECK} CHECK (
                (kind = 'thread' AND thread_id IS NOT NULL AND artifact_id IS NULL
                    AND skill_name IS NULL)
                OR (kind = 'file' AND artifact_id IS NOT NULL AND thread_id IS NULL
                    AND skill_name IS NULL)
                OR (kind = 'skill' AND thread_id IS NULL AND artifact_id IS NULL
                    AND skill_name IS NOT NULL AND content_hash IS NOT NULL
                    AND archive_bytes IS NOT NULL
                    AND (archive_key IS NULL) <> (archive_blob IS NULL))
            ) NOT VALID
    """)
    op.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_org_library_items_skill
        ON org_library_items (org_id, publisher_id, skill_name) WHERE kind = 'skill'
    """)
    op.execute(
        "ALTER TABLE user_skills ADD COLUMN IF NOT EXISTS org_library_item_id UUID"
    )
    with op.get_context().autocommit_block():
        op.execute("SET lock_timeout = 0")
        op.execute(f"ALTER TABLE org_library_items VALIDATE CONSTRAINT {_CHECK}")
        op.execute("RESET lock_timeout")


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("ALTER TABLE user_skills DROP COLUMN IF EXISTS org_library_item_id")
    op.execute("DELETE FROM org_library_items WHERE kind = 'skill'")
    op.execute("DROP INDEX IF EXISTS idx_org_library_items_skill")
    op.execute(f"""
        ALTER TABLE org_library_items
            DROP CONSTRAINT IF EXISTS {_CHECK},
            DROP COLUMN IF EXISTS skill_name,
            DROP COLUMN IF EXISTS content_hash,
            DROP COLUMN IF EXISTS archive_key,
            DROP COLUMN IF EXISTS archive_blob,
            DROP COLUMN IF EXISTS archive_bytes
    """)
    op.execute(f"""
        ALTER TABLE org_library_items
            ADD CONSTRAINT {_CHECK} CHECK (
                (kind = 'thread' AND thread_id IS NOT NULL AND artifact_id IS NULL)
                OR (kind = 'file' AND artifact_id IS NOT NULL AND thread_id IS NULL)
            )
    """)
