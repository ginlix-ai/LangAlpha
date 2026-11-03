"""Migration 104: the files the owner left out of what a page publishes.

A page's links go along with it, so a visitor who follows one finds the page
there. An owner may not want a linked page shared, and the review lets them say
so. Kept with the record rather than with one publish, or the next review and
every kept-updated version would put the file straight back. Defaulted, so a
worker of the previous build can leave it out.
"""

from alembic import op

revision = "104"
down_revision = "103"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Every share lookup reads this table: give up rather than queue them all.
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(
        """
        ALTER TABLE share_links
        ADD COLUMN IF NOT EXISTS left_out_files TEXT[] NOT NULL DEFAULT '{}'
        """
    )


def downgrade() -> None:
    op.execute("ALTER TABLE share_links DROP COLUMN IF EXISTS left_out_files")
