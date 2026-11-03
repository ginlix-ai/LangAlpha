"""Migration 103: the files a page loaded in code, kept with its record.

A published page may build a path as it runs (``'data/' + ticker + '.csv'``),
so no scan of its source names the file, and only the live page can report it.
Publishing carries such a file, and ``loaded_files`` remembers it: without the
list, the next review, read where the page has not run, would see the file as
removed and the next version would lose it. A file the source names is not
kept here, so a reference taken out of the page takes its file out of the next
version. Defaulted, so a worker of the previous build can leave it out.
"""

from alembic import op

revision = "103"
down_revision = "102"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Every share lookup reads this table: give up rather than queue them all.
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(
        """
        ALTER TABLE share_links
        ADD COLUMN IF NOT EXISTS loaded_files TEXT[] NOT NULL DEFAULT '{}'
        """
    )


def downgrade() -> None:
    op.execute("ALTER TABLE share_links DROP COLUMN IF EXISTS loaded_files")
