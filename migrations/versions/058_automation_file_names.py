"""Migration 058: the file name each automation is served under.

The agent sees each automation as its own file in `.agents/user/automations/`,
named by ``file_name``, unique per user. It is a reference, apart from the
``name`` the user sees: the agent picks it when it creates one, and one made
elsewhere gets one derived from its name. Existing rows are named here by the
rule ``automation.file_name_stem`` follows (accents folded, anything else not
ASCII dropped, lowercase, runs of anything else as ``-``, at most 48
characters), numbered ``-2``, ``-3`` in creation order where names repeat; a
numbered name that another row already has as its own takes ``_`` and the
id's first 8 characters instead, which no derived name can hold.
"""

from alembic import op

revision = "058"
down_revision = "057"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("ALTER TABLE automations ADD COLUMN IF NOT EXISTS file_name VARCHAR(255)")
    # The stem as file_name_stem derives it: NFKD splits an accent off its
    # letter, then everything not ASCII goes, as encode("ascii", "ignore")
    # drops it, and "C" keeps lower() to ASCII whatever the database's locale.
    # The class's colons are escaped, or SQLAlchemy reads a bind parameter.
    op.execute("""
        WITH slugs AS (
            SELECT automation_id, user_id, created_at,
                   COALESCE(
                       NULLIF(btrim(left(btrim(regexp_replace(
                           lower(regexp_replace(normalize(name, NFKD), '[^[\\:ascii\\:]]+', '', 'g')
                                 COLLATE "C"),
                           '[^a-z0-9]+', '-', 'g'), '-'), 48), '-'), ''),
                       'automation'
                   ) AS base
            FROM automations
            WHERE file_name IS NULL
        ),
        numbered AS (
            SELECT automation_id, user_id, base,
                   row_number() OVER (
                       PARTITION BY user_id, base ORDER BY created_at, automation_id
                   ) AS n
            FROM slugs
        ),
        candidates AS (
            SELECT automation_id, user_id, base, n,
                   CASE WHEN n = 1 THEN base || '.json' ELSE base || '-' || n || '.json' END AS candidate
            FROM numbered
        )
        UPDATE automations a
        SET file_name = CASE
            WHEN c.n > 1 AND EXISTS (
                SELECT 1 FROM candidates o
                WHERE o.user_id = c.user_id AND o.n = 1 AND o.candidate = c.candidate
            ) THEN c.base || '_' || left(c.automation_id::text, 8) || '.json'
            ELSE c.candidate
        END
        FROM candidates c
        WHERE a.automation_id = c.automation_id
    """)
    # The release before this one inserts without a file name while a
    # blue/green cutover drains it, so the column names such a row itself.
    op.execute("""
        ALTER TABLE automations ALTER COLUMN file_name SET DEFAULT
            ('automation-' || substr(md5(random()::text || clock_timestamp()::text), 1, 8) || '.json')
    """)
    op.execute("ALTER TABLE automations ALTER COLUMN file_name SET NOT NULL")
    op.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_automations_user_file_name
        ON automations (user_id, file_name)
    """)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_automations_user_file_name")
    op.execute("ALTER TABLE automations DROP COLUMN IF EXISTS file_name")
