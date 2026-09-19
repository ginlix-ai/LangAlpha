"""Migration 046: separate computers while retaining workspace shadows for old readers.

Keep shadows for rolling deployments and platform quotas; deploy 046, then
ginlix-platform, then langalpha, since platform /validate counts computers
without a schema probe. Partial UNIQUE (kind, provider_ref) excludes NULL refs
and deleted rows; shared sandbox ids go to the most recently active workspace,
leaving losers with NULL computer_id on the legacy path. dir_name stays nullable
because the old deployment still inserts workspaces without it.
"""

import os

from alembic import op

revision = "046"
down_revision = "045"
branch_labels = None
depends_on = None

# Flash workspaces have no sandbox and therefore no computer.
_STATUSES = (
    "creating",
    "starting",
    "running",
    "stopping",
    "stopped",
    "error",
    "deleted",
)

# Use the runtime's per-process provider choice; unknown kinds use hosted default daytona.
_KNOWN_KINDS = ("daytona", "docker")
DEFAULT_ROOT_DIR = "/home/workspace"
COMPUTER_NAMES_BY_KIND = {"daytona": "Cloud computer", "docker": "Local computer"}


def _backfill_kind() -> str:
    kind = (os.getenv("SANDBOX_PROVIDER") or "").strip().lower()
    return kind if kind in _KNOWN_KINDS else "daytona"


def upgrade() -> None:
    statuses = ", ".join(f"'{s}'" for s in _STATUSES)
    op.execute(f"""
        CREATE TABLE IF NOT EXISTS computers (
            computer_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id VARCHAR(255) NOT NULL
                REFERENCES users(user_id) ON DELETE CASCADE,
            kind VARCHAR(32) NOT NULL DEFAULT 'daytona',
            provider_ref VARCHAR(255),
            name VARCHAR(255) NOT NULL DEFAULT '{COMPUTER_NAMES_BY_KIND["daytona"]}',
            is_primary BOOLEAN NOT NULL DEFAULT FALSE,
            status VARCHAR(50) NOT NULL DEFAULT 'creating'
                CHECK (status IN ({statuses})),
            resource_tier VARCHAR(32) NOT NULL DEFAULT 'standard',
            is_always_on BOOLEAN NOT NULL DEFAULT FALSE,
            platform_secret_version INTEGER NOT NULL DEFAULT 0,
            mcp_config_version INTEGER NOT NULL DEFAULT 0,
            root_dir VARCHAR(255) NOT NULL DEFAULT '{DEFAULT_ROOT_DIR}',
            layout_version INTEGER NOT NULL DEFAULT 0,
            -- The project whose files sit at the machine's root while the
            -- layout is v3, so the v3 to v4 move sweeps them into that
            -- project's folder rather than into whichever sibling starts
            -- first. Only a backfilled machine has one: a machine minted
            -- after this migration is born at v4 with every project already
            -- in a folder, so its root belongs to nobody.
            origin_workspace_id UUID
                REFERENCES workspaces(workspace_id) ON DELETE SET NULL,
            provider_config JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            artifacts JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            last_activity_at TIMESTAMPTZ,
            stopped_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            config JSONB NOT NULL DEFAULT '{{}}'::jsonb
        )
    """)

    # Ensure one live provisioner per vendor sandbox; tombstones must not block rebinding.
    op.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_computers_provider_ref
        ON computers (kind, provider_ref)
        WHERE provider_ref IS NOT NULL AND status <> 'deleted'
    """)
    # Fence the tombstone, as every primary probe does: a deleted row that kept
    # the flag would hold the slot and wedge the user's next primary forever.
    op.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_computers_primary_by_user
        ON computers (user_id)
        WHERE is_primary AND status <> 'deleted'
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_computers_user_status
        ON computers (user_id, status)
    """)
    # Match 016's per-user always-on quota lookup.
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_computers_always_on_by_user
        ON computers (user_id)
        WHERE is_always_on
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_computers_status_activity
        ON computers (status, last_activity_at ASC NULLS FIRST)
    """)
    # ON DELETE SET NULL has no index of its own, so a hard project delete
    # would scan every computer to clear the reference.
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_computers_origin_workspace
        ON computers (origin_workspace_id)
        WHERE origin_workspace_id IS NOT NULL
    """)

    # Deleting a machine must preserve projects and their files.
    op.execute("""
        ALTER TABLE workspaces
            ADD COLUMN IF NOT EXISTS computer_id UUID
                REFERENCES computers(computer_id) ON DELETE SET NULL
    """)
    op.execute("""
        ALTER TABLE workspaces
            ADD COLUMN IF NOT EXISTS dir_name VARCHAR(64)
    """)
    # The layout a project's files were first written under. The backfill
    # stamps 3 on every row that predates the folder layout; the app leaves it
    # NULL on a new row. The prompt tells a stamped project that its older
    # files and notes may still spell the pre-folder paths.
    op.execute("""
        ALTER TABLE workspaces
            ADD COLUMN IF NOT EXISTS layout_origin SMALLINT
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_workspaces_computer
        ON workspaces (computer_id)
        WHERE computer_id IS NOT NULL
    """)
    op.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_workspaces_computer_dir
        ON workspaces (computer_id, dir_name)
        WHERE computer_id IS NOT NULL
    """)

    # workspace_id still authorizes grants; this unread shadow must not cascade-delete them.
    op.execute("""
        ALTER TABLE sandbox_egress_grants
            ADD COLUMN IF NOT EXISTS computer_id UUID
                REFERENCES computers(computer_id) ON DELETE SET NULL
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_sandbox_egress_grants_computer
        ON sandbox_egress_grants (computer_id)
        WHERE computer_id IS NOT NULL
    """)

    _backfill(_backfill_kind())


def _backfill(kind: str) -> None:
    """Report skipped shared-id workspaces via NOTICE for operators; no later migration needs counts."""
    name = COMPUTER_NAMES_BY_KIND[kind]
    op.execute(f"""
        DO $$
        DECLARE
            shared_ids   INTEGER := 0;
            shared_rows  INTEGER := 0;
            computers_n  INTEGER := 0;
            bound_n      INTEGER := 0;
            primary_n    INTEGER := 0;
            dirs_n       INTEGER := 0;
            grants_n     INTEGER := 0;
            no_sandbox_n INTEGER := 0;
        BEGIN
            SELECT count(*) INTO no_sandbox_n
              FROM workspaces
             WHERE sandbox_id IS NULL AND status NOT IN ('deleted', 'flash');

            SELECT COALESCE(count(*), 0), COALESCE(sum(n) - count(*), 0)
              INTO shared_ids, shared_rows
              FROM (
                SELECT sandbox_id, count(*) AS n
                  FROM workspaces
                 WHERE sandbox_id IS NOT NULL AND status NOT IN ('deleted', 'flash')
                 GROUP BY sandbox_id
                HAVING count(*) > 1
              ) d;

            WITH winners AS (
                SELECT DISTINCT ON (sandbox_id)
                       workspace_id, user_id, sandbox_id, status, resource_tier,
                       is_always_on, platform_secret_version, mcp_config_version,
                       artifacts, last_activity_at, stopped_at, created_at
                  FROM workspaces w
                 WHERE sandbox_id IS NOT NULL AND status NOT IN ('deleted', 'flash')
                   -- Skip what is already bound or already claimed, so a
                   -- second pass elects nobody instead of re-inserting the
                   -- same provider_ref and tripping the unique index. Every
                   -- DDL statement above is IF NOT EXISTS; this is what makes
                   -- the backfill match them.
                   AND w.computer_id IS NULL
                   AND NOT EXISTS (
                       SELECT 1
                         FROM computers c
                        WHERE c.kind = '{kind}'
                          AND c.provider_ref = w.sandbox_id
                          AND c.status <> 'deleted'
                   )
                 ORDER BY sandbox_id,
                          COALESCE(last_activity_at, updated_at, created_at)
                              DESC NULLS LAST,
                          workspace_id DESC
            ),
            inserted AS (
                -- layout_version stays 0 on purpose. The sandbox manifest is
                -- the only authority for it and workspaces never recorded it,
                -- so there is nothing here to copy; 0 means "not observed yet"
                -- and the machine's first asset sync stamps what it finds.
                -- The elected workspace is also the machine's root owner:
                -- under the layout this fleet is on, its files are the ones
                -- lying at the sandbox root with no folder of their own.
                INSERT INTO computers (
                    user_id, kind, name, provider_ref, status, resource_tier,
                    is_always_on, platform_secret_version, mcp_config_version,
                    layout_version, origin_workspace_id, artifacts,
                    last_activity_at, stopped_at, created_at, updated_at
                )
                SELECT user_id, '{kind}', '{name}', sandbox_id, status, resource_tier,
                       is_always_on, platform_secret_version, mcp_config_version,
                       0, workspace_id, COALESCE(artifacts, '{{}}'::jsonb),
                       last_activity_at, stopped_at, created_at, NOW()
                  FROM winners
                RETURNING computer_id, provider_ref
            )
            UPDATE workspaces w
               SET computer_id = i.computer_id
              FROM inserted i
              JOIN winners x ON x.sandbox_id = i.provider_ref
             WHERE w.workspace_id = x.workspace_id;
            GET DIAGNOSTICS bound_n = ROW_COUNT;

            SELECT count(*) INTO computers_n FROM computers;

            -- The user's primary is the computer they used last. Every other
            -- one of theirs stays non-primary, which is what the partial unique
            -- index enforces from here on.
            WITH ranked AS (
                SELECT computer_id, user_id,
                       row_number() OVER (
                           PARTITION BY user_id
                           ORDER BY COALESCE(last_activity_at, updated_at,
                                             created_at) DESC NULLS LAST,
                                    computer_id DESC
                       ) AS rn
                  FROM computers
                 WHERE status <> 'deleted'
            )
            UPDATE computers c
               SET is_primary = TRUE
              FROM ranked r
             WHERE c.computer_id = r.computer_id AND r.rn = 1
               -- Skip a user who already has their primary. A re-run ranks
               -- live computers again, so a machine the app minted since
               -- would win the ranking and be raised beside the existing
               -- primary, which the partial unique index refuses.
               AND NOT EXISTS (
                   SELECT 1
                     FROM computers p
                    WHERE p.user_id = r.user_id
                      AND p.is_primary
                      AND p.status <> 'deleted'
               );
            GET DIAGNOSTICS primary_n = ROW_COUNT;

            -- Slug of the name plus a deterministic 4-char suffix, so two
            -- projects called the same thing still get two folders. Fixed at
            -- backfill; a rename does not move the folder.
            UPDATE workspaces w
               SET dir_name = COALESCE(
                       NULLIF(
                           trim(BOTH '-' FROM left(
                               regexp_replace(lower(w.name), '[^a-z0-9]+',
                                              '-', 'g'), 59)),
                           ''),
                       'workspace')
                   || '-' || substr(md5(w.workspace_id::text), 1, 4),
                   layout_origin = 3
             WHERE w.computer_id IS NOT NULL AND w.dir_name IS NULL;
            GET DIAGNOSTICS dirs_n = ROW_COUNT;

            UPDATE sandbox_egress_grants g
               SET computer_id = w.computer_id
              FROM workspaces w
             WHERE w.workspace_id = g.workspace_id
               AND w.computer_id IS NOT NULL
               -- Re-run guard: a grant the app has since placed or revoked
               -- keeps its machine; rewriting it would collide with 047's
               -- partial unique index.
               AND g.computer_id IS NULL;
            GET DIAGNOSTICS grants_n = ROW_COUNT;

            -- USING MESSAGE rather than a format string: the message never
            -- reaches a driver that would read a '%' as a placeholder.
            RAISE NOTICE USING MESSAGE =
                '046: ' || computers_n || ' computer(s) created, '
                || bound_n || ' workspace(s) bound, '
                || primary_n || ' marked primary, '
                || dirs_n || ' dir_name(s), '
                || grants_n || ' grant(s) labelled; '
                || 'layout_version 0 (not observed) on every new row, '
                || 'stamped from the sandbox manifest at first asset sync; '
                || 'origin_workspace_id names the project that owns each '
                || 'machine root until the layout moves';
            -- Two deliberate skips the operator should see, because both make
            -- the platform's per-user count after cutover slightly looser than
            -- the per-workspace count it replaces.
            IF shared_ids > 0 THEN
                RAISE NOTICE USING MESSAGE =
                    '046: skipped ' || shared_rows || ' workspace row(s) '
                    || 'across ' || shared_ids || ' sandbox id(s) shared by '
                    || 'more than one live workspace; they keep computer_id '
                    || 'NULL and stay on the pre-computer path';
            END IF;
            IF no_sandbox_n > 0 THEN
                RAISE NOTICE USING MESSAGE =
                    '046: skipped ' || no_sandbox_n || ' live workspace(s) '
                    || 'with no sandbox_id (never provisioned, or caught '
                    || 'mid-provision); they get a computer at their next bind';
            END IF;
        END $$;
    """)


def downgrade() -> None:
    """Rollback preserves old readers: disk is unchanged and workspace shadows remain current."""
    op.execute("DROP INDEX IF EXISTS idx_sandbox_egress_grants_computer")
    op.execute("ALTER TABLE sandbox_egress_grants DROP COLUMN IF EXISTS computer_id")
    op.execute("DROP INDEX IF EXISTS idx_workspaces_computer_dir")
    op.execute("DROP INDEX IF EXISTS idx_workspaces_computer")
    op.execute("ALTER TABLE workspaces DROP COLUMN IF EXISTS layout_origin")
    op.execute("ALTER TABLE workspaces DROP COLUMN IF EXISTS dir_name")
    op.execute("ALTER TABLE workspaces DROP COLUMN IF EXISTS computer_id")
    op.execute("DROP TABLE IF EXISTS computers CASCADE")
