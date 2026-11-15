"""Migration 131: Firecrawl accounts per user, and the calls the relay meters.

``user_firecrawl_accounts`` holds the Firecrawl account the deployment's
Firecrawl for Platforms integration created for each user, one per user,
created when the user, with credits to spend, accepts Firecrawl's Terms of
Service, which their agent's first Firecrawl call asks of them. ``api_key`` is ``pgp_sym_encrypt`` of the account's key, with the key
the vault and the OAuth tokens use. The key never leaves the host: the egress relay attaches it.

``metered_vendor_calls`` is one row per paid call the relay forwarded. A row
starts ``held`` at the call's estimated price, so calls running at once see
each other, and becomes ``settled`` at what the reply says it cost, or
``estimated`` when the reply never said (cut short, or unreadable). As it
settles it is charged on a usage row of its own on its thread (``thread_id``,
who the command that made it ran for) and stamped ``collected_at`` in the same
transaction; a row with no thread known, or a hold nothing settled, is charged
when the user's next turn starts. ``call_id`` names the command whose code
made the call, whose result lists what it cost. ``job_ref`` keys a vendor job
whose cost grows across polls (a crawl), so each poll records only the growth.
The ``(user_id, collected_at)`` index serves what admission reads (calls owed,
and calls charged since the balance was read) and the turn-start check for
anything owed.

``relayed_sources`` holds the sources the relay read off those replies (a
page, a search result, a catalog record), each with its full body, until the
command that made the call collects them into the turn's provenance, where
they are redacted and stored like any other source's. ``job_ref`` keeps a
crawl's pages from being recorded again on each status poll; the index
compares a digest of each page's URL, since a btree entry holds at most about
2.7 KB and a crawl reaches longer URLs.
``provenance_records`` gains ``cost_credits`` (what the source cost the user)
and ``site`` (the provider's site, for its icon).

All three reference ``users`` with ``ON DELETE CASCADE ON UPDATE CASCADE``,
so their rows follow ``migrate_user_id`` and go with the user. The Firecrawl
account itself stays at Firecrawl, which offers no way to delete one.

``sandbox_egress_grants`` gains the ``platform_mcp`` kind: a built-in server
whose credential the host holds for the user, named by ``server_name`` like a
header grant.

Locks, since the previous build serves while this runs: the grants CHECK
is added ``NOT VALID`` and committed on its own, then validated in an
autocommit block, where VALIDATE takes SHARE UPDATE EXCLUSIVE and blocks
neither reads nor writes. The new tables follow in a second transaction, with
the one ALTER of ``provenance_records`` last, so its ACCESS EXCLUSIVE lock
is held only for the commit.

Rollback: ``alembic downgrade 130``. It drops the three tables, the two
provenance columns and the platform grants, so every provisioned key is
forgotten (the accounts remain at Firecrawl, and the next provision for the
same user returns the same key). It also drops paid calls not yet charged to
any thread and sources no turn has collected.
"""

from alembic import op

revision = "131"
down_revision = "130"
branch_labels = None
depends_on = None


_GRANT_CHECK = "sandbox_egress_grants_kind_reference"


def _replace_grant_check(kinds: str) -> None:
    """Swap the grants CHECK for one allowing ``kinds`` beside OAuth, as a
    catalog change that commits on its own, then validate it outside any
    transaction. Re-runnable: the DROP clears a check left unvalidated by an
    abort in between."""
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(f"""
        ALTER TABLE sandbox_egress_grants
            DROP CONSTRAINT IF EXISTS {_GRANT_CHECK},
            ADD CONSTRAINT {_GRANT_CHECK} CHECK (
                (kind = 'oauth_mcp'
                    AND connection_id IS NOT NULL AND server_name IS NULL)
                OR (kind IN ({kinds})
                    AND connection_id IS NULL AND server_name IS NOT NULL)
            ) NOT VALID
    """)
    with op.get_context().autocommit_block():
        op.execute("SET lock_timeout = 0")
        op.execute(f"ALTER TABLE sandbox_egress_grants VALIDATE CONSTRAINT {_GRANT_CHECK}")
        op.execute("RESET lock_timeout")


def upgrade() -> None:
    _replace_grant_check("'header_mcp', 'platform_mcp'")
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("""
        CREATE TABLE IF NOT EXISTS user_firecrawl_accounts (
            user_id VARCHAR(255) PRIMARY KEY
                REFERENCES users(user_id) ON DELETE CASCADE ON UPDATE CASCADE,
            api_key BYTEA NOT NULL,
            contact_email TEXT NOT NULL,
            gateway_status TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)
    op.execute("""
        CREATE TABLE IF NOT EXISTS metered_vendor_calls (
            id BIGSERIAL PRIMARY KEY,
            user_id VARCHAR(255) NOT NULL
                REFERENCES users(user_id) ON DELETE CASCADE ON UPDATE CASCADE,
            computer_id UUID,
            workspace_id UUID,
            call_id TEXT,
            thread_id UUID,
            vendor TEXT NOT NULL,
            tool TEXT NOT NULL,
            job_ref TEXT,
            vendor_credits NUMERIC NOT NULL,
            status TEXT NOT NULL DEFAULT 'settled'
                CHECK (status IN ('held', 'settled', 'estimated')),
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            collected_at TIMESTAMPTZ
        )
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS metered_vendor_calls_call
        ON metered_vendor_calls (call_id)
        WHERE call_id IS NOT NULL
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS metered_vendor_calls_job
        ON metered_vendor_calls (user_id, vendor, job_ref)
        WHERE job_ref IS NOT NULL
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS metered_vendor_calls_owed
        ON metered_vendor_calls (user_id, collected_at)
    """)
    op.execute("""
        CREATE TABLE IF NOT EXISTS relayed_sources (
            id BIGSERIAL PRIMARY KEY,
            user_id VARCHAR(255) NOT NULL
                REFERENCES users(user_id) ON DELETE CASCADE ON UPDATE CASCADE,
            call_id TEXT,
            job_ref TEXT,
            source JSONB NOT NULL,
            body TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            collected_at TIMESTAMPTZ
        )
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS relayed_sources_call ON relayed_sources (call_id)
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS relayed_sources_age ON relayed_sources (created_at)
    """)
    op.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS relayed_sources_job_page
        ON relayed_sources (user_id, job_ref, md5(source->>'identifier'))
        WHERE job_ref IS NOT NULL
    """)
    op.execute("""
        ALTER TABLE provenance_records
            ADD COLUMN IF NOT EXISTS cost_credits DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS site TEXT
    """)


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("DELETE FROM sandbox_egress_grants WHERE kind = 'platform_mcp'")
    _replace_grant_check("'header_mcp'")
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("DROP TABLE IF EXISTS relayed_sources")
    op.execute("DROP TABLE IF EXISTS metered_vendor_calls")
    op.execute("DROP TABLE IF EXISTS user_firecrawl_accounts")
    op.execute("""
        ALTER TABLE provenance_records
            DROP COLUMN IF EXISTS site,
            DROP COLUMN IF EXISTS cost_credits
    """)
