"""Add provenance_result_bodies: a global content-addressed store of raw result bodies.

The 500-char ``result_snippet`` renders the Sources panel but is too truncated
to verify a response against — the figures a verifier must check (revenue, EPS)
live in the full result body (e.g. a 126 KB 10-Q), not its metadata head. This
table stores the raw per-access result body keyed by ``result_sha256`` so a
post-hoc verifier can read the exact bytes the agent reasoned over. The store is
global: a static 10-Q fetched by many users across months is stored once. Small
bodies live inline in ``body_inline``; bodies over the cap keep a head inline and
spill the full body to object storage (``object_key``). ``byte_len`` is the TRUE
full length, so ``truncated = byte_len > length(body_inline)``.

No FK from ``provenance_records`` — bodies are shared across rows and GC'd
independently; ``result_sha256`` is the logical link. Bodies are immutable, so
writes are ``ON CONFLICT DO NOTHING`` (a dedup hit is a pure no-op). The GC
mark-sweep deletes rows that no provenance record references and that are older
than a grace window keyed on ``created_at`` (a body written mid-turn is younger
than the grace, so it is never reaped before its provenance row commits — no
per-access ``last_seen_at`` bump is needed). ``idx_provenance_records_sha`` serves
the NOT EXISTS orphan check + verifier joins; ``idx_provenance_result_bodies_created_at``
lets the sweep seek old rows without scanning the whole table.

Revision ID: 015
Revises: 014
"""

from alembic import op


revision = "015"
down_revision = "014"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS provenance_result_bodies (
            result_sha256 TEXT PRIMARY KEY,
            body_inline TEXT NOT NULL,
            object_key TEXT,
            byte_len BIGINT NOT NULL,
            content_type TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)

    # Serves the GC mark-sweep NOT EXISTS against provenance_records + verifier joins.
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_provenance_records_sha
        ON provenance_records(result_sha256)
    """)

    # Lets the GC sweep seek bodies past the created_at grace window cheaply.
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_provenance_result_bodies_created_at
        ON provenance_result_bodies(created_at)
    """)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_provenance_result_bodies_created_at")
    op.execute("DROP INDEX IF EXISTS idx_provenance_records_sha")
    op.execute("DROP TABLE IF EXISTS provenance_result_bodies")
