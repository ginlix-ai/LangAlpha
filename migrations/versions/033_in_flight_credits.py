"""In-flight credit ledger columns on runs.

A run's platform-credit spend is only visible today after finalize writes its
usage row; between admission and settle the spend is dark. These columns let a
live run heartbeat its own cumulative spend onto its own ledger row, and let a
reader aggregate "unsettled in-flight spend per user" in one snapshot:

- ``in_flight_credits``: monotone cumulative platform credits for this run,
  written as an absolute value by the run's own heartbeat. Never incremented.
- ``user_id``: denormalized so the per-user aggregate needs no join at read
  time. Nullable on purpose: rows inserted by pre-gate code omit it, and the
  aggregate never sees those rows (they stay at zero in-flight credits), so a
  backfill would rewrite a large hot table for nothing. Contraction to
  NOT NULL, if ever, follows the 017/018 expand/contract precedent.
- ``usage_settled_at``: stamped in the same transaction as the run's usage
  insert (or a recovery path's degraded settle). A row counts toward in-flight
  spend until this is set, regardless of run status: terminal and
  billing-settled are separate states, because a subagent run turns terminal
  in one CAS while its usage is collected later.

Those four semantics are a contract rather than incidental. These columns are
read outside this service to split spend that is already billed from spend that
is not, so changing the monotone write, the settle transaction, the nullability
of ``user_id`` or the status-independence of the in-flight window all change
what that split means.

The partial indexes match the only read shape: SUM per user over unsettled
rows that have actually heartbeated. Filtering on ``in_flight_credits > 0``
keeps every pre-gate row and every old-worker row (which never heartbeats)
out of the index and out of the aggregate, which is what makes the no-backfill
rollout safe under a rolling deploy.

``user_id IS NOT NULL`` is part of the predicate for a different reason: the
aggregate reads per user, so an owner-less row with real spend on it is
invisible to every reader while still costing index space. Excluding it makes
that set cheap to ask about directly (``in_flight_credits > 0 AND user_id IS
NULL``) instead of leaving it to hide inside a scan nobody runs.

One further index covers the complement: settled rows that carried spend,
keyed by ``parent_run_id``. Reconciling a turn family against what has already
been billed asks the opposite question of the aggregate above -- which of this
parent's children are done -- and neither index above can answer it, since both
are keyed on ``user_id`` and their predicate excludes exactly the rows it
needs. ``user_id`` rides in the INCLUDE so a reader can check a row's owner
without leaving the index.

Revision ID: 033
Revises: 032

NOTE (numbering): merge order is the rule — this is the next id free on main's
head at the time it landed, not the id it was written under. A branch that
renumbers behind it has to repoint its ``down_revision`` as well, which is the
line that actually matters; the filename prefix is cosmetic. Alembic refuses to
run on two heads rather than picking one, so skipping that step cannot land
quietly.

That safety net does NOT cover a database already stamped with an id this file
used earlier: the version table holds one string, so only one head is visible
and the upgrade walks straight past whatever else claimed that id. Any database
stamped 031 or 032 by an earlier form of this file must be checked against
that id's own DDL by hand before it is upgraded — the stamp is not evidence
that this file's DDL ran.
"""

from alembic import op

revision = "033"
down_revision = "032"
branch_labels = None
depends_on = None

# One name per index, so the drop and the create below can never disagree.
_RESPONSES_INDEX = "ix_responses_in_flight_by_user"
_SUBAGENT_INDEX = "ix_subagent_runs_in_flight_by_user"
_SUBAGENT_SETTLED_INDEX = "ix_subagent_runs_settled_by_parent"


def upgrade() -> None:
    # Bounds how long we WAIT for the lock, not how long we hold it — which is
    # why the column adds below are kept to metadata-only work. A statement
    # that has to rewrite or scan the heap would hold ACCESS EXCLUSIVE for the
    # whole scan no matter what this is set to.
    # NUMERIC unconstrained, and no inline CHECK: a column-level CHECK on ADD
    # COLUMN defeats the fast-default path and forces a validating scan of the
    # whole heap under ACCESS EXCLUSIVE. The constraint is added separately
    # below as NOT VALID and validated outside the lock.
    #
    # Each table gets its own autocommit block, which is what actually makes
    # good on "one table's lock is never held across the other's work" —
    # sharing the migration's transaction holds ACCESS EXCLUSIVE on
    # conversation_responses until COMMIT, and so right through the second
    # ALTER's wait for subagent_runs, blocking every read and write to the
    # hottest table in the schema for up to the whole lock_timeout.
    for table in ("conversation_responses", "subagent_runs"):
        with op.get_context().autocommit_block():
            op.execute("SET lock_timeout = '5s'")
            op.execute(
                f"""
                ALTER TABLE {table}
                    ADD COLUMN IF NOT EXISTS in_flight_credits NUMERIC NOT NULL DEFAULT 0,
                    ADD COLUMN IF NOT EXISTS user_id VARCHAR(255),
                    ADD COLUMN IF NOT EXISTS usage_settled_at TIMESTAMPTZ
                """
            )

    # Everything below runs outside the migration's transaction: CONCURRENTLY
    # requires it, and VALIDATE CONSTRAINT wants its own so its SHARE UPDATE
    # EXCLUSIVE is released promptly. Safe here because every statement above
    # is additive and every reader tolerates a missing index or an unvalidated
    # constraint.
    with op.get_context().autocommit_block():
        # The cap stays on for the constraint DDL. NOT VALID skips the heap
        # SCAN, not the LOCK: ADD CONSTRAINT ... CHECK takes ACCESS EXCLUSIVE
        # either way, and so does the DROP above it — the reduced-lock ALTER
        # forms are ADD FOREIGN KEY and VALIDATE, not this one. Skipping the
        # scan makes the hold brief but does nothing about the WAIT, and an
        # ACCESS EXCLUSIVE queued behind one long transaction parks every
        # reader and writer of a hot run table behind it for as long as that
        # transaction lives. Both tables are done here, before either scan
        # below, so the exclusive work is over early rather than interleaved
        # with a validation that can run for minutes.
        op.execute("SET lock_timeout = '5s'")
        for table in ("conversation_responses", "subagent_runs"):
            constraint = f"ck_{table}_in_flight_credits_nonneg"
            op.execute(
                f"""
                ALTER TABLE {table}
                    DROP CONSTRAINT IF EXISTS {constraint}
                """
            )
            op.execute(
                f"""
                ALTER TABLE {table}
                    ADD CONSTRAINT {constraint}
                    CHECK (in_flight_credits >= 0) NOT VALID
                """
            )

        # Only now drop the cap, for the two kinds of statement that need it
        # dropped. CREATE INDEX CONCURRENTLY spends most of its life in two
        # waits for transactions that can still see the table, and those are
        # lock waits, so lock_timeout aborts them: on a table taking continuous
        # writes a 5s cap fails the build almost every time and leaves an
        # INVALID index behind. VALIDATE takes SHARE UPDATE EXCLUSIVE and scans
        # without blocking readers or writers, so it has nothing worth bounding
        # either. Every existing row holds the column default, so validation
        # cannot fail.
        op.execute("SET lock_timeout = 0")
        for table in ("conversation_responses", "subagent_runs"):
            op.execute(
                f"ALTER TABLE {table} "
                f"VALIDATE CONSTRAINT ck_{table}_in_flight_credits_nonneg"
            )

        # Drop before create, always. IF NOT EXISTS matches on NAME alone, so
        # on its own it adopts whatever index already wears the name: a failed
        # CONCURRENTLY build leaves an INVALID one that Postgres keeps
        # maintaining on every write, and an earlier revision of this file
        # leaves a narrower one that cannot serve the reader the INCLUDE was
        # widened for. Both skip silently and neither shows up in
        # alembic_version. Dropping first makes the definition below the only
        # one that can survive this migration.
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {_RESPONSES_INDEX}")
        op.execute(
            f"""
            CREATE INDEX CONCURRENTLY {_RESPONSES_INDEX}
                ON conversation_responses (user_id)
                INCLUDE (in_flight_credits)
                WHERE usage_settled_at IS NULL AND in_flight_credits > 0
                  AND user_id IS NOT NULL
            """
        )
        # parent_run_id rides along so a reader can resolve a subagent row to
        # the turn that leased for it without leaving the index.
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {_SUBAGENT_INDEX}")
        op.execute(
            f"""
            CREATE INDEX CONCURRENTLY {_SUBAGENT_INDEX}
                ON subagent_runs (user_id)
                INCLUDE (in_flight_credits, parent_run_id)
                WHERE usage_settled_at IS NULL AND in_flight_credits > 0
                  AND user_id IS NOT NULL
            """
        )
        # The settled complement of the index above, keyed by the parent rather
        # than the owner. No predicate here overlaps the two above, so this is
        # an additional index rather than a wider one: a reader reconciling a
        # family against already-billed spend needs precisely the rows they
        # exclude, and needs them by parent.
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {_SUBAGENT_SETTLED_INDEX}")
        op.execute(
            f"""
            CREATE INDEX CONCURRENTLY {_SUBAGENT_SETTLED_INDEX}
                ON subagent_runs (parent_run_id)
                INCLUDE (in_flight_credits, user_id)
                WHERE usage_settled_at IS NOT NULL AND in_flight_credits > 0
            """
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("SET lock_timeout = 0")  # same reason as upgrade()
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {_SUBAGENT_SETTLED_INDEX}")
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {_SUBAGENT_INDEX}")
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {_RESPONSES_INDEX}")

    # Own block per table, for the same reason as upgrade(): a DROP COLUMN
    # takes ACCESS EXCLUSIVE too, and one shared transaction would hold the
    # first table's lock across the second's wait.
    for table in ("subagent_runs", "conversation_responses"):
        with op.get_context().autocommit_block():
            op.execute("SET lock_timeout = '5s'")
            op.execute(
                f"""
                ALTER TABLE {table}
                    DROP CONSTRAINT IF EXISTS ck_{table}_in_flight_credits_nonneg
                """
            )
            op.execute(
                f"""
                ALTER TABLE {table}
                    DROP COLUMN IF EXISTS usage_settled_at,
                    DROP COLUMN IF EXISTS user_id,
                    DROP COLUMN IF EXISTS in_flight_credits
                """
            )
