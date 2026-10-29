"""Migration 052: link an automation execution to its run, and let it wait.

``conversation_response_id`` names the run an execution produced. Under
``thread_strategy='continue'`` one thread holds a run per firing, so the
thread alone cannot say which report is this execution's. It is written as
soon as the turn is admitted, so a firing whose process went away can be
settled from what the run ledger says became of that run. ``result_excerpt``
is the plain head of that run's answer, read from the checkpoint once as the
run completes, so the list and the run feed read a column on every poll
instead of materializing history. NULL means none was recorded: an older or
failed run, or an answer with no text.

An automation whose thread already has a turn running is ``waiting`` until
that turn ends, rather than steering into it. ``skipped`` is a firing that
did not run to its end and is not a failure; ``skip_reason`` says why:
``user`` (someone skipped it, paused or removed the automation while it
waited, or stopped its run midway),
``thread_busy`` (the wait ran out, or an earlier firing was already waiting),
``interrupted`` (the server stopped while it waited).

``failure_reason`` names the kind of a ``failed`` firing that the user, not
the automation, has to act on: ``usage_limit`` (a usage limit refused the
firing or paused its run, which never counts toward auto-disable) or
``provider_auth`` (the provider rejected the user's own key, which disables
the automation at once). NULL is any other failure, and every other status.

``automations.disable_reason`` says why the server switched an automation
off: ``provider_auth`` (the provider rejected the user's own key) or
``max_failures``. The strike that disables the automation writes it and the
resume that turns it back on clears it, each in the statement that moves
``status``. NULL is an automation the server has not switched off, or one an
earlier build did. It is a column, not a key in ``metadata``, because the
client writes ``metadata`` whole.

``heartbeat_at`` is touched by the process holding a firing while it is
pending, waiting or running. A firing whose heartbeat went quiet lost that
process, and the scheduler's sweep settles it. NULL is a row written before
the column: a process on the previous build may still be running it, so the
sweep gives it a day and then closes the row without any of what a settled
firing normally sets off.

No FK, like ``conversation_thread_id`` beside it: a thread delete must not
have to visit this table on its way through ``conversation_responses``, and
the row keeps naming the run it produced after that run is gone.

Two indexes serve the per-automation reads. ``(automation_id, created_at
DESC)`` is an automation's history and its newest firing; the partial one over
unsettled statuses finds a firing still in flight, which an automation card
leads with even when a newer firing was skipped, and the earlier firing a new
one queues behind. The first replaces 001's single-column index on
``automation_id``, which its leading column serves.

Lock discipline follows 033/039. The two ACCESS EXCLUSIVE statements are
catalog work only: nullable columns and a CHECK added NOT VALID. Validation
and the index builds run afterwards in an autocommit block with no lock
timeout; VALIDATE takes SHARE UPDATE EXCLUSIVE and blocks neither readers nor
writers, and CREATE INDEX CONCURRENTLY spends most of its life in lock waits
that a cap would turn into an INVALID index.

Rollback is a redeploy of the previous build with the schema left at 052:
that build never reads the new columns, and the status CHECK only widened.
``downgrade`` is lossy and only safe once no process of this build is left,
since it drops the run links, the heartbeats and the failure and disable
reasons and turns every waiting or skipped firing into a failed one.
"""

from alembic import op

revision = "052"
down_revision = "051"
branch_labels = None
depends_on = None

_STATUS_CHECK = "automation_executions_status_check"
_HISTORY_INDEX = "idx_automation_executions_automation_created"
_UNSETTLED_INDEX = "idx_automation_executions_unsettled"
# 001's single-column index, whose lookups and delete cascade the history
# index serves through its leading column.
_AUTOMATION_INDEX = "idx_automation_executions_automation_id"


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    # ADD CONSTRAINT has no IF NOT EXISTS; the DROP makes this re-runnable
    # after an abort in the autocommit block below.
    op.execute(f"""
        ALTER TABLE automation_executions
            ADD COLUMN IF NOT EXISTS conversation_response_id UUID,
            ADD COLUMN IF NOT EXISTS result_excerpt TEXT,
            ADD COLUMN IF NOT EXISTS skip_reason TEXT,
            ADD COLUMN IF NOT EXISTS heartbeat_at TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS failure_reason TEXT,
            DROP CONSTRAINT IF EXISTS {_STATUS_CHECK},
            ADD CONSTRAINT {_STATUS_CHECK} CHECK (status IN (
                'pending', 'waiting', 'running', 'completed', 'failed',
                'timeout', 'skipped'
            )) NOT VALID
    """)
    op.execute(
        "ALTER TABLE automations ADD COLUMN IF NOT EXISTS disable_reason TEXT"
    )

    with op.get_context().autocommit_block():
        op.execute("SET lock_timeout = 0")
        # Every existing row already satisfies the narrower CHECK it replaces.
        op.execute(
            f"ALTER TABLE automation_executions VALIDATE CONSTRAINT {_STATUS_CHECK}"
        )
        # Drop before create: IF NOT EXISTS matches on the name alone and would
        # adopt an INVALID leftover of a failed CONCURRENTLY build.
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {_HISTORY_INDEX}")
        op.execute(f"""
            CREATE INDEX CONCURRENTLY {_HISTORY_INDEX}
                ON automation_executions (automation_id, created_at DESC)
        """)
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {_AUTOMATION_INDEX}")
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {_UNSETTLED_INDEX}")
        op.execute(f"""
            CREATE INDEX CONCURRENTLY {_UNSETTLED_INDEX}
                ON automation_executions (automation_id, created_at DESC)
                WHERE status IN ('pending', 'waiting', 'running')
        """)


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("SET lock_timeout = 0")
        # Back before the history index goes, so automation_id stays indexed.
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {_AUTOMATION_INDEX}")
        op.execute(f"""
            CREATE INDEX CONCURRENTLY {_AUTOMATION_INDEX}
                ON automation_executions (automation_id)
        """)
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {_UNSETTLED_INDEX}")
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {_HISTORY_INDEX}")

    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("""
        UPDATE automation_executions
        SET status = 'failed', completed_at = COALESCE(completed_at, NOW())
        WHERE status IN ('waiting', 'skipped')
    """)
    op.execute(f"""
        ALTER TABLE automation_executions
            DROP CONSTRAINT IF EXISTS {_STATUS_CHECK},
            ADD CONSTRAINT {_STATUS_CHECK} CHECK (status IN (
                'pending', 'running', 'completed', 'failed', 'timeout'
            )) NOT VALID,
            DROP COLUMN IF EXISTS failure_reason,
            DROP COLUMN IF EXISTS heartbeat_at,
            DROP COLUMN IF EXISTS skip_reason,
            DROP COLUMN IF EXISTS result_excerpt,
            DROP COLUMN IF EXISTS conversation_response_id
    """)
    op.execute("ALTER TABLE automations DROP COLUMN IF EXISTS disable_reason")

    with op.get_context().autocommit_block():
        op.execute("SET lock_timeout = 0")
        # The UPDATE above left no row outside the old vocabulary.
        op.execute(
            f"ALTER TABLE automation_executions VALIDATE CONSTRAINT {_STATUS_CHECK}"
        )
