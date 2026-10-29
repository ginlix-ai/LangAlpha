"""Migration 090: command_runs, the ledger of background commands.

A Bash or ExecuteCode call that outlasts the tool's wait is handed off: the
job keeps running on the computer, and this row is its record from then on.
A command that ends within the wait leaves no row; one launched with
``run_in_background`` gets its row at launch. Any worker may watch, stop or
report a command, so the row, not the process that launched it, is the truth.

``job_ref`` is what finds the job on the computer from any worker (its state
directory, log and trace paths); ``log_path`` is the workspace-relative log
the agent reads. ``status`` moves once, from ``running`` to ``completed``,
``failed`` or ``stopped``, by one guarded UPDATE that enqueues its notice
in ``hook_outbox`` in the same transaction: a ``command_report_back`` due
now, or a ``command_notice`` for the thread's next turn.
``failure_reason`` says why a command failed: ``exit`` (a non-zero exit),
``lost`` (its computer stopped under it), ``vanished`` (it ended with no exit
recorded while its computer ran, as a process killed from outside does) or
``after_turn_limit`` (stopped an hour after its turn ended with nothing
checking on it). ``output_tail`` keeps
the last 4 KB of the log, so a listing can say how a command ended without
waking a stopped computer, and ``mcp_trace`` the MCP calls it made.

``next_check_at`` is the watcher's lease: a claim moves it forward, so the
workers polling the same rows never read one twice in a round.
``result_delivered_at`` is stamped when the agent saw the end some other
way, which drops the notice.

No foreign key to ``workspaces`` or ``computers``: a command outlives
neither, and the watcher must still find the row to report one lost.

Lock discipline follows 088: the foreign key holds SHARE ROW EXCLUSIVE on
``conversation_threads`` until the transaction commits, and every other
statement is on the new, empty table, so the transaction is short and the
lock waits 5 s at most. ``hook_outbox`` needs nothing: it has no hook-type
CHECK. The previous build never reads or writes this table.

Rollback: ``alembic downgrade 089`` while this build is stopped. It deletes
the ``command_report_back`` and ``command_notice`` jobs, which a build
without their executor would dead-letter, and drops the table with its
history. A report turn's query row
keeps its ``report_source`` of kind ``command_report``, which the previous
build reads as no source.
"""

from alembic import op

revision = "090"
down_revision = "089"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("""
        CREATE TABLE IF NOT EXISTS command_runs (
            command_id VARCHAR(32) PRIMARY KEY
                CHECK (command_id ~ '^bg-[0-9a-f]{12}$'),
            conversation_thread_id UUID NOT NULL
                REFERENCES conversation_threads(conversation_thread_id)
                ON DELETE CASCADE,
            user_id VARCHAR(255) NOT NULL,
            workspace_id UUID NOT NULL,
            computer_id UUID NOT NULL,
            -- The turn that launched it. No foreign key: an edit that drops
            -- that turn leaves the command running and owed to the thread.
            run_id UUID,
            tool_call_id VARCHAR(255),
            tool VARCHAR(16) NOT NULL CHECK (tool IN ('Bash', 'ExecuteCode')),
            description TEXT NOT NULL DEFAULT '',
            command_preview TEXT NOT NULL DEFAULT '',
            log_path TEXT NOT NULL,
            job_ref JSONB NOT NULL,
            status VARCHAR(12) NOT NULL DEFAULT 'running'
                CHECK (status IN ('running', 'completed', 'failed', 'stopped')),
            failure_reason VARCHAR(20)
                CHECK (failure_reason IN ('exit', 'lost', 'vanished', 'after_turn_limit')),
            exit_code INTEGER,
            stopped_by VARCHAR(32),
            output_tail TEXT,
            mcp_trace JSONB,
            started_at TIMESTAMPTZ NOT NULL,
            handed_off_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            ended_at TIMESTAMPTZ,
            next_check_at TIMESTAMPTZ,
            result_delivered_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT command_runs_failure_reason
                CHECK ((status = 'failed') = (failure_reason IS NOT NULL)),
            CONSTRAINT command_runs_ended
                CHECK ((status = 'running') = (ended_at IS NULL)),
            CONSTRAINT command_runs_watched
                CHECK (status <> 'running' OR next_check_at IS NOT NULL)
        )
    """)
    # What the watcher claims on every worker: running rows, soonest due first.
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_command_runs_due
        ON command_runs (next_check_at) WHERE status = 'running'
    """)
    # A thread's commands, newest first: the thread listing, ThreadStatus,
    # and the stop on archive or delete.
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_command_runs_thread_started
        ON command_runs (conversation_thread_id, started_at DESC)
    """)
    # The running commands in a workspace: its delete stops them.
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_command_runs_running_workspace
        ON command_runs (workspace_id) WHERE status = 'running'
    """)


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("SET lock_timeout = '5s'")
        op.execute(
            "DELETE FROM hook_outbox"
            " WHERE hook_type IN ('command_report_back', 'command_notice')"
        )
        op.execute("RESET lock_timeout")

    # Dropping the foreign key takes ACCESS EXCLUSIVE on conversation_threads,
    # so the table goes in a transaction that holds no other hot lock.
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("DROP TABLE IF EXISTS command_runs")
