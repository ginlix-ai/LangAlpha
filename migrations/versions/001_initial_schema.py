"""initial schema

Revision ID: 001
Revises:
Create Date: 2026-03-15

All langalpha tables: 17 application tables + LangGraph infrastructure
(checkpoint tables via library API, store table via inline DDL).

Application tables:
  users, workspaces, workspace_files, user_preferences, watchlists,
  watchlist_items, user_portfolios, user_api_keys, user_oauth_tokens,
  conversation_threads, conversation_queries, conversation_responses,
  conversation_usages, conversation_feedback, automations,
  automation_executions, market_insights

LangGraph tables:
  checkpoint_migrations, checkpoints, checkpoint_blobs, checkpoint_writes
  store, store_migrations
"""

import asyncio
from typing import Sequence, Union

from alembic import op

revision: str = "001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _get_psycopg_url() -> str:
    """Get the raw psycopg connection URL from the Alembic bind engine."""
    url = op.get_bind().engine.url.render_as_string(hide_password=False)
    return url.replace("postgresql+psycopg://", "postgresql://")


async def _setup_checkpoint_tables(db_url: str) -> None:
    """Create LangGraph checkpoint tables via the library's own setup() API.

    Uses a separate connection with autocommit — the library manages its own
    DDL and internal migration tracking (checkpoint_migrations table).
    """
    from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
    from psycopg_pool import AsyncConnectionPool
    from psycopg.rows import dict_row

    async with AsyncConnectionPool(
        conninfo=db_url,
        min_size=1,
        max_size=1,
        kwargs={"autocommit": True, "prepare_threshold": 0, "row_factory": dict_row},
    ) as pool:
        await pool.wait()
        checkpointer = AsyncPostgresSaver(pool)
        await checkpointer.setup()


def upgrade() -> None:
    # -- Extensions --
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")

    # -- Trigger function --
    op.execute("""
        CREATE OR REPLACE FUNCTION update_updated_at_column()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = NOW();
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql
    """)

    # -- 1. users --
    op.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id VARCHAR(255) PRIMARY KEY,
            email VARCHAR(255),
            name VARCHAR(255),
            avatar_url TEXT,
            timezone VARCHAR(100),
            locale VARCHAR(20),
            onboarding_completed BOOLEAN NOT NULL DEFAULT FALSE,
            membership_id INT NOT NULL DEFAULT 1,
            byok_enabled BOOLEAN NOT NULL DEFAULT FALSE,
            auth_provider VARCHAR(50),
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            last_login_at TIMESTAMPTZ
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS idx_users_email ON users(email)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_users_created_at ON users(created_at DESC)")

    # -- 2. workspaces --
    op.execute("""
        CREATE TABLE IF NOT EXISTS workspaces (
            workspace_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id VARCHAR(255) NOT NULL
                REFERENCES users(user_id) ON DELETE CASCADE,
            name VARCHAR(255) NOT NULL,
            description TEXT,
            sandbox_id VARCHAR(255),
            status VARCHAR(50) NOT NULL DEFAULT 'creating'
                CHECK (status IN (
                    'creating','running','stopping',
                    'stopped','error','deleted','flash'
                )),
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            last_activity_at TIMESTAMPTZ,
            stopped_at TIMESTAMPTZ,
            config JSONB DEFAULT '{}'::jsonb,
            is_pinned BOOLEAN NOT NULL DEFAULT FALSE,
            sort_order INTEGER NOT NULL DEFAULT 0
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS idx_workspaces_user_id ON workspaces(user_id)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_workspaces_user_status ON workspaces(user_id, status)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_workspaces_updated_at ON workspaces(updated_at DESC)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_workspaces_user_pin_sort ON workspaces(user_id, is_pinned DESC, sort_order ASC)")

    # -- 3. workspace_files --
    op.execute("""
        CREATE TABLE IF NOT EXISTS workspace_files (
            workspace_file_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            workspace_id UUID NOT NULL
                REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
            file_path VARCHAR(1024) NOT NULL,
            file_name VARCHAR(255) NOT NULL,
            file_size BIGINT NOT NULL DEFAULT 0,
            content_hash VARCHAR(64),
            content_text TEXT,
            content_binary BYTEA,
            mime_type VARCHAR(255),
            is_binary BOOLEAN NOT NULL DEFAULT FALSE,
            permissions VARCHAR(10),
            sandbox_modified_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT unique_file_per_workspace
                UNIQUE (workspace_id, file_path)
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS idx_workspace_files_workspace_id ON workspace_files(workspace_id)")

    # -- 4. user_preferences --
    op.execute("""
        CREATE TABLE IF NOT EXISTS user_preferences (
            user_preference_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id VARCHAR(255) UNIQUE NOT NULL
                REFERENCES users(user_id) ON DELETE CASCADE,
            risk_preference JSONB DEFAULT '{}'::jsonb,
            investment_preference JSONB DEFAULT '{}'::jsonb,
            agent_preference JSONB DEFAULT '{}'::jsonb,
            other_preference JSONB DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)

    # -- 5. watchlists --
    op.execute("""
        CREATE TABLE IF NOT EXISTS watchlists (
            watchlist_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id VARCHAR(255) NOT NULL
                REFERENCES users(user_id) ON DELETE CASCADE,
            name VARCHAR(100) NOT NULL,
            description TEXT,
            is_default BOOLEAN NOT NULL DEFAULT FALSE,
            display_order INTEGER DEFAULT 0,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT unique_user_watchlist_name UNIQUE (user_id, name)
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS idx_watchlists_user_id ON watchlists(user_id)")

    # -- 6. watchlist_items --
    op.execute("""
        CREATE TABLE IF NOT EXISTS watchlist_items (
            watchlist_item_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            watchlist_id UUID NOT NULL
                REFERENCES watchlists(watchlist_id) ON DELETE CASCADE,
            user_id VARCHAR(255) NOT NULL
                REFERENCES users(user_id) ON DELETE CASCADE,
            symbol VARCHAR(50) NOT NULL,
            instrument_type VARCHAR(30) NOT NULL,
            exchange VARCHAR(50),
            name VARCHAR(255),
            notes TEXT,
            alert_settings JSONB DEFAULT '{}'::jsonb,
            metadata JSONB DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT unique_watchlist_item
                UNIQUE (watchlist_id, symbol, instrument_type)
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS idx_watchlist_items_watchlist_id ON watchlist_items(watchlist_id)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_watchlist_items_user_id ON watchlist_items(user_id)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_watchlist_items_symbol ON watchlist_items(symbol)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_watchlist_items_user_symbol ON watchlist_items(user_id, symbol, instrument_type)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_watchlist_items_created_at ON watchlist_items(created_at DESC)")

    # -- 7. user_portfolios --
    op.execute("""
        CREATE TABLE IF NOT EXISTS user_portfolios (
            user_portfolio_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id VARCHAR(255) NOT NULL
                REFERENCES users(user_id) ON DELETE CASCADE,
            symbol VARCHAR(50) NOT NULL,
            instrument_type VARCHAR(30) NOT NULL,
            exchange VARCHAR(50),
            name VARCHAR(255),
            quantity DECIMAL(18, 8) NOT NULL,
            average_cost DECIMAL(18, 4),
            currency VARCHAR(10) DEFAULT 'USD',
            account_name VARCHAR(100),
            notes TEXT,
            metadata JSONB DEFAULT '{}'::jsonb,
            first_purchased_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT unique_user_holding
                UNIQUE (user_id, symbol, instrument_type, account_name)
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS idx_portfolios_user_id ON user_portfolios(user_id)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_portfolios_symbol ON user_portfolios(symbol)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_portfolios_instrument_type ON user_portfolios(instrument_type)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_portfolios_user_instrument ON user_portfolios(user_id, symbol, instrument_type)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_portfolios_account ON user_portfolios(account_name)")

    # -- 8. user_api_keys --
    op.execute("""
        CREATE TABLE IF NOT EXISTS user_api_keys (
            user_id VARCHAR(255) NOT NULL
                REFERENCES users(user_id)
                ON DELETE CASCADE ON UPDATE CASCADE,
            provider VARCHAR(50) NOT NULL,
            api_key BYTEA NOT NULL,
            base_url TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (user_id, provider)
        )
    """)

    # -- 9. user_oauth_tokens --
    op.execute("""
        CREATE TABLE IF NOT EXISTS user_oauth_tokens (
            user_id TEXT NOT NULL,
            provider TEXT NOT NULL,
            access_token BYTEA NOT NULL,
            refresh_token BYTEA NOT NULL,
            account_id TEXT NOT NULL,
            email TEXT,
            plan_type TEXT,
            expires_at TIMESTAMPTZ NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (user_id, provider)
        )
    """)

    # -- 10. conversation_threads --
    op.execute("""
        CREATE TABLE IF NOT EXISTS conversation_threads (
            conversation_thread_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            workspace_id UUID NOT NULL
                REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
            msg_type VARCHAR(50)
                CHECK (msg_type IN ('flash','ptc','interrupted','task')),
            current_status VARCHAR(50) NOT NULL
                CHECK (current_status IN (
                    'in_progress','interrupted','completed','error','cancelled'
                )),
            thread_index INTEGER NOT NULL,
            title VARCHAR(255),
            external_id VARCHAR(255),
            platform VARCHAR(50),
            share_token VARCHAR(32) UNIQUE,
            is_shared BOOLEAN NOT NULL DEFAULT FALSE,
            share_permissions JSONB NOT NULL DEFAULT '{}',
            shared_at TIMESTAMPTZ,
            latest_checkpoint_id TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            CONSTRAINT unique_thread_index_per_workspace
                UNIQUE (workspace_id, thread_index)
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS idx_threads_created_at ON conversation_threads(created_at DESC)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_threads_current_status ON conversation_threads(current_status)")
    op.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_threads_share_token
        ON conversation_threads(share_token) WHERE share_token IS NOT NULL
    """)
    op.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_conversation_threads_external
        ON conversation_threads (platform, external_id)
        WHERE external_id IS NOT NULL
    """)

    # -- 11. conversation_queries --
    op.execute("""
        CREATE TABLE IF NOT EXISTS conversation_queries (
            conversation_query_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            conversation_thread_id UUID NOT NULL
                REFERENCES conversation_threads(conversation_thread_id)
                ON DELETE CASCADE,
            turn_index INTEGER NOT NULL,
            content TEXT,
            type VARCHAR(50) NOT NULL
                CHECK (type IN (
                    'initial','follow_up','resume_feedback','regenerate'
                )),
            feedback_action TEXT,
            metadata JSONB DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL,
            CONSTRAINT unique_turn_index_per_thread_query
                UNIQUE (conversation_thread_id, turn_index)
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS idx_queries_thread_id ON conversation_queries(conversation_thread_id)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_queries_created_at ON conversation_queries(created_at DESC)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_queries_type ON conversation_queries(type)")

    # -- 12. conversation_responses --
    op.execute("""
        CREATE TABLE IF NOT EXISTS conversation_responses (
            conversation_response_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            conversation_thread_id UUID NOT NULL
                REFERENCES conversation_threads(conversation_thread_id)
                ON DELETE CASCADE,
            turn_index INTEGER NOT NULL,
            status VARCHAR(50) NOT NULL
                CHECK (status IN (
                    'in_progress','interrupted','completed','error','cancelled'
                )),
            interrupt_reason VARCHAR(100),
            metadata JSONB DEFAULT '{}'::jsonb,
            warnings TEXT[],
            errors TEXT[],
            execution_time FLOAT,
            created_at TIMESTAMPTZ NOT NULL,
            sse_events JSONB,
            CONSTRAINT unique_turn_index_per_thread_response
                UNIQUE (conversation_thread_id, turn_index)
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS idx_responses_thread_id ON conversation_responses(conversation_thread_id)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_responses_status ON conversation_responses(status)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_responses_created_at ON conversation_responses(created_at DESC)")

    # -- 13. conversation_usages --
    op.execute("""
        CREATE TABLE IF NOT EXISTS conversation_usages (
            conversation_usage_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            conversation_response_id UUID NOT NULL,
            user_id VARCHAR(255) NOT NULL,
            conversation_thread_id UUID NOT NULL,
            workspace_id UUID NOT NULL,
            msg_type VARCHAR(50) NOT NULL DEFAULT 'ptc'
                CHECK (msg_type IN ('flash','ptc','interrupted','task')),
            status VARCHAR(50) NOT NULL
                CHECK (status IN (
                    'in_progress','interrupted','completed','error','cancelled'
                )),
            token_usage JSONB,
            infrastructure_usage JSONB,
            token_credits DECIMAL(10, 6) NOT NULL DEFAULT 0,
            infrastructure_credits DECIMAL(10, 6) NOT NULL DEFAULT 0,
            total_credits DECIMAL(10, 6) NOT NULL DEFAULT 0,
            is_byok BOOLEAN NOT NULL DEFAULT FALSE,
            credit_exempt BOOLEAN NOT NULL DEFAULT FALSE,
            credit_exempt_reason VARCHAR(100),
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS idx_usages_user_timestamp ON conversation_usages(user_id, created_at DESC)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_usages_thread_id ON conversation_usages(conversation_thread_id)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_usages_workspace_id ON conversation_usages(workspace_id)")

    # -- 14. conversation_feedback --
    op.execute("""
        CREATE TABLE IF NOT EXISTS conversation_feedback (
            conversation_feedback_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            conversation_response_id UUID NOT NULL
                REFERENCES conversation_responses(conversation_response_id)
                ON DELETE CASCADE,
            user_id VARCHAR(255) NOT NULL,
            rating VARCHAR(20) NOT NULL
                CHECK (rating IN ('thumbs_up', 'thumbs_down')),
            issue_categories TEXT[],
            comment TEXT,
            consent_human_review BOOLEAN NOT NULL DEFAULT FALSE,
            review_status VARCHAR(50)
                CHECK (review_status IN ('pending', 'confirmed', 'rejected')),
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT unique_feedback_per_response_user
                UNIQUE (conversation_response_id, user_id)
        )
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_feedback_review_status
        ON conversation_feedback(review_status)
        WHERE review_status IS NOT NULL
    """)

    # -- 15. automations --
    op.execute("""
        CREATE TABLE IF NOT EXISTS automations (
            automation_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id VARCHAR(255) NOT NULL
                REFERENCES users(user_id) ON DELETE CASCADE,
            name VARCHAR(255) NOT NULL,
            description TEXT,
            trigger_type VARCHAR(20) NOT NULL
                CHECK (trigger_type IN ('cron', 'once')),
            cron_expression VARCHAR(100),
            timezone VARCHAR(100) NOT NULL DEFAULT 'UTC',
            trigger_config JSONB DEFAULT '{}'::jsonb,
            next_run_at TIMESTAMPTZ,
            last_run_at TIMESTAMPTZ,
            agent_mode VARCHAR(20) NOT NULL DEFAULT 'flash'
                CHECK (agent_mode IN ('ptc', 'flash')),
            instruction TEXT NOT NULL,
            workspace_id UUID
                REFERENCES workspaces(workspace_id) ON DELETE SET NULL,
            llm_model VARCHAR(100),
            additional_context JSONB,
            thread_strategy VARCHAR(20) NOT NULL DEFAULT 'new'
                CHECK (thread_strategy IN ('new', 'continue')),
            conversation_thread_id UUID,
            status VARCHAR(20) NOT NULL DEFAULT 'active'
                CHECK (status IN ('active', 'paused', 'completed', 'disabled')),
            max_failures INT NOT NULL DEFAULT 3,
            failure_count INT NOT NULL DEFAULT 0,
            delivery_config JSONB DEFAULT '{}'::jsonb,
            metadata JSONB DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_automations_next_run
        ON automations(next_run_at ASC)
        WHERE status = 'active' AND next_run_at IS NOT NULL
    """)
    op.execute("CREATE INDEX IF NOT EXISTS idx_automations_user_id ON automations(user_id)")

    # -- 16. automation_executions --
    op.execute("""
        CREATE TABLE IF NOT EXISTS automation_executions (
            automation_execution_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            automation_id UUID NOT NULL
                REFERENCES automations(automation_id) ON DELETE CASCADE,
            status VARCHAR(20) NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending', 'running', 'completed', 'failed', 'timeout')),
            conversation_thread_id UUID,
            scheduled_at TIMESTAMPTZ NOT NULL,
            started_at TIMESTAMPTZ,
            completed_at TIMESTAMPTZ,
            error_message TEXT,
            server_id VARCHAR(100),
            delivery_result JSONB,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS idx_automation_executions_automation_id ON automation_executions(automation_id)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_automation_executions_status ON automation_executions(status)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_automation_executions_created_at ON automation_executions(created_at DESC)")

    # -- 17. market_insights --
    op.execute("""
        CREATE TABLE IF NOT EXISTS market_insights (
            market_insight_id UUID PRIMARY KEY,
            user_id UUID,
            type VARCHAR(30) NOT NULL DEFAULT 'daily_brief',
            status VARCHAR(20) NOT NULL DEFAULT 'pending',
            headline TEXT,
            summary TEXT,
            content JSONB,
            topics JSONB,
            sources JSONB,
            model VARCHAR(10),
            error_message TEXT,
            generation_time_ms INTEGER,
            metadata JSONB,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            completed_at TIMESTAMPTZ
        )
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_market_insights_latest
        ON market_insights (type, created_at DESC)
        WHERE status = 'completed' AND user_id IS NULL
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_market_insights_user
        ON market_insights (user_id, type, created_at DESC)
        WHERE status = 'completed' AND user_id IS NOT NULL
    """)

    # -- Attach updated_at triggers --
    _tables_with_trigger = [
        "users", "workspaces", "workspace_files", "user_preferences",
        "watchlists", "watchlist_items", "user_portfolios",
        "conversation_threads", "automations", "conversation_feedback",
    ]
    for table in _tables_with_trigger:
        trigger = f"trg_{table}_updated_at"
        op.execute(f"DROP TRIGGER IF EXISTS {trigger} ON {table}")
        op.execute(f"""
            CREATE TRIGGER {trigger}
                BEFORE UPDATE ON {table}
                FOR EACH ROW
                EXECUTE FUNCTION update_updated_at_column()
        """)

    # =====================================================================
    # LangGraph infrastructure tables
    # =====================================================================

    # -- Checkpoint tables (via library API, separate autocommit connection) --
    db_url = _get_psycopg_url()
    asyncio.run(_setup_checkpoint_tables(db_url))

    # -- Store table (inline DDL) --
    op.execute("""
        CREATE TABLE IF NOT EXISTS store (
            prefix text NOT NULL,
            key text NOT NULL,
            value jsonb NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            expires_at TIMESTAMP WITH TIME ZONE,
            ttl_minutes INT,
            PRIMARY KEY (prefix, key)
        )
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS store_prefix_idx
        ON store USING btree (prefix text_pattern_ops)
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_store_expires_at ON store (expires_at)
        WHERE expires_at IS NOT NULL
    """)

    # Track migrations so LangGraph's store.setup() knows they're applied
    op.execute("""
        CREATE TABLE IF NOT EXISTS store_migrations (
            v INTEGER PRIMARY KEY
        )
    """)
    op.execute(
        "INSERT INTO store_migrations (v) VALUES (0),(1),(2),(3) ON CONFLICT DO NOTHING"
    )


def downgrade() -> None:
    # LangGraph tables
    for table in [
        "store_migrations", "store",
        "checkpoint_writes", "checkpoint_blobs", "checkpoints",
        "checkpoint_migrations",
    ]:
        op.execute(f"DROP TABLE IF EXISTS {table} CASCADE")

    # Application tables (reverse FK order)
    for table in [
        "market_insights",
        "automation_executions",
        "automations",
        "conversation_feedback",
        "conversation_usages",
        "conversation_responses",
        "conversation_queries",
        "conversation_threads",
        "user_oauth_tokens",
        "user_api_keys",
        "user_portfolios",
        "watchlist_items",
        "watchlists",
        "user_preferences",
        "workspace_files",
        "workspaces",
        "users",
    ]:
        op.execute(f"DROP TABLE IF EXISTS {table} CASCADE")

    op.execute("DROP FUNCTION IF EXISTS update_updated_at_column()")
