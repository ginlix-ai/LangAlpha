#!/usr/bin/env python3
"""
Unified database setup script for fresh installs.

Creates all application tables in FK dependency order, with indexes,
triggers, and verification.

Replaces the old setup_conversation_tables.py and setup_user_tables.py scripts.

Note: Membership/plan tables (memberships, redemption_codes, redemption_histories)
have been migrated to ginlix-auth's auth_plans table and are no longer created here.

Tables created (in order):
 1. users                - Central user profiles
 2. workspaces           - Daytona sandbox workspaces (FK -> users)
 3. user_preferences     - Categorized user prefs (FK -> users)
 4. watchlists           - Named watchlist containers (FK -> users)
 5. watchlist_items      - Instruments in watchlists (FK -> watchlists)
 6. user_portfolios      - Current holdings (FK -> users)
 7. user_api_keys        - Encrypted BYOK keys (FK -> users)
 8. conversation_threads - Chat threads (FK -> workspaces)
 9. conversation_queries - User messages (FK -> conversation_threads)
10. conversation_responses - Agent responses (FK -> conversation_threads)
11. conversation_usages  - Token/credit tracking (FK -> conversation_responses, threads, workspaces)

Usage:
    uv run python scripts/setup_tables.py
"""

import sys
import os
import asyncio
from pathlib import Path
from dotenv import load_dotenv

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Load environment variables from .env file
load_dotenv(project_root / ".env")

from psycopg_pool import AsyncConnectionPool
from psycopg.rows import dict_row


# ---------------------------------------------------------------------------
# Table names in FK dependency order (used for verification at the end)
# ---------------------------------------------------------------------------
ALL_TABLES = [
    "users",
    "workspaces",
    "workspace_files",
    "user_preferences",
    "watchlists",
    "watchlist_items",
    "user_portfolios",
    "user_api_keys",
    "conversation_threads",
    "conversation_queries",
    "conversation_responses",
    "conversation_usages",
    "automations",
    "automation_executions",
]

# Tables that have an updated_at column and need the auto-update trigger
TABLES_WITH_UPDATED_AT_TRIGGER = [
    "users",
    "workspaces",
    "workspace_files",
    "user_preferences",
    "watchlists",
    "watchlist_items",
    "user_portfolios",
    "conversation_threads",
    "automations",
]


async def setup_tables_async():
    """Initialize all application tables in PostgreSQL."""

    print("Setting up all application tables...")

    # Get database configuration from environment variables
    print("   Using database configuration (DB_*)")
    storage_type = os.getenv("DB_TYPE", "memory")

    if storage_type != "postgres":
        print(f"Error: Storage type is '{storage_type}', not 'postgres'")
        print("   Please set DB_TYPE=postgres in .env file")
        return False

    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "postgres")
    db_user = os.getenv("DB_USER", "postgres")
    db_password = os.getenv("DB_PASSWORD", "postgres")

    # Determine SSL mode based on host
    sslmode = "require" if "supabase.com" in db_host else "disable"

    db_uri = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}?sslmode={sslmode}"

    print(f"\n   Database Configuration:")
    print(f"   Host: {db_host}")
    print(f"   Port: {db_port}")
    print(f"   Database: {db_name}")
    print(f"   User: {db_user}")
    print(f"   SSL Mode: {sslmode}")

    try:
        print("\n   Connecting to database...")

        # Connection kwargs with prepare_threshold=0 for Supabase transaction pooler
        connection_kwargs = {
            "autocommit": True,
            "prepare_threshold": 0,
            "row_factory": dict_row,
        }

        # Create async connection pool
        async with AsyncConnectionPool(
            conninfo=db_uri,
            min_size=1,
            max_size=1,
            kwargs=connection_kwargs,
        ) as pool:
            await pool.wait()
            print("   Connected successfully!\n")

            async with pool.connection() as conn:
                async with conn.cursor() as cur:

                    # -------------------------------------------------------
                    # 0. Extensions
                    # -------------------------------------------------------
                    print("-- Installing extensions ...")
                    await cur.execute(
                        "CREATE EXTENSION IF NOT EXISTS pgcrypto;"
                    )
                    print("   pgcrypto OK")

                    # -------------------------------------------------------
                    # 0b. updated_at trigger function
                    # -------------------------------------------------------
                    print("\n-- Creating updated_at trigger function ...")
                    await cur.execute("""
                        CREATE OR REPLACE FUNCTION update_updated_at_column()
                        RETURNS TRIGGER AS $$
                        BEGIN
                            NEW.updated_at = NOW();
                            RETURN NEW;
                        END;
                        $$ LANGUAGE plpgsql;
                    """)
                    print("   update_updated_at_column() OK")

                    # ===================================================
                    # 1. users
                    # ===================================================
                    print("\n-- Creating 'users' table ...")
                    await cur.execute("""
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
                        );
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_users_email
                        ON users(email);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_users_created_at
                        ON users(created_at DESC);
                    """)
                    print("   users OK")

                    # ===================================================
                    # 3. workspaces
                    # ===================================================
                    print("\n-- Creating 'workspaces' table ...")
                    await cur.execute("""
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
                            config JSONB DEFAULT '{}'::jsonb
                        );
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_workspaces_user_id
                        ON workspaces(user_id);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_workspaces_user_status
                        ON workspaces(user_id, status);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_workspaces_updated_at
                        ON workspaces(updated_at DESC);
                    """)
                    print("   workspaces OK")

                    # ===================================================
                    # 3b. workspace_files
                    # ===================================================
                    print("\n-- Creating 'workspace_files' table ...")
                    await cur.execute("""
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
                        );
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_workspace_files_workspace_id
                        ON workspace_files(workspace_id);
                    """)
                    print("   workspace_files OK")

                    # ===================================================
                    # 4. user_preferences
                    # ===================================================
                    print("\n-- Creating 'user_preferences' table ...")
                    await cur.execute("""
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
                        );
                    """)
                    print("   user_preferences OK")

                    # ===================================================
                    # 5. watchlists
                    # ===================================================
                    print("\n-- Creating 'watchlists' table ...")
                    await cur.execute("""
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
                        );
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_watchlists_user_id
                        ON watchlists(user_id);
                    """)
                    print("   watchlists OK")

                    # ===================================================
                    # 6. watchlist_items
                    # ===================================================
                    print("\n-- Creating 'watchlist_items' table ...")
                    await cur.execute("""
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
                        );
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_watchlist_items_watchlist_id
                        ON watchlist_items(watchlist_id);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_watchlist_items_user_id
                        ON watchlist_items(user_id);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_watchlist_items_symbol
                        ON watchlist_items(symbol);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_watchlist_items_user_symbol
                        ON watchlist_items(user_id, symbol, instrument_type);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_watchlist_items_created_at
                        ON watchlist_items(created_at DESC);
                    """)
                    print("   watchlist_items OK")

                    # ===================================================
                    # 7. user_portfolios
                    # ===================================================
                    print("\n-- Creating 'user_portfolios' table ...")
                    await cur.execute("""
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
                        );
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_portfolios_user_id
                        ON user_portfolios(user_id);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_portfolios_symbol
                        ON user_portfolios(symbol);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_portfolios_instrument_type
                        ON user_portfolios(instrument_type);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_portfolios_user_instrument
                        ON user_portfolios(user_id, symbol, instrument_type);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_portfolios_account
                        ON user_portfolios(account_name);
                    """)
                    print("   user_portfolios OK")

                    # ===================================================
                    # 8. user_api_keys
                    # ===================================================
                    print("\n-- Creating 'user_api_keys' table ...")
                    await cur.execute("""
                        CREATE TABLE IF NOT EXISTS user_api_keys (
                            user_id VARCHAR(255) NOT NULL
                                REFERENCES users(user_id)
                                ON DELETE CASCADE ON UPDATE CASCADE,
                            provider VARCHAR(50) NOT NULL,
                            api_key BYTEA NOT NULL,
                            created_at TIMESTAMPTZ DEFAULT NOW(),
                            updated_at TIMESTAMPTZ DEFAULT NOW(),
                            PRIMARY KEY (user_id, provider)
                        );
                    """)
                    print("   user_api_keys OK")

                    # ===================================================
                    # 10b. user_oauth_tokens
                    # ===================================================
                    print("\n-- Creating 'user_oauth_tokens' table ...")
                    await cur.execute("""
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
                        );
                    """)
                    print("   user_oauth_tokens OK")

                    # ===================================================
                    # 11. conversation_threads
                    # ===================================================
                    print("\n-- Creating 'conversation_threads' table ...")
                    await cur.execute("""
                        CREATE TABLE IF NOT EXISTS conversation_threads (
                            conversation_thread_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                            workspace_id UUID NOT NULL
                                REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
                            msg_type VARCHAR(50)
                                CHECK (msg_type IN (
                                    'flash','ptc','interrupted','task'
                                )),
                            current_status VARCHAR(50) NOT NULL
                                CHECK (current_status IN (
                                    'in_progress','interrupted','completed','error','cancelled'
                                )),
                            thread_index INTEGER NOT NULL,
                            title VARCHAR(255),
                            external_id   VARCHAR(255),
                            platform      VARCHAR(50),
                            share_token VARCHAR(32) UNIQUE,
                            is_shared BOOLEAN NOT NULL DEFAULT FALSE,
                            share_permissions JSONB NOT NULL DEFAULT '{}',
                            shared_at TIMESTAMPTZ,
                            created_at TIMESTAMPTZ DEFAULT NOW(),
                            updated_at TIMESTAMPTZ DEFAULT NOW(),
                            CONSTRAINT unique_thread_index_per_workspace
                                UNIQUE (workspace_id, thread_index)
                        );
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_threads_created_at
                        ON conversation_threads(created_at DESC);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_threads_current_status
                        ON conversation_threads(current_status);
                    """)
                    await cur.execute("""
                        CREATE UNIQUE INDEX IF NOT EXISTS idx_threads_share_token
                        ON conversation_threads(share_token) WHERE share_token IS NOT NULL;
                    """)
                    await cur.execute("""
                        CREATE UNIQUE INDEX IF NOT EXISTS idx_conversation_threads_external
                        ON conversation_threads (platform, external_id)
                        WHERE external_id IS NOT NULL;
                    """)
                    print("   conversation_threads OK")

                    # ===================================================
                    # 12. conversation_queries
                    # ===================================================
                    print("\n-- Creating 'conversation_queries' table ...")
                    await cur.execute("""
                        CREATE TABLE IF NOT EXISTS conversation_queries (
                            conversation_query_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                            conversation_thread_id UUID NOT NULL
                                REFERENCES conversation_threads(conversation_thread_id)
                                ON DELETE CASCADE,
                            turn_index INTEGER NOT NULL,
                            content TEXT,
                            type VARCHAR(50) NOT NULL
                                CHECK (type IN (
                                    'initial','follow_up','resume_feedback'
                                )),
                            feedback_action TEXT,
                            metadata JSONB DEFAULT '{}'::jsonb,
                            created_at TIMESTAMPTZ NOT NULL,
                            CONSTRAINT unique_turn_index_per_thread_query
                                UNIQUE (conversation_thread_id, turn_index)
                        );
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_queries_thread_id
                        ON conversation_queries(conversation_thread_id);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_queries_created_at
                        ON conversation_queries(created_at DESC);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_queries_type
                        ON conversation_queries(type);
                    """)
                    print("   conversation_queries OK")

                    # ===================================================
                    # 13. conversation_responses
                    # ===================================================
                    print("\n-- Creating 'conversation_responses' table ...")
                    await cur.execute("""
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
                        );
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_responses_thread_id
                        ON conversation_responses(conversation_thread_id);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_responses_status
                        ON conversation_responses(status);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_responses_created_at
                        ON conversation_responses(created_at DESC);
                    """)
                    print("   conversation_responses OK")

                    # ===================================================
                    # 14. conversation_usages
                    # ===================================================
                    print("\n-- Creating 'conversation_usages' table ...")
                    await cur.execute("""
                        CREATE TABLE IF NOT EXISTS conversation_usages (
                            conversation_usage_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                            conversation_response_id UUID NOT NULL
                                REFERENCES conversation_responses(conversation_response_id)
                                ON DELETE CASCADE,
                            user_id VARCHAR(255) NOT NULL,
                            conversation_thread_id UUID NOT NULL
                                REFERENCES conversation_threads(conversation_thread_id)
                                ON DELETE CASCADE,
                            workspace_id UUID NOT NULL
                                REFERENCES workspaces(workspace_id)
                                ON DELETE CASCADE,
                            msg_type VARCHAR(50) NOT NULL DEFAULT 'ptc'
                                CHECK (msg_type IN (
                                    'flash','ptc','interrupted','task'
                                )),
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
                            created_at TIMESTAMPTZ DEFAULT NOW()
                        );
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_usages_user_timestamp
                        ON conversation_usages(user_id, created_at DESC);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_usages_thread_id
                        ON conversation_usages(conversation_thread_id);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_usages_workspace_id
                        ON conversation_usages(workspace_id);
                    """)
                    print("   conversation_usages OK")

                    # ===================================================
                    # 15. automations
                    # ===================================================
                    print("\n-- Creating 'automations' table ...")
                    await cur.execute("""
                        CREATE TABLE IF NOT EXISTS automations (
                            automation_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                            user_id             VARCHAR(255) NOT NULL
                                                    REFERENCES users(user_id) ON DELETE CASCADE,
                            name                VARCHAR(255) NOT NULL,
                            description         TEXT,
                            trigger_type        VARCHAR(20) NOT NULL
                                                    CHECK (trigger_type IN ('cron', 'once')),
                            cron_expression     VARCHAR(100),
                            timezone            VARCHAR(100) NOT NULL DEFAULT 'UTC',
                            trigger_config      JSONB DEFAULT '{}'::jsonb,
                            next_run_at         TIMESTAMPTZ,
                            last_run_at         TIMESTAMPTZ,
                            agent_mode          VARCHAR(20) NOT NULL DEFAULT 'flash'
                                                    CHECK (agent_mode IN ('ptc', 'flash')),
                            instruction         TEXT NOT NULL,
                            workspace_id        UUID
                                                    REFERENCES workspaces(workspace_id) ON DELETE SET NULL,
                            llm_model           VARCHAR(100),
                            additional_context  JSONB,
                            thread_strategy     VARCHAR(20) NOT NULL DEFAULT 'new'
                                                    CHECK (thread_strategy IN ('new', 'continue')),
                            conversation_thread_id UUID,
                            status              VARCHAR(20) NOT NULL DEFAULT 'active'
                                                    CHECK (status IN ('active', 'paused', 'completed', 'disabled')),
                            max_failures        INT NOT NULL DEFAULT 3,
                            failure_count       INT NOT NULL DEFAULT 0,
                            delivery_config     JSONB DEFAULT '{}'::jsonb,
                            metadata            JSONB DEFAULT '{}'::jsonb,
                            created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                            updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
                        );
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_automations_next_run
                            ON automations(next_run_at ASC)
                            WHERE status = 'active' AND next_run_at IS NOT NULL;
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_automations_user_id
                            ON automations(user_id);
                    """)
                    print("   automations OK")

                    # ===================================================
                    # 16. automation_executions
                    # ===================================================
                    print("\n-- Creating 'automation_executions' table ...")
                    await cur.execute("""
                        CREATE TABLE IF NOT EXISTS automation_executions (
                            automation_execution_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                            automation_id       UUID NOT NULL
                                                    REFERENCES automations(automation_id) ON DELETE CASCADE,
                            status              VARCHAR(20) NOT NULL DEFAULT 'pending'
                                                    CHECK (status IN ('pending', 'running', 'completed', 'failed', 'timeout')),
                            conversation_thread_id UUID,
                            scheduled_at        TIMESTAMPTZ NOT NULL,
                            started_at          TIMESTAMPTZ,
                            completed_at        TIMESTAMPTZ,
                            error_message       TEXT,
                            server_id           VARCHAR(100),
                            created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
                        );
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_automation_executions_automation_id
                            ON automation_executions(automation_id);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_automation_executions_status
                            ON automation_executions(status);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_automation_executions_created_at
                            ON automation_executions(created_at DESC);
                    """)
                    print("   automation_executions OK")

                    # ===================================================
                    # Attach updated_at triggers
                    # ===================================================
                    print("\n-- Attaching updated_at triggers ...")
                    for table in TABLES_WITH_UPDATED_AT_TRIGGER:
                        trigger_name = f"trg_{table}_updated_at"
                        await cur.execute(
                            f"DROP TRIGGER IF EXISTS {trigger_name} ON {table};"
                        )
                        await cur.execute(f"""
                            CREATE TRIGGER {trigger_name}
                                BEFORE UPDATE ON {table}
                                FOR EACH ROW
                                EXECUTE FUNCTION update_updated_at_column();
                        """)
                        print(f"   {trigger_name} OK")

                    # ===================================================
                    # Verification
                    # ===================================================
                    print("\n-- Verifying all tables ...")
                    placeholders = ",".join(
                        [f"'{t}'" for t in ALL_TABLES]
                    )
                    await cur.execute(f"""
                        SELECT table_name
                        FROM information_schema.tables
                        WHERE table_schema = 'public'
                          AND table_name IN ({placeholders})
                        ORDER BY table_name;
                    """)

                    tables = await cur.fetchall()
                    found = {row["table_name"] for row in tables}
                    missing = [t for t in ALL_TABLES if t not in found]

                    print(f"   Found {len(found)}/{len(ALL_TABLES)} tables:")
                    for t in ALL_TABLES:
                        status = "OK" if t in found else "MISSING"
                        print(f"     {status}  {t}")

                    if missing:
                        print(
                            f"\n   ERROR: Missing tables: {', '.join(missing)}"
                        )
                        return False

            print(f"\n   Setup complete! All {len(ALL_TABLES)} tables are ready.")
            print("\n   Schema Summary:")
            print("    - users:                  Central user profiles")
            print("    - workspaces:             Daytona sandbox workspaces")
            print("    - workspace_files:        Persisted workspace files (offline access)")
            print("    - user_preferences:       Categorized user preferences")
            print("    - watchlists:             Named watchlist containers")
            print("    - watchlist_items:        Instruments in watchlists")
            print("    - user_portfolios:        Current holdings")
            print("    - user_api_keys:          Encrypted BYOK API keys")
            print("    - conversation_threads:   Chat threads per workspace")
            print("    - conversation_queries:   User messages per thread")
            print("    - conversation_responses:  Agent responses (with sse_events)")
            print("    - conversation_usages:    Token/credit usage tracking")
            print("    - automations:            Scheduled automation triggers")
            print("    - automation_executions:  Automation run history")
            return True

    except Exception as e:
        print(f"\n   Error during setup: {e}")
        print("\nPlease check:")
        print("  1. Database credentials in .env file are correct")
        print("  2. Database server is accessible (SSH tunnel if needed)")
        print("  3. User has permission to create tables")
        import traceback

        traceback.print_exc()
        return False


def setup_tables():
    """Synchronous wrapper for async setup function."""
    return asyncio.run(setup_tables_async())


if __name__ == "__main__":
    success = setup_tables()
    sys.exit(0 if success else 1)
