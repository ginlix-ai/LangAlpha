#!/usr/bin/env python3
"""
Setup script for initializing user-related database tables in PostgreSQL.

This script creates the schema for storing user profiles, preferences,
watchlists, and portfolios.

Tables created:
- users: Central user profile table
- user_preferences: User preferences in categorized JSONB columns
- watchlists: Watchlist metadata (name, description, is_default)
- watchlist_items: Items within watchlists (formerly user_watchlist)
- user_portfolio: User's current holdings

Also adds FK constraint to workspaces.user_id if workspaces table exists.

Usage:
    uv run python scripts/setup_user_tables.py
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


async def setup_user_tables_async():
    """Initialize user-related tables in PostgreSQL."""

    print("🔧 Setting up user tables...")

    # Get database configuration from environment variables
    print("   Using database configuration (DB_*)")
    storage_type = os.getenv("DB_TYPE", "memory")

    if storage_type != "postgres":
        print(f"❌ Storage type is '{storage_type}', not 'postgres'")
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

    print(f"\n📊 Database Configuration:")
    print(f"   Host: {db_host}")
    print(f"   Port: {db_port}")
    print(f"   Database: {db_name}")
    print(f"   User: {db_user}")
    print(f"   SSL Mode: {sslmode}")

    try:
        print("\n🔌 Connecting to database...")

        # Connection kwargs with prepare_threshold=0 for Supabase transaction pooler
        connection_kwargs = {
            "autocommit": True,
            "prepare_threshold": 0,  # Disable prepared statements for transaction pooler
            "row_factory": dict_row
        }

        # Create async connection pool
        async with AsyncConnectionPool(
            conninfo=db_uri,
            min_size=1,
            max_size=1,  # Only need 1 connection for setup
            kwargs=connection_kwargs
        ) as pool:
            # Wait for pool to be ready
            await pool.wait()
            print("✅ Connected successfully!")

            async with pool.connection() as conn:
                async with conn.cursor() as cur:
                    # ===========================================
                    # 1. Create users table
                    # ===========================================
                    print("\n📝 Creating 'users' table...")
                    await cur.execute("""
                        CREATE TABLE IF NOT EXISTS users (
                            user_id VARCHAR(255) PRIMARY KEY,
                            email VARCHAR(255),
                            name VARCHAR(255),
                            avatar_url TEXT,

                            -- Locale settings
                            timezone VARCHAR(100),
                            locale VARCHAR(20),

                            -- Onboarding status
                            onboarding_completed BOOLEAN NOT NULL DEFAULT FALSE,

                            -- Timestamps
                            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                            last_login_at TIMESTAMPTZ
                        );
                    """)

                    print("   Creating indexes on 'users'...")
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_users_email
                        ON users(email);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_users_created_at
                        ON users(created_at DESC);
                    """)
                    print("✅ 'users' table created!")

                    # ===========================================
                    # 2. Create user_preferences table
                    # ===========================================
                    print("\n📝 Creating 'user_preferences' table...")
                    await cur.execute("""
                        CREATE TABLE IF NOT EXISTS user_preferences (
                            preference_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                            user_id VARCHAR(255) UNIQUE NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,

                            -- Categorized preferences (JSONB for flexibility)
                            risk_preference JSONB DEFAULT '{}'::jsonb,
                            investment_preference JSONB DEFAULT '{}'::jsonb,
                            agent_preference JSONB DEFAULT '{}'::jsonb,
                            other_preference JSONB DEFAULT '{}'::jsonb,

                            -- Timestamps
                            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                        );
                    """)

                    print("   Creating indexes on 'user_preferences'...")
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_user_preferences_user_id
                        ON user_preferences(user_id);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_user_preferences_risk
                        ON user_preferences USING GIN (risk_preference);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_user_preferences_investment
                        ON user_preferences USING GIN (investment_preference);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_user_preferences_agent
                        ON user_preferences USING GIN (agent_preference);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_user_preferences_other
                        ON user_preferences USING GIN (other_preference);
                    """)
                    print("✅ 'user_preferences' table created!")

                    # ===========================================
                    # 3. Create watchlists table (list metadata)
                    # ===========================================
                    print("\n📝 Creating 'watchlists' table...")
                    await cur.execute("""
                        CREATE TABLE IF NOT EXISTS watchlists (
                            watchlist_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                            user_id VARCHAR(255) NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,

                            -- Watchlist metadata
                            name VARCHAR(100) NOT NULL,
                            description TEXT,
                            is_default BOOLEAN NOT NULL DEFAULT FALSE,
                            display_order INTEGER DEFAULT 0,

                            -- Timestamps
                            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

                            CONSTRAINT unique_user_watchlist_name UNIQUE (user_id, name)
                        );
                    """)

                    print("   Creating indexes on 'watchlists'...")
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_watchlists_user_id
                        ON watchlists(user_id);
                    """)
                    print("✅ 'watchlists' table created!")

                    # ===========================================
                    # 3b. Create watchlist_items table
                    # ===========================================
                    print("\n📝 Creating 'watchlist_items' table...")
                    await cur.execute("""
                        CREATE TABLE IF NOT EXISTS watchlist_items (
                            item_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                            watchlist_id UUID NOT NULL REFERENCES watchlists(watchlist_id) ON DELETE CASCADE,
                            user_id VARCHAR(255) NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,

                            -- Instrument identification
                            symbol VARCHAR(50) NOT NULL,
                            instrument_type VARCHAR(30) NOT NULL,
                            exchange VARCHAR(50),
                            name VARCHAR(255),

                            -- Item metadata
                            notes TEXT,
                            alert_settings JSONB DEFAULT '{}'::jsonb,
                            metadata JSONB DEFAULT '{}'::jsonb,

                            -- Timestamps
                            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

                            CONSTRAINT unique_watchlist_item UNIQUE (watchlist_id, symbol, instrument_type)
                        );
                    """)

                    print("   Creating indexes on 'watchlist_items'...")
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
                        CREATE INDEX IF NOT EXISTS idx_watchlist_items_instrument_type
                        ON watchlist_items(instrument_type);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_watchlist_items_user_symbol
                        ON watchlist_items(user_id, symbol, instrument_type);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_watchlist_items_created_at
                        ON watchlist_items(created_at DESC);
                    """)
                    print("✅ 'watchlist_items' table created!")

                    # ===========================================
                    # 4. Create user_portfolio table
                    # ===========================================
                    print("\n📝 Creating 'user_portfolio' table...")
                    await cur.execute("""
                        CREATE TABLE IF NOT EXISTS user_portfolio (
                            holding_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                            user_id VARCHAR(255) NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,

                            -- Instrument identification
                            symbol VARCHAR(50) NOT NULL,
                            instrument_type VARCHAR(30) NOT NULL,
                            exchange VARCHAR(50),
                            name VARCHAR(255),

                            -- Holding details
                            quantity DECIMAL(18, 8) NOT NULL,
                            average_cost DECIMAL(18, 4),
                            currency VARCHAR(10) DEFAULT 'USD',

                            -- Optional metadata
                            account_name VARCHAR(100),
                            notes TEXT,
                            metadata JSONB DEFAULT '{}'::jsonb,

                            -- Timestamps
                            first_purchased_at TIMESTAMPTZ,
                            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

                            CONSTRAINT unique_user_holding UNIQUE (user_id, symbol, instrument_type, account_name)
                        );
                    """)

                    print("   Creating indexes on 'user_portfolio'...")
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_portfolio_user_id
                        ON user_portfolio(user_id);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_portfolio_symbol
                        ON user_portfolio(symbol);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_portfolio_instrument_type
                        ON user_portfolio(instrument_type);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_portfolio_user_instrument
                        ON user_portfolio(user_id, symbol, instrument_type);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_portfolio_account
                        ON user_portfolio(account_name);
                    """)
                    print("✅ 'user_portfolio' table created!")

                    # ===========================================
                    # 5. Add FK constraint to workspaces table
                    # ===========================================
                    print("\n📝 Adding FK constraint to 'workspaces' table...")

                    # Check if workspaces table exists
                    await cur.execute("""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables
                            WHERE table_schema = 'public'
                            AND table_name = 'workspaces'
                        );
                    """)
                    result = await cur.fetchone()
                    workspaces_exists = result['exists'] if result else False

                    if workspaces_exists:
                        # Check if FK constraint already exists
                        await cur.execute("""
                            SELECT EXISTS (
                                SELECT FROM information_schema.table_constraints
                                WHERE constraint_name = 'fk_workspaces_user_id'
                                AND table_name = 'workspaces'
                            );
                        """)
                        result = await cur.fetchone()
                        fk_exists = result['exists'] if result else False

                        if fk_exists:
                            print("   FK constraint 'fk_workspaces_user_id' already exists, skipping...")
                        else:
                            # Check if there are orphan user_ids in workspaces that don't exist in users
                            await cur.execute("""
                                SELECT COUNT(*) as orphan_count
                                FROM workspaces w
                                WHERE NOT EXISTS (
                                    SELECT 1 FROM users u WHERE u.user_id = w.user_id
                                );
                            """)
                            result = await cur.fetchone()
                            orphan_count = result['orphan_count'] if result else 0

                            if orphan_count > 0:
                                print(f"   ⚠️  Found {orphan_count} workspace(s) with user_id not in users table.")
                                print("   Creating missing user records for existing workspaces...")

                                # Insert missing users (minimal records)
                                await cur.execute("""
                                    INSERT INTO users (user_id, created_at, updated_at)
                                    SELECT DISTINCT w.user_id, NOW(), NOW()
                                    FROM workspaces w
                                    WHERE NOT EXISTS (
                                        SELECT 1 FROM users u WHERE u.user_id = w.user_id
                                    )
                                    ON CONFLICT (user_id) DO NOTHING;
                                """)
                                print("   ✅ Missing user records created.")

                            # Now add the FK constraint
                            await cur.execute("""
                                ALTER TABLE workspaces
                                ADD CONSTRAINT fk_workspaces_user_id
                                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE;
                            """)
                            print("✅ FK constraint added to 'workspaces' table!")
                    else:
                        print("   ⚠️  'workspaces' table does not exist, skipping FK constraint.")
                        print("   Run setup_conversation_tables.py first, then re-run this script.")

                    # ===========================================
                    # Verify tables exist
                    # ===========================================
                    print("\n🔍 Verifying tables...")
                    await cur.execute("""
                        SELECT table_name
                        FROM information_schema.tables
                        WHERE table_schema = 'public'
                        AND table_name IN (
                            'users',
                            'user_preferences',
                            'watchlists',
                            'watchlist_items',
                            'user_portfolio'
                        )
                        ORDER BY table_name;
                    """)

                    tables = await cur.fetchall()
                    print(f"   Found {len(tables)} tables:")
                    for table in tables:
                        print(f"     ✓ {table['table_name']}")

                    # Verify FK constraint on workspaces
                    if workspaces_exists:
                        await cur.execute("""
                            SELECT constraint_name
                            FROM information_schema.table_constraints
                            WHERE constraint_name = 'fk_workspaces_user_id'
                            AND table_name = 'workspaces';
                        """)
                        fk_result = await cur.fetchone()
                        if fk_result:
                            print(f"     ✓ FK constraint: {fk_result['constraint_name']}")

            print("\n🎉 Setup complete! User tables are ready.")
            print("\n📋 Schema Summary:")
            print("   • users: Central user profile table")
            print("   • user_preferences: User preferences (risk, investment, agent, other)")
            print("   • watchlists: Named watchlist containers per user")
            print("   • watchlist_items: Instruments within watchlists")
            print("   • user_portfolio: User's current holdings")
            if workspaces_exists:
                print("   • FK constraint on workspaces.user_id → users.user_id")
            return True

    except Exception as e:
        print(f"\n❌ Error during setup: {e}")
        print("\nPlease check:")
        print("  1. Database credentials in .env file are correct")
        print("  2. Database server is accessible (SSH tunnel if needed)")
        print("  3. User has permission to create/alter tables")
        import traceback
        traceback.print_exc()
        return False


def setup_user_tables():
    """Synchronous wrapper for async setup function."""
    return asyncio.run(setup_user_tables_async())


if __name__ == "__main__":
    success = setup_user_tables()
    sys.exit(0 if success else 1)
