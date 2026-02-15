#!/usr/bin/env python3
"""
Migration 005: Add membership_id column to users if missing.

Some databases were created with an older schema that did not include
membership_id. This migration adds the column when it does not exist.

Usage:
    uv run python scripts/migrations/005_add_membership_id_to_users.py
"""

import sys
import os
import asyncio
from pathlib import Path
from dotenv import load_dotenv

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))
load_dotenv(project_root / ".env")

from psycopg_pool import AsyncConnectionPool
from psycopg.rows import dict_row


async def column_exists(cur, table: str, col: str) -> bool:
    await cur.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s AND column_name = %s
        )
    """, (table, col))
    result = await cur.fetchone()
    return result['exists']


async def table_exists(cur, name: str) -> bool:
    await cur.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = %s
        )
    """, (name,))
    result = await cur.fetchone()
    return result['exists']


async def main():
    print("Migration 005: Add membership_id to users if missing")
    print("=" * 50)

    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "postgres")
    db_user = os.getenv("DB_USER", "postgres")
    db_password = os.getenv("DB_PASSWORD", "postgres")

    sslmode = "require" if "supabase.com" in db_host else "disable"
    db_uri = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}?sslmode={sslmode}"

    connection_kwargs = {
        "autocommit": True,
        "prepare_threshold": 0,
        "row_factory": dict_row,
    }

    try:
        async with AsyncConnectionPool(
            conninfo=db_uri,
            min_size=1,
            max_size=1,
            kwargs=connection_kwargs,
        ) as pool:
            await pool.wait()
            async with pool.connection() as conn:
                async with conn.cursor() as cur:
                    if not await table_exists(cur, "users"):
                        print("   users table does not exist, skipping")
                        return True

                    membership_id_exists = await column_exists(cur, "users", "membership_id")
                    if membership_id_exists:
                        print("   users.membership_id already exists, skipping")
                    else:
                        # Ensure memberships table exists (create if missing)
                        if not await table_exists(cur, "memberships"):
                            print("   Creating memberships table ...")
                            await cur.execute("""
                            CREATE TABLE IF NOT EXISTS memberships (
                                membership_id SERIAL PRIMARY KEY,
                                name VARCHAR(50) NOT NULL UNIQUE,
                                display_name VARCHAR(100) NOT NULL,
                                rank INT NOT NULL UNIQUE,
                                daily_credits NUMERIC(10,2) NOT NULL DEFAULT 500.0,
                                max_active_workspaces INT NOT NULL DEFAULT 3,
                                max_concurrent_requests INT NOT NULL DEFAULT 5,
                                is_default BOOLEAN NOT NULL DEFAULT FALSE,
                                created_at TIMESTAMPTZ DEFAULT NOW(),
                                updated_at TIMESTAMPTZ DEFAULT NOW()
                            )
                        """)
                            await cur.execute("""
                                INSERT INTO memberships
                                (name, display_name, rank, daily_credits,
                                 max_active_workspaces, max_concurrent_requests, is_default)
                            VALUES
                                ('free', 'Free', 0, 1000.0, 3, 5, TRUE),
                                ('pro', 'Pro', 1, 5000.0, 10, 20, FALSE),
                                ('enterprise', 'Enterprise', 2, -1, -1, -1, FALSE)
                                ON CONFLICT (name) DO NOTHING
                            """)
                            print("   memberships table created and seeded")

                        # Add column with default; memberships(membership_id=1) must exist
                        await cur.execute("""
                            ALTER TABLE users
                            ADD COLUMN membership_id INT NOT NULL DEFAULT 1
                            REFERENCES memberships(membership_id)
                        """)
                        print("   Added users.membership_id")

                        await cur.execute("""
                            CREATE INDEX IF NOT EXISTS idx_users_membership_id
                            ON users(membership_id)
                        """)
                        print("   Created idx_users_membership_id")

                    # Rename user_preferences.preference_id -> user_preference_id if needed
                    if await table_exists(cur, "user_preferences"):
                        if await column_exists(cur, "user_preferences", "preference_id") and not await column_exists(
                            cur, "user_preferences", "user_preference_id"
                        ):
                            await cur.execute(
                                "ALTER TABLE user_preferences RENAME COLUMN preference_id TO user_preference_id"
                            )
                            print("   Renamed user_preferences.preference_id -> user_preference_id")
                        elif await column_exists(cur, "user_preferences", "user_preference_id"):
                            print("   user_preferences.user_preference_id already exists, skipping")
                        else:
                            print("   user_preferences has neither preference_id nor user_preference_id, skipping")

        print("\nMigration 005 complete.")
        return True

    except Exception as e:
        print(f"\nMigration error: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
