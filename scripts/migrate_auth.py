#!/usr/bin/env python3
"""
Auth migration: add ON UPDATE CASCADE to FK constraints and migrate
email-based user_ids to UUIDs.

Two-step migration:
  1. Drop and recreate FK constraints with ON UPDATE CASCADE so that when
     we change a user's PK, all child tables cascade automatically.
  2. For each existing user whose user_id looks like an email (contains '@'),
     generate a UUID and UPDATE the PK. Thanks to ON UPDATE CASCADE the
     change propagates to workspaces, watchlists, watchlist_items,
     user_preferences, and user_portfolio.

Idempotent — safe to run multiple times.

Usage:
    uv run python scripts/migrate_auth.py
"""

import asyncio
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
load_dotenv(project_root / ".env")

from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

# FK constraints that need ON UPDATE CASCADE.
# (table, constraint_name, fk_column, ref_table, ref_column, extra)
FK_DEFINITIONS = [
    (
        "user_preferences",
        "user_preferences_user_id_fkey",
        "user_id",
        "users",
        "user_id",
        "UNIQUE (user_id)",  # keep the UNIQUE alongside the FK
    ),
    (
        "watchlists",
        "watchlists_user_id_fkey",
        "user_id",
        "users",
        "user_id",
        None,
    ),
    (
        "watchlist_items",
        "watchlist_items_user_id_fkey",
        "user_id",
        "users",
        "user_id",
        None,
    ),
    (
        "user_portfolio",
        "user_portfolio_user_id_fkey",
        "user_id",
        "users",
        "user_id",
        None,
    ),
    (
        "workspaces",
        "fk_workspaces_user_id",
        "user_id",
        "users",
        "user_id",
        None,
    ),
]


def _build_db_uri() -> str:
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    name = os.getenv("DB_NAME", "postgres")
    user = os.getenv("DB_USER", "postgres")
    password = os.getenv("DB_PASSWORD", "postgres")
    sslmode = "require" if "supabase.com" in host else "disable"
    return f"postgresql://{user}:{password}@{host}:{port}/{name}?sslmode={sslmode}"


async def _table_exists(cur, table: str) -> bool:
    await cur.execute(
        "SELECT EXISTS (SELECT FROM information_schema.tables "
        "WHERE table_schema='public' AND table_name=%s)",
        (table,),
    )
    row = await cur.fetchone()
    return row["exists"] if row else False


async def _constraint_exists(cur, table: str, constraint: str) -> bool:
    await cur.execute(
        "SELECT EXISTS (SELECT FROM information_schema.table_constraints "
        "WHERE table_name=%s AND constraint_name=%s)",
        (table, constraint),
    )
    row = await cur.fetchone()
    return row["exists"] if row else False


async def step1_update_cascade(cur):
    """Drop and recreate FK constraints with ON UPDATE CASCADE."""
    print("\n=== Step 1: Add ON UPDATE CASCADE to FK constraints ===")

    for table, constraint, fk_col, ref_table, ref_col, _extra in FK_DEFINITIONS:
        if not await _table_exists(cur, table):
            print(f"  SKIP  {table} (table does not exist)")
            continue

        # Find ALL FK constraints on this column (may have different names)
        await cur.execute(
            """
            SELECT tc.constraint_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
            WHERE tc.table_name = %s
              AND tc.constraint_type = 'FOREIGN KEY'
              AND kcu.column_name = %s
            """,
            (table, fk_col),
        )
        rows = await cur.fetchall()
        existing_names = [r["constraint_name"] for r in rows]

        for name in existing_names:
            await cur.execute(
                f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {name}"
            )
            print(f"  DROP  {table}.{name}")

        # Recreate with ON UPDATE CASCADE
        await cur.execute(
            f"ALTER TABLE {table} ADD CONSTRAINT {constraint} "
            f"FOREIGN KEY ({fk_col}) REFERENCES {ref_table}({ref_col}) "
            f"ON DELETE CASCADE ON UPDATE CASCADE"
        )
        print(f"  ADD   {table}.{constraint}  (ON DELETE CASCADE ON UPDATE CASCADE)")

    print("  Done.")


async def step2_migrate_email_to_uuid(cur):
    """For users whose user_id contains '@', replace with a UUID."""
    print("\n=== Step 2: Migrate email-based user_ids to UUID ===")

    await cur.execute(
        "SELECT user_id FROM users WHERE user_id LIKE '%%@%%'"
    )
    rows = await cur.fetchall()
    if not rows:
        print("  No email-based user_ids found. Nothing to migrate.")
        return

    print(f"  Found {len(rows)} user(s) with email-based user_id")
    for row in rows:
        old_id = row["user_id"]
        await cur.execute("SELECT gen_random_uuid()::text AS new_id")
        new_row = await cur.fetchone()
        new_id = new_row["new_id"]

        await cur.execute(
            "UPDATE users SET user_id = %s WHERE user_id = %s",
            (new_id, old_id),
        )
        print(f"  MIGRATE  {old_id}  ->  {new_id}")

    print("  Done.")


async def main():
    print("Auth Migration — ON UPDATE CASCADE + email→UUID")

    db_uri = _build_db_uri()
    print(f"  DB: {db_uri.split('@')[1] if '@' in db_uri else db_uri}")

    connection_kwargs = {
        "autocommit": True,
        "prepare_threshold": 0,
        "row_factory": dict_row,
    }

    async with AsyncConnectionPool(
        conninfo=db_uri,
        min_size=1,
        max_size=1,
        kwargs=connection_kwargs,
    ) as pool:
        await pool.wait()
        print("  Connected.\n")

        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await step1_update_cascade(cur)
                await step2_migrate_email_to_uuid(cur)

    print("\nMigration complete.")


if __name__ == "__main__":
    asyncio.run(main())
