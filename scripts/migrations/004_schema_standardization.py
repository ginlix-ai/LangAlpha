#!/usr/bin/env python3
"""
Migration 004: Schema standardization.

Detect-first-then-migrate approach -- every step checks current state via
information_schema before acting.  Idempotent -- safe to re-run after partial
failure.

Changes:
  - Drop unused workspace file tables
  - Rename plans -> memberships (with FK updates)
  - Pluralize singular table names
  - Convert VARCHAR(255) PKs to UUID on conversation tables
  - Standardize PK/FK column naming conventions
  - Rename legacy columns (pair_index -> turn_index, etc.)
  - Drop dead columns from conversation_responses
  - Add CHECK constraints
  - Replace bloated indexes with targeted ones
  - Add updated_at trigger to relevant tables
  - Create workspace_files table (file persistence)

Usage:
    uv run python scripts/migrations/004_schema_standardization.py
"""

import sys
import os
import asyncio
from pathlib import Path
from dotenv import load_dotenv

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Load environment variables
load_dotenv(project_root / ".env")

from psycopg_pool import AsyncConnectionPool
from psycopg.rows import dict_row


# ---------------------------------------------------------------------------
# Detection helpers
# ---------------------------------------------------------------------------

async def table_exists(cur, name: str) -> bool:
    await cur.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = %s
        )
    """, (name,))
    result = await cur.fetchone()
    return result['exists']


async def column_exists(cur, table: str, col: str) -> bool:
    await cur.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s AND column_name = %s
        )
    """, (table, col))
    result = await cur.fetchone()
    return result['exists']


async def constraint_exists(cur, name: str) -> bool:
    await cur.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.table_constraints
            WHERE table_schema = 'public' AND constraint_name = %s
        )
    """, (name,))
    result = await cur.fetchone()
    return result['exists']


async def index_exists(cur, name: str) -> bool:
    await cur.execute("""
        SELECT EXISTS (
            SELECT FROM pg_indexes
            WHERE schemaname = 'public' AND indexname = %s
        )
    """, (name,))
    result = await cur.fetchone()
    return result['exists']


async def trigger_exists(cur, name: str) -> bool:
    await cur.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.triggers
            WHERE trigger_schema = 'public' AND trigger_name = %s
        )
    """, (name,))
    result = await cur.fetchone()
    return result['exists']


async def get_column_type(cur, table: str, col: str) -> str:
    await cur.execute("""
        SELECT data_type FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s AND column_name = %s
    """, (table, col))
    result = await cur.fetchone()
    return result['data_type'] if result else None


async def drop_fk_on_column(cur, table: str, col: str):
    """Drop all FK constraints on a given table.column."""
    await cur.execute("""
        SELECT tc.constraint_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        WHERE tc.table_schema = 'public'
          AND tc.table_name = %s
          AND tc.constraint_type = 'FOREIGN KEY'
          AND kcu.column_name = %s
    """, (table, col))
    rows = await cur.fetchall()
    for row in rows:
        cname = row['constraint_name']
        await cur.execute(f'ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {cname}')
        print(f"      Dropped FK {table}.{cname}")


async def drop_unique_on_columns(cur, table: str, col: str):
    """Drop UNIQUE constraints that involve a given column."""
    await cur.execute("""
        SELECT tc.constraint_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        WHERE tc.table_schema = 'public'
          AND tc.table_name = %s
          AND tc.constraint_type = 'UNIQUE'
          AND kcu.column_name = %s
    """, (table, col))
    rows = await cur.fetchall()
    for row in rows:
        cname = row['constraint_name']
        await cur.execute(f'ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {cname}')
        print(f"      Dropped UNIQUE {table}.{cname}")


# ---------------------------------------------------------------------------
# Migration steps
# ---------------------------------------------------------------------------

async def step1_drop_workspace_file_tables(cur):
    """Drop unused workspace file tables."""
    print("\n== Step 1: Drop workspace file tables ==")
    for table in ['workspace_file_operations', 'workspace_files', 'workspace_filesystems']:
        if await table_exists(cur, table):
            await cur.execute(f"DROP TABLE {table} CASCADE")
            print(f"   Dropped {table}")
        else:
            print(f"   {table} already gone, skipping")


async def step2_rename_plans_to_memberships(cur):
    """Rename plans -> memberships and fix PK column."""
    print("\n== Step 2: Rename plans -> memberships ==")

    if not await table_exists(cur, 'plans'):
        if await table_exists(cur, 'memberships'):
            print("   Already renamed to memberships, skipping")
        else:
            print("   Neither plans nor memberships exists, skipping")
        return

    if await table_exists(cur, 'memberships'):
        print("   Both plans and memberships exist -- skipping rename (manual intervention needed)")
        return

    # Drop dependent FK constraints before rename
    for dep_table, dep_col in [('users', 'plan_id'), ('redemption_codes', 'plan_id')]:
        if await table_exists(cur, dep_table) and await column_exists(cur, dep_table, dep_col):
            await drop_fk_on_column(cur, dep_table, dep_col)

    # Rename the table
    await cur.execute("ALTER TABLE plans RENAME TO memberships")
    print("   Renamed plans -> memberships")

    # Rename PK column id -> membership_id (if still named 'id')
    if await column_exists(cur, 'memberships', 'id') and not await column_exists(cur, 'memberships', 'membership_id'):
        await cur.execute("ALTER TABLE memberships RENAME COLUMN id TO membership_id")
        print("   Renamed memberships.id -> membership_id")

    # Rename indexes if they exist
    for old_idx, new_idx in [
        ('idx_plans_default', 'idx_memberships_default'),
        ('idx_plans_rank', 'idx_memberships_rank'),
        ('plans_pkey', 'memberships_pkey'),
        ('plans_name_key', 'memberships_name_key'),
    ]:
        if await index_exists(cur, old_idx):
            try:
                await cur.execute(f"ALTER INDEX {old_idx} RENAME TO {new_idx}")
                print(f"   Renamed index {old_idx} -> {new_idx}")
            except Exception as e:
                print(f"   Could not rename index {old_idx}: {e}")


async def step3_rename_plan_id_fk_columns(cur):
    """Rename plan_id -> membership_id on users and redemption_codes."""
    print("\n== Step 3: Rename FK columns referencing plans ==")

    memberships_table = 'memberships' if await table_exists(cur, 'memberships') else 'plans'
    memberships_pk = 'membership_id' if await column_exists(cur, memberships_table, 'membership_id') else 'id'

    for dep_table in ['users', 'redemption_codes']:
        if not await table_exists(cur, dep_table):
            print(f"   {dep_table} does not exist, skipping")
            continue

        if await column_exists(cur, dep_table, 'plan_id') and not await column_exists(cur, dep_table, 'membership_id'):
            # Drop any remaining FKs on plan_id
            await drop_fk_on_column(cur, dep_table, 'plan_id')

            await cur.execute(f"ALTER TABLE {dep_table} RENAME COLUMN plan_id TO membership_id")
            print(f"   Renamed {dep_table}.plan_id -> membership_id")

            # Re-add FK constraint
            fk_name = f"fk_{dep_table}_membership_id"
            if not await constraint_exists(cur, fk_name):
                try:
                    await cur.execute(
                        f"ALTER TABLE {dep_table} ADD CONSTRAINT {fk_name} "
                        f"FOREIGN KEY (membership_id) REFERENCES {memberships_table}({memberships_pk})"
                    )
                    print(f"   Added FK {fk_name}")
                except Exception as e:
                    print(f"   Could not add FK {fk_name}: {e}")
        elif await column_exists(cur, dep_table, 'membership_id'):
            print(f"   {dep_table}.membership_id already exists, skipping")
        else:
            print(f"   {dep_table} has no plan_id column, skipping")

    # Rename the users index if it exists
    if await index_exists(cur, 'idx_users_plan_id') and not await index_exists(cur, 'idx_users_membership_id'):
        try:
            await cur.execute("ALTER INDEX idx_users_plan_id RENAME TO idx_users_membership_id")
            print("   Renamed index idx_users_plan_id -> idx_users_membership_id")
        except Exception as e:
            print(f"   Could not rename index: {e}")


async def step4_pluralize_table_names(cur):
    """Rename singular table names to plural."""
    print("\n== Step 4: Pluralize table names ==")

    renames = [
        ('conversation_thread', 'conversation_threads'),
        ('conversation_query', 'conversation_queries'),
        ('conversation_response', 'conversation_responses'),
        ('conversation_usage', 'conversation_usages'),
        ('user_portfolio', 'user_portfolios'),
        ('redemption_history', 'redemption_histories'),
    ]

    for old_name, new_name in renames:
        if await table_exists(cur, old_name) and not await table_exists(cur, new_name):
            await cur.execute(f"ALTER TABLE {old_name} RENAME TO {new_name}")
            print(f"   Renamed {old_name} -> {new_name}")
        elif await table_exists(cur, new_name):
            print(f"   {new_name} already exists, skipping")
        else:
            print(f"   {old_name} does not exist, skipping")


async def step5_convert_varchar_pks_to_uuid(cur):
    """Convert VARCHAR(255) PK/FK columns to UUID type on conversation tables."""
    print("\n== Step 5: Convert VARCHAR PKs/FKs to UUID ==")

    # Use current table names (may already be plural from step 4)
    threads_table = 'conversation_threads' if await table_exists(cur, 'conversation_threads') else 'conversation_thread'
    queries_table = 'conversation_queries' if await table_exists(cur, 'conversation_queries') else 'conversation_query'
    responses_table = 'conversation_responses' if await table_exists(cur, 'conversation_responses') else 'conversation_response'
    usages_table = 'conversation_usages' if await table_exists(cur, 'conversation_usages') else 'conversation_usage'

    # Determine current PK column names (may already be renamed)
    def pk_col(table, old, new):
        """Will be resolved async below."""
        return (table, old, new)

    # --- conversation_threads ---
    if await table_exists(cur, threads_table):
        # Determine the PK column name
        thread_pk = 'conversation_thread_id' if await column_exists(cur, threads_table, 'conversation_thread_id') else 'thread_id'

        col_type = await get_column_type(cur, threads_table, thread_pk)
        if col_type and col_type == 'character varying':
            print(f"   Converting {threads_table}.{thread_pk} VARCHAR -> UUID")

            # Drop FK constraints from children that reference this column
            for child_table in [queries_table, responses_table, usages_table]:
                if await table_exists(cur, child_table):
                    # FK column name may be thread_id or conversation_thread_id
                    for fk_col in ['thread_id', 'conversation_thread_id']:
                        if await column_exists(cur, child_table, fk_col):
                            await drop_fk_on_column(cur, child_table, fk_col)
                            await drop_unique_on_columns(cur, child_table, fk_col)

            # Drop unique constraints on the PK column itself
            await drop_unique_on_columns(cur, threads_table, thread_pk)

            # Convert parent PK
            await cur.execute(
                f"ALTER TABLE {threads_table} ALTER COLUMN {thread_pk} TYPE UUID USING {thread_pk}::uuid"
            )
            print(f"      Converted {threads_table}.{thread_pk}")

            # Convert FK columns in children
            for child_table in [queries_table, responses_table, usages_table]:
                if await table_exists(cur, child_table):
                    for fk_col in ['thread_id', 'conversation_thread_id']:
                        if await column_exists(cur, child_table, fk_col):
                            child_type = await get_column_type(cur, child_table, fk_col)
                            if child_type == 'character varying':
                                await cur.execute(
                                    f"ALTER TABLE {child_table} ALTER COLUMN {fk_col} TYPE UUID USING {fk_col}::uuid"
                                )
                                print(f"      Converted {child_table}.{fk_col}")

            # Re-add FK constraints from children
            for child_table in [queries_table, responses_table, usages_table]:
                if await table_exists(cur, child_table):
                    for fk_col in ['thread_id', 'conversation_thread_id']:
                        if await column_exists(cur, child_table, fk_col):
                            fk_name = f"fk_{child_table}_{fk_col}_threads"
                            if not await constraint_exists(cur, fk_name):
                                try:
                                    await cur.execute(
                                        f"ALTER TABLE {child_table} ADD CONSTRAINT {fk_name} "
                                        f"FOREIGN KEY ({fk_col}) REFERENCES {threads_table}({thread_pk}) ON DELETE CASCADE"
                                    )
                                    print(f"      Re-added FK {fk_name}")
                                except Exception as e:
                                    print(f"      Could not re-add FK {fk_name}: {e}")

            # Re-add unique constraint for workspace_id + thread_index
            if await column_exists(cur, threads_table, 'workspace_id') and await column_exists(cur, threads_table, 'thread_index'):
                uq_name = 'unique_thread_index_per_workspace'
                if not await constraint_exists(cur, uq_name):
                    try:
                        await cur.execute(
                            f"ALTER TABLE {threads_table} ADD CONSTRAINT {uq_name} "
                            f"UNIQUE (workspace_id, thread_index)"
                        )
                        print(f"      Re-added UNIQUE {uq_name}")
                    except Exception as e:
                        print(f"      Could not re-add UNIQUE {uq_name}: {e}")
        else:
            print(f"   {threads_table}.{thread_pk} is already UUID (or missing), skipping")

    # --- conversation_queries ---
    if await table_exists(cur, queries_table):
        query_pk = 'conversation_query_id' if await column_exists(cur, queries_table, 'conversation_query_id') else 'query_id'
        col_type = await get_column_type(cur, queries_table, query_pk)
        if col_type and col_type == 'character varying':
            print(f"   Converting {queries_table}.{query_pk} VARCHAR -> UUID")
            await drop_unique_on_columns(cur, queries_table, query_pk)
            await cur.execute(
                f"ALTER TABLE {queries_table} ALTER COLUMN {query_pk} TYPE UUID USING {query_pk}::uuid"
            )
            print(f"      Converted {queries_table}.{query_pk}")
        else:
            print(f"   {queries_table}.{query_pk} is already UUID (or missing), skipping")

    # --- conversation_responses ---
    if await table_exists(cur, responses_table):
        response_pk = 'conversation_response_id' if await column_exists(cur, responses_table, 'conversation_response_id') else 'response_id'
        col_type = await get_column_type(cur, responses_table, response_pk)
        if col_type and col_type == 'character varying':
            print(f"   Converting {responses_table}.{response_pk} VARCHAR -> UUID")

            # Drop FK from usages.response_id first
            if await table_exists(cur, usages_table):
                for fk_col in ['response_id', 'conversation_response_id']:
                    if await column_exists(cur, usages_table, fk_col):
                        await drop_fk_on_column(cur, usages_table, fk_col)
                        await drop_unique_on_columns(cur, usages_table, fk_col)

            await drop_unique_on_columns(cur, responses_table, response_pk)
            await cur.execute(
                f"ALTER TABLE {responses_table} ALTER COLUMN {response_pk} TYPE UUID USING {response_pk}::uuid"
            )
            print(f"      Converted {responses_table}.{response_pk}")

            # Convert FK column in usages
            if await table_exists(cur, usages_table):
                for fk_col in ['response_id', 'conversation_response_id']:
                    if await column_exists(cur, usages_table, fk_col):
                        child_type = await get_column_type(cur, usages_table, fk_col)
                        if child_type == 'character varying':
                            await cur.execute(
                                f"ALTER TABLE {usages_table} ALTER COLUMN {fk_col} TYPE UUID USING {fk_col}::uuid"
                            )
                            print(f"      Converted {usages_table}.{fk_col}")

                        # Re-add FK
                        fk_name = f"fk_{usages_table}_{fk_col}_responses"
                        if not await constraint_exists(cur, fk_name):
                            try:
                                await cur.execute(
                                    f"ALTER TABLE {usages_table} ADD CONSTRAINT {fk_name} "
                                    f"FOREIGN KEY ({fk_col}) REFERENCES {responses_table}({response_pk}) ON DELETE CASCADE"
                                )
                                print(f"      Re-added FK {fk_name}")
                            except Exception as e:
                                print(f"      Could not re-add FK {fk_name}: {e}")

                        # Re-add UNIQUE on response_id in usages
                        uq_name = f"uq_{usages_table}_{fk_col}"
                        if not await constraint_exists(cur, uq_name):
                            try:
                                await cur.execute(
                                    f"ALTER TABLE {usages_table} ADD CONSTRAINT {uq_name} UNIQUE ({fk_col})"
                                )
                                print(f"      Re-added UNIQUE {uq_name}")
                            except Exception as e:
                                print(f"      Could not re-add UNIQUE {uq_name}: {e}")
        else:
            print(f"   {responses_table}.{response_pk} is already UUID (or missing), skipping")

    # --- conversation_usages PK ---
    if await table_exists(cur, usages_table):
        usage_pk = 'conversation_usage_id' if await column_exists(cur, usages_table, 'conversation_usage_id') else 'usage_id'
        col_type = await get_column_type(cur, usages_table, usage_pk)
        if col_type and col_type == 'character varying':
            print(f"   Converting {usages_table}.{usage_pk} VARCHAR -> UUID")
            # Drop default first; PostgreSQL cannot cast VARCHAR default to UUID automatically
            try:
                await cur.execute(
                    f"ALTER TABLE {usages_table} ALTER COLUMN {usage_pk} DROP DEFAULT"
                )
            except Exception:
                pass  # No default or already dropped
            await cur.execute(
                f"ALTER TABLE {usages_table} ALTER COLUMN {usage_pk} TYPE UUID USING {usage_pk}::uuid"
            )
            await cur.execute(
                f"ALTER TABLE {usages_table} ALTER COLUMN {usage_pk} SET DEFAULT gen_random_uuid()"
            )
            print(f"      Converted {usages_table}.{usage_pk}")
        else:
            print(f"   {usages_table}.{usage_pk} is already UUID (or missing), skipping")

    # Re-add unique constraints on pair_index (or turn_index) per thread for queries and responses
    for tbl, pair_col_candidates in [
        (queries_table, ['turn_index', 'pair_index']),
        (responses_table, ['turn_index', 'pair_index']),
    ]:
        if not await table_exists(cur, tbl):
            continue
        thread_fk = 'conversation_thread_id' if await column_exists(cur, tbl, 'conversation_thread_id') else 'thread_id'
        for pair_col in pair_col_candidates:
            if await column_exists(cur, tbl, pair_col):
                suffix = 'query' if 'quer' in tbl else 'response'
                uq_name = f"unique_pair_index_per_thread_{suffix}"
                if not await constraint_exists(cur, uq_name):
                    try:
                        await cur.execute(
                            f"ALTER TABLE {tbl} ADD CONSTRAINT {uq_name} UNIQUE ({thread_fk}, {pair_col})"
                        )
                        print(f"      Re-added UNIQUE {uq_name}")
                    except Exception as e:
                        print(f"      Could not re-add UNIQUE {uq_name}: {e}")
                break


async def step6_rename_pk_columns(cur):
    """Rename PK columns to convention: <table_singular>_id."""
    print("\n== Step 6: Rename PK columns ==")

    # Use current table names
    threads_table = 'conversation_threads' if await table_exists(cur, 'conversation_threads') else 'conversation_thread'
    queries_table = 'conversation_queries' if await table_exists(cur, 'conversation_queries') else 'conversation_query'
    responses_table = 'conversation_responses' if await table_exists(cur, 'conversation_responses') else 'conversation_response'
    usages_table = 'conversation_usages' if await table_exists(cur, 'conversation_usages') else 'conversation_usage'

    renames = [
        (threads_table, 'thread_id', 'conversation_thread_id'),
        (queries_table, 'query_id', 'conversation_query_id'),
        (responses_table, 'response_id', 'conversation_response_id'),
        (usages_table, 'usage_id', 'conversation_usage_id'),
    ]

    for table, old_col, new_col in renames:
        if not await table_exists(cur, table):
            print(f"   {table} does not exist, skipping")
            continue
        if await column_exists(cur, table, old_col) and not await column_exists(cur, table, new_col):
            await cur.execute(f"ALTER TABLE {table} RENAME COLUMN {old_col} TO {new_col}")
            print(f"   Renamed {table}.{old_col} -> {new_col}")
        elif await column_exists(cur, table, new_col):
            print(f"   {table}.{new_col} already exists, skipping")
        else:
            print(f"   {table}.{old_col} not found, skipping")


async def step7_rename_fk_columns(cur):
    """Rename FK columns in child tables to match parent PK names."""
    print("\n== Step 7: Rename FK columns in child tables ==")

    threads_table = 'conversation_threads' if await table_exists(cur, 'conversation_threads') else 'conversation_thread'
    queries_table = 'conversation_queries' if await table_exists(cur, 'conversation_queries') else 'conversation_query'
    responses_table = 'conversation_responses' if await table_exists(cur, 'conversation_responses') else 'conversation_response'
    usages_table = 'conversation_usages' if await table_exists(cur, 'conversation_usages') else 'conversation_usage'

    # Determine current PK column name for threads
    thread_pk = 'conversation_thread_id' if await column_exists(cur, threads_table, 'conversation_thread_id') else 'thread_id'
    # Determine current PK column name for responses
    response_pk = 'conversation_response_id' if await column_exists(cur, responses_table, 'conversation_response_id') else 'response_id'

    # thread_id -> conversation_thread_id in child tables
    fk_renames = [
        (queries_table, 'thread_id', 'conversation_thread_id', threads_table, thread_pk),
        (responses_table, 'thread_id', 'conversation_thread_id', threads_table, thread_pk),
        (usages_table, 'thread_id', 'conversation_thread_id', threads_table, thread_pk),
        (usages_table, 'response_id', 'conversation_response_id', responses_table, response_pk),
    ]

    for table, old_col, new_col, ref_table, ref_col in fk_renames:
        if not await table_exists(cur, table):
            print(f"   {table} does not exist, skipping")
            continue

        if await column_exists(cur, table, old_col) and not await column_exists(cur, table, new_col):
            # Drop FK constraints on old column
            await drop_fk_on_column(cur, table, old_col)
            # Drop unique constraints that involve old column
            await drop_unique_on_columns(cur, table, old_col)

            # Rename column
            await cur.execute(f"ALTER TABLE {table} RENAME COLUMN {old_col} TO {new_col}")
            print(f"   Renamed {table}.{old_col} -> {new_col}")

            # Re-add FK constraint
            fk_name = f"fk_{table}_{new_col}"
            if not await constraint_exists(cur, fk_name):
                try:
                    await cur.execute(
                        f"ALTER TABLE {table} ADD CONSTRAINT {fk_name} "
                        f"FOREIGN KEY ({new_col}) REFERENCES {ref_table}({ref_col}) ON DELETE CASCADE"
                    )
                    print(f"      Added FK {fk_name}")
                except Exception as e:
                    print(f"      Could not add FK {fk_name}: {e}")

            # Re-add UNIQUE constraint where needed (response_id in usages was UNIQUE)
            if old_col == 'response_id':
                uq_name = f"uq_{table}_{new_col}"
                if not await constraint_exists(cur, uq_name):
                    try:
                        await cur.execute(f"ALTER TABLE {table} ADD CONSTRAINT {uq_name} UNIQUE ({new_col})")
                        print(f"      Added UNIQUE {uq_name}")
                    except Exception as e:
                        print(f"      Could not add UNIQUE {uq_name}: {e}")

        elif await column_exists(cur, table, new_col):
            # Ensure FK exists even if column already renamed
            fk_name = f"fk_{table}_{new_col}"
            if not await constraint_exists(cur, fk_name):
                try:
                    await cur.execute(
                        f"ALTER TABLE {table} ADD CONSTRAINT {fk_name} "
                        f"FOREIGN KEY ({new_col}) REFERENCES {ref_table}({ref_col}) ON DELETE CASCADE"
                    )
                    print(f"      Added missing FK {fk_name}")
                except Exception:
                    # May already have FK under different name
                    pass
            print(f"   {table}.{new_col} already exists, skipping rename")
        else:
            print(f"   {table}.{old_col} not found, skipping")

    # Re-add unique constraints on turn_index/pair_index per thread for queries and responses
    for tbl in [queries_table, responses_table]:
        if not await table_exists(cur, tbl):
            continue
        thread_fk_col = 'conversation_thread_id' if await column_exists(cur, tbl, 'conversation_thread_id') else 'thread_id'
        for pair_col in ['turn_index', 'pair_index']:
            if await column_exists(cur, tbl, pair_col):
                suffix = 'query' if 'quer' in tbl else 'response'
                uq_name = f"unique_pair_index_per_thread_{suffix}"
                if not await constraint_exists(cur, uq_name):
                    try:
                        await cur.execute(
                            f"ALTER TABLE {tbl} ADD CONSTRAINT {uq_name} UNIQUE ({thread_fk_col}, {pair_col})"
                        )
                        print(f"      Re-added UNIQUE {uq_name}")
                    except Exception as e:
                        print(f"      Could not re-add UNIQUE {uq_name}: {e}")
                break


async def step8_rename_columns(cur):
    """Rename legacy column names."""
    print("\n== Step 8: Rename other columns ==")

    queries_table = 'conversation_queries' if await table_exists(cur, 'conversation_queries') else 'conversation_query'
    responses_table = 'conversation_responses' if await table_exists(cur, 'conversation_responses') else 'conversation_response'
    usages_table = 'conversation_usages' if await table_exists(cur, 'conversation_usages') else 'conversation_usage'

    # pair_index -> turn_index
    for table in [queries_table, responses_table]:
        if await table_exists(cur, table):
            if await column_exists(cur, table, 'pair_index') and not await column_exists(cur, table, 'turn_index'):
                await cur.execute(f"ALTER TABLE {table} RENAME COLUMN pair_index TO turn_index")
                print(f"   Renamed {table}.pair_index -> turn_index")
            elif await column_exists(cur, table, 'turn_index'):
                print(f"   {table}.turn_index already exists, skipping")

    # streaming_chunks -> sse_events
    if await table_exists(cur, responses_table):
        if await column_exists(cur, responses_table, 'streaming_chunks') and not await column_exists(cur, responses_table, 'sse_events'):
            await cur.execute(f"ALTER TABLE {responses_table} RENAME COLUMN streaming_chunks TO sse_events")
            print(f"   Renamed {responses_table}.streaming_chunks -> sse_events")
        elif await column_exists(cur, responses_table, 'sse_events'):
            print(f"   {responses_table}.sse_events already exists, skipping")

    # timestamp -> created_at (queries, responses)
    for table in [queries_table, responses_table]:
        if await table_exists(cur, table):
            if await column_exists(cur, table, 'timestamp') and not await column_exists(cur, table, 'created_at'):
                await cur.execute(f'ALTER TABLE {table} RENAME COLUMN "timestamp" TO created_at')
                print(f"   Renamed {table}.timestamp -> created_at")
            elif await column_exists(cur, table, 'created_at'):
                print(f"   {table}.created_at already exists, skipping")

    # conversation_usages: special handling -- may have both timestamp and created_at
    if await table_exists(cur, usages_table):
        has_timestamp = await column_exists(cur, usages_table, 'timestamp')
        has_created_at = await column_exists(cur, usages_table, 'created_at')

        if has_timestamp and has_created_at:
            # Both exist -- drop timestamp (created_at is the canonical column)
            await cur.execute(f'ALTER TABLE {usages_table} DROP COLUMN "timestamp"')
            print(f"   Dropped {usages_table}.timestamp (created_at already exists)")
        elif has_timestamp and not has_created_at:
            await cur.execute(f'ALTER TABLE {usages_table} RENAME COLUMN "timestamp" TO created_at')
            print(f"   Renamed {usages_table}.timestamp -> created_at")
        else:
            print(f"   {usages_table} timestamp handling already done, skipping")


async def step9_drop_dead_columns(cur):
    """Drop dead columns from conversation_responses."""
    print("\n== Step 9: Drop dead columns from conversation_responses ==")

    responses_table = 'conversation_responses' if await table_exists(cur, 'conversation_responses') else 'conversation_response'

    if not await table_exists(cur, responses_table):
        print(f"   {responses_table} does not exist, skipping")
        return

    for col in ['state_snapshot', 'agent_messages']:
        if await column_exists(cur, responses_table, col):
            await cur.execute(f"ALTER TABLE {responses_table} DROP COLUMN {col}")
            print(f"   Dropped {responses_table}.{col}")
        else:
            print(f"   {responses_table}.{col} already gone, skipping")


async def step10_rename_remaining_pk_columns(cur):
    """Rename remaining PK columns to standard convention."""
    print("\n== Step 10: Rename remaining PK columns ==")

    portfolios_table = 'user_portfolios' if await table_exists(cur, 'user_portfolios') else 'user_portfolio'
    histories_table = 'redemption_histories' if await table_exists(cur, 'redemption_histories') else 'redemption_history'

    renames = [
        ('user_preferences', 'preference_id', 'user_preference_id'),
        ('watchlist_items', 'item_id', 'watchlist_item_id'),
        (portfolios_table, 'holding_id', 'user_portfolio_id'),
        (histories_table, 'id', 'redemption_id'),
    ]

    for table, old_col, new_col in renames:
        if not await table_exists(cur, table):
            print(f"   {table} does not exist, skipping")
            continue
        if await column_exists(cur, table, old_col) and not await column_exists(cur, table, new_col):
            await cur.execute(f"ALTER TABLE {table} RENAME COLUMN {old_col} TO {new_col}")
            print(f"   Renamed {table}.{old_col} -> {new_col}")
        elif await column_exists(cur, table, new_col):
            print(f"   {table}.{new_col} already exists, skipping")
        else:
            print(f"   {table}.{old_col} not found, skipping")


async def step11_add_check_constraints(cur):
    """Add CHECK constraints for status/type enums."""
    print("\n== Step 11: Add CHECK constraints ==")

    checks = [
        (
            'workspaces', 'chk_workspaces_status',
            "status IN ('creating','running','stopping','stopped','error','deleted','flash')"
        ),
        (
            'conversation_threads', 'chk_threads_status',
            "current_status IN ('in_progress','interrupted','completed','error','cancelled')"
        ),
        (
            'conversation_threads', 'chk_threads_msg_type',
            "msg_type IN ('flash','ptc','chat','deep_thinking','interrupted')"
        ),
        (
            'conversation_responses', 'chk_responses_status',
            "status IN ('in_progress','interrupted','completed','error','cancelled')"
        ),
        (
            'conversation_usages', 'chk_usages_status',
            "status IN ('in_progress','interrupted','completed','error','cancelled')"
        ),
        (
            'conversation_usages', 'chk_usages_msg_type',
            "msg_type IN ('flash','ptc','chat','deep_thinking','interrupted')"
        ),
        (
            'conversation_queries', 'chk_queries_type',
            "type IN ('initial','follow_up','resume_feedback')"
        ),
    ]

    for table, cname, check_expr in checks:
        if not await table_exists(cur, table):
            print(f"   {table} does not exist, skipping {cname}")
            continue
        if await constraint_exists(cur, cname):
            print(f"   {cname} already exists, skipping")
            continue

        try:
            await cur.execute(f"ALTER TABLE {table} ADD CONSTRAINT {cname} CHECK ({check_expr})")
            print(f"   Added {cname}")
        except Exception as e:
            print(f"   Could not add {cname}: {e}")


async def step12_replace_indexes(cur):
    """Drop bloated indexes and add targeted ones."""
    print("\n== Step 12: Drop bloated indexes + add targeted ones ==")

    indexes_to_drop = [
        # conversation_usages: drop 9 of 13
        'idx_usage_user_id',
        'idx_usage_response_id',
        'idx_usage_timestamp',
        'idx_usage_user_credits',
        'idx_usage_msg_type',
        'idx_usage_status',
        'idx_usage_user_msg_type',
        'idx_usage_user_status',
        'idx_usage_token_usage_gin',
        'idx_usage_infrastructure_usage_gin',
        # user_preferences: 4 GIN indexes
        'idx_user_preferences_risk',
        'idx_user_preferences_investment',
        'idx_user_preferences_agent',
        'idx_user_preferences_other',
        'idx_user_preferences_user_id',  # redundant with UNIQUE
        # conversation_responses
        'idx_conversation_response_pair_index',
        'idx_conversation_response_streaming_chunks',
        # conversation_queries
        'idx_conversation_query_pair_index',
        # conversation_threads
        'idx_conversation_thread_workspace_id',  # redundant with UNIQUE
        'idx_conversation_thread_thread_index',  # useless standalone
        'idx_conversation_thread_msg_type',  # low cardinality
        # workspaces
        'idx_workspaces_status',  # low cardinality
    ]

    for idx_name in indexes_to_drop:
        if await index_exists(cur, idx_name):
            await cur.execute(f"DROP INDEX IF EXISTS {idx_name}")
            print(f"   Dropped index {idx_name}")

    print()

    # Add targeted indexes
    new_indexes = [
        "CREATE INDEX IF NOT EXISTS idx_threads_created_at ON conversation_threads(created_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_threads_current_status ON conversation_threads(current_status)",
        "CREATE INDEX IF NOT EXISTS idx_queries_thread_id ON conversation_queries(conversation_thread_id)",
        "CREATE INDEX IF NOT EXISTS idx_queries_created_at ON conversation_queries(created_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_queries_type ON conversation_queries(type)",
        "CREATE INDEX IF NOT EXISTS idx_responses_thread_id ON conversation_responses(conversation_thread_id)",
        "CREATE INDEX IF NOT EXISTS idx_responses_status ON conversation_responses(status)",
        "CREATE INDEX IF NOT EXISTS idx_responses_created_at ON conversation_responses(created_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_usages_user_timestamp ON conversation_usages(user_id, created_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_usages_thread_id ON conversation_usages(conversation_thread_id)",
        "CREATE INDEX IF NOT EXISTS idx_usages_workspace_id ON conversation_usages(workspace_id)",
    ]

    for stmt in new_indexes:
        # Extract table name to check existence
        # Format: ... ON table_name(...)
        table_name = stmt.split(' ON ')[1].split('(')[0].strip()
        if not await table_exists(cur, table_name):
            print(f"   {table_name} does not exist, skipping index")
            continue

        try:
            await cur.execute(stmt)
            idx_name = stmt.split('IF NOT EXISTS ')[1].split(' ON ')[0]
            print(f"   Created index {idx_name}")
        except Exception as e:
            print(f"   Index creation failed: {e}")


async def step13_updated_at_trigger(cur):
    """Create updated_at trigger function and attach to tables."""
    print("\n== Step 13: Create updated_at trigger ==")

    # Create or replace the trigger function
    await cur.execute("""
        CREATE OR REPLACE FUNCTION update_updated_at_column()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = NOW();
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql
    """)
    print("   Created/updated update_updated_at_column() function")

    portfolios_table = 'user_portfolios' if await table_exists(cur, 'user_portfolios') else 'user_portfolio'
    memberships_table = 'memberships' if await table_exists(cur, 'memberships') else 'plans'

    tables = [
        'workspaces',
        'conversation_threads',
        'users',
        'user_preferences',
        'watchlists',
        'watchlist_items',
        portfolios_table,
        memberships_table,
    ]

    for table in tables:
        if not await table_exists(cur, table):
            print(f"   {table} does not exist, skipping trigger")
            continue
        if not await column_exists(cur, table, 'updated_at'):
            print(f"   {table} has no updated_at column, skipping trigger")
            continue

        await cur.execute(f"DROP TRIGGER IF EXISTS update_updated_at ON {table}")
        await cur.execute(
            f"CREATE TRIGGER update_updated_at BEFORE UPDATE ON {table} "
            f"FOR EACH ROW EXECUTE FUNCTION update_updated_at_column()"
        )
        print(f"   Attached trigger to {table}")


async def step14_create_workspace_files(cur):
    """Create workspace_files table for file persistence."""
    print("\n== Step 14: Create workspace_files table ==")

    if await table_exists(cur, 'workspace_files'):
        print("   workspace_files already exists, skipping")
    else:
        await cur.execute("""
            CREATE TABLE workspace_files (
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
        print("   Created workspace_files table")

    # Index
    await cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_workspace_files_workspace_id
        ON workspace_files(workspace_id)
    """)

    # Trigger
    trigger_name = "trg_workspace_files_updated_at"
    if not await trigger_exists(cur, trigger_name):
        await cur.execute(f"""
            CREATE TRIGGER {trigger_name}
                BEFORE UPDATE ON workspace_files
                FOR EACH ROW
                EXECUTE FUNCTION update_updated_at_column()
        """)
        print("   Attached updated_at trigger")


async def step15_verification(cur):
    """Verify final state of the schema."""
    print("\n== Step 14: Verification ==")

    # Tables that should exist
    expected_tables = [
        'workspaces',
        'conversation_threads',
        'conversation_queries',
        'conversation_responses',
        'conversation_usages',
        'users',
        'user_preferences',
        'watchlists',
        'watchlist_items',
        'user_portfolios',
        'memberships',
        'redemption_codes',
        'redemption_histories',
        'workspace_files',
    ]

    # Tables that should NOT exist
    dropped_tables = [
        'workspace_file_operations',
        'workspace_files',
        'workspace_filesystems',
        'plans',
        'conversation_thread',
        'conversation_query',
        'conversation_response',
        'conversation_usage',
        'user_portfolio',
        'redemption_history',
    ]

    print("   Expected tables:")
    all_ok = True
    for table in expected_tables:
        exists = await table_exists(cur, table)
        status = "OK" if exists else "MISSING"
        if not exists:
            all_ok = False
        print(f"      {table}: {status}")

    print("   Dropped tables:")
    for table in dropped_tables:
        exists = await table_exists(cur, table)
        status = "STILL EXISTS" if exists else "OK (gone)"
        if exists:
            all_ok = False
        print(f"      {table}: {status}")

    # Check key UUID types
    print("   UUID column types:")
    uuid_checks = [
        ('conversation_threads', 'conversation_thread_id'),
        ('conversation_queries', 'conversation_query_id'),
        ('conversation_responses', 'conversation_response_id'),
        ('conversation_usages', 'conversation_usage_id'),
    ]
    for table, col in uuid_checks:
        if await table_exists(cur, table) and await column_exists(cur, table, col):
            col_type = await get_column_type(cur, table, col)
            status = "OK" if col_type == 'uuid' else f"WRONG ({col_type})"
            if col_type != 'uuid':
                all_ok = False
            print(f"      {table}.{col}: {status}")
        else:
            print(f"      {table}.{col}: column/table missing")

    if all_ok:
        print("\n   All checks passed!")
    else:
        print("\n   Some checks failed -- review output above")

    return all_ok


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main():
    print("Migration 004: Schema Standardization")
    print("=" * 50)

    # Database connection
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "postgres")
    db_user = os.getenv("DB_USER", "postgres")
    db_password = os.getenv("DB_PASSWORD", "postgres")

    sslmode = "require" if "supabase.com" in db_host else "disable"
    db_uri = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}?sslmode={sslmode}"

    print(f"Database: {db_host}:{db_port}/{db_name}")

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
            print("Connected to database")

            async with pool.connection() as conn:
                async with conn.cursor() as cur:
                    await step1_drop_workspace_file_tables(cur)
                    await step2_rename_plans_to_memberships(cur)
                    await step3_rename_plan_id_fk_columns(cur)
                    await step4_pluralize_table_names(cur)
                    await step5_convert_varchar_pks_to_uuid(cur)
                    await step6_rename_pk_columns(cur)
                    await step7_rename_fk_columns(cur)
                    await step8_rename_columns(cur)
                    await step9_drop_dead_columns(cur)
                    await step10_rename_remaining_pk_columns(cur)
                    await step11_add_check_constraints(cur)
                    await step12_replace_indexes(cur)
                    await step13_updated_at_trigger(cur)
                    await step14_create_workspace_files(cur)
                    ok = await step15_verification(cur)

            print("\nMigration 004 complete.")
            return ok

    except Exception as e:
        print(f"\nMigration error: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
