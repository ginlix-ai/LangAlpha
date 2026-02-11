#!/usr/bin/env python3
"""
Database migration runner.

Runs SQL migration files from scripts/migrations/ in order.
Tracks applied migrations in a migrations table.

Usage:
    uv run python scripts/migrate.py
"""

import os
import sys
import asyncio
from pathlib import Path
from dotenv import load_dotenv

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Load environment variables
load_dotenv(project_root / ".env")

from psycopg_pool import AsyncConnectionPool
from psycopg.rows import dict_row


MIGRATIONS_DIR = Path(__file__).parent / "migrations"


async def run_migrations():
    """Run all pending migrations."""
    print("🔄 Running database migrations...")

    # Get database configuration
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "postgres")
    db_user = os.getenv("DB_USER", "postgres")
    db_password = os.getenv("DB_PASSWORD", "postgres")

    sslmode = "require" if "supabase.com" in db_host else "disable"
    db_uri = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}?sslmode={sslmode}"

    print(f"📊 Database: {db_host}:{db_port}/{db_name}")

    connection_kwargs = {
        "autocommit": True,
        "prepare_threshold": 0,
        "row_factory": dict_row
    }

    try:
        async with AsyncConnectionPool(
            conninfo=db_uri,
            min_size=1,
            max_size=1,
            kwargs=connection_kwargs
        ) as pool:
            await pool.wait()
            print("✅ Connected to database")

            async with pool.connection() as conn:
                async with conn.cursor() as cur:
                    # Create migrations tracking table if not exists
                    await cur.execute("""
                        CREATE TABLE IF NOT EXISTS _migrations (
                            id SERIAL PRIMARY KEY,
                            name VARCHAR(255) NOT NULL UNIQUE,
                            applied_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                        )
                    """)

                    # Get already applied migrations
                    await cur.execute("SELECT name FROM _migrations ORDER BY name")
                    applied = {row['name'] for row in await cur.fetchall()}

                    # Get migration files
                    if not MIGRATIONS_DIR.exists():
                        print("⚠️  No migrations directory found")
                        return True

                    migration_files = sorted(MIGRATIONS_DIR.glob("*.sql"))

                    if not migration_files:
                        print("ℹ️  No migration files found")
                        return True

                    # Run pending migrations
                    pending = [f for f in migration_files if f.name not in applied]

                    if not pending:
                        print("✅ All migrations already applied")
                        return True

                    for migration_file in pending:
                        print(f"\n📝 Applying: {migration_file.name}")

                        sql = migration_file.read_text()

                        try:
                            # Split SQL into individual statements, respecting
                            # parenthesized blocks (e.g. CREATE TABLE (...;)).
                            # Only split on ';' at top-level (depth == 0).
                            statements = []
                            buf = []
                            depth = 0
                            for line in sql.splitlines():
                                stripped = line.strip()
                                if not stripped or stripped.startswith('--'):
                                    continue
                                depth += stripped.count('(') - stripped.count(')')
                                if stripped.endswith(';') and depth <= 0:
                                    buf.append(stripped[:-1])  # drop trailing ;
                                    stmt = ' '.join(buf).strip()
                                    if stmt:
                                        statements.append(stmt)
                                    buf = []
                                    depth = 0
                                else:
                                    buf.append(stripped)
                            # Catch trailing statement without semicolon
                            trailing = ' '.join(buf).strip()
                            if trailing:
                                statements.append(trailing)

                            for stmt in statements:
                                await cur.execute(stmt)
                            await cur.execute(
                                "INSERT INTO _migrations (name) VALUES (%s)",
                                (migration_file.name,)
                            )
                            print(f"   ✅ Applied successfully")
                        except Exception as e:
                            print(f"   ❌ Failed: {e}")
                            return False

                    print(f"\n🎉 Applied {len(pending)} migration(s)")
                    return True

    except Exception as e:
        print(f"\n❌ Migration error: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = asyncio.run(run_migrations())
    sys.exit(0 if success else 1)
