#!/usr/bin/env python3
"""
Setup script for initializing database tables in PostgreSQL.

This script creates the schema for storing workspaces, threads, queries,
responses, usage tracking, and filesystem state.

Tables created:
- workspaces: Workspace management with Daytona sandbox mapping
- conversation_thread: Workflow execution threads (linked to workspaces)
- conversation_query: User queries with pair_index
- conversation_response: System responses with state snapshots
- conversation_usage: Usage tracking (tokens, infrastructure, credits)
- workspace_filesystems: Filesystem state per workspace
- workspace_files: Files within filesystem (current state only)
- workspace_file_operations: File operation audit trail

Usage:
    uv run python scripts/db/setup_conversation_tables.py
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


async def setup_query_response_tables_async():
    """Initialize query-response logging tables in PostgreSQL."""

    print("🔧 Setting up query-response logging tables...")
    print("⚠️  WARNING: This will DROP the legacy conversations and conversation_threads tables!")

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
                    # Drop legacy tables (no backward compatibility)
                    print("\n🗑️  Dropping legacy tables...")
                    await cur.execute("DROP TABLE IF EXISTS conversation_threads CASCADE;")
                    await cur.execute("DROP TABLE IF EXISTS conversations CASCADE;")
                    await cur.execute("DROP TABLE IF EXISTS conversation_history CASCADE;")
                    await cur.execute("DROP TABLE IF EXISTS conversation_filesystems CASCADE;")
                    await cur.execute("DROP TABLE IF EXISTS conversation_files CASCADE;")
                    await cur.execute("DROP TABLE IF EXISTS conversation_file_operations CASCADE;")
                    # Drop existing conversation_thread, conversation_query, conversation_response, conversation_usage
                    # tables to recreate with new schema (workspace_id instead of conversation_id)
                    await cur.execute("DROP TABLE IF EXISTS conversation_usage CASCADE;")
                    await cur.execute("DROP TABLE IF EXISTS conversation_response CASCADE;")
                    await cur.execute("DROP TABLE IF EXISTS conversation_query CASCADE;")
                    await cur.execute("DROP TABLE IF EXISTS conversation_thread CASCADE;")
                    print("✅ Legacy tables dropped!")

                    # Create workspaces table FIRST (referenced by other tables)
                    print("\n📝 Creating 'workspaces' table...")
                    await cur.execute("""
                        CREATE TABLE IF NOT EXISTS workspaces (
                            workspace_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                            user_id VARCHAR(255) NOT NULL,
                            name VARCHAR(255) NOT NULL,
                            description TEXT,

                            -- Sandbox reference (Daytona)
                            sandbox_id VARCHAR(255),

                            -- Lifecycle state
                            -- Values: creating, running, stopping, stopped, error, deleted, flash
                            status VARCHAR(50) NOT NULL DEFAULT 'creating',

                            -- Timestamps
                            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                            last_activity_at TIMESTAMPTZ,
                            stopped_at TIMESTAMPTZ,

                            -- Configuration (flexible)
                            config JSONB DEFAULT '{}'::jsonb
                        );
                    """)

                    print("   Creating indexes on 'workspaces'...")
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_workspaces_user_id
                        ON workspaces(user_id);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_workspaces_status
                        ON workspaces(status);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_workspaces_user_status
                        ON workspaces(user_id, status);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_workspaces_updated_at
                        ON workspaces(updated_at DESC);
                    """)
                    print("✅ 'workspaces' table created!")

                    # Create thread table (now references workspaces instead of conversation_history)
                    print("\n📝 Creating 'conversation_thread' table...")
                    await cur.execute("""
                        CREATE TABLE IF NOT EXISTS conversation_thread (
                            thread_id VARCHAR(255) PRIMARY KEY,
                            workspace_id UUID NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
                            msg_type VARCHAR(50),
                            current_status VARCHAR(50) NOT NULL,  -- Status values: in_progress, interrupted, completed, error, cancelled, timeout
                            thread_index INTEGER NOT NULL,
                            title VARCHAR(255),  -- User-editable thread title
                            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                            CONSTRAINT unique_thread_index_per_workspace UNIQUE (workspace_id, thread_index)
                        );
                    """)

                    # Create indexes for thread
                    print("   Creating indexes on 'conversation_thread'...")
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_conversation_thread_workspace_id
                        ON conversation_thread(workspace_id);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_conversation_thread_thread_index
                        ON conversation_thread(thread_index);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_conversation_thread_created_at
                        ON conversation_thread(created_at DESC);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_conversation_thread_current_status
                        ON conversation_thread(current_status);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_conversation_thread_msg_type
                        ON conversation_thread(msg_type);
                    """)
                    print("✅ 'conversation_thread' table created!")

                    # Create query table
                    print("\n📝 Creating 'conversation_query' table...")
                    await cur.execute("""
                        CREATE TABLE IF NOT EXISTS conversation_query (
                            query_id VARCHAR(255) PRIMARY KEY,
                            thread_id VARCHAR(255) NOT NULL REFERENCES conversation_thread(thread_id) ON DELETE CASCADE,
                            pair_index INTEGER NOT NULL,
                            content TEXT,
                            type VARCHAR(50) NOT NULL,
                            feedback_action TEXT,
                            metadata JSONB DEFAULT '{}'::jsonb,
                            timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                            CONSTRAINT unique_pair_index_per_thread_query UNIQUE (thread_id, pair_index)
                        );
                    """)

                    # Create indexes for query
                    print("   Creating indexes on 'conversation_query'...")
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_conversation_query_thread_id
                        ON conversation_query(thread_id);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_conversation_query_pair_index
                        ON conversation_query(pair_index);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_conversation_query_timestamp
                        ON conversation_query(timestamp DESC);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_conversation_query_type
                        ON conversation_query(type);
                    """)
                    print("✅ 'conversation_query' table created!")

                    # Create response table
                    print("\n📝 Creating 'conversation_response' table...")
                    await cur.execute("""
                        CREATE TABLE IF NOT EXISTS conversation_response (
                            response_id VARCHAR(255) PRIMARY KEY,
                            thread_id VARCHAR(255) NOT NULL REFERENCES conversation_thread(thread_id) ON DELETE CASCADE,
                            pair_index INTEGER NOT NULL,
                            status VARCHAR(50) NOT NULL,
                            interrupt_reason VARCHAR(100),
                            agent_messages JSONB,
                            metadata JSONB DEFAULT '{}'::jsonb,
                            state_snapshot JSONB,
                            warnings TEXT[],
                            errors TEXT[],
                            execution_time FLOAT,
                            timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                            streaming_chunks JSONB,
                            CONSTRAINT unique_pair_index_per_thread_response UNIQUE (thread_id, pair_index)
                        );
                    """)

                    # Create indexes for response
                    print("   Creating indexes on 'conversation_response'...")
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_conversation_response_thread_id
                        ON conversation_response(thread_id);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_conversation_response_pair_index
                        ON conversation_response(pair_index);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_conversation_response_status
                        ON conversation_response(status);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_conversation_response_timestamp
                        ON conversation_response(timestamp DESC);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_conversation_response_streaming_chunks
                        ON conversation_response USING GIN (streaming_chunks);
                    """)
                    print("✅ 'conversation_response' table created!")

                    # Create usage table
                    print("\n📝 Creating 'conversation_usage' table...")
                    await cur.execute("""
                        CREATE TABLE IF NOT EXISTS conversation_usage (
                            usage_id VARCHAR(255) PRIMARY KEY DEFAULT gen_random_uuid()::text,
                            response_id VARCHAR(255) UNIQUE NOT NULL REFERENCES conversation_response(response_id) ON DELETE CASCADE,

                            -- Denormalized fields for fast user-level queries
                            user_id VARCHAR(255) NOT NULL,
                            thread_id VARCHAR(255) NOT NULL REFERENCES conversation_thread(thread_id) ON DELETE CASCADE,
                            workspace_id UUID NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE,

                            -- Workflow metadata
                            msg_type VARCHAR(50) NOT NULL DEFAULT 'chat',
                            status VARCHAR(50) NOT NULL,

                            -- Usage data
                            token_usage JSONB,
                            infrastructure_usage JSONB,

                            -- Credit breakdown
                            token_credits DECIMAL(10, 6) NOT NULL DEFAULT 0,
                            infrastructure_credits DECIMAL(10, 6) NOT NULL DEFAULT 0,
                            total_credits DECIMAL(10, 6) NOT NULL DEFAULT 0,

                            -- Timestamps
                            timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                        );
                    """)

                    # Create indexes for usage
                    print("   Creating indexes on 'conversation_usage'...")
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_usage_user_id
                        ON conversation_usage(user_id);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_usage_user_timestamp
                        ON conversation_usage(user_id, timestamp DESC);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_usage_response_id
                        ON conversation_usage(response_id);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_usage_thread_id
                        ON conversation_usage(thread_id);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_usage_workspace_id
                        ON conversation_usage(workspace_id);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_usage_timestamp
                        ON conversation_usage(timestamp DESC);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_usage_user_credits
                        ON conversation_usage(user_id, timestamp DESC, total_credits);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_usage_msg_type
                        ON conversation_usage(msg_type);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_usage_status
                        ON conversation_usage(status);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_usage_user_msg_type
                        ON conversation_usage(user_id, msg_type, timestamp DESC);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_usage_user_status
                        ON conversation_usage(user_id, status, timestamp DESC);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_usage_token_usage_gin
                        ON conversation_usage USING GIN (token_usage);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_usage_infrastructure_usage_gin
                        ON conversation_usage USING GIN (infrastructure_usage);
                    """)
                    print("✅ 'conversation_usage' table created!")

                    # Create filesystem tables (now linked to workspaces)
                    print("\n📝 Creating 'workspace_filesystems' table...")
                    await cur.execute("""
                        CREATE TABLE IF NOT EXISTS workspace_filesystems (
                            filesystem_id VARCHAR(255) PRIMARY KEY,
                            workspace_id UUID UNIQUE NOT NULL REFERENCES workspaces(workspace_id) ON DELETE CASCADE,
                            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                        );
                    """)

                    print("   Creating indexes on 'workspace_filesystems'...")
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_filesystems_workspace
                        ON workspace_filesystems(workspace_id);
                    """)
                    print("✅ 'workspace_filesystems' table created!")

                    print("\n📝 Creating 'workspace_files' table...")
                    await cur.execute("""
                        CREATE TABLE IF NOT EXISTS workspace_files (
                            file_id VARCHAR(255) PRIMARY KEY,
                            filesystem_id VARCHAR(255) NOT NULL REFERENCES workspace_filesystems(filesystem_id) ON DELETE CASCADE,
                            file_path TEXT NOT NULL,
                            content TEXT,
                            line_count INTEGER,
                            created_in_thread_id VARCHAR(255),
                            created_in_pair_index INTEGER,
                            updated_in_thread_id VARCHAR(255),
                            updated_in_pair_index INTEGER,
                            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                            CONSTRAINT unique_file_path_per_filesystem UNIQUE(filesystem_id, file_path)
                        );
                    """)

                    print("   Creating indexes on 'workspace_files'...")
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_files_filesystem
                        ON workspace_files(filesystem_id);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_files_path
                        ON workspace_files(filesystem_id, file_path);
                    """)
                    print("✅ 'workspace_files' table created!")

                    print("\n📝 Creating 'workspace_file_operations' table...")
                    await cur.execute("""
                        CREATE TABLE IF NOT EXISTS workspace_file_operations (
                            operation_id VARCHAR(255) PRIMARY KEY,
                            file_id VARCHAR(255) NOT NULL REFERENCES workspace_files(file_id) ON DELETE CASCADE,
                            operation VARCHAR(50) NOT NULL,
                            thread_id VARCHAR(255) NOT NULL,
                            pair_index INTEGER NOT NULL,
                            agent VARCHAR(100),
                            tool_call_id VARCHAR(255),
                            old_string TEXT,
                            new_string TEXT,
                            timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                            operation_index INTEGER NOT NULL
                        );
                    """)

                    print("   Creating indexes on 'workspace_file_operations'...")
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_operations_file
                        ON workspace_file_operations(file_id, timestamp);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_operations_file_index
                        ON workspace_file_operations(file_id, operation_index);
                    """)
                    await cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_operations_thread
                        ON workspace_file_operations(thread_id, pair_index);
                    """)
                    print("✅ 'workspace_file_operations' table created!")

                    # Verify tables exist
                    print("\n🔍 Verifying tables...")
                    await cur.execute("""
                        SELECT table_name
                        FROM information_schema.tables
                        WHERE table_schema = 'public'
                        AND table_name IN (
                            'workspaces',
                            'conversation_thread',
                            'conversation_query',
                            'conversation_response',
                            'conversation_usage',
                            'workspace_filesystems',
                            'workspace_files',
                            'workspace_file_operations'
                        )
                        ORDER BY table_name;
                    """)

                    tables = await cur.fetchall()
                    print(f"   Found {len(tables)} tables:")
                    for table in tables:
                        print(f"     ✓ {table['table_name']}")

            print("\n🎉 Setup complete! Database tables are ready.")
            print("\n📋 Schema Summary:")
            print("   • workspaces: Workspace management with Daytona sandbox mapping")
            print("   • conversation_thread: Workflow execution threads (linked to workspaces)")
            print("   • conversation_query: User queries with pair_index")
            print("   • conversation_response: System responses with state snapshots")
            print("   • conversation_usage: Usage tracking (tokens, infrastructure, credits)")
            print("   • workspace_filesystems: Filesystem state per workspace")
            print("   • workspace_files: Files within filesystem")
            print("   • workspace_file_operations: File operation audit trail")
            print("\n⚠️  Note: Legacy tables (conversation_history, conversation_filesystems, etc.) have been dropped.")
            return True

    except Exception as e:
        print(f"\n❌ Error during setup: {e}")
        print("\nPlease check:")
        print("  1. Database credentials in .env file are correct")
        print("  2. Database server is accessible (SSH tunnel if needed)")
        print("  3. User has permission to create/drop tables")
        import traceback
        traceback.print_exc()
        return False


def setup_query_response_tables():
    """Synchronous wrapper for async setup function."""
    return asyncio.run(setup_query_response_tables_async())


if __name__ == "__main__":
    success = setup_query_response_tables()
    sys.exit(0 if success else 1)
