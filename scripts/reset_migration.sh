#!/bin/bash
# Reset migration tracking and re-run migration for title column

# Connect to PostgreSQL and remove the migration record
docker exec -i ptc-postgres psql -U postgres -d postgres <<EOF
-- Remove the migration record so it can be re-run
DELETE FROM _migrations WHERE name = '001_add_thread_title.sql';
EOF

echo "✅ Migration record removed. Now re-running migration..."

# Re-run the migration
uv run python scripts/migrate.py
