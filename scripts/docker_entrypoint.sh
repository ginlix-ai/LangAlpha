#!/bin/bash
# Docker entrypoint: wait for database, run migrations, then exec CMD.
# Used by both dev (docker-compose.yml) and prod (docker-compose.prod.yml).
set -e

DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-5432}"
DB_USER="${DB_USER:-postgres}"

echo "Waiting for PostgreSQL at ${DB_HOST}:${DB_PORT}..."
until pg_isready -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -q; do
  sleep 1
done

echo "Running database migrations..."
uv run alembic upgrade head

echo "Database ready."
exec "$@"
