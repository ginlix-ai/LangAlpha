#!/bin/bash
# Docker entrypoint: run database migrations then start the backend server.
set -e

DB_HOST="${DB_HOST:-postgres}"
DB_PORT="${DB_PORT:-5432}"
DB_USER="${DB_USER:-postgres}"

echo "Waiting for PostgreSQL at ${DB_HOST}:${DB_PORT}..."
until pg_isready -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -q; do
  sleep 1
done

echo "Running database migrations..."
uv run alembic upgrade head

echo "Database ready. Starting server..."
exec uv run python server.py --host 0.0.0.0 --port 8000 --reload
