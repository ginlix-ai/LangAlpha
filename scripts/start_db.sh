#!/bin/bash
# Start PostgreSQL 18 and Redis in Docker for local development

set -e

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Load .env file if it exists
if [ -f "$PROJECT_ROOT/.env" ]; then
    set -a
    source "$PROJECT_ROOT/.env"
    set +a
fi

# Configuration (use environment variables with defaults)
POSTGRES_CONTAINER="langalpha-postgresql"
POSTGRES_IMAGE="postgres:18"
POSTGRES_PORT="${DB_PORT:-5432}"
POSTGRES_USER="${DB_USER:-postgres}"
POSTGRES_PASSWORD="${DB_PASSWORD:-postgres}"
POSTGRES_DB="${DB_NAME:-langalpha}"
POSTGRES_VOLUME="langalpha-postgresql-data"

REDIS_CONTAINER="langalpha-redis"
REDIS_IMAGE="redis:7-alpine"
REDIS_PORT="${REDIS_PORT:-6379}"
REDIS_PASSWORD="${REDIS_PASSWORD:-redis}"
REDIS_VOLUME="langalpha-redis-data"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check container status
# Returns: "running", "stopped", or "not_found"
check_container_status() {
    local container_name=$1

    if docker ps --format '{{.Names}}' | grep -q "^${container_name}$"; then
        echo "running"
    elif docker ps -a --format '{{.Names}}' | grep -q "^${container_name}$"; then
        echo "stopped"
    else
        echo "not_found"
    fi
}

# Start or create PostgreSQL container
start_or_create_postgres() {
    local status=$(check_container_status "$POSTGRES_CONTAINER")

    case $status in
        "running")
            log_info "PostgreSQL container '$POSTGRES_CONTAINER' is already running"
            ;;
        "stopped")
            log_info "Starting existing PostgreSQL container '$POSTGRES_CONTAINER'..."
            docker start "$POSTGRES_CONTAINER"
            ;;
        "not_found")
            log_info "Creating and starting PostgreSQL container '$POSTGRES_CONTAINER'..."
            docker run -d \
                --name "$POSTGRES_CONTAINER" \
                -e POSTGRES_USER="$POSTGRES_USER" \
                -e POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
                -e POSTGRES_DB="$POSTGRES_DB" \
                -p "$POSTGRES_PORT:5432" \
                -v "$POSTGRES_VOLUME:/var/lib/postgresql" \
                "$POSTGRES_IMAGE"
            ;;
    esac
}

# Start or create Redis container
start_or_create_redis() {
    local status=$(check_container_status "$REDIS_CONTAINER")

    case $status in
        "running")
            log_info "Redis container '$REDIS_CONTAINER' is already running"
            ;;
        "stopped")
            log_info "Starting existing Redis container '$REDIS_CONTAINER'..."
            docker start "$REDIS_CONTAINER"
            ;;
        "not_found")
            log_info "Creating and starting Redis container '$REDIS_CONTAINER'..."
            docker run -d \
                --name "$REDIS_CONTAINER" \
                -p "$REDIS_PORT:6379" \
                -v "$REDIS_VOLUME:/data" \
                "$REDIS_IMAGE" \
                redis-server --requirepass "$REDIS_PASSWORD"
            ;;
    esac
}

# Wait for PostgreSQL to be ready
wait_for_postgres() {
    log_info "Waiting for PostgreSQL to be ready..."
    local max_attempts=30
    local attempt=1

    while [ $attempt -le $max_attempts ]; do
        if docker exec "$POSTGRES_CONTAINER" pg_isready -U "$POSTGRES_USER" -q 2>/dev/null; then
            log_info "PostgreSQL is ready"
            return 0
        fi
        echo -n "."
        sleep 1
        attempt=$((attempt + 1))
    done

    echo ""
    log_error "PostgreSQL failed to become ready after $max_attempts seconds"
    return 1
}

# Wait for Redis to be ready
wait_for_redis() {
    log_info "Waiting for Redis to be ready..."
    local max_attempts=30
    local attempt=1

    while [ $attempt -le $max_attempts ]; do
        if docker exec "$REDIS_CONTAINER" redis-cli -a "$REDIS_PASSWORD" ping 2>/dev/null | grep -q "PONG"; then
            log_info "Redis is ready"
            return 0
        fi
        echo -n "."
        sleep 1
        attempt=$((attempt + 1))
    done

    echo ""
    log_error "Redis failed to become ready after $max_attempts seconds"
    return 1
}

# Ensure the target database exists (POSTGRES_DB only works on first container init)
ensure_database() {
    log_info "Ensuring database '$POSTGRES_DB' exists..."
    docker exec "$POSTGRES_CONTAINER" psql -U "$POSTGRES_USER" -tc \
        "SELECT 1 FROM pg_database WHERE datname = '$POSTGRES_DB'" | grep -q 1 \
        && log_info "Database '$POSTGRES_DB' already exists" \
        || {
            log_info "Creating database '$POSTGRES_DB'..."
            docker exec "$POSTGRES_CONTAINER" psql -U "$POSTGRES_USER" -c "CREATE DATABASE \"$POSTGRES_DB\""
        }
}

# Setup database tables
setup_tables() {
    log_info "Running database migrations..."
    uv run alembic upgrade head
    log_info "Database setup complete"
}

# Main execution
main() {
    log_info "Starting database services..."
    echo ""

    # Start PostgreSQL
    start_or_create_postgres

    # Start Redis
    start_or_create_redis

    echo ""

    # Wait for services to be ready
    wait_for_postgres
    wait_for_redis

    echo ""

    # Ensure database exists, then setup tables
    ensure_database
    setup_tables

    echo ""
    log_info "All services are running!"
    echo ""
    echo "PostgreSQL: localhost:$POSTGRES_PORT (user: $POSTGRES_USER, password: $POSTGRES_PASSWORD, database: $POSTGRES_DB)"
    echo "Redis:      localhost:$REDIS_PORT (redis://:$REDIS_PASSWORD@localhost:$REDIS_PORT/0)"
}

main
