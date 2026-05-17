.PHONY: help configup down clean dev dev-web install test test-sandbox test-web test-all lint setup-db migrate

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

config: ## Interactive setup wizard (LLM, data, sandbox, search)
	@bash scripts/configure.sh
# ---------------------------------------------------------------------------
# Docker Compose (full-stack)
# ---------------------------------------------------------------------------
up: _sandbox-prepare ## Start full stack (PROVIDER=docker|daytona)
	SANDBOX_PROVIDER=$(PROVIDER) docker compose up --build

down: ## Stop all Docker Compose services
	docker compose down

clean: down ## Stop everything and remove stale sandbox containers
	@echo "Removing stale sandbox containers..."
	@docker rm -f $$(docker ps -aq --filter "name=langalpha-sandbox") 2>/dev/null || true
	@echo "Clean."

# Build sandbox image only when provider needs it (docker)
_sandbox-prepare:
ifeq ($(PROVIDER),docker)
	@echo "Building sandbox image for docker provider..."
	docker build -f Dockerfile.sandbox -t langalpha-sandbox:latest .
endif

# ---------------------------------------------------------------------------
# Manual development (without Docker for backend/frontend)
# ---------------------------------------------------------------------------
install: ## Install all dependencies (backend + frontend; observability extra if .env enables OTel)
	@if [ -f .env ] && grep -qE "^OTEL_EXPORTER_OTLP_ENDPOINT=." .env; then \
		echo "→ OTEL_EXPORTER_OTLP_ENDPOINT set in .env — installing observability extra"; \
		uv sync --group dev --extra test --extra observability; \
	else \
		echo "→ OTel endpoint unset — skipping observability extra (set OTEL_EXPORTER_OTLP_ENDPOINT in .env to enable)"; \
		uv sync --group dev --extra test; \
	fi
	cd web && pnpm install

dev: ## Start backend with hot-reload (requires DB + Redis running)
	uv run python server.py --reload

dev-web: ## Start frontend dev server
	cd web && pnpm dev

# ---------------------------------------------------------------------------
# Testing
# ---------------------------------------------------------------------------
test: ## Run backend unit tests
	uv run pytest tests/unit/ -v --tb=short

test-sandbox: _sandbox-prepare ## Run sandbox integration tests (PROVIDER=memory|docker|daytona)
	SANDBOX_TEST_PROVIDER=$(PROVIDER) uv run pytest tests/integration/sandbox/ -v --tb=short

test-web: ## Run frontend unit tests
	cd web && pnpm vitest run

test-all: test test-web ## Run all tests (backend + frontend)
	$(MAKE) test-sandbox PROVIDER=memory

# ---------------------------------------------------------------------------
# Linting
# ---------------------------------------------------------------------------
lint: ## Run all linters (Ruff + ESLint)
	uv run ruff check src/
	cd web && pnpm lint

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
setup-db: ## Start PostgreSQL + Redis in Docker and initialize tables
	./scripts/start_db.sh

migrate: ## Run database migrations
	uv run alembic upgrade head
