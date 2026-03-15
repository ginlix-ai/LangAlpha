.PHONY: help up down dev dev-web install test test-web test-all lint setup-db migrate deploy deploy-sync

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------------------
# Docker Compose (full-stack)
# ---------------------------------------------------------------------------
up: ## Start full stack (backend + frontend + DB + Redis) via Docker Compose
	docker compose up --build

down: ## Stop all Docker Compose services
	docker compose down

# ---------------------------------------------------------------------------
# Manual development (without Docker for backend/frontend)
# ---------------------------------------------------------------------------
install: ## Install all dependencies (backend + frontend)
	uv sync --group dev --extra test
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

test-web: ## Run frontend unit tests
	cd web && pnpm vitest run

test-all: test test-web ## Run all tests (backend + frontend)

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

# ---------------------------------------------------------------------------
# Deployment (internal — requires deploy.sh, not distributed)
# ---------------------------------------------------------------------------
deploy: ## Deploy (use ARGS= for options, e.g. make deploy ARGS=web)
	./deploy.sh $(ARGS)

deploy-sync: ## Deploy with sync
	./deploy.sh sync
