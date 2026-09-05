"""
Environment variable constants.

These are resolved once at import time from .env / process environment.
No YAML dependency — pure os.getenv.
"""

import os

# Deployment mode: "oss" (self-hosted, no auth) or "platform" (Supabase auth + quota service)
HOST_MODE: str = os.getenv("HOST_MODE", "oss")

# Auth / Login Service (Supabase) — credential, not a mode flag
SUPABASE_URL: str = os.getenv("SUPABASE_URL", "")
LOCAL_DEV_USER_ID: str = os.getenv("AUTH_USER_ID", "local-dev-user")

# Quota / auth enforcement service URL
AUTH_SERVICE_URL: str = os.getenv("AUTH_SERVICE_URL", "")

# TLS mode for the app-data and checkpointer pools. "prefer" negotiates TLS when the
# server offers it and falls back to plaintext when it doesn't — the only value that
# works across the range of Postgres a self-hosted user may bring. The pools log once
# when a session ends up plaintext, so the fallback is visible rather than silent.
# Deployments that must guarantee encryption set DB_SSLMODE=require.
# Read from the environment directly outside src/ (migrations, ops scripts), which
# must run without the application stack importable.
DB_SSLMODE: str = os.getenv("DB_SSLMODE", "prefer")

# Minimum platform access tier required to customize the web-search provider.
# Only enforced in platform mode; OSS deployments are ungated.
SEARCH_PROVIDER_MIN_TIER: int = int(os.getenv("SEARCH_PROVIDER_MIN_TIER", "1"))

# ginlix-data (real-time market data proxy)
GINLIX_DATA_URL: str = os.getenv("GINLIX_DATA_URL", "")
GINLIX_DATA_WS_URL: str = os.getenv("GINLIX_DATA_WS_URL", "") or (
    GINLIX_DATA_URL.replace("http://", "ws://").replace("https://", "wss://")
    if GINLIX_DATA_URL
    else ""
)
GINLIX_DATA_ENABLED: bool = bool(GINLIX_DATA_URL)

# Public base URL of this server (used in agent-generated URLs like preview links)
SERVER_BASE_URL: str = os.getenv("SERVER_BASE_URL", "http://localhost:8000")

# Where this deployment serves its OAuth client metadata document (CIMD). An
# authorization server advertising client_id_metadata_document_supported fetches
# this URL and treats it as the client_id, so there is no registration call and
# no per-user client to store. Must be HTTPS on a non-root path, and the
# document's own client_id must be this same URL — that self-reference is what
# replaces registration. Unset means Dynamic Client Registration, which is what
# every server advertising a registration_endpoint uses.
MCP_CLIENT_METADATA_URL: str = os.getenv("MCP_CLIENT_METADATA_URL", "")

# Internal base URL the server-side PDF renderer (headless Chromium) loads
# workspace HTML from. Must be the server's own loopback listen address so
# Chromium fetches bytes from this process, not the public ingress.
PDF_RENDER_INTERNAL_BASE: str = os.getenv("PDF_RENDER_INTERNAL_BASE", "http://127.0.0.1:8000")

# Shared HS256 secret authenticating sandboxes to the egress relay
# (/v1/egress/*). Empty disables the relay — OAuth-connected MCP servers
# then have no execution path, but nothing else is affected.
EGRESS_RELAY_SECRET: str = os.getenv("EGRESS_RELAY_SECRET", "")

# Base URL sandboxes use to reach the egress relay (the generated client
# appends /v1/egress/{grant_id}). Sandboxes are remote (or in OSS Docker, on a
# different network), so this must be a sandbox-reachable address. Empty means
# unconfigured: services/egress/reachability.py then falls back to the server
# base, adapted per sandbox provider (Docker host gateway) or warned about
# (Daytona + a local address). Point it at the API origin, not an SPA-fallback
# frontend.
EGRESS_RELAY_BASE_URL: str = os.getenv("EGRESS_RELAY_BASE_URL", "")

# Credit conversion rate (USD → credits).  Override with USD_TO_CREDITS_RATE env var.
USD_TO_CREDITS_RATE: int = int(os.getenv("USD_TO_CREDITS_RATE", "1000"))

# Automation webhook delivery (channel gateway)
AUTOMATION_WEBHOOK_URL: str = os.getenv("AUTOMATION_WEBHOOK_URL", "")
AUTOMATION_WEBHOOK_SECRET: str = os.getenv("AUTOMATION_WEBHOOK_SECRET", "")

# Host IP for local LLM providers (Ollama, LM Studio, vLLM).
# In Docker, "localhost" means the container — use host.docker.internal to reach the host.
_IN_DOCKER: bool = os.path.exists("/.dockerenv")
HOST_IP: str = os.getenv("HOST_IP", "host.docker.internal" if _IN_DOCKER else "localhost")
