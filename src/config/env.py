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

# Access tokens outside apps get from an OAuth issuer. Off unless all three
# are set; see src/server/auth/oauth_resource.py for what accepts them.
OAUTH_RESOURCE_ISSUER: str = os.getenv("OAUTH_RESOURCE_ISSUER", "").strip()
OAUTH_RESOURCE_JWKS_URL: str = os.getenv("OAUTH_RESOURCE_JWKS_URL", "").strip()
OAUTH_RESOURCE_AUDIENCES: tuple[str, ...] = tuple(
    aud.strip()
    for aud in os.getenv("OAUTH_RESOURCE_AUDIENCE", "").split(",")
    if aud.strip()
)

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

# Where a sandbox reaches the platform's public API, ginlix-data included.
# Sandboxes are remote, so this stays publicly reachable even when this server
# reaches the same services over a private address; unset, they use
# GINLIX_DATA_URL as this server does.
PUBLIC_API_BASE_URL: str = os.getenv("PUBLIC_API_BASE_URL", "").rstrip("/")

# Public base URL of this server (used in agent-generated URLs like preview links)
SERVER_BASE_URL: str = os.getenv("SERVER_BASE_URL", "http://localhost:8000")

# Where the web app is served, for links the agent hands to a person
# (``/a/<code>``). Defaults to the API origin, which is where the app lives
# unless the frontend is served from its own host.
PUBLIC_APP_URL: str = (os.getenv("PUBLIC_APP_URL", "") or SERVER_BASE_URL).rstrip("/")

# Where this server serves previews that a browser cannot open directly (a
# private E2B computer's ports): a scheme and host, each preview a subdomain
# of it. Unset turns the preview proxy off.
PREVIEW_PROXY_BASE_URL: str = os.getenv("PREVIEW_PROXY_BASE_URL", "").strip()

# Origins allowed to frame a proxied preview, as a CSP frame-ancestors source
# list. Unset sends none, which leaves framing to the preview itself.
PREVIEW_PROXY_FRAME_ANCESTORS: str = os.getenv("PREVIEW_PROXY_FRAME_ANCESTORS", "").strip()

# Where this deployment serves its OAuth client metadata document (CIMD). An
# authorization server advertising client_id_metadata_document_supported fetches
# this URL and treats it as the client_id, so there is no registration call and
# no per-user client to store. Must be HTTPS on a non-root path, and the
# document's own client_id must be this same URL — that self-reference is what
# replaces registration. Unset means Dynamic Client Registration, which is what
# every server advertising a registration_endpoint uses.
MCP_CLIENT_METADATA_URL: str = os.getenv("MCP_CLIENT_METADATA_URL", "")

# The catalog the Plugins page's Discover tab lists: a forge repository whose
# marketplace.json indexes the plugins it offers. Which catalog a deployment
# offers its users is that deployment's choice, so none is assumed: unset,
# Discover is off and the rest of the Plugins page works the same. Any URL a
# plugin install accepts works, so a /tree/<ref> form pins a branch; a private
# catalog also needs PLUGIN_MARKETPLACE_TOKEN.
PLUGIN_MARKETPLACE_URL: str = os.getenv("PLUGIN_MARKETPLACE_URL", "")

# A read-only GitHub token, so a team can point Discover at its own private
# catalog. Sent to api.github.com only, only when fetching the marketplace's
# own repository (the listing, and installs and updates of what it lists), and
# never along a redirect: services/plugins/fetch.py's _first_hop is the one
# place that sends it.
PLUGIN_MARKETPLACE_TOKEN: str = os.getenv("PLUGIN_MARKETPLACE_TOKEN", "")

# Internal base URL the server-side PDF renderer (headless Chromium) loads
# workspace HTML from. Must be the server's own loopback listen address so
# Chromium fetches bytes from this process, not the public ingress.
PDF_RENDER_INTERNAL_BASE: str = os.getenv("PDF_RENDER_INTERNAL_BASE", "http://127.0.0.1:8000")

# Shared HS256 secret authenticating sandboxes to the egress relay
# (/v1/egress/*). Empty disables the relay — OAuth-connected MCP servers
# then have no execution path, but nothing else is affected.
EGRESS_RELAY_SECRET: str = os.getenv("EGRESS_RELAY_SECRET", "")

# Base URL sandboxes use to reach this backend: the egress relay (the generated
# client appends /v1/egress/{grant_id}) and the file mount that serves memory,
# profile, automations and workflows to sandbox code, plus memos and
# transcripts read-only (/api/v1/livefs/). Sandboxes are remote
# (or in OSS Docker, on a different network), so this must be a
# sandbox-reachable address. Empty means
# unconfigured: services/egress/reachability.py then falls back to the server
# base, adapted per sandbox provider (Docker host gateway) or warned about
# (Daytona + a local address). Point it at the API origin, not an SPA-fallback
# frontend.
EGRESS_RELAY_BASE_URL: str = os.getenv("EGRESS_RELAY_BASE_URL", "")

# Where THIS process reaches its own egress relay for directly bound MCP
# tools. Loopback on purpose: the host-side call takes the same route, policy
# and audit as a sandbox call, without leaving the box.
EGRESS_RELAY_LOOPBACK_URL: str = os.getenv("EGRESS_RELAY_LOOPBACK_URL", "http://127.0.0.1:8000")

# Credit conversion rate (USD → credits).  Override with USD_TO_CREDITS_RATE env var.
USD_TO_CREDITS_RATE: int = int(os.getenv("USD_TO_CREDITS_RATE", "1000"))

# Automation webhook delivery (channel gateway)
AUTOMATION_WEBHOOK_URL: str = os.getenv("AUTOMATION_WEBHOOK_URL", "")
AUTOMATION_WEBHOOK_SECRET: str = os.getenv("AUTOMATION_WEBHOOK_SECRET", "")

# Base URL of a channel gateway's API, path prefix included. Set together with
# INTERNAL_SERVICE_TOKEN, it gives the agent send_message and
# list_message_targets, which deliver to the messaging channels connected to
# the user's account. Unset, neither tool exists.
CHANNEL_GATEWAY_URL: str = os.getenv("CHANNEL_GATEWAY_URL", "").strip().rstrip("/")

# Host IP for local LLM providers (Ollama, LM Studio, vLLM).
# In Docker, "localhost" means the container — use host.docker.internal to reach the host.
_IN_DOCKER: bool = os.path.exists("/.dockerenv")
HOST_IP: str = os.getenv("HOST_IP", "host.docker.internal" if _IN_DOCKER else "localhost")
