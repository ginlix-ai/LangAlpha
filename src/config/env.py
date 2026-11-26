"""
Environment variable constants.

These are resolved once at import time from .env / process environment.
No YAML dependency — pure os.getenv.
"""

import os

#: Settings a release renamed, each former name to the name read now. A former
#: name is never read, and set to anything but empty, whitespace included, it
#: fails the boot (empty too, for a name in BLANK_IS_A_VALUE).
RENAMED_SETTINGS: dict[str, str] = {
    "GINLIX_DATA_URL": "LANGALPHA_DATA_URL",
    "GINLIX_DATA_WS_URL": "LANGALPHA_DATA_WS_URL",
    "GINLIX_DATA_SEC_ENABLED": "LANGALPHA_DATA_SEC_ENABLED",
    "AUTH_SERVICE_URL": "PLATFORM_SERVICE_URL",
    "GINLIXFLOW_BASE_URL": "SERVER_LOOPBACK_URL",
    "PDF_RENDER_INTERNAL_BASE": "SERVER_LOOPBACK_URL",
    "EGRESS_RELAY_LOOPBACK_URL": "SERVER_LOOPBACK_URL",
}

#: Former names whose blank was a value, not unset: a blank GINLIX_DATA_SEC_ENABLED
#: turned SEC reads off, where unset LANGALPHA_DATA_SEC_ENABLED turns them on.
BLANK_IS_A_VALUE: frozenset[str] = frozenset({"GINLIX_DATA_SEC_ENABLED"})


class RenamedSettingError(RuntimeError):
    """A setting named by a former name: a boot error, never caught as optional config."""


def _refuse_renamed(renamed: dict[str, str], blank_is_a_value: frozenset[str] = BLANK_IS_A_VALUE) -> None:
    """Fail the boot on a setting still under its former name, naming the new one."""
    for former, name in renamed.items():
        value = os.getenv(former)
        if value is None or (value == "" and former not in blank_is_a_value):
            continue
        if os.getenv(name, "").strip():
            raise RenamedSettingError(f"{former} was renamed to {name}, which is also set: remove the {former} line.")
        raise RenamedSettingError(
            f"{former} was renamed to {name}: rename the line in your env file, the value stays the same."
        )


_refuse_renamed(RENAMED_SETTINGS)

# Deployment mode: "oss" (self-hosted, no auth) or "platform" (Supabase auth + quota service)
HOST_MODE: str = os.getenv("HOST_MODE", "oss")

# Auth / Login Service (Supabase) — credential, not a mode flag
SUPABASE_URL: str = os.getenv("SUPABASE_URL", "")
LOCAL_DEV_USER_ID: str = os.getenv("AUTH_USER_ID", "local-dev-user")

# The platform service this deployment validates turns against: sign-in tokens,
# quota, billing leases and data tokens. Unset runs standalone.
PLATFORM_SERVICE_URL: str = os.getenv("PLATFORM_SERVICE_URL", "")

# Access tokens outside apps get from an OAuth issuer. Off unless all three
# are set; see src/server/auth/oauth_resource.py for what accepts them.
OAUTH_RESOURCE_ISSUER: str = os.getenv("OAUTH_RESOURCE_ISSUER", "").strip()
OAUTH_RESOURCE_JWKS_URL: str = os.getenv("OAUTH_RESOURCE_JWKS_URL", "").strip()
OAUTH_RESOURCE_AUDIENCES: tuple[str, ...] = tuple(
    aud.strip()
    for aud in os.getenv("OAUTH_RESOURCE_AUDIENCE", "").split(",")
    if aud.strip()
)
# The issuer's public client the `la` CLI signs in with (PKCE, loopback
# redirect). Unset, `la login` takes a personal access token instead.
OAUTH_CLI_CLIENT_ID: str = os.getenv("OAUTH_CLI_CLIENT_ID", "").strip()

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


# LangAlpha Data, the hosted market-data service behind LangAlpha.ai. It runs only
# as a hosted service; a stack without LANGALPHA_DATA_URL gets prices,
# fundamentals and news from FMP and the other configured providers.
LANGALPHA_DATA_URL: str = os.getenv("LANGALPHA_DATA_URL", "")
LANGALPHA_DATA_WS_URL: str = os.getenv("LANGALPHA_DATA_WS_URL", "") or (
    LANGALPHA_DATA_URL.replace("http://", "ws://").replace("https://", "wss://")
    if LANGALPHA_DATA_URL
    else ""
)
LANGALPHA_DATA_ENABLED: bool = bool(LANGALPHA_DATA_URL)
# Whether get_sec_filing reads filings from LangAlpha Data before SEC EDGAR, with
# LANGALPHA_DATA_URL set. false reads them from EDGAR alone and leaves market data
# on LangAlpha Data.
LANGALPHA_DATA_SEC_ENABLED: bool = os.getenv("LANGALPHA_DATA_SEC_ENABLED", "true").lower() == "true"

# Where a sandbox reaches the platform's public API, LangAlpha Data included.
# Sandboxes are remote, so this stays publicly reachable even when this server
# reaches the same services over a private address; unset, they use
# LANGALPHA_DATA_URL as this server does.
PUBLIC_API_BASE_URL: str = os.getenv("PUBLIC_API_BASE_URL", "").rstrip("/")

# Public base URL of this server (used in agent-generated URLs like preview links)
SERVER_BASE_URL: str = os.getenv("SERVER_BASE_URL", "http://localhost:8000")

# Where the web app is served, for links the agent hands to a person
# (``/a/<code>``). Defaults to the API origin, which is where the app lives
# unless the frontend is served from its own host.
PUBLIC_APP_URL: str = (os.getenv("PUBLIC_APP_URL", "") or SERVER_BASE_URL).rstrip("/")

# Where this server serves previews that a browser cannot open directly (a
# private E2B computer's ports) and the pages the agent publishes: a scheme and
# host, each preview and page a subdomain of it. Unset turns the preview proxy
# off, and pages are served sandboxed from the API origin instead.
PREVIEW_PROXY_BASE_URL: str = os.getenv("PREVIEW_PROXY_BASE_URL", "").strip()

# Origins allowed to frame a proxied preview or a published page, as a CSP
# frame-ancestors source list. Unset sends none, which leaves framing to the
# preview itself.
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

# This process's own listener, for its calls to itself: report-backs and
# hand-offs posted to its thread routes, the PDF renderer loading workspace
# HTML, and directly bound MCP tools going through its egress relay. Each has
# to reach this process rather than the public ingress. Unset, server.py's
# --host and --port name it (LANGALPHA_LISTEN_URL), else port 8000.
SERVER_LOOPBACK_URL: str = (
    os.getenv("SERVER_LOOPBACK_URL") or os.getenv("LANGALPHA_LISTEN_URL") or "http://127.0.0.1:8000"
).rstrip("/")

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

# Credit conversion rate (USD → credits).  Override with USD_TO_CREDITS_RATE env var.
USD_TO_CREDITS_RATE: int = int(os.getenv("USD_TO_CREDITS_RATE", "1000"))

# Firecrawl for Platforms (Gateway). With the platform key set, each user gets
# their own Firecrawl account, created on first use and billed to this
# deployment; the alias email's domain must be one we own and that accepts
# mail. Unset, every user shares FIRECRAWL_API_KEY.
FIRECRAWL_PLATFORM_KEY: str | None = os.getenv("FIRECRAWL_PLATFORM_KEY") or None
FIRECRAWL_ACCOUNT_EMAIL_DOMAIN: str | None = (
    os.getenv("FIRECRAWL_ACCOUNT_EMAIL_DOMAIN", "").strip().lstrip("@").lower() or None
)
FIRECRAWL_PARTNER_BASE_URL: str = os.getenv(
    "FIRECRAWL_PARTNER_BASE_URL", "https://integrations.firecrawl.dev"
).rstrip("/")

# Automation webhook delivery (channel gateway)
AUTOMATION_WEBHOOK_URL: str = os.getenv("AUTOMATION_WEBHOOK_URL", "")
AUTOMATION_WEBHOOK_SECRET: str = os.getenv("AUTOMATION_WEBHOOK_SECRET", "")

# Base URL of a channel gateway's API, path prefix included. Set together with
# INTERNAL_SERVICE_TOKEN, it gives the agent send_message and
# list_message_targets, which deliver to the messaging channels connected to
# the user's account. Unset, neither tool exists.
CHANNEL_GATEWAY_URL: str = os.getenv("CHANNEL_GATEWAY_URL", "").strip().rstrip("/")

# Apple Push Notification service, for the iOS app. Push is on only when the
# key id, the team id, the .p8 key (inline PEM, whose newlines may arrive as
# literal ``\n``, or a path) and the app's bundle ids are all set; the key file
# is read on first send.
APNS_KEY_ID: str = os.getenv("APNS_KEY_ID", "").strip()
APNS_TEAM_ID: str = os.getenv("APNS_TEAM_ID", "").strip()
APNS_PRIVATE_KEY: str = os.getenv("APNS_PRIVATE_KEY", "").replace("\\n", "\n").strip()
APNS_PRIVATE_KEY_PATH: str = os.getenv("APNS_PRIVATE_KEY_PATH", "").strip()
# The APNs topics a device may register under: the bundle ids of the app
# builds this server pushes to. No default, since they name one team's apps.
APNS_BUNDLE_IDS: tuple[str, ...] = tuple(
    b.strip() for b in os.getenv("APNS_BUNDLE_IDS", "").split(",") if b.strip()
)
APNS_ENABLED: bool = bool(
    APNS_KEY_ID
    and APNS_TEAM_ID
    and (APNS_PRIVATE_KEY or APNS_PRIVATE_KEY_PATH)
    and APNS_BUNDLE_IDS
)

# Host IP for local LLM providers (Ollama, LM Studio, vLLM).
# In Docker, "localhost" means the container — use host.docker.internal to reach the host.
_IN_DOCKER: bool = os.path.exists("/.dockerenv")
HOST_IP: str = os.getenv("HOST_IP", "host.docker.internal" if _IN_DOCKER else "localhost")
