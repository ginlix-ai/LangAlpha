"""
Environment variable constants.

These are resolved once at import time from .env / process environment.
No YAML dependency — pure os.getenv.
"""

import os
from typing import NamedTuple

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
    "EGRESS_RELAY_BASE_URL": "SERVER_SANDBOX_URL",
    "PUBLIC_API_BASE_URL": "LANGALPHA_DATA_SANDBOX_URL",
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


class UrlSource(NamedTuple):
    #: "set", or what an unset setting fell back to: "default", "listener",
    #: "from:<NAME>" or "unset"; prefixed "blank," when it is set empty and
    #: "whitespace," when it is set to only whitespace.
    source: str
    #: The value read, before any trailing slash is cut.
    value: str


#: Each URL setting below, by name, for the boot report and checks in
#: src/config/urls.py.
URL_SOURCES: dict[str, UrlSource] = {}


def _url(name: str, fallback: str = "", fallback_source: str = "default") -> str:
    """*name* stripped of whitespace, else *fallback*, recording which in URL_SOURCES.

    Blank counts as unset.
    """
    value = os.getenv(name, "").strip()
    if value:
        source = "set"
    else:
        source = fallback_source if fallback else "unset"
        if name in os.environ:
            source = f"{'whitespace' if os.environ[name] else 'blank'},{source}"
        value = fallback
    URL_SOURCES[name] = UrlSource(source, value)
    return value

# Deployment mode: "oss" (self-hosted, no auth) or "platform" (Supabase auth + quota service)
HOST_MODE: str = os.getenv("HOST_MODE", "oss")
# The config overlay this process layers on its YAML (APP_ENV=production reads
# agent_config.production.yaml). With HOST_MODE=platform, production also makes
# a deployment managed: it refuses to start on a URL setting it cannot run on.
APP_ENV: str = os.getenv("APP_ENV", "").strip()

# Auth / Login Service (Supabase) — credential, not a mode flag
SUPABASE_URL: str = os.getenv("SUPABASE_URL", "")
LOCAL_DEV_USER_ID: str = os.getenv("AUTH_USER_ID", "local-dev-user")

# The platform service this deployment validates turns against: sign-in tokens,
# quota, billing leases and data tokens. Unset runs standalone.
_PLATFORM_SERVICE_URL = _url("PLATFORM_SERVICE_URL")
PLATFORM_SERVICE_URL: str = _PLATFORM_SERVICE_URL.rstrip("/") or _PLATFORM_SERVICE_URL

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
LANGALPHA_DATA_URL: str = _url("LANGALPHA_DATA_URL")
_data_scheme, _, _data_rest = LANGALPHA_DATA_URL.partition("://")
LANGALPHA_DATA_WS_URL: str = _url(
    "LANGALPHA_DATA_WS_URL",
    f"{_data_scheme.lower().replace('http', 'ws')}://{_data_rest}" if _data_rest else "",
    "from:LANGALPHA_DATA_URL",
)
LANGALPHA_DATA_ENABLED: bool = bool(LANGALPHA_DATA_URL)
# Whether get_sec_filing reads filings from LangAlpha Data before SEC EDGAR, with
# LANGALPHA_DATA_URL set. false reads them from EDGAR alone and leaves market data
# on LangAlpha Data.
LANGALPHA_DATA_SEC_ENABLED: bool = os.getenv("LANGALPHA_DATA_SEC_ENABLED", "true").lower() == "true"

# Where sandboxes reach LangAlpha Data. Sandboxes are remote, so this stays
# publicly reachable even when this server reaches the service over a private
# address; unset, they use LANGALPHA_DATA_URL as this server does.
LANGALPHA_DATA_SANDBOX_URL: str = _url(
    "LANGALPHA_DATA_SANDBOX_URL", LANGALPHA_DATA_URL, "from:LANGALPHA_DATA_URL"
).rstrip("/")

# Public base URL of this server (used in agent-generated URLs like preview links)
SERVER_BASE_URL: str = _url("SERVER_BASE_URL", "http://localhost:8000")

# Where the web app is served, for links the agent hands to a person
# (``/a/<code>``). Defaults to the API origin, which is where the app lives
# unless the frontend is served from its own host.
PUBLIC_APP_URL: str = _url("PUBLIC_APP_URL", SERVER_BASE_URL, "from:SERVER_BASE_URL").rstrip("/")

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
_LISTEN_URL = os.getenv("LANGALPHA_LISTEN_URL", "").strip()
SERVER_LOOPBACK_URL: str = _url(
    "SERVER_LOOPBACK_URL",
    _LISTEN_URL or "http://127.0.0.1:8000",
    "listener" if _LISTEN_URL else "default",
).rstrip("/")

# Shared HS256 secret authenticating sandboxes to the egress relay
# (/v1/egress/*). Empty disables the relay — OAuth-connected MCP servers
# then have no execution path, but nothing else is affected.
EGRESS_RELAY_SECRET: str = os.getenv("EGRESS_RELAY_SECRET", "")

# Where sandboxes, SSH machines and their la CLI reach this backend: the egress
# relay (the generated client appends /v1/egress/{grant_id}), the file and
# Drive mounts (/api/v1/livefs/), the la CLI's token file and the SSH pairing
# script. Sandboxes are remote (or, in OSS Docker, on another network), so this
# must be an address they can reach. Unset, services/egress/reachability.py
# falls back to the server base, adapted per sandbox provider (Docker host
# gateway) or warned about (a remote provider with a local address). Point it
# at the API origin, not an SPA-fallback frontend.
SERVER_SANDBOX_URL: str = _url("SERVER_SANDBOX_URL")
if not SERVER_SANDBOX_URL:
    URL_SOURCES["SERVER_SANDBOX_URL"] = UrlSource(
        URL_SOURCES["SERVER_SANDBOX_URL"].source.replace("unset", "from:SERVER_BASE_URL"), SERVER_BASE_URL
    )

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
AUTOMATION_WEBHOOK_URL: str = _url("AUTOMATION_WEBHOOK_URL")
AUTOMATION_WEBHOOK_SECRET: str = os.getenv("AUTOMATION_WEBHOOK_SECRET", "")

# Base URL of a channel gateway's API, path prefix included. Set together with
# INTERNAL_SERVICE_TOKEN, it gives the agent send_message and
# list_message_targets, which deliver to the messaging channels connected to
# the user's account. Unset, neither tool exists.
CHANNEL_GATEWAY_URL: str = _url("CHANNEL_GATEWAY_URL").rstrip("/")

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
