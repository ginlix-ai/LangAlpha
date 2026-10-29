"""Endpoint checks and URL shaping for provider-specific requests."""

import os
from urllib.parse import urlparse, urlsplit, urlunsplit

_OFFICIAL_OPENAI_HOST = "api.openai.com"
OFFICIAL_ANTHROPIC_HOST = "api.anthropic.com"


def url_host(url: str | None) -> str:
    """The lowercased hostname ``url`` names, empty when it names none."""
    if not url:
        return ""
    parsed = urlparse(url)
    if parsed.hostname is None:
        # Scheme-less values ("api.openai.com/v1") parse as path-only; re-parse
        # as network location so the host comparison sees them.
        parsed = urlparse(f"//{url}")
    return (parsed.hostname or "").lower()


def is_official_openai_endpoint(base_url: str | None) -> bool:
    """True when requests with this base_url would hit api.openai.com.

    A ``None`` base_url falls through to the SDK default (api.openai.com)
    unless an env override (``OPENAI_API_BASE`` / ``OPENAI_BASE_URL``)
    redirects it — the same fallback chain langchain-openai and the openai
    SDK apply.
    """
    url = base_url or os.getenv("OPENAI_API_BASE") or os.getenv("OPENAI_BASE_URL")
    return not url or url_host(url) == _OFFICIAL_OPENAI_HOST


def is_official_anthropic_endpoint(base_url: str | None) -> bool:
    """True when requests with this base_url would hit api.anthropic.com,
    through the env fallbacks langchain-anthropic applies to a ``None``
    (``ANTHROPIC_API_URL``, then ``ANTHROPIC_BASE_URL``)."""
    url = base_url or os.getenv("ANTHROPIC_API_URL") or os.getenv("ANTHROPIC_BASE_URL")
    return not url or url_host(url) == OFFICIAL_ANTHROPIC_HOST


def anthropic_sdk_base_url(base_url: str | None) -> str | None:
    """The base an Anthropic-SDK client needs for a saved base URL.

    The SDK appends ``/v1/messages`` itself, so the ``.../v1`` shape that
    OpenAI-style URLs take reaches ``/v1/v1/messages`` and 404s. Applied where a
    client is built and never written back: the saved value also keys the
    route's identity.
    """
    if not base_url:
        return base_url
    parts = urlsplit(base_url.strip())
    path = parts.path.rstrip("/")
    if path.endswith("/v1"):
        path = path[: -len("/v1")].rstrip("/")
    return urlunsplit(parts._replace(path=path))
