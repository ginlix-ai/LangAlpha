"""Endpoint eligibility checks for provider-specific request features."""

import os
from urllib.parse import urlparse

_OFFICIAL_OPENAI_HOST = "api.openai.com"


def is_official_openai_endpoint(base_url: str | None) -> bool:
    """True when requests with this base_url would hit api.openai.com.

    A ``None`` base_url falls through to the SDK default (api.openai.com)
    unless an env override (``OPENAI_API_BASE`` / ``OPENAI_BASE_URL``)
    redirects it — the same fallback chain langchain-openai and the openai
    SDK apply.
    """
    url = base_url or os.getenv("OPENAI_API_BASE") or os.getenv("OPENAI_BASE_URL")
    if not url:
        return True
    parsed = urlparse(url)
    if parsed.hostname is None:
        # Scheme-less values ("api.openai.com/v1") parse as path-only; re-parse
        # as network location so the host comparison sees them.
        parsed = urlparse(f"//{url}")
    return (parsed.hostname or "").lower() == _OFFICIAL_OPENAI_HOST
