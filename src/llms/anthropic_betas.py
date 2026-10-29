"""The Anthropic betas a client was built to send."""

from typing import Any


def client_betas(model: Any) -> list[str]:
    """The betas ``model`` sends on its own: its ``anthropic-beta`` header,
    under any casing of the name, then its ``betas`` field."""
    headers = getattr(model, "default_headers", None) or {}
    header = next((v for k, v in headers.items() if k.lower() == "anthropic-beta"), "")
    own = [b.strip() for b in str(header).split(",") if b.strip()]
    return list(dict.fromkeys([*own, *(getattr(model, "betas", None) or ())]))
