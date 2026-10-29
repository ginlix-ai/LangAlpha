"""Source code for the ``vault`` Python module uploaded to the sandbox.

The module is written to ``_internal/src/vault.py`` so it's importable via
``from vault import get, list_names, load_env``. It reads the owner's secrets
from the computer root's ``_internal/.vault_secrets.json``, the one vault every
workspace on the computer shares.
"""

from ..paths import WorkspaceLayout

_VAULT_MODULE_TEMPLATE = '''\
"""Vault: access user-provided API keys and credentials.

Usage::

    from vault import get, list_names

    api_key = get("MY_API_KEY")
    print(list_names())          # list available secret names
"""

import json
import os

_SECRETS_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    ".vault_secrets.json",
)
_WS_CONFIG_REL = "__WS_CONFIG_REL__"


def _names_own_vault() -> bool:
    """Whether the calling folder's tools still name a per-workspace vault.

    An earlier version gave each workspace its own vault, and the root one is
    the account's, where a secret of the same name can hold another value.
    """
    here = os.getcwd()
    for _ in range(32):
        try:
            with open(os.path.join(here, _WS_CONFIG_REL), encoding="utf-8") as f:
                view = json.load(f)
        except (OSError, ValueError):
            view = None
        if isinstance(view, dict):
            return bool(view.get("vault_file"))
        parent = os.path.dirname(here)
        if parent == here:
            break
        here = parent
    return False


def _load() -> dict[str, str]:
    if _names_own_vault():
        raise RuntimeError(
            "This workspace was set up by an earlier version and still names "
            "its own vault; secrets are available once the workspace starts "
            "again"
        )
    try:
        with open(_SECRETS_FILE) as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def get(name: str) -> str:
    """Return the value of a vault secret by name.

    Raises ``KeyError`` if the secret does not exist.
    """
    secrets = _load()
    if name not in secrets:
        available = ", ".join(sorted(secrets)) or "(none)"
        raise KeyError(
            f"Vault secret {name!r} not found. Available: {available}"
        )
    return secrets[name]


def list_names() -> list[str]:
    """Return a sorted list of available secret names."""
    return sorted(_load())


def load_env() -> int:
    """Set all vault secrets as environment variables.

    Returns the number of variables set.  Useful for libraries that
    read credentials from ``os.environ``.
    """
    secrets = _load()
    for k, v in secrets.items():
        os.environ[k] = v
    return len(secrets)
'''

VAULT_MODULE_SOURCE = _VAULT_MODULE_TEMPLATE.replace(
    "__WS_CONFIG_REL__", WorkspaceLayout.MCP_CLIENT_CONFIG_FILE
)
