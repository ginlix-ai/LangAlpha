"""Source code for the ``vault`` Python module uploaded to the sandbox.

The module is written to ``_internal/src/vault.py`` so it's importable via
``from vault import get, list_names, load_env``.  It reads secrets from
``_internal/.vault_secrets.json`` at runtime.
"""

VAULT_MODULE_SOURCE = '''\
"""Workspace vault — access user-provided API keys and credentials.

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

def _load() -> dict[str, str]:
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
