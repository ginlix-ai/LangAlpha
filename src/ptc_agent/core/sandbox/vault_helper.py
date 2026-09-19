"""Source code for the ``vault`` Python module uploaded to the sandbox.

The module is written to ``_internal/src/vault.py`` so it's importable via
``from vault import get, list_names, load_env``. The computer root's
``_internal/.vault_secrets.json`` holds the user tier; each workspace's own
merged set lives under :data:`VAULTS_DIR`, named by the workspace's claim, and
the helper finds it through the calling folder's ``mcp_client_config.json``.
"""

from ..paths import SandboxLayout, WorkspaceLayout

#: Per-workspace vault files, under the runtime tier so file tools, backups
#: and the file panel never see them.
VAULTS_DIR = f"{SandboxLayout.INTERNAL_DIR}/vaults"


def workspace_vault_path(root: str, claim: str) -> str:
    """Where one workspace's effective secrets live on the computer."""
    return f"{root.rstrip('/')}/{VAULTS_DIR}/{claim}.json"


_VAULT_MODULE_TEMPLATE = '''\
"""Workspace vault — access user-provided API keys and credentials.

Usage::

    from vault import get, list_names

    api_key = get("MY_API_KEY")
    print(list_names())          # list available secret names
"""

import json
import os

_ROOT_SECRETS_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    ".vault_secrets.json",
)
_WS_CONFIG_REL = "__WS_CONFIG_REL__"


def _secrets_file() -> str:
    """The calling workspace's vault, found from the working directory.

    Several workspaces share this computer and each has its own secrets; the
    folder a call runs in names which. Outside every folder the root file (the
    user tier) answers.
    """
    here = os.getcwd()
    for _ in range(32):
        candidate = os.path.join(here, _WS_CONFIG_REL)
        try:
            with open(candidate, encoding="utf-8") as f:
                view = json.load(f)
        except (OSError, ValueError):
            view = None
        if isinstance(view, dict) and view.get("vault_file"):
            return str(view["vault_file"])
        parent = os.path.dirname(here)
        if parent == here:
            break
        here = parent
    return _ROOT_SECRETS_FILE


def _load() -> dict[str, str]:
    try:
        with open(_secrets_file()) as f:
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
