"""Secret redactor — redacts known secret values from user-facing content.

Used by file viewer and download endpoints to prevent API key leakage
when agents write secrets to files in the sandbox.

Resolution logic mirrors LeakDetectionMiddleware.__init__ in
src/ptc_agent/agent/middleware/tool/leak_detection.py.
"""

import os
import re

import structlog

logger = structlog.get_logger(__name__)

# Env var names injected into sandbox that are NOT secrets
_NON_SECRET_KEYS = frozenset({
    "GIT_AUTHOR_NAME",
    "GIT_AUTHOR_EMAIL",
    "GIT_COMMITTER_NAME",
    "GIT_COMMITTER_EMAIL",
})

# Matches sandbox access tokens (gxsa_...) and refresh tokens (gxsr_...)
_SANDBOX_TOKEN_RE = re.compile(r"gxs[ar]_[A-Za-z0-9_.\-]+")


class SecretRedactor:
    """Resolves secret values from MCP config and provides redaction methods."""

    def __init__(self) -> None:
        from src.config.settings import get_nested_config
        from src.config.tool_settings import _get_agent_config_dict

        secrets: dict[str, str] = {}

        agent_config = _get_agent_config_dict()
        mcp_config = agent_config.get("mcp", {})
        for server in mcp_config.get("servers", []):
            if not server.get("enabled", True):
                continue
            for key, value in (server.get("env") or {}).items():
                if key in _NON_SECRET_KEYS:
                    continue
                if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
                    var_name = value[2:-1]
                    resolved = os.environ.get(var_name)
                    if resolved and len(resolved) >= 8:
                        secrets[key] = resolved
                elif isinstance(value, str) and len(value) >= 8:
                    secrets[key] = value

        # GITHUB_TOKEN — injected separately by _build_sandbox_env_vars
        if get_nested_config("github.enabled", False):
            token_env = get_nested_config("github.token_env", "GITHUB_BOT_TOKEN")
            gh_token = os.environ.get(token_env)
            if gh_token and len(gh_token) >= 8:
                secrets["GITHUB_TOKEN"] = gh_token

        # Sort by value length descending so longer matches replace first
        self._secrets = sorted(secrets.items(), key=lambda kv: len(kv[1]), reverse=True)

        if self._secrets:
            logger.info(
                "SecretRedactor initialized",
                secret_count=len(self._secrets),
                names=[name for name, _ in self._secrets],
            )

    def redact(
        self, text: str, vault_secrets: dict[str, str] | None = None,
    ) -> str:
        """Replace known secret values with [REDACTED:KEY_NAME].

        Args:
            text: Content to scan.
            vault_secrets: A workspace's effective vault secrets ({name: value}).
                Merged into the scan alongside global MCP secrets.
        """
        for name, value in self._secrets:
            if value in text:
                text = text.replace(value, f"[REDACTED:{name}]")
        if vault_secrets:
            for name, value in sorted(
                vault_secrets.items(), key=lambda kv: len(kv[1]), reverse=True,
            ):
                if value and len(value) >= 8 and value in text:
                    text = text.replace(value, f"[REDACTED:{name}]")
        text = _SANDBOX_TOKEN_RE.sub("[REDACTED:SANDBOX_TOKEN]", text)
        return text

    def redact_bytes(
        self, data: bytes, encoding: str = "utf-8",
        vault_secrets: dict[str, str] | None = None,
    ) -> bytes:
        """Decode bytes, redact secrets, re-encode.

        On a decode failure, fall back to latin-1 — it maps every byte 0–255 to
        a codepoint and round-trips losslessly, so a secret written into a
        non-UTF-8 body is still scrubbed without corrupting binary content (only
        the secret's own byte-run is replaced). Wide (UTF-16) encodings of a
        secret are not caught.
        """
        try:
            text = data.decode(encoding)
            return self.redact(text, vault_secrets=vault_secrets).encode(encoding)
        except (UnicodeDecodeError, LookupError):
            text = data.decode("latin-1")
            return self.redact(text, vault_secrets=vault_secrets).encode("latin-1")


_instance: SecretRedactor | None = None


def get_redactor() -> SecretRedactor:
    """Lazy singleton — initialized on first call."""
    global _instance
    if _instance is None:
        _instance = SecretRedactor()
    return _instance


async def get_vault_secrets_for_redaction(workspace_id: str) -> dict[str, str]:
    """The workspace's redactable secret set: effective vault secrets
    (user ∪ workspace) plus credential-looking inline connector literals.

    Always reads the DB, never a live session's cached copy: that cache is
    process-local and written once at upload, so a rotation handled by another
    worker leaves this process holding the RETIRED value, which would scrub the
    dead secret and pass the live one through in cleartext. A failed read
    propagates: an empty dict means the workspace has no secrets, never "the
    lookup failed" — callers serve file bytes on this answer, one of them on a
    route whose only credential is the workspace UUID.
    """
    from src.server.database.vault_secrets import get_effective_secrets

    literals = await _connector_secret_literals(workspace_id)
    vault = await get_effective_secrets(workspace_id)
    return {**literals, **vault}


async def _connector_secret_literals(workspace_id: str) -> dict[str, str]:
    """Inline env/header/arg literals from the workspace's plugins that
    read as credentials.

    The sanctioned home for these values is a ``${vault:NAME}`` ref, but the
    API accepts plain literals too, and a literal the platform delivers into
    every inheriting workspace deserves the same scrubbing a vault value gets.
    Collection is over-broad on rows (both tiers, disabled included — a
    credential on a disabled row is still a credential) and narrow on values:
    ``looks_like_secret`` keeps ordinary config (``application/json``,
    ``LOG_LEVEL=ERROR``) from being redacted out of served files.
    """
    from ptc_agent.core.mcp_sanitize import (
        VAULT_REF_RE,
        iter_arg_credentials,
        looks_like_secret,
    )
    from src.server.database.mcp_servers import (
        list_catalog_servers,
        list_workspace_servers,
    )
    from src.server.database.workspace import get_workspace

    def _collect(server: str, mapping, out: dict[str, str]) -> None:
        for key, value in (mapping or {}).items():
            if not isinstance(value, str) or key in _NON_SECRET_KEYS:
                continue
            if VAULT_REF_RE.fullmatch(value):
                continue  # resolves to a vault value the scan already covers
            if len(value) < 8 or not looks_like_secret(key, value):
                continue
            out[f"mcp:{server}:{key}"] = value

    def _collect_args(server: str, args, out: dict[str, str]) -> None:
        for key, value in iter_arg_credentials(args):
            if len(value) >= 8:
                out[f"mcp:{server}:{key}"] = value

    entries: list[tuple[str, object, object, object]] = []
    for row in await list_workspace_servers(workspace_id):
        config = row.get("config") or {}
        entries.append(
            (
                row.get("name") or "",
                config.get("env"),
                config.get("headers"),
                config.get("args"),
            )
        )
    workspace = await get_workspace(workspace_id)
    user_id = (workspace or {}).get("user_id")
    if user_id:
        for row in await list_catalog_servers(str(user_id)):
            entries.append(
                (
                    row.get("name") or "",
                    row.get("env"),
                    row.get("headers"),
                    row.get("args"),
                )
            )

    literals: dict[str, str] = {}
    for server, env, headers, args in entries:
        _collect(server, env, literals)
        _collect(server, headers, literals)
        _collect_args(server, args, literals)
    return literals
