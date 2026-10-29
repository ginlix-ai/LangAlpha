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
            vault_secrets: The owner's vault secrets ({name: value}).
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


async def get_vault_secrets_for_redaction(user_id: str) -> dict[str, str]:
    """The owner's redactable secret set: their whole vault plus
    credential-looking inline connector literals.

    Takes the owner the caller already resolved instead of re-reading the
    workspace: a workspace deleted mid-request names no owner, and reading that
    as "no secrets" would serve the file unredacted. The whole vault counts,
    since every workspace of the user can read every secret.

    Always reads the DB, never a live session's cached copy: that cache is
    process-local and written once at upload, so a rotation handled by another
    worker leaves this process holding the RETIRED value, which would scrub the
    dead secret and pass the live one through in cleartext. A failed read
    propagates: an empty dict means the owner has no secrets, never "the
    lookup failed". Callers serve file bytes on this answer, some of them on
    routes whose only credential is the URL.
    """
    from src.server.database.user_vault_secrets import get_user_secrets_decrypted

    if not user_id:
        raise ValueError("Redacting a workspace file needs its owner")
    literals = await _connector_secret_literals(str(user_id))
    vault = await get_user_secrets_decrypted(str(user_id))
    return {**literals, **vault}


async def _connector_secret_literals(user_id: str) -> dict[str, str]:
    """Inline env/header/arg literals from the user's plugins that read as
    credentials.

    The sanctioned home for these values is a ``${vault:NAME}`` ref, but the
    API accepts plain literals too, and a literal the platform delivers into
    every inheriting workspace deserves the same scrubbing a vault value gets.
    Collection is over-broad on rows (disabled included, as a credential on a
    disabled row is still a credential) and narrow on values:
    ``looks_like_secret`` keeps ordinary config (``application/json``,
    ``LOG_LEVEL=ERROR``) from being redacted out of served files.
    """
    from ptc_agent.core.mcp_sanitize import (
        VAULT_REF_RE,
        iter_arg_credentials,
        looks_like_secret,
    )
    from src.server.database.mcp_servers import list_catalog_servers

    def _collect(server: str, mapping, out: dict[str, str]) -> None:
        for key, value in (mapping or {}).items():
            if not isinstance(value, str) or key in _NON_SECRET_KEYS:
                continue
            # A value carrying a ref resolves to a vault value the scan already
            # covers, and the text around it is a template (``Bearer ``), not
            # a credential; ``search`` is the verdict vault extraction reaches.
            if VAULT_REF_RE.search(value):
                continue
            if len(value) < 8 or not looks_like_secret(key, value):
                continue
            out[f"mcp:{server}:{key}"] = value

    def _collect_args(server: str, args, out: dict[str, str]) -> None:
        for key, value in iter_arg_credentials(args):
            if len(value) >= 8:
                out[f"mcp:{server}:{key}"] = value

    entries = [
        (row.get("name") or "", row.get("env"), row.get("headers"), row.get("args"))
        for row in await list_catalog_servers(user_id)
    ]

    literals: dict[str, str] = {}
    for server, env, headers, args in entries:
        _collect(server, env, literals)
        _collect(server, headers, literals)
        _collect_args(server, args, literals)
    return literals
