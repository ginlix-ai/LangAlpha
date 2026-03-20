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

    def redact(self, text: str) -> str:
        """Replace known secret values with [REDACTED:KEY_NAME]."""
        for name, value in self._secrets:
            if value in text:
                text = text.replace(value, f"[REDACTED:{name}]")
        text = _SANDBOX_TOKEN_RE.sub("[REDACTED:SANDBOX_TOKEN]", text)
        return text

    def redact_bytes(self, data: bytes, encoding: str = "utf-8") -> bytes:
        """Decode bytes, redact secrets, re-encode. Returns original on decode failure."""
        try:
            text = data.decode(encoding)
        except (UnicodeDecodeError, LookupError):
            return data
        return self.redact(text).encode(encoding)


_instance: SecretRedactor | None = None


def get_redactor() -> SecretRedactor:
    """Lazy singleton — initialized on first call."""
    global _instance
    if _instance is None:
        _instance = SecretRedactor()
    return _instance
