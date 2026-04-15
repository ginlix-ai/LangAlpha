"""Leak detection middleware — redacts known secret values from tool outputs.

Discovers secret env var names from MCP server config and resolves their
values from os.environ. Scans ToolMessage.content for exact occurrences
and redacts before they reach the LLM context.

"""

import os
import re

import structlog
from langchain.agents.middleware import AgentMiddleware
from langchain_core.messages import ToolMessage

logger = structlog.get_logger(__name__)

# Env var names that are injected into the sandbox but are NOT secrets
_NON_SECRET_KEYS = frozenset({
    "GIT_AUTHOR_NAME",
    "GIT_AUTHOR_EMAIL",
    "GIT_COMMITTER_NAME",
    "GIT_COMMITTER_EMAIL",
})


class LeakDetectionMiddleware(AgentMiddleware):
    """Scans tool outputs for known secret values and redacts them.

    Discovers secrets from MCP server config env declarations. Matches
    exact values of secrets actually accessible to the agent (sandbox
    env vars like FMP_API_KEY, GITHUB_TOKEN, etc.).
    """

    # Matches sandbox access tokens (gxsa_...) and refresh tokens (gxsr_...)
    _SANDBOX_TOKEN_RE = re.compile(r"gxs[ar]_[A-Za-z0-9_.\-]+")

    def __init__(
        self,
        mcp_servers: list | None = None,
        vault_secrets: dict[str, str] | None = None,
    ) -> None:
        """Initialize by extracting secret values from MCP server config.

        Args:
            mcp_servers: List of MCPServerConfig objects. Each server's
                env dict is scanned for ${VAR} placeholders, which are
                resolved from os.environ to get the actual secret values.
            vault_secrets: Per-workspace user vault secrets (name→value).
                Merged into the redaction list alongside MCP secrets.
        """
        secrets: dict[str, str] = {}

        for server in mcp_servers or []:
            if not server.enabled:
                continue
            for key, value in (server.env or {}).items():
                if key in _NON_SECRET_KEYS:
                    continue
                # Resolve ${VAR} placeholders to get actual values
                if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
                    var_name = value[2:-1]
                    resolved = os.environ.get(var_name)
                    if resolved and len(resolved) >= 8:
                        secrets[key] = resolved
                elif isinstance(value, str) and len(value) >= 8:
                    # Literal value (not a placeholder)
                    secrets[key] = value

        # Also check for GITHUB_TOKEN (injected separately by _build_sandbox_env_vars).
        # Use get_nested_config to match sandbox.py's resolution logic.
        from src.config.settings import get_nested_config

        if get_nested_config("github.enabled", False):
            token_env = get_nested_config("github.token_env", "GITHUB_BOT_TOKEN")
            gh_token = os.environ.get(token_env)
            if gh_token and len(gh_token) >= 8:
                secrets["GITHUB_TOKEN"] = gh_token

        # Merge vault secrets (user-provided API keys stored per-workspace)
        # Use same >=8 threshold as MCP secrets to avoid false-positive redaction
        for name, value in (vault_secrets or {}).items():
            if value and len(value) >= 8:
                secrets[name] = value

        # Sort by value length descending so longer matches replace first
        self._secrets = sorted(secrets.items(), key=lambda kv: len(kv[1]), reverse=True)

        if self._secrets:
            logger.debug(
                "LeakDetectionMiddleware initialized",
                secret_count=len(self._secrets),
                names=[name for name, _ in self._secrets],
            )

    def _scan(self, content: str) -> str:
        for name, value in self._secrets:
            if value in content:
                logger.warning("Leak detected in tool output", secret_name=name)
                content = content.replace(value, f"[REDACTED:{name}]")
        # Pattern-based redaction for prefixed sandbox tokens (catches refreshed tokens)
        content = self._SANDBOX_TOKEN_RE.sub("[REDACTED:SANDBOX_TOKEN]", content)
        return content

    def wrap_tool_call(self, request, handler):
        result = handler(request)
        if isinstance(result, ToolMessage) and isinstance(result.content, str):
            result.content = self._scan(result.content)
        return result

    async def awrap_tool_call(self, request, handler):
        result = await handler(request)
        if isinstance(result, ToolMessage) and isinstance(result.content, str):
            result.content = self._scan(result.content)
        return result
