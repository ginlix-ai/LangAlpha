"""Tests for SecretRedactor.

Verifies that the SecretRedactor correctly discovers secrets from
MCP config and redacts them in text and bytes content.
"""

import os
from unittest.mock import patch

from src.server.utils.secret_redactor import SecretRedactor, get_redactor

# Patch targets (imported inside SecretRedactor.__init__)
_GAC = "src.config.tool_settings._get_agent_config_dict"
_GNC = "src.config.settings.get_nested_config"


def _disable_github(key, default):
    if key == "github.enabled":
        return False
    return default


def _make_agent_config(servers=None):
    return {"mcp": {"servers": servers or []}}


def _make_server(name="test", env=None, enabled=True):
    return {"name": name, "env": env or {}, "enabled": enabled}


class TestSecretDiscovery:
    """Verify secrets are correctly discovered from MCP config."""

    def test_resolves_placeholder_secrets(self):
        cfg = _make_agent_config([_make_server(env={"FMP_API_KEY": "${FMP_API_KEY}"})])
        with (
            patch.dict(os.environ, {"FMP_API_KEY": "fmp_secret_12345"}, clear=False),
            patch(_GAC, return_value=cfg),
            patch(_GNC, side_effect=_disable_github),
        ):
            r = SecretRedactor()
        assert len(r._secrets) == 1
        assert r._secrets[0][0] == "FMP_API_KEY"

    def test_skips_short_values(self):
        """Values shorter than 8 chars are ignored to avoid false positives."""
        cfg = _make_agent_config([_make_server(env={"SHORT": "${SHORT}"})])
        with (
            patch.dict(os.environ, {"SHORT": "abc"}, clear=False),
            patch(_GAC, return_value=cfg),
            patch(_GNC, side_effect=_disable_github),
        ):
            r = SecretRedactor()
        assert len(r._secrets) == 0

    def test_literal_env_values(self):
        """Literal (non-placeholder) values are also tracked."""
        cfg = _make_agent_config([_make_server(env={"KEY": "literal_secret_val"})])
        with (
            patch(_GAC, return_value=cfg),
            patch(_GNC, side_effect=_disable_github),
        ):
            r = SecretRedactor()
        assert len(r._secrets) == 1
        assert r._secrets[0][1] == "literal_secret_val"

    def test_skips_non_secret_keys(self):
        """GIT_AUTHOR_NAME and similar non-secret keys are excluded."""
        cfg = _make_agent_config([_make_server(env={
            "GIT_AUTHOR_NAME": "langalpha-bot",
            "GIT_AUTHOR_EMAIL": "bot@ginlix.ai",
        })])
        with (
            patch(_GAC, return_value=cfg),
            patch(_GNC, side_effect=_disable_github),
        ):
            r = SecretRedactor()
        assert len(r._secrets) == 0

    def test_skips_disabled_servers(self):
        cfg = _make_agent_config([
            _make_server(env={"SECRET": "${SECRET}"}, enabled=False),
        ])
        with (
            patch.dict(os.environ, {"SECRET": "disabled_secret"}, clear=False),
            patch(_GAC, return_value=cfg),
            patch(_GNC, side_effect=_disable_github),
        ):
            r = SecretRedactor()
        assert len(r._secrets) == 0

    def test_github_token_from_config(self):
        def _enable_github(key, default):
            return {
                "github.enabled": True,
                "github.token_env": "GITHUB_BOT_TOKEN",
            }.get(key, default)

        cfg = _make_agent_config([])
        with (
            patch.dict(os.environ, {"GITHUB_BOT_TOKEN": "ghp_1234567890abcdef"}, clear=False),
            patch(_GAC, return_value=cfg),
            patch(_GNC, side_effect=_enable_github),
        ):
            r = SecretRedactor()
        assert any(name == "GITHUB_TOKEN" for name, _ in r._secrets)

    def test_empty_config(self):
        cfg = _make_agent_config([])
        with (
            patch(_GAC, return_value=cfg),
            patch(_GNC, side_effect=_disable_github),
        ):
            r = SecretRedactor()
        assert r._secrets == []

    def test_secrets_sorted_by_length(self):
        """Longer secrets should be replaced first to avoid partial matches."""
        cfg = _make_agent_config([_make_server(env={
            "SHORT": "abcdefgh",
            "LONG": "abcdefghijklmnop",
        })])
        with (
            patch(_GAC, return_value=cfg),
            patch(_GNC, side_effect=_disable_github),
        ):
            r = SecretRedactor()
        assert len(r._secrets) == 2
        assert len(r._secrets[0][1]) >= len(r._secrets[1][1])


class TestRedact:
    """Verify redact() replaces secrets in text."""

    def _make_redactor(self, secrets: dict[str, str]) -> SecretRedactor:
        r = SecretRedactor.__new__(SecretRedactor)
        r._secrets = sorted(secrets.items(), key=lambda kv: len(kv[1]), reverse=True)
        return r

    def test_redacts_single_secret(self):
        r = self._make_redactor({"FMP_API_KEY": "fmp_secret_12345"})
        result = r.redact("key is fmp_secret_12345 here")
        assert result == "key is [REDACTED:FMP_API_KEY] here"

    def test_redacts_multiple_secrets(self):
        r = self._make_redactor({
            "FMP_API_KEY": "fmp_secret_12345",
            "GITHUB_TOKEN": "ghp_abcdef1234567",
        })
        result = r.redact("fmp_secret_12345 and ghp_abcdef1234567")
        assert "fmp_secret_12345" not in result
        assert "ghp_abcdef1234567" not in result
        assert "[REDACTED:FMP_API_KEY]" in result
        assert "[REDACTED:GITHUB_TOKEN]" in result

    def test_redacts_repeated_occurrences(self):
        r = self._make_redactor({"SECRET": "my_secret_value"})
        result = r.redact("first my_secret_value then my_secret_value")
        assert result == "first [REDACTED:SECRET] then [REDACTED:SECRET]"

    def test_no_secrets_passthrough(self):
        r = self._make_redactor({})
        assert r.redact("no secrets") == "no secrets"

    def test_no_match_passthrough(self):
        r = self._make_redactor({"KEY": "not_present_here"})
        assert r.redact("nothing to redact") == "nothing to redact"

    def test_redacts_sandbox_access_tokens(self):
        r = self._make_redactor({})
        result = r.redact("token gxsa_abc123_def456 found")
        assert result == "token [REDACTED:SANDBOX_TOKEN] found"

    def test_redacts_sandbox_refresh_tokens(self):
        r = self._make_redactor({})
        result = r.redact("refresh gxsr_token789.extra found")
        assert result == "refresh [REDACTED:SANDBOX_TOKEN] found"

    def test_longer_secret_replaced_first(self):
        """When one secret is a prefix of another, longer replaces first."""
        r = self._make_redactor({
            "SHORT": "abcdefgh",
            "LONG": "abcdefghijkl",
        })
        result = r.redact("value is abcdefghijkl end")
        assert result == "value is [REDACTED:LONG] end"


class TestRedactBytes:
    """Verify redact_bytes() works on byte content."""

    def _make_redactor(self, secrets: dict[str, str]) -> SecretRedactor:
        r = SecretRedactor.__new__(SecretRedactor)
        r._secrets = sorted(secrets.items(), key=lambda kv: len(kv[1]), reverse=True)
        return r

    def test_redacts_text_bytes(self):
        r = self._make_redactor({"KEY": "secret_value_123"})
        result = r.redact_bytes(b"data: secret_value_123 end")
        assert result == b"data: [REDACTED:KEY] end"

    def test_binary_passthrough(self):
        """Non-UTF-8 bytes are returned unchanged."""
        r = self._make_redactor({"KEY": "secret_value_123"})
        binary = bytes(range(256))
        assert r.redact_bytes(binary) == binary

    def test_empty_bytes(self):
        r = self._make_redactor({"KEY": "secret_value_123"})
        assert r.redact_bytes(b"") == b""

    def test_redacts_secret_in_non_utf8_body(self):
        """A secret in a non-UTF-8 body is scrubbed via the lossless latin-1
        fallback; surrounding (undecodable) bytes are preserved."""
        r = self._make_redactor({"KEY": "secret_value_123"})
        data = b"\xff\xfe head secret_value_123 tail"
        result = r.redact_bytes(data)
        assert b"secret_value_123" not in result
        assert b"[REDACTED:KEY]" in result
        assert result.startswith(b"\xff\xfe head ")


class TestGetRedactor:
    """Verify singleton behavior."""

    def test_returns_same_instance(self):
        cfg = _make_agent_config([])
        with (
            patch(_GAC, return_value=cfg),
            patch(_GNC, side_effect=_disable_github),
            patch("src.server.utils.secret_redactor._instance", None),
        ):
            r1 = get_redactor()
            r2 = get_redactor()
        assert r1 is r2

    def test_creates_instance_on_first_call(self):
        cfg = _make_agent_config([])
        with (
            patch(_GAC, return_value=cfg),
            patch(_GNC, side_effect=_disable_github),
            patch("src.server.utils.secret_redactor._instance", None),
        ):
            r = get_redactor()
        assert isinstance(r, SecretRedactor)
