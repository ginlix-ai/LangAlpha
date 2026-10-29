"""Tests for SecretRedactor.

Verifies that the SecretRedactor correctly discovers secrets from
MCP config and redacts them in text and bytes content.
"""

import os
from unittest.mock import AsyncMock, patch

import pytest

import src.server.database.mcp_servers as mcp_servers_db
import src.server.database.user_vault_secrets as user_vault_db
import src.server.utils.secret_redactor as secret_redactor
from src.server.utils.secret_redactor import (
    SecretRedactor,
    get_redactor,
    get_vault_secrets_for_redaction,
)

# Patch targets (imported inside SecretRedactor.__init__)
_GAC = "src.config.tool_settings._get_agent_config_dict"


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
        ):
            r = SecretRedactor()
        assert len(r._secrets) == 0

    def test_literal_env_values(self):
        """Literal (non-placeholder) values are also tracked."""
        cfg = _make_agent_config([_make_server(env={"KEY": "literal_secret_val"})])
        with patch(_GAC, return_value=cfg):
            r = SecretRedactor()
        assert len(r._secrets) == 1
        assert r._secrets[0][1] == "literal_secret_val"

    def test_skips_non_secret_keys(self):
        """GIT_AUTHOR_NAME and similar non-secret keys are excluded."""
        cfg = _make_agent_config([_make_server(env={
            "GIT_AUTHOR_NAME": "langalpha-bot",
            "GIT_AUTHOR_EMAIL": "bot@ginlix.ai",
        })])
        with patch(_GAC, return_value=cfg):
            r = SecretRedactor()
        assert len(r._secrets) == 0

    def test_skips_disabled_servers(self):
        cfg = _make_agent_config([
            _make_server(env={"SECRET": "${SECRET}"}, enabled=False),
        ])
        with (
            patch.dict(os.environ, {"SECRET": "disabled_secret"}, clear=False),
            patch(_GAC, return_value=cfg),
        ):
            r = SecretRedactor()
        assert len(r._secrets) == 0

    def test_empty_config(self):
        cfg = _make_agent_config([])
        with patch(_GAC, return_value=cfg):
            r = SecretRedactor()
        assert r._secrets == []

    def test_secrets_sorted_by_length(self):
        """Longer secrets should be replaced first to avoid partial matches."""
        cfg = _make_agent_config([_make_server(env={
            "SHORT": "abcdefgh",
            "LONG": "abcdefghijklmnop",
        })])
        with patch(_GAC, return_value=cfg):
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


class TestVaultSecretsForRedaction:
    """The redaction input must always be current truth, never a cached copy."""

    def _patch_sources(self, monkeypatch, *, vault):
        monkeypatch.setattr(
            secret_redactor, "_connector_secret_literals",
            AsyncMock(return_value={}),
        )
        reader = vault if isinstance(vault, AsyncMock) else AsyncMock(return_value=vault)
        monkeypatch.setattr(user_vault_db, "get_user_secrets_decrypted", reader)
        return reader

    @pytest.mark.asyncio
    async def test_reads_db_even_when_a_session_holds_a_stale_dict(self, monkeypatch):
        """A sandbox session caches the secret set at upload time, and that
        cache is process-local: after a rotation handled by another worker it
        still holds the RETIRED value. Redacting from it would scrub the dead
        secret and pass the live one through in cleartext.
        """
        self._patch_sources(monkeypatch, vault={"API_KEY": "rotated_value_111"})

        assert await get_vault_secrets_for_redaction("u1") == {
            "API_KEY": "rotated_value_111"
        }

    @pytest.mark.asyncio
    async def test_reads_the_owners_whole_vault(self, monkeypatch):
        """Every workspace of the user can read every secret, so a value is a
        secret here whether or not a server in this workspace references it."""
        reader = self._patch_sources(
            monkeypatch, vault={"UNREFERENCED": "agent_code_value_1"}
        )

        secrets = await get_vault_secrets_for_redaction("u7")

        reader.assert_awaited_once_with("u7")
        assert secrets == {"UNREFERENCED": "agent_code_value_1"}

    @pytest.mark.asyncio
    async def test_lookup_failure_propagates(self, monkeypatch):
        """Fail closed. Swallowing this returns "no secrets" and silently
        disables vault redaction, and the caller then serves the file — on a
        route whose only credential is the workspace UUID. The 5xx is the
        correct answer to "I don't know"."""
        self._patch_sources(
            monkeypatch, vault=AsyncMock(side_effect=RuntimeError("db down"))
        )
        with pytest.raises(RuntimeError):
            await get_vault_secrets_for_redaction("u1")

    @pytest.mark.asyncio
    async def test_empty_vault_is_not_a_failure(self, monkeypatch):
        """The other half of the contract: {} means the owner has none."""
        self._patch_sources(monkeypatch, vault={})
        assert await get_vault_secrets_for_redaction("u1") == {}

    @pytest.mark.asyncio
    async def test_a_missing_owner_is_not_an_empty_vault(self, monkeypatch):
        """A workspace deleted mid-request names no owner; reading that as
        "no secrets" served the file with nothing redacted."""
        reader = self._patch_sources(monkeypatch, vault={"K": "v" * 12})
        with pytest.raises(ValueError):
            await get_vault_secrets_for_redaction("")
        reader.assert_not_awaited()


class TestConnectorLiteralRedaction:
    """Inline connector credentials join the redaction set.

    The vault is the sanctioned home for these values, but the API accepts
    plain literals, and a literal the platform delivers into every inheriting
    workspace deserves the same scrubbing a vault value gets.
    """

    def _patch_sources(self, monkeypatch, *, catalog_rows=()):
        catalog = AsyncMock(return_value=list(catalog_rows))
        monkeypatch.setattr(mcp_servers_db, "list_catalog_servers", catalog)
        monkeypatch.setattr(
            user_vault_db, "get_user_secrets_decrypted", AsyncMock(return_value={}),
        )
        return catalog

    @pytest.mark.asyncio
    async def test_credential_literals_join_the_set(self, monkeypatch):
        catalog = self._patch_sources(
            monkeypatch,
            catalog_rows=[
                {"name": "alpha", "env": {"API_TOKEN": "envtoken_value_123"}},
                {
                    "name": "beta",
                    "headers": {"Authorization": "Bearer usertoken_9999"},
                },
            ],
        )

        secrets = await get_vault_secrets_for_redaction("u1")

        catalog.assert_awaited_once_with("u1")
        assert secrets["mcp:alpha:API_TOKEN"] == "envtoken_value_123"
        assert secrets["mcp:beta:Authorization"] == "Bearer usertoken_9999"

    @pytest.mark.asyncio
    async def test_a_templated_vault_reference_is_not_a_literal(self, monkeypatch):
        """``Bearer ${vault:K}`` resolves to a vault value already in the set.
        Collected as a literal, the template text itself became a "secret",
        and a served file quoting the plugin's own config came back mangled."""
        self._patch_sources(
            monkeypatch,
            catalog_rows=[{
                "name": "beta",
                "headers": {"Authorization": "Bearer ${vault:BETA_API_KEY}"},
                "env": {"API_TOKEN": "${vault:BETA_TOKEN}"},
            }],
        )

        assert await get_vault_secrets_for_redaction("u1") == {}

    @pytest.mark.asyncio
    async def test_ordinary_config_values_stay_servable(self, monkeypatch):
        """The false-positive class this filter exists for: a served README
        that says "application/json" must not come back with holes in it."""
        self._patch_sources(
            monkeypatch,
            catalog_rows=[{
                "name": "beta",
                "headers": {"Accept": "application/json"},
                "env": {"LOG_LEVEL": "VERBOSE_MODE"},
            }],
        )

        assert await get_vault_secrets_for_redaction("u1") == {}

    @pytest.mark.asyncio
    async def test_a_long_opaque_value_is_a_credential_whatever_its_key(
        self, monkeypatch
    ):
        self._patch_sources(
            monkeypatch,
            catalog_rows=[{
                "name": "beta",
                "env": {"SESSION": "abcdefghij0123456789abcde"},
            }],
        )

        secrets = await get_vault_secrets_for_redaction("u1")
        assert secrets == {"mcp:beta:SESSION": "abcdefghij0123456789abcde"}

    @pytest.mark.asyncio
    async def test_arg_credentials_join_the_set(self, monkeypatch):
        self._patch_sources(
            monkeypatch,
            catalog_rows=[
                {"name": "alpha", "args": ["--api-key=argkey_value_12345"]},
                {"name": "beta", "args": ["--token", "userargtok_9999"]},
            ],
        )

        secrets = await get_vault_secrets_for_redaction("u1")

        assert secrets["mcp:alpha:api-key"] == "argkey_value_12345"
        assert secrets["mcp:beta:token"] == "userargtok_9999"

    @pytest.mark.asyncio
    async def test_ordinary_args_stay_servable(self, monkeypatch):
        """Arg lists are full of paths and URLs; only a flag that NAMES the
        value a credential may pull it into the redaction set."""
        self._patch_sources(
            monkeypatch,
            catalog_rows=[{
                "name": "beta",
                "args": [
                    "--config",
                    "/workspace/output/analysis_results_2026.csv",
                    "https://api.example.com/v1/some/endpoint",
                ],
            }],
        )

        assert await get_vault_secrets_for_redaction("u1") == {}

    @pytest.mark.asyncio
    async def test_vault_refs_are_not_collected(self, monkeypatch):
        """A ``${vault:NAME}`` ref resolves to a vault value the scan already
        covers; the ref text itself is not a secret."""
        self._patch_sources(
            monkeypatch,
            catalog_rows=[{
                "name": "beta",
                "env": {"API_KEY": "${vault:MY_KEY}"},
            }],
        )

        assert await get_vault_secrets_for_redaction("u1") == {}

    @pytest.mark.asyncio
    async def test_connector_lookup_failure_propagates(self, monkeypatch):
        """Same fail-closed contract as the vault read."""
        self._patch_sources(monkeypatch)
        monkeypatch.setattr(
            mcp_servers_db, "list_catalog_servers",
            AsyncMock(side_effect=RuntimeError("db down")),
        )
        with pytest.raises(RuntimeError):
            await get_vault_secrets_for_redaction("u1")


class TestGetRedactor:
    """Verify singleton behavior."""

    def test_returns_same_instance(self):
        cfg = _make_agent_config([])
        with (
            patch(_GAC, return_value=cfg),
            patch("src.server.utils.secret_redactor._instance", None),
        ):
            r1 = get_redactor()
            r2 = get_redactor()
        assert r1 is r2

    def test_creates_instance_on_first_call(self):
        cfg = _make_agent_config([])
        with (
            patch(_GAC, return_value=cfg),
            patch("src.server.utils.secret_redactor._instance", None),
        ):
            r = get_redactor()
        assert isinstance(r, SecretRedactor)
