from __future__ import annotations

import json

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="class")]


def _payload(token: str, iat_ms: int, expires_at: str) -> dict[str, str | int]:
    return {
        "access_token": token,
        "iat_ms": iat_ms,
        "expires_at": expires_at,
        "ginlix_data_url": "https://data.example",
    }


class TestTokenUpload:
    """PTCSandbox.upload_token_file() -- the server writes, the sandbox reads."""

    async def _read(self, shared_sandbox) -> dict:
        content = await shared_sandbox.runtime.download_file(
            shared_sandbox._token_file_path
        )
        return json.loads(content)

    async def test_upload_token(self, shared_sandbox):
        payload = _payload("gxsa_test_access", 1, "2026-01-01T01:00:00Z")
        assert await shared_sandbox.upload_token_file(payload) is True

        assert await self._read(shared_sandbox) == payload
        mode = await shared_sandbox.runtime.exec(
            f"stat -c %a {shared_sandbox._token_file_path}"
        )
        assert mode.stdout.strip() == "600"

    async def test_only_a_later_token_replaces_the_file(self, shared_sandbox):
        """A write that stalled behind a newer one must not put its older,
        possibly revoked, token back, nor report it written. Issue order wins
        over expiry, which a shorter lifetime puts out of order."""
        v1 = _payload("v1", 2, "2026-01-01T03:00:00Z")
        assert await shared_sandbox.upload_token_file(v1) is True
        v2 = _payload("v2", 3, "2026-01-01T02:00:00Z")
        assert await shared_sandbox.upload_token_file(v2) is True
        assert (await self._read(shared_sandbox))["access_token"] == "v2"

        assert await shared_sandbox.upload_token_file(v1) is False
        assert (await self._read(shared_sandbox))["access_token"] == "v2"


class TestManifest:
    """PTCSandbox manifest lifecycle -- write, read, and sync detection."""

    async def test_write_and_read_manifest(self, shared_sandbox):
        # Compute and write
        manifest = await shared_sandbox._compute_sandbox_manifest()
        await shared_sandbox._write_unified_manifest(manifest)

        # Read back
        remote = await shared_sandbox._read_unified_manifest()
        assert remote is not None
        assert remote.get("schema_version") == 1
        assert "modules" in remote

    async def test_manifest_versioning(self, shared_sandbox):
        """Two consecutive manifests should have matching versions when
        nothing changed."""
        m1 = await shared_sandbox._compute_sandbox_manifest()
        m2 = await shared_sandbox._compute_sandbox_manifest()

        # Module versions should match for same config
        for module_name in m1.get("modules", {}):
            v1 = m1["modules"][module_name].get("version")
            v2 = m2["modules"][module_name].get("version")
            if v1 is not None:
                assert v1 == v2, f"Module {module_name} version mismatch"
