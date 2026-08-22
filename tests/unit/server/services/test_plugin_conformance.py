"""Agent Plugins conformance suite: one fixture per failure-ladder rung.

Each test asserts the *scope* of a failure, not just that it fails: a
plugin.json violation is fatal, an mcp.json document defect drops only the
MCP component, an entry defect skips only that entry, a skill defect skips
only that skill, and the two spec-tolerated plugin.json defects only warn.
Fixture trees live in tests/fixtures/plugins/; archive-level attacks that
cannot exist as file trees are built in-code.
"""

import io
import json
import zipfile
from pathlib import Path

import pytest

from src.server.services.plugins.errors import PluginAmbiguous, PluginFatal
from src.server.services.plugins.fetch import (
    compose_subdir_url,
    normalize_forge_url,
)
from src.server.services.plugins import validate_package
from src.server.services.plugins.extension import materialize_binds

FIXTURES = Path(__file__).parents[3] / "fixtures" / "plugins"

CANONICAL_PLUGIN_SCHEMA = (
    "https://agent-plugins.org/schemas/1.0.0/plugin.schema.json"
)


def in_memory_zip(members: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, data in members.items():
            zf.writestr(name, data)
    return buf.getvalue()


def fixture_zip(name: str) -> bytes:
    root = FIXTURES / name
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for path in sorted(root.rglob("*")):
            if path.is_file():
                zf.writestr(str(path.relative_to(root)), path.read_bytes())
    return buf.getvalue()


def entry(package, key):
    return next(p for p in package.entry_plans if p.key == key)


def skill(package, directory):
    return next(p for p in package.skill_plans if p.dir == directory)


class TestValidAnchor:
    """The everything-at-once package: every component class in one archive."""

    def test_valid_full(self):
        package = validate_package(fixture_zip("valid-full"))

        assert package.name == "valid-full"
        assert package.version == "1.0.0"

        local = entry(package, "local")
        assert local.installable and local.transport == "stdio"

        remote = entry(package, "remote")
        assert remote.installable and remote.transport == "http"
        # Validation records the bind but writes nothing: whether the user has
        # granted this plugin that name is a question only the install path can
        # answer, so the plan stays literal until it does.
        assert "Authorization" not in (remote.config.get("headers") or {})
        materialize_binds(package.extension, package.entry_plans, {"FX_KEY"})
        assert (
            remote.config["headers"]["Authorization"] == "${vault:FX_KEY}"
        )

        legacy = entry(package, "legacy")
        assert legacy.transport == "sse" and not legacy.installable

        forbidden = entry(package, "forbidden")
        assert forbidden.skip_code == "policy"

        fx = skill(package, "fx-notes")
        assert fx.skip_code is None and fx.zip_bytes

        codes = {d.code for d in package.diagnostics}
        assert "unknown_root_key" in codes
        assert package.dropped_files == ["README.md"]

    def test_portable_documents_stay_verbatim(self):
        """Export safety: binds never rewrite the stored spec documents.

        Export regenerates plugin.json/mcp.json from these verbatim copies,
        so a vault ref (or worse, a secret value) entering them would leak
        into every exported package.
        """
        package = validate_package(fixture_zip("valid-full"))
        original = json.loads((FIXTURES / "valid-full" / "mcp.json").read_bytes())
        assert package.mcp_document == original
        assert "${vault:" not in json.dumps(package.mcp_document)
        original_manifest = json.loads(
            (FIXTURES / "valid-full" / "plugin.json").read_bytes()
        )
        assert package.manifest == original_manifest


class TestPluginManifestLadder:
    def test_unknown_root_key_warns_only(self):
        package = validate_package(fixture_zip("warn-unknown-root-key"))
        diags = [d for d in package.diagnostics if d.code == "unknown_root_key"]
        assert diags and all(d.level == "warning" for d in diags)

    def test_extensions_not_object_warns_only(self):
        package = validate_package(fixture_zip("warn-extensions-not-object"))
        diags = [d for d in package.diagnostics if d.code == "extensions_invalid"]
        assert diags and all(d.level == "warning" for d in diags)
        assert not package.extension.secrets

    def test_bad_name_is_fatal(self):
        with pytest.raises(PluginFatal) as exc:
            validate_package(fixture_zip("fatal-bad-name"))
        assert any(d.code == "manifest_invalid" for d in exc.value.diagnostics)

    def test_unsupported_version_is_fatal_with_clean_message(self):
        with pytest.raises(PluginFatal) as exc:
            validate_package(fixture_zip("fatal-unsupported-version"))
        assert "targets Agent Plugins 9.9.9" in str(exc.value)

    def test_missing_manifest_is_fatal(self):
        with pytest.raises(PluginFatal) as exc:
            validate_package(fixture_zip("fatal-missing-manifest"))
        assert any(d.code == "missing_manifest" for d in exc.value.diagnostics)


class TestMcpComponentLadder:
    def test_document_defect_drops_only_the_mcp_component(self):
        package = validate_package(fixture_zip("mcp-doc-invalid"))
        assert package.mcp_document is None
        assert package.entry_plans == []
        assert any(
            d.scope == "mcp" and d.level == "error"
            for d in package.diagnostics
        )
        # The sibling skill component is untouched.
        assert skill(package, "survivor").skip_code is None

    def test_document_defect_is_distinguishable_from_no_document(self):
        # Update reads "no entry plans" as "the plugin removed every server"
        # and deletes the owned rows. A document that failed to parse must not
        # look like that, or an upstream typo costs the user their servers.
        assert validate_package(fixture_zip("mcp-doc-invalid")).mcp_document_invalid
        assert not validate_package(fixture_zip("valid-full")).mcp_document_invalid

    def test_embedded_credential_never_reaches_the_stored_document(self):
        # The spec forbids embedded secrets; the import path vaults any it
        # finds, but it rewrites the entry plan, not the document we persist.
        # An unscrubbed document is a second plaintext copy that /export hands
        # straight back.
        raw = in_memory_zip({
            "plugin.json": json.dumps({
                "$schema": CANONICAL_PLUGIN_SCHEMA,
                "name": "leaky",
                "version": "1.0.0",
            }).encode(),
            "mcp.json": json.dumps({
                "$schema": (
                    "https://agent-plugins.org/schemas/1.0.0/mcp.schema.json"
                ),
                "mcpServers": {
                    "svc": {
                        "type": "streamable-http",
                        "url": "https://example.com/mcp",
                        "headers": {
                            "Authorization": "Bearer sk-live-abcdef0123456789",
                            "X-Region": "eu",
                        },
                    }
                },
            }).encode(),
        })
        package = validate_package(raw)
        stored = package.mcp_document["mcpServers"]["svc"]["headers"]
        assert stored["Authorization"] == ""
        # The slot survives so a re-import still asks for the value, and a
        # benign literal beside it is left alone.
        assert "Authorization" in stored
        assert stored["X-Region"] == "eu"
        # The plan keeps the real value: the vault extraction runs off this
        # copy, so scrubbing the document must not disarm it.
        plan = entry(package, "svc")
        assert plan.config["headers"]["Authorization"].endswith("abcdef0123456789")

    def test_stdio_arg_credential_is_scrubbed_like_env_and_headers(self):
        # A stdio entry carries its credential in argv, not in env — the same
        # shape plan_vault_extraction lifts, so the document owes it the same
        # blanking. Scrubbing env and headers alone left this door open.
        raw = in_memory_zip({
            "plugin.json": json.dumps({
                "$schema": CANONICAL_PLUGIN_SCHEMA,
                "name": "argleak",
                "version": "1.0.0",
            }).encode(),
            "mcp.json": json.dumps({
                "$schema": (
                    "https://agent-plugins.org/schemas/1.0.0/mcp.schema.json"
                ),
                "mcpServers": {
                    "svc": {
                        "type": "stdio",
                        "command": "npx",
                        "args": [
                            "-y",
                            "some-server",
                            "--token=sk-live-abcdef0123456789",
                            "--region=eu",
                        ],
                    }
                },
            }).encode(),
        })
        package = validate_package(raw)
        stored = package.mcp_document["mcpServers"]["svc"]["args"]
        # Flag kept, value gone; the benign flag and the positionals untouched.
        assert stored == ["-y", "some-server", "--token=", "--region=eu"]
        plan = entry(package, "svc")
        assert any(
            a.endswith("abcdef0123456789") for a in plan.config["args"]
        )

    def test_a_vault_reference_is_not_mistaken_for_a_literal(self):
        # ``Bearer ${vault:X}`` is a reference the user already set up, not an
        # embedded secret. Blanking it would silently break a working server on
        # every re-validation, so the scrub must match the extractor's verdict.
        raw = in_memory_zip({
            "plugin.json": json.dumps({
                "$schema": CANONICAL_PLUGIN_SCHEMA,
                "name": "reffed",
                "version": "1.0.0",
            }).encode(),
            "mcp.json": json.dumps({
                "$schema": (
                    "https://agent-plugins.org/schemas/1.0.0/mcp.schema.json"
                ),
                "mcpServers": {
                    "svc": {
                        "type": "streamable-http",
                        "url": "https://example.com/mcp",
                        "headers": {"Authorization": "Bearer ${vault:SVC_KEY}"},
                    }
                },
            }).encode(),
        })
        package = validate_package(raw)
        stored = package.mcp_document["mcpServers"]["svc"]["headers"]
        assert stored["Authorization"] == "Bearer ${vault:SVC_KEY}"

    def test_a_vault_ref_in_stdio_args_is_diagnosed(self):
        # The ref syntax in argv hands this account's secret to whatever the
        # command fetches. Scanning env and headers alone left that case, the
        # loudest one, with no diagnostic at all.
        raw = in_memory_zip({
            "plugin.json": json.dumps({
                "$schema": CANONICAL_PLUGIN_SCHEMA,
                "name": "argref",
                "version": "1.0.0",
            }).encode(),
            "mcp.json": json.dumps({
                "$schema": (
                    "https://agent-plugins.org/schemas/1.0.0/mcp.schema.json"
                ),
                "mcpServers": {
                    "svc": {
                        "type": "stdio",
                        "command": "npx",
                        "args": ["-y", "pkg", "--key=${vault:POLYGON_API_KEY}"],
                    }
                },
            }).encode(),
        })
        plan = entry(validate_package(raw), "svc")
        assert any(
            d.code == "vault_ref_in_portable" for d in plan.diagnostics
        )

    def test_entry_defect_skips_only_that_entry(self):
        package = validate_package(fixture_zip("entry-schema-error"))
        assert entry(package, "no-url").skip_code == "schema"
        assert entry(package, "dup-headers").skip_code == "duplicate_header"
        assert entry(package, "good").installable

    def test_disallowed_command_is_a_policy_skip(self):
        package = validate_package(fixture_zip("entry-policy-command"))
        assert entry(package, "sh").skip_code == "policy"
        assert entry(package, "ok").installable

    def test_plugin_tree_entries_are_skipped(self):
        package = validate_package(fixture_zip("entry-plugin-tree"))
        assert entry(package, "rooted").skip_code == "plugin_tree_unsupported"
        assert entry(package, "cwded").skip_code == "plugin_tree_unsupported"

    def test_vault_ref_in_portable_field_warns_and_stays_literal(self):
        package = validate_package(fixture_zip("vault-ref-portable"))
        plan = entry(package, "r")
        assert plan.installable
        assert plan.config["headers"]["Authorization"] == "${vault:MY_KEY}"
        assert any(
            d.code == "vault_ref_in_portable" for d in plan.diagnostics
        )


class TestSkillLadder:
    def test_skill_defects_isolate_per_skill(self):
        package = validate_package(fixture_zip("skill-defects"))
        assert skill(package, "broken").skip_code == "missing_skill_md"
        assert skill(package, "good").skip_code is None
        assert any(d.code == "loose_file" for d in package.diagnostics)


class TestExtensionLadder:
    def test_bind_to_unknown_server_is_fatal(self):
        with pytest.raises(PluginFatal) as exc:
            validate_package(fixture_zip("fatal-ext-bind-unknown"))
        assert "ghost" in str(exc.value)


class TestDiscoveryAndDialects:
    """Marketplace-repo traversal + vendor-dialect adaptation.

    Modeled on the three public marketplaces (openai/plugins, cursor/plugins,
    anthropics/claude-plugins-official): manifests live in ``.codex-plugin``/
    ``.cursor-plugin``/``.claude-plugin`` dirs under per-plugin subdirectories,
    never carry ``$schema``, and the claude layout ships an ``.mcp.json``
    whose credentials are ``${VAR}`` environment references.
    """

    def test_multi_plugin_archive_is_ambiguous_with_candidates(self):
        with pytest.raises(PluginAmbiguous) as exc:
            validate_package(fixture_zip("marketplace-mixed"))
        by_path = {c.path: c for c in exc.value.candidates}
        assert set(by_path) == {
            "plugins/alpha",
            "external_plugins/ctx",
            "plugins/widget",
        }
        assert by_path["plugins/alpha"].dialect == "codex"
        assert by_path["plugins/alpha"].name == "alpha"
        assert by_path["plugins/alpha"].version == "0.1.0"
        assert by_path["external_plugins/ctx"].dialect == "claude"

    def test_marketplace_index_enriches_and_adds_external_entries(self):
        with pytest.raises(PluginAmbiguous) as exc:
            validate_package(fixture_zip("marketplace-mixed"))
        by_path = {c.path: c for c in exc.value.candidates}
        # alpha's own manifest has no description; the index fills the gap.
        assert by_path["plugins/alpha"].description == (
            "Alpha, as the marketplace describes it."
        )
        assert by_path["plugins/alpha"].source_url is None
        remote = by_path["plugins/widget"]
        assert remote.dialect == "external"
        assert remote.name == "remote-widget"
        assert remote.source_url == (
            "https://github.com/acme/widgets/tree/v1.0.0/plugins/widget"
        )
        # The composed URL parses straight back to (archive, subdir).
        archive, subdir = normalize_forge_url(remote.source_url)
        assert archive.endswith("/tar.gz/v1.0.0")
        assert subdir == "plugins/widget"

    def test_external_entry_never_wins_selection(self):
        # subdir matching and single-candidate auto-select both skip
        # external entries — their bytes are in another repo.
        with pytest.raises(PluginAmbiguous):
            validate_package(
                fixture_zip("marketplace-mixed"), subdir="plugins/widget"
            )
        raw = in_memory_zip({
            "marketplace.json": json.dumps({
                "plugins": [
                    {
                        "name": "only-remote",
                        "source": {
                            "source": "git",
                            "url": "https://github.com/acme/solo",
                        },
                    }
                ]
            }).encode(),
        })
        with pytest.raises(PluginAmbiguous) as exc:
            validate_package(raw)
        assert [c.source_url for c in exc.value.candidates] == [
            "https://github.com/acme/solo/tree/HEAD"
        ]

    def test_external_entry_shadowed_by_vendored_copy(self):
        raw = in_memory_zip({
            "vendored/.codex-plugin/plugin.json": b'{"name": "dup"}',
            "other/.codex-plugin/plugin.json": b'{"name": "other"}',
            "marketplace.json": json.dumps({
                "plugins": [
                    {
                        "name": "dup",
                        "source": {
                            "source": "git-subdir",
                            "url": "https://github.com/acme/dup.git",
                            "path": "plugins/dup",
                        },
                    }
                ]
            }).encode(),
        })
        with pytest.raises(PluginAmbiguous) as exc:
            validate_package(raw)
        assert {c.path for c in exc.value.candidates} == {"vendored", "other"}
        assert all(c.source_url is None for c in exc.value.candidates)

    def test_subdir_selects_and_adapts(self):
        package = validate_package(
            fixture_zip("marketplace-mixed"), subdir="plugins/alpha"
        )
        assert package.name == "alpha"
        assert package.manifest["$schema"] == CANONICAL_PLUGIN_SCHEMA
        assert skill(package, "alpha-notes").skip_code is None
        assert any(d.code == "dialect_adapted" for d in package.diagnostics)

    def test_claude_dialect_maps_env_refs_to_declared_secrets(self):
        package = validate_package(
            fixture_zip("marketplace-mixed"), subdir="external_plugins/ctx"
        )
        assert package.name == "ctx"
        assert package.manifest["author"] == {"name": "Acme Docs"}

        remote = entry(package, "ctx")
        local = entry(package, "local")
        assert remote.installable and remote.transport == "http"
        assert local.installable and local.transport == "stdio"

        # The pure ${VAR} references left the portable document...
        doc = package.mcp_document
        assert "headers" not in doc["mcpServers"]["ctx"]
        assert doc["mcpServers"]["local"]["env"] == {"MODE": "fast"}
        # ...became declared ai.langalpha secrets...
        assert {s.name for s in package.extension.secrets} == {
            "CTX_API_KEY",
            "CTX_TOKEN",
        }
        # ...and bind back onto the plans as vault references once granted.
        materialize_binds(
            package.extension,
            package.entry_plans,
            {"CTX_API_KEY", "CTX_TOKEN"},
        )
        assert (
            remote.config["headers"]["Authorization"] == "${vault:CTX_API_KEY}"
        )
        assert local.config["env"]["CTX_TOKEN"] == "${vault:CTX_TOKEN}"
        assert local.config["env"]["MODE"] == "fast"
        assert any(d.code == "env_ref_mapped" for d in package.diagnostics)

    def test_root_vendor_manifest_is_a_root_plugin(self):
        package = validate_package(fixture_zip("dialect-cursor"))
        assert package.name == "cli-tips"
        assert skill(package, "cli-tips").skip_code is None
        # Vendor extras (displayName, category) ride the tolerated warn rung.
        assert any(
            d.code == "unknown_root_key" for d in package.diagnostics
        )

    def test_subdir_miss_is_ambiguous_with_fresh_candidates(self):
        with pytest.raises(PluginAmbiguous) as exc:
            validate_package(
                fixture_zip("marketplace-mixed"), subdir="plugins/gone"
            )
        assert exc.value.candidates

    def test_root_plugin_shadows_nested_candidates(self):
        raw = in_memory_zip({
            "plugin.json": TestArchiveHardening.MANIFEST,
            "vendored/other/.claude-plugin/plugin.json": b"{}",
        })
        package = validate_package(raw)
        assert package.name == "hardened"

    def test_nested_candidate_inside_a_candidate_is_dropped(self):
        raw = in_memory_zip({
            "a/.codex-plugin/plugin.json": b'{"name": "outer"}',
            "a/inner/.codex-plugin/plugin.json": b'{"name": "inner"}',
            "b/.codex-plugin/plugin.json": b'{"name": "b"}',
        })
        with pytest.raises(PluginAmbiguous) as exc:
            validate_package(raw)
        assert {c.path for c in exc.value.candidates} == {"a", "b"}


class TestForgeUrlSubpaths:
    def test_github_tree_url_carries_the_subpath(self):
        archive, subdir = normalize_forge_url(
            "https://github.com/openai/plugins/tree/main/plugins/alpha"
        )
        assert archive == (
            "https://codeload.github.com/openai/plugins/tar.gz/main"
        )
        assert subdir == "plugins/alpha"

    def test_gitlab_tree_url_carries_the_subpath(self):
        archive, subdir = normalize_forge_url(
            "https://gitlab.com/group/proj/-/tree/main/plugins/x"
        )
        assert "sha=main" in archive
        assert subdir == "plugins/x"

    def test_fragment_subdir_on_bare_repo_url(self):
        archive, subdir = normalize_forge_url(
            "https://github.com/openai/plugins#subdir=plugins/alpha"
        )
        assert archive.endswith("/tar.gz/HEAD")
        assert subdir == "plugins/alpha"

    def test_fragment_subdir_on_direct_archive_url(self):
        archive, subdir = normalize_forge_url(
            "https://example.com/pkg.zip#subdir=sub/dir"
        )
        assert archive == "https://example.com/pkg.zip"
        assert subdir == "sub/dir"

    def test_compose_roundtrips_through_normalize(self):
        url = compose_subdir_url(
            "https://github.com/openai/plugins", "plugins/alpha"
        )
        _archive, subdir = normalize_forge_url(url)
        assert subdir == "plugins/alpha"

    def test_escaping_subdir_is_refused(self):
        with pytest.raises(ValueError):
            normalize_forge_url("https://github.com/o/r#subdir=../evil")


class TestArchiveHardening:
    """Attacks that cannot be represented as fixture file trees."""

    @staticmethod
    def _zip(members: dict[str, bytes]) -> bytes:
        return in_memory_zip(members)

    def test_a_corrupt_payload_is_a_refusal_not_a_crash(self):
        # An interrupted upload is the ordinary way to get a well-formed
        # archive with an unreadable member. zlib.error is not PluginFatal, so
        # before this it escaped the pipeline as a 500 with a stack trace.
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("plugin.json", json.dumps({"name": "x"}) * 200)
        raw = bytearray(buf.getvalue())
        for i in (60, 61, 62):
            raw[i] ^= 0xFF
        with pytest.raises(PluginFatal) as excinfo:
            validate_package(bytes(raw))
        assert any(
            d.code == "unreadable" for d in excinfo.value.diagnostics
        )

    MANIFEST = json.dumps({
        "$schema": "https://agent-plugins.org/schemas/1.0.0/plugin.schema.json",
        "name": "hardened",
    }).encode()

    def test_path_traversal_is_fatal(self):
        raw = self._zip({"plugin.json": self.MANIFEST, "../evil.txt": b"x"})
        with pytest.raises(PluginFatal) as exc:
            validate_package(raw)
        assert any(d.code == "member_escape" for d in exc.value.diagnostics)

    def test_absolute_member_is_fatal(self):
        raw = self._zip({"plugin.json": self.MANIFEST, "/etc/evil": b"x"})
        with pytest.raises(PluginFatal) as exc:
            validate_package(raw)
        assert any(d.code == "member_escape" for d in exc.value.diagnostics)

    def test_encrypted_member_is_fatal(self):
        # zipfile clears flag bits on write, so set the encryption bit by
        # patching the raw headers (flags at offset 6 in the local header,
        # 8 in the central directory; the filename follows at 30/46).
        raw = bytearray(
            self._zip({"plugin.json": self.MANIFEST, "secret.bin": b"x"})
        )
        for sig, flag_off, name_off in (
            (b"PK\x03\x04", 6, 30),
            (b"PK\x01\x02", 8, 46),
        ):
            start = 0
            while (idx := raw.find(sig, start)) != -1:
                if raw[idx + name_off : idx + name_off + 10] == b"secret.bin":
                    raw[idx + flag_off] |= 0x1
                start = idx + 4
        with pytest.raises(PluginFatal) as exc:
            validate_package(bytes(raw))
        assert any(
            d.code == "encrypted_member" for d in exc.value.diagnostics
        )

    def test_member_count_cap_is_fatal(self):
        from src.server.services.plugins.archive import MAX_MEMBERS

        members = {"plugin.json": self.MANIFEST}
        members.update(
            {f"skills/x/f{i}.txt": b"" for i in range(MAX_MEMBERS + 1)}
        )
        with pytest.raises(PluginFatal) as exc:
            validate_package(self._zip(members))
        assert any(
            d.code == "too_many_members" for d in exc.value.diagnostics
        )

    def test_garbage_bytes_are_fatal(self):
        with pytest.raises(PluginFatal) as exc:
            validate_package(b"\x00\x01not-an-archive")
        assert any(d.code == "unreadable" for d in exc.value.diagnostics)

    def test_single_root_dir_is_stripped(self):
        """Forge tarballs wrap everything in <repo>-<sha>/ — install must
        see through it."""
        raw = self._zip({
            "repo-abc123/plugin.json": self.MANIFEST,
            "repo-abc123/skills/s/SKILL.md": (
                b"---\nname: s\ndescription: d\n---\nbody\n"
            ),
        })
        package = validate_package(raw)
        assert package.name == "hardened"
        assert skill(package, "s").skip_code is None
