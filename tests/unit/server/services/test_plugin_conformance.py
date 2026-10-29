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
import random
import stat
import tarfile
import tracemalloc
import zipfile
from pathlib import Path

from unittest.mock import AsyncMock, patch

import pytest

from src.server.models.mcp_server import McpServerInput
from src.server.services.plugins import archive
from src.server.services.plugins.bare_skills import plugin_name_from_hint
from src.server.services.plugins.errors import PluginAmbiguous, PluginFatal
from src.server.services.plugins.source import (
    PackageSource,
    name_hint,
    validate_source,
)
from src.server.services.plugins.fetch import (
    compose_subdir_url,
    normalize_forge_url,
)
from src.server.services.plugins import validate_package
from src.server.services.mcp_import import plan_vault_extraction
from src.server.services.plugins.export import export_plugin_zip
from src.server.services.plugins.extension import SecretBind, materialize_binds
from src.server.services.plugins.grants import secret_names_in, strip_refs
from tests.unit.server.mcp_builders import zip_package

FIXTURES = Path(__file__).parents[3] / "fixtures" / "plugins"

CANONICAL_PLUGIN_SCHEMA = (
    "https://agent-plugins.org/schemas/1.0.0/plugin.schema.json"
)


def fixture_zip(name: str) -> bytes:
    root = FIXTURES / name
    return zip_package({
        str(path.relative_to(root)): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    })


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
        # Validation records the bind but writes nothing: which names the user
        # already held is a question only the install path can answer, so the
        # plan stays literal until it does.
        assert "Authorization" not in (remote.config.get("headers") or {})
        materialize_binds(package.extension, package.entry_plans, {"FX_KEY"})
        assert (
            remote.config["headers"]["Authorization"] == "${vault:FX_KEY}"
        )

        legacy = entry(package, "legacy")
        assert legacy.transport == "sse" and not legacy.installable

        # The fixture's shell entry: nothing about the command is filtered.
        assert entry(package, "forbidden").installable

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
        # Every dialect puts something here, so this warning shows on nearly
        # every real install and has to read like the benign thing it is: it
        # names the key and says the spec's own answer was carried out.
        assert "'homepage2'" in diags[0].message
        assert "Additional properties" not in diags[0].message

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
        # The fixture declares a bind on an entry the dropped document would
        # have carried. That is the shape that used to escalate: with no plans
        # to resolve against, every bind reads as naming an unknown server, so
        # a component defect refused the whole install with a message blaming
        # a server the package does declare.
        package = validate_package(fixture_zip("mcp-doc-invalid"))
        assert package.mcp_document is None
        assert package.entry_plans == []
        assert any(
            d.scope == "mcp" and d.level == "error"
            for d in package.diagnostics
        )
        # The sibling skill component is untouched.
        assert skill(package, "survivor").skip_code is None

    def test_a_bind_naming_a_server_the_document_lacks_is_reported(self):
        # The bind is the package's own error, but it is an error in an
        # optional block of our own invention. Losing every server and every
        # skill over it was the wrong trade; the bind is dropped and named.
        raw = zip_package({
            "plugin.json": json.dumps({
                "$schema": CANONICAL_PLUGIN_SCHEMA,
                "name": "bind-typo",
                "version": "1.0.0",
                "extensions": {"ai.langalpha": {"secrets": [{
                    "name": "T", "label": "T",
                    "bind": [{"server": "absent", "header": "Authorization"}],
                }]}},
            }).encode(),
            "mcp.json": json.dumps({
                "$schema": (
                    "https://agent-plugins.org/schemas/1.0.0/mcp.schema.json"
                ),
                "mcpServers": {
                    "api": {
                        "type": "streamable-http",
                        "url": "https://api.example/mcp",
                    }
                },
            }).encode(),
        })
        package = validate_package(raw)
        assert entry(package, "api").installable
        assert any(
            d.code == "bind_unusable" and "unknown server" in d.message
            for d in package.diagnostics
        )

    def test_document_defect_is_distinguishable_from_no_document(self):
        # Update reads "no entry plans" as "the plugin removed every server"
        # and deletes the owned rows. A document that failed to parse must not
        # look like that, or an upstream typo costs the user their servers.
        assert validate_package(fixture_zip("mcp-doc-invalid")).mcp_document_invalid
        assert not validate_package(fixture_zip("valid-full")).mcp_document_invalid

    @pytest.mark.parametrize(
        "document",
        [
            '{"mcpServers": ' + "[" * 100_000 + "]" * 100_000 + "}",
            '{"mcpServers": {"api\\ud800": {"url": "https://api.example/mcp"}}}',
        ],
        ids=["nested-past-the-stack", "lone-surrogate"],
    )
    def test_an_mcp_json_no_parser_can_hold_drops_only_that_component(
        self, document
    ):
        package = validate_package(zip_package({
            "plugin.json": json.dumps({
                "$schema": CANONICAL_PLUGIN_SCHEMA,
                "name": "unreadable-mcp",
                "version": "1.0.0",
            }).encode(),
            "mcp.json": document.encode(),
        }))
        assert package.mcp_document is None
        assert package.mcp_document_invalid
        assert any(
            d.scope == "mcp" and d.code == "invalid_json"
            for d in package.diagnostics
        )

    def test_embedded_credential_never_reaches_the_stored_document(self):
        # The spec forbids embedded secrets; the import path vaults any it
        # finds, but it rewrites the entry plan, not the document we persist.
        # An unscrubbed document is a second plaintext copy that /export hands
        # straight back.
        raw = zip_package({
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
        #
        # Both argv shapes, because they were once handled differently: the
        # space-separated pair was left alone on the grounds that nothing could
        # tell it from a positional, which the benign entries below disprove —
        # the flag naming the value is signal enough.
        raw = zip_package({
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
                            "--secret",
                            "sk-live-9999999999999999",
                            "--port",
                            "8080",
                            "/data/a-very-long-looking-output-path.json",
                        ],
                    }
                },
            }).encode(),
        })
        package = validate_package(raw)
        stored = package.mcp_document["mcpServers"]["svc"]["args"]
        # Flag kept, value gone; the benign flag and the positionals untouched.
        assert stored == [
            "-y",
            "some-server",
            "--token=",
            "--region=eu",
            "--secret",
            "",
            "--port",
            "8080",
            "/data/a-very-long-looking-output-path.json",
        ]
        plan = entry(package, "svc")
        assert any(
            a.endswith("abcdef0123456789") for a in plan.config["args"]
        )
        assert any(
            a.endswith("9999999999999999") for a in plan.config["args"]
        )

    def test_a_vault_reference_is_not_mistaken_for_a_literal(self):
        # ``Bearer ${vault:X}`` is a reference the user already set up, not an
        # embedded secret. Blanking it would silently break a working server on
        # every re-validation, so the scrub must match the extractor's verdict.
        raw = zip_package({
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

        # The other half of "must match the extractor's verdict", and the half
        # this test used to leave unasserted while the two lanes disagreed:
        # the extractor matched the ref with `fullmatch`, so a value that only
        # CONTAINED one read as a credential literal and was vaulted whole.
        # The entry then authenticated with the string "Bearer ${vault:SVC_KEY}"
        # under a freshly minted name, and the user's own SVC_KEY was never
        # consulted.
        plan = entry(package, "svc")
        entry_plan = plan_vault_extraction(
            "svc", plan.config, allocated={}, used_secret_names=set()
        )
        assert entry_plan.secrets == ()
        assert plan.config["headers"]["Authorization"] == (
            "Bearer ${vault:SVC_KEY}"
        )

    def test_a_vault_ref_in_stdio_args_is_diagnosed(self):
        # The ref syntax in argv hands this account's secret to whatever the
        # command fetches. Scanning env and headers alone left that case, the
        # loudest one, with no diagnostic at all.
        raw = zip_package({
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

    def test_a_key_the_sandbox_reserves_installs_renamed(self):
        # The key becomes a module name in the sandbox. A package author has
        # no reason to know that, so the entry lands under a legal name and
        # reports the rename instead of being skipped.
        raw = zip_package({
            "plugin.json": json.dumps({
                "$schema": CANONICAL_PLUGIN_SCHEMA,
                "name": "reserved-keys",
                "version": "1.0.0",
            }).encode(),
            "mcp.json": json.dumps({
                "$schema": (
                    "https://agent-plugins.org/schemas/1.0.0/mcp.schema.json"
                ),
                "mcpServers": {
                    "class": {"type": "stdio", "command": "npx"},
                    "mcp_client": {
                        "type": "streamable-http",
                        "url": "https://api.example.com/mcp",
                    },
                    "__init__": {"type": "stdio", "command": "npx"},
                },
            }).encode(),
        })
        package = validate_package(raw)
        for key, name in (
            ("class", "class_server"),
            ("mcp_client", "mcp_client_server"),
            ("__init__", "init__"),
        ):
            plan = entry(package, key)
            assert plan.installable, plan.skip_reason
            assert (plan.name, plan.renamed) == (name, True)
            assert McpServerInput(**plan.config).name == name

    def test_a_shell_command_installs_like_any_other(self):
        # The command reaches an argv list in the user's own sandbox, where the
        # agent already runs whatever it likes. Refusing it here only decided
        # which published servers were installable.
        package = validate_package(fixture_zip("entry-policy-command"))
        assert entry(package, "sh").installable
        assert entry(package, "ok").installable

    def test_only_plugin_tree_vars_are_held_back(self):
        package = validate_package(fixture_zip("entry-plugin-tree"))
        assert entry(package, "rooted").skip_code == "plugin_tree_unsupported"
        # cwd is dropped rather than the entry: the path it names is almost
        # never what makes the server work, and the warning says it was ignored.
        cwded = entry(package, "cwded")
        assert cwded.installable
        assert "cwd" not in cwded.config
        assert any(d.code == "cwd_ignored" for d in cwded.diagnostics)

    def test_vault_ref_in_portable_field_warns_and_stays_literal(self):
        package = validate_package(fixture_zip("vault-ref-portable"))
        plan = entry(package, "r")
        assert plan.installable
        assert plan.config["headers"]["Authorization"] == "${vault:MY_KEY}"
        assert any(
            d.code == "vault_ref_in_portable" for d in plan.diagnostics
        )

    def test_a_ref_whose_name_cannot_resolve_says_so(self):
        """A hyphen is the usual typo, and it makes the text a literal rather
        than a reference. Reporting that as "kept as written and resolved" is
        the one sentence that stops the author from spotting it."""
        raw = zip_package({
            "plugin.json": json.dumps({
                "$schema": CANONICAL_PLUGIN_SCHEMA, "name": "typo-ref",
                "version": "1.0.0",
            }).encode(),
            "mcp.json": json.dumps({
                "$schema": (
                    "https://agent-plugins.org/schemas/1.0.0/mcp.schema.json"
                ),
                "mcpServers": {
                    "typo": {
                        "type": "streamable-http",
                        "url": "https://api.example.com/mcp",
                        "headers": {"Authorization": "${vault:my-token}"},
                    },
                    "fine": {
                        "type": "streamable-http",
                        "url": "https://api.example.com/mcp",
                        "headers": {"Authorization": "${vault:GOOD_TOKEN}"},
                    },
                },
            }).encode(),
        })
        package = validate_package(raw)
        typo = {d.code for d in entry(package, "typo").diagnostics}
        assert "vault_ref_malformed" in typo
        assert "vault_ref_in_portable" not in typo
        assert {d.code for d in entry(package, "fine").diagnostics} == {
            "vault_ref_in_portable"
        }


class TestSkillLadder:
    def test_skill_defects_isolate_per_skill(self):
        package = validate_package(fixture_zip("skill-defects"))
        assert skill(package, "broken").skip_code == "missing_skill_md"
        assert skill(package, "good").skip_code is None
        assert any(d.code == "loose_file" for d in package.diagnostics)


class TestExtensionLadder:
    def test_bind_to_unknown_server_drops_the_bind_not_the_package(self):
        package = validate_package(fixture_zip("fatal-ext-bind-unknown"))
        assert any(
            d.code == "bind_unusable" and "ghost" in d.message
            for d in package.diagnostics
        )


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
        raw = zip_package({
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
        raw = zip_package({
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

    def test_every_way_a_vendor_declares_its_servers_is_read(self):
        """Four layouts in the wild, and only the first used to be found — a
        plugin using any of the others installed with none of its servers, its
        only hint a warning that ``mcpServers`` was an unexpected key."""
        server = {"command": "npx", "args": ["-y", "srv"]}
        layouts = {
            "root .mcp.json, no manifest key": (
                {}, {".mcp.json": {"mcpServers": {"srv": server}}},
            ),
            "manifest names the file": (
                {"mcpServers": "./config/servers.json"},
                {"config/servers.json": {"mcpServers": {"srv": server}}},
            ),
            "manifest carries the servers": (
                {"mcpServers": {"srv": server}}, {},
            ),
            "document inside the vendor dir": (
                {}, {".claude-plugin/.mcp.json": {"mcpServers": {"srv": server}}},
            ),
        }
        for label, (manifest_extra, documents) in layouts.items():
            members = {
                ".claude-plugin/plugin.json": json.dumps(
                    {"name": "vendored", "version": "1.0.0", **manifest_extra}
                ).encode(),
            }
            for path, doc in documents.items():
                members[path] = json.dumps(doc).encode()
            package = validate_package(zip_package(members))
            plan = entry(package, "srv")
            assert plan.installable, label
            assert plan.config["command"] == "npx", label

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
        raw = zip_package({
            "plugin.json": TestArchiveHardening.MANIFEST,
            "skills/s/SKILL.md": SKILL_MD,
            "vendored/other/.claude-plugin/plugin.json": b"{}",
        })
        package = validate_package(raw)
        assert package.name == "hardened"

    def test_a_root_that_carries_nothing_offers_what_it_was_hiding(self):
        """The rule above is what a marketplace cover page hides behind.

        wshobson/agents ships a root ``.cursor-plugin/plugin.json`` holding a
        name, a version and no components at all, beside a marketplace index
        listing ninety-one real plugins. Root-wins installed the cover page:
        a 201, a plugin slot spent, a card in the list, and nothing whatsoever
        to run. Nothing in the tree separates that from a genuine root plugin
        — only having validated it and found it empty does.
        """
        raw = zip_package({
            "plugin.json": TestArchiveHardening.MANIFEST,
            "plugins/real/.claude-plugin/plugin.json": b'{"name": "real"}',
            "plugins/real/skills/s/SKILL.md": SKILL_MD,
        })
        with pytest.raises(PluginAmbiguous) as exc:
            validate_package(raw)
        assert [c.path for c in exc.value.candidates] == ["plugins/real"]
        # The caller that is not asking a human installs this instead.
        assert exc.value.fallback_path == ""

        # And the path it offered actually resolves, which root-wins used to
        # prevent: the chooser would list plugins it then refused to install.
        package = validate_package(raw, subdir="plugins/real")
        assert package.name == "real"
        assert skill(package, "s").skip_code is None

    def test_a_package_with_nothing_to_offer_says_it_installs_nothing(self):
        """An editor extension or a hosted app, packaged as a plugin. There is
        no better candidate to fall through to, so the install stands and the
        report is the only place the user learns it is inert."""
        raw = zip_package({
            "plugin.json": TestArchiveHardening.MANIFEST,
            "assets/logo.png": b"\x89PNG",
        })
        package = validate_package(raw)
        empty = next(d for d in package.diagnostics if d.code == "no_components")
        assert empty.level == "warning"
        assert "assets" in empty.message

    def test_nested_candidate_inside_a_candidate_is_dropped(self):
        raw = zip_package({
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

    def test_a_truncated_tarball_is_a_refusal_at_every_cut(self):
        """Truncation surfaces in three places, and every one owes a 422.

        A .tar.gz cut short raises from open (gzip magic gone), from the
        header advance, or from the member read, and the first two raise
        EOFError, which is not a ValueError and so never reaches the router's
        conflict mapping. Swept rather than sampled because which of the three
        fires depends on where the cut lands.
        """
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w:gz") as tf:
            for name, data in (
                ("plugin.json", self.MANIFEST),
                ("skills/s/SKILL.md", b"---\nname: s\ndescription: d\n---\nb\n"),
                ("README.md", b"pad" * 4000),
            ):
                info = tarfile.TarInfo(name)
                info.size = len(data)
                tf.addfile(info, io.BytesIO(data))
        raw = buf.getvalue()

        for cut in range(1, len(raw), 7):
            try:
                validate_package(raw[:cut])
            except PluginFatal:
                pass
            except Exception as e:  # noqa: BLE001 - the point of the test
                pytest.fail(f"cut at {cut} escaped as {type(e).__name__}: {e}")

    def test_path_traversal_is_fatal(self):
        raw = zip_package({"plugin.json": self.MANIFEST, "../evil.txt": b"x"})
        with pytest.raises(PluginFatal) as exc:
            validate_package(raw)
        assert any(d.code == "member_escape" for d in exc.value.diagnostics)

    def test_absolute_member_is_fatal(self):
        raw = zip_package({"plugin.json": self.MANIFEST, "/etc/evil": b"x"})
        with pytest.raises(PluginFatal) as exc:
            validate_package(raw)
        assert any(d.code == "member_escape" for d in exc.value.diagnostics)

    def test_encrypted_member_is_fatal(self):
        # zipfile clears flag bits on write, so set the encryption bit by
        # patching the raw headers (flags at offset 6 in the local header,
        # 8 in the central directory; the filename follows at 30/46).
        raw = bytearray(
            zip_package({"plugin.json": self.MANIFEST, "secret.bin": b"x"})
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
            validate_package(zip_package(members))
        assert any(
            d.code == "too_many_members" for d in exc.value.diagnostics
        )

    def test_garbage_bytes_are_fatal(self):
        with pytest.raises(PluginFatal) as exc:
            validate_package(b"\x00\x01not-an-archive")
        assert any(d.code == "unreadable" for d in exc.value.diagnostics)

    @staticmethod
    def _bomb(*, symlink: bool, lie: bool, payload: int = 16 * 1024 * 1024):
        """A ~16 KiB zip carrying a member that inflates to ``payload``."""
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("plugin.json", TestArchiveHardening.MANIFEST)
            info = zipfile.ZipInfo("payload.bin")
            info.compress_type = zipfile.ZIP_DEFLATED
            info.create_system = 3  # Unix, so the mode bits are read at all
            info.external_attr = (
                (stat.S_IFLNK | 0o777) << 16 if symlink else (0o644 << 16)
            )
            zf.writestr(info, b"\0" * payload)
        raw = bytearray(buf.getvalue())
        if lie:
            # The uncompressed-size field, in the local header at offset 22 and
            # in the central directory at 24. Every header guard reads it, and
            # nothing makes it true.
            for sig, size_off, name_off in (
                (b"PK\x03\x04", 22, 30), (b"PK\x01\x02", 24, 46)
            ):
                idx = raw.find(sig)
                while idx != -1:
                    if raw[idx + name_off:idx + name_off + 11] == b"payload.bin":
                        raw[idx + size_off:idx + size_off + 4] = (
                            (20).to_bytes(4, "little")
                        )
                        break
                    idx = raw.find(sig, idx + 4)
        return bytes(raw)

    def test_a_link_target_is_inflated_under_a_cap_of_its_own(self):
        """A link's target is its member's content, so reading one means
        inflating attacker-chosen bytes. The ratio guard sits on the
        regular-file path, and this member never reaches it: the same bomb
        refused for free as a file was inflated in full once it was marked a
        link."""
        with pytest.raises(PluginFatal) as exc:
            validate_package(self._bomb(symlink=True, lie=False))
        assert any(
            d.code == "link_target_too_long" for d in exc.value.diagnostics
        )

    def test_a_lying_size_field_does_not_buy_an_unbounded_read(self):
        """Both header guards are computed from a number the archive wrote.
        ``ZipFile.read`` inflates the whole stream before noticing it
        disagrees with the CRC, so a member declaring twenty bytes still cost
        whatever it really expanded to; the refusal came after the damage."""
        payload = 16 * 1024 * 1024
        raw = self._bomb(symlink=False, lie=True, payload=payload)
        tracemalloc.start()
        try:
            with pytest.raises(PluginFatal):
                validate_package(raw)
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        assert peak < payload // 2

    def test_a_tar_header_extension_cannot_outgrow_the_budget(
        self, monkeypatch
    ):
        """tarfile inflates a GNU long name while parsing the header, before
        the member reaches the loop that checks sizes. Unbounded, a 400 MiB
        name cost 2 GB and escaped as an OSError, which is a 500 for what is a
        malformed upload."""
        monkeypatch.setattr(archive, "MAX_UNCOMPRESSED_BYTES", 1024 * 1024)
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w:gz", format=tarfile.GNU_FORMAT) as tf:
            info = tarfile.TarInfo("plugin.json")
            info.size = len(self.MANIFEST)
            tf.addfile(info, io.BytesIO(self.MANIFEST))
            tf.addfile(tarfile.TarInfo("a" * (4 * 1024 * 1024)))
        with pytest.raises(PluginFatal):
            validate_package(buf.getvalue())

    def test_single_root_dir_is_stripped(self):
        """Forge tarballs wrap everything in <repo>-<sha>/ — install must
        see through it."""
        raw = zip_package({
            "repo-abc123/plugin.json": self.MANIFEST,
            "repo-abc123/skills/s/SKILL.md": (
                b"---\nname: s\ndescription: d\n---\nbody\n"
            ),
        })
        package = validate_package(raw)
        assert package.name == "hardened"
        assert skill(package, "s").skip_code is None

    def test_finder_litter_does_not_hide_the_wrapper(self):
        """Finder's Compress adds a __MACOSX/ tree and .DS_Store files, which
        read as a second root and kept the wrapper from being stripped."""
        raw = zip_package({
            "acme/plugin.json": self.MANIFEST,
            "acme/skills/s/SKILL.md": b"---\nname: s\ndescription: d\n---\nbody\n",
            "acme/.DS_Store": b"\x00\x00\x00\x01Bud1",
            "__MACOSX/acme/._plugin.json": b"\x00\x05\x16\x07",
            "__MACOSX/acme/skills/s/._SKILL.md": b"\x00\x05\x16\x07",
        })
        files, _ = archive.extract_plugin_archive(raw)
        assert sorted(files) == ["plugin.json", "skills/s/SKILL.md"]
        assert validate_package(raw).name == "hardened"


SKILL_MD = b"---\nname: s\ndescription: d\n---\nbody\n"


def zip_with_links(
    files: dict[str, bytes], links: dict[str, str]
) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, data in files.items():
            zf.writestr(name, data)
        for name, target in links.items():
            info = zipfile.ZipInfo(name)
            info.create_system = 3  # Unix, so the mode bits are read at all
            info.external_attr = (stat.S_IFLNK | 0o777) << 16
            zf.writestr(info, target)
    return buf.getvalue()


def tar_with_links(
    files: dict[str, bytes], links: dict[str, tuple[str, bool]]
) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tf:
        for name, data in files.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tf.addfile(info, io.BytesIO(data))
        for name, (target, hard) in links.items():
            info = tarfile.TarInfo(name)
            info.type = tarfile.LNKTYPE if hard else tarfile.SYMTYPE
            info.linkname = target
            tf.addfile(info)
    return buf.getvalue()


class TestSingleRootPrefix:
    @pytest.mark.parametrize(
        ("paths", "prefix"),
        [
            (["agents/a.md", "agents/b.md"], ""),
            (["agents/a.md", "agents/plugin.json"], "agents/"),
            (["agents/a.md", "agents/.claude-plugin/plugin.json"], "agents/"),
            (["agents/a.md", "agents/sub/x.md"], ""),
            (["agents/x.md", "agents/README.txt"], ""),
            (["agents/README.md", "agents/agents/x.md"], "agents/"),
            (["repo-main/agents/a.md", "repo-main/plugin.json"], "repo-main/"),
            (["repo-main/a.md"], "repo-main/"),
        ],
    )
    def test_only_a_wrapper_is_stripped_not_a_lone_agents_folder(self, paths, prefix):
        assert archive._single_root_prefix(paths) == prefix


class TestArchiveLinks:
    """Links inside a package resolve to content; only escapes are dropped.

    Publishing one plugin for several vendors is done with links, so refusing
    a package over any link at all cost real installs: a third of a sample of
    popular plugin repos carry at least one, usually ``CLAUDE.md ->
    AGENTS.md``. Nothing here is ever written to disk, which is what makes
    resolving them safe — the escape a link normally threatens needs a link on
    disk for a later member to be written through.
    """

    MANIFEST = json.dumps({
        "$schema": CANONICAL_PLUGIN_SCHEMA, "name": "linked", "version": "1.0.0",
    }).encode()

    def test_a_link_to_a_sibling_file_carries_its_content(self):
        package = validate_package(zip_with_links(
            {"plugin.json": self.MANIFEST, "skills/s/AGENTS.md": SKILL_MD},
            {"skills/s/SKILL.md": "AGENTS.md"},
        ))
        assert skill(package, "s").skip_code is None

    def test_a_link_to_a_directory_mirrors_the_subtree(self):
        """The whole point of the vendor-alias layout: one skills tree
        published under two names."""
        package = validate_package(tar_with_links(
            {"plugin.json": self.MANIFEST, "vendor/s/SKILL.md": SKILL_MD},
            {"skills": ("vendor", False)},
        ))
        assert skill(package, "s").skip_code is None

    def test_a_hardlink_resolves_from_the_archive_root(self):
        package = validate_package(tar_with_links(
            {"plugin.json": self.MANIFEST, "docs/SKILL.md": SKILL_MD},
            {"skills/s/SKILL.md": ("docs/SKILL.md", True)},
        ))
        assert skill(package, "s").skip_code is None

    def test_a_link_climbing_inside_the_package_still_resolves(self):
        package = validate_package(zip_with_links(
            {"plugin.json": self.MANIFEST, "shared/SKILL.md": SKILL_MD},
            {"skills/s/SKILL.md": "../../shared/SKILL.md"},
        ))
        assert skill(package, "s").skip_code is None

    def test_a_link_leaving_the_package_is_skipped_with_a_diagnostic(self):
        """The rest of the package installs. A file that silently went missing
        is worse than one the report names."""
        package = validate_package(zip_with_links(
            {"plugin.json": self.MANIFEST, "skills/good/SKILL.md": SKILL_MD},
            {
                "skills/escape/SKILL.md": "../../../../etc/passwd",
                "skills/absolute/SKILL.md": "/etc/passwd",
            },
        ))
        assert skill(package, "good").skip_code is None
        dangling = {
            d.target for d in package.diagnostics if d.code == "link_unresolved"
        }
        assert dangling == {
            "skills/escape/SKILL.md", "skills/absolute/SKILL.md"
        }

    def test_a_link_cycle_is_reported_rather_than_followed(self):
        package = validate_package(zip_with_links(
            {"plugin.json": self.MANIFEST},
            {"a.md": "b.md", "b.md": "a.md"},
        ))
        assert package.name == "linked"
        assert {
            d.target for d in package.diagnostics if d.code == "link_unresolved"
        } == {"a.md", "b.md"}

    def test_mirrored_bytes_count_against_the_uncompressed_cap(self):
        """Otherwise a link is a decompression bomb with no ratio to catch."""
        from src.server.services.plugins.archive import MAX_UNCOMPRESSED_BYTES

        big = b"x" * (MAX_UNCOMPRESSED_BYTES // 2 + 1024)
        raw = zip_with_links(
            {"plugin.json": self.MANIFEST, "tree/big.bin": big},
            {"copy": "tree"},
        )
        with pytest.raises(PluginFatal) as exc:
            validate_package(raw)
        assert any(d.code == "too_large" for d in exc.value.diagnostics)

    def test_mirrored_entries_count_against_the_member_cap(self):
        """Two links per level to the level below double the tree each time,
        so sixteen levels of a few hundred bytes would be 131k files, none of
        them costing a byte against the size cap."""
        links = {
            f"d{k:02d}/{side}": (f"../d{k - 1:02d}", False)
            for k in range(1, 17)
            for side in "ab"
        }
        raw = tar_with_links(
            {"plugin.json": self.MANIFEST, "d00/f": b""}, links
        )
        assert len(raw) < 64 * 1024
        with pytest.raises(PluginFatal) as exc:
            validate_package(raw)
        assert any(
            d.code == "too_many_members" for d in exc.value.diagnostics
        )

    def test_a_package_is_charged_only_for_the_links_it_can_see(self):
        """A forge archive is the whole repository. One that mirrors its
        skills for other agents keeps its links outside the package, and the
        package installs from its folder; the repository as a whole still
        meets the cap."""
        mirror = {
            f"repo-abc/.mirror/d{k:02d}/{side}": (f"../d{k - 1:02d}", False)
            for k in range(1, 17)
            for side in "ab"
        }
        raw = tar_with_links(
            {
                "repo-abc/pkg/plugin.json": self.MANIFEST,
                "repo-abc/pkg/skills/s/SKILL.md": SKILL_MD,
                "repo-abc/.mirror/d00/f": b"",
            },
            mirror,
        )
        package = validate_package(raw, subdir="pkg")
        assert skill(package, "s").skip_code is None
        with pytest.raises(PluginFatal) as exc:
            validate_package(raw)
        assert any(
            d.code == "too_many_members" for d in exc.value.diagnostics
        )

    def test_a_link_in_the_package_still_reaches_outside_it(self):
        package = validate_package(
            tar_with_links(
                {
                    "pkg/plugin.json": self.MANIFEST,
                    "vendor/s/SKILL.md": SKILL_MD,
                },
                {"pkg/skills": ("../vendor", False)},
            ),
            subdir="pkg",
        )
        assert skill(package, "s").skip_code is None

    def test_a_package_reached_through_a_linked_directory(self):
        package = validate_package(
            tar_with_links(
                {
                    "packages/pkg/plugin.json": self.MANIFEST,
                    "packages/pkg/skills/s/SKILL.md": SKILL_MD,
                },
                {"plugins": ("packages", False)},
            ),
            subdir="plugins/pkg",
        )
        assert skill(package, "s").skip_code is None

    def test_a_folder_reached_through_a_link_is_charged_for_itself(self):
        """Through ``plugins -> packages``, the package is ``packages/a``;
        the links of a sibling beside it stay unexpanded."""
        doubling = {
            f"packages/b/d{k:02d}/{side}": (f"../d{k - 1:02d}", False)
            for k in range(1, 17)
            for side in "ab"
        }
        raw = tar_with_links(
            {
                "packages/a/plugin.json": self.MANIFEST,
                "packages/a/skills/s/SKILL.md": SKILL_MD,
                "packages/b/d00/f": b"",
            },
            {"plugins": ("packages", False), **doubling},
        )
        package = validate_package(raw, subdir="plugins/a")
        assert skill(package, "s").skip_code is None

    def test_a_link_above_the_package_mirrors_only_its_folder(self):
        """``plugins -> packages`` copies ``packages/a`` for ``plugins/a``,
        not the files of every sibling beside it."""
        raw = tar_with_links(
            {
                "packages/a/plugin.json": self.MANIFEST,
                "packages/a/skills/s/SKILL.md": SKILL_MD,
                **{f"packages/b/f{i}": b"" for i in range(30)},
            },
            {"plugins": ("packages", False)},
        )
        with patch.object(archive, "MAX_MEMBERS", 40):
            package = validate_package(raw, subdir="plugins/a")
            with pytest.raises(PluginFatal):
                validate_package(raw)
        assert skill(package, "s").skip_code is None

    def test_a_broken_link_is_reported_where_the_package_sees_it(self):
        """A package reached through ``plugins -> packages`` holds the broken
        link at ``skills/bad``, though the archive keeps it under ``packages``."""
        raw = tar_with_links(
            {
                "packages/a/plugin.json": self.MANIFEST,
                "packages/a/skills/s/SKILL.md": SKILL_MD,
            },
            {
                "plugins": ("packages", False),
                "packages/a/skills/bad": ("nowhere", False),
            },
        )
        for subdir in ("plugins/a", None):
            files, diagnostics = archive.extract_plugin_archive(
                raw, subdir=subdir
            )
            assert "plugins/a/skills/bad" in {d.target for d in diagnostics}
        package = validate_package(raw, subdir="plugins/a")
        assert [
            d.target for d in package.diagnostics if d.code == "link_unresolved"
        ] == ["skills/bad"]

    def test_links_into_a_folder_of_links_are_reported_once_each(self):
        """Links that reach only each other copy nothing, so their reports of
        what is missing are not copied either."""
        _, diagnostics = archive.extract_plugin_archive(
            tar_with_links(
                {"plugin.json": self.MANIFEST},
                {f"d/l{i}": (".", False) for i in range(10)},
            )
        )
        assert sorted(d.target for d in diagnostics) == [
            f"d/l{i}" for i in range(10)
        ]

    def test_a_package_holds_what_the_whole_archive_shows_under_it(self):
        """The chooser expands every link and an install only the ones its
        folder can see, and on random link-heavy trees, wrapped or not, they
        agree on every file and broken link under the folder."""
        rng = random.Random(532)

        def place(lo: int = 1, hi: int = 4, names: str = "abcd") -> str:
            return "/".join(
                rng.choice(names) for _ in range(rng.randint(lo, hi))
            ).replace("....", "..")

        def read(raw: bytes, subdir: str | None):
            try:
                return archive.extract_plugin_archive(raw, subdir=subdir)
            except PluginFatal:
                return None

        def under(read, folder: str):
            files, diagnostics = read
            return (
                {k: v for k, v in files.items() if k.startswith(folder + "/")},
                sorted(
                    d.target for d in diagnostics
                    if d.target.startswith(folder + "/")
                ),
            )

        for case in range(1000):
            wrapper = "r/" if case % 2 else ""
            files = {
                wrapper + place(): rng.choice([b"", b"x"])
                for _ in range(rng.randint(1, 8))
            }
            links = {}
            for _ in range(rng.randint(1, 8)):
                roll = rng.random()
                if roll < 0.75:
                    target = (place(names="abcd.."), False)
                elif roll < 0.9:
                    target = (wrapper + place(), True)
                else:
                    target = ("/etc/passwd", False)
                links[wrapper + place()] = target
            raw = tar_with_links(files, links)
            folder = place(1, 2)
            whole, package = read(raw, None), read(raw, folder)
            if package is None:
                assert whole is None, (files, links, folder)
            elif whole is not None:
                assert under(whole, folder) == under(package, folder), (
                    files, links, folder,
                )

    def test_each_folder_the_chooser_offers_installs(self):
        """The chooser expands every link and an install only the ones its
        folder can see, so both must name folders from the same root: here
        the top-level alias keeps ``plugins/`` from being stripped."""
        raw = tar_with_links(
            {
                "plugins/a/plugin.json": self.MANIFEST,
                "plugins/a/skills/s/SKILL.md": SKILL_MD,
                "plugins/b/plugin.json": self.MANIFEST,
            },
            {"latest": ("plugins/b", False)},
        )
        with pytest.raises(PluginAmbiguous) as exc:
            validate_package(raw)
        offered = {c.path for c in exc.value.candidates}
        assert offered == {"latest", "plugins/a", "plugins/b"}
        package = validate_package(raw, subdir="plugins/a")
        assert skill(package, "s").skip_code is None

    def test_a_link_to_nothing_leaves_its_wrapper_alone(self):
        """A working folder zipped from inside keeps its stray links, and one
        that leads out of the archive, to nothing, or only to other links is
        never content."""
        package = validate_package(
            tar_with_links(
                {
                    "src/pkg/plugin.json": self.MANIFEST,
                    "src/vendor/s/SKILL.md": SKILL_MD,
                },
                {
                    "src/pkg/skills": ("../vendor", False),
                    ".venv": ("/opt/venv", False),
                    ".cache": ("missing", False),
                    "loop/back": (".", False),
                    "stray": ("src/pkg/skills/missing", False),
                },
            ),
            subdir="pkg",
        )
        assert skill(package, "s").skip_code is None
        assert not any(d.code == "link_unresolved" for d in package.diagnostics)

    def test_a_tar_link_target_that_is_not_utf8_is_only_a_broken_link(self):
        """A tar header may hold any bytes, and measuring the target for its
        cap must not refuse the archive over them."""
        package = validate_package(
            tar_with_links(
                {"plugin.json": self.MANIFEST},
                {"l": (b"caf\xe9".decode("utf-8", "surrogateescape"), False)},
            )
        )
        assert [d.target for d in package.diagnostics if d.code == "link_unresolved"] == ["l"]

    def test_a_tar_link_target_answers_to_the_path_cap(self):
        """As a zip's does: where a link lands is walked level by level."""
        raw = tar_with_links(
            {"plugin.json": self.MANIFEST}, {"l": ("a/" * 3000, False)}
        )
        with pytest.raises(PluginFatal) as exc:
            validate_package(raw)
        assert exc.value.diagnostics[0].code == "link_target_too_long"


class TestSchemaMessages:
    """What a package author reads when the canonical schema refuses.

    Every case here declares ``$schema`` itself, so the document is taken at
    its word rather than adapted, and the schema's verdict reaches the user.
    A tagged union reported as "not valid under any of the given schemas" over
    a repr of the entry names neither the key at fault nor the fix.
    """

    @staticmethod
    def _package(entry_config: dict) -> object:
        return validate_package(zip_package({
            "plugin.json": json.dumps({
                "$schema": CANONICAL_PLUGIN_SCHEMA, "name": "msgs",
                "version": "1.0.0",
            }).encode(),
            "mcp.json": json.dumps({
                "$schema": (
                    "https://agent-plugins.org/schemas/1.0.0/mcp.schema.json"
                ),
                "mcpServers": {"srv": entry_config},
            }).encode(),
        }))

    def test_a_missing_type_names_type(self):
        # The omission every real-world mcp.json makes: no vendor writes
        # "type" for stdio.
        package = self._package({"command": "npx", "args": ["-y", "x"]})
        assert entry(package, "srv").skip_reason == "'type' is a required property"

    def test_an_unknown_transport_lists_the_ones_that_exist(self):
        package = self._package(
            {"type": "http", "url": "https://api.example.com/mcp"}
        )
        reason = entry(package, "srv").skip_reason
        assert "'http'" in reason
        assert "stdio, streamable-http, sse" in reason

    def test_a_junk_key_is_reported_against_the_branch_that_matched(self):
        package = self._package(
            {"type": "stdio", "command": "npx", "retries": 3}
        )
        reason = entry(package, "srv").skip_reason
        assert "'retries' was unexpected" in reason
        # The url branch's complaint is about a shape this entry never claimed.
        assert "url" not in reason

    def test_a_bad_plugin_name_states_the_rule_not_the_regex(self):
        raw = zip_package({
            "plugin.json": json.dumps({
                "$schema": CANONICAL_PLUGIN_SCHEMA, "name": "My Plugin",
                "version": "1.0.0",
            }).encode(),
        })
        with pytest.raises(PluginFatal) as exc:
            validate_package(raw)
        assert "lowercase letters" in str(exc.value)
        assert "(?!" not in str(exc.value)


CANONICAL_MCP_SCHEMA = "https://agent-plugins.org/schemas/1.0.0/mcp.schema.json"


def bound_plugin(binds: list[dict]) -> bytes:
    """A canonical package whose one declared secret binds where ``binds``
    say, with one remote and one stdio server to bind onto."""
    return zip_package({
        "plugin.json": json.dumps({
            "$schema": CANONICAL_PLUGIN_SCHEMA,
            "name": "wind",
            "version": "1.0.0",
            "extensions": {"ai.langalpha": {"secrets": [{
                "name": "WIND_API_KEY", "label": "Wind API key", "bind": binds,
            }]}},
        }).encode(),
        "mcp.json": json.dumps({
            "$schema": CANONICAL_MCP_SCHEMA,
            "mcpServers": {
                "wind_stock_data": {
                    "type": "streamable-http",
                    "url": "https://wind.example/mcp",
                },
                "wind_cli": {"type": "stdio", "command": "wind-mcp"},
            },
        }).encode(),
    })


class TestBindPrefix:
    """``prefix`` on a secret bind: the ``Bearer `` nearly every vendor wants
    in front of its key, which a bare ``${vault:NAME}`` cannot express."""

    HEADER = {
        "server": "wind_stock_data",
        "header": "Authorization",
        "prefix": "Bearer ",
    }

    @pytest.mark.parametrize(
        "prefix", ["Bearer ", "Token token=", "Basic ", "x", "~" * 64]
    )
    def test_a_printable_prefix_is_accepted(self, prefix):
        assert SecretBind(server="s", header="h", prefix=prefix).prefix == prefix

    @pytest.mark.parametrize(
        "prefix",
        [
            "",
            "x" * 65,
            "Bearer\r\nX-Injected: 1\r\n",
            "Bearer\n",
            "Bearer\x00",
            "Bearer\t",
            "Bearer \u00e9",
            "${vault:OTHER} ",
            "$OTHER ",
        ],
    )
    def test_a_prefix_that_could_inject_or_reference_is_refused(self, prefix):
        with pytest.raises(ValueError, match="prefix"):
            SecretBind(server="s", header="h", prefix=prefix)

    def test_a_refused_prefix_costs_its_bind_alone(self):
        # Same scope as every other malformed bind: that bind is dropped with a
        # warning naming it, the secret keeps its other binds, the servers
        # still install, and nothing lands half-written.
        package = validate_package(bound_plugin([
            {**self.HEADER, "prefix": "Bearer\r\n"},
            {"server": "wind_cli", "env": "WIND_AUTH"},
        ]))
        [secret] = package.extension.secrets
        assert [b.server for b in secret.bind] == ["wind_cli"]
        assert any(
            d.code == "extension_invalid"
            and "secrets[0].bind[0]" in d.message
            and "prefix" in d.message
            for d in package.diagnostics
        )
        assert entry(package, "wind_stock_data").installable
        materialize_binds(package.extension, package.entry_plans, {"WIND_API_KEY"})
        assert "Authorization" not in (
            entry(package, "wind_stock_data").config.get("headers") or {}
        )

    def test_the_secret_is_written_behind_its_prefix(self):
        package = validate_package(bound_plugin([
            self.HEADER,
            {"server": "wind_cli", "env": "WIND_AUTH", "prefix": "Token "},
        ]))
        assert not any(d.code == "extension_invalid" for d in package.diagnostics)
        materialize_binds(package.extension, package.entry_plans, {"WIND_API_KEY"})

        remote = entry(package, "wind_stock_data")
        assert remote.config["headers"]["Authorization"] == (
            "Bearer ${vault:WIND_API_KEY}"
        )
        assert entry(package, "wind_cli").config["env"]["WIND_AUTH"] == (
            "Token ${vault:WIND_API_KEY}"
        )
        # The row accepts it, and the extractor reads the prefix as template
        # text: vaulting "Bearer " whole would mint a secret holding a
        # reference and never consult the user's key.
        assert McpServerInput(**remote.config).headers["Authorization"] == (
            "Bearer ${vault:WIND_API_KEY}"
        )
        extracted = plan_vault_extraction(
            "wind_stock_data", remote.config, allocated={}, used_secret_names=set()
        )
        assert extracted.secrets == ()
        # The portable document never learns the template.
        assert "headers" not in package.mcp_document["mcpServers"][
            "wind_stock_data"
        ]

    def test_an_ungranted_bind_writes_nothing_not_a_bare_prefix(self):
        package = validate_package(bound_plugin([self.HEADER]))
        materialize_binds(package.extension, package.entry_plans, set())
        assert "Authorization" not in (
            entry(package, "wind_stock_data").config.get("headers") or {}
        )

    def test_grant_scans_and_scrubs_read_a_prefixed_reference(self):
        config = {
            "transport": "http",
            "url": "https://wind.example/mcp",
            "headers": {
                "Authorization": "Bearer ${vault:WIND_API_KEY}",
                "Accept": "application/json",
            },
        }
        assert secret_names_in(config) == {"WIND_API_KEY"}
        # The whole header goes, not just the reference: "Bearer " alone is
        # still a header the endpoint sees.
        strip_refs(config, frozenset({"WIND_API_KEY"}))
        assert config["headers"] == {"Accept": "application/json"}

    def test_a_vendor_template_becomes_a_prefixed_bind(self):
        raw = zip_package({
            ".claude-plugin/plugin.json": json.dumps(
                {"name": "wind", "description": "Wind market data"}
            ).encode(),
            ".mcp.json": json.dumps({"mcpServers": {
                "wind_stock_data": {
                    "type": "http",
                    "url": "https://wind.example/mcp",
                    "headers": {
                        "Authorization": "Bearer ${WIND_API_KEY}",
                        "Accept": "application/json",
                    },
                },
                "wind_cli": {
                    "command": "wind-mcp",
                    "env": {"WIND_AUTH": "Token ${WIND_TOKEN:-}"},
                },
                "refused": {
                    "type": "http",
                    "url": "https://refused.example/mcp",
                    "headers": {
                        # Over the prefix cap, and a second variable in front:
                        # neither is a prefix, so both stay as written.
                        "X-Long": "x" * 65 + "${LONG_KEY}",
                        "X-Two": "$OTHER ${TWO_KEY}",
                    },
                },
            }}).encode(),
        })
        package = validate_package(raw)

        doc = package.mcp_document["mcpServers"]
        assert doc["wind_stock_data"]["headers"] == {"Accept": "application/json"}
        assert "env" not in doc["wind_cli"]
        # Left in the document as literals (where the stored copy's own scrub
        # blanks what looks like a credential), and declared as nothing.
        assert set(doc["refused"]["headers"]) == {"X-Long", "X-Two"}

        binds = {
            s.name: [b.model_dump(exclude_none=True) for b in s.bind]
            for s in package.extension.secrets
        }
        assert binds == {
            "WIND_API_KEY": [{
                "server": "wind_stock_data",
                "header": "Authorization",
                "prefix": "Bearer ",
            }],
            "WIND_TOKEN": [
                {"server": "wind_cli", "env": "WIND_AUTH", "prefix": "Token "}
            ],
        }
        mapped = {
            d.target: d.message
            for d in package.diagnostics
            if d.code == "env_ref_mapped"
        }
        assert set(mapped) == {"WIND_API_KEY", "WIND_TOKEN"}
        assert "(after 'Bearer ')" in mapped["WIND_API_KEY"]

        materialize_binds(
            package.extension, package.entry_plans, {"WIND_API_KEY", "WIND_TOKEN"}
        )
        assert entry(package, "wind_stock_data").config["headers"] == {
            "Accept": "application/json",
            "Authorization": "Bearer ${vault:WIND_API_KEY}",
        }
        assert entry(package, "wind_cli").config["env"] == {
            "WIND_AUTH": "Token ${vault:WIND_TOKEN}"
        }

    @pytest.mark.asyncio
    async def test_the_prefix_survives_export_and_reinstall(self):
        package = validate_package(bound_plugin([self.HEADER]))
        stored = {
            "user_plugin_id": "p1",
            "manifest": package.manifest,
            "mcp_document": package.mcp_document,
        }
        with patch(
            "src.server.services.plugins.export.list_plugin_skill_names",
            new=AsyncMock(return_value=[]),
        ), patch(
            "src.server.services.plugins.export.list_plugin_subagents",
            new=AsyncMock(return_value=[]),
        ):
            exported = await export_plugin_zip("u1", stored)

        with zipfile.ZipFile(io.BytesIO(exported)) as zf:
            portable = json.loads(zf.read("mcp.json"))
        # The template lives in the extension, never baked into mcp.json,
        # where another host would send "Bearer ${vault:...}" verbatim.
        assert "${vault:" not in json.dumps(portable)

        again = validate_package(exported)
        assert again.extension.secrets[0].bind[0].prefix == "Bearer "
        materialize_binds(again.extension, again.entry_plans, {"WIND_API_KEY"})
        assert entry(again, "wind_stock_data").config["headers"][
            "Authorization"
        ] == "Bearer ${vault:WIND_API_KEY}"


def skill_md(name: str, description: str = "does a thing") -> bytes:
    return f"---\nname: {name}\ndescription: {description}\n---\nbody\n".encode()


def skill_files(package, directory) -> set[str]:
    with zipfile.ZipFile(io.BytesIO(skill(package, directory).zip_bytes)) as zf:
        return set(zf.namelist())


class TestBareSkillRepos:
    """A tree with no plugin anywhere installs as a folder of skills under a
    synthesized plugin.json, instead of failing over a manifest skills do not
    need."""

    def test_a_root_skill_is_its_own_plugin(self):
        package = validate_package(
            zip_package({
                "SKILL.md": skill_md("pdf-tools", "Read and fill PDFs"),
                "scripts/fill.py": b"print('ok')\n",
                ".github/workflows/ci.yml": b"on: push\n",
                "node_modules/dep/index.js": b"",
            }),
            name_hint="my-pdf-repo",
        )
        assert package.synthesized
        assert package.name == "pdf-tools"
        assert package.manifest["description"] == "Read and fill PDFs"
        assert package.manifest["version"] == "0.0.0"
        assert package.manifest["$schema"] == CANONICAL_PLUGIN_SCHEMA
        assert "extensions" not in package.manifest
        assert skill(package, "pdf-tools").skip_code is None
        files = skill_files(package, "pdf-tools")
        assert any(f.endswith("scripts/fill.py") for f in files)
        assert not any(".github" in f or "node_modules" in f for f in files)
        first = package.diagnostics[0]
        # info, not warning: the install report says it beside what landed.
        assert (first.code, first.scope, first.level) == (
            "synthesized_manifest",
            "plugin",
            "info",
        )

    def test_a_skills_folder_is_named_after_its_source(self):
        package = validate_package(
            zip_package({
                "README.md": b"# skills\n",
                "skills/alpha/SKILL.md": skill_md("alpha"),
                "skills/beta/SKILL.md": skill_md("beta"),
            }),
            name_hint="Trading Skills",
        )
        assert package.name == "trading-skills"
        assert package.manifest["description"] == "Skills from Trading Skills"
        assert {p.dir for p in package.skill_plans} == {"alpha", "beta"}
        assert all(p.skip_code is None for p in package.skill_plans)
        assert "2 skills" in package.diagnostics[0].message

    def test_nested_groups_flatten_and_a_skill_keeps_what_it_holds(self):
        package = validate_package(
            zip_package({
                "finance/dcf/SKILL.md": skill_md("dcf"),
                "a/b/folder-x/SKILL.md": skill_md("real-name"),
                # Below a found skill: a file of that skill, not a second one.
                "a/b/folder-x/reference/SKILL.md": skill_md("inner"),
                # Not the collection's own skills.
                ".claude/skills/hidden/SKILL.md": skill_md("hidden"),
                "examples/demo/skills/dcf/SKILL.md": skill_md("dcf"),
                "tests/fixtures/broken/SKILL.md": skill_md("broken"),
            }),
            name_hint="collection",
        )
        assert {p.dir for p in package.skill_plans} == {"dcf", "real-name"}
        assert any(
            f.endswith("reference/SKILL.md")
            for f in skill_files(package, "real-name")
        )
        assert not any(d.code == "duplicate_skill" for d in package.diagnostics)

    def test_an_invalid_declared_name_falls_back_to_the_folder(self):
        package = validate_package(
            zip_package({
                "skills/good-folder/SKILL.md": skill_md("Not A Name"),
                "skills/other/SKILL.md": skill_md("other"),
            }),
            name_hint="x",
        )
        # Laid out under the folder's name, so the fan-out refuses it with the
        # specific reason, per skill, and its sibling is unaffected.
        assert {p.dir for p in package.skill_plans} == {"good-folder", "other"}
        assert package.name == "x"

    def test_a_duplicate_name_keeps_the_shallower_skill(self):
        package = validate_package(
            zip_package({
                "alpha/SKILL.md": skill_md("alpha", "the real one"),
                "x/y/alpha/SKILL.md": skill_md("alpha", "a vendored copy"),
                "beta/SKILL.md": skill_md("beta"),
            }),
            name_hint="dupes",
        )
        assert {p.dir for p in package.skill_plans} == {"alpha", "beta"}
        with zipfile.ZipFile(
            io.BytesIO(skill(package, "alpha").zip_bytes)
        ) as zf:
            body = next(
                zf.read(n) for n in zf.namelist() if n.endswith("SKILL.md")
            )
        assert b"the real one" in body
        dupes = [d for d in package.diagnostics if d.code == "duplicate_skill"]
        assert [(d.scope, d.target) for d in dupes] == [("skill", "x/y/alpha")]

    def test_a_subdir_reads_only_that_folder(self):
        raw = zip_package({
            "finance/dcf/SKILL.md": skill_md("dcf"),
            "finance/comps/SKILL.md": skill_md("comps"),
            "marketing/seo/SKILL.md": skill_md("seo"),
        })
        package = validate_package(raw, subdir="finance", name_hint="finance")
        assert package.name == "finance"
        assert {p.dir for p in package.skill_plans} == {"dcf", "comps"}

    def test_a_plugin_json_anywhere_still_wins(self):
        raw = zip_package({
            "plugin.json": json.dumps({
                "$schema": CANONICAL_PLUGIN_SCHEMA, "name": "declared",
            }).encode(),
            "skills/alpha/SKILL.md": skill_md("alpha"),
            "elsewhere/beta/SKILL.md": skill_md("beta"),
        })
        package = validate_package(raw, name_hint="ignored")
        assert not package.synthesized
        assert package.name == "declared"
        assert {p.dir for p in package.skill_plans} == {"alpha"}
        assert not any(
            d.code == "synthesized_manifest" for d in package.diagnostics
        )

    def test_a_vendor_manifest_still_wins(self):
        raw = zip_package({
            ".claude-plugin/plugin.json": json.dumps({"name": "vendored"}).encode(),
            "skills/alpha/SKILL.md": skill_md("alpha"),
        })
        package = validate_package(raw, name_hint="ignored")
        assert (package.synthesized, package.name) == (False, "vendored")

    def test_past_the_cap_the_user_is_asked_to_link_a_folder(self):
        raw = zip_package({
            f"s/skill-{i:03d}/SKILL.md": skill_md(f"skill-{i:03d}")
            for i in range(101)
        })
        with pytest.raises(PluginFatal) as exc:
            validate_package(raw, name_hint="monorepo")
        assert [d.code for d in exc.value.diagnostics] == ["too_many_skills"]

    def test_a_rename_is_not_a_content_change(self):
        raw = zip_package({
            "skills/alpha/SKILL.md": skill_md("alpha"),
            "skills/beta/SKILL.md": skill_md("beta"),
        })
        one = validate_package(raw, name_hint="one")
        two = validate_package(raw, name_hint="two")
        assert (one.name, two.name) == ("one", "two")
        assert one.content_hash == two.content_hash
        renamed = two.renamed("one")
        assert (renamed.name, renamed.content_hash) == ("one", one.content_hash)
        assert two.name == "two"  # a copy, not an edit

    def test_only_a_synthesized_name_can_be_renamed(self):
        package = validate_package(fixture_zip("valid-full"))
        with pytest.raises(ValueError):
            package.renamed("other")

    @pytest.mark.asyncio
    async def test_update_keeps_the_installed_name(self):
        # Install named it after its one skill; a second skill since makes the
        # guess the repository's name, which must not read as another plugin.
        raw = zip_package({
            "skills/alpha/SKILL.md": skill_md("alpha"),
            "skills/beta/SKILL.md": skill_md("beta"),
        })
        source = PackageSource(
            raw=raw, subdir=None, name_hint="repo", source_type="zip", source_ref=None
        )
        package = await validate_source(source, expected_name="alpha")
        assert package.name == "alpha"
        fresh = await validate_source(source)
        assert fresh.name == "repo"

    @pytest.mark.asyncio
    async def test_an_uploaded_zip_update_names_a_manifestless_package_after_the_plugin(self):
        raw = zip_package({"agents/x.md": b"---\ndescription: Does x.\n---\nBody.\n"})
        source = PackageSource(
            raw=raw, subdir=None, name_hint="download", source_type="zip", source_ref=None
        )
        package = await validate_source(source, expected_name="screen-kit")
        assert package.name == "screen-kit"
        assert package.manifest["description"] == "Subagents from screen-kit"
        assert "screen-kit" in package.diagnostics[0].message
        assert package.diagnostics[0].code == "synthesized_manifest"
        assert (await validate_source(source)).name == "download"

    @pytest.mark.parametrize(
        ("hint", "name"),
        [
            ("claude-trading-skills", "claude-trading-skills"),
            ("Trading Skills", "trading-skills"),
            ("Foo__Bar..baz", "foo-bar.baz"),
            ("-.leading.and.trailing.-", "leading.and.trailing"),
            ("x" * 80, "x" * 64),
            ("\u91d1\u878d", "skills"),
            ("---", "skills"),
            ("", "skills"),
            (None, "skills"),
        ],
    )
    def test_a_hint_is_sanitized_to_the_name_rule(self, hint, name):
        assert plugin_name_from_hint(hint) == name

    @pytest.mark.parametrize(
        ("source", "subdir", "hint"),
        [
            ("https://github.com/anthropics/skills", None, "skills"),
            ("https://github.com/o/repo.git", None, "repo"),
            ("https://github.com/o/repo/tree/main", None, "repo"),
            (
                "https://github.com/o/agents/tree/abc/plugins/quant",
                "plugins/quant",
                "quant",
            ),
            ("https://gitlab.com/g/sub/proj/-/tree/main", None, "proj"),
            ("https://codeberg.org/o/repo/src/branch/main", None, "repo"),
            ("https://example.com/dl/pack.tar.gz", None, "pack"),
            ("my-skills.zip", None, "my-skills"),
            ("C:\\Users\\me\\my-skills.tgz", None, "my-skills"),
            ("bundle.zip", "inner/finance/", "finance"),
            (None, None, None),
        ],
    )
    def test_the_hint_comes_from_the_link_or_file(self, source, subdir, hint):
        assert name_hint(source, subdir) == hint
