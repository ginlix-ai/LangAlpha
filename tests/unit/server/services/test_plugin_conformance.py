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
import stat
import tarfile
import tracemalloc
import zipfile
from pathlib import Path

import pytest

from src.server.services.plugins import archive
from src.server.services.plugins.errors import PluginAmbiguous, PluginFatal
from src.server.services.plugins.fetch import (
    compose_subdir_url,
    normalize_forge_url,
)
from src.server.services.plugins import validate_package
from src.server.services.mcp_import import plan_vault_extraction
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
        raw = in_memory_zip({
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
        #
        # Both argv shapes, because they were once handled differently: the
        # space-separated pair was left alone on the grounds that nothing could
        # tell it from a positional, which the benign entries below disprove —
        # the flag naming the value is signal enough.
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
        raw = in_memory_zip({
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
            package = validate_package(in_memory_zip(members))
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
        raw = in_memory_zip({
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
        raw = in_memory_zip({
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
        raw = in_memory_zip({
            "plugin.json": TestArchiveHardening.MANIFEST,
            "assets/logo.png": b"\x89PNG",
        })
        package = validate_package(raw)
        empty = next(d for d in package.diagnostics if d.code == "no_components")
        assert empty.level == "warning"
        assert "assets" in empty.message

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
        raw = self._zip({
            "repo-abc123/plugin.json": self.MANIFEST,
            "repo-abc123/skills/s/SKILL.md": (
                b"---\nname: s\ndescription: d\n---\nbody\n"
            ),
        })
        package = validate_package(raw)
        assert package.name == "hardened"
        assert skill(package, "s").skip_code is None


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


class TestSchemaMessages:
    """What a package author reads when the canonical schema refuses.

    Every case here declares ``$schema`` itself, so the document is taken at
    its word rather than adapted, and the schema's verdict reaches the user.
    A tagged union reported as "not valid under any of the given schemas" over
    a repr of the entry names neither the key at fault nor the fix.
    """

    @staticmethod
    def _package(entry_config: dict) -> object:
        return validate_package(in_memory_zip({
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
        raw = in_memory_zip({
            "plugin.json": json.dumps({
                "$schema": CANONICAL_PLUGIN_SCHEMA, "name": "My Plugin",
                "version": "1.0.0",
            }).encode(),
        })
        with pytest.raises(PluginFatal) as exc:
            validate_package(raw)
        assert "lowercase letters" in str(exc.value)
        assert "(?!" not in str(exc.value)
