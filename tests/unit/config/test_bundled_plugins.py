"""The servers that ship with the app are declared by the bundles, not YAML.

``plugins/`` is the source of truth for the built-in MCP set: each directory
is an Agent Plugins package whose ``mcp.json`` names the servers and whose
``extensions["ai.langalpha"]`` block carries the fields the closed format has
nowhere to put. What is left in ``agent_config.yaml`` is the operator's own
list, and a name in both wins there.

The repo's real bundles are exercised at the bottom, because the whole point
of moving them was that the 10 servers we ship keep landing with the
description, instruction and exposure mode they had in YAML.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest

from ptc_agent.config import plugins as bundles
from ptc_agent.config.utils import create_mcp_config

REPO_ROOT = Path(__file__).resolve().parents[3]


def _write(root: Path, name: str, *, manifest: dict, mcp: dict | None = None):
    bundle = root / name
    bundle.mkdir(parents=True)
    (bundle / "plugin.json").write_text(json.dumps(manifest))
    if mcp is not None:
        (bundle / "mcp.json").write_text(json.dumps(mcp))
    return bundle


def _manifest(name: str, servers: dict | None = None) -> dict:
    extensions = {"ai.langalpha": {"servers": servers}} if servers else {}
    return {
        "$schema": "https://agent-plugins.org/schemas/1.0.0/plugin.schema.json",
        "name": name,
        "extensions": extensions,
    }


def _stdio(script: str, env: dict | None = None) -> dict:
    entry = {"type": "stdio", "command": "uv", "args": ["run", "python", script]}
    if env is not None:
        entry["env"] = env
    return entry


@pytest.fixture
def bundles_dir(monkeypatch, tmp_path):
    root = tmp_path / "plugins"
    root.mkdir()
    monkeypatch.setattr(bundles, "BUNDLES_DIR", root)
    return root


def _section(servers=None):
    section = {"tool_discovery_enabled": True}
    if servers is not None:
        section["servers"] = servers
    return section


class TestComposition:
    def test_bundles_become_the_builtin_servers(self, bundles_dir):
        _write(
            bundles_dir, "data",
            manifest=_manifest("data", {
                "alpha": {
                    "description": "the alpha server",
                    "instruction": "reach for it when asked",
                    "tool_exposure_mode": "detailed",
                }
            }),
            mcp={"mcpServers": {"alpha": _stdio("mcp_servers/alpha.py")}},
        )
        cfg = create_mcp_config(_section())

        assert [s.name for s in cfg.servers] == ["alpha"]
        alpha = cfg.servers[0]
        assert alpha.description == "the alpha server"
        assert alpha.instruction == "reach for it when asked"
        assert alpha.tool_exposure_mode == "detailed"
        assert alpha.command == "uv"
        assert alpha.args == ["run", "python", "mcp_servers/alpha.py"]
        # Built-in, never a workspace server: the whole trust model hangs off it.
        assert alpha.source == "builtin"

    def test_a_server_with_no_meta_block_still_loads(self, bundles_dir):
        _write(
            bundles_dir, "data", manifest=_manifest("data"),
            mcp={"mcpServers": {"alpha": _stdio("mcp_servers/alpha.py")}},
        )
        cfg = create_mcp_config(_section())

        assert [s.name for s in cfg.servers] == ["alpha"]
        assert cfg.servers[0].description == ""
        assert cfg.servers[0].tool_exposure_mode is None

    def test_bundle_without_mcp_json_contributes_nothing(self, bundles_dir):
        _write(bundles_dir, "skills-only", manifest=_manifest("skills-only"))
        assert create_mcp_config(_section()).servers == []

    def test_order_is_bundle_then_declaration(self, bundles_dir):
        _write(
            bundles_dir, "zebra", manifest=_manifest("zebra"),
            mcp={"mcpServers": {
                "z_one": _stdio("z_one.py"), "z_two": _stdio("z_two.py"),
            }},
        )
        _write(
            bundles_dir, "alpha", manifest=_manifest("alpha"),
            mcp={"mcpServers": {"a_one": _stdio("a_one.py")}},
        )
        cfg = create_mcp_config(_section())
        assert [s.name for s in cfg.servers] == ["a_one", "z_one", "z_two"]

    def test_env_expands_from_the_process_environment(
        self, bundles_dir, monkeypatch
    ):
        monkeypatch.setenv("PROBE_KEY", "s3cret")
        _write(
            bundles_dir, "data", manifest=_manifest("data"),
            mcp={"mcpServers": {
                "alpha": _stdio("alpha.py", {"PROBE_KEY": "${PROBE_KEY}"}),
            }},
        )
        assert create_mcp_config(_section()).servers[0].env == {
            "PROBE_KEY": "s3cret"
        }

    def test_a_vault_blueprint_rides_the_meta_block(self, bundles_dir):
        _write(
            bundles_dir, "data",
            manifest=_manifest("data", {
                "alpha": {
                    "vault_blueprints": [
                        {"name": "PROBE_TOKEN", "label": "Probe token"}
                    ]
                }
            }),
            mcp={"mcpServers": {"alpha": _stdio("alpha.py")}},
        )
        blueprints = create_mcp_config(_section()).servers[0].vault_blueprints
        assert [b.name for b in blueprints] == ["PROBE_TOKEN"]


class TestYamlStillWorks:
    def test_yaml_servers_append_after_the_bundles(self, bundles_dir):
        _write(
            bundles_dir, "data", manifest=_manifest("data"),
            mcp={"mcpServers": {"alpha": _stdio("alpha.py")}},
        )
        cfg = create_mcp_config(
            _section([{"name": "mine", "command": "npx", "args": ["-y", "x"]}])
        )
        assert [s.name for s in cfg.servers] == ["alpha", "mine"]

    def test_the_servers_key_is_optional(self, bundles_dir):
        assert create_mcp_config({"tool_discovery_enabled": True}).servers == []

    def test_missing_bundles_directory_is_survivable(self, monkeypatch, tmp_path):
        monkeypatch.setattr(bundles, "BUNDLES_DIR", tmp_path / "gone")
        assert create_mcp_config(_section()).servers == []


class TestOverrides:
    """A name a bundle already declares is an override, not a collision.

    The keys in YAML are laid over the shipped server and everything left out
    keeps its shipped value. Replacing the whole entry instead reads the same
    in the file and silently drops the command a partial override never
    restates, which fails at connect time three files from the line that
    caused it.
    """

    @pytest.fixture(autouse=True)
    def _shipped(self, bundles_dir):
        _write(
            bundles_dir, "data",
            manifest=_manifest("data", {
                "alpha": {
                    "description": "shipped",
                    "instruction": "shipped instruction",
                    "tool_exposure_mode": "summary",
                }
            }),
            mcp={"mcpServers": {
                "alpha": _stdio("alpha.py", {"KEEP": "1", "DROP": "2"}),
            }},
        )

    def test_it_lands_in_place_not_as_a_second_row(self):
        cfg = create_mcp_config(_section([{"name": "alpha", "enabled": False}]))
        # An operator switching a shipped server off must not end up with two
        # rows claiming the name.
        assert [s.name for s in cfg.servers] == ["alpha"]

    def test_switching_one_off_keeps_everything_else(self):
        server = create_mcp_config(
            _section([{"name": "alpha", "enabled": False}])
        ).servers[0]
        assert server.enabled is False
        assert server.command == "uv"
        assert server.description == "shipped"

    def test_retuning_one_field_leaves_the_command_alone(self):
        # The whole point: this used to yield an enabled stdio server with no
        # command, which only fails once something tries to launch it.
        server = create_mcp_config(
            _section([{"name": "alpha", "tool_exposure_mode": "detailed"}])
        ).servers[0]
        assert server.tool_exposure_mode == "detailed"
        assert server.enabled is True
        assert server.command == "uv"
        assert server.args == ["run", "python", "alpha.py"]
        assert server.instruction == "shipped instruction"

    def test_it_can_repoint_the_command(self):
        server = create_mcp_config(_section([{
            "name": "alpha", "command": "uvx",
            "args": ["--from", "my-fork", "serve"],
        }])).servers[0]
        assert server.command == "uvx"
        assert server.args == ["--from", "my-fork", "serve"]
        assert server.description == "shipped"

    def test_env_is_replaced_whole_rather_than_key_merged(self):
        # A deep merge would have no way to take DROP away.
        server = create_mcp_config(
            _section([{"name": "alpha", "env": {"KEEP": "9"}}])
        ).servers[0]
        assert server.env == {"KEEP": "9"}

    def test_an_override_stays_a_builtin(self):
        # source is stripped on both paths: a config file must not be able to
        # relabel a shipped server as an untrusted workspace one.
        server = create_mcp_config(
            _section([{"name": "alpha", "source": "workspace"}])
        ).servers[0]
        assert server.source == "builtin"


class TestDefects:
    def test_an_unusable_meta_block_leaves_the_server_running(self, bundles_dir):
        _write(
            bundles_dir, "data",
            manifest=_manifest("data", {"alpha": {"nonsense": True}}),
            mcp={"mcpServers": {"alpha": _stdio("alpha.py")}},
        )
        cfg = create_mcp_config(_section())
        assert [s.name for s in cfg.servers] == ["alpha"]
        assert cfg.servers[0].description == ""

    def test_unparseable_manifest_costs_only_its_metadata(self, bundles_dir):
        bundle = _write(
            bundles_dir, "data", manifest=_manifest("data"),
            mcp={"mcpServers": {"alpha": _stdio("alpha.py")}},
        )
        (bundle / "plugin.json").write_text("{ not json")
        assert [s.name for s in create_mcp_config(_section()).servers] == ["alpha"]

    def test_unparseable_mcp_json_drops_that_bundle_only(self, bundles_dir):
        broken = _write(
            bundles_dir, "broken", manifest=_manifest("broken"),
            mcp={"mcpServers": {"gone": _stdio("gone.py")}},
        )
        (broken / "mcp.json").write_text("{ not json")
        _write(
            bundles_dir, "ok", manifest=_manifest("ok"),
            mcp={"mcpServers": {"alpha": _stdio("alpha.py")}},
        )
        assert [s.name for s in create_mcp_config(_section()).servers] == ["alpha"]

    def test_unknown_transport_is_skipped(self, bundles_dir):
        _write(
            bundles_dir, "data", manifest=_manifest("data"),
            mcp={"mcpServers": {
                "alpha": {"type": "carrier-pigeon", "url": "https://e.test"},
                "beta": _stdio("beta.py"),
            }},
        )
        assert [s.name for s in create_mcp_config(_section()).servers] == ["beta"]


class TestOneEntryPointFileOneServer:
    """A sandbox stages every entry point into one flat directory.

    The launch args are rewritten to the bare file name, so two servers whose
    entry points share one would both point at whichever file won the upload.
    The loser would run the winner's code under its own name and its own
    environment, and nothing downstream can tell the two apart.
    """

    def test_a_second_bundle_cannot_claim_a_taken_entry_point(self, bundles_dir):
        _write(
            bundles_dir, "aaa", manifest=_manifest("aaa"),
            mcp={"mcpServers": {"first": _stdio("plugins/aaa/server.py")}},
        )
        _write(
            bundles_dir, "zzz", manifest=_manifest("zzz"),
            mcp={"mcpServers": {"second": _stdio("plugins/zzz/server.py")}},
        )
        assert [s.name for s in bundles.bundled_mcp_servers()] == ["first"]

    def test_the_same_file_name_under_a_different_command_is_not_a_clash(
        self, bundles_dir
    ):
        """Only the ``uv run python`` shape is staged and rewritten."""
        _write(
            bundles_dir, "aaa", manifest=_manifest("aaa"),
            mcp={"mcpServers": {"first": _stdio("plugins/aaa/server.py")}},
        )
        _write(
            bundles_dir, "zzz", manifest=_manifest("zzz"),
            mcp={"mcpServers": {"second": {
                "type": "stdio",
                "command": "uvx",
                "args": ["some-package", "plugins/zzz/server.py"],
            }}},
        )
        assert [s.name for s in bundles.bundled_mcp_servers()] == ["first", "second"]

    def test_distinct_file_names_from_one_bundle_both_survive(self, bundles_dir):
        _write(
            bundles_dir, "data", manifest=_manifest("data"),
            mcp={"mcpServers": {
                "alpha": _stdio("plugins/data/alpha.py"),
                "beta": _stdio("plugins/data/beta.py"),
            }},
        )
        assert [s.name for s in bundles.bundled_mcp_servers()] == ["alpha", "beta"]


class TestOneNameOneServer:
    """A server name is what the registry and the ownership map both key on.

    ``MCPRegistry.connect_all`` stores connectors in a name-keyed dict, so a
    duplicate name silently keeps the last one, while ``component_owners``
    keeps the first claim. The two then disagree by construction: the second
    bundle's code runs, disabling that bundle removes nothing, and disabling
    the first removes a server it does not ship.
    """

    def test_a_second_bundle_cannot_claim_a_taken_server_name(self, bundles_dir):
        _write(
            bundles_dir, "aaa", manifest=_manifest("aaa"),
            mcp={"mcpServers": {"price": _stdio("plugins/aaa/a_server.py")}},
        )
        _write(
            bundles_dir, "zzz", manifest=_manifest("zzz"),
            mcp={"mcpServers": {"price": _stdio("plugins/zzz/z_server.py")}},
        )
        servers = bundles.bundled_mcp_servers()
        assert [(s.name, s.args[2]) for s in servers] == [
            ("price", "plugins/aaa/a_server.py")
        ]


class TestAMisEncodedManifestIsSkipped:
    """Bundles are read while composing config, so a raise here is a dead boot.

    ``UnicodeDecodeError`` is a ``ValueError``, not an ``OSError``, so it is
    the one malformed-file case the JSON handling does not already cover.
    """

    def test_a_mis_encoded_mcp_json_drops_that_bundle_only(self, bundles_dir):
        bad = _write(
            bundles_dir, "bad", manifest=_manifest("bad"),
            mcp={"mcpServers": {"gone": _stdio("plugins/bad/gone.py")}},
        )
        (bad / "mcp.json").write_bytes(b'{"mcpServers": "\xff\xfe"}')
        _write(
            bundles_dir, "ok", manifest=_manifest("ok"),
            mcp={"mcpServers": {"alpha": _stdio("plugins/ok/alpha.py")}},
        )
        assert [s.name for s in create_mcp_config(_section()).servers] == ["alpha"]

    def test_a_mis_encoded_plugin_json_costs_only_its_metadata(self, bundles_dir):
        # Same split the JSON cases already make: mcp.json says what runs,
        # plugin.json only says how it is described.
        bad = _write(
            bundles_dir, "bad", manifest=_manifest("bad"),
            mcp={"mcpServers": {"alpha": _stdio("plugins/bad/alpha.py")}},
        )
        (bad / "plugin.json").write_bytes(b'{"name": "\xff\xfe"}')
        cfg = create_mcp_config(_section())
        assert [s.name for s in cfg.servers] == ["alpha"]
        assert cfg.servers[0].description == ""


class TestShippedBundles:
    """The real ``plugins/`` directory, not a fixture.

    These servers reach the agent's prompt, so a manifest that stops parsing
    is not a config-loading bug — it is the agent quietly losing a data source.
    """

    @pytest.fixture(autouse=True)
    def _real_bundles(self, monkeypatch):
        monkeypatch.setattr(bundles, "BUNDLES_DIR", REPO_ROOT / "plugins")

    def test_every_shipped_server_is_described(self):
        servers = bundles.bundled_mcp_servers()
        assert servers, "plugins/ declares no MCP servers"
        undescribed = [
            s.name for s in servers if not (s.description and s.instruction)
        ]
        assert undescribed == []

    def test_every_declared_meta_names_a_real_server(self):
        for bundle in bundles.bundles():
            keys = set(bundle.servers)
            declared = set(bundle.namespace.get("servers") or {})
            assert declared <= keys, f"{bundle.name} describes {declared - keys}"


class TestABundleRootThatWillNotOpen:
    """The root itself, not one bundle inside it.

    ``_bundle_dirs`` runs while composing configuration, so an ``OSError`` out
    of ``iterdir`` aborted startup instead of degrading to the same loud
    nothing a missing directory already gets. Its own docstring promises the
    loud nothing, which is the contract this pins.
    """

    def test_an_unreadable_root_is_the_same_loud_nothing_as_a_missing_one(
        self, monkeypatch, tmp_path, caplog
    ):
        root = tmp_path / "plugins"
        root.mkdir()
        monkeypatch.setattr(bundles, "BUNDLES_DIR", root)

        real_iterdir = Path.iterdir

        def _iterdir(self):
            if self == root:
                raise PermissionError(13, "Permission denied")
            return real_iterdir(self)

        monkeypatch.setattr(Path, "iterdir", _iterdir)

        with caplog.at_level(logging.ERROR):
            assert bundles._bundle_dirs() == []
        assert str(root) in caplog.text

    def test_composition_answers_an_empty_set_rather_than_raising(
        self, monkeypatch, tmp_path
    ):
        root = tmp_path / "plugins"
        root.mkdir()
        monkeypatch.setattr(bundles, "BUNDLES_DIR", root)

        real_iterdir = Path.iterdir

        def _iterdir(self):
            if self == root:
                raise PermissionError(13, "Permission denied")
            return real_iterdir(self)

        monkeypatch.setattr(Path, "iterdir", _iterdir)

        assert bundles.bundled_mcp_servers() == []
        assert bundles.bundled_skill_dirs() == []
