"""Tests for create_mcp_config(): headers pass-through + source is YAML-immune.

Built-ins may declare http/sse ``headers``; the parser must carry them. An
explicit ``source`` key in YAML must be ignored — only the model default
("builtin") is allowed, so a config file can't mark a server as untrusted.
"""

from ptc_agent.config.utils import create_mcp_config


def _mcp_section(servers):
    return {"servers": servers, "tool_discovery_enabled": True}


class TestCreateMcpConfigHeaders:
    def test_headers_pass_through(self):
        cfg = create_mcp_config(
            _mcp_section([
                {
                    "name": "remote",
                    "transport": "http",
                    "url": "https://example.test/mcp",
                    "headers": {"Authorization": "${vault:TOKEN}"},
                }
            ])
        )
        assert cfg.servers[0].headers == {"Authorization": "${vault:TOKEN}"}

    def test_headers_default_empty(self):
        cfg = create_mcp_config(
            _mcp_section([{"name": "local", "command": "npx", "args": ["-y", "x"]}])
        )
        assert cfg.servers[0].headers == {}

    def test_source_key_in_yaml_is_ignored(self):
        # Even if YAML tries to declare source='workspace', built-ins stay builtin.
        cfg = create_mcp_config(
            _mcp_section([
                {"name": "local", "command": "npx", "source": "workspace"}
            ])
        )
        assert cfg.servers[0].source == "builtin"

    def test_source_defaults_to_builtin(self):
        cfg = create_mcp_config(
            _mcp_section([{"name": "local", "command": "npx"}])
        )
        assert cfg.servers[0].source == "builtin"
