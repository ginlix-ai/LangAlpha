"""Bundled Agent Plugins, read as the built-in MCP server set.

Every directory under ``plugins/`` is an Agent Plugins 1.0.0 package: a
portable ``mcp.json`` naming the servers, and a ``plugin.json`` whose
``extensions["ai.langalpha"]`` block carries what the format has nowhere to
put. ``mcp.json`` is closed at every level — the document, each server, and
each transport variant all set ``additionalProperties: false`` — so a
server's description, its usage instruction, its tool exposure mode and the
credentials it expects cannot be declared there. ``extensions`` is the one
extension point the format defines, and it is where ours go. Strip that block
and what is left is a package any Agent Plugins host can read, which is the
reason to keep it out of ``mcp.json`` rather than bend the document.

A bundle ships inside the image, so ``${VAR}`` in its args, env, url or
headers expands from the process environment the same way it did when these
servers were declared in YAML. Nothing here reads an uploaded plugin: those
arrive as untrusted input, resolve credentials from the vault instead, and
never touch this module.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field

from ptc_agent.config.core import MCPServerConfig, VaultBlueprint
from ptc_agent.config.file_utils import substitute_env_vars

logger = logging.getLogger(__name__)

NAMESPACE = "ai.langalpha"

# src/ptc_agent/config/plugins.py → repo root
BUNDLES_DIR = Path(__file__).resolve().parents[3] / "plugins"

# mcp.json's transport names, mapped onto ours. Legacy ``sse`` keeps its name;
# the sandbox client refuses it, which is the outcome we want for a bundle
# that declares it.
_TRANSPORTS = {"stdio": "stdio", "streamable-http": "http", "sse": "sse"}


class ServerMeta(BaseModel):
    """What a package says about one of its servers, beyond mcp.json's reach.

    Keyed by the ``mcp.json`` entry key rather than the installed server name,
    the same way ``secrets[].bind`` names its target.
    """

    description: str = ""
    instruction: str = ""
    tool_exposure_mode: Literal["summary", "detailed"] | None = None
    # Read from a bundle only. An uploaded plugin declares the credentials it
    # expects through the namespace's ``secrets[]``, which also says where
    # each one binds; the request model refuses this key on a user server.
    vault_blueprints: list[VaultBlueprint] = Field(default_factory=list)

    model_config = {"extra": "forbid"}


def _read_json(path: Path) -> tuple[dict[str, Any] | None, bool]:
    """The JSON object at ``path``, and whether reading it went as it should.

    Absence is silent and fine: a bundle that ships only skills has no
    ``mcp.json``, and a warning per startup for the ordinary case teaches the
    reader to ignore the one that means something. A file that is there and
    unusable is a different answer, and the two have to be told apart --
    collapsing both to an empty document says "this bundle declares nothing",
    which is indistinguishable from the truth and is how a bundle a user
    switched off quietly came back on.
    """
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None, True
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as e:
        # UnicodeDecodeError is a ValueError, not an OSError, so it needs
        # naming: bundles are read while composing config, and a package with
        # one mis-encoded byte would otherwise abort startup rather than be
        # skipped like any other manifest that does not read.
        logger.warning("bundled plugin: unreadable %s: %s", path, e)
        return None, False
    if not isinstance(loaded, dict):
        logger.warning("bundled plugin: %s is not an object", path)
        return None, False
    return loaded, True


def _bundle_dirs() -> list[Path]:
    """Bundle directories, name-ordered. The one place that decides what counts.

    A missing directory is a loud nothing rather than a quiet one: the servers
    that used to be a list in agent_config.yaml now live here, so a build that
    forgot to ship them starts clean and answers every question with no tools.
    """
    if not BUNDLES_DIR.is_dir():
        logger.warning(
            "no bundled plugins: %s is not a directory, so nothing ships the "
            "built-in MCP servers", BUNDLES_DIR,
        )
        return []
    try:
        entries = sorted(BUNDLES_DIR.iterdir())
    except OSError as e:
        # The root itself, not one bundle: this runs while composing config,
        # so an unreadable directory here would abort startup rather than
        # degrade to the same loud nothing a missing one already gets.
        logger.error(
            "no bundled plugins: %s cannot be read (%s), so nothing ships the "
            "built-in MCP servers", BUNDLES_DIR, e,
        )
        return []
    return [p for p in entries if p.is_dir() and not p.name.startswith((".", "_"))]


def _namespace_block(manifest: dict[str, Any]) -> dict[str, Any]:
    """The ``ai.langalpha`` block of a manifest, or an empty one."""
    extensions = manifest.get("extensions")
    if not isinstance(extensions, dict):
        return {}
    block = extensions.get(NAMESPACE)
    return block if isinstance(block, dict) else {}


@dataclass(frozen=True)
class Bundle:
    """One package on disk, parsed once for whoever asks.

    Three surfaces read this directory -- the MCP server set, the Plugins page
    and the icon route -- and each wants a different slice of the same two
    files. Handing them a record rather than a path is what keeps the parsing
    in one place instead of once per reader.
    """

    name: str
    path: Path
    manifest: dict[str, Any]
    #: The ``ai.langalpha`` block, empty when the package declares none.
    namespace: dict[str, Any]
    #: ``mcp.json``'s ``mcpServers``, empty for a bundle that ships only skills.
    servers: dict[str, Any]
    #: False when a manifest was there and could not be used, so the fields
    #: above are what we managed to read rather than what the package declares.
    #: Anything subtracting a bundle's components has to check this: an
    #: incomplete answer and "owns nothing" look the same to the caller.
    readable: bool = True


def bundles() -> list[Bundle]:
    """Every bundle under ``plugins/``, name-ordered. Never raises.

    Re-read on each call rather than memoized. The cost is a handful of small
    files, and the alternative is module-level state that a request path
    consults, which this server does not keep. What it buys is that the
    Plugins page shows an edited manifest immediately; the MCP server set
    still does not move, because its caller composes it once at startup.
    """
    out: list[Bundle] = []
    for path in _bundle_dirs():
        manifest, manifest_ok = _read_json(path / "plugin.json")
        manifest = manifest or {}
        declared, servers_ok = _read_json(path / "mcp.json")
        entries = (declared or {}).get("mcpServers")
        out.append(
            Bundle(
                name=path.name,
                path=path,
                manifest=manifest,
                namespace=_namespace_block(manifest),
                servers=entries if isinstance(entries, dict) else {},
                readable=manifest_ok and servers_ok,
            )
        )
    return out


def _expand(value: Any) -> Any:
    if isinstance(value, str):
        return substitute_env_vars(value)
    if isinstance(value, list):
        return [_expand(v) for v in value]
    if isinstance(value, dict):
        return {k: _expand(v) for k, v in value.items()}
    return value


def _server_metas(block: dict[str, Any], bundle: str) -> dict[str, ServerMeta]:
    declared = block.get("servers")
    if not isinstance(declared, dict):
        return {}
    metas: dict[str, ServerMeta] = {}
    for key, payload in declared.items():
        try:
            metas[key] = ServerMeta(**payload)
        except (TypeError, ValueError) as e:
            # The server still runs; it just reaches the prompt undescribed.
            logger.warning(
                "bundled plugin %s: server %r has an unusable meta block: %s",
                bundle, key, e,
            )
    return metas


def _server(
    key: str, entry: dict[str, Any], meta: ServerMeta
) -> MCPServerConfig | None:
    transport = _TRANSPORTS.get(entry.get("type") or "stdio")
    if transport is None:
        return None
    common = {
        "name": key,
        "transport": transport,
        "description": meta.description,
        "instruction": meta.instruction,
        "tool_exposure_mode": meta.tool_exposure_mode,
        "vault_blueprints": list(meta.vault_blueprints),
    }
    if transport == "stdio":
        return MCPServerConfig(
            **common,
            command=_expand(entry.get("command")),
            args=_expand(list(entry.get("args") or [])),
            env=_expand(dict(entry.get("env") or {})),
        )
    return MCPServerConfig(
        **common,
        url=_expand(entry.get("url")),
        headers=_expand(dict(entry.get("headers") or {})),
    )


def _staged_entrypoint(server: MCPServerConfig) -> str | None:
    """The file name this server's entry point takes in a sandbox, if any.

    Staging is one flat ``mcp_servers/`` directory and the launch args are
    rewritten to the bare name, so this string -- not the path the bundle
    wrote -- is what picks the file that actually runs.
    """
    if server.transport != "stdio" or server.command != "uv":
        return None
    args = [str(a) for a in server.args or ()]
    if len(args) < 3 or args[0] != "run" or args[1] != "python":
        return None
    return Path(args[2]).name


def bundled_mcp_servers() -> list[MCPServerConfig]:
    """Every server the bundles declare, in bundle then declaration order.

    Never raises: a bundle whose manifests are unreadable is skipped with a
    log line rather than taking the process down at startup.
    """
    servers: list[MCPServerConfig] = []
    staged: dict[str, str] = {}
    claimed_names: dict[str, str] = {}
    for bundle in bundles():
        metas = _server_metas(bundle.namespace, bundle.name)
        for key, entry in bundle.servers.items():
            if not isinstance(key, str) or not isinstance(entry, dict):
                continue
            try:
                server = _server(key, entry, metas.get(key) or ServerMeta())
            except (TypeError, ValueError) as e:
                logger.warning(
                    "bundled plugin %s: server %r is unusable: %s",
                    bundle.name, key, e,
                )
                continue
            if server is None:
                logger.warning(
                    "bundled plugin %s: server %r declares unknown transport %r",
                    bundle.name, key, entry.get("type"),
                )
                continue
            # Two bundles whose entry points share a file name stage to the
            # same sandbox path, and the launch args are rewritten to that
            # bare name, so the loser would run the winner's code under its
            # own name and environment -- with its own credentials in that
            # env. Drop it here, where it is neither staged nor launched,
            # rather than let the two disagree about which file is running.
            # The shipped set cannot reach this: it is gated at build time by
            # test_entrypoint_basenames_are_unique_across_bundles.
            # The registry keys connectors by name and the ownership map
            # keeps the first claim, so a name declared twice runs the second
            # bundle's code while attribution points at the first: disabling
            # the bundle that actually ships it removes nothing, and disabling
            # the other removes a server it does not own.
            if (owner := claimed_names.get(server.name)) is not None:
                logger.error(
                    "bundled plugin %s: server %r is already declared by %s; "
                    "dropping it",
                    bundle.name, key, owner,
                )
                continue
            if (filename := _staged_entrypoint(server)) is not None:
                if (owner := staged.get(filename)) is not None:
                    logger.error(
                        "bundled plugin %s: server %r stages its entry point "
                        "as %s, which %s already claimed; dropping it",
                        bundle.name, key, filename, owner,
                    )
                    continue
                staged[filename] = f"{bundle.name}/{key}"
            claimed_names[server.name] = f"{bundle.name}/{key}"
            servers.append(server)
    return servers


def bundled_skill_dirs() -> list[Path]:
    """Each bundle's ``skills/`` directory, in bundle order.

    A bundle carries its skills as directories rather than as a list, so a
    package that ships one is read the same way whether it arrived in the
    image or as an upload. The manifest's ``skills`` key only says what the
    Plugins page should show.
    """
    return [d for p in _bundle_dirs() if (d := p / "skills").is_dir()]
