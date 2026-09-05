# Bundled plugins

Every directory here is an [Agent Plugins 1.0.0](https://agent-plugins.org)
package, the same format a user uploads on the Plugins page. These ship inside
the app: nothing fetches them, they take none of a user's plugin slots, and
uninstall has nothing to remove. Together they are where the built-in MCP
servers and skills live, and the loader reads this directory at startup rather
than a list in `agent_config.yaml`.

```
<bundle>/
  plugin.json   identity, plus our extension block
  mcp.json      the servers, portable
  <name>_mcp_server.py   an entry point this bundle owns
  skills/       one directory per skill, each with a SKILL.md
  README.md     only when there is something non-obvious to say
```

A bundle carries its own files. The servers `mcp.json` names sit next to it,
and the skills it owns are the directories under its `skills/` -- the same
layout an uploaded package arrives in, so nothing about reading one depends on
whether it shipped in the image.

## What goes where

`mcp.json` is the portable half and it is closed: `additionalProperties: false`
on the document, on each server, and on each transport variant. Only
`type`, `command`, `args`, `env`, `cwd` (stdio) or `type`, `url`, `headers`
(remote) are legal there.

Everything else goes in `plugin.json` under `extensions["ai.langalpha"]`, the
one extension point the format defines. Strip that block and what is left
still installs into any Agent Plugins host, which is why the extras live there
instead of bending `mcp.json`:

```json
"extensions": {
  "ai.langalpha": {
    "servers": {
      "<mcp.json key>": {
        "description": "one line, shown in the UI and the prompt",
        "instruction": "when and how the agent should reach for it",
        "tool_exposure_mode": "summary | detailed",
        "vault_blueprints": [{ "name": "...", "label": "...", "regex": "..." }]
      }
    },
    "icon": "vendor.example.com"
  }
}
```

- **`servers`** is keyed by the `mcp.json` entry key. `description` and
  `instruction` reach the agent's prompt, so they are worth writing carefully;
  `tool_exposure_mode` decides whether the agent sees full tool signatures
  (`detailed`) or a summary. An uploaded plugin may declare `description`,
  `instruction` and `tool_exposure_mode` too; `vault_blueprints` is read here
  only, because a user plugin declares its credentials through the namespace's
  `secrets[]`, which also says where each one binds.
- **`icon`** names the site that owns a wrapper bundle's mark. Ours ship their
  logo with the frontend instead, so a self-host with no outbound network
  still draws them.

## Adding a server to a bundle

Declare it in the bundle's `mcp.json`, describe it under `servers`, and
restart. `${VAR}` in a bundle's `args`, `env`, `url` or `headers` expands from
the process environment, so credentials stay in `.env`.

Servers we own live in the bundle that declares them and launch with
`uv run python`, from the shared environment; the runtime several of them share
-- `_bootstrap`, the response envelope, the output schemas -- stays in
`mcp_servers/`. Anything we do not own must launch isolated instead
(`uvx --from '<package>==<version>'`, or `npx <package>@<version>`): the shared
environment pins the `mcp` SDK, and a third-party server born under one major
dies on the next. Ours import through `_bootstrap` and survive it.

## If you are self-hosting

Three places to put a server, with three different lifetimes.

**A bundle of your own.** A new directory here is read exactly like ours:
several servers, descriptions, exposure modes, vault blueprints and an icon in
one package, with its skills as directories under `skills/`. `./plugins` is
bind-mounted in `docker-compose.yml`, so adding one is a restart rather than a
rebuild.

**`mcp.servers` in `agent_config.yaml`.** One server, no packaging. This is
also where you change something we ship. A name a bundle already declares is an
override, not a collision: the keys you write are laid over the shipped server
and every key you leave out keeps its shipped value.

```yaml
servers:
  - name: "price_data"
    enabled: false
```

Two lines, and the subprocess never launches. Nothing else about the server has
to be restated, and the same shape retunes one field:
`tool_exposure_mode: "detailed"` on its own leaves the command and the
description alone. Override here rather than by editing a manifest, so the next
`git pull` cannot take the choice back.

**The Plugins page.** Upload a package, or install from a public git URL. These
land as per-user rows in the database, apply without a restart, and are the only
one of the three that needs no filesystem access to the host.
