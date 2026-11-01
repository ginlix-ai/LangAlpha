---
title: Skill, plugin or MCP server?
description: Choose between a skill (instructions), an MCP server (new tools) and a plugin (a package of both). Learn how they nest and when to write each one.
sidebar:
  order: 4
---

Write a **skill** when you want to teach the agent how to do something, write an **MCP server** when the agent needs a new tool or a connection to a service, and use a **plugin** to ship or install either of them together. A plugin is not a third kind of capability: it is a package that can bundle MCP servers and skills.

| | Skill | MCP server | Plugin |
|---|---|---|---|
| What it is | A folder with a `SKILL.md` of instructions, plus optional scripts and reference files | A program or web service that exposes tools | A package (`plugin.json`, optional `mcp.json`, optional `skills/`) |
| What it adds | Know-how: a workflow, a house style, a checklist | New tools: data feeds, a scraper, a broker, a database | Any mix of servers and skills, installed as one unit |
| Can it add a tool | No | Yes | Only through the servers it carries |
| How the agent uses it | Reads it on demand: one line in a manifest every turn, the full text when it fits | Imports the tools as Python modules in the sandbox, or calls them directly | Uses what the plugin contains |
| Where it lives | Your account, or one workspace | Your account, switched on per workspace | Your account |
| You add it by | Uploading a folder, zip or `SKILL.md` | Add MCP server form, or importing a config | Install plugin from a zip or git URL |
| Credentials | None | Secrets referenced as `${vault:NAME}` | Declares the secrets its servers need |

## How they nest

A plugin sits on top. Inside it you can have several MCP servers (listed in `mcp.json`) and several skills (one folder each under `skills/`). Installing a plugin adds all of them at once, switching the plugin off switches all of them off, and uninstalling removes them. LangAlpha's own tools and skills arrive this way, in six built-in plugins. Skills that call a server's tools are the common pairing: the server provides the capability, and the skill says when and how to use it.

## When to write which

- **You keep correcting the same behavior.** Write a **skill**. For example: how your firm formats an earnings note, or your checklist before a model goes out. Skills are plain text, load only when relevant, and cost nothing until used.
- **The agent needs data or an action it cannot reach.** Add an **MCP server**: a vendor's research API, a private database, a market data feed. A skill cannot add a tool, no matter what it says. If the vendor already publishes a server, add it with the form; if you are writing your own, run it isolated with a pinned version, and keep its keys in secrets.
- **You want to share or reinstall a set of related pieces.** Build a **plugin**: one folder with the servers and skills that belong together, installed from a git URL or zip in one step, with the secrets it needs requested at install.
- **You only need it in one workspace.** Upload a **workspace skill** from that workspace's settings. It overrides one of yours with the same name, and applies nowhere else.
- **You need to place orders.** None of these. Brokerages are their own connection type with approval controls. See [Brokerages and orders](/concepts/brokerages-and-orders/).

A useful test: if deleting the thing would remove a capability the agent cannot do any other way, it is a server. If deleting it would only change how the agent goes about a task, it is a skill.

## Related

- [Skills](/concepts/skills/)
- [Plugins, MCP servers and programmatic tool calling](/concepts/plugins-mcp-ptc/)
- [Writing skills](/best-practices/writing-skills/)
- [Brokerages and orders](/concepts/brokerages-and-orders/)
