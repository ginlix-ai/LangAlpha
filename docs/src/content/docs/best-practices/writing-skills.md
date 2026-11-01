---
title: Write your own skills
description: "How to write a skill LangAlpha loads at the right time: SKILL.md format, slash command rules, upload limits, tiers, GitHub install and plugin packaging."
sidebar:
  order: 3
---

A skill is a folder with a `SKILL.md` file that teaches the agent one repeatable job, such as your house format for an earnings note. LangAlpha uses the open Agent Skills format: YAML front matter with a `name` and a `description`, followed by Markdown instructions, plus optional files the instructions point to. This page covers how to write a skill the agent picks up at the right moment and how to install it.

## Write the description as a trigger

The agent does not read every skill. On each turn it sees one line per enabled skill, the name and the description, and it reads a skill's `SKILL.md` only when a task fits that line. The description alone decides whether your skill loads. It costs context on every model call, and it is capped at 1,024 characters.

Say what the skill produces and when to use it, with the phrases you would actually type. The built-in `earnings-preview` skill does this:

```yaml
---
name: earnings-preview
description: "Pre-print setup for a company about to report: the expectation bar, EPS-quality watch, call questions, scenarios and the reaction framework. Triggers on earnings preview, what to watch for [company] earnings, pre-earnings setup, preview Q[N]."
---
```

## Keep SKILL.md short and push detail into files

The whole `SKILL.md` body is loaded into context when the skill runs, and it can be at most 64 KB. Put the steps in it and move long reference material into a `references/` folder, then tell the agent when to read each file. The built-in `research-conventions` skill is a worked example: its `SKILL.md` lists five reference files, each with the situation that calls for it. `earnings-preview` ends each step with a "Complete when" line so the agent knows when the step is done.

## Name it and give it a slash command

A name is 1 to 64 characters: lowercase letters, digits and single hyphens, with no leading or trailing hyphen. You run a skill by typing `/` in the chat box. The command defaults to the skill name. Set a `command:` line in the front matter, or use Edit command on the skill's page, to change it. A command uses lowercase letters, digits and hyphens, up to 64 characters.

You cannot reuse the name or command of a built-in skill, or the composer's own commands: `subagent`, `compact`, `compaction`, `summarize`, `offload` and `truncate`. An upload that clashes is refused with the reason.

## Upload limits

| Limit | Value |
| --- | --- |
| Archive (zip) | 2 MB |
| Uncompressed total | 8 MB |
| One file | 1 MB |
| Files per skill | 64 |
| `SKILL.md` | 64 KB |
| Description | 1,024 characters |
| Skills per account | 200, with 32 MB in total |

Open Plugins, then Skills, and upload a folder, a .zip or a `SKILL.md`, or paste one in. A zip needs `SKILL.md` at its root or inside one top-level folder. The upload drops hidden files (including a stray `.env`), `node_modules` and macOS leftovers, and it rejects symlinks and unsafe paths.

## Choose a tier

An uploaded skill is available in every workspace. You can instead keep a skill in one workspace. A copy uploaded to a workspace shadows a user skill of the same name there, so you can run a variant in one place without changing the rest. Moving a skill between the two scopes is refused when the destination already has that name. Built-in and plugin skills can be switched off per workspace but not deleted.

## Install from GitHub, or package a plugin

On the Plugins page you can install from a public git URL. A repository with no `plugin.json` still installs when it holds folders with a `SKILL.md`: each becomes a skill of a plugin named after the folder or repository. You can also ask the agent to install a skill from a repository into the workspace.

To ship skills together with connectors, make a plugin. It follows the Agent Plugins format: a `plugin.json`, an `mcp.json` for MCP servers, a `skills/` folder with one directory per skill, and an `agents/` folder for subagents. Uninstalling the plugin removes its skills, connectors and subagents together. [Skill, plugin or MCP server?](/compare/skill-plugin-or-mcp-server/) explains when to use which.

:::note[Hosted and self-hosted]
**Hosted:** you add skills and plugins from the Plugins page.

**Self-hosted:** you can also drop a skill folder into `~/.ptc-agent/skills` (the `skills.user_skills_dir` setting), which overrides a shipped skill of the same name and reserves that name. The Discover tab lists a catalog only when `PLUGIN_MARKETPLACE_URL` is set in `.env`.
:::

## Related

- [Skills](/concepts/skills/)
- [Skill, plugin or MCP server?](/compare/skill-plugin-or-mcp-server/)
- [Plugins, MCP and PTC](/concepts/plugins-mcp-ptc/)
- [Get good research output](/best-practices/research-output/)
