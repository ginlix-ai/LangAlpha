---
title: Workspaces
description: A workspace is a folder on one of your computers with its own threads, files, notes, memory, skills and tools. How naming, layout, Home and trust work.
---

A **workspace** is where one body of work lives: a folder on one of your [computers](/concepts/computers/), together with its threads, files, notes and an Analyst of its own. You might keep one workspace per company, per theme or per portfolio. The folder is named after the workspace, so a workspace called "NVDA" is the folder `NVDA` on its computer, beside your other workspaces.

A second workspace opens in seconds because it is a new folder on a machine that is already running, not a new machine. The order of things is User, Computer, Workspace, Thread, Turn, and a workspace holds many [threads](/concepts/threads-and-turns/).

## Names and folders

The workspace name is also its folder name, so a few rules apply:

- A name is at most 80 characters and needs at least one usable character.
- Names are unique per user, ignoring case. "Research" and "research" would be the same folder on some disks, so they count as one name.
- Spaces and non-English letters stay as you typed them. Characters that break a path or a shell command (`/`, `\`, `:`, `$`, backticks, quotes and control characters) are replaced with a dash, and leading dots, dashes and spaces are dropped.
- A few names are reserved because a computer already uses them as folders: `Home`, `_internal`, `mcp_servers`, `tools` and `code`.

When you rename a workspace, the new name shows at once. The folder on the computer follows the next time the computer is opened for that workspace, between turns and never during one.

You choose the computer when you create a workspace. Without a choice, the new workspace goes on your [primary computer](/concepts/computers/#the-primary-computer). You can later [move a workspace](/concepts/computers/#moving-a-workspace-to-another-computer) to another computer, or duplicate it: a duplicate is named "name (copy)", gets a copy of the files, and lands on the same computer. Files you add while creating a workspace are uploaded right after it is ready, up to 50 MB each.

## What is inside a workspace folder

The agent keeps a simple layout, and you can read and edit all of it in the file browser:

| Item | What it is |
|---|---|
| `agent.md` | The workspace's notebook, which the agent maintains: purpose, key findings, what each task produced and where. It is shown to the agent at the start of every conversation here. |
| `data/` | A shared folder for datasets the whole workspace uses. |
| One folder per task | Each task that produces files gets a folder named for it, for example `sector_analysis/`, with its own `data/` and `charts/` as needed. |
| `.agents/` | Agent infrastructure, behind the file browser's system files toggle: this workspace's memory and skills, tool wrappers and docs for the data servers enabled here, saved transcripts, scratch files and large tool results. |

The agent is told not to write task files loose at the top of the folder. See [Structuring workspaces](/best-practices/structuring-workspaces/) for patterns that work.

## What belongs to a workspace and what to your account

Some things are specific to one workspace, and others are yours everywhere:

- **Threads, files and `agent.md`** belong to the workspace.
- **Memory** has two tiers: user memory applies to every agent in every workspace, and workspace memory applies only to agents in this workspace. See [Memory, memos and the scratchpad](/concepts/memory-memos-scratchpad/).
- **Skills** you install on your account appear in every workspace, and you can switch one off in a single workspace. A skill uploaded inside a workspace runs only there, and overrides an account skill with the same name. See [Skills](/concepts/skills/).
- **MCP servers and connectors** are installed on your account and switched on per workspace. A connector you add from inside a workspace is on in that workspace only, and stays off in your other workspaces and in new ones until you switch it on there. Only servers switched on in a workspace have wrappers and docs there. See [Plugins, MCP servers and PTC](/concepts/plugins-mcp-ptc/).

## Home

**Home** is a special folder for the [Chief of Staff](/concepts/chief-of-staff-and-analysts/), the agent that works across your workspaces. It sits beside your workspace folders on your primary computer, and the Chief of Staff keeps its own `agent.md` there. Chat apps call this agent "All workspaces".

Home is reserved. You cannot name a workspace `Home`, rename Home, delete it, or duplicate it. It moves only when you make a different computer primary.

## Workspaces share a computer

Every workspace on a computer runs as the same operating system user, on the same disk. One workspace's code can read another workspace's files, and they share the computer's MCP credentials. Turning a tool off in a workspace changes what the agent reaches for, but it is not a security boundary. If you need work kept apart, put it on separate computers. See [Sandboxes and SSH computers](/concepts/sandboxes-and-ssh/#trust-between-workspaces).

Starting or stopping the computer also affects every workspace on it, and they fill one disk together.

:::note[Hosted and self-hosted]
**Hosted:** Your plan can limit how many workspaces you can create.

**Self-hosted:** No plan limits apply to workspaces.
:::

## Related

- [Computers](/concepts/computers/)
- [Files and what persists](/concepts/files/)
- [Chief of Staff and Analysts](/concepts/chief-of-staff-and-analysts/)
- [Structuring workspaces](/best-practices/structuring-workspaces/)
