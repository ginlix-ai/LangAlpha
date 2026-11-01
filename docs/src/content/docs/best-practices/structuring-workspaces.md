---
title: Structure your workspaces
description: How to split research into workspaces and threads, when a separate computer helps, and how to keep a workspace rebuildable after a move or restore.
sidebar:
  order: 1
---

A workspace is the unit that keeps your work together: its files, its `agent.md` notebook, its workspace memory and every thread you run in it. How you cut workspaces decides what the agent can build on, what it can see and what survives a move to another computer.

## One workspace per thesis, sector or portfolio

Each workspace has its own Analyst. Every thread in it shares the same files, and the Analyst keeps `agent.md` current: the workspace's purpose, key findings, and what each task produced and where. Workspace memory (thesis, watchlist, research plan) is shared by the threads in that workspace and is invisible to other workspaces. Your user memory and your memos follow you into every workspace.

So put work that should build on itself in one workspace, and give a new thesis its own: "NVDA", "Semis supply chain" and "Core portfolio" keep three notebooks and three memories. The Analyst is told to keep `agent.md` under about 2,000 words, so a workspace that covers everything has to squeeze all of it into that one notebook.

## Start a new thread or continue one

Continue the thread when you are iterating on one deliverable, because the agent still holds the whole conversation. Start a new thread for a new question in the same workspace: the files, `agent.md` and workspace memory carry over, and the context starts clean. A long thread is compacted when its context fills: older turns are folded into a summary, and the full transcript is saved so the agent can read back what the summary left out. The summary is a paraphrase, so anything you cannot afford to lose belongs in a file or in `agent.md`. You can also run `/compact` yourself once a turn has finished.

Two more behaviors shape the choice:

- A thread runs one turn at a time. A message you send while the agent is working steers that turn instead of starting a second one. For parallel work, start another thread or let the agent use subagents.
- Threads in one workspace share its files, so ask each parallel thread to use its own task directory.

:::caution
Archiving a thread removes its scratchpad notes. Move anything lasting into `agent.md`, memory or a file before you archive.
:::

## Use a separate computer for separation

Workspaces on one computer run as the same operating system user. Code in one workspace can read its siblings' files, and the computer's connector wiring covers every workspace on it. Switching a connector on for only one workspace routes the agent's tool use. It is not a lock.

If one workspace must stay out of another's reach, such as client work or a connector with sensitive access, put it on its own computer. A separate computer separates files and running code. It does not separate secrets: connectors and keys belong to your account and reach every computer you own, so keeping a key away from a workspace takes a separate account. [Which computer?](/compare/which-computer/) compares the options.

## Keep a computer rebuildable

LangAlpha backs up each workspace's files so a lost folder can be restored. Moving a workspace to another computer likewise leaves out installed dependencies, caches and `.git` history. The backup skips folders that can be regenerated, at any depth in the tree:

| Skipped | Examples |
| --- | --- |
| Dependencies and virtual environments | `node_modules`, `.venv`, `venv`, `vendor` |
| Caches and build output | `.cache`, `.next`, `.nuxt`, `__pycache__`, `.pytest_cache`, `.mypy_cache`, `.ruff_cache`, `.npm` |
| Version control and tool state | `.git`, `.config`, `.local`, `.ipython` |
| Compiled files | `.pyc`, `.pyo`, `.so`, `.dylib`, `.o` |

Three habits follow:

1. Keep a requirements file (or an install script) in the workspace and ask the agent to reinstall from it after a restore. A restored workspace comes back without its installed packages.
2. Do not store irreplaceable data in a folder with one of those names. A folder called `vendor` holding vendor data is skipped as well.
3. Commit and push cloned repositories. A repository comes back as a plain working tree without its history.

Files too large for the deployment's transfer path are listed as not saved. On a computer you connect over SSH, a Back up files switch controls whether the backup keeps updating. Turn it off and the last backup stays as it was.

## Use Home for cross-workspace questions

Home is the Chief of Staff's folder. The Chief of Staff can read every workspace, so ask it for questions that cross workspaces: "compare what my NVDA and AMD workspaces concluded" is answered from their `agent.md` notebooks and latest files, and the comparison is written in Home. For new hands-on work it hands off to the workspace's Analyst. Its answers are only as current as each `agent.md`.

## Related

- [Workspaces](/concepts/workspaces/)
- [Chief of Staff or Analyst?](/compare/chief-of-staff-or-analyst/)
- [Which computer?](/compare/which-computer/)
- [Memory, memos or scratchpad?](/compare/memory-memo-or-scratchpad/)
- [Delegate work well](/best-practices/when-to-delegate/)
