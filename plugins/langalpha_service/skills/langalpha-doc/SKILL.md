---
name: langalpha-doc
description: "How the platform under you works: the computer and its workspaces, files and what survives a restart, conversation transcripts, saved tool results, memory, chat apps, and adding skills, MCP servers, brokerages or plugins. Read it when the user points back at an earlier conversation, asks how their chat messages reach you, when something is missing or behaves unexpectedly, and before adding any of those."
---

# How LangAlpha Works

You run on a **computer**: one sandbox machine that belongs to the user. Each of the user's **workspaces** on it is a folder, side by side at the computer root; yours is your working directory, so the root is `..`. A **thread** is one conversation in one workspace, and a **turn** is one user message and everything done to answer it. Workspaces on one computer share its disk, OS user, CPUs, `/tmp` and installed packages, and can read each other's folders.

## Reference files

- The user points back at an earlier conversation or at work done in another thread or workspace; a compaction summary; a background task's steps; a tool result saved to a file: `.agents/skills/langalpha-doc/references/history.md`.
- Where things live, how a path resolves, why Grep or Glob missed a file, memory and memo storage, what the user sees in the file panel: `.agents/skills/langalpha-doc/references/files.md`.
- Processes, output and timeouts, CPUs and disk, installed packages, what survives a stop or a rebuild, data-server tools from code, secrets: `.agents/skills/langalpha-doc/references/computer.md`.
- Installing, writing or changing a skill, here or for every workspace: `.agents/skills/langalpha-doc/references/skills.md`.
- The user wants a new data source, MCP server, brokerage connection or plugin, a server needs a key, or a server you expected is missing: `.agents/skills/langalpha-doc/references/plugins.md`.
- Talking with the user in a chat app, or sending a message or file to Slack, Discord, Telegram or iMessage: `.agents/skills/langalpha-doc/references/chat.md`.
- The user asks which conversation or workspace their chat message reached, why an answer came from another conversation, or how `/new`, `/workspace` and replies pick one: `.agents/skills/langalpha-doc/references/chat-conversations.md`.

## Traps

- Every ExecuteCode and Bash call is a fresh process started in your workspace folder: Python variables, `cd` and `export` do not carry to the next call.
- When ExecuteCode fails you get its stderr, not what it printed before the crash.
- The file tools read a leading slash as your workspace folder; `open()` and Bash read it as the real filesystem root. Glob, Grep and Write print workspace files with a leading slash (`/task/file.md`), so drop it before pasting a path into code.
- Grep skips hidden and git-ignored files and folders, `.agents/` included, unless `path` points inside one.
- Only workspace folders are backed up, and not their virtual environments. Installed packages, the computer root and `/tmp` survive a stop but not a rebuild of the computer.
- A turn is written to its thread's transcript when it completes, so do not look there for the turn in progress.
- Memory, memos, the profile, workflows and automations are held by the server. Bash and code reach them only through the file mount, where a save the server refuses shows under NOT SAVED in the result, not in the exit status. Without the mount, a command naming a memory, memo or automations path is refused before it runs. The chat-app settings in `.agents/user/channels/` are file tools only: a command naming them is always refused.
- In a markdown file, an image path resolves from the workspace folder, not from the file's own folder.
- `plt.show()` output goes nowhere. Save charts with `savefig` into `<task>/charts/` and link the file.
