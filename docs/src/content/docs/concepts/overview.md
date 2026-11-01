---
title: How LangAlpha fits together
description: The LangAlpha hierarchy of user, computer, workspace, thread and turn, the two agent roles, and which data lives where and follows you across computers.
---

LangAlpha organizes work in five levels: a **user** owns one or more **computers**, each computer holds **workspaces**, each workspace holds **threads**, and each thread is a series of **turns**. One agent does the work, in one of two roles: the **Analyst** inside a workspace, or the **Chief of Staff** for work that belongs to no single workspace. Some data belongs to a workspace's folder on a computer, and some belongs to you and follows you to every computer.

## The hierarchy

| Level | What it is | Learn more |
| --- | --- | --- |
| User | You: your account on the hosted platform, or the single local user of a self-hosted install. Your memory, profile, skills, plugins, MCP servers, secrets and brokerage connections belong here. | [Models and your own keys](/concepts/models-and-keys/) |
| Computer | The machine your workspaces run on: a Daytona, E2B or Docker sandbox, or your own machine over SSH. It has a size, a disk shared by its workspaces, and is started and stopped as one. | [Computers](/concepts/computers/) |
| Workspace | A folder on a computer for one thesis, sector or portfolio. It keeps files, the agent's notes and workspace memory across threads. | [Workspaces](/concepts/workspaces/) |
| Thread | One conversation in a workspace, with its own history. | [Threads and turns](/concepts/threads-and-turns/) |
| Turn | One run of the agent answering a message, from your message to its final reply. A thread runs one turn at a time. | [Threads and turns](/concepts/threads-and-turns/) |

## One agent, two roles

Every turn runs the same agent with the same tools. What changes is the role, picked from where the turn happens:

- **Analyst**: a turn inside a workspace. The Analyst does the research and produces the deliverables, which stay in that workspace.
- **Chief of Staff**: a turn outside any workspace. It runs in a folder called **Home** on your computer, beside the workspace folders. It answers quick questions and anything your workspaces already hold, reading their folders and past threads, and hands new research to a workspace's Analyst, which reports back when it is done. Chat apps list it as "All workspaces".

See [Chief of Staff or Analyst?](/compare/chief-of-staff-or-analyst/) for which one to ask.

## What lives where

Workspace files live in the workspace's folder on its computer, and are backed up so they survive the computer stopping or being rebuilt. Everything else the agent reads is held by the server and shown on every computer as ordinary files, so it carries across computers and rebuilds:

| Store | Belongs to | What it holds |
| --- | --- | --- |
| Workspace files | Workspace | Data, models, reports and the agent's notes for the workspace. |
| Memory | User and workspace | Durable preferences and findings the agent saves and keeps current. |
| Memos | User | Documents you upload. Read-only to the agent. |
| Profile | User | Your portfolio, watchlists and investing preferences. |
| Automations | User | One file per automation. |
| Workflows | User | Saved scripts that fan work out to many subagents. |
| Transcripts | Thread | Every past thread, searchable by the agent. Read-only. |

See [Files and what persists](/concepts/files/) and [Memory, memos and scratchpad](/concepts/memory-memos-scratchpad/).

## What the agent works with

- **Tools** to run Python and shell commands, read and write files, search and fetch the web, and look up quotes, filings and fundamentals.
- **MCP servers** for data and services, which the agent calls from the Python it writes. See [Plugins, MCP servers and programmatic tool calling](/concepts/plugins-mcp-ptc/).
- **Skills** that teach it how to produce a deliverable, loaded when needed. See [Skills](/concepts/skills/).
- **Subagents** it runs in parallel for pieces of a larger question.
- **Brokerage connections**, limited to the capabilities you switch on. See [Brokerages and orders](/concepts/brokerages-and-orders/).

## Work that happens later

A turn ends when the agent replies, but work can continue: an **automation** you set runs on a schedule or a price move, a **wakeup** the agent sets brings it back to the same thread, and a long **background command** reports back to the thread when it finishes. See [Automation or wakeup?](/compare/automation-or-wakeup/).

## Related

- [What is LangAlpha](/get-started/what-is-langalpha/)
- [Computers](/concepts/computers/)
- [Workspaces](/concepts/workspaces/)
- [Threads and turns](/concepts/threads-and-turns/)
- [Chief of Staff and Analysts](/concepts/chief-of-staff-and-analysts/)
