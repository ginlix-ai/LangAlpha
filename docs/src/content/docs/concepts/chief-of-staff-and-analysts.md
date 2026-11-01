---
title: Chief of Staff and Analysts
description: LangAlpha runs one agent in two roles. The Analyst works inside a workspace. The Chief of Staff works from Home across workspaces and hands off deep work.
---

LangAlpha has one agent that runs in one of two **roles** on each turn. The **Analyst** is the agent inside a workspace: it does the research and builds the deliverables, and they stay in that workspace. The **Chief of Staff** is the same agent working for you across all workspaces: it answers quick questions, reads what your workspaces already hold, and hands new hands-on work to the right workspace's Analyst. Which role answers is decided by where you start the thread: in a workspace you get its Analyst, and in the Chief of Staff you get the Chief of Staff.

## The Analyst

An Analyst works inside one [workspace](/concepts/workspaces/). Every thread in that workspace shares the same files, data and past work, so research builds on itself. The Analyst has the full toolset: running code, financial data tools, charts, filesystem tools and subagents, including the built-in `equity-analyst` subagent for company analysis. What it produces (Excel models, Word documents, PDFs, dashboards, code) is saved in the workspace's folder, where you look for it.

## The Chief of Staff

The Chief of Staff works in **Home**, its own folder on your primary [computer](/concepts/computers/), beside your workspace folders. Whatever it produces itself, such as a comparison across workspaces, a digest or a quick chart, is kept in Home. Its threads are listed under Chief of Staff in the web app, and chat apps call it "All workspaces".

It does four kinds of work:

1. **Answers directly.** Quick facts, prices, news and a quick read on a stock, in a few sentences and without writing code or files.
2. **Answers from work that already exists.** It can read every workspace's folder and notes, read what the Analysts wrote in past threads, and search the transcripts of earlier conversations on your computers (see [Threads and turns](/concepts/threads-and-turns/#searching-past-threads)). Ask "what did my NVDA analysis conclude?" and it looks, links the report and summarizes the findings.
3. **Manages workspaces and threads.** It can list your workspaces, create one for new work or delete one, and list, stop or delete threads. Creating a workspace and deleting a workspace or thread wait for your confirmation, and in Settings, under approvals, you can let it create workspaces without asking.
4. **Hands work to an Analyst.** For new research, a model, a report or code that a workspace should keep, it sends the task to that workspace's Analyst, creating the workspace first when none covers the topic.

It does not get the `equity-analyst` subagent, because new analysis belongs to a workspace's Analyst, which keeps the work and its history. If you ask for something to be done in Home and not handed off, it will do it there.

## How a hand-off works

When the Chief of Staff hands off a task, it picks the workspace (or continues an existing Analyst thread), writes the task for the Analyst, and starts it in the background. The task is written to stand alone, because the Analyst does not see your conversation with the Chief of Staff. In the web app each hand-off shows as a card you can follow or stop. Handing off can ask for your approval first. Settings, under approvals, has "Hand off work without asking" to skip the card.

The Chief of Staff's turn then ends: it tells you who is on it and does not wait. When the Analyst finishes, is stopped or fails, the Chief of Staff gets a message, reads what the Analyst wrote and tells you how it went. The Chief of Staff leaves the report-back off when you ask for none, for example because you plan to follow the work in that workspace yourself, and the result then waits in the workspace.

If you add to the task while the Analyst is still working, the Chief of Staff sends your addition to the same thread. The Analyst takes it in before its next step, as it would a message you typed in that thread (see [Threads and turns](/concepts/threads-and-turns/#sending-a-message-while-the-agent-works)).

## What the Chief of Staff starts with

At the start of a thread the Chief of Staff is given an overview of what you have been doing, so "my analysis" or "that report" usually needs no explanation:

- your five most recently used workspaces and your five most recent threads,
- today's automation runs (up to ten), with failures first,
- up to twenty of your portfolio holdings, and
- your computers and which workspaces sit on which.

It uses this to open with a short catch-up when you greet it, and to mention anything you have not yet heard: a failed run, a hand-off that finished or one waiting on an answer. For anything beyond the latest few, it lists workspaces and threads on demand.

## Several computers

If you have more than one computer, the Chief of Staff can work on the others too. Its Bash, code and file tools accept a computer name, so it can read a workspace folder that lives on your Mac or on a second cloud computer.  The data tools it uses in code run only on the computer that holds Home, so for work elsewhere it fetches the data there first and saves the result onto the other computer. See [Computers](/concepts/computers/).

## Where each role runs

| | Analyst | Chief of Staff |
|---|---|---|
| Works in | One workspace folder | Home |
| Started from | A workspace's thread | The Chief of Staff, or "All workspaces" in a chat app |
| Keeps its output | In that workspace | In Home |
| Built-in subagents | All five by default | All except `equity-analyst` |
| Hands tasks to a workspace's Analyst | No | Yes |
| Reads other workspaces | Works inside its own workspace | Yes, on any of your computers |

Not sure which to ask? See [Chief of Staff or Analyst?](/compare/chief-of-staff-or-analyst/).

## Related

- [Chief of Staff or Analyst?](/compare/chief-of-staff-or-analyst/)
- [Workspaces](/concepts/workspaces/)
- [Threads and turns](/concepts/threads-and-turns/)
- [Chat apps](/concepts/chat-apps/)
- [When to delegate](/best-practices/when-to-delegate/)
