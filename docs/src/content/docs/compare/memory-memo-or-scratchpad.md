---
title: Memory, memo or scratchpad?
description: "Which of LangAlpha's three stores to use: memory for what the agent should always know, memos for documents it should cite, scratchpad for one thread's work."
sidebar:
  order: 3
---

Use **memory** for durable facts about you and your work that the agent should know before it acts, use a **memo** for a document you want the agent to read and cite, and use the **scratchpad** for temporary files and task notes that only matter inside one thread. The three differ in who writes them, how far they reach and how long they last.

| | Memory | Memo | Scratchpad |
|---|---|---|---|
| What it holds | Short notes: your preferences, mandate, conventions, a workspace's thesis | Whole documents: theses, reports, decks, models, filings | Scripts, drafts, logs, and the agent's running notes on a task |
| Who writes it | The agent, when you tell it something worth keeping (say "remember this") | You, by upload or by adding a workspace file | The agent |
| Can the agent change it | Yes: it adds, updates and removes entries | No: read-only to the agent | Yes |
| Can you read it | Yes, in the file panel's Memory tab (read-only) | Yes, in the Memos panel, and you can edit or delete | It is the agent's own working space, not a place you manage |
| Reach | User tier: every workspace and thread. Workspace tier: one workspace. Home has its own | All your workspaces and threads | One thread |
| Loaded into context | An index (`memory.md`) every turn; details when relevant | A catalog of names and one-line descriptions; a memo is opened only when needed | A folder path; files only when the agent opens them |
| Size limit | The index is cut at 32,768 characters (about 200 lines) | 5 MB per upload, 1 MB of extracted text, 500 PDF pages | No stated limit |
| Lifetime | Until removed | Until you delete it | Until the thread is archived or deleted |

## What to put where

**Memory** is for facts that should shape many future turns:

- "I run a concentrated long-only book, benchmark the S&P 500, and never use margin."
- "Reports should lead with the conclusion and put tables after it."
- In one workspace: "This workspace tracks the semiconductor supply chain thesis; the watchlist is NVDA, AMD, TSM."

Keep market data, one-off results, headlines and price targets out of memory. They go stale by the next turn, and the agent is told not to save them.

**A memo** is for source material you want quoted accurately. Upload your own IC memo, a 10-K PDF, a valuation framework or a term sheet. The agent reads the catalog first, then opens the memos that match the question and cites them. Because a memo is the same on every thread, it is the right place for anything long, and anything you want the agent unable to rewrite. Scanned PDFs without a text layer are refused, so use a text version.

**The scratchpad** is for work in progress. The agent uses it for throwaway scripts and drafts so they stay out of your deliverables, and for a note per task: your request, your decisions, why it took an approach, findings and what remains. If a long conversation is summarized to free up context, the agent goes back to these notes. Nothing needs to survive the thread. When you archive a thread, its scratchpad is deleted, so anything you want to last should be saved in a workspace file, in memory, or in a memo.

## How to decide

- Will you want this in a different thread next week? If it is a short fact, use **memory**. If it is a document, use a **memo**.
- Do you want the agent to quote it exactly and never change it? Use a **memo**.
- Is it only useful until this task is done? Leave it in the **scratchpad**, which the agent manages itself.
- Is it a deliverable, such as a model or report? It belongs in the workspace's files, not in any of these. See [Files](/concepts/files/).

The scratchpad is on by default and can be switched off in Settings, where it is called "Scratchpad and working notes". Memory and memos are managed by you through conversation and the Memos panel.

## Related

- [Memory, memos and scratchpad](/concepts/memory-memos-scratchpad/)
- [Files](/concepts/files/)
- [Skill, plugin or MCP server?](/compare/skill-plugin-or-mcp-server/)
- [Threads and turns](/concepts/threads-and-turns/)
