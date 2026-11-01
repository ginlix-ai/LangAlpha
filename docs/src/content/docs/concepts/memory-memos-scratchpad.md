---
title: Memory, memos and scratchpad
description: How LangAlpha remembers you (agent-written memory), grounds answers in your documents (memos) and keeps per-thread working files (scratchpad).
---

LangAlpha gives the agent three different places to keep information: **memory** is what the agent writes down about you and your work so future threads start informed, **memos** are documents you upload for the agent to read and cite, and the **scratchpad** is a throwaway folder and set of task notes that belong to one thread. They differ in who writes them, how far they reach and how long they last. If you only need the short comparison, see [Memory, memo or scratchpad?](/compare/memory-memo-or-scratchpad/).

## Memory

Memory is long-term context that the agent writes itself: how you like reports written, your risk limits, the conventions you work by, what a workspace is trying to prove. It survives across threads and is kept apart from workspace files, so it also survives resetting a workspace. You can say "remember this" to add to it.

Memory has two tiers:

| Tier | Reach | Typical content |
|---|---|---|
| User | Every workspace and thread you have | Investor profile, durable style preferences, long-term context |
| Workspace | One workspace, shared by its threads, invisible to other workspaces | The thesis, watchlist and research plan for that workspace |

[Home](/concepts/chief-of-staff-and-analysts/), the Chief of Staff's folder, has its own workspace-tier memory, shared across the Chief of Staff's threads and not visible to the Analysts. It holds how you want the Chief of Staff to work: how to catch you up, what to report and what to leave out. User-tier memory applies to every agent, Analysts included.

### How memory is loaded

Each tier has an index file called `memory.md`. It is a list of one-line pointers, and each pointer leads to a detail file that holds the substance (one topic per file, labelled by what it holds: who you are, how you want work done, what a workspace is doing, or where to look). The index is loaded into the agent's context every turn, so it stays short: it is cut at 32,768 characters, about 200 lines, and the agent is warned as it fills. The agent reads a detail file only when it looks relevant, for example when you refer to earlier work.

### Reading and correcting memory

You can read both tiers in the chat's file panel, on the Memory tab, which has a User list and a Workspace list. The panel is read-only. To change something, tell the agent. The agent is instructed to update an entry instead of duplicating it, to remove entries that turn out wrong, and to trust what it observes over a stale memory. It is also told not to save market data, one-off computed results, headlines or sell-side price targets, because they go stale.

## Memos

A memo is a document you upload so the agent can ground its work in your own research: past theses, notes, models, reports. Memos belong to you, not to a workspace, so every workspace and thread can read them. They are read-only to the agent: it can read, search and cite them, but it cannot write or edit them.

- **Formats:** Markdown (`.md`), text (`.txt`), CSV, JSON and PDF. Text files must be UTF-8.
- **Limits:** 5 MB per upload, and 1 MB of extracted text. A PDF can have up to 500 pages. A PDF needs at least 50 characters of readable text, so a scanned PDF with no text layer is refused.
- **Description and summary:** after you upload, LangAlpha generates a short description and a summary in the background using your background model. Until it finishes the memo shows "Summary generating". The agent reads a catalog of every memo's filename and one-sentence description first, then opens the ones that matter.
- **Ways to add one:** upload in the Memos panel, or add an existing workspace file to your memos from the file panel ("Add to memo"). If the workspace file changes later, the file panel offers "Sync with memo".
- **Editing:** you can edit a memo's text and delete memos in the Memos panel.

## Scratchpad

The scratchpad is a folder that belongs to a single thread. The agent puts temporary files there, such as a script written only to get a result, an exploratory check, a draft or a log, so they stay out of the files you will see and out of `/tmp`, which is not backed up. It also keeps a **task note** per task in the same folder: the request in one line, then what you decided, why it chose an approach or dropped one, findings with their source, work it handed off, and what remains. When a long thread is summarized to free up context, the summary points the agent back to these notes, so details are not lost in paraphrase. The agent is asked to write its notes before a summary happens and again after a run of tool calls with no note written.

The scratchpad is a feature you control. In Settings it is called "Scratchpad and working notes" and it is on unless you turn it off. The tradeoff the settings page states: the agent spends a few extra tool calls per task keeping notes. When you archive a thread, its scratchpad and notes are removed (deleted threads lose theirs too), so anything you want to keep belongs in the workspace's own files, in memory or in a memo.

## Your profile

Separate from memory, your profile is data you manage in the app that the agent can read as files: your portfolio holdings (quantity and cost basis), your watchlists, your preferences (risk tolerance, investment style, agent settings) and your account details such as name, timezone and locale. The agent checks the portfolio, watchlist and preference files when a question involves your positions, tracked stocks or style, and can edit them; the server validates every edit. Preferences you write are followed as instructions.

:::note[Hosted and self-hosted]
**Hosted:** Memory, memos, the scratchpad and your profile work as described, and the scratchpad is on by default.

**Self-hosted:** The same behavior. An operator can turn the scratchpad off for everyone, or change whether it is on by default, in the `features:` section of `config.yaml`. PDF memos keep their original file in object storage when you have configured it.
:::

## Related

- [Memory, memo or scratchpad?](/compare/memory-memo-or-scratchpad/)
- [Files](/concepts/files/)
- [Threads and turns](/concepts/threads-and-turns/)
- [Workspaces](/concepts/workspaces/)
