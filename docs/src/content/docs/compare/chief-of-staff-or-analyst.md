---
title: Chief of Staff or Analyst?
description: Ask the Chief of Staff for quick answers and questions about past work. Open a workspace and use its Analyst for hands-on work you want to keep in one place.
sidebar:
  order: 1
---

Ask the **Chief of Staff** when a request is quick, spans several workspaces, or is about work you already have. Open a **workspace** and talk to its **Analyst** when the work is hands-on and you want the files, history and follow-ups to live in one place. If you are unsure, start with the Chief of Staff: it answers what it can, and hands real analysis to the right Analyst (creating a workspace if none fits) without you picking one. The roles are the same agent working in two places, described in [Chief of Staff and Analysts](/concepts/chief-of-staff-and-analysts/).

## Side by side

| | Chief of Staff | Analyst (in a workspace) |
|---|---|---|
| Where it works | Home, across all workspaces | One workspace's folder |
| Best for | Quick facts, "what did we conclude?", digests, setting things up | New research, models, charts, reports, code |
| Output lives in | Home | The workspace, with its threads |
| Reads other workspaces | Yes, their folders, notes and past threads | Works inside its own workspace |
| Starts new analysis | Hands it to a workspace's Analyst | Does it itself, with all five built-in subagents |
| Creates workspaces, manages threads | Yes | No |
| In chat apps | "All workspaces" | A workspace you pick with `/workspace` |
| Reports back | Tells you when a handed-off task finishes | You read the thread |

## Requests that suit the Chief of Staff

- "Morning, anything new?" It gives a short catch-up from your recent threads, today's automation runs (failures first) and your holdings.
- "What did NVDA close at?" or "Is MSFT expensive right now?" A few sentences, no files. It then offers to have an Analyst run a full valuation.
- "What did my NVDA analysis conclude?" It reads that workspace's notes and latest report, summarizes the findings and links the file.
- "Compare what my NVDA and AMD workspaces concluded." It reads both and writes the comparison in Home.
- "Start fresh on lithium miners." It creates a workspace and hands the first task to its Analyst.
- "Set up a weekday 8:00 briefing and send it to Slack." It manages [automations](/concepts/automations-wakeups-commands/) for you.

## Requests that suit a workspace's Analyst

- "Build a DCF for NVDA and stress test the margin assumptions." It runs code, pulls financial data, and saves the model beside earlier work.
- "Write an initiation report on this company, with charts." The report and charts land in the workspace's file panel.
- "Continue the earnings preview from last week and add the new guidance." It builds on that workspace's past threads and files.
- Long, iterative sessions where you read, steer and revise as it works. You can [steer a running turn](/concepts/threads-and-turns/#sending-a-message-while-the-agent-works) in any thread.

## When the choice matters

**Keep related work together.** Give each real task its own workspace, such as one per company or theme, so threads share files and data. Asking the Chief of Staff to do substantial work leads to the same place anyway: it hands the task to that workspace's Analyst. See [Structuring workspaces](/best-practices/structuring-workspaces/) and [When to delegate](/best-practices/when-to-delegate/).

**Know who has the files.** Whatever the Chief of Staff builds itself goes in Home. If you want it with the rest of a workspace's research, ask for the task to go to that workspace.

**Say so if you want it done in Home.** The Chief of Staff hands off complex analysis by default. If you say "build it here yourself, do not hand it off", it builds it in Home.

## Related

- [Chief of Staff and Analysts](/concepts/chief-of-staff-and-analysts/)
- [Workspaces](/concepts/workspaces/)
- [When to delegate](/best-practices/when-to-delegate/)
- [Chat apps](/concepts/chat-apps/)
