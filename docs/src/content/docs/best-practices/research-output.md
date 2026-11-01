---
title: Get good research output
description: "Practical ways to get sourced, decision-ready research from LangAlpha: give context, name the deliverable, check Sources, choose model effort and steer mid-run."
sidebar:
  order: 4
---

Good output from LangAlpha depends on three things you control: the context the agent starts with, the deliverable you name, and how you steer while it works. The built-in research skills also enforce conventions that tell you what to ask for and what to check.

## Give the agent what it cannot guess

Each of these has a different reach, so use the one that fits:

- **Memos** hold your own documents: past theses, notes, models, filings. Upload Markdown, text, CSV, JSON or PDF files up to 5 MB each. They belong to your account, so every workspace can read them, and the agent cannot edit them. It reads the memo index first and cites memos it uses.
- **Memory** holds durable preferences and facts: your mandate, benchmark, horizon, how you want reports written. Say "remember this". User memory applies in every workspace, and workspace memory applies only in that workspace. The research skills read your mandate from memory and ask only about what it leaves open.
- **Workspace files** are the work so far. Type `@` in the chat box to point the agent at a file in the workspace, and attach files or a chart when a thread needs them. If you already have a model or a deck, say whether to extend it or build alongside it. That is one of the few things the research skills ask about, because guessing wrong wastes the whole build.

## What the research conventions enforce

The `research-conventions` skill sits under the research skills, so you get these without asking:

- **Evidence labels.** Every load-bearing figure is tagged as reported, a management statement, a consensus estimate, our estimate, a judgement or an assumption. Delivered documents print the plain-word version.
- **Source order.** A filing beats an investor deck, and a deck beats a news summary. When two sources disagree on a figure that matters, the agent writes a conflict table instead of picking silently or averaging.
- **Timestamps.** Prices, multiples, consensus, short interest and any "currently" sentence carry an as-of. Consensus is fresh for seven days. A figure past its threshold is marked aging or stale, and a stale figure is refreshed or dropped.
- **A readiness line.** Each deliverable states one posture near the top: decision-grade, review-ready, screen-grade, not-ready or blocked. Read it first. It tells you whether the inputs support acting.
- **Depth.** A first pass runs 400 to 1,200 words, a working analysis 2,500 to 5,000, and a diligence package 25 to 50 pages. Name the depth you want. A short version of a finished report is rebuilt, not truncated.
- **Few questions.** The agent asks at most three questions before starting, with a recommended option first. If you skip, it takes the recommendation and tells you what it assumed. Put the horizon, the scope and the audience in your request to avoid the questions.

## Ask for the deliverable by name

Naming the skill or the format beats describing it. Type `/` in the chat box to browse skills such as `dcf-model`, `comps-analysis`, `earnings-preview`, `earnings-analysis`, `initiating-coverage`, `morning-note` and `thesis-tracker`. For the file type, ask for Excel, Word, PowerPoint, PDF, an HTML report or an interactive dashboard, and the agent loads the matching format skill. The [Explore](/explore/earnings-preview/) pages show full examples.

## Check the Sources panel

Open the Sources pill in a thread to see what the agent read. You can scope it to the current turn or the whole thread. Entries are grouped as web search, web pages, files, memos, memory, SEC filings, market data and financial data tools, and each shows details such as the time, size and arguments. Use it to confirm that a figure came from the filing and not from a search snippet. It records what the agent accessed. It does not prove the agent read it correctly, so open the source for any number you will act on.

## Choose model and effort for hard work

Reasoning effort runs on one scale from none to max, and a model that lacks a level gets the nearest one it offers. Pick it with the Effort control next to the model name in the chat box. It stays on that thread, and your default lives in Settings. The effort menu shows an estimate of cost per task at each level, so you can spend high effort on a valuation or an initiation and keep lookups low. Subagents have their own model and effort under Plugins, then Subagents.

For long research threads, the compaction profile in Settings sets how early older turns fold into a summary.

## Steer mid-run instead of restarting

A message you send while the agent works lands before its next model call, so correct course as soon as you see it drifting: add a constraint, name a source, narrow the scope. Everything the agent already pulled stays in context and in the workspace. Use Stop to halt it.

Avoid editing an earlier message to fix a run. The app warns that messages after the edited one are replaced and cannot be recovered.

## Related

- [Memory, memos or scratchpad?](/compare/memory-memo-or-scratchpad/)
- [Models and keys](/concepts/models-and-keys/)
- [Delegate work well](/best-practices/when-to-delegate/)
- [Write your own skills](/best-practices/writing-skills/)
- [Earnings preview](/explore/earnings-preview/)
