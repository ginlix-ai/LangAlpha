---
title: Delegate work well
description: When to hand work to subagents, workflows or a workspace Analyst, what each built-in subagent can do, and the limits on custom subagents and workflows.
sidebar:
  order: 2
---

Delegating means the agent hands part of a job to a helper that works in its own context and reports back. LangAlpha has three levels: subagents inside one turn, workflows that script many subagents, and the Chief of Staff handing work to a workspace's Analyst. Each helper spends model tokens of its own, so delegate large or independent work, not everything.

## Choose the built-in subagent by its tools

Five subagents ship with LangAlpha. They differ in what they can reach, which is the quickest way to pick one.

| Subagent | Can use | Use it for |
| --- | --- | --- |
| `research` | Web search and files | One topic or question at a time, answered with citations. It cannot run code or call market-data tools. |
| `general-purpose` | Code, shell, files, market data, web search | Multi-step operations, data processing and file work. |
| `data-prep` | Code, shell, files, market data | Bulk pulls across tickers, cleaning, and saving structured datasets. No web search. Several can run at once. |
| `equity-analyst` | Code, shell, files, market data, web search | Company analysis, valuation, earnings and sector work with models and charts. Available to Analysts only, not to the Chief of Staff. |
| `report-builder` | Code, shell, files | DOCX, XLSX, PPTX and PDF output. It loads the matching format skill first, so hand it finished analysis. |

## Delegate what is heavy and independent

The agent is instructed to delegate work that takes three or more tool calls and does not need intermediate results to decide the next step, such as bulk data, many fetches or independent comparisons. It does quick lookups itself. Subagents run in the background in their own context and return a concise result, which keeps the main thread free for reasoning. The agent is told to launch up to three independent subagents in one response and is notified as each finishes.

Split comparisons by element ("one subagent per peer"). A subagent knows only what its prompt says, since it does not see your conversation, so ask for the task directory and the output file to be named in the prompt.

You can turn subagents off in Settings, or per thread with the Subagents switch in the chat input. Type `/subagent` to ask for them on a single task.

## Steer a running subagent

A message you send while agents work lands before the next model call. To direct one subagent, click it while it runs and type in its "Add instruction for this agent" box. A finished subagent can be resumed with a follow-up ("now also add a chart") instead of repeating its work.

## Write your own subagents

Open Plugins, then Subagents, or ask in any chat: "Set up a subagent that ...". The limits come from the validation rules:

- Up to 20 subagents of your own. The name is 1 to 48 lowercase letters, digits or hyphens.
- The description is at most 400 characters and is how the agent decides when to hand work over, so say when to pick this subagent. Two subagents with the same description trigger a warning, because the agent would choose between them at random.
- Instructions are at most 16 KB.
- Tools come from four sets: web, market_data, code and shell. A new subagent gets all four unless you narrow them to what the job needs.
- Each can have its own model and reasoning effort, brokerage access, connector access, memory access and preferred skills, and you choose whether Analysts, the Chief of Staff or both can use it.

You cannot edit a built-in subagent's instructions. Duplicate it to write your own, or change its model, effort and tools, switch it off, or reset it.

:::note[Hosted and self-hosted]
**Hosted:** a subagent's model must be one your plan allows, or one whose provider you added your own key for.

**Self-hosted:** any model you have configured can be picked.
:::

## Use workflows for fan-out at scale

When the same job repeats per ticker, filing or finding, ask the agent for a workflow. It writes a short JavaScript script that dispatches subagents with `agent()`, `pipeline()` and `parallel()`, and the run reports back when it finishes. Use `pipeline` when each item has several stages, so a slow item does not hold the others up.

| Limit | Default |
| --- | --- |
| Subagent dispatches per run | 64 |
| Running at once | 8 (extra calls queue) |
| Time per subagent | 30 minutes |
| Concurrent runs per thread | 2 |

Know these properties before you start:

- Every dispatch is a full subagent run with its own tokens, so a 64-way fan-out spends about 64 subagents' worth. Try it on three items first.
- A subagent that fails or times out comes back as null, so ask for a synthesis that survives partial results.
- Each subagent starts blank and cannot see the conversation or its siblings. The script cannot read workspace files; the subagents can.
- Saved workflows persist across threads under `.agents/workflows/`.

A self-hosted install can change these limits, or turn workflows off, in the `workflow:` block of `config.yaml`.

## Hand work from the Chief of Staff to an Analyst

The Chief of Staff answers quick questions and anything the workspaces already hold itself. New hands-on work, such as research, a model or a report, goes to the workspace's Analyst, who keeps the files and history there. The Chief of Staff creates the workspace first if none fits (with your approval, unless you gave it in advance), and tells you when the Analyst finishes. A follow-up to a running hand-off goes to the same thread instead of starting over.

## Related

- [Chief of Staff or Analyst?](/compare/chief-of-staff-or-analyst/)
- [Chief of Staff and Analysts](/concepts/chief-of-staff-and-analysts/)
- [Structure your workspaces](/best-practices/structuring-workspaces/)
- [Get good research output](/best-practices/research-output/)
