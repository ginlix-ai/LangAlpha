---
title: Automation or wakeup?
description: "Compare automations, wakeups, background commands and workflows in LangAlpha: who creates each, where it runs, how it reports back, and its limits."
sidebar:
  order: 2
---

Use an **automation** for recurring or standing work you want to own: it runs on a schedule, once, or on a price move, whether or not you are around. Use a **wakeup** when the agent is mid-task and needs to come back to the same thread later; the agent sets it, not you. A **background command** is a long Bash or code run that reports back when it ends. A **workflow** is a saved script that fans out many subagents at once. Concepts are explained in [Automations, wakeups and background commands](/concepts/automations-wakeups-commands/); this page helps you choose.

## Side by side

| | Automation | Wakeup | Background command | Workflow |
|---|---|---|---|---|
| Who creates it | You, or the agent at your request | The agent, on its own | The agent, by running a command | The agent, from a saved or new script |
| Who owns it | You (your account) | The thread | The thread | The thread |
| Where it runs | A workspace or Home, in a new thread each run or one ongoing thread | The same thread it was set in | On the computer, reporting to the thread | As background subagents of the thread |
| Triggers | Cron, once, price move | At a time, cron, price move, order fills or ends | Starts now, ends when the command ends | Starts now |
| Reports back | A thread per run, the Automations feed, optional chat app or webhook delivery | A new turn in the thread, or arrives in the running turn; quiet if nothing new | The thread, as a turn or into the running turn | The thread, when the run finishes |
| Survives you closing the tab | Yes | Yes | Yes (within the hour rule below) | Yes |
| You can stop it | Pause, delete, Skip, Stop a run | Cancel in the chip above the message box | The agent stops it, or you stop its turn | Stop the task or the turn |

## Limits that decide it

| | Limit |
|---|---|
| Automation failures | Switches off after 3 failed runs in a row by default (1 to 100); a rejected provider key switches it off at once |
| Automation price cooldown | At least 4 hours when recurring |
| Wakeup lead time | 1 minute to 7 days ahead |
| Wakeup cron | At least 5 minutes apart, at most 24 fires or 7 days per series |
| Pending wakeups | 10 per thread (a series counts as one) |
| Command wait | 5 minutes by default, up to 10 for Bash; then it is handed off |
| Command after the turn | Stopped an hour after the thread's last turn on a computer the idle sweep would stop |
| Workflow | 64 dispatches, 8 at once, 30 minutes per child, 2 runs per thread |

## Choosing

**You want something to happen every day, or every Friday.** Automation. A wakeup lasts at most 7 days and a series at most 24 fires, and it belongs to one thread.

**You want an alert when a price crosses a level and you want it to outlast this conversation.** Automation. A price wakeup lasts through the market's next full session, then lapses unless the agent sets it again. See [Price-move automation](/explore/price-move-automation/).

**The agent is partway through something and needs to check back.** Wakeup. "Check the order at 3:30", "look again in 20 minutes", "tell me when it fills". It continues the same thread with all the context, and you do not have to create anything.

**A model run, backtest or download takes more than a few minutes.** Background command. The agent keeps working or ends its turn and is told when the job ends. If the job needs longer than an hour past the turn, the agent can schedule a wakeup to check on it.

**You have forty tickers to brief the same way.** Workflow. One brief per ticker as parallel subagents, then a synthesis.

## Examples

- "Every weekday at 8:00 send me a market briefing on Slack." An automation, cron, delivered to Slack.
- "Alert me if TSLA moves 5% from the previous close." An automation with a price trigger, recurring with a cooldown if you want repeats.
- "Run the backtest and tell me when it is done." A background command.
- "Watch NVDA this morning and tell me if it dips below 190." A wakeup, because the agent is in the middle of a conversation about NVDA and needs to return to it.
- "Review the latest 10-Q for these 30 companies." A workflow.

## Related

- [Automations, wakeups and background commands](/concepts/automations-wakeups-commands/)
- [Price-move automation](/explore/price-move-automation/)
- [Threads and turns](/concepts/threads-and-turns/)
- [Chat apps](/concepts/chat-apps/)
