---
title: Automations, wakeups and background commands
description: "Four ways LangAlpha works later: automations you set, wakeups the agent sets, background commands that report back, and workflows that fan out subagents."
---

LangAlpha has four ways for work to happen when you are not watching. **Automations** are runs you set up: on a schedule, once, or when a price moves. **Wakeups** are follow-up turns the agent sets for itself inside one thread. **Background commands** are Bash or code runs that outlast the agent's wait and report back when they end. **Workflows** are saved scripts that fan out many subagents at once. For a side-by-side view, see [Automation or wakeup?](/compare/automation-or-wakeup/).

## Automations

An automation is an instruction the agent runs on its own, without you present, until you pause or delete it. You make one on the Automations page (blank or from a template such as a morning briefing or earnings watch), or ask the agent in chat. The agent writes it for you after summarizing the schedule and instruction and getting your yes.

**Triggers.** An automation has one of three kinds of trigger, and the kind cannot change after creation:

| Trigger | What it does |
|---|---|
| Schedule (cron) | Runs on a five-field cron expression in a time zone you pick, such as `0 9 * * 1-5` for weekdays at 9:00. Seconds fields are refused. |
| Once | Runs one time at a future date and time. After it runs it is finished. |
| Price | Watches one US stock or index and runs when a condition holds: price above or below a level, or a percent change up or down from the previous close or the day's open. Several conditions on one automation must all hold. Crypto, currency and futures symbols are refused. |

A price automation is either **one-shot** (fires once, then completes) or **recurring**. A recurring one re-arms after a cooldown you choose, at least 4 hours, or the next trading day if you leave the cooldown empty.

**Where each run goes.** Each automation names a workspace to run in, or Home to run as the Chief of Staff. It also names a thread strategy: a **new thread each run**, or **continue** one ongoing thread (its own, or one you pick). If a continuing automation is due while its thread is mid-turn, the run waits for that turn to end, for up to 30 minutes, and is then skipped. A frequent schedule never queues more than one waiting run.

**Delivery.** Every run is a thread you can open, and results land in the Automations feed. You can also deliver them to a chat app.

:::note[Hosted and self-hosted]
**Hosted:** delivery goes to your connected chat apps (Slack, Discord, Telegram, iMessage, Feishu). A failed or stopped run posts a short notice there.

**Self-hosted:** there are no chat apps. Each run's start, completion and failure is posted as JSON to the URL in `AUTOMATION_WEBHOOK_URL`. See [Chat apps](/concepts/chat-apps/#self-hosted-webhook-and-gateway).
:::

**Failures.** Each automation has a *max failures* setting (1 to 100, default 3). After that many failed runs in a row it switches itself off, and the Automations page shows it under "Needs attention". A run that fails because the model provider rejected your own API key switches the automation off at once. Failures that are not the automation's fault never count: a usage limit, a server problem, a restart, or you pressing Stop. Resume a switched-off automation from the Automations page once the cause is fixed.

## Wakeups

A wakeup is a follow-up turn the agent schedules for itself in the thread it is working in. The agent uses it to carry on a task whose next step it already knows: "check back at 3 PM", "look at NVDA every 15 minutes this morning", "tell me when my order fills". When a wakeup fires while the agent is working, it arrives in that turn. Otherwise it starts a new turn in the same thread, with no message from you.

A wakeup can fire:

- **At a time**, once, from 1 minute to 7 days ahead.
- **On a cron schedule**, at least 5 minutes between fires. A series stops after 24 fires or 7 days, whichever comes first, or sooner at an end time the agent sets. Every 5 minutes therefore ends after 2 hours.
- **On a price move**: a price level, or a percent move from the previous close or from the price when it was set. It lasts through the market's next full session, and shortly before it lapses the agent is woken to renew it or let it go.
- **When an order fills or ends**, for an order placed in the same thread. It lasts until the order ends, at most 7 days.

A thread holds at most **10 pending wakeups**, and a repeating series counts as one. Only the main agent sets wakeups, and each thread keeps its own. A wake turn that finds nothing worth telling you about ends **quietly**: the thread stays marked as read, and the check appears in a collapsed list of quiet checks ("1 check, nothing new"). A turn that has a message from you in it cannot end quietly.

You see pending wakeups in a chip above the message box ("2 wakeups, next 3:00 PM"). Open it to cancel any of them or all of them. A wakeup that has already fired and is on its way to the agent cannot be cancelled. Use Stop to end that turn instead.

:::note[Hosted and self-hosted]
**Hosted:** wakeups are on by default. Turn them off in Settings, Experiments, Wakeups. Turning them off cancels every pending wakeup.

**Self-hosted:** same toggle. An operator can also change the default in the `features` section of `config.yaml`.
:::

## Background commands

Every Bash or code run the main agent makes starts as a detached job on the computer, with its output going to a log file in the thread's folder. The agent waits for a while. If the job ends within the wait, you get an ordinary result. If it outlasts the wait, it is **handed off**: the agent gets a short placeholder and carries on, the job keeps running, and when it ends the thread is told. A live turn takes the report in before its next model call, and otherwise the report starts a turn of its own.

- The wait is 5 minutes by default. Bash can ask for up to 10 minutes, or hand off at once.
- On a computer that the idle sweep would stop, a handed-off command is stopped an hour after the thread's last turn ended. A time wakeup due inside that hour carries it to an hour past that wakeup, so the agent can keep a long job alive by scheduling checks on it. Computers the sweep never stops are not bound by the hour.
- A command you stop, or one stopped by the hour, does not wake the thread. Its notice waits for the thread's next turn.

The agent's **ThreadStatus** tool lists what is running or set to report back in the thread: background commands, subagent tasks, wakeups, automations that run there and, for the Chief of Staff, the Analysts it handed work to. It can also stop a command. 

## Workflows

A workflow is a saved JavaScript script that fans out many subagents at once, for work that repeats one pattern across many items (a research brief per ticker, a filing review per company). The agent launches it with the RunWorkflow tool, which returns immediately and notifies the thread when the run finishes. Each subagent it dispatches is a background task you can open. Saved workflows persist across threads, and the agent runs one by name.

| Limit | Default |
|---|---|
| Subagent dispatches per run | 64 |
| Children running at once | 8 (more queue) |
| Time per child | 30 minutes |
| Whole run | 4.5 hours |
| Concurrent runs per thread | 2 |

A child that fails or times out returns no result. A self-hosted install sets these limits in the `workflow` section of `config.yaml`.

## Related

- [Automation or wakeup?](/compare/automation-or-wakeup/)
- [Threads and turns](/concepts/threads-and-turns/)
- [Chat apps](/concepts/chat-apps/)
- [Price-move automation](/explore/price-move-automation/)
- [Chief of Staff and Analysts](/concepts/chief-of-staff-and-analysts/)
