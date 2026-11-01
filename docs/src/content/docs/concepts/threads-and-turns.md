---
title: Threads and turns
description: A thread is one conversation in a LangAlpha workspace and a turn is one run of the agent answering it. How steering, cancelling, approvals and compaction work.
---

A **thread** is one conversation with the LangAlpha agent, kept inside a workspace (or in Home, for the Chief of Staff). A **turn** is one run of the agent answering: it starts when you send a message and ends when the agent has finished its reply. A thread holds many turns, one after another, and each new turn sees the earlier ones.

## One live turn per thread

A thread runs one turn at a time. If you send a message while the agent is still working, LangAlpha does not queue a second turn or refuse the message. It **steers** the running turn instead.

If the previous turn is still shutting down after a stop, or the thread is being compacted, a message is not steered. The server answers "wait a moment, then retry", and the web app queues your message while a compaction finishes.

## Sending a message while the agent works

A message you send mid-turn is taken in before the agent's next model call. The agent reads it as new input from you and changes course without losing the work already done. The web app marks such a message as "Steered": taken in by a run already going.

Steering has a few edges worth knowing:

- If the turn ends before the agent reads your message, the message is not lost. It becomes the input of the next turn.
- Sending the same message twice (a double click, a retry from a flaky connection) steers once.
- Edits, regenerates and retries replace a turn, so they cannot be added to a running one. They are refused while another turn is live.
- An [automation](/concepts/automations-wakeups-commands/) run is a turn of its own and does not steer a running turn.
- The [Chief of Staff](/concepts/chief-of-staff-and-analysts/) steers the same way when it hands more work to an Analyst whose thread is already running.

You can also message a single background subagent that is still running. The message reaches it before its next model call.

## Stopping a turn and leaving a thread

Stop (cancel) ends the turn and interrupts the step the agent is on. The work saved so far stays in the thread, and any steering message the agent had not read yet is handed back to your message box so you can send it again.

Closing the tab, losing your connection or switching devices does **not** stop anything. The turn keeps running on the server. When you open the thread again, the web app reconnects to the live turn and catches up on everything it missed. If the turn has already finished, you see the finished result. That is what lets a long research turn run while you are away.

## Approvals and questions pause a turn

Some steps need you before the agent can continue:

- **Questions.** The agent can ask you a structured question with two to four options (you can also type your own answer, and some questions allow several options). The turn pauses until you answer or skip. A skipped question is passed back to the agent as skipped.
- **Approvals.** Live orders at a connected brokerage wait for your approval unless your trading permission says otherwise. Other actions, such as handing work to an Analyst or creating a workspace, can show an approval card too, and Settings lists which of them the agent may take without one. See [Brokerages and orders](/concepts/brokerages-and-orders/).

Your answer resumes the same thread, and the agent carries on from the point where it stopped.

## Subagents run inside a turn

The agent can delegate work to subagents that run in the background while it keeps going. Five come built in: `research`, `general-purpose`, `data-prep`, `equity-analyst` and `report-builder`. The Chief of Staff does not get `equity-analyst`, because new analysis goes to a workspace's Analyst instead. Each subagent runs as a background task with its own card in the thread, which you can open, message or stop without ending the turn.

A subagent that finishes reports back to the thread. The report reaches the agent before its next model call, the same way your steering messages do, so the agent folds the result into its work. If the agent's turn has already ended when a subagent finishes, the report starts a turn of its own. Saved [workflows](/concepts/automations-wakeups-commands/#workflows) use the same mechanism to run many subagents at once.

## Compaction of long threads

A model can only read so much at once. When a thread's context grows past the limit for the model in use, LangAlpha **compacts** it: the older turns are folded into a summary and the most recent part of the conversation is kept word for word. The agent carries on from the summary plus the recent turns.

Compaction does not delete anything. The full conversation stays saved and you can still scroll back through it. A compaction card in the thread shows when it happened. Settings lets you pick a compaction profile (how early it fires) and a compaction model.

Type `/compact` in the composer to compact by hand. It works only when the current turn has finished.

## Searching past threads

Every thread is saved as a transcript: your messages, the agent's replies, its tool calls and results, and the work of its subagents. The agent can search the transcripts of earlier threads on the same computer, so when you say "like the model we built last week" or "what did we decide about the position size", it looks the conversation up instead of guessing. The Chief of Staff uses the same search to answer questions about work in other workspaces.

A turn is saved to the transcript when it completes. One that ended interrupted or cancelled is saved by the thread's next completed turn at the latest. Transcripts are read-only for the agent.

## Related

- [Chief of Staff and Analysts](/concepts/chief-of-staff-and-analysts/)
- [Workspaces](/concepts/workspaces/)
- [Automations, wakeups and background commands](/concepts/automations-wakeups-commands/)
- [Memory, memos and scratchpad](/concepts/memory-memos-scratchpad/)
