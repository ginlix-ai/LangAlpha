---
title: Chat apps
description: On the hosted platform, talk to LangAlpha from Slack, Discord, Telegram, Feishu and iMessage. How messages route, the commands, and how self-hosting differs.
---

Chat apps let you talk to the LangAlpha agent from Slack, Discord, Telegram, Feishu or iMessage instead of the web app. On the hosted platform you connect an app to your account, message the agent there, receive files it produces, and have automations send their results to a chat. Each conversation in a chat app is an ordinary [thread](/concepts/threads-and-turns/), and its messages show in the web app marked "Via" and the app's name. A self-hosted install includes no chat apps; automation results go to a webhook instead (see [below](#self-hosted-webhook-and-gateway)).

:::note[Hosted and self-hosted]
**Hosted:** Settings, Integrations, Chat apps lists each app with Set up or Manage. It opens your account console, where you connect the app. Inside a chat, `/connect` also starts linking your account. iMessage appears in the list once it is linked.

**Self-hosted:** the Integrations section does not exist and no chat app is included. You can send automation results to a webhook, or run your own channel gateway.
:::

## Where your agent works: All workspaces or one workspace

Every conversation in a chat app runs in one place and keeps it as long as it lasts:

- **All workspaces** is the [Chief of Staff](/concepts/chief-of-staff-and-analysts/), answering from Home. It answers quick questions, reads your workspaces, and hands deep work to an Analyst.
- **One workspace** is that workspace's [Analyst](/concepts/chief-of-staff-and-analysts/).

You choose the place with `/workspace`. In a group, each person has their own choice. A chat you have not set uses your account's default place, and with no default it runs in All workspaces. Changing a chat's place never moves a conversation already started; it applies to the next one.

## Which conversation a message reaches

- A message that is not a reply goes to the chat's current conversation, or starts a new one (the table shows when).
- A reply goes to the conversation of the message it answers, not the chat's current one. In Slack, Discord and Feishu a reply is a message in a thread or topic. In Telegram it is a message in a topic or, in a chat without topics, a reply to one message. In iMessage it is an inline reply.
- A message that starts with `/new` starts a new conversation and runs the rest of the message there, on every app.
- While an answer is running, a message you send in that conversation joins the answer. On Slack, Discord, Telegram and Feishu it steers the running turn (see [Threads and turns](/concepts/threads-and-turns/#sending-a-message-while-the-agent-works)). A `/new` with text, or the first message after a bare `/new`, waits and runs as a new conversation when the answer ends.
- When the Chief of Staff hands a task to an Analyst, the result comes back in the thread, topic or chat the request came from.
- Someone who has not connected their account gets a prompt to connect it, not an answer.

One chat can show answers from more than one conversation, because each person in a group has their own and the agent's own sends and automation results sit in the chat too. In iMessage and in Telegram chats without topics they sit among the chat's messages, so reply to the message you mean.

| | Slack | Discord | Telegram | Feishu | iMessage |
|---|---|---|---|---|---|
| Answers | DM: every message. Channel: mention the app to start; no mention needed inside its thread | Server: mention the app; the answer opens a thread. DM: one conversation until `/new` | Private chat: every message. Group: mention, reply, or use a command | DM: every message. Group: mention the app | Every message |
| New conversation | A message at the top of a DM or channel | Each mention in a channel; in a DM after `/new` | After 4 hours without topics; a new topic with topics | A message at the top starts a topic | Only after `/new` or a change of place |
| Stop a running answer | No command | No command | `/stop` | No command | `stop` turns off unprompted updates; it does not stop an answer |

In a Slack or Discord thread, only the person who started it can continue it; anyone else gets a lock and no answer.

## Commands

| Command | What it does |
|---|---|
| `/new` | Starts a new conversation. With text, it runs the text there at once. Alone, your next message within 24 hours starts one. |
| `/workspace` | Chooses where the agent works: All workspaces or a named workspace. On iMessage it shows a numbered list; `back` returns to your previous place. |
| `/model` | Chooses the model (Discord, Telegram and Feishu). |
| `/connect` | Connects your LangAlpha account to the app, where the app offers it (Discord, Telegram private chats and Feishu). |
| `/help` | Lists the commands. |
| `/stop` | Telegram only: stops your running answer. |

## The agent messaging you

The agent can send you a message, with up to 10 files attached and up to 20,000 characters of text, when you ask for something to be sent to you or to a chat. It reaches the conversation it is in, your direct messages, and the shared chats you allowed. The Chief of Staff can attach files from Home or from any workspace. In the web app each send shows as a delivery card with its status: sent, partly sent or failed. On iMessage, `stop` turns off everything the agent would send unasked, including automation and hand-off results, until you send `start`, and unprompted sends are limited to 20 a day.

## Automation delivery

An [automation](/concepts/automations-wakeups-commands/) can deliver to a chat app. Set it under More options when you create or edit the automation, or ask the agent. You can name an app, which lands in the workspace's default chat, then the app's preferred chat, then your direct messages, or name one chat. Each run sends its result itself. A chat the agent missed gets the final answer, and a failed or stopped run posts a short notice. A reply in that thread continues the run's conversation.

## Self-hosted: webhook and gateway

A self-hosted install has no chat apps, but two settings in `.env` cover the same ground.

**Automation webhook.** Set `AUTOMATION_WEBHOOK_URL`, and each automation run that picks a delivery method POSTs JSON there, one request per method, for three events: `automation.started`, `automation.completed` and `automation.failed`. The body holds the event, a timestamp, the automation's id and name, the execution, thread, run and workspace ids, your user id, a title and, on failure, an error and a failure reason, plus `config.channel` naming the method. Without a gateway, a delivery method is an app name such as `slack`; naming one chat needs a gateway. If you also set `AUTOMATION_WEBHOOK_SECRET`, each request carries an `X-Webhook-Signature` header with `sha256=` followed by the HMAC-SHA256 of the raw request body under that secret. Check it before trusting a request.

**Your own gateway.** Set `CHANNEL_GATEWAY_URL` to the base URL of a channel gateway (with `INTERNAL_SERVICE_TOKEN` set) and the agent gets two tools: one to message you and one to list where it can message. Automations then deliver through the gateway, and the webhook is the fallback for a run the gateway does not take. This repository does not ship a gateway: you point the setting at one you run. Unset, the agent has neither tool.

## Related

- [Automations, wakeups and background commands](/concepts/automations-wakeups-commands/)
- [Chief of Staff and Analysts](/concepts/chief-of-staff-and-analysts/)
- [Deliver to Slack](/explore/deliver-to-slack/)
- [Hosted or self-hosted?](/get-started/hosted-or-self-hosted/)
