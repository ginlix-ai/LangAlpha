---
title: Hosted or self-hosted
description: Compare the hosted LangAlpha platform with a self-hosted install, covering sign-in, market data, price triggers, chat apps, models and operations.
---

LangAlpha runs in two ways from the same open-source code. The hosted platform at [langalpha.ai](https://langalpha.ai) gives you an account with market data, cloud computers, brokerage connections and chat apps set up for you. A self-hosted install runs the backend, web app, Postgres and Redis on your own machine with Docker, under your control. Agents, workspaces, automations, wakeups, skills, plugins, MCP servers, memory, memos, sharing and governed orders behave the same in both.

## At a glance

| | Hosted | Self-hosted |
| --- | --- | --- |
| Sign-in | An account per person, with separate data per user. | No sign-in. One local user, so anyone who can reach the server acts as you, connected brokerages included. |
| Market data | Live quotes for US equities and options, with extended hours. | Yahoo Finance, and FMP when you add a key. Quotes refresh every 60 seconds and show as delayed. |
| Options and market structure | Options chains and snapshots, short interest, float and top movers. | Not available. |
| Price triggers | Fire on live ticks. | Poll about every 30 seconds against the delayed quotes. |
| Chat apps | Slack, Discord, Telegram, Feishu and iMessage. | Not included. Automation results can go to a webhook you run. |
| Models | Models included with the platform, your own API keys, or Claude, ChatGPT and Grok sign-in. | Your own keys, coding plans, subscription sign-in, and local servers such as Ollama, LM Studio and vLLM. Keys in the server's `.env` count as yours. |
| Computers | The computer types the platform offers. Your plan can limit computer size, count and always-on. | The providers you configure: Docker, Daytona, E2B, and machines you add over SSH. No limits on size or count. |
| Availability | Runs around the clock, so automations and price triggers fire while your laptop sleeps. | Runs only while your machine and its Docker stack are up. |
| Operations | Upgrades, migrations, backups and sandbox capacity are handled for you. | You run upgrades, database migrations and backups. Docker sandboxes share your machine's CPU, memory and disk. |
| Desktop app | The LangAlpha edition opens hosted sign-in. | The LangAlpha OSS edition asks for your server's address on first run. |

## When to choose hosted

Choose hosted when you want live US market data and options, chat apps, and automations that keep running while your own computer is off, without operating a server.

## When to choose self-hosted

Choose self-hosting when you want everything on your own hardware or network, when you want to run local models, or when you want to read, change or extend the code that every order passes through. You need Docker. See the [self-hosting quickstart](/get-started/self-hosting-quickstart/).

:::caution
A self-hosted server has no sign-in. The web app and API listen on every network interface, so your local network can reach them. Keep the server on your own machine or a private network, and put a VPN or an authenticating proxy in front of it before exposing it anywhere else.
:::

## How differences are marked in these docs

Pages describe the behavior both setups share. Where they differ, a note titled "Hosted and self-hosted" says what each one does:

:::note[Hosted and self-hosted]
**Hosted:** what you see on langalpha.ai.

**Self-hosted:** what an open-source install does, and the setting that controls it.
:::

## Related

- [What is LangAlpha](/get-started/what-is-langalpha/)
- [Self-hosting quickstart](/get-started/self-hosting-quickstart/)
- [Which computer should I use?](/compare/which-computer/)
- [Chat apps](/concepts/chat-apps/)
- [Models and your own keys](/concepts/models-and-keys/)
