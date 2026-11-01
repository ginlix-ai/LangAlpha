---
title: Self-hosting quickstart
description: Run LangAlpha on your own machine with Docker. Clone the repository, answer the make config wizard, start the stack with make up and open the web app.
---

You can run the whole LangAlpha stack (Postgres, Redis, the backend and the web app) on your own machine with Docker in two commands: `make config` writes your settings, and `make up` starts everything. With no data keys at all you still get Yahoo Finance prices, fundamentals, analyst data and screening, SEC EDGAR filings, and sandboxes in local Docker.

## Requirements

- Docker with Docker Compose, running.
- `git` and `make`.
- Free ports 8000 (backend), 5173 (web app), 5432 (Postgres) and 6379 (Redis). The wizard checks these for you.
- A way to reach a model: a Claude, ChatGPT or Grok subscription, a coding plan, an API key, or a local model server.

## 1. Get the code

```bash
git clone https://github.com/ginlix-ai/langalpha.git
cd langalpha
```

## 2. Run the setup wizard

```bash
make config
```

The wizard asks seven questions and writes your answers to `.env` (keys and service settings) and `agent_config.yaml` (models, MCP servers, search provider). Press Enter to take the default on any step.

| Step | Choices | Default |
| --- | --- | --- |
| LLM provider | Connect a subscription or coding plan in the web app, enter an API key and pick main and background models, or skip and choose in the web app | Subscription or coding plan |
| Financial data | Yahoo Finance (no key), FMP (`FMP_API_KEY`, free tier available), or both | Yahoo Finance |
| Sandbox | Docker (local containers), Daytona (`DAYTONA_API_KEY`), or E2B (`E2B_API_KEY`) | Docker |
| Web search | None, or one of the supported search providers with its key | None |
| Web fetch | The built-in crawler, or a provider that upgrades extraction on JavaScript-heavy pages | Built-in |
| Cloud storage | None, AWS S3, Cloudflare R2 or Alibaba Cloud OSS | None |
| Infrastructure | Postgres and Redis in Docker Compose, or your own instances | Docker Compose |

It then checks that Docker is running and the ports are free, and generates the key that encrypts the API keys and sign-in tokens you save in the app.

## 3. Start the stack

```bash
make up
```

The first run builds the backend image and installs the web app, which takes a few minutes. The backend applies database migrations each time it starts. When the logs settle, open [http://localhost:5173](http://localhost:5173).

The sandbox step of the wizard writes your choice to `SANDBOX_PROVIDER` in `.env`. To change it later, edit that line, or pass it for one run, for example `make up PROVIDER=docker`, which also builds the Docker sandbox image before starting.

## 4. Finish in the web app

If you skipped the model step or chose a subscription, the web app opens a setup wizard: choose how to connect (a subscription, an API key, or a model server you run locally), connect a provider, and pick your default models. You can leave it with **Exit setup** and come back from Settings later.

Then create a workspace and ask a question. The first workspace on a Docker computer builds the sandbox image, so the first answer takes longer than the rest.

The API listens on port 8000, with interactive API docs at [http://localhost:8000/docs](http://localhost:8000/docs).

## Optional keys and what they add

Add these to `.env` and restart with `make up`.

| Key or setting | Adds |
| --- | --- |
| `FMP_API_KEY` | Fundamentals, financial statements, macro and analyst data. |
| `DAYTONA_API_KEY` or `E2B_API_KEY` | Cloud sandboxes instead of local Docker. `make daytona-snapshots` and `make e2b-templates` prebuild them for every resource tier. |
| `sandbox.offered_providers` in `agent_config.yaml` including `ssh` | Computers on your own Linux machines and Macs over SSH. See [Sandboxes and SSH computers](/concepts/sandboxes-and-ssh/). |
| `TAVILY_API_KEY`, `SERPER_API_KEY`, `EXA_API_KEY`, `PARALLEL_API_KEY` or `BOCHA_API_KEY` | Web search. `search_api` in `agent_config.yaml` picks the default engine, and each user can change it in Settings. |
| `FIRECRAWL_API_KEY` | Upgraded web fetch and site crawling. |
| `R2_*`, `S3_*` or `OSS_*`, with `storage.provider` in `agent_config.yaml` | Object storage for workspace file backups, memos, attachments and downloads. Without it these stay in Postgres. |
| `AUTOMATION_WEBHOOK_URL` and `AUTOMATION_WEBHOOK_SECRET` | Where automation results are posted when an automation picks a delivery method. See [Chat apps](/concepts/chat-apps/). |

## Stop, restart and upgrade

- `make down` stops the stack. Your data stays in Docker volumes.
- `make up` starts it again.
- To upgrade, pull the latest code and run `make up`. The images rebuild and the backend migrates the database on start.
- `make clean` stops the stack and removes leftover Docker sandbox containers.
- `make help` lists every target.

To run the backend and web app directly on your host for development, see [CONTRIBUTING.md](https://github.com/ginlix-ai/langalpha/blob/main/CONTRIBUTING.md).

:::caution
A self-hosted server has no sign-in, and the backend and web app ports listen on every network interface. Anyone who can reach them acts as you, connected brokerages included. Keep the machine on a private network, or put a VPN or an authenticating proxy in front of it.
:::

## Related

- [Hosted or self-hosted](/get-started/hosted-or-self-hosted/)
- [Which computer should I use?](/compare/which-computer/)
- [Models and your own keys](/concepts/models-and-keys/)
- [How it fits together](/concepts/overview/)
