---
title: Models and your own keys
description: How to connect a model to LangAlpha with a Claude or ChatGPT sign-in, a coding plan, an API key or a local server, and tune reasoning and compaction.
---

LangAlpha works with the model you already have. You connect one or more model sources to your account (a subscription sign-in, a coding plan, an API key, or on a self-hosted install a local model server), then choose which models run your conversations and your background work. Your keys are stored encrypted, and you can add, test and remove them at any time in Settings under Model access.

## Ways to connect a model

| Way | What you do | Examples |
|---|---|---|
| Subscription sign-in | Sign in with an account you already pay for | ChatGPT (Plus, Pro, Business, Enterprise), Claude, Grok |
| Coding plan | Add the plan's key | Kimi Code, GLM Coding Plan, MiniMax Coding Plan, OpenCode Go |
| API key | Paste a key from the provider and test it | OpenAI, Anthropic, Gemini, SpaceXSI (Grok), DashScope (Qwen), Moonshot (Kimi), Zhipu AI (GLM), MiniMax, DeepSeek, OpenRouter, Groq, Cerebras |
| Your own endpoint | Add a base URL and key for an OpenAI, Anthropic or Gemini compatible API, then define the model | Any compatible service |
| Local server | Point LangAlpha at a model server you run (self-hosted only) | Ollama, LM Studio, vLLM |

A subscription sign-in uses a short code or a pasted code and counts against your own plan with the provider. The sign-in is stored encrypted and used only for your requests, and you can disconnect it at any time. If it stops working the row shows "Needs sign-in" and a Reconnect button. Some providers have separate China and international accounts, so pick the one your key was issued for.

The first-run setup asks "How would you like to connect?" with three answers: use a subscription, "I have an API key", or "I run models locally". You can add more providers later.

## Where keys live

API keys are encrypted at rest in the database. Settings only ever shows a masked key, and removing a key also removes its models from your lists, defaults and fallbacks.

## Choosing models

Under Settings, Model, you pick:

- **Default model:** runs every conversation, both your Chief of Staff and each workspace's Analyst.
- **Background model:** runs work around a conversation, such as thread titles, reading web pages and summarizing memos. A faster, cheaper model is usually enough.
- **Compaction model:** summarizes long conversations when their context fills. By default it follows the conversation's model.
- **Web fetch model:** extracts content from web pages.
- **Fallback models:** tried in order when your default or background model is unavailable. With none selected, no fallback is used.

If a model call fails, LangAlpha retries it up to 3 times with backoff before moving to the next fallback. A call that cannot succeed (a bad request or an auth error) goes straight to the next fallback. If the conversation is too long for a model, only a fallback with a larger context window is tried, and otherwise the conversation is compacted.

### Tuning per account or per model

Advanced model settings apply to your whole account and can be overridden for a single model in the Per-model settings table:

| Setting | What it does |
|---|---|
| Reasoning effort | How hard a model thinks before answering. The scale runs none, minimal, low, medium, high, xhigh, max, but each model offers only some of those steps |
| Prompt guidance | "Thorough" gives the agent full coaching; "Concise" trims the coaching and keeps the facts. Left alone, each model gets the level suited to it |
| Compaction profile | When a conversation is summarized |
| Speed | A faster mode on models that offer one, at higher usage |
| Provider compaction | Lets Claude and OpenAI models summarize with their provider's own compaction (on by default) |

**Reasoning effort is clamped per model.** If you choose a level a model does not offer, LangAlpha steps down to the nearest level it does offer, and never above what you asked for. A model with no reasoning control ignores the setting.

**Compaction profiles** set how many tokens a conversation can reach before it is summarized:

| Profile | Summarizes at |
|---|---|
| Aggressive | 100k tokens |
| Moderate | 130k |
| Extended | 200k |
| Economy | 250k |
| Relaxed | 300k |
| Spacious | 400k |
| Expansive | 500k |
| Maximum | 750k |

With "Use system default", each model gets a profile suited to its context window (for Claude and GPT models, 500k). A later threshold keeps more context but makes each call larger. Choose a profile your model's window can hold.

:::note[Hosted and self-hosted]
**Hosted:** You can use the models LangAlpha provides, your own subscriptions, or your own API keys. Local model servers are not offered, and a base URL that points into LangAlpha's own network (a private or reserved address) is refused: use a public URL.

**Self-hosted:** The API keys in the server's `.env` (for example `OPENAI_API_KEY` or `ANTHROPIC_API_KEY`) count as your own keys, so a fresh install with a key set is ready to use. Local servers (Ollama, LM Studio, vLLM) are available, and their base URLs can point at the machine running them. The default and background models for the install are set in `agent_config.yaml`.
:::

## Related

- [Self-hosting quickstart](/get-started/self-hosting-quickstart/)
- [Hosted or self-hosted?](/get-started/hosted-or-self-hosted/)
- [Memory, memos and scratchpad](/concepts/memory-memos-scratchpad/)
- [Threads and turns](/concepts/threads-and-turns/)
