---
title: Skills
description: Skills are SKILL.md instruction folders the agent loads on demand. Learn the three tiers, slash commands, uploads and the 35 built-in skills.
---

A skill is a folder with a `SKILL.md` file that teaches the agent how to do one kind of work, such as building a DCF model, writing a morning note or producing a Word document. LangAlpha follows the open Agent Skills format: `SKILL.md` starts with a short header giving a `name` and a `description`, followed by instructions, and the folder can carry scripts, templates and reference files next to it. A skill is instructions and files only. It cannot add a new tool; tools come from [MCP servers](/concepts/plugins-mcp-ptc/).

## Three tiers

| Tier | Who owns it | Where it applies |
|---|---|---|
| Built-in | LangAlpha | Every workspace, for every user |
| Yours | You, uploaded to your account (or added by a plugin you install) | Every workspace you have |
| Workspace | You, added to one workspace | That workspace only |

A workspace skill overrides one of yours with the same name, and the Skills tab marks it "Overrides your skill". Built-in names cannot be reused: LangAlpha refuses an upload whose name or command matches a built-in skill, and a few commands (`subagent`, `compact`, `compaction`, `summarize`, `offload`, `truncate`) are reserved for chat controls. Built-in skills can be switched off but not removed.

## How the agent loads a skill

Skills load on demand so they do not crowd the agent's context. Every turn the agent sees a manifest with **one line per enabled skill**: the skill's name and its description. The full instructions are only read in when:

- you type the skill's slash command in chat (type `/` in the message box to see the list), or
- the agent decides the description fits the task and reads that skill's `SKILL.md`.

Because every description is listed on every turn, descriptions are capped at 1,024 characters, and `SKILL.md` itself at 64 KB (the agent reads all of it). Detail that is not always needed belongs in files next to `SKILL.md`.

## Slash commands and aliases

Every skill you add answers to `/<name>`. A skill can declare a `command:` alias in its header, which replaces the name as its slash command; it is read on the first upload, and dropped without a message if another skill already uses it. Commands use lowercase letters, digits and hyphens, up to 64 characters. You can also rename the command of a built-in skill from the Skills tab. Not every built-in skill has a command: the tables below say which do.

## Adding your own skills

On the Plugins page, Add, **Upload a skill** accepts a skill's folder, a `.zip` of one, or a single `SKILL.md` (you can also paste one in). Re-uploading a name replaces your existing skill. Limits:

| Limit | Value |
|---|---|
| Zip size | 2 MB |
| Unzipped size | 8 MB |
| One file | 1 MB |
| Files per skill | 64 |
| `SKILL.md` | 64 KB |
| Skill name | lowercase letters, digits and single hyphens, up to 64 characters |
| Description | up to 1,024 characters |
| Skills per account | 200 (32 MB in total) |

To add a skill to one workspace only, upload it from that workspace's settings, Skills tab; it then runs only there. The agent can also create workspace skills itself: a folder placed at `.agents/skills/<name>/` in the workspace with a valid `SKILL.md` is saved as that workspace's skill shortly after the turn completes. That is how you install a skill from a GitHub repository into one workspace: ask the agent to fetch the repository and copy the skill's folder into the workspace's skills. A workspace skill can later be moved to all your workspaces from the Skills tab.

To install skills from a public git URL for your whole account, use **Install plugin** on the Plugins page. A repository that has no plugin manifest but contains `SKILL.md` folders installs too: each one becomes a skill of a plugin named after the repository. See [Plugins, MCP servers and programmatic tool calling](/concepts/plugins-mcp-ptc/).

## Switching skills off

You can disable a built-in skill for your whole account, or for a single workspace. A workspace cannot re-enable a skill you disabled account-wide. Switching off a plugin switches off the skills it carries. If one skill relies on a shared layer (research-conventions), that layer stays available as long as any skill that needs it stays on.

## Built-in skills

LangAlpha ships 35 skills, grouped here by the built-in plugin that carries them. Two are special: **onboarding** is hidden and runs automatically after you save your investing profile, and **research-conventions** is a shared layer of evidence and judgement rules that the research skills read, with no command of its own.

### Research (langalpha-research)

| Skill | Command | What it does |
|---|---|---|
| 3-statements | `/3-statement-model` | Builds or repairs a linked income statement, balance sheet and cash flow model with scenarios and checks |
| catalyst-calendar | `/catalyst-calendar` | Lists dated events ranked by what they can change: earnings, regulatory decisions, macro releases |
| check-deck | `/check-deck` | Quality-checks an investment deck for number consistency, source coverage and language before it circulates |
| check-model | `/check-model` | Audits a model someone else built and reports issues without editing it |
| company-profile | `/company-profile` | Produces a one-page company profile slide, or a compact profile paragraph |
| competitive-analysis | `/competitive-analysis` | Compares competitors: positioning, scorecards, moat and market share |
| comps-analysis | `/comps-analysis` | Comparable company analysis: peer set, multiples and an implied value |
| dcf-model | `/dcf-model` | Builds a discounted cash flow valuation in Excel with scenarios and sensitivity grids |
| earnings-analysis | `/earnings-analysis` | Post-earnings update: beat or miss, EPS quality, call debate, revisions and thesis impact |
| earnings-preview | `/earnings-preview` | Pre-earnings setup: expectations, what to watch, call questions and scenarios |
| idea-generation | `/idea-generation` | Finds long and short candidates across a universe when no name is chosen yet |
| impact-analysis | `/impact-analysis` | Traces a macro, policy or industry shock to the stocks it affects |
| initiating-coverage | `/initiating-coverage` | First-time coverage of a company: research, model, valuation, charts and a 30 to 50 page report |
| market-watch | `/market-watch` | Tracks live prices for the tickers in your task (needs the Market watch feature turned on) |
| model-update | `/model-update` | Refreshes an existing financial model after earnings, guidance or filings |
| morning-note | `/morning-note` | Daily briefing on overnight news, movers, earnings and macro events |
| portfolio-diagnosis | `/portfolio-diagnosis` | Health check of your portfolio against your profile: concentration, exposure and risk fit |
| research-conventions | none | Shared evidence, judgement and market-data rules that every research deliverable follows |
| sector-overview | `/sector-overview` | Sector or industry landscape report |
| thesis-tracker | `/thesis-tracker` | Keeps a live thesis honest with pillar status, evidence and monitoring triggers |
| trade-pitch | `/trade-pitch` | Turns finished analysis on one security into a position with scenarios, risks and monitoring |

### Deliverables (langalpha-deliverables)

| Skill | Command | What it does |
|---|---|---|
| chart-annotation | `/chart-annotation` | Extra guidance for marking events and levels on charts |
| docx | none | Builds and edits Word documents, including tracked changes and comments |
| html-report | `/html-report` | Writes self-contained styled HTML reports that export to PDF |
| inline-widget | none | Renders charts, dashboards and tables directly in the chat |
| interactive-dashboard | `/dashboard` | Builds interactive dashboards such as stock trackers and sector heatmaps |
| pdf | none | Reads, fills and builds PDFs |
| pptx | none | Builds and edits PowerPoint decks and checks them for layout problems |
| ui-design | none | Design reference for typography, color and layout of visual output |
| xlsx | none | Builds and edits Excel workbooks with live formulas |

### Data (alternative-data)

| Skill | Command | What it does |
|---|---|---|
| web-scraping | `/web-scraping` | Fetches web pages as markdown, HTML or text, including pages that block simple requests |
| x-api | none | Searches X posts, profiles and reply threads for sentiment and event research |

### Platform (langalpha-service)

| Skill | Command | What it does |
|---|---|---|
| langalpha-doc | none | Lets the agent look up how LangAlpha itself works |
| onboarding | hidden | Runs after you save your profile: imports holdings and watchlists, then proposes first tasks |
| run-workflow | none | Runs parallel subagent pipelines from a workflow script (available when workflows are enabled) |

The bundles `langalpha-market-data` and `yfinance` carry market-data MCP servers and no skills.

:::note[Hosted and self-hosted]
**Hosted:** Built-in skills, uploads and the limits above apply as described.

**Self-hosted:** The same, plus an operator can add skills from a drop-in folder; those names become reserved too. Without object storage configured, a stored skill archive is limited to 512 KB. The workflow skill is on only when workflow orchestration is enabled in `config.yaml`.
:::

## Related

- [Skill, plugin or MCP server?](/compare/skill-plugin-or-mcp-server/)
- [Plugins, MCP servers and programmatic tool calling](/concepts/plugins-mcp-ptc/)
- [Writing skills](/best-practices/writing-skills/)
- [Workspaces](/concepts/workspaces/)
