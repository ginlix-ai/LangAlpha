---
name: self-improve
description: Report issues and propose fixes to improve your own capabilities when you encounter errors or limitations
---

# Self-Improvement

You have access to `gh` CLI and `git` in the sandbox. Use this skill when
you encounter bugs, errors, or limitations in your own capabilities during
your work.

## When to Activate This Skill

Recognize these situations during your normal work:
- A tool call fails with an unexpected error
- A skill's instructions (SKILL.md) have wrong examples or outdated API usage
- An MCP tool returns malformed or unexpected data
- A prompt instruction is unclear, contradictory, or missing
- A sandbox script or package is broken or misconfigured
- You notice a code pattern that would make your work more effective if fixed

**Always ask the user for consent before filing an issue or creating a PR.**
Briefly explain what you found and what you want to report, then ask for
approval. Do NOT file anything without explicit user confirmation.

**Do NOT derail the user's current task.** Keep the consent request brief,
file the issue quickly after approval, then continue with the user's request.

## Authentication
- `GITHUB_TOKEN` env var is pre-configured (injected from `GITHUB_BOT_TOKEN`)
- Git identity is set via env vars — no `git config` needed

## Workflow 1: Report a Bug (default — quick, no cloning)

Use when: you encounter any issue with your own capabilities.
Time: <30 seconds. Don't interrupt the user's flow.

Template:
```bash
gh issue create --repo "ginlix-ai/LangAlpha" \
  --title "bug(agent): <what broke>" \
  --label "agent-reported" \
  --body "<structured body>"
```

Issue body structure:
```
## What I was doing
<user's task context — what were you trying to accomplish>

## What went wrong
<exact error message or unexpected behavior>

## Where the issue likely is
<file paths, function names, skill names — be specific>

## Suggested fix
<if obvious, describe; otherwise "Needs investigation">

## Environment
- Thread: <thread_id if available>
- Tool/Skill: <which tool or skill was involved>
- Error type: <tool_error | skill_instruction | mcp_data | prompt | sandbox>

@claude Please triage this issue — verify the root cause, assess severity, and suggest a fix if straightforward.
```

## Workflow 2: Propose a Fix (rare — only when user explicitly asks)

**Default to Workflow 1 (filing an issue).** Only create a PR when the user
explicitly asks you to fix it yourself. Do NOT propose PRs on your own initiative.

Steps:
1. Clone or update: if `.self-improve/langalpha` exists, `cd .self-improve/langalpha && git checkout main && git pull origin main` to get latest. Otherwise `gh repo clone "ginlix-ai/LangAlpha" .self-improve/langalpha -- --depth 1`
2. Branch: `cd .self-improve/langalpha && git checkout main && git checkout -b bot/fix/<short-desc>`
3. Make the fix (keep it minimal and focused)
4. Test: `ruff check . && pytest` (or relevant subset)
5. Commit: conventional format — `fix(scope): description`
6. PR:
```bash
gh pr create --repo "ginlix-ai/LangAlpha" \
  --base main \
  --title "fix(agent): <what's fixed>" \
  --label "agent-reported" \
  --body "<structured body>"
```

PR body structure:
```
## Problem
<link to issue if filed, or describe the bug>

## Root Cause
<what was wrong and why>

## Fix
<what was changed and why this approach>

## Testing
<what tests were run, what was verified>

## Context
- Discovered during: <brief user task description>
- Thread: <thread_id>
```

## Codebase Guide — Where to Look

Use this to identify the right module when filing issues or proposing fixes.

| Directory | What lives here | Example issues |
|-----------|----------------|----------------|
| `skills/` | Skill SKILL.md instructions and assets | Wrong examples in `skills/dcf-model/SKILL.md`, bug in a provided script snippet, outdated API usage, missing steps in a workflow, new best practice to add |
| `mcp_servers/` | MCP server implementations (yfinance, fundamentals, macro, price_data) | `yfinance_mcp_server.py` returns malformed data, a fundamentals endpoint is missing a field, macro data has wrong units |
| `src/tools/` | External tool implementations (web fetch, crawl, search, SEC, market data) | `fetch.py` times out on certain URLs, SEC filing parser fails on 10-K amendments, search returns stale results |
| `src/ptc_agent/agent/tools/` | Core sandbox tools (ExecuteCode, Bash, file ops, grep, glob, think, todo) | `code_execution.py` mishandles large stdout, `bash.py` doesn't escape special chars, `file_ops.py` fails on binary files |
| `src/ptc_agent/agent/middleware/` | Middleware stack (skills, subagents, plan mode, compaction, memory, caching) | Skill loading fails silently, subagent doesn't inherit context, compaction truncates important content |
| `src/ptc_agent/agent/prompts/` | System prompt templates (Jinja2) and config | Redundant or wrongful instructions in `system.md.j2`, useful tips and experience worth persisting into prompts |

## Label Convention
- Always use `agent-reported` label
- Add `bug` for broken behavior, `enhancement` for capability gaps
- Add scope labels: `skills`, `tools`, `mcp`, `prompt`, `sandbox`

## Safety Rules
- NEVER push directly to `main` — always `bot/fix/` or `bot/feat/` branches
- `main` branch contains the latest code. Always branch from `main`, target PRs to `main`
- ALWAYS run linting and tests before creating a PR
- Keep PRs small — one fix per PR, max 1-3 files
- Clone to `.self-improve/langalpha` (inside workspace, persists across restarts)
- NEVER commit tokens, secrets, API keys, or user data
- NEVER include confidential or private information in issues or PRs — no user data, no internal business context, no API responses containing private data, no conversation content. Describe the technical problem only.
- After filing/PR, immediately return to the user's original task

## Pre-Submit Checklist

Go through EVERY item before running `gh issue create` or `gh pr create`:

- [ ] **User consent obtained** — user explicitly approved filing this issue/PR
- [ ] **No secrets or tokens** — title, body, and diff contain zero credentials, API keys, or env values
- [ ] **No private data** — no user names, portfolio holdings, conversation content, or internal business context
- [ ] **No raw API responses** — sanitize or omit any data returned from MCP tools or external APIs
- [ ] **Technical description only** — the issue/PR describes the bug or fix, not what the user was working on
- [ ] **Correct repo** — targeting `ginlix-ai/LangAlpha`
- [ ] **Correct branch** (PRs only) — branched from `main`, PR base is `main`
- [ ] **Minimal diff** (PRs only) — only the files needed for the fix, no unrelated changes
