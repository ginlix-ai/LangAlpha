# Prompt templates

Jinja2 sources for the PTC, Flash, and subagent system prompts. Rendered by
`PromptLoader` (`../loader.py`); defaults live in `../config/prompts.yaml`.

Preview any surface as it renders at runtime:

```bash
uv run python scripts/utils/render_prompt.py --mode ptc --crawl --count-tokens
uv run python scripts/utils/render_prompt.py --subagent equity-analyst
```

## Two guidance levels: lean and detailed

The service is model-agnostic — the same prompt has to serve frontier models
and small self-hosted ones. Frontier models regress when given procedural
scaffolding they already fold in; a 20b model falls apart without it. So every
template renders at one of two levels, resolved per request (see
`../guidance.py`):

```
user model_preference.prompt_guidance → config.yaml prompt.guidance → models.json prompt_guidance → detailed
```

`detailed` is the fail-safe default: an unannotated or unknown model gets more
scaffolding, never less.

### You never write the lean version

There is one template body. **Lean is what remains when the coaching is
stripped** — it is not a second text to keep in sync. The split is:

| | Goes where | Examples |
|---|---|---|
| **Facts** the model cannot infer | Unconditional | Workspace paths, the `([domain](url))` citation format, that MCP tools exist only as Python imports from `tools.<server_name>`, memory tiers and the 200-line truncation |
| **Coaching** the model may already know | Inside a guidance fence | Worked examples, numbered procedures, rules of thumb, anti-patterns that only restate the rule above them |

A frontier model needs the facts exactly as much as a small one does. Only the
coaching varies. Anti-patterns split on the same test: the `NEVER use these
path patterns` list in `workspace_paths.md.j2` names paths this sandbox does
not have, so it stays unconditional; a "don't do X" that only inverts the
rule above it is coaching.

There is a prior question, though: **a fact about one tool belongs in that
tool's own description, not here.** The model reads both, and the tool
description travels with the tool — a subagent that never binds the tool never
pays for it. A parameter table, an argument's format, a per-tool constraint:
all of that is the docstring's job. What stays in a template is what no single
tool can know, which is how tools compose: sequencing, routing between them,
what to do with a result once you have it.

### The fence rule

**A fence wraps whole blocks — a `##` section, a worked example, a bullet
list. It never splits a sentence or a line.**

```jinja
# Data Processing

**Core principle**: Always dump data first, then process.

Write raw tool output to your task directory before working with it.
{% if guidance | default("detailed") == "detailed" %}

## Dump First, Then Process

1. **Dump raw result** → ...

## Storage and Summary Pattern

```python
result = some_mcp_tool(query="...")
```
{% endif %}
```

**Write the fence exactly that way.** Two details carry weight:

*Compare the level by name, don't branch on a boolean.* `formatter.py` in this
same package calls its MCP tool-exposure mode `"detailed"` too, and a bare
`{% if detailed %}` does not say which of the two axes it gates. Naming the
variable at the fence does.

*Keep the `| default("detailed")`.* House style here is an explicit default at
every use site so a missing context key cannot silently decide. The other flags
default *false* because absent means feature-off; guidance defaults **detailed**
for the same reason that is the fail-safe level — a render path that forgot to
inject the key must get more scaffolding, never less. Without the filter, a
missing key resolves the comparison to false and silently goes lean.

The cost of comparing by name is that a typo in the *value* is a no-op: the
block vanishes from both tiers, and the subset invariant still passes because
lean adds nothing. `TestFenceExpressions` in `test_prompt_guidance.py` scans
every `.j2` for these two properties precisely because nothing else catches it.

Multiple fences per file are fine when the coaching genuinely belongs inline —
an example should stay next to the rule it illustrates, because the detailed
prompt is the product for the models that depend on it. What is not fine is
sentence-level gating: it makes the file unreadable and the rendered output
unpredictable.

When a component is coaching end to end, gate it at the include site in
`system.md.j2` instead of fencing its whole body.

### When the steering inverts

Rarely, a tier needs the opposite instruction rather than less of the same —
frontier models need delegation *damped* where small models need it
encouraged. That is the only case for `{% else %}`. It breaks the subset
guarantee for that block, so it also requires registering the lean-only text
in `EITHER_OR_ALLOWLIST` in
`tests/unit/ptc_agent/agent/prompts/test_lean_subset_invariant.py`, with a note
on why it inverts. The speed bump is deliberate.

### Checking your work

```bash
# What does lean drop?
uv run python scripts/utils/render_prompt.py --mode ptc --diff

# Resolve the level from a real model, as runtime would
uv run python scripts/utils/render_prompt.py --mode ptc --model claude-opus-5
```

`test_lean_subset_invariant.py` enforces the subset property across PTC, Flash,
and all five subagents. If it fails, lean is rendering text detailed never
sees — move it outside the fence, or register it as an either/or.

## Other conventions

- **`agent_preference` is echoed to the model verbatim.**
  `components/user_profile.md.j2` prints its keys as user instructions, so a
  renderer-consumed knob must not be stored there — it would become prompt
  text about itself. `output_format` is the one exception, and it shows the
  cost: the loop carries an explicit `if key != "output_format"` so that key
  can select a section instead of printing. Model-scoped settings (the guidance level
  among them) live in the sibling `model_preference` bucket, which the template
  never iterates.
- **The static prompt excludes time and user profile.** Both are appended by
  `RuntimeContextMiddleware` after the cache breakpoint. Adding either to a
  system template silently breaks prompt caching.
