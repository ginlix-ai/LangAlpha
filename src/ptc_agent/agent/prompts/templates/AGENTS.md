# Prompt templates

Jinja2 sources for the PTC, Flash, and subagent system prompts. Rendered by
`PromptLoader` (`../loader.py`); defaults live in `../config/prompts.yaml`.
The PTC prompt renders per role: the analyst's is the base prompt, and the
Chief of Staff's adds `components/chief_of_staff.md.j2` as a `<role>` section,
so a change there never touches a workspace's prompt or its cached prefix.
`workspace_context`, `workspace_paths`, `task_workflow` and `memory` also read
`role`: Home's `agent.md` is the Chief of Staff's notebook of what the user is
focused on and where that work lives, laid out in `<role>`, not a workspace's
index of its own work, and Home's workspace memory is how the user wants it to
work. A shared section that describes the analyst's
`agent.md` argues with `<role>`, and the model then keeps both layouts. Subagents
render these sections without `role`, so they read the analyst's wording.

Preview any surface as it renders at runtime:

```bash
uv run python scripts/utils/render_prompt.py --mode ptc --crawl --count-tokens
uv run python scripts/utils/render_prompt.py --subagent equity-analyst
uv run python scripts/utils/render_prompt.py --mode ptc --role chief_of_staff
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
uv run python scripts/utils/render_prompt.py --mode ptc --model claude-opus-5-5
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
- **`trading_permission` is not a preference.** The agent can write
  `agent_preference` (its profile files, its preference tool), so a level that
  lets orders skip approval cannot live there. It has its own table and
  endpoint, and the turn runner adds it to the profile when a server the
  workspace is configured with can place a live order or stage an instruction
  as a direct tool (`src/server/services/trading_rule.py`). The rule follows
  the user's settings, not this turn's binding: a lapsed grant, a connection
  waiting on re-auth or order tools the turn budget dropped leave it as it
  was, since a profile that moved with them would reach the model as a
  `profile_changed` row saying its rule changed when the user changed nothing.
  It is read off the same binding plans that stamp each order tool's
  approval, never a second read, so it cannot promise an approval step the
  turn did not wire.
  Subagents are bound no order tools, so their copy of the profile drops it.
  Beside it, `trading_asks_on` names the brokerages (by server) whose switch
  still asks at a level that skips approval, so the rule does not tell the
  model those orders go straight out.
- **The static prompt excludes every per-request value.** The clock, the user's
  identity, the surface this turn is running on, and anything that changes
  between turns are rendered from `templates/envelope/` and appended after the
  cache breakpoint. Putting one of them in a system template silently breaks
  prompt caching. What stays static is only the rules that hold on every turn:
  `components/time_rules.md.j2` frames what runtime context is, who it is from
  and how to resolve a time frame against the stamp. The rules that depend on
  the turn ride with it: `envelope/surface_rules.md.j2` states what the surface
  in hand accepts, inside the turn row, and only when those rules are not the
  last ones the model can still see. It holds a line only for the surfaces
  langalpha renders itself (`web`, `market_view`); any other surface sends its
  own text on the request (`ChatRequest.surface_rules`) and the template renders
  that verbatim in place of a built-in line.

## `templates/envelope/`

Fragments of the runtime context, rendered by `middleware/runtime_context/`
rather than included from a system template. Three lifetimes share the
directory:

- **Tail fragments** (`header`, `updates`) re-render per model call, after the
  last breakpoint, so their cost is a few hundred tokens rather than the whole
  history. They emit bare markdown lines: the envelope supplies the
  `<system-reminder>` wrapper, so a fragment that adds one nests it.
- **Baseline fragments** (`baseline_files`, `baseline_agentmd`,
  `baseline_memory`, `baseline_identity`, and the harness blocks
  `baseline_mcp_servers`, `baseline_skills` and the Chief of Staff's
  `baseline_activity`) land *inside* the cached per-thread
  prefix, so they must be byte-identical for equal inputs: no timestamps, no
  dict iteration, and no guidance fence. A guidance flip must not move them,
  because the model can change mid-thread.
- **Row fragments** (`turn`, `surface_rules`, `update_row`) are rendered once,
  at the turn boundary, and persisted as messages in history. `turn` is the
  stamp the turn opened with, `surface_rules` is the paragraph it carries about
  what this turn's reply has to be shaped like (langalpha's own line for `web`
  and `market_view`, otherwise the client's `surface_rules` text rendered as
  sent), and `update_row` is one thing that moved underneath the baseline.
  They are read for the rest of the thread and cannot be re-rendered, so
  nothing in them may be relative to now and none of them takes a guidance
  fence: the model that reads one back may not be the one it was written for.
  `turn` states an absolute stamp rather than an age or a countdown;
  `update_row` states none at all and leans on where it sits, directly after
  the anchor that already gives the turn its time. A change row also carries
  the paragraph explaining what a change row is, because nothing in the system
  prefix does.
  Like a tail fragment they emit bare markdown, because which wrapper they get
  (a `<system-reminder>` block or an operator-role message) is decided per call
  by `runtime_context/carrier.py`.

Guidance splits here the same way it does in a component, with one extra rule:
**lean carries state, detailed carries state plus steering.** A lean fragment is
facts and values only, on the assumption a frontier model knows what to do with
a clock. Provenance and trust labels are not steering and render at both levels.

Every line is guarded on its own value. Jinja resolves an unknown name to the
empty string, so an unwired kwarg would otherwise render a headed blank
(`**Now:** `) that reads as authoritative and says nothing; an empty section is
caught by eye on the first render, a blank stamp is not caught at all. The kwarg
each fragment expects is recorded under `envelope:` in `../config/prompts.yaml`
and pinned by `tests/unit/ptc_agent/agent/prompts/test_runtime_context_templates.py`.
