# Agent-facing tool docstrings

A tool's docstring is not documentation. It is the tool's **prompt description**:
the model reads it at call time, before any result exists, and it is the only thing
the model knows about the tool. This file is the contract for every agent-facing
tool in the repo.

| Surface | Lives in | Read by |
|---|---|---|
| **Direct tools** | `src/tools/`, `src/ptc_agent/agent/tools/` | The model, as a LangChain tool description |
| **MCP server tools** | `plugins/*/` | The model (via `tool_summary` and the generated sandbox wrappers) **and** agent-written Python that indexes the result |

Envelope shape, published schemas, error arms and lock mechanics for the MCP surface
are in `mcp_servers/AGENT_CONTRACT.md`. This file governs the docstring itself, on
both surfaces.

## The rule

**Every sentence must change a decision the model makes at call time**: whether to
call this tool rather than another one, what to pass, or what to do with the result.
A sentence that changes none of the three is prompt cost with no return, paid on
every turn of every conversation.

## Three sections, three jobs

```
<What the tool is, and when to use it. 1-2 lines.>
<Optional, on demand: where its coverage stops and what it will not accept.>

Args:
    <param>: What to pass, and the accepted values.

Returns:
    <What to expect back.>

<Optional, and only where the tool's nature demands it: what to do with the result.>
```

Two of the four are optional. A tool whose coverage has no edge and whose result needs
no handling is finished after `Args:` and one line of `Returns:`, and `get_quote` is.

| Section | Owns | Never carries |
|---|---|---|
| **Summary** | What the tool is. When to use it. When *not* to, where a genuine constraint exists. | What comes back |
| **Limits** *(optional)* | Where coverage stops, and hard caps the call will be refused for | Soft warnings, restated defaults, "may be slow" |
| **`Args:`** | Each parameter: what to pass, the accepted vocabulary | Behaviour that is not about an argument |
| **`Returns:`** | What to expect back | Section inventories, formatting trivia, anything the model will never see |
| **Handling** *(optional)* | What to do with the result, where the tool's own nature decides it | Workflow advice that would read the same on ten tools |

A parameter is described **either** in the docstring's `Args:` block **or** in
`Annotated[...]`/pydantic field descriptions, never both.

Which copy to keep depends on the decorator, so check it before deleting either.
Most tools here do not set `parse_docstring=True`, so their `Args:` block does not
reach the JSON schema: it is prose the model reads alongside a schema carrying only
names, types and defaults, and the `Annotated`/`Field` text is the copy that ships.
`TodoWrite` (`ptc_agent/agent/tools/todo/tool.py`) and `think_tool`
(`ptc_agent/agent/tools/think.py`) **do** set it, so there the `Args:` block *is* the
published description and the `Field`/`Annotated` copy is the one to drop. Deleting
the wrong half silently changes what the model is told a parameter means.

**The split is load-bearing, so move return information rather than deleting it.**
A fact about the result written into the summary reads as a description of the tool
and gets skimmed. The same fact under `Returns:` is where a model looks when it is
working out what happens next. `Read`'s "with line numbers (cat -n format)" is
return information; it belongs one section down, not in the bin.

## Limits (optional)

The one element that can save a call rather than improve one. A coverage boundary is
pure routing: a model that reads "US SEC filers only" stops and picks a different tool
instead of spending a call to discover it. Put it directly under the summary, on both
surfaces, so it is the second thing read.

Three kinds qualify:

- **Coverage** — where the data stops. `get_sec_filing` is US SEC filers only; a
  Hong Kong or A-share listing has no 10-K, 10-Q or 8-K to fetch.
- **Capacity** — a cap the call is refused for, not a suggestion. `get_quote` takes
  20 symbols; `scrape_pages` takes 10 URLs, 4 in browser mode.
- **Vocabulary** — a closed set the caller must draw from, where it is too long to
  sit in `Args:` (interval names, symbol spellings across markets).

On demand means exactly that: **write one only where a real edge exists.** A tool with
no boundary gets no block. Padding this section with soft warnings is worse than
leaving it out, because it trains the model to skim the place where the hard limits
live.

## `Returns:` differs by surface, on purpose

**Direct tools: one line, and it is a selection signal.** The result is markdown the
model reads a moment later, so the shape cannot inform the call. What can inform it
is anything that separates this tool from the alternatives:

- **how much** comes back (one number, a table, a long report that will cost context)
- **in what form** (paths rather than contents, text rather than an artifact you cannot see)
- **what is deliberately not in it**

If no such line exists, there is no `Returns:` block. Two that earn their place:

```
Returns:
    File paths only, not contents; newest first.        # Glob: tells you a Read is coming

Returns:
    A long markdown report with the filing's official   # get_sec_filing: the cost signal
    source URLs. Expect it to consume real context.     # is the reason to prefer a cheaper tool
```

**MCP tools: the full machine shape.** Agent-written Python indexes `resp["data"]`
at runtime, so keys, ordering, units, timestamp format and the error shape are a
programming contract, not a preview. Codegen also anchors on the literal `Returns:`
label (`_extract_return_info`). Keep it complete; `AGENT_CONTRACT.md` § Docstring
standard has the exact pattern.

Do not harmonise the two. A direct tool given the MCP treatment buys nothing, and an
MCP tool given the direct treatment breaks the sandbox code that reads it.

## How to handle it (optional)

Some tools do not hand back the thing they were asked for. `Task` returns an ID, not
the work. A crawl returns files on disk, not pages. `get_sec_filing` returns text that
carries official source URLs. For those, what to *do* with the result is part of what
the tool is, and a model holding the result will do the wrong thing without a line
saying so.

The test is essence, not helpfulness: **would this sentence read the same on ten other
tools?** If yes it is workflow convention, it belongs in a prompt template, and it
belongs there once. If it follows from what *this* tool returns, it belongs here.

```
Returns:
    A task ID immediately, not the subagent's work.

Keep working; the report arrives through TaskOutput once the task is named
in a completion notification.
```

**Placement differs by surface, and on the MCP side it is not cosmetic.**
`_extract_return_info` captures from `Returns:` until it meets `Args:`, `Example`,
`Note`, `Raises:` or the end of the docstring. A handling section under any other
label (`Handling:`, `Usage:`, a bare paragraph) is **absorbed into the structured
return slot** and corrupts the generated wrapper's return description.

- **Direct tools** — a short trailing paragraph after `Returns:`. Nothing parses it.
- **MCP tools** — put it in the constraint lines under the summary, *before* `Args:`.
  Never after `Returns:`.

## Cross-references

No "for X, use `tool_Y`" webs: with N tools each naming the others, the same routing
sentence is paid N times. One exception, **downward in cost only**: an expensive tool
may name the cheap alternative, never the reverse, and never as a list. A tool that
tells the model it is expensive should be able to say what to reach for instead.

## Anti-patterns

| Anti-pattern | Why it fails the rule |
|---|---|
| `Returns: Search results or ERROR` | Restates the tool's name. Nothing decided. |
| Describing an artifact that "never enters the LLM context" | Telling the model about a thing defined as invisible to it |
| A `Returns:` inventory of the sections a report will contain | A table of contents for a document arriving in full a moment later |
| `Example:` / `Note:` blocks on an MCP tool | They terminate the codegen `Returns:` capture and rot |
| A parameter table for the tool copied into a prompt template | Facts about one tool belong with that tool: the description travels with the binding, so an agent that never binds it never pays. See `src/ptc_agent/agent/prompts/templates/AGENTS.md`. |
| An undocumented parameter the tool accepts | The model cannot use what it is not told about, and hits an avoidable failure |
| Handling advice that is really workflow convention ("dump results to disk first", "synthesise before answering") | True of many tools, so it is paid once per tool instead of once per prompt |
| A limit that is not one ("may be slow", "large requests can time out") | Nothing is decided by it, and it dilutes the section a hard boundary lives in |

## Length

MCP tools target **≤800 characters**, the figure `AGENT_CONTRACT.md` sets: 61 of them
ship as generated wrapper docstrings and per-tool docs the agent reads in the sandbox.
Direct tools have no fixed cap, since each one is bound individually and only the
bound set is paid for. The same instinct still applies: the current direct-tool median
is ~560 characters, and anything past twice that should have a reason.

## Before you edit a pinned one

The seven direct market tools in `src/tools/market_data/tool.py` and every
`plugins/*/*_mcp_server.py` docstring are snapshot-locked
(`tests/unit/mcp_servers/agent_docstring_lock.json`); an edit fails the default unit
suite until the lock is regenerated in the same commit. Return **annotations** are
exempt. Warm sandboxes need no version bump: `MCP_CLIENT_CODEGEN_VERSION` is derived,
and server files resync by content hash.
