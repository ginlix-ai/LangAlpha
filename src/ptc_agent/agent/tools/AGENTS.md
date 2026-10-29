# Tools

Platform tools bound to the PTC agent: code execution, shell, filesystem, widgets,
sandbox previews.

Their docstrings are prompt surface, governed by **`src/tools/AGENTS.md`** (the
contract for every agent-facing tool docstring in the repo). This pointer exists
because these tools were the population that drifted: the rule used to live only as
an out-of-scope clause inside the MCP contract, which named `src/tools/market_data/`
and nothing here.
