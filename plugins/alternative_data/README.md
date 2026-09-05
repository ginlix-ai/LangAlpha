# alternative-data

Two servers that look outside the market tape: X post search and general web
scraping.

## `x_api` ships with an empty `env` on purpose

The bearer token reaches the tool as a per-call argument read from the
workspace vault (`from vault import get; get("X_BEARER_TOKEN")`), never from
the MCP subprocess environment. In a multi-tenant deployment every workspace
gets its own token that way; putting it in `env` would hand one host-wide
token to all of them.

A single-tenant self-host that wants the convenience can add
`"X_BEARER_TOKEN": "${X_BEARER_TOKEN}"` to the server's `env` in `mcp.json`.
The tool falls back to the process environment when the argument is omitted.

## `scrape` is ours rather than `scrapling mcp`

It imports `scrapling` as a library and runs under either `mcp` SDK era. The
package's own `scrapling mcp` entrypoint is pinned to `mcp` 1.x and cannot
start from a 2.x environment, which is the failure this server exists to
avoid.
