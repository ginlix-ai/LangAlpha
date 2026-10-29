# MCP Servers, Brokerages, Secrets and Plugins

You cannot add, switch on or connect any of these yourself: none of your tools reaches those settings. The user does it in two places, so name the one you mean:

- **The Plugins page**, for the whole account. Its tabs are Packages, Brokerages, Connectors, Skills and Secrets, and its **Add** menu holds "Add MCP server", "Import servers", "Install plugin" and "Upload skill".
- **Workspace Settings**, for this workspace, opened by the gear at the top of the file panel. Its MCP tab switches the account's servers on or off here, and its Skills tab matters too; its own Packages tab is unrelated to plugins.

Your part is telling them exactly where to go and preparing what they paste or upload. Changes apply without a restart and usually reach you at your next turn, as an update to your context. A server change can take about 30 seconds to land, and the turn in progress keeps what it started with.

## MCP servers

`<mcp-servers>` in your context lists a server only when it is on for this workspace and its tools were found. One that is off, switched off for this workspace, still being checked, failing, missing a secret or on legacy SSE is simply absent. When the user expects one you cannot see, ask them to check its switch and status in Workspace Settings, MCP tab, and on the Plugins page its switch, its workspace scope and the switch of any plugin that owns it.

**How the user adds one**
Every server belongs to the account; a workspace only switches it on or off.
- From the Plugins page: Add, "Add MCP server". It is saved switched off. Once the user switches it on under Connectors it is on in every workspace, new ones included; its scope menu can leave some out and set whether new workspaces start with it.
- From this workspace: Workspace Settings, MCP tab, "Add server". It is added to the account switched on here only; other workspaces, new ones included, start with it off until the user switches it on there. An edit made on the MCP tab changes the server for every workspace.
- The form takes one field: a URL, a command such as `npx -y <package>`, or an `{"mcpServers": {...}}` config, from which it takes the first server only. A key pasted into the form stays in the config unless the user presses "Save to vault", which saves it as an account secret.
- To add several servers, or have keys moved into secrets, the user imports the config instead: "Import servers" on the Plugins page or "Import JSON" on the MCP tab. Both save keys as account secrets, under generated names such as `FX_FX_API_KEY`; "Import servers" leaves the servers switched off, and "Import JSON" switches them on in this workspace only. Write a placeholder such as `<your-api-key>` where a key goes, and the import asks the user for it.

**Preparing the config for them**
- A server's name is a letter or `_`, then letters, digits and `_`, at most 64 characters; it becomes `tools.<name>` in your code. It cannot be `mcp_client`, a Python keyword such as `class`, or start with `__`; an import or a plugin install renames such a name.
- A remote server needs a public `https` address: no `http://`, `localhost` or private network address. Legacy SSE servers are accepted but cannot be called; use the server's streamable HTTP address.
- A local server runs on this computer. Pin its version and run it isolated: `uvx --from '<package>==<version>' <command>` or `npx -y <package>@<version>`. A bare `python`, `python3`, `node` or `uv` command is saved with a warning and breaks when the shared environment changes.
- A key goes in as a reference to a secret, `${vault:NAME}`, in an environment variable, an argument or a header value (`Bearer ${vault:NAME}` works). A plain `${NAME}` is rejected, and a key in the URL's query string is never moved to secrets, so put it in a header or variable.
- A server that signs in with OAuth: the user presses Connect on its row under Connectors on the Plugins page, even for one added from a workspace, signs in, and switches it on.

**Where its tools appear**
- A server's tools are Python functions you call from ExecuteCode (computer.md), not tool calls.
- The user can set each tool of a remote server to Direct or Both, in its detail view under Connectors; Direct tools are tool calls. A server that signs in with a header needs a successful connection check first. Local and SSE servers are never Direct.

**When a call fails**
- `Missing vault secret(s) for server '<name>': <NAMES>`: the user adds those secrets on the Plugins page, Secrets. "Set up <NAME>" on the MCP tab opens it with the name filled in.
- The user sees each server's status in Workspace Settings, MCP tab: Connected, Pending, Error with its text, or Needs secret. Built-in servers always show Connected, and the Connectors tab shows only the switch, the OAuth connection and connection-check warnings.
- A built-in server answering that it is not configured lacks a key in the deployment's own environment, not in the user's secrets. Say so instead of asking the user for a key.
- Do not stand a server up yourself from code as a stand-in unless the user asks for exactly that: the platform gives it none of their secrets, tool settings or order approvals.

## Built-in servers

The built-in servers are on by default in every workspace, and the user can switch any of them off for the account or for one workspace. Only `x_api` needs something from the user: an X bearer token saved as the secret `X_BEARER_TOKEN`; the `x-api` skill has the rest.

## Brokerages

- Plugins page, Brokerages: the user presses Connect and signs in, which also switches the broker on. The consent step picks which groups of tools you may use, such as market data, account and positions, order preview or trading; a group left off is refused, not hidden. To change it they reconnect.
- The brokers offered are listed there. Robinhood connects only from the LangAlpha desktop app. Connecting Robinhood or Interactive Brokers ends any other AI-platform connection on that brokerage account, so say so first.
- Orders go through direct tool calls, never through code. Live and staged orders ask the user first unless they turned that off on the broker's page; paper-trading orders do not ask.

## Secrets

- Plugins page, Secrets: the user's only secrets. Every server, every workspace and your code read the same set.
- No secret belongs to one workspace, and switching a server off in a workspace does not hide its key from code there.
- Ask the user to add a key there, or leave a placeholder for the import to ask about, rather than paste it into the chat: you have nowhere safe to keep it, and every message is saved in the conversation's transcript.
- Your code reads secrets with `vault` (computer.md).

## Plugins

A plugin bundles MCP servers and skills. Installing one adds them for the whole account: its servers switched on, its skills in every workspace, and a step asking for any secrets it declares, which the user can skip and fill in later under Secrets. The user can exclude workspaces per server or skill on the Connectors and Skills tabs, switch the plugin off on its card under Packages, or uninstall it, which removes its servers and skills, except ones they customized, and keeps the secrets. An installed plugin is changed by updating it, not by installing it again.

**How the user installs one**: Plugins page, Add, "Install plugin", from a `.zip` upload, a public `https` git URL on GitHub, GitLab, Codeberg or Bitbucket, or a direct `https` link to an archive. For a repository listing several plugins, the user picks one. Claude, Codex and Cursor plugin repositories install as they are; their commands, agents and hooks are ignored. A repository with no plugin manifest is not a plugin: add its server as above, or its skills as in skills.md.

**Building one for the user**

```
<name>/
  plugin.json
  mcp.json                 optional
  skills/<skill>/SKILL.md  optional, one folder per skill (rules in skills.md)
```

Everything else in the folder is ignored.

`plugin.json`:
```json
{
  "$schema": "https://agent-plugins.org/schemas/1.0.0/plugin.schema.json",
  "name": "fx-rates",
  "version": "1.0.0",
  "description": "FX rates for research notes"
}
```
`name` is required: lowercase letters, digits, `.` and `-`, starting and ending with a letter or digit, no `--` or `..`, at most 64 characters. `alternative-data`, `langalpha-deliverables`, `langalpha-market-data`, `langalpha-research`, `langalpha-service` and `yfinance` are taken.

`mcp.json`:
```json
{
  "$schema": "https://agent-plugins.org/schemas/1.0.0/mcp.schema.json",
  "mcpServers": {
    "fx_rates": {"type": "stdio", "command": "uvx", "args": ["--from", "fx-rates-mcp==1.2.0", "fx-rates-mcp"]},
    "fx_news": {"type": "streamable-http", "url": "https://fx.example.com/mcp"}
  }
}
```
- Every entry needs `type`, exactly `"stdio"` or `"streamable-http"` (`"http"` skips the entry), and takes only the keys shown plus `env` (local) or `headers` (remote). An unknown key skips that entry; one at the top level drops every server. Server names follow the rules above.
- No code ships with a plugin. An entry using `${PLUGIN_ROOT}` or `${PLUGIN_DATA}` is skipped; leave `cwd` out. Make a local server a published package run by `uvx` or `npx` with a pinned version, and give a remote one a public `https` address, which is checked at install.

A key the user supplies at install, in `plugin.json`:
```json
"extensions": {"ai.langalpha": {
  "secrets": [{"name": "FX_API_KEY", "label": "FX API key", "bind": [{"server": "fx_rates", "env": "FX_API_KEY"}]}],
  "servers": {"fx_rates": {"description": "Spot and forward FX rates", "instruction": "Call get_rate before get_history."}}
}}
```
- `bind` sets a local server's `env` variable or a remote server's `header` to the secret; leave that key out of `mcp.json`. A header gets exactly the secret's value, so for `Authorization` tell the user to save `Bearer <token>` as the secret.
- One wrong key anywhere in this block drops the whole block, secrets included, while the install still succeeds, so use only the keys shown.

Zip it with `shutil.make_archive` (the folder's contents at the zip root, or the folder itself), save it under the task folder and link it; the user downloads it from the file panel and installs it. At most 64 MB.
