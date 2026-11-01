# langalpha

The LangAlpha command line: threads, workspaces, computers and automations,
for a person at a terminal, a script, or the agent inside a computer.

`langalpha` is its name and `la` the short one; both run the same program.
Many shells already alias `la` to `ls` (oh-my-zsh, Ubuntu's default
`.bashrc`), and an alias wins over the program, so type `langalpha` there, or
`command la`. Usage lines and hints name `la` only when that is what you
typed. Inside a computer the agent's commands run without your aliases, so
`la` is safe there.

```bash
cd cli && pnpm install && pnpm build   # dist/la.mjs, one file, no dependencies
node dist/la.mjs --help
```

## Signing in

`langalpha` takes the first credential it finds:

1. `--token T`
2. `LANGALPHA_TOKEN`
3. `LANGALPHA_TOKEN_FILE`, the file the server keeps fresh inside a computer
   (the agent's commands have it set). Read on every request, and again
   after a 401, since the server rewrites it under a running command.
   Where it is set, nothing below is tried: a file with no token, or naming
   no server without `--server`, is an error.
4. Your sign-in, from `langalpha login`, kept in `~/.config/langalpha/credentials.json`
   (owner-only). A refresh, `langalpha login` and `langalpha logout` take a lock beside it,
   so commands run side by side spend the rotating refresh token once and share
   what it bought, and a refresh never writes back a sign-in removed or
   replaced meanwhile.
5. None, which a self-hosted server in oss mode takes.

`langalpha login` asks the server (`GET /api/v1/auth/cli`) how to sign in. It opens
the browser (authorization code with PKCE, answered on a `127.0.0.1` port)
when the server names an issuer, whose metadata must name that issuer too,
asking it for the API scopes the server lists where it reads them from the
token, and otherwise reads a personal access token (`la_pat_...`) from stdin;
`--with-token` forces the latter, for CI.
`--server URL` (or `LANGALPHA_URL`) picks the server.

## Commands

From the command spec (`src/commands/`), which `langalpha --help` and each
`langalpha <command> --help` also print; an option a command does not declare is
refused.

```
langalpha login [--with-token] [--no-browser]
langalpha logout
langalpha whoami
langalpha computers list
langalpha computers add [--name N] [--kind K] [--tier T] [--ssh USER@HOST[:PORT]] [--root DIR] [--host-key-fingerprint FP] [-y]
langalpha computers start <id>
langalpha computers stop <id>
langalpha computers remove <id> [-y]
langalpha workspaces list [--include-home] [--limit N]
langalpha workspaces create <name> [--description D] [--computer C]
langalpha workspaces archive <ws>
langalpha workspaces delete <ws> [-y] [--delete-files]
langalpha threads list [-w WS] [--archived] [--limit N]
langalpha threads show <id>
langalpha threads new <message...> -w WS [-m MODEL] [--wait] [--detach]
langalpha threads send <id> <message...> [-m MODEL] [--wait] [--detach]
langalpha threads follow <id>
langalpha threads retry <id> [-m MODEL] [--wait] [--detach]
langalpha threads approve <id> [--message M] [--interrupt I] [--wait] [--detach]
langalpha threads reject <id> [--message M] [--interrupt I] [--wait] [--detach]
langalpha threads cancel <id>
langalpha threads archive <id>
langalpha threads delete <id> [-y]
langalpha automations list [--status S] [--limit N]
langalpha automations create --name N --prompt P [--cron EXPR] [--at ISO] [--timezone TZ] -w WS [-m MODEL]
langalpha automations edit <id> [--name N] [--prompt P] [--cron EXPR] [--at ISO] [--timezone TZ] [-w WS] [-m MODEL]
langalpha automations trigger <id>
langalpha automations pause <id>
langalpha automations resume <id>
langalpha automations delete <id> [-y]
langalpha models list
```

`WS` is a workspace id, its exact name, or `home`. A turn streams the
agent's words by default; `--wait` prints only the answer, `--detach` returns
once the server names the thread and run, and `--json` prints every event as
a line. `approve` and `reject` answer what the thread is waiting on now, read
from the server, so any machine can answer; `--interrupt` picks one when it
waits on several. An order is answered only in the app. A turn the server
gave up on exits 7 and says how to run it again: `langalpha threads retry` when the
failed run was kept, sending the message again when it never started. A delete asks first at a terminal, and with no one there
needs `--yes`.

Exit codes: 0 ok, 1 error, 2 usage, 3 not signed in or not allowed (401/403), 4 not found,
5 conflict, 6 the turn is waiting for approval, 7 the turn failed or was cancelled.

## Inside a computer

Every computer gets the bundle with its internal packages, a shim on every
agent command's `PATH` answering to both `la` and `langalpha`, and a token of
its own (`lac1.`), which reaches only what that computer holds and never
deletes, answers an approval, stops its own computer, or sends into the
conversation the command runs in
(`src/server/dependencies/principal.py`).

The shim runs the bundle on the machine's `node`, which needs Node 20 or
later. Sandboxes ship it; on an SSH machine it is whatever the machine has,
and adding one without it warns that the agent will have no CLI there.

Each request carries the command's call id (`LIVEFS_CALL_ID`, sent as
`X-Langalpha-Call`), which the server reads as the thread that ran it. Every
request is recorded with that thread, refused ones included
(`computer_cli_calls`, kept 90 days), and a thread `la` starts, or a message
it sends, is marked as from that thread (`origin: {type: agent, agent: cli}`),
which the app shows as "From another thread". The thread is the command's
own report: another process on the computer could send a sibling's id.
