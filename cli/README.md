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

Guides for each part of the CLI are in the [CLI docs](https://docs.langalpha.ai/cli/overview/), and each section below links its page.

## Signing in

The [signing in](https://docs.langalpha.ai/cli/signing-in/) and [sign-in commands](https://docs.langalpha.ai/cli/auth/) pages walk through each route.

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
5. None, which a self-hosted server in oss mode takes. Which server that is
   follows [Choosing the server](#choosing-the-server).

`langalpha login` asks the server (`GET /api/v1/auth/cli`) how to sign in. It opens
the browser (authorization code with PKCE, answered on a `127.0.0.1` port)
when the server names an issuer, whose metadata must name that issuer too,
asking it for the API scopes the server lists where it reads them from the
token, and otherwise reads a personal access token (`la_pat_...`) from stdin;
`--with-token` forces the latter, for CI.

## Choosing the server

`langalpha` talks to the first server it finds:

1. `--server URL`
2. `LANGALPHA_URL`
3. The server of your last `langalpha login`
4. This build's default: the hosted service for the package from npm, so
   `npm install -g langalpha` then `langalpha login` signs a hosted user in
   with no setup, and a local server (`http://localhost:8000`) for a build
   from source (`pnpm build` in this folder), which is what a self-hosted
   install wants.

Where `LANGALPHA_TOKEN_FILE` is set, the `base_url` in that file is the
server instead of a saved sign-in or the default; `--server` or
`LANGALPHA_URL` must then have the same origin, or the command is refused.

If `langalpha login` cannot reach the default and no sign-in is saved, it
says to pass `--server URL` or set `LANGALPHA_URL`. To sign in to a
self-hosted server from the npm package, run
`langalpha login --server http://localhost:8000` once; later commands use it.
A token passed with `--token` or `LANGALPHA_TOKEN` goes to the same server,
so with the npm package set `LANGALPHA_URL` beside it to reach a self-hosted
server.

## Commands

From the command spec (`src/commands/`), which `langalpha --help` and each
`langalpha <command> --help` also print; an option a command does not declare is
refused. The docs cover [computers](https://docs.langalpha.ai/cli/computers/), [workspaces](https://docs.langalpha.ai/cli/workspaces/),
[threads](https://docs.langalpha.ai/cli/threads/), [automations](https://docs.langalpha.ai/cli/automations/) and [models](https://docs.langalpha.ai/cli/models/), with
[scripting recipes](https://docs.langalpha.ai/cli/scripting/).

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
langalpha threads list [-w WS] [--archived] [--limit N] [--offset N]
langalpha threads show <id>
langalpha threads output <id> [--turns N]
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
langalpha drive ls [path] [-r] [--space SPACE]
langalpha drive cat <path> [--rev N] [--space SPACE]
langalpha drive get <path> [-o FILE] [--rev N] [--force] [--space SPACE]
langalpha drive put <file> <path> [--note WHY] [--new] [--if-match SHA256] [--space SPACE]
langalpha drive mkdir <path> [--space SPACE]
langalpha drive mv <from> <to> [--space SPACE]
langalpha drive rm <path> [--space SPACE]
langalpha drive history <path> [--space SPACE]
langalpha drive history clear <path> [--before N] [--space SPACE]
langalpha drive restore <path> --rev N [--space SPACE]
langalpha drive trash [--space SPACE]
langalpha drive trash restore <trash_id> [--space SPACE]
langalpha drive usage [--space SPACE]
langalpha models list
```

`drive` is the server-held file tree: paths like `research/nvda/thesis.md`, `put -`
reads stdin, and `put` takes a folder too, sending every file under it with its
relative path. Files of any size go in parts, a few at a time, straight to the
Drive's storage (or through the server where the storage cannot be reached), with
memory held to a few parts; a terminal shows progress on stderr. Run the same `put`
again after an interruption and it continues from the parts the Drive holds, and bytes
the Drive already holds are answered at once. `get` downloads by ranges into a
hidden `.NAME.la-part` file beside the target, checks the file's SHA-256, then
renames it into place; run it again to continue an interrupted download. `rm` moves to
a trash kept 14 days (`drive trash restore` undoes it, emptying it is done only in the
app), and `put --new` or `--if-match SHA256` exit 5 rather than overwrite a file that
is there or has changed (the hash is `sha256` in `drive ls --json`). `--rev N` reads
an older version. `history` says when each earlier version goes: a text file keeps
every version, another file under 100 MiB keeps them 30 days and at most 10, and a
larger one keeps none, so saving over it replaces it. `history clear` deletes a file's
earlier versions for good and prints the room it freed (`--before N` keeps version N
and later). `usage` shows the space the Drive counts, live files and their earlier
versions, against its allowance, and the trash apart: the trash does not count, and
when it grows past the allowance its oldest items go before their 14 days.

`WS` is a workspace id, its exact name, or `home`. A turn streams the
agent's words by default; `--wait` prints only the answer, `--detach` returns
once the server names the thread and run, and `--json` prints every event as
a line. `approve` and `reject` answer what the thread is waiting on now, read
from the server, so any machine can answer; `--interrupt` picks one when it
waits on several. An order is answered only in the app. A turn the server
gave up on exits 7 and says how to run it again: `langalpha threads retry` when the
failed run was kept, sending the message again when it never started. A delete asks first at a terminal, and with no one there
needs `--yes`.

Exit codes: 0 ok, 1 error, 2 usage, 3 not signed in or not allowed (401/403),
including a request you declined in the app, 4 not found, 5 conflict, 6 the
turn is waiting for approval, 7 the turn failed or was cancelled.

## Inside a computer

The [inside a computer](https://docs.langalpha.ai/cli/inside-a-computer/) page is the user-facing version of this section.

Every computer gets the bundle with its internal packages, a shim on every
agent command's `PATH` answering to both `la` and `langalpha`, and a token of
its own (`lac1.`). Every computer's token follows the same rule. It reaches
all of your computers and Home, since the Chief of Staff manages everything
through `la` and an analyst reads the workspaces and threads its work builds
on. It never adds a computer, answers an approval or an order, stops,
archives or removes its own computer, rewrites a turn, or sends into the
conversation the command runs in or the one that sent that conversation its
current turn (`src/server/dependencies/principal.py`).

Some requests wait for your OK in the LangAlpha app
(`src/server/dependencies/cli_approval.py`): deleting a thread, a workspace
or an automation, creating a workspace, removing another computer, and
starting work on a computer other than the one asking. That last one is
sending a message into a thread there, starting a thread in a workspace
there, retrying a turn there, and creating, editing or running an automation
whose turns run there. Home counts as being on the computer that holds it
(your primary), so from any other computer a new thread with no workspace
asks. Reading, cancelling a run, archiving, renaming, and starting or
stopping another computer never ask, and neither does anything else on the
computer asking. A workspace is created without asking when you let the
Chief of Staff create workspaces without asking. The same command sent again
waits on the same request, and a different message asks anew. The command
waits: it prints
`Waiting for your approval in the LangAlpha app (expires in N minutes)…` once and
asks again until you answer, then prints the real result, or exits 3 when you
decline. A request unanswered for 30 minutes stops waiting, as the server
counts it, not the machine's clock, and the command exits 1 having done
nothing; run it again to ask again. At most five requests
wait at once, and a sixth exits 1 with the server's message, as it does when
the server cannot ask right now; run it again shortly. Any other delete from a
computer's token is refused outright and exits 3.

These limits hold where sign-in is on. With sign-in off (oss mode with no
auth configured) the API takes calls with no credentials at all, so anything
on the computer can do what you can.

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

## Releasing

Releases go to npm from `.github/workflows/cli-release.yml`, never from a
laptop. Computers don't install from npm: the backend image builds the CLI
from this folder, so a release only reaches people's own machines.

1. Set `version` in `package.json` and merge it to `main`.
2. Tag that commit `cli-v<version>` and push the tag:
   `git tag cli-v0.1.0 && git push origin cli-v0.1.0`.
3. The workflow refuses a tag that doesn't match `package.json`, then
   typechecks, tests, builds and checks what the package would ship
   (`scripts/check-package.mjs`, which the test workflow runs too).
4. The build sets the hosted service as the default server from the
   repository variable `LANGALPHA_CLI_DEFAULT_SERVER` (`build.mjs` reads it as
   an environment variable and defines `__LA_DEFAULT_SERVER__`); the workflow
   fails when the variable is unset or not an https URL, so a release never
   points at localhost. It then asks that server for the sign-in `login`
   asks for (`scripts/check-default-server.mjs`): the release fails unless it
   names an https issuer and a client and the issuer publishes settings for
   itself with https endpoints, so the hosted service has to serve the CLI routes before a release.
   A build without it, as from source, defaults to a local server.
5. A maintainer approves the `npm` environment's deployment, and the exact
   tarball that was checked is published: a prerelease (`0.2.0-rc.1`) under
   `next`, anything else under `latest`.

npm trusts the workflow by name, so no npm token is stored anywhere. That
trust is set on npmjs.com, under the package's trusted publishers: GitHub
Actions, repository `ginlix-ai/LangAlpha`, workflow `cli-release.yml`,
environment `npm`, with `npm publish` allowed.
