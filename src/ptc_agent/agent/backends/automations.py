"""AutomationsBackend: each of the user's automations as a file in `.agents/user/automations/`.

The rows live in Postgres; ``services.automations.file`` serializes each one
and turns a save of its file into a create, update or delete. Flash, which has
no filesystem, manages the same rows through the automation tools.
"""

from __future__ import annotations

from ptc_agent.agent.backends.db_json_folder import DbJsonFolderRoute
from ptc_agent.agent.backends.db_json_route import README_FILE
from ptc_agent.core.paths import SandboxLayout
from src.server.services.automations import file

__all__ = ["README_FILE", "AutomationsBackend"]

_README_CONTENT = """\
# Automations

Each automation the user has is one JSON file in this folder: an agent run
that starts on a schedule or when a price condition is met, without the user
present. The files are the live database, not a copy. Reads are fresh. A write
is checked, then saved, and the Write/Edit result says what it created,
updated or deleted. If anything is refused, nothing is saved.

The file name is a reference you pick: 1 to 64 letters, digits, `-` or `_`,
starting with a letter or digit, then `.json` (`morning-brief.json`). It need
not match `name`, which is what the user sees. Automations made elsewhere get
one derived from their name.

**Confirm with the user first.** Automations run unattended, so before you
create or delete one, summarize it and get a yes. Pin down:
- the schedule: "every morning" means which days, what time, which timezone?
- the instruction: it runs with no one to ask. Make it self-contained: which
  tickers, which metrics, what format.
- the thread: a fresh one each run, one ongoing thread, or this conversation.
- delivery: in-app only, or also a chat app such as Slack, or one chat there.

Change only what the user asked for; suggest any other edit instead of making
it. These files are the record of the user's automations: Read them when you
need them rather than keeping their schedules or statuses in notes or memory,
where they go stale.

## Editing

- **Create:** Write a new file. A name that is taken is refused, never
  overwritten.
- **Update:** Read the file, then Write it, or Edit it. A Write is refused if
  you haven't read the file in this run, or if it changed since (the user's
  Automations page, a run, another turn); the user's answer to a question
  starts a new run, so Read again after asking. A field you leave out keeps
  its value; `null` clears `description` or `llm_model`. Each file is checked
  on its own, so a change to one never conflicts with another.
- **Pause / resume:** set `status` to `"paused"` or `"active"`.
- **Delete:** set `status` to `"deleted"`, or `rm` the file from code. Its run
  history goes with it and cannot be restored.
- **Rename:** `mv` a file to another name in this folder changes only the
  file name; the automation and its history stay.
- Copying a file creates a second automation that runs too, so never copy one
  as a backup.
- `state` is kept by the server and moves as runs happen, which never makes a
  write conflict. A write ignores it, and the result notes an edit to it: to
  move a run, change `cron_expression` or `next_run_at`.
- Nothing in a file runs an automation now. The user can, with Run now on the
  Automations page.

Code sees these files at the same paths when the sandbox has the file mount,
and the command's result lists each change or why a save was refused. Write a
file in place: `sed -i` and write-then-rename helpers save a temporary file
first, which is refused or becomes an automation of its own.

## Example

`morning-brief.json`:

```json
{
  "name": "Morning market brief",
  "description": null,
  "status": "active",
  "trigger_type": "cron",
  "cron_expression": "0 9 * * 1-5",
  "timezone": "America/New_York",
  "instruction": "Summarize overnight moves for my watchlist: top movers, news, one line each.",
  "agent_mode": "flash",
  "workspace_id": null,
  "thread": "new",
  "llm_model": null,
  "delivery": ["slack"],
  "max_failures": 3,
  "state": {
    "automation_id": "00000000-0000-4000-8000-000000000001",
    "next_run_at": "2026-09-29T09:00:00-04:00",
    "last_run": {"status": "completed", "at": "2026-09-28T09:01:12-04:00"}
  }
}
```

A new one needs only what has no default, e.g. `aapl-below-200.json`:

```json
{
  "name": "AAPL below 200",
  "trigger_type": "price",
  "trigger_config": {
    "symbol": "AAPL",
    "conditions": [{"type": "price_below", "value": 200}]
  },
  "instruction": "AAPL just fell below $200. Summarize the news and analyst moves behind it.",
  "agent_mode": "flash"
}
```

## Fields

| Field | Required on create | Notes |
|-------|--------------------|-------|
| name           | yes | What the user sees; up to 255 characters. |
| description    | no  | Free text. |
| status         | no  | `active` (default) or `paused`. The server also sets `executing` (a price alert firing now), `completed` (a one-time or one-shot run finished) and `disabled` (switched off, see `state.disable_reason`); set `active` to resume a disabled one. A `completed` one won't run on its own again: write a new file for another run. `deleted` deletes it. |
| trigger_type   | no  | `cron`, `once` or `price`; inferred from the schedule field you give. Fixed once created: to change it, create a new automation and delete the old one. |
| cron_expression| cron | 5-field cron on the automation's clock: `0 9 * * 1-5` weekdays 9:00, `0 */4 * * *` every 4 hours, `30 8 1 * *` the 1st at 8:30. No seconds field; a step can't span fields (no "every 90 minutes"). |
| next_run_at    | once | A future ISO time with a time of day, e.g. `2026-10-01T09:00:00`. Without an offset it is read in `timezone`. |
| trigger_config | price | See Price alerts. |
| timezone       | no  | IANA region name, e.g. `America/New_York` (not `EST`, which ignores daylight saving). Defaults to the user's timezone. Changing it keeps a one-time automation's moment, since `next_run_at` carries its offset. |
| instruction    | yes | The prompt each run executes. |
| agent_mode     | no  | `ptc` (default here): runs as you do, in `workspace_id`, with code execution. `flash`: fast, no sandbox; best for price alerts and quick lookups. |
| workspace_id   | ptc | Defaults to this workspace. |
| thread         | no  | `new` (default): a fresh thread each run. `persistent`: one thread that every run continues, created by the first run (`state.last_run.thread_id` names it). `current`: continue in this conversation. A thread id: continue in that thread. |
| llm_model      | no  | A model the user can run; `null` uses their default. An unknown name is refused with the list. |
| delivery       | no  | Where results also go besides the app: a chat app, e.g. `["slack"]`, or one chat there by address, e.g. `["slack:T1/C0123"]` from `list_message_targets`; `[]` for none. A chat is checked when you save, and so is an app, which is refused while the user hasn't linked it. An app alone posts to the chat set for this workspace's automations on that app, else the app's preferred chat, else the user's direct messages; to always post to the direct messages, name them, e.g. `["discord:@me"]` (`slack:<team>` on Slack). Leave sending out of `instruction`: each run sends its result there itself, a chat it missed gets the final answer, and a failed or stopped run posts a short notice. |
| max_failures   | no  | 1–100, default 3. Consecutive failed runs before it is disabled. |

## Price alerts

```json
"trigger_config": {
  "symbol": "TSLA",
  "conditions": [
    {"type": "pct_change_above", "value": 5, "reference": "previous_close"}
  ],
  "retrigger": {"mode": "recurring", "cooldown_seconds": 14400}
}
```

- `symbol`: a bare US stock or index ticker (`AAPL`, `SPX`), no `^` or `I:` prefix.
  There is no crypto, currency or futures feed, so no alert can watch those.
- `conditions`: one or more, all of which must hold. `type` is `price_above`,
  `price_below` (a dollar `value`), `pct_change_above` or `pct_change_below` (a
  percent `value`, always positive). `reference` sets the base for a percent:
  `previous_close` (default) or `day_open`.
- `retrigger.mode`: `one_shot` (default) fires once and completes. `recurring`
  re-arms after `cooldown_seconds` (at least 14400, i.e. 4 hours), or the next
  trading day when it is left out. Default to `one_shot` unless the user wants
  repeated alerts.

Repeat the symbol, condition, value and retrigger mode back to the user before
you save a price alert.

## state (read-only)

- `automation_id`: the server's id for the automation.
- `next_run_at`: when a cron automation runs next.
- `last_run`: its newest run, absent before the first. `status` is `pending`,
  `waiting` (queued behind the turn running on its thread), `running`,
  `completed`, `failed`, `timeout` or `skipped`, as of `at`. `thread_id` is the
  conversation the run posted to, and `excerpt` is the start of its answer once
  it completes. `error` is the start of a failed run's error. `failure_reason`
  `usage_limit` is a usage limit and never counts toward `max_failures`;
  `server_error` and `interrupted` mean the service failed or cut the run off,
  so nothing in the automation needs changing. `skip_reason` says why a run was
  skipped: `user`, `thread_busy` (its thread was running another turn) or
  `interrupted`.
  `dismissed: true` means the user has already seen the failure and set it aside.
- `failure_count`: consecutive failures so far.
- `disable_reason`: why a `disabled` automation was switched off.
  `provider_auth`: the model provider rejected the user's own key; resume it
  once the key is fixed. `max_failures`: it failed `max_failures` times in a row.
"""


class AutomationsBackend(DbJsonFolderRoute):
    """Filesystem surface backed by the `automations` table."""

    directory = SandboxLayout.AUTOMATIONS_DIR
    readme_content = _README_CONTENT
    name_rule = file.NAME_RULE
    entry = "an automation"

    source = "automations_backend"
    read_failure = "Failed to read automations data"
    read_only = (
        "Automation files are read-only through the file panel. "
        "Edit via the Automations page or ask the agent to update them."
    )
    undeletable = (
        "Automation files cannot be deleted through the file panel. "
        "Manage automations via the Automations page."
    )

    @classmethod
    def file_named(cls, name: str) -> file.AutomationFile | None:
        return file.AutomationFile(name) if file.is_file_name(name) else None

    @classmethod
    async def names(cls, user_id: str) -> list[str]:
        return await file.file_names(user_id)

    @classmethod
    async def rendered(cls, user_id: str) -> dict[str, tuple[str, str]]:
        return await file.rendered_files(user_id)
