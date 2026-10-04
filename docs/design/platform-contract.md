# Platform contract: the surface a turn arrives on

A chat turn may declare the surface it came from, and the delivery rules that surface accepts.
langalpha turns the declaration into one line of runtime context, written into the conversation
when the turn opens, and pairs it in the same row with a paragraph saying what that surface
promises. This document is that contract as the calling client sees it.

Who owns what: the client posting the turn owns the surface vocabulary, every per-message
payload it sends, and the delivery rules for any surface it renders itself. For chat channels
that client is the gateway holding the channel webhook. langalpha owns the grammar, the
transport of the rules, and the rules for the two surfaces it renders itself (`web` and
`market_view`). langalpha builds no channel-specific content of its own: it never formats for a
channel's message API, never splits a reply into a channel's chunks, and never addresses a
channel by name in output, and it carries no wording of its own about what a channel accepts.
It reads the pointer and states the rules for that row.

## `platform`

`platform`, optional, on `POST /api/v1/threads/{id}/messages` (`ChatRequest`) and on
`POST /api/v1/threads` (`ThreadCreateRequest`).

Grammar: `^[a-z_]+(:[A-Z0-9][A-Z0-9.-]*)?$`, at most 50 characters.

- The surface name is lowercase letters and underscores.
- An optional `:<SYMBOL>` suffix scopes the surface to one ticker. The symbol starts with a
  letter or a digit: digit-first tickers are ordinary (`0700.HK`, `600519.SH`, `002851.SZ`),
  so the suffix does not require a leading letter.

Examples: `web`, `market_view:AAPL`, `market_view:002851.SZ`, `telegram`.

An absent `platform` renders no surface pointer at all. That is the right value when nothing
about the turn's origin should steer delivery, and it is what a system-initiated thread sends.

## `surface_rules`

`surface_rules`, optional, on `POST /api/v1/threads/{id}/messages` (`ChatRequest`). A plain
string, at most 2000 characters; whitespace is stripped and an empty string is the same as
sending nothing. It is honoured on the same trust an `X-Dispatch: background` dispatch needs:
a request that presents the service token, or any request on an `oss` stack that has no
`INTERNAL_SERVICE_TOKEN` configured, where there is no caller to tell apart. On any other
request the field is dropped before the turn starts, since the text is carried on the
operator role where the provider has one.

This is where a client states what its own surface accepts. langalpha renders two surfaces
itself and ships a paragraph for each of them (below); for every other surface the client that
draws the reply is the one that knows the shape its transport takes, so it writes the paragraph
and sends it here. Whatever arrives is carried to the model verbatim, with no wrapper and no
label, and it replaces langalpha's built-in line rather than joining it: on any surface, the
client that renders the answer is the authority on it.

Send it on every request. langalpha decides when the model actually reads it, on the same "only
when they are news" rule as the pointer, and a change of text counts as news: the first turn
after a gateway rewords its rules restates them. That comparison is why the field is sent every
time rather than once.

A worked example of the shape a gateway might send:

```json
{
  "platform": "slack",
  "surface_rules": "Surface slack: the channel renders a subset of markdown, truncates long messages, and has no widget surface. Reply in plain text or the channel's limited markdown, a few short paragraphs, one message per answer. Wide tables, code blocks and long reports go into a file deliverable; send the summary and the link, not the body."
}
```

## The surfaces langalpha renders

| Surface | What it promises the model |
|---|---|
| `web` | Full markdown, no length cap. Widgets, charts, HTML reports and file deliverables are all available. The default surface and the only one with no constraints. |
| `market_view:<SYMBOL>` | The user is looking at that symbol's chart and asking about what is in front of them. Anything they selected on the chart arrives with their message, and annotations the agent draws on that chart are visible to them on it. Facts, not a format: the shape of the answer is left to the model. |

Every other surface renders whatever the client sent in `surface_rules`, and nothing at all when
it sent none. `telegram`, `slack`, `discord`, `feishu` and `imessage` are still names langalpha recognizes,
so they pass the grammar and render in the pointer, but the wording for them belongs to the
gateway.

Two more sets of rules ride in the same row and are not surfaces; neither is sent in
`platform`:

- **origin `automation`**: full markdown and the full deliverable set, plus the rule that
  nobody is waiting and no question can be answered, so the model states its assumptions
  rather than asking.
- **subagent run**: a role flag set inside langalpha, never by a client. The report is
  addressed to the parent agent rather than to a person.

## `origin`

`origin` is orthogonal to `platform`: `platform` is which surface, `origin` is who started the
thread. It is an object, `{"type": "agent" | "automation" | "system", "id": "<optional>"}`,
recorded once at thread creation and ignored for existing threads. An absent `origin` means
user-initiated, which is the common case and is never written. The label is advisory: it is
client-supplied, it is not authenticated, and no access decision reads it.

## The pointer is re-read every turn

The surface pointer is current state, not a property of the thread. It is rendered from the
turn's own `platform` value once, at the turn boundary, into the row that turn writes into
history, so a thread asked in the web app and followed up from a chat channel carries `web` on
one turn and the channel on the next, with no thread edit in between. Each row keeps its place
in time and describes the turn it was written for.

The rules themselves ride in that row too, and only when they are news. langalpha compares the
rules this turn needs against the last ones the model can still see on the wire, and writes the
paragraph when they differ: the thread's first turn, a turn that changed surface, a turn whose
`surface_rules` text changed, the first turn after a compaction dropped the row that stated
them, and every subagent run, which starts with no history of its own. A thread that stays in
the web app therefore states the web rules once and never repeats them.

A consequence for the caller: send `platform` and `surface_rules` on every message, not only on
the first. A turn that omits either does not inherit the previous turn's value.

Two kinds of turn are the exception, because they finish work rather than start it: a request
that resumes an interrupt, and a notification turn langalpha posts to itself when background
work it dispatched finishes (a dispatched run or a background task reporting back). Both run
under the last rules stated, so a channel conversation's rules stay in force while the report
lands there, and a notification never counts as the attended turn that follows an automation's.

## Messaging tools

With `CHANNEL_GATEWAY_URL` (the gateway's API base, path prefix included) and
`INTERNAL_SERVICE_TOKEN` both set, both agents get `send_message` and `list_message_targets`.
Unset, neither exists. Delivery is the gateway's: it decides who may be reached and re-checks
every address on every send, so an address the model passes is a claim, not a credential.

Both calls carry `X-Service-Token` and `X-User-Id`, and name the conversation the turn is in by
`thread_id`, `run_id` and `turn_platform`. `turn_platform` is the turn's surface name, without
its symbol. A notification turn and a resumed interrupt arrive without a surface of their own,
so they send the surface the thread is bound to (the one it was created on, or a channel
identity stamped onto it later). A plain web turn sends none. A retry (`/retry`) names no
surface either, so it runs on the surface and under the rules its failed attempt ran with,
which each attempt records on its run row: a channel turn's retry sends the channel, and a web
turn's sends none, whatever the thread is bound to.

- `GET {base}/agent/targets?thread_id=&run_id=&turn_platform=` answers `current` (the
  conversation this turn is in, or null), `targets` (`address`, `platform`, `kind`, `name`, and
  `thread.last_used_at`, epoch seconds, when this conversation already has a thread in that chat,
  and `preferred`, true for the chat the user picked as the app's default target),
  `unavailable` (`platform`, `reason`, `message`) and `settings_url`. A direct message's
  `address` is `<app>:@me`, or `slack:<team>` on Slack.
- `POST {base}/agent/send` takes `thread_id`, `run_id`, `tool_call_id`, `turn_platform`,
  `workspace_id`, `target` (null for the conversation this turn is in; an app name alone is the
  user's direct messages there), `text`, `files`
  (`[{"path", "workspace_id"}]`, a null `workspace_id` meaning the request's), `reply` and
  `new_thread` (true starts a fresh thread in the chat instead of continuing this conversation's;
  always sent), plus `automation_execution_id` on an automation's turn whose delivery the
  messaging service holds (see Automation delivery), and only then. Every
  delivery outcome is a 200 carrying `status` (`sent`, `partial`, `failed`), `code`, `message`,
  `address`, `current`, `duplicate` and per-file `files` (`path`, `status`, `reason`). The same
  `tool_call_id` twice is the graph replaying a tool step after a resume, and must not send
  twice.

`send_message` also returns a `message_delivery` artifact beside the text the model reads: `status`,
`code`, `address`, `platform` (the address's app), `current`, `duplicate`, `message` and `files`
(`path`, `status`, `reason`). It travels on the `tool_call_result` event and in replayed history for
clients to render, and the model never sees it.

The tools never raise into the graph. A gateway that cannot be reached, refuses the token, or
does not answer in time comes back to the model as a failed or unknown result, and the model is
told to claim nothing as sent unless the status says so.

### Channel settings

With the same configuration, the PTC agent also gets `.agents/user/channels/`, reached only
through the file tools (never the file mount, and Bash and ExecuteCode refuse a command naming
it). `channels.json` is the user's settings and `available.json` the choices they may name; the
gateway holds both, with the same two headers:

- `GET {base}/agent/settings` answers `{"version", "settings"}`. `settings` has `default`
  (`mode`, `workspace_id`) and one key per linked app: `preferred` (an address, or null for the
  user's direct messages), `chats` (address → `mode`, `workspace_id`, `workspace`, `name`),
  `automation_output` (workspace id → address) and `agent_messages` (`enabled`, `allowed`). A
  preferred chat counts as allowed, and `name` is the gateway's read-only label.
- `PUT {base}/agent/settings` takes `{"version", "settings"}` and answers 200
  `{"version", "settings", "changes"}`, 409 `{"code": "version_conflict"}` when the settings moved
  since `version`, 400 `{"code": "invalid", "message", "problems": [{"field", "message"}]}` or 503
  `{"code": "unavailable", "message"}`. A 503 whose `applied` lists change lines saved those and
  not the rest: langalpha reports them with the message and drops the agent's Read, so it reads
  the file again before retrying. langalpha checks the shape first (`default` is an object, never
  null) and that every workspace id the save sets or moves (in `default`, `chats` and the
  `automation_output` keys) is one of the user's non-flash workspaces; an id the settings already
  hold at that place passes as it is, so a binding left on a deleted workspace doesn't block other
  edits. It fills each binding's `workspace` with that workspace's name, null when it no longer
  exists. On a read it renders the names from its own workspaces, since the gateway's copy may be
  stale. A key it doesn't model goes to the gateway as written, at every level: the gateway owns
  the schema, so it refuses a key it doesn't know with a 400 problem naming that field.
- `GET {base}/agent/settings/available` answers `{"apps": {app: {"chats": [{"address", "name",
  "kind"}], "complete", "error"}}}`, direct messages as `<app>:@me`, or `slack:<team>` on Slack.
  `available.json` adds the user's workspaces by id and name.
- `POST {base}/agent/check-target` takes `{"address", "purpose": "automation"}` and answers
  `{"ok", "address", "name", "message"}`, with `<app>:@me` back as sent. Every save of an
  automation's `delivery` (file, REST or tool) checks each entry it newly names here: a chat
  address is stored as the canonical `address`, and an app name (`"slack"`) is kept as written,
  refused while the user hasn't linked that app. An entry the automation already holds is not
  checked again. Without a gateway, an address entry is refused, while an app name saves
  unchecked. An automation holds at most 20 entries, what `automation-runs` takes below, of at
  most 256 characters each, what this check takes: every save refuses an entry past either
  limit before anything is checked, as REST's 409 `problems`, a tool's error or a problem on the file's `delivery`.
  The gateway resolves an app name to the workspace's `automation_output`, else the
  app's `preferred`, else the user's direct messages; an address, `<app>:@me` included, posts to
  that chat.

### Automation delivery

With the same configuration, an automation run delivers through the messaging service (the
gateway above) instead of the automation webhook (`AUTOMATION_WEBHOOK_URL`), none of whose
events such a run fires. Same two headers:

- `POST {base}/agent/automation-runs` takes `{"execution_id", "workspace_id",
  "automation_name", "entries", "thread_id"}` before the run's turn starts, `thread_id` being
  the run's thread, already resolved (both calls send it, so what the service posts links back
  to the thread and a reply there continues it), and answers `{"targets":
  [{"entry", "address", "name", "ok", "message"}]}`; a refused entry has `ok` false and a null
  `address`. The same `execution_id` again answers the same run. When the call fails or no entry
  is `ok`, the run delivers by webhook as before. Otherwise the run's agent is told the `ok`
  targets, its `send_message` calls carry `automation_execution_id`, and the targets are stamped
  on the run row, so the settle ends the run the same way on whichever worker drains it.
- `POST {base}/agent/automation-runs/{execution_id}/finish` takes `{"status", "final_text",
  "thread_id"}` as the run settles. `status` is `completed`, `failed` (an error, a refused key,
  a usage limit, a server fault, an interrupted run) or `stopped` (the user stopped it);
  `final_text`, the run's last answer cut to 20,000 characters, rides only on `completed`. It
  answers `{"targets":
  [{"entry", "address", "name", "reached", "via", "error"}]}` with `via` one of `agent`,
  `fallback`, `notice` or null; a second finish posts nothing more, and 404 is a run it has no
  record of. Each target becomes a `delivery_result` item `{"method", "address", "name",
  "success", "via", "error"}`, `method` being the entry and `success` meaning reached or posted
  to. `address` is where the post landed, possibly a thread (`slack:T1/C9/1800.000001`) where
  the start named the chat, so an item is matched to its target by `method`. A finish with no
  readable answer records every target as failed, saying delivery couldn't be confirmed. A
  finish answered while another finish for the run is mid-post carries rows still being posted
  (`reached` false, `via` and `error` null; a final row that didn't land always has an
  error): those read as unconfirmed, never as landed, until an answer says where they landed.
  A finish that got no answer, a 502, 503 or 504, or rows still being posted is asked again up
  to twice, 2s and then 5s later, beside the settle rather than in its way. Each answer
  replaces the record, and a refusal replaces only one no answer made. A refused token
  (401/403), a 404 or any other answer is not asked again. A run skipped while it waited, or a
  repeat of a refusal already announced, is not finished.
- `GET {base}/agent/automation-targets?workspace_id=` answers `{"apps": {app: {"chats":
  [{"address", "name", "kind"}], "default": {"address", "name", "via"} | null, "error"}}}`,
  `via` one of `workspace`, `preferred` or `dm`: the chat an entry naming only the app reaches.
  langalpha serves it as `GET /api/v1/automations/delivery-options?workspace_id=`, which adds
  `enabled` (false, with no apps, without a messaging service) and answers 503 `{"detail"}` when
  the service fails.
- `PUT {base}/agent/automation-output` takes `{"workspace_id", "platform", "address"}` (a null
  `address` clears it) and answers 200 `{"address", "name"}`, 400 `{"code": "invalid",
  "message", "problems": [{"field", "message"}]}`, 409 `{"code": "busy", "message"}` or 503
  `{"code": "unavailable", "message"}`. langalpha serves it as
  `PUT /api/v1/automations/delivery-default`: 400 `{"detail", "problems"}`, 409 and 503
  `{"detail"}`, and 404 without a messaging service. Both routes refuse a workspace that isn't
  the caller's.

A create or update refused for its delivery stays a 409 whose `detail` is the joined sentence,
and adds `problems: [{"entry", "message"}]`, one per refused entry.

## Adding a surface

One step, on the client: start sending the new name in `platform` and its rules in
`surface_rules`. It takes effect on the next turn, with no langalpha release in between. A name
langalpha does not recognize still renders in the pointer, because the client ships on its own
schedule and an unfamiliar name is version skew, not a bad turn; langalpha logs it once per
process at debug level, since the thing that hides is a typo.

Adding the name to `KNOWN_SURFACES` in
`src/ptc_agent/agent/middleware/runtime_context/surface.py` is optional and only silences that
log line. It does not give the surface any rules.

A surface that sends no `surface_rules` leaves the model with nothing to steer on, so it falls
back to its default behavior, which is the `web` shape. A surface whose whole point is a
constraint (short replies, no widgets) therefore needs its paragraph before the traffic
matters.

## Where this lives in the code

| Piece | File |
|---|---|
| Fields and grammar | `src/server/models/chat.py` (`ChatRequest`), `src/server/models/conversation.py` (`ThreadCreateRequest`) |
| Per-turn resolution | `src/server/handlers/chat/request_prep.py` (`TurnRuntimeContext`) |
| Grammar and known set | `src/ptc_agent/agent/middleware/runtime_context/surface.py` |
| Pointer rendering | `runtime_context/turn.py` (`TurnContextMiddleware`), `templates/envelope/turn.md.j2` |
| Built-in rules prose, and where caller rules land | `templates/envelope/surface_rules.md.j2` |
| When the rules ride | `runtime_context/turn.py` (`rules_key`, `_rules_already_stated`) |
| Messaging tools, and the surface they name | `src/tools/messaging/tools.py`; `turn_surface` in `src/server/handlers/chat/request_prep.py` |
| Automation delivery | `src/server/services/automation_delivery.py`; its routes in `src/server/app/automations.py` |
