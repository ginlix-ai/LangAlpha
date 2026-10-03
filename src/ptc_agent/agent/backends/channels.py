"""ChannelsBackend: the user's chat-app settings as JSON files.

At `.agents/user/channels/` (``channels.json``, ``available.json``), only
when the channel gateway is configured. The gateway holds the settings, so
each read asks it and a save is one call carrying the version read; the file
plumbing is ``DbJsonRoute``'s and the files are ``services.channel_settings``'.
Only the file tools reach this folder: it is no mount tier, and Bash and code
are refused a command that names it.
"""

from __future__ import annotations

import contextlib

from ptc_agent.agent.backends.db_json_route import README_FILE, DbJsonRoute
from ptc_agent.core.paths import SandboxLayout
from src.server.services.channel_settings import (
    AVAILABLE_FILE,
    CHANNEL_FILES,
    CHANNELS_FILE,
)

__all__ = ["AVAILABLE_FILE", "CHANNELS_FILE", "README_FILE", "ChannelsBackend"]

_README_CONTENT = """\
# Channels

The user's chat-app settings, kept by the messaging service. Reach this folder
with Read, Write, Edit, Grep and Glob only; Bash and code can't.

- `channels.json`: the settings. Edit it to change them.
- `available.json`: read-only. The chats on each connected app, and the user's
  workspaces by id, which `channels.json` may name.

Read `channels.json` before you write it; a save over a change you haven't
seen is refused.

## channels.json

```json
{
  "default": {"mode": "ptc", "workspace_id": "9f2c…", "workspace": "Research"},
  "slack": {
    "preferred": "slack:T1/C0456",
    "chats": {
      "slack:T1/C0456": {"mode": "ptc", "workspace_id": "9f2c…", "workspace": "Research", "name": "#research"}
    },
    "automation_output": {"9f2c…": "slack:T1/C0123"},
    "agent_messages": {"enabled": true, "allowed": ["slack:T1/C0456"]}
  }
}
```

| Field | Meaning |
|-------|---------|
| `default` | The mode and workspace a chat runs in when it has no entry in its app's `chats`. |
| `<app>` | One key per connected app. Connecting or removing an app happens in the app, not here: keep every key. |
| `<app>.chats` | Per chat address: `mode` and `workspace_id`. Applies to new threads started in that chat; a thread already running keeps its workspace. |
| `<app>.preferred` | Where to send on this app when the user names only the app. `null` means the user's direct messages. A preferred chat counts as allowed. |
| `<app>.automation_output` | Per workspace id: the chat that automation results from that workspace go to on this app. |
| `<app>.agent_messages` | `enabled`: whether you may message the user on this app at all. `allowed`: the shared chats you may send to, besides the conversation you are in and the user's direct messages. |

`workspace` and `name` are labels, ignored on save. A workspace id you set
must be one of the user's workspaces in `available.json`. A `workspace` of
`null` means that workspace no longer exists: point the binding at another
one or remove it. Left as it is, it doesn't stop other changes saving.

An automation whose `delivery` names only an app posts to its workspace's
`automation_output` on that app, else to the app's `preferred`, else to the
user's direct messages. To always post to the direct messages, name them:
`discord:@me`, `imessage:@me`, or `slack:<team>` on Slack.

## Editing rules

- Change only what the user asked for. Never add an allowed chat or set a
  preferred chat on your own initiative.
- Take addresses from `available.json` or `list_message_targets`; never
  make one up.
- Removing a chat's entry removes its binding; removing an address from
  `allowed` takes away your reach there.
- The messaging service checks every save. A refused save is an error listing
  each problem, and nothing is saved: fix them all and save again.
"""


class ChannelsBackend(DbJsonRoute):
    """The channel gateway's settings for this user, as two JSON files."""

    directory = SandboxLayout.CHANNELS_DIR
    files = CHANNEL_FILES
    read_only_files = frozenset({AVAILABLE_FILE})
    readme_content = _README_CONTENT
    mountable = False

    source = "channels_backend"
    read_failure = "Failed to read channel settings"
    read_only = "Channel settings are read-only here. Ask the agent to change them."
    undeletable = "Channel settings cannot be deleted."

    @staticmethod
    def _transaction() -> contextlib.nullcontext[None]:
        # The gateway checks the version as it saves, so a save holds no
        # database connection across the call to it.
        return contextlib.nullcontext()
