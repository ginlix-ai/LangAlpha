"""The user's chat-app settings as two JSON files, kept by the channel gateway.

Served by ``ChannelsBackend`` (`.agents/user/channels/`). ``channels.json`` is
the settings as the gateway answers them, with each bound workspace named from
this server's own rows; a save is one call to the gateway carrying the version
the writer read, which the gateway checks again as it stores. ``available.json``
is what the settings may name: the chats on each app and the user's
workspaces. Neither file holds a database connection across a gateway call.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from typing import Any, Literal, NamedTuple

import httpx
from pydantic import BaseModel, ConfigDict, ValidationError

from ptc_agent.agent.backends.db_json_route import (
    DbJsonFile,
    Plan,
    ReadUnavailable,
    StaleVersion,
    UserDataValidationError,
)
from ptc_agent.core.sandbox.livefs_mount import CallContext
from src.server.database import workspace as workspace_db
from src.server.services.user_data_io import parse_json, serialize_json
from src.server.utils.pg_sanitize import normalize_uuid
from src.tools.messaging import tools as messaging

logger = logging.getLogger(__name__)

CHANNELS_FILE = "channels.json"
AVAILABLE_FILE = "available.json"

_READ_TIMEOUT = httpx.Timeout(20.0, connect=5.0)
# Listing the chats asks each linked app for its own, so it gets as long as a save.
_AVAILABLE_TIMEOUT = httpx.Timeout(30.0, connect=5.0)
# A save may check each newly named chat with its app before answering.
_SAVE_TIMEOUT = httpx.Timeout(30.0, connect=5.0)

_RETRY = "Retry once. If it fails again, stop and tell the user."


# --- the settings as a writer may send them ---


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class _Binding(_Strict):
    mode: Literal["ptc", "flash"]
    workspace_id: str | None = None
    # Labels for the reader, ignored on input: the workspace's name, which
    # this server fills, and the chat's, which the gateway keeps.
    workspace: Any = None
    name: Any = None


class _AgentMessages(_Strict):
    enabled: bool
    allowed: list[str]


class _App(_Strict):
    preferred: str | None
    chats: dict[str, _Binding]
    automation_output: dict[str, str]
    agent_messages: _AgentMessages


_DEFAULT = "default"
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def _field(*parts: Any) -> str:
    """``slack.chats["slack:T1/C2"].mode`` for the parts of a field's path."""
    out = ""
    for part in parts:
        if isinstance(part, int):
            out += f"[{part}]"
        elif _IDENTIFIER.fullmatch(str(part)):
            out += f".{part}" if out else str(part)
        else:
            out += f"[{json.dumps(str(part), ensure_ascii=False)}]"
    return out


def _problem_message(error: Any) -> str:
    kind = error.get("type")
    if kind == "missing":
        return "missing"
    if kind == "extra_forbidden":
        return "unknown field"
    if kind in ("model_type", "dict_type"):
        return "must be a JSON object"
    if kind == "list_type":
        return "must be a list"
    if kind == "string_type":
        return "must be a string"
    if kind == "bool_type":
        return "must be true or false"
    return str(error.get("msg") or "invalid")


def _refused(problems: list[tuple[str, str]]) -> UserDataValidationError:
    path, message = problems[0]
    return UserDataValidationError(
        error_type="schema_error",
        file=CHANNELS_FILE,
        field_path=path,
        hint=message,
        problems=problems,
    )


def _failed(hint: str) -> UserDataValidationError:
    return UserDataValidationError(
        error_type="server_error", file=CHANNELS_FILE, field_path="", hint=hint
    )


# --- shared by both files ---


async def _get(
    user_id: str, path: str, what: str, timeout: httpx.Timeout = _READ_TIMEOUT
) -> dict[str, Any]:
    """The gateway's answer to a GET, or ``ReadUnavailable`` saying why not."""
    try:
        answer = await messaging.gateway_request(
            "GET", path, user_id=user_id, timeout=timeout
        )
    except messaging.GatewayError as exc:
        raise ReadUnavailable(
            f"{what} can't be read right now: {exc.message} {_RETRY}"
        ) from exc
    if answer.status != 200 or answer.data is None:
        logger.warning("[channels] gateway answered %s on %s", answer.status, path)
        raise ReadUnavailable(
            f"{what} can't be read right now: the messaging service failed ({answer.status}). {_RETRY}"
        )
    return answer.data


class _GatewayFile[R, P, C](DbJsonFile[R, P, C]):
    """A file whose rows the gateway holds: a save takes no lock past the
    route's own, since the gateway checks the version as it stores."""

    async def lock(self, user_id: str, conn: Any) -> None:
        return None


# --- channels.json ---


class Snapshot(NamedTuple):
    version: str
    settings: dict[str, Any]
    # Name by id of each workspace the settings name that is still the user's.
    names: dict[str, str]


def _bindings(settings: dict[str, Any]) -> list[tuple[tuple[Any, ...], dict[str, Any]]]:
    """Each binding in ``settings`` with its field path: the default, then
    every app's chats."""
    out: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
    for key, value in settings.items():
        if key == _DEFAULT:
            if isinstance(value, dict):
                out.append(((key,), value))
        elif isinstance(value, dict) and isinstance(value.get("chats"), dict):
            out += [
                ((key, "chats", address), b)
                for address, b in value["chats"].items()
                if isinstance(b, dict)
            ]
    return out


def _workspace_ids(settings: dict[str, Any]) -> list[tuple[tuple[Any, ...], str]]:
    """Every workspace id ``settings`` names, with its field path."""
    out = [
        ((*path, "workspace_id"), binding["workspace_id"])
        for path, binding in _bindings(settings)
        if isinstance(binding.get("workspace_id"), str)
    ]
    for key, value in settings.items():
        if (
            key != _DEFAULT
            and isinstance(value, dict)
            and isinstance(value.get("automation_output"), dict)
        ):
            out += [
                ((key, "automation_output", ws), ws)
                for ws in value["automation_output"]
            ]
    return out


def _labelled(binding: dict[str, Any], names: dict[str, str]) -> dict[str, Any]:
    """``binding`` with its workspace's name beside its id, as this server
    knows it: null when the id is no longer one of the user's workspaces."""
    out: dict[str, Any] = {}
    for key, value in binding.items():
        if key == "workspace":
            continue
        out[key] = value
        if key == "workspace_id":
            out["workspace"] = names.get(normalize_uuid(value) or "") if value else None
    return out


def _render_settings(settings: dict[str, Any], names: dict[str, str]) -> dict[str, Any]:
    out = json.loads(json.dumps(settings))
    for path, _ in _bindings(out):
        parent = out
        for part in path[:-1]:
            parent = parent[part]
        parent[path[-1]] = _labelled(parent[path[-1]], names)
    return out


def _stored(models: dict[str, Any], names: dict[str, str]) -> dict[str, Any]:
    """Validated settings as the gateway takes them: chat names dropped,
    workspace ids canonical and each named."""

    def binding(b: _Binding) -> dict[str, Any]:
        out: dict[str, Any] = {"mode": b.mode, "workspace_id": None}
        if b.workspace_id is not None:
            ws = normalize_uuid(b.workspace_id) or b.workspace_id
            out.update(workspace_id=ws, workspace=names.get(ws))
        return out

    settings: dict[str, Any] = {}
    for key, model in models.items():
        if model is None or isinstance(model, _Binding):
            settings[key] = binding(model) if model else None
            continue
        settings[key] = {
            "preferred": model.preferred,
            "chats": {address: binding(b) for address, b in model.chats.items()},
            "automation_output": {
                (normalize_uuid(ws) or ws): address
                for ws, address in model.automation_output.items()
            },
            "agent_messages": {
                "enabled": model.agent_messages.enabled,
                "allowed": list(model.agent_messages.allowed),
            },
        }
    return settings


def _validate(value: dict[str, Any]) -> tuple[dict[str, Any], list[tuple[str, str]]]:
    """The models for ``value``'s keys, and every problem found."""
    models: dict[str, Any] = {}
    problems: list[tuple[str, str]] = []
    for key, raw in value.items():
        if key == _DEFAULT and raw is None:
            models[key] = None
            continue
        try:
            models[key] = (_Binding if key == _DEFAULT else _App).model_validate(raw)
        except ValidationError as exc:
            problems += [
                (_field(key, *e["loc"]), _problem_message(e)) for e in exc.errors()
            ]
    return models, problems


def _removed(before: dict[str, Any], after: dict[str, Any]) -> list[str]:
    """What a save of ``after`` over ``before`` deletes, named for a refusal."""
    out: list[str] = []
    for app, old in before.items():
        new = after.get(app)
        if app == _DEFAULT or not isinstance(old, dict) or not isinstance(new, dict):
            continue
        out += [
            f"{app} chat {a}" for a in old.get("chats") or {} if a not in new["chats"]
        ]
        out += [
            f"{app} automation output for workspace {ws}"
            for ws in old.get("automation_output") or {}
            if ws not in new["automation_output"]
        ]
        allowed = (old.get("agent_messages") or {}).get("allowed") or []
        out += [
            f"{app} allowed chat {a}"
            for a in allowed
            if a not in new["agent_messages"]["allowed"]
        ]
    return out


class ChannelsFile(
    _GatewayFile[Snapshot, dict[str, Any], tuple[str, dict[str, Any]] | None]
):
    """``channels.json``: one save is one PUT of the whole settings."""

    unchanged = f"No changes: {CHANNELS_FILE} already matches the saved settings"

    async def fetch(self, user_id: str, conn: Any = None) -> Snapshot:
        data = await _get(user_id, "/agent/settings", "Channel settings")
        version, settings = data.get("version"), data.get("settings")
        if not isinstance(version, str) or not isinstance(settings, dict):
            raise ReadUnavailable(
                f"Channel settings can't be read right now: the messaging service sent an answer "
                f"that could not be read. {_RETRY}"
            )
        names = await workspace_db.get_workspace_names(
            user_id, [ws for _, ws in _workspace_ids(settings)]
        )
        return Snapshot(version, settings, names)

    def render(self, rows: Snapshot) -> tuple[str, str]:
        return serialize_json(_render_settings(rows.settings, rows.names)), rows.version

    async def parse(
        self, user_id: str, call: CallContext, content: str, served: str | None
    ) -> dict[str, Any]:
        value = parse_json(content, CHANNELS_FILE)
        if not isinstance(value, dict):
            raise _refused([("", "the file must be one JSON object, as Read shows it")])
        models, problems = _validate(value)
        named = _workspace_ids(value) if not problems else []
        # Checked even when unchanged, so a save never carries an id that is
        # not one of the user's workspaces.
        names = (
            await workspace_db.get_workspace_names(user_id, [ws for _, ws in named])
            if named
            else {}
        )
        for path, ws in named:
            if normalize_uuid(ws) not in names:
                problems.append(
                    (
                        _field(*path),
                        f"{ws!r} is not one of your workspaces; use a workspace_id from {AVAILABLE_FILE}",
                    )
                )
        if problems:
            raise _refused(problems)
        return _stored(models, names)

    def plan(
        self, call: CallContext, parsed: dict[str, Any], rows: Snapshot
    ) -> Plan[tuple[str, dict[str, Any]] | None]:
        apps = sorted(k for k in rows.settings if k != _DEFAULT)
        listing = ", ".join(apps) or "no apps"
        problems = [
            (_field(key), f"not a connected app here; the connected apps are {listing}")
            for key in parsed
            if key not in rows.settings
        ]
        problems += [
            (_field(key), "missing; keep every key the file had")
            for key in rows.settings
            if key not in parsed
        ]
        if problems:
            raise _refused(problems)
        models, malformed = _validate(rows.settings)
        if not malformed and _stored(models, rows.names) == parsed:
            return Plan(None)
        return Plan((rows.version, parsed), _removed(rows.settings, parsed))

    async def commit(
        self, user_id: str, changes: tuple[str, dict[str, Any]] | None, conn: Any
    ) -> str | None:
        if changes is None:
            return self.unchanged
        version, settings = changes
        try:
            answer = await messaging.gateway_request(
                "PUT",
                "/agent/settings",
                user_id=user_id,
                timeout=_SAVE_TIMEOUT,
                body={"version": version, "settings": settings},
            )
        except messaging.GatewayError as exc:
            if exc.delivered_unknown:
                raise _failed(
                    f"{exc.message} The save may or may not have landed. Read {CHANNELS_FILE} again: "
                    f"if it shows your change, it saved; if not, retry once. If it fails again, stop and "
                    "tell the user."
                ) from exc
            raise _failed(f"{exc.message} Nothing was saved. {_RETRY}") from exc
        data = answer.data or {}
        if answer.status == 200:
            listed = [c for c in data.get("changes") or [] if isinstance(c, str) and c]
            if not listed:
                return self.unchanged
            return "\n".join([f"Saved {CHANNELS_FILE}:", *(f"- {c}" for c in listed)])
        if answer.status == 409:
            raise StaleVersion
        if answer.status == 400:
            problems = [
                (str(p.get("field") or ""), str(p.get("message") or "invalid"))
                for p in data.get("problems") or []
                if isinstance(p, dict)
            ]
            raise _refused(
                problems
                or [
                    (
                        "",
                        str(
                            data.get("message")
                            or "the messaging service refused the save"
                        ),
                    )
                ]
            )
        if answer.status == 503:
            raise _failed(
                f"the messaging service can't save channel settings right now; nothing was saved. {_RETRY}"
            )
        logger.warning(
            "[channels] gateway answered %s on a settings save", answer.status
        )
        raise _failed(
            f"the messaging service failed ({answer.status}); nothing was saved. {_RETRY}"
        )


# --- available.json ---


class Choices(NamedTuple):
    apps: dict[str, Any]
    workspaces: dict[str, str]


def _chat(chat: Any) -> dict[str, Any] | None:
    if not isinstance(chat, dict) or not isinstance(chat.get("address"), str):
        return None
    return {
        "address": chat["address"],
        "name": chat.get("name"),
        "kind": chat.get("kind"),
    }


class AvailableFile(_GatewayFile[Choices, None, None]):
    """``available.json``: read-only, so no save reaches it."""

    async def fetch(self, user_id: str, conn: Any = None) -> Choices:
        data = await _get(
            user_id,
            "/agent/settings/available",
            "The available chats",
            timeout=_AVAILABLE_TIMEOUT,
        )
        apps = data.get("apps")
        if not isinstance(apps, dict):
            raise ReadUnavailable(
                f"The available chats can't be read right now: the messaging service sent an answer "
                f"that could not be read. {_RETRY}"
            )
        return Choices(apps, await workspace_db.get_workspace_names(user_id))

    def render(self, rows: Choices) -> tuple[str, str]:
        apps = {}
        for app, listing in sorted(rows.apps.items()):
            listing = listing if isinstance(listing, dict) else {}
            apps[app] = {
                "chats": [
                    c for chat in listing.get("chats") or [] if (c := _chat(chat))
                ],
                "complete": listing.get("complete") is not False,
                "error": listing.get("error"),
            }
        payload = {
            "apps": apps,
            "workspaces": [
                {"workspace_id": ws, "name": name}
                for ws, name in rows.workspaces.items()
            ],
        }
        text = serialize_json(payload)
        return text, hashlib.sha256(text.encode()).hexdigest()[:32]


CHANNEL_FILES = {CHANNELS_FILE: ChannelsFile(), AVAILABLE_FILE: AvailableFile()}
