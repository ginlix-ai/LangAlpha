"""Each of the user's automations as one JSON file, and a save of it as a row change.

Serves ``AutomationsBackend`` (`.agents/user/automations/<file_name>`). A file
is one object: the automation's definition, which the agent may edit, then a
read-only ``state`` block the server keeps (the automation's id, next cron
run, newest run, strikes). The file name is a reference the agent picks,
stored in ``automations.file_name`` and unique per user; ``name`` is what the
user sees.

A save is planned against the one row its file name holds: a name no row has
creates an automation, and a changed file updates it (``status`` pauses,
resumes or deletes it). The file's own shape is checked first
(``AutomationFile.parse``), before the save reads or locks anything. The plan
then checks the guards against an agent's slips (a past or date-only time, a
fixed-offset zone) and what only the file can see, such as a completed
automation keeping its schedule; every rule an automation obeys anywhere is
``lifecycle``'s, which the commit runs. The report names the change, because
the agent cannot see the row it touched any other way.

The version is a hash of the id and definition only. ``state`` moves with
every firing and every scheduler poll, and hashing it would refuse most writes.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal
from uuid import UUID

from fastapi import HTTPException
from psycopg.errors import DataError, IntegrityError, UniqueViolation
from pydantic import BaseModel, ConfigDict, ValidationError, ValidationInfo, field_validator

from ptc_agent.agent.backends.db_json_route import (
    DbJsonFile,
    ErrorType,
    Plan,
    UnreadableJsonError,
    UserDataValidationError,
    load_json,
)
from ptc_agent.core.paths import AUTOMATION_FILE_NAME
from ptc_agent.core.sandbox.livefs_mount import CallContext
from src.server.database import automation as auto_db
from src.server.models.automation import (
    SCHEDULE_FIELD,
    AutomationCreate,
    AutomationUpdate,
    MarketType,
    PriceTriggerConfig,
    future_run,
    in_own_thread,
    on_clock,
    parse_delivery,
    region_zone,
    run_time,
    thread_fields,
)
from src.server.services.automations import lifecycle
from src.server.services.llm import user_models
from src.server.services.price_monitor import watched_market
from src.utils.timezone_utils import zone_or_none

logger = logging.getLogger(__name__)

_FILE_NAME = re.compile(AUTOMATION_FILE_NAME)

# The rule AUTOMATION_FILE_NAME states, as a refusal of another name says it.
NAME_RULE = (
    "an automation's file name is 1 to 64 letters, digits, - or _, starting with a "
    "letter or digit, then .json (e.g. morning-brief.json)"
)

# The fields a file may carry, in the order they are written.
_EDITABLE = (
    "name",
    "description",
    "status",
    "trigger_type",
    "cron_expression",
    "next_run_at",
    "trigger_config",
    "timezone",
    "instruction",
    "agent_mode",
    "workspace_id",
    "thread",
    "llm_model",
    "delivery",
    "max_failures",
)

# Model field → the file field a refusal of it should point at.
_FILE_FIELD = {
    "thread_strategy": "thread",
    "conversation_thread_id": "thread",
    "delivery_config": "delivery",
}

_ERROR_EXCERPT = 300
_MAX_PROBLEMS = 10


def is_file_name(name: str) -> bool:
    return _FILE_NAME.fullmatch(name) is not None


# =============================================================================
# Errors
# =============================================================================


@dataclass
class AutomationFileError(UserDataValidationError):
    """A refused write. Nothing was saved; ``message`` goes to the agent verbatim."""


def _refuse(file_name: str, hint: str, error_type: ErrorType = "schema_error") -> AutomationFileError:
    return AutomationFileError(error_type=error_type, file=file_name, field_path="", hint=hint)


# =============================================================================
# Serialization
# =============================================================================


def _zone(name: str | None):
    return zone_or_none(name or "UTC") or UTC


def _iso(value: datetime | str | None, tz_name: str | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    return on_clock(value, tz_name)


def _thread_value(row: dict[str, Any]) -> str:
    # A persistent automation pins the thread its first run creates, and
    # still reads "persistent" after that; only a thread it was pointed at
    # shows as an id.
    if row.get("thread_strategy") != "continue":
        return "new"
    return "persistent" if in_own_thread(row) else str(row["conversation_thread_id"])


def _definition(row: dict[str, Any]) -> dict[str, Any]:
    """The editable part of a row, as the file shows it."""
    kind = row["trigger_type"]
    tz = row.get("timezone") or "UTC"
    entry: dict[str, Any] = {
        "name": row["name"],
        "description": row.get("description"),
        "status": row["status"],
        "trigger_type": kind,
    }
    if kind == "cron":
        entry["cron_expression"] = row.get("cron_expression")
    elif kind == "once":
        entry["next_run_at"] = _iso(row.get("next_run_at"), tz)
    else:
        entry["trigger_config"] = row.get("trigger_config") or {}
    workspace_id = row.get("workspace_id")
    entry.update(
        timezone=tz,
        instruction=row["instruction"],
        agent_mode=row["agent_mode"],
        workspace_id=str(workspace_id) if workspace_id else None,
        thread=_thread_value(row),
        llm_model=row.get("llm_model"),
        delivery=[
            m
            for m in (row.get("delivery_config") or {}).get("methods") or []
            if not isinstance(m, str) or m.strip()
        ],
        max_failures=row["max_failures"],
    )
    return entry


def _state(row: dict[str, Any]) -> dict[str, Any]:
    """What the server keeps about the automation; read-only in the file."""
    tz = row.get("timezone")
    state: dict[str, Any] = {"automation_id": str(row["automation_id"])}
    if row["trigger_type"] == "cron" and row.get("next_run_at"):
        state["next_run_at"] = _iso(row["next_run_at"], tz)
    last = row.get("last_execution")
    if last:
        run: dict[str, Any] = {
            "status": last.get("status"),
            "at": _iso(
                last.get("completed_at") or last.get("started_at") or last.get("scheduled_at"),
                tz,
            ),
        }
        if last.get("conversation_thread_id"):
            run["thread_id"] = str(last["conversation_thread_id"])
        if last.get("excerpt"):
            run["excerpt"] = last["excerpt"]
        for key in ("failure_reason", "skip_reason"):
            if last.get(key):
                run[key] = last[key]
        error = last.get("error_message")
        if error:
            run["error"] = error if len(error) <= _ERROR_EXCERPT else f"{error[:_ERROR_EXCERPT]}…"
        if last.get("dismissed_at"):
            run["dismissed"] = True
        state["last_run"] = run
    if row.get("failure_count"):
        state["failure_count"] = row["failure_count"]
    if row.get("disable_reason"):
        state["disable_reason"] = row["disable_reason"]
    return state


def _version(row: dict[str, Any]) -> str:
    # The id too, so a file deleted and made again with the same content
    # is not the one a writer read.
    definition = {"automation_id": str(row["automation_id"]), **_definition(row)}
    blob = json.dumps(definition, sort_keys=True, ensure_ascii=False, default=str)
    return "sha256:" + hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _render(row: dict[str, Any]) -> tuple[str, str]:
    content = json.dumps({**_definition(row), "state": _state(row)}, indent=2, ensure_ascii=False) + "\n"
    return content, _version(row)


async def rendered_files(user_id: str) -> dict[str, tuple[str, str]]:
    """Every automation's file, by file name: its content and version."""
    return {row["file_name"]: _render(row) for row in await auto_db.list_all_automations(user_id)}


async def file_names(user_id: str) -> list[str]:
    return await auto_db.list_automation_file_names(user_id)


# =============================================================================
# Planning a write
# =============================================================================


def _not_null(value: Any) -> Any:
    if value is None:
        raise ValueError("can't be null")
    return value


class _Entry(BaseModel):
    """A file as written. Only the file's own shape is checked here: the
    automation's rules run where every surface's do, in ``lifecycle``."""

    model_config = ConfigDict(extra="forbid")

    name: str | None = None
    description: str | None = None
    status: str | None = None
    trigger_type: Literal["cron", "once", "price"] | None = None
    cron_expression: str | None = None
    next_run_at: datetime | None = None
    trigger_config: dict[str, Any] | None = None
    timezone: str | None = None
    instruction: str | None = None
    agent_mode: Literal["ptc", "flash"] | None = None
    workspace_id: UUID | None = None
    thread: dict[str, Any] | None = None
    llm_model: str | None = None
    delivery: dict[str, list[str]] | None = None
    max_failures: int | None = None
    state: Any = None

    # Fields every automation has: null is refused rather than read as
    # "clear". A schedule field's null depends on the kind, so the plan reads it.
    @field_validator("name", "status", "trigger_type", "timezone", "instruction", "agent_mode", "max_failures", mode="before")
    @classmethod
    def _required_value(cls, value: Any) -> Any:
        return _not_null(value)

    @field_validator("timezone")
    @classmethod
    def _written_zone(cls, value: str | None, info: ValidationInfo) -> str | None:
        # Only a zone the agent writes is held to region zones; the one the
        # row already has passes, whichever surface stored it.
        stored = (info.context or {}).get("zone")
        return value if value is None or value == stored else region_zone(value)

    @field_validator("thread", mode="before")
    @classmethod
    def _thread(cls, value: Any, info: ValidationInfo) -> Any:
        return thread_fields(_not_null(value), (info.context or {}).get("thread_id"))

    @field_validator("delivery", mode="before")
    @classmethod
    def _delivery(cls, value: Any) -> Any:
        return parse_delivery(_not_null(value))

    @field_validator("next_run_at", mode="before")
    @classmethod
    def _time(cls, value: Any) -> Any:
        # Naive when it names no offset: the plan reads it on the automation's clock.
        if value is None:
            return None
        if not isinstance(value, str):
            raise ValueError("must be an ISO datetime string, e.g. 2026-10-01T09:00:00")
        return run_time(value)


_ALLOWED = ", ".join(_Entry.model_fields)


@dataclass
class _Create:
    data: AutomationCreate
    pause: bool


@dataclass
class _Update:
    automation_id: str
    name: str
    fields: dict[str, Any]
    changed: list[str]
    action: str | None  # "pause" | "resume"
    # The row as the save's check read it, which the lifecycle writes over
    # rather than reading it again.
    row: dict[str, Any]


@dataclass
class _Delete:
    automation_id: str
    name: str


@dataclass
class FilePlan:
    """What a save of one file changes: at most one of the three."""

    create: _Create | None = None
    update: _Update | None = None
    delete: _Delete | None = None
    notes: list[str] = field(default_factory=list)
    # What the lifecycle checks a named model against; None reads it there.
    model_pref: dict[str, Any] | None = None

    def __bool__(self) -> bool:
        return bool(self.create or self.update or self.delete)


class _Problems:
    """Refusals gathered across the whole file, so one retry can fix all."""

    def __init__(self, file_name: str) -> None:
        self.file_name = file_name
        self.items: list[tuple[str, str]] = []

    def add(self, path: str, msg: str) -> None:
        if len(self.items) < _MAX_PROBLEMS:
            self.items.append((path, msg))

    def add_validation(self, exc: ValidationError) -> None:
        for err in exc.errors(include_url=False):
            loc = [str(p) for p in err["loc"]]
            if loc:
                loc[0] = _FILE_FIELD.get(loc[0], loc[0])
            if err["type"] == "extra_forbidden":
                msg = f"unknown field; allowed: {_ALLOWED}"
            else:
                msg = err["msg"].removeprefix("Value error, ")
            self.add(".".join(loc), msg)

    def raise_if_any(self) -> None:
        if self.items:
            first_path, first_msg = self.items[0]
            raise AutomationFileError(
                error_type="schema_error",
                file=self.file_name,
                field_path=first_path,
                hint=first_msg,
                problems=list(self.items),
            )


class _DuplicateKey(Exception):
    pass


def _unique_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    # json keeps the last of a repeated key, so an Edit that adds "status"
    # above the one already there would be dropped without a word.
    seen: set[str] = set()
    for key, _ in pairs:
        if key in seen:
            name = dict(pairs).get("name")
            where = f' in "{name}"' if isinstance(name, str) else ""
            raise _DuplicateKey(f'"{key}" appears twice{where}; keep one')
        seen.add(key)
    return dict(pairs)


def _parse_object(content: str, file_name: str) -> dict[str, Any]:
    try:
        written = load_json(content, object_pairs_hook=_unique_keys)
    except json.JSONDecodeError as exc:
        raise _refuse(file_name, f"invalid JSON at line {exc.lineno}, column {exc.colno}: {exc.msg}", "parse_error")
    except _DuplicateKey as exc:
        raise _refuse(file_name, str(exc), "parse_error")
    except UnreadableJsonError as exc:
        raise _refuse(file_name, f"invalid JSON: {exc}", "parse_error")
    if not isinstance(written, dict):
        raise _refuse(file_name, "the file must be one JSON object: one automation's fields")
    return written


def _shown(served_content: str) -> dict[str, Any]:
    """The file as the agent was shown it."""
    try:
        shown = json.loads(served_content)
    except ValueError:
        return {}
    return shown if isinstance(shown, dict) else {}


def _same(written: Any, stored: Any) -> bool:
    """Whether a written value is the stored one in another form ("3" for 3)."""
    if written is None or stored is None:
        return written is None and stored is None
    if isinstance(written, dict) or isinstance(stored, dict):
        return json.dumps(written, sort_keys=True, default=str) == json.dumps(stored, sort_keys=True, default=str)
    return str(written) == str(stored)


_RAN_ONCE = "already ran and is completed; it won't run on its own again. Write a new file for another run."


# The status changes a writer may ask for, as the lifecycle call that makes
# each. "deleted" is the other, from any status.
_STATUS_ACTIONS = {
    ("active", "paused"): "pause",
    ("paused", "active"): "resume",
    ("disabled", "active"): "resume",
}


def _status_problem(current: str, value: Any) -> str:
    if current == "completed":
        return f"this automation {_RAN_ONCE}"
    return (
        f'can\'t go from "{current}" to {json.dumps(value)}: set "paused" to pause an active '
        'automation, "active" to resume a paused or disabled one, or "deleted" to delete it. '
        "Other statuses are set by the server."
    )


@dataclass
class Document:
    """A written file, its fields not yet checked (the plan checks them
    against the row), and what the save read about the user for it."""

    fields: dict[str, Any]
    # The file as the writer was shown it; None when it saw the live file,
    # whose row the plan reads its ``state`` from.
    shown: dict[str, Any] | None
    # A new automation's clock when the file names none.
    timezone: str
    # What the lifecycle checks a named model against; None when the file
    # names no model it would check.
    model_pref: dict[str, Any] | None
    # The delivery with each chat it newly names as the messaging service
    # files it; None when it names none to check. ``delivery_problems`` are
    # the chats it refused.
    delivery: list[str] | None = None
    delivery_problems: list[str] = field(default_factory=list)


def _names_model(fields: dict[str, Any], shown: dict[str, Any] | None) -> bool:
    model = fields.get("llm_model")
    if not model:
        return False
    # The model the writer was shown is the stored one, or the version check
    # refuses the save; resent as it was, it isn't checked.
    return shown is None or shown.get("llm_model") != model


def _plan_create(entry: _Entry, call: CallContext, zone: str, problems: _Problems) -> _Create | None:
    written = entry.model_fields_set
    kind = entry.trigger_type or next(
        (k for k, f in SCHEDULE_FIELD.items() if getattr(entry, f) is not None), None
    )
    status = entry.status or "active"
    if status not in ("active", "paused"):
        problems.add("status", 'a new automation starts "active" or "paused"')
    if kind is None:
        problems.add(
            "trigger_type",
            'required: "cron" (with cron_expression), "once" (with next_run_at) or "price" (with trigger_config)',
        )
        return None

    # The defaults of the old automation tool, so an automation made through
    # a file matches one it made: this workspace, the user's clock, a PTC run.
    tz = entry.timezone or zone
    data: dict[str, Any] = {
        "trigger_type": kind,
        "timezone": tz,
        "agent_mode": entry.agent_mode or "ptc",
        "workspace_id": entry.workspace_id if "workspace_id" in written else call.workspace_id,
        **(entry.thread or {}),
    }
    for key in ("name", "description", "instruction", "llm_model", "max_failures"):
        if key in written:
            data[key] = getattr(entry, key)
    if "delivery" in written:
        data["delivery_config"] = entry.delivery
    for other_kind, kind_field in SCHEDULE_FIELD.items():
        value = getattr(entry, kind_field)
        # Another kind's field left null reads as absent, as on an update.
        if kind_field in written and (other_kind == kind or value is not None):
            data[kind_field] = value
    when = data.get("next_run_at")
    if when is not None and when.tzinfo is None:
        data["next_run_at"] = when.replace(tzinfo=_zone(tz))

    try:
        create = AutomationCreate.model_validate(data)
        if create.next_run_at is not None:
            future_run(create.next_run_at, tz)
    except ValidationError as exc:
        problems.add_validation(exc)
        return None
    except ValueError as exc:
        problems.add("next_run_at", str(exc))
        return None
    return _Create(data=create, pause=status == "paused")


def _plan_update(entry: _Entry, raw: dict[str, Any], row: dict[str, Any], problems: _Problems) -> _Update | None:
    """The fields that differ from the row, and the pause or resume a status
    asks for; what the row allows beyond that is the lifecycle's to say."""
    current = _definition(row)
    kind = row["trigger_type"]
    tz = entry.timezone or current["timezone"]
    fields: dict[str, Any] = {}
    changed: list[str] = []
    action: str | None = None

    for key in _EDITABLE:
        if key not in entry.model_fields_set:
            continue  # an omitted field keeps its value
        value = getattr(entry, key)
        if value is None and key in SCHEDULE_FIELD.values():
            if key != SCHEDULE_FIELD[kind]:
                continue  # another kind's schedule field, left null, is absent
            if current.get(key) is None:
                # A one-time automation claimed or completed shows null, and
                # an edit to another field writes it back as it read it.
                continue
            problems.add(
                key,
                'a one-time automation needs a time; set status "paused" to hold it instead'
                if kind == "once"
                else "can't be null",
            )
            continue
        if key == "status":
            if value == current["status"]:
                continue
            action = _STATUS_ACTIONS.get((current["status"], value))
            if action is None:
                problems.add(key, _status_problem(current["status"], value))
            continue
        if key == "next_run_at":
            wanted = value if value.tzinfo else value.replace(tzinfo=_zone(tz))
            # Compared with the time as the file shows it, to the second.
            if current.get("next_run_at") and wanted == datetime.fromisoformat(current["next_run_at"]):
                continue
            if current["status"] == "completed":
                problems.add(key, f"this automation {_RAN_ONCE}")
                continue
            try:
                fields[key] = future_run(wanted, tz)
            except ValueError as exc:
                problems.add(key, str(exc))
                continue
            changed.append(key)
            continue
        if key == "thread":
            if raw[key] == current["thread"]:
                continue
            pinned = row.get("conversation_thread_id")
            stored = (row.get("thread_strategy"), str(pinned) if pinned else None)
            if (value["thread_strategy"], value["conversation_thread_id"]) != stored:
                fields.update(value)
                changed.append(key)
            continue
        if key == "delivery":
            if value["methods"] != current["delivery"]:
                fields["delivery_config"] = value
                changed.append(key)
            continue
        if _same(value, current.get(key)):
            continue
        if key == "trigger_config" and current["status"] == "completed":
            problems.add(key, f"this automation {_RAN_ONCE}")
            continue
        if key == "workspace_id" and value is None:
            problems.add(key, "can't be cleared; point it at another workspace instead")
            continue
        fields[key] = value
        changed.append(key)

    if fields:
        # Checked again by the lifecycle under the lock; checked here too so
        # one refusal lists every field's problems, as a create's does.
        try:
            AutomationUpdate.model_validate(fields, context={"trigger_type": kind})
        except ValidationError as exc:
            problems.add_validation(exc)
            return None
    if not fields and action is None:
        return None
    return _Update(
        automation_id=str(row["automation_id"]),
        name=entry.name or current["name"],
        fields=fields,
        changed=changed,
        action=action,
        row=row,
    )


# =============================================================================
# Committing a write
# =============================================================================


def _next_run(row: dict[str, Any] | None) -> str:
    # Only an active automation runs; a paused one keeps its next_run_at.
    if not row or row.get("status") != "active":
        return ""
    if row["trigger_type"] == "price":
        # Named with the feed it is watched on, so a symbol read as a US
        # stock doesn't pass for the crypto or foreign listing meant.
        try:
            config = PriceTriggerConfig(**(row.get("trigger_config") or {}))
        except (ValidationError, TypeError):
            return ""
        return f"; watching {config.symbol} as {_watched_as(config)}"
    when = _iso(row.get("next_run_at"), row.get("timezone"))
    return f"; next run {when}" if when else ""


def _watched_as(config: PriceTriggerConfig) -> str:
    """``a CN stock``, ``an HK index``: the market is the monitor's own reading."""
    kind = "index" if config.market == MarketType.INDEX else "stock"
    market = watched_market(config.symbol, config.market) or "other"
    if market == "other":
        return f"{'an' if kind == 'index' else 'a'} {kind} on an unknown venue"
    label = market.upper()
    # The article follows the first letter's spoken name: an HK, a US.
    return f"{'an' if label[0] in 'AEFHILMNORSX' else 'a'} {label} {kind}"


def _at(model_field: str | None) -> str:
    return _FILE_FIELD.get(model_field, model_field) if model_field else ""


_REFUSED = object()


def _taken(file_name: str) -> AutomationFileError:
    # The user's lock keeps the agent's own saves apart, but a create on the
    # Automations page takes a name without it.
    return _refuse(file_name, "another automation took this file name meanwhile; use another name", "exists")


# =============================================================================
# The file
# =============================================================================


class AutomationFile(DbJsonFile[dict[str, Any] | None, Document, FilePlan]):
    """The automation filed as ``file_name``, or the one a save there makes."""

    def __init__(self, file_name: str) -> None:
        self.file_name = file_name
        self.unchanged = self._no_changes([])

    def _no_changes(self, notes: list[str]) -> str:
        """The report of a save that changed nothing."""
        return "\n".join([f"No changes: {self.file_name} already matches the saved automation", *notes])

    async def fetch(self, user_id: str, conn: Any = None) -> dict[str, Any] | None:
        return await auto_db.get_automation_file(user_id, self.file_name, conn=conn)

    def render(self, row: dict[str, Any] | None) -> tuple[str, str] | None:
        return _render(row) if row else None

    async def lock(self, user_id: str, conn: Any) -> None:
        await auto_db.lock_user_automations(user_id, conn=conn)

    async def parse(self, user_id: str, call: CallContext, content: str, served: str | None) -> Document:
        """The object ``content`` holds. Content that is no object is refused
        here, before the save reads or locks anything; its fields are checked
        by the plan, which lists their problems with the ones the row shows,
        so one retry can fix all. ``served`` is the file the writer read,
        whose ``state`` the write may repeat but not change; None compares it
        with the live row.

        What the save reads about the user, it reads only when the file as
        written may need it: a create's default clock, or a model it names.
        """
        written = _parse_object(content, self.file_name)
        shown = _shown(served) if served is not None else None
        # The conversation's clock first, else the user's stored zone, else
        # UTC. A writer that read the file is updating it, so needs neither.
        zone = call.timezone or "UTC"
        if not call.timezone and served is None and written.get("timezone") is None:
            zone = await auto_db.get_user_timezone(user_id) or "UTC"
        delivery, refused = await self._check_delivery(user_id, written, shown)
        return Document(
            fields=written,
            shown=shown,
            timezone=zone,
            model_pref=await user_models.get_model_preference(user_id) if _names_model(written, shown) else None,
            delivery=delivery,
            delivery_problems=refused,
        )

    async def _check_delivery(
        self, user_id: str, written: dict[str, Any], shown: dict[str, Any] | None
    ) -> tuple[list[str] | None, list[str]]:
        """The written delivery with each chat it names checked, here rather
        than in the lifecycle, since a check is a call to the messaging
        service the save's locks would wait on. A chat the writer was shown
        is the stored one and isn't checked again."""
        try:
            methods = parse_delivery(written.get("delivery") or [])["methods"]
        except ValueError:
            return None, []  # the plan refuses the shape
        if not any(lifecycle.names_chat(m) for m in methods):
            return None, []
        if shown is not None:
            stored = shown.get("delivery")
        else:
            row = await auto_db.get_automation_file(user_id, self.file_name)
            stored = _definition(row)["delivery"] if row else []
        if not isinstance(stored, list):
            stored = []
        stored = [m for m in stored if isinstance(m, str)]
        try:
            return await lifecycle.check_delivery(user_id, methods, stored=stored), []
        except lifecycle.DeliveryRefused as exc:
            return None, exc.problems

    def plan(self, call: CallContext, document: Document, row: dict[str, Any] | None) -> Plan[FilePlan]:
        """The change a save of ``document`` over ``row`` makes. Raises
        ``AutomationFileError`` with every problem it finds."""
        problems = _Problems(self.file_name)
        raw = document.fields
        result = FilePlan(model_pref=document.model_pref)
        for refused in document.delivery_problems:
            problems.add("delivery", refused)
        try:
            entry = _Entry.model_validate(
                raw, context={"thread_id": call.thread_id, "zone": row.get("timezone") if row else None}
            )
        except ValidationError as exc:
            problems.add_validation(exc)
            problems.raise_if_any()
        if document.delivery is not None:
            checked = {"methods": document.delivery}
            entry = entry.model_copy(update={"delivery": checked})
        shown_state = (
            document.shown.get("state") if document.shown is not None else _state(row) if row else None
        )
        if "state" in raw and (raw["state"] or {}) != (shown_state or {}):
            # Ignored, not refused: models "fix" state.next_run_at alongside a
            # schedule change, and a refusal cost a whole retry.
            result.notes.append("- note: state is kept by the server, so what you wrote in it was ignored")
        if entry.status == "deleted":
            if row is None:
                problems.add("status", f'"deleted" deletes an existing automation, and none is filed as {self.file_name}')
            else:
                result.delete = _Delete(automation_id=str(row["automation_id"]), name=row["name"])
        elif row is None:
            result.create = _plan_create(entry, call, document.timezone, problems)
        else:
            result.update = _plan_update(entry, raw, row, problems)
        problems.raise_if_any()
        return Plan(result)

    def plan_delete(self, row: dict[str, Any] | None) -> Plan[FilePlan]:
        return Plan(FilePlan(delete=_Delete(automation_id=str(row["automation_id"]), name=row["name"])))

    async def hold(self, user_id: str, plan: FilePlan, row: dict[str, Any] | None, conn: Any) -> str | None:
        """Lock the row before a save writes over it: its version as it
        stands locked, which the caller holds to the version it checked,
        since an edit on the Automations page may have landed after its
        read. None for a create, which the user's lock and the unique file
        name keep apart, and for a save with no changes."""
        if not (plan.update or plan.delete):
            return None
        locked = await auto_db.get_automation_file(user_id, self.file_name, conn=conn, lock=True)
        return _version(locked) if locked else ""

    async def commit(self, user_id: str, plan: FilePlan, conn: Any) -> str:
        """Apply ``plan`` in ``conn``'s transaction and report what changed.

        Each lifecycle call runs under its own savepoint, and its refusal is
        gathered as a problem rather than raised mid-save; any refusal raises
        ``AutomationFileError`` at the end, which rolls the whole save back.
        The lifecycle writes over the row the plan read, which ``hold``
        locked unchanged, rather than reading it again.
        """
        if not plan:
            return self._no_changes(plan.notes)

        problems = _Problems(self.file_name)
        report = ""
        warning: str | None = None

        async def step(change: Callable[..., Awaitable[Any]], *args: Any, **kwargs: Any) -> Any:
            try:
                async with conn.transaction():
                    return await change(*args, conn=conn, **kwargs)
            except UniqueViolation as exc:
                raise _taken(self.file_name) from exc
            except ValidationError as exc:
                problems.add_validation(exc)
            except lifecycle.AutomationRefusal as exc:
                problems.add(_at(exc.field), str(exc))
            except HTTPException as exc:
                detail = {404: "not found", 403: "belongs to another user"}.get(exc.status_code, str(exc.detail))
                problems.add(_at(getattr(exc, "field", None)), detail)
            except ValueError as exc:
                problems.add("", str(exc))
            except DataError as exc:
                # A value the column cannot hold: the content's fault, not an outage.
                problems.add("", f"a value is too long or out of range for its column ({type(exc).__name__})")
            except IntegrityError as exc:
                # A value a constraint refuses (a check, a required column, a
                # row it names that is gone), told by the constraint's name:
                # the error's text quotes the row.
                rule = exc.diag.constraint_name
                logger.warning(
                    "automation file save refused by a constraint: %s sqlstate=%s constraint=%s column=%s",
                    type(exc).__name__,
                    exc.sqlstate,
                    rule,
                    exc.diag.column_name,
                )
                named = f"the {rule} rule" if rule else "a rule"
                problems.add("", f"a value breaks {named} on its column ({type(exc).__name__})")
            return _REFUSED

        if delete := plan.delete:
            await auto_db.delete_automation(delete.automation_id, user_id, conn=conn)
            report = f'Deleted "{delete.name}" ({self.file_name}), with its run history'

        if update := plan.update:
            row = update.row
            if update.fields:
                row = await step(
                    lifecycle.update_automation,
                    update.automation_id,
                    user_id,
                    update.fields,
                    model_pref=plan.model_pref,
                    current=row,
                    delivery_checked=True,
                )
            if update.action and row is not _REFUSED:
                control = lifecycle.pause_automation if update.action == "pause" else lifecycle.resume_automation
                row = await step(control, update.automation_id, user_id, current=row)
            if row is not _REFUSED:
                parts = [", ".join(update.changed)] if update.changed else []
                if update.action:
                    parts.append("paused" if update.action == "pause" else "resumed")
                report = f'Saved {self.file_name}: updated "{update.name}": {"; ".join(parts)}{_next_run(row)}'
                warning = lifecycle.delivery_warning((update.fields.get("delivery_config") or {}).get("methods"))

        if create := plan.create:
            row = await step(
                lifecycle.create_automation,
                user_id,
                create.data,
                model_pref=plan.model_pref,
                file_name=self.file_name,
                delivery_checked=True,
            )
            if row is not _REFUSED and create.pause:
                row = await step(lifecycle.pause_automation, str(row["automation_id"]), user_id, current=row)
                if row is not _REFUSED:
                    report = f'Saved {self.file_name}: created "{row["name"]}", paused'
            elif row is not _REFUSED:
                report = f'Saved {self.file_name}: created "{row["name"]}"{_next_run(row)}'
            delivery = create.data.delivery_config
            warning = lifecycle.delivery_warning(delivery.methods if delivery else None)

        problems.raise_if_any()
        lines = [report]
        if warning:
            lines.append(f"Note: {warning}")
        return "\n".join([*lines, *plan.notes])

    async def rename(self, user_id: str, to: str, conn: Any) -> str | None:
        # The row is locked, since a delete on the Automations page takes no
        # user lock: a rename that lost to one moved nothing.
        row = await auto_db.get_automation_file(user_id, self.file_name, conn=conn, lock=True)
        if row is None:
            return None
        try:
            moved = await auto_db.rename_automation_file(str(row["automation_id"]), user_id, to, conn=conn)
        except UniqueViolation as exc:
            raise _taken(to) from exc
        return f"Renamed {self.file_name} to {to}" if moved else None
