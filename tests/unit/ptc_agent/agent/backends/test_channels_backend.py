"""``ChannelsBackend``: the user's chat-app settings as files the gateway keeps.

The gateway is faked at the HTTP transport and the workspace names at the
database helper, so these pin langalpha's half of the contract: what a read
renders, what a save sends and when it sends nothing, how each gateway answer
reaches the writer, and that no save opens a database transaction.
"""

from __future__ import annotations

import copy
import json
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from ptc_agent.agent.backends import db_json_route
from ptc_agent.agent.backends.channels import ChannelsBackend
from ptc_agent.agent.backends.db_json_route import (
    ReadUnavailable,
    UserDataValidationError,
)
from ptc_agent.agent.filesystem_routes import IdentityGates, build_filesystem_backend
from ptc_agent.core.sandbox.livefs_mount import CallContext
from src.config import env
from src.server.services import channel_settings
from src.server.utils.pg_sanitize import normalize_uuid
from src.tools.messaging import tools as messaging

ROOT = "/home/workspace"
PREFIX = f"{ROOT}/.agents/user/channels/"
CHANNELS = f"{PREFIX}channels.json"
AVAILABLE = f"{PREFIX}available.json"
README = f"{PREFIX}README.md"
USER = "user-1"
GATEWAY = "http://gateway.test/api/prefix"

RESEARCH = "9f2c0000-0000-4000-8000-000000000001"
MACRO = "9f2c0000-0000-4000-8000-000000000002"
FLASH = "9f2c0000-0000-4000-8000-000000000003"
STRANGERS = "9f2c0000-0000-4000-8000-000000000004"
# One of the user's workspaces, since deleted.
GONE = "9f2c0000-0000-4000-8000-000000000005"
# The user's live workspaces as langalpha's own table names them.
NAMES = {RESEARCH: "Research", MACRO: "Macro"}

SETTINGS = {
    "default": {"mode": "ptc", "workspace_id": RESEARCH},
    "slack": {
        "preferred": "slack:T1/C0456",
        "chats": {
            "slack:T1/C0456": {
                "mode": "ptc",
                "workspace_id": RESEARCH,
                "workspace": "Old name",
                "name": "#research",
            }
        },
        "automation_output": {RESEARCH: "slack:T1/C0123"},
        "agent_messages": {"enabled": True, "allowed": ["slack:T1/C0456"]},
    },
}


@pytest.fixture
def gateway(monkeypatch):
    """The gateway: settings at ``version``, a PUT answered by ``put`` (a
    response, an exception, or None to store the settings); every request
    is in ``requests``."""

    class _Gateway:
        def __init__(self) -> None:
            self.version = "v1"
            self.settings = copy.deepcopy(SETTINGS)
            self.available = {
                "apps": {
                    "slack": {
                        "chats": [
                            {
                                "address": "slack:T1/C0456",
                                "name": "#research",
                                "kind": "channel",
                            },
                            {
                                "address": "slack:T1/C0789",
                                "name": "#macro",
                                "kind": "channel",
                            },
                        ],
                        "complete": True,
                        "error": None,
                    }
                }
            }
            self.put: httpx.Response | Exception | None = None
            self.get_error: Exception | None = None
            self.requests: list[httpx.Request] = []

        @property
        def puts(self) -> list[dict]:
            return [json.loads(r.content) for r in self.requests if r.method == "PUT"]

        def handle(self, request: httpx.Request) -> httpx.Response:
            self.requests.append(request)
            path = request.url.path.removeprefix("/api/prefix")
            assert request.headers["X-Service-Token"] == "svc-token"
            assert request.headers["X-User-Id"] == USER
            if request.method == "GET" and path == "/agent/settings":
                if self.get_error is not None:
                    raise self.get_error
                return httpx.Response(
                    200, json={"version": self.version, "settings": self.settings}
                )
            if request.method == "GET" and path == "/agent/settings/available":
                return httpx.Response(200, json=self.available)
            if request.method == "PUT" and path == "/agent/settings":
                if isinstance(self.put, Exception):
                    raise self.put
                if self.put is not None:
                    return self.put
                body = json.loads(request.content)
                self.settings, self.version = body["settings"], "v2"
                return httpx.Response(
                    200,
                    json={
                        "version": "v2",
                        "settings": self.settings,
                        "changes": ["slack: preferred chat is now #macro"],
                    },
                )
            return httpx.Response(404)

    fake = _Gateway()
    transport = httpx.MockTransport(fake.handle)
    monkeypatch.setattr(env, "CHANNEL_GATEWAY_URL", GATEWAY)
    monkeypatch.setenv("INTERNAL_SERVICE_TOKEN", "svc-token")
    monkeypatch.setattr(
        messaging,
        "_client",
        lambda timeout: httpx.AsyncClient(transport=transport, timeout=timeout),
    )
    return fake


@pytest.fixture
def names(monkeypatch) -> AsyncMock:
    """langalpha's workspaces table: the user's live, non-flash ones."""

    async def get_workspace_names(user_id, workspace_ids=None):
        assert user_id == USER
        if workspace_ids is None:
            return dict(NAMES)
        ids = {normalize_uuid(w) for w in workspace_ids}
        return {w: name for w, name in NAMES.items() if w in ids}

    mock = AsyncMock(side_effect=get_workspace_names)
    monkeypatch.setattr(channel_settings.workspace_db, "get_workspace_names", mock)
    return mock


@pytest.fixture(autouse=True)
def no_database(monkeypatch):
    """A save here holds no database connection: opening one fails the test."""

    @asynccontextmanager
    async def _refuse():
        raise AssertionError("a channels save opened a database transaction")
        yield

    monkeypatch.setattr(db_json_route, "get_db_connection", _refuse)


def _sandbox() -> MagicMock:
    sb = MagicMock()
    sb.computer_root = ROOT
    sb.normalize_path.side_effect = lambda p: p if p.startswith("/") else f"{ROOT}/{p}"
    sb.aread_text = AsyncMock(
        side_effect=AssertionError("read fell through to the sandbox")
    )
    sb.awrite_text = AsyncMock(
        side_effect=AssertionError("write fell through to the sandbox")
    )
    return sb


@pytest.fixture
def backend(gateway, names) -> ChannelsBackend:
    return ChannelsBackend(
        user_id=USER, call=CallContext(), sandbox_backend=_sandbox(), root_prefix=PREFIX
    )


async def _read(backend: ChannelsBackend) -> dict:
    return json.loads(await backend.aread_range(CHANNELS))


async def _write(backend: ChannelsBackend, change) -> str | None:
    """Read channels.json, apply ``change`` to it and Write it back."""
    settings = await _read(backend)
    change(settings)
    result = await backend.awrite_text(CHANNELS, json.dumps(settings, indent=2))
    return result["message"] if isinstance(result, dict) else None


async def _refused(backend: ChannelsBackend, change) -> UserDataValidationError:
    with pytest.raises(UserDataValidationError) as exc:
        await _write(backend, change)
    return exc.value


def _prefer_macro(settings: dict) -> None:
    settings["slack"]["preferred"] = "slack:T1/C0789"


class TestRead:
    @pytest.mark.asyncio
    async def test_names_come_from_langalphas_workspaces(self, backend):
        settings = await _read(backend)

        chat = settings["slack"]["chats"]["slack:T1/C0456"]
        # The gateway's copy said "Old name"; the table says "Research".
        assert chat == {
            "mode": "ptc",
            "workspace_id": RESEARCH,
            "workspace": "Research",
            "name": "#research",
        }
        assert list(chat) == ["mode", "workspace_id", "workspace", "name"]
        assert settings["default"] == {
            "mode": "ptc",
            "workspace_id": RESEARCH,
            "workspace": "Research",
        }

    @pytest.mark.asyncio
    async def test_a_workspace_that_is_no_longer_the_users_reads_null(
        self, backend, gateway
    ):
        gateway.settings["slack"]["chats"]["slack:T1/C0456"]["workspace_id"] = STRANGERS

        settings = await _read(backend)

        assert settings["slack"]["chats"]["slack:T1/C0456"]["workspace"] is None

    @pytest.mark.asyncio
    async def test_a_gateway_outage_reads_as_unavailable_not_as_no_file(
        self, backend, gateway
    ):
        gateway.get_error = httpx.ConnectError("down")

        with pytest.raises(ReadUnavailable, match="can't be read right now"):
            await backend.aread_range(CHANNELS)

    @pytest.mark.asyncio
    async def test_available_lists_chats_and_workspaces(self, backend):
        available = json.loads(await backend.aread_range(AVAILABLE))

        assert available["apps"]["slack"]["chats"][1] == {
            "address": "slack:T1/C0789",
            "name": "#macro",
            "kind": "channel",
        }
        assert available["apps"]["slack"]["complete"] is True
        assert available["workspaces"] == [
            {"workspace_id": RESEARCH, "name": "Research"},
            {"workspace_id": MACRO, "name": "Macro"},
        ]

    @pytest.mark.asyncio
    async def test_available_gets_as_long_as_a_save(self, backend, gateway):
        await backend.aread_range(AVAILABLE)
        await backend.aread_range(CHANNELS)

        waits = {r.url.path: r.extensions["timeout"]["read"] for r in gateway.requests}
        assert waits == {
            "/api/prefix/agent/settings/available": 30.0,
            "/api/prefix/agent/settings": 20.0,
        }

    @pytest.mark.asyncio
    async def test_the_readme_is_served(self, backend):
        readme = await backend.aread_range(README)

        assert "available.json" in readme and "Editing rules" in readme


class TestSave:
    @pytest.mark.asyncio
    async def test_the_put_carries_the_version_read_and_the_whole_settings(
        self, backend, gateway
    ):
        def change(settings):
            _prefer_macro(settings)
            settings["slack"]["chats"]["slack:T1/C0789"] = {
                "mode": "ptc",
                "workspace_id": MACRO.upper(),
            }

        message = await _write(backend, change)

        assert message == "Saved channels.json:\n- slack: preferred chat is now #macro"
        (put,) = gateway.puts
        assert put["version"] == "v1"
        assert put["settings"] == {
            "default": {
                "mode": "ptc",
                "workspace_id": RESEARCH,
                "workspace": "Research",
            },
            "slack": {
                "preferred": "slack:T1/C0789",
                "chats": {
                    # The gateway's label is dropped; langalpha names the workspace.
                    "slack:T1/C0456": {
                        "mode": "ptc",
                        "workspace_id": RESEARCH,
                        "workspace": "Research",
                    },
                    # A workspace id is sent canonical.
                    "slack:T1/C0789": {
                        "mode": "ptc",
                        "workspace_id": MACRO,
                        "workspace": "Macro",
                    },
                },
                "automation_output": {RESEARCH: "slack:T1/C0123"},
                "agent_messages": {"enabled": True, "allowed": ["slack:T1/C0456"]},
            },
        }

    @pytest.mark.asyncio
    async def test_every_workspace_id_a_save_sets_must_be_the_users(
        self, backend, gateway
    ):
        def change(settings):
            settings["default"]["workspace_id"] = FLASH
            settings["slack"]["chats"]["slack:T1/C0456"]["workspace_id"] = STRANGERS
            settings["slack"]["chats"]["slack:T1/C0789"] = {
                "mode": "ptc",
                "workspace_id": GONE,
            }
            settings["slack"]["automation_output"] = {"not-a-uuid": "slack:T1/C0123"}

        exc = await _refused(backend, change)

        assert exc.error_type == "schema_error"
        assert [path for path, _ in exc.problems] == [
            "default.workspace_id",
            'slack.chats["slack:T1/C0456"].workspace_id',
            'slack.chats["slack:T1/C0789"].workspace_id',
            'slack.automation_output["not-a-uuid"]',
        ]
        assert all("not one of your workspaces" in msg for _, msg in exc.problems)
        assert f"See {README}" in exc.message
        assert gateway.puts == []

    @pytest.mark.asyncio
    async def test_a_deleted_workspace_left_in_place_does_not_block_a_save(
        self, backend, gateway
    ):
        gateway.settings["default"]["workspace_id"] = GONE
        gateway.settings["slack"]["chats"]["slack:T1/C0456"]["workspace_id"] = GONE
        gateway.settings["slack"]["automation_output"] = {GONE: "slack:T1/C0123"}

        def change(settings):
            assert settings["default"]["workspace"] is None
            assert settings["slack"]["chats"]["slack:T1/C0456"]["workspace"] is None
            settings["slack"]["agent_messages"]["enabled"] = False

        await _write(backend, change)

        (put,) = gateway.puts
        assert put["settings"]["default"] == {
            "mode": "ptc",
            "workspace_id": GONE,
            "workspace": None,
        }
        slack = put["settings"]["slack"]
        assert slack["chats"]["slack:T1/C0456"]["workspace_id"] == GONE
        assert slack["automation_output"] == {GONE: "slack:T1/C0123"}
        assert slack["agent_messages"]["enabled"] is False

    @pytest.mark.asyncio
    async def test_a_deleted_workspace_moved_elsewhere_is_refused(
        self, backend, gateway
    ):
        gateway.settings["slack"]["chats"]["slack:T1/C0456"]["workspace_id"] = GONE

        def change(settings):
            settings["default"]["workspace_id"] = GONE.upper()

        exc = await _refused(backend, change)

        assert [path for path, _ in exc.problems] == ["default.workspace_id"]
        assert gateway.puts == []

    @pytest.mark.asyncio
    @pytest.mark.parametrize("default", [None, "ptc", []])
    async def test_the_default_must_be_an_object(self, backend, gateway, default):
        def change(settings):
            settings["default"] = default

        exc = await _refused(backend, change)

        assert exc.problems == [
            ("default", "must be an object with mode and workspace_id")
        ]
        assert gateway.puts == []

    @pytest.mark.asyncio
    async def test_every_shape_problem_is_listed_at_once(self, backend, gateway):
        def change(settings):
            settings["default"]["mode"] = "fast"
            settings["slack"]["colour"] = "blue"
            settings["slack"]["agent_messages"]["enabled"] = "yes"
            del settings["slack"]["chats"]

        exc = await _refused(backend, change)

        assert dict(exc.problems) == {
            "default.mode": "Input should be 'ptc' or 'flash'",
            "slack.chats": "missing",
            "slack.agent_messages.enabled": "must be true or false",
            "slack.colour": "unknown field",
        }
        assert exc.message.startswith(
            "schema_error:channels.json: 4 problems, nothing was saved."
        )
        assert gateway.puts == []

    @pytest.mark.asyncio
    async def test_apps_are_the_linked_ones(self, backend, gateway):
        def change(settings):
            settings["discord"] = copy.deepcopy(settings.pop("slack"))

        exc = await _refused(backend, change)

        assert dict(exc.problems) == {
            "discord": "not a connected app here; the connected apps are slack",
            "slack": "missing; keep every key the file had",
        }
        assert gateway.puts == []

    @pytest.mark.asyncio
    async def test_labels_are_ignored_so_changing_only_them_saves_nothing(
        self, backend, gateway
    ):
        def change(settings):
            settings["slack"]["chats"]["slack:T1/C0456"]["name"] = "#renamed"
            settings["default"]["workspace"] = "Whatever"

        message = await _write(backend, change)

        assert message == channel_settings.ChannelsFile.unchanged
        assert gateway.puts == []

    @pytest.mark.asyncio
    async def test_writing_back_what_was_read_sends_nothing(self, backend, gateway):
        content = await backend.aread_range(CHANNELS)

        result = await backend.awrite_text(CHANNELS, content)

        assert result["message"] == channel_settings.ChannelsFile.unchanged
        assert gateway.puts == []

    @pytest.mark.asyncio
    async def test_a_save_the_gateway_found_no_change_in_is_unchanged(
        self, backend, gateway
    ):
        gateway.put = httpx.Response(
            200, json={"version": "v1", "settings": SETTINGS, "changes": []}
        )

        assert (
            await _write(backend, _prefer_macro)
            == channel_settings.ChannelsFile.unchanged
        )

    @pytest.mark.asyncio
    async def test_a_partial_read_may_not_drop_a_chat(self, backend, gateway):
        await backend.aread_range(CHANNELS, 0, 3)
        settings = copy.deepcopy(SETTINGS)
        settings["slack"]["chats"] = {}

        with pytest.raises(UserDataValidationError) as exc:
            await backend.awrite_text(CHANNELS, json.dumps(settings))

        assert exc.value.error_type == "incomplete_read"
        assert "slack chat slack:T1/C0456" in exc.value.message
        assert gateway.puts == []


class TestGatewayAnswers:
    @pytest.mark.asyncio
    async def test_a_conflict_is_a_version_conflict_and_drops_the_read(
        self, backend, gateway
    ):
        gateway.put = httpx.Response(
            409, json={"code": "version_conflict", "message": "moved"}
        )

        exc = await _refused(backend, _prefer_macro)

        assert exc.error_type == "version_conflict"
        assert f"Read({CHANNELS}) again" in exc.message
        # The stale Read is gone, so the retry has to read again.
        with pytest.raises(UserDataValidationError) as again:
            await backend.awrite_text(CHANNELS, json.dumps(SETTINGS))
        assert again.value.error_type == "read_required"

    @pytest.mark.asyncio
    async def test_every_problem_the_gateway_lists_reaches_the_writer(
        self, backend, gateway
    ):
        gateway.put = httpx.Response(
            400,
            json={
                "code": "invalid",
                "message": "2 problems",
                "problems": [
                    {"field": "slack.preferred", "message": "the bot is not in #macro"},
                    {
                        "field": "slack.agent_messages.allowed[0]",
                        "message": "no such chat",
                    },
                ],
            },
        )

        exc = await _refused(backend, _prefer_macro)

        assert exc.error_type == "schema_error"
        assert exc.message == (
            "schema_error:channels.json: 2 problems, nothing was saved.\n"
            "- slack.preferred: the bot is not in #macro\n"
            "- slack.agent_messages.allowed[0]: no such chat\n"
            f"See {README} for the fields and examples."
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("applied", [None, [], "default: mode is now flash"])
    async def test_unavailable_saved_nothing(self, backend, gateway, applied):
        body = {"code": "unavailable", "message": "storage down"}
        if applied is not None:
            body["applied"] = applied
        gateway.put = httpx.Response(503, json=body)

        exc = await _refused(backend, _prefer_macro)

        assert exc.error_type == "server_error"
        assert (
            "can't save channel settings right now; nothing was saved. Retry once"
            in exc.message
        )
        # Nothing moved, so the Read still vouches for a retry.
        gateway.put = None
        settings = copy.deepcopy(SETTINGS)
        _prefer_macro(settings)
        assert await backend.awrite_text(CHANNELS, json.dumps(settings)) is not None
        assert len(gateway.puts) == 2

    @pytest.mark.asyncio
    async def test_a_save_that_landed_in_part_says_what_and_drops_the_read(
        self, backend, gateway
    ):
        gateway.put = httpx.Response(
            503,
            json={
                "code": "unavailable",
                "message": "The default was saved and the rest was not; try again shortly.",
                "applied": ["default: mode is now flash", 7, ""],
            },
        )

        exc = await _refused(backend, _prefer_macro)

        assert exc.error_type == "server_error"
        assert exc.message == (
            "server_error:channels.json: The default was saved and the rest was not; "
            "try again shortly.\n"
            "What was saved:\n"
            "- default: mode is now flash\n"
            "Read channels.json again before you retry; it shows what saved, so reapply "
            "only the rest. If that fails again, stop and tell the user."
        )
        with pytest.raises(UserDataValidationError) as again:
            await backend.awrite_text(CHANNELS, json.dumps(SETTINGS))
        assert again.value.error_type == "read_required"

    @pytest.mark.asyncio
    async def test_an_unreachable_gateway_saved_nothing(self, backend, gateway):
        gateway.put = httpx.ConnectError("refused")

        exc = await _refused(backend, _prefer_macro)

        assert exc.error_type == "server_error"
        assert "could not be reached. Nothing was saved." in exc.message

    @pytest.mark.asyncio
    async def test_a_timeout_says_the_save_may_have_landed(self, backend, gateway):
        gateway.put = httpx.ReadTimeout("slow")

        exc = await _refused(backend, _prefer_macro)

        assert exc.error_type == "server_error"
        assert "may or may not have landed" in exc.message

    @pytest.mark.asyncio
    async def test_a_save_that_cannot_read_the_settings_saved_nothing(
        self, backend, gateway
    ):
        await backend.aread_range(CHANNELS)
        gateway.get_error = httpx.ConnectError("down")

        with pytest.raises(UserDataValidationError) as exc:
            await backend.awrite_text(
                CHANNELS,
                json.dumps(
                    {**SETTINGS, "default": {"mode": "flash", "workspace_id": None}}
                ),
            )

        assert exc.value.error_type == "server_error"
        assert "can't be read right now" in exc.value.message
        assert exc.value.message.endswith("Nothing was saved.")


class TestReadOnlyAvailable:
    @pytest.mark.asyncio
    async def test_a_write_is_refused(self, backend, gateway):
        await backend.aread_range(AVAILABLE)

        with pytest.raises(UserDataValidationError) as exc:
            await backend.awrite_text(AVAILABLE, "{}")

        assert (
            "is kept by the server; it can't be edited. Update channels.json instead."
            in exc.value.message
        )
        assert gateway.puts == []

    @pytest.mark.asyncio
    async def test_an_edit_is_refused(self, backend):
        await backend.aread_range(AVAILABLE)

        result = await backend.aedit_text(AVAILABLE, "#macro", "#other")

        assert result["success"] is False
        assert "can't be edited" in result["error"]

    def test_it_is_listed_as_not_writable(self, backend):
        assert backend.is_writable(CHANNELS)
        assert not backend.is_writable(AVAILABLE)


def _gates(*, channels: bool) -> IdentityGates:
    return IdentityGates(
        user_memory=False,
        workspace_memory=False,
        memo=False,
        user_data=True,
        workflow=False,
        workflow_fs=False,
        workflow_tool=False,
        channels=channels,
    )


@pytest.fixture
def filesystem(gateway, names):
    composite, _ = build_filesystem_backend(
        backend=_sandbox(),
        gates=_gates(channels=True),
        store=None,
        user_id=USER,
        workspace_id=RESEARCH,
    )
    return composite


class TestThroughTheFileTools:
    def test_the_route_is_there_only_when_the_gate_is(self):
        on, _ = build_filesystem_backend(
            backend=_sandbox(),
            gates=_gates(channels=True),
            store=None,
            user_id=USER,
            workspace_id=None,
        )
        off, _ = build_filesystem_backend(
            backend=_sandbox(),
            gates=_gates(channels=False),
            store=None,
            user_id=USER,
            workspace_id=None,
        )

        assert isinstance(on.route_for(CHANNELS), ChannelsBackend)
        assert off.route_for(CHANNELS) is None

    @pytest.mark.asyncio
    async def test_an_edit_saves_over_the_version_read(self, filesystem, gateway):
        await filesystem.aread_range(CHANNELS)

        result = await filesystem.aedit_text(
            CHANNELS, '"preferred": "slack:T1/C0456"', '"preferred": null'
        )

        assert result["success"] is True
        assert result["message"].startswith("Saved channels.json:")
        (put,) = gateway.puts
        assert put["version"] == "v1"
        assert put["settings"]["slack"]["preferred"] is None

    @pytest.mark.asyncio
    async def test_an_edit_after_the_settings_moved_is_a_conflict(
        self, filesystem, gateway
    ):
        await filesystem.aread_range(CHANNELS)
        gateway.version = "v9"

        result = await filesystem.aedit_text(
            CHANNELS, '"enabled": true', '"enabled": false'
        )

        assert result["success"] is False
        assert result["error"].startswith("version_conflict:")
        assert gateway.puts == []

    @pytest.mark.asyncio
    async def test_glob_finds_every_file(self, filesystem):
        found = await filesystem.aglob_paths("*.json", path=PREFIX)

        assert sorted(found) == [AVAILABLE, CHANNELS]

    @pytest.mark.asyncio
    async def test_grep_searches_the_rendered_files(self, filesystem):
        assert await filesystem.agrep_rich("#macro", path=PREFIX) == [AVAILABLE]
        assert await filesystem.agrep_rich("preferred", path=PREFIX) == [
            README,
            CHANNELS,
        ]
