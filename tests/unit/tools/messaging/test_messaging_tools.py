"""send_message and list_message_targets: when they exist, what they send, and
what the model reads back.

Delivery is the gateway's, so these tests pin the langalpha half of the
contract: the tools exist only with a gateway configured, carry the turn's
identity and exactly the documented fields, and turn every answer, including
no answer at all, into a result the model can act on without raising into the
graph.
"""

import json

import httpx
import pytest
from langchain_core.messages import ToolMessage

from src.config import env
from src.tools.messaging import build_messaging_tools, messaging_enabled
from src.tools.messaging import tools as messaging

GATEWAY = "http://gateway.test/api/prefix"
TOKEN = "svc-token"

CONFIGURABLE = {
    "user_id": "user-1",
    "thread_id": "thread-1",
    "run_id": "run-1",
    "workspace_id": "ws-1",
    "platform": "slack",
}


@pytest.fixture
def configured(monkeypatch):
    monkeypatch.setattr(env, "CHANNEL_GATEWAY_URL", GATEWAY)
    monkeypatch.setenv("INTERNAL_SERVICE_TOKEN", TOKEN)


@pytest.fixture
def gateway(monkeypatch, configured):
    """A fake gateway. Set ``.reply`` to a response or an exception to raise;
    every request it received is in ``.requests``."""

    class _Gateway:
        def __init__(self):
            self.requests: list[httpx.Request] = []
            self.reply: httpx.Response | Exception = httpx.Response(
                200, json={"status": "sent", "address": "imessage", "current": False}
            )

        def handle(self, request: httpx.Request) -> httpx.Response:
            self.requests.append(request)
            if isinstance(self.reply, Exception):
                raise self.reply
            return self.reply

    fake = _Gateway()
    transport = httpx.MockTransport(fake.handle)
    monkeypatch.setattr(
        messaging,
        "_client",
        lambda timeout: httpx.AsyncClient(transport=transport, timeout=timeout),
    )
    return fake


def _tool(name: str, *, has_workspace_files: bool = True):
    tools = {
        t.name: t
        for t in build_messaging_tools(has_workspace_files=has_workspace_files)
    }
    return tools[name]


async def _call(tool, args: dict, configurable: dict | None = None) -> str:
    result = await tool.ainvoke(
        {"name": tool.name, "args": args, "id": "call-7", "type": "tool_call"},
        config={"configurable": CONFIGURABLE if configurable is None else configurable},
    )
    assert isinstance(result, ToolMessage)
    return result.content


async def _message(tool, args: dict, configurable: dict | None = None) -> ToolMessage:
    result = await tool.ainvoke(
        {"name": tool.name, "args": args, "id": "call-7", "type": "tool_call"},
        config={"configurable": CONFIGURABLE if configurable is None else configurable},
    )
    assert isinstance(result, ToolMessage)
    return result


# -- registration -------------------------------------------------------------


class TestTheToolsExistOnlyWithAGateway:
    def test_the_default_build_has_neither(self, monkeypatch):
        monkeypatch.setattr(env, "CHANNEL_GATEWAY_URL", "")
        monkeypatch.delenv("INTERNAL_SERVICE_TOKEN", raising=False)

        assert not messaging_enabled()
        assert build_messaging_tools(has_workspace_files=True) == []

    def test_a_gateway_without_a_token_is_not_enough(self, monkeypatch):
        monkeypatch.setattr(env, "CHANNEL_GATEWAY_URL", GATEWAY)
        monkeypatch.setenv("INTERNAL_SERVICE_TOKEN", "  ")

        assert build_messaging_tools(has_workspace_files=False) == []

    def test_a_token_without_a_gateway_is_not_enough(self, monkeypatch):
        monkeypatch.setattr(env, "CHANNEL_GATEWAY_URL", "")
        monkeypatch.setenv("INTERNAL_SERVICE_TOKEN", TOKEN)

        assert build_messaging_tools(has_workspace_files=False) == []

    @pytest.mark.usefixtures("configured")
    def test_both_set_gives_both_tools(self):
        names = [t.name for t in build_messaging_tools(has_workspace_files=True)]
        assert names == ["send_message", "list_message_targets"]

    @pytest.mark.usefixtures("configured")
    def test_only_an_agent_without_workspace_files_names_a_workspace(self):
        """PTC attaches its own files by path; Flash has none, so it says
        where a file lives."""
        ptc = _tool("send_message", has_workspace_files=True).tool_call_schema
        flash = _tool("send_message", has_workspace_files=False).tool_call_schema

        assert set(ptc.model_json_schema()["properties"]) == {
            "text",
            "files",
            "target",
            "reply",
            "new_thread",
        }
        assert set(flash.model_json_schema()["properties"]) == {
            "text",
            "files",
            "workspace_id",
            "target",
            "reply",
            "new_thread",
        }

    @pytest.mark.usefixtures("configured")
    def test_the_turn_identity_never_reaches_the_schema(self):
        for tool in build_messaging_tools(has_workspace_files=False):
            props = tool.tool_call_schema.model_json_schema().get("properties", {})
            assert not {"config", "tool_call_id"} & set(props)


class TestBothAgentsBindThem:
    def _flash_tools(self, monkeypatch):
        from unittest.mock import MagicMock

        from ptc_agent.agent.flash import agent as flash_module

        monkeypatch.setattr(
            flash_module, "get_web_search_tool", lambda **_: MagicMock(name="WebSearch")
        )
        return [
            getattr(t, "name", None)
            for t in flash_module.FlashAgent._build_tools(MagicMock())
        ]

    def test_flash_binds_them_when_configured(self, monkeypatch, configured):
        names = self._flash_tools(monkeypatch)
        assert {"send_message", "list_message_targets"} <= set(names)

    def test_flash_has_neither_by_default(self, monkeypatch):
        monkeypatch.setattr(env, "CHANNEL_GATEWAY_URL", "")
        names = self._flash_tools(monkeypatch)
        assert not {"send_message", "list_message_targets"} & set(names)

    def _ptc_build(self):
        """Build the PTC agent far enough to see the tools it binds, and the
        tools its subagents inherit."""
        from unittest.mock import MagicMock, patch

        from langchain_core.language_models.fake_chat_models import GenericFakeChatModel

        from ptc_agent.agent.agent import PTCAgent
        from ptc_agent.config import AgentConfig, LLMConfig
        from ptc_agent.config.core import (
            DaytonaConfig,
            FilesystemConfig,
            LoggingConfig,
            MCPConfig,
            SandboxConfig,
            SecurityConfig,
        )

        config = AgentConfig(
            llm=LLMConfig(name="test-model", flash="test-model"),
            security=SecurityConfig(),
            logging=LoggingConfig(),
            sandbox=SandboxConfig(daytona=DaytonaConfig(api_key="test-key")),
            mcp=MCPConfig(),
            filesystem=FilesystemConfig(),
        )
        config.llm_client = GenericFakeChatModel(messages=iter([]))
        captured: dict = {}

        def _capture(_model, **kwargs):
            captured.update(kwargs)
            return MagicMock()

        with (
            patch("ptc_agent.agent.agent.create_agent", side_effect=_capture),
            patch("ptc_agent.agent.agent.CompactionMiddleware"),
            patch("ptc_agent.agent.agent.SubAgentMiddleware") as subagents,
        ):
            PTCAgent(config).create_agent(
                sandbox=MagicMock(),
                mcp_registry=MagicMock(),
                tool_summary="",
                user_id="u",
            )
        main = [getattr(t, "name", None) for t in captured["tools"]]
        inherited = [
            getattr(t, "name", None)
            for t in subagents.call_args.kwargs["default_tools"]
        ]
        return main, inherited

    def test_ptc_binds_them_on_the_main_agent_only(self, configured):
        main, inherited = self._ptc_build()

        assert {"send_message", "list_message_targets"} <= set(main)
        # A subagent reports to its parent, never to a person.
        assert not {"send_message", "list_message_targets"} & set(inherited)

    def test_ptc_has_neither_by_default(self, monkeypatch):
        monkeypatch.setattr(env, "CHANNEL_GATEWAY_URL", "")
        main, _ = self._ptc_build()

        assert not {"send_message", "list_message_targets"} & set(main)


# -- what goes out --------------------------------------------------------------


class TestTheSendRequest:
    @pytest.mark.asyncio
    async def test_it_carries_the_turn_and_exactly_the_contract_fields(self, gateway):
        await _call(
            _tool("send_message"),
            {
                "text": "Here is the model.",
                "files": ["results/model.xlsx"],
                "target": "slack:T1",
                "reply": True,
            },
        )

        (request,) = gateway.requests
        assert request.method == "POST"
        assert str(request.url) == f"{GATEWAY}/agent/send"
        assert request.headers["X-Service-Token"] == TOKEN
        assert request.headers["X-User-Id"] == "user-1"
        assert json.loads(request.content) == {
            "thread_id": "thread-1",
            "run_id": "run-1",
            "tool_call_id": "call-7",
            "turn_platform": "slack",
            "workspace_id": "ws-1",
            "target": "slack:T1",
            "text": "Here is the model.",
            "files": [{"path": "results/model.xlsx", "workspace_id": None}],
            "reply": True,
            "new_thread": False,
        }

    @pytest.mark.asyncio
    @pytest.mark.parametrize("has_workspace_files", [True, False])
    async def test_an_automations_turn_names_its_run(self, gateway, has_workspace_files):
        await _call(
            _tool("send_message", has_workspace_files=has_workspace_files),
            {"text": "Done.", "target": "slack:T1/C2"},
            {**CONFIGURABLE, "automation_execution_id": "exec-9"},
        )

        assert json.loads(gateway.requests[0].content)["automation_execution_id"] == "exec-9"

    @pytest.mark.asyncio
    async def test_any_other_turn_names_no_run(self, gateway):
        await _call(
            _tool("send_message"),
            {"text": "hi"},
            {**CONFIGURABLE, "automation_execution_id": None},
        )

        assert "automation_execution_id" not in json.loads(gateway.requests[0].content)

    @pytest.mark.asyncio
    async def test_an_omitted_target_asks_for_this_conversation(self, gateway):
        await _call(_tool("send_message"), {"text": "hi"})

        body = json.loads(gateway.requests[0].content)
        assert body["target"] is None
        assert body["files"] == []
        assert body["reply"] is False
        assert body["new_thread"] is False

    @pytest.mark.asyncio
    @pytest.mark.parametrize("has_workspace_files", [True, False])
    async def test_a_new_thread_is_asked_for_by_name(
        self, gateway, has_workspace_files
    ):
        await _call(
            _tool("send_message", has_workspace_files=has_workspace_files),
            {"text": "hi", "target": "slack:T1/C2", "new_thread": True},
        )

        assert json.loads(gateway.requests[0].content)["new_thread"] is True

    @pytest.mark.asyncio
    async def test_a_turn_in_no_conversation_sends_a_null_surface(self, gateway):
        await _call(
            _tool("send_message"), {"text": "hi"}, {**CONFIGURABLE, "platform": None}
        )

        assert json.loads(gateway.requests[0].content)["turn_platform"] is None

    @pytest.mark.asyncio
    async def test_flash_names_the_workspace_on_every_file(self, gateway):
        await _call(
            _tool("send_message", has_workspace_files=False),
            {"text": "", "files": ["a.csv", "b.csv"], "workspace_id": "ws-ptc"},
        )

        body = json.loads(gateway.requests[0].content)
        assert body["workspace_id"] == "ws-1"
        assert body["files"] == [
            {"path": "a.csv", "workspace_id": "ws-ptc"},
            {"path": "b.csv", "workspace_id": "ws-ptc"},
        ]

    @pytest.mark.asyncio
    async def test_flash_files_without_a_workspace_are_refused_before_any_call(
        self, gateway
    ):
        content = await _call(
            _tool("send_message", has_workspace_files=False),
            {"text": "x", "files": ["a.csv"]},
        )

        assert gateway.requests == []
        assert content.startswith("status: failed\ncode: invalid_request")
        assert "workspace_id" in content

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("args", "phrase"),
        [
            ({"text": "   "}, "Nothing to send"),
            ({"text": "x" * (messaging.MAX_TEXT_CHARS + 1)}, "Split it"),
            ({"text": "x", "files": [f"f{i}.csv" for i in range(11)]}, "at most 10"),
        ],
    )
    async def test_a_request_the_gateway_would_refuse_never_leaves(
        self, gateway, args, phrase
    ):
        content = await _call(_tool("send_message"), args)

        assert gateway.requests == []
        assert "status: failed" in content
        assert phrase in content

    @pytest.mark.asyncio
    async def test_a_turn_with_no_user_messages_nobody(self, gateway):
        content = await _call(_tool("send_message"), {"text": "hi"}, {"thread_id": "t"})

        assert gateway.requests == []
        assert content.startswith("status: failed")


class TestTheTargetsRequest:
    @pytest.mark.asyncio
    async def test_it_names_the_conversation_the_turn_is_in(self, gateway):
        gateway.reply = httpx.Response(200, json={"current": None, "targets": []})
        await _call(_tool("list_message_targets"), {})

        (request,) = gateway.requests
        assert request.method == "GET"
        assert request.url.path == "/api/prefix/agent/targets"
        assert dict(request.url.params) == {
            "thread_id": "thread-1",
            "run_id": "run-1",
            "turn_platform": "slack",
        }
        assert request.headers["X-Service-Token"] == TOKEN
        assert request.headers["X-User-Id"] == "user-1"

    @pytest.mark.asyncio
    async def test_an_unknown_surface_is_left_out_rather_than_sent_empty(self, gateway):
        gateway.reply = httpx.Response(200, json={})
        await _call(
            _tool("list_message_targets"), {}, {**CONFIGURABLE, "platform": None}
        )

        assert "turn_platform" not in gateway.requests[0].url.params


# -- what comes back --------------------------------------------------------------


class TestTheSendResult:
    @pytest.mark.asyncio
    async def test_a_send_into_this_conversation(self, gateway):
        gateway.reply = httpx.Response(
            200,
            json={
                "status": "sent",
                "code": None,
                "message": "Sent here.",
                "address": "slack:T1/C1/1.2",
                "current": True,
                "duplicate": False,
                "files": [],
            },
        )

        content = await _call(_tool("send_message"), {"text": "hi"})

        assert (
            content
            == "status: sent\nto: slack:T1/C1/1.2 (this conversation)\nSent here."
        )

    @pytest.mark.asyncio
    async def test_a_partial_send_names_each_file(self, gateway):
        gateway.reply = httpx.Response(
            200,
            json={
                "status": "partial",
                "code": "file_error",
                "message": "The text went; one file did not.",
                "address": "imessage",
                "current": False,
                "files": [
                    {"path": "a.xlsx", "status": "linked", "reason": None},
                    {"path": "b.csv", "status": "failed", "reason": "not found"},
                    {"path": "c.png", "status": "sent", "reason": None},
                ],
            },
        )

        content = await _call(
            _tool("send_message"), {"text": "hi", "files": ["a", "b", "c"]}
        )

        assert content.splitlines() == [
            "status: partial",
            "code: file_error",
            "to: imessage",
            "The text went; one file did not.",
            "files:",
            "- a.xlsx: linked (sent as a download link)",
            "- b.csv: failed (not found)",
            "- c.png: sent",
        ]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "code",
        [
            "not_linked",
            "disabled",
            "not_allowed",
            "no_current_conversation",
            "invalid_address",
            "ambiguous",
            "unsupported",
            "unreachable",
            "rate_limited",
            "channel_error",
            "file_error",
            "unavailable",
        ],
    )
    async def test_a_refusal_carries_its_code_and_the_gateway_words(
        self, gateway, code
    ):
        gateway.reply = httpx.Response(
            200, json={"status": "failed", "code": code, "message": f"Because {code}."}
        )

        content = await _call(_tool("send_message"), {"text": "hi"})

        assert content == f"status: failed\ncode: {code}\nBecause {code}."

    @pytest.mark.asyncio
    async def test_a_replayed_call_says_nothing_went_twice(self, gateway):
        gateway.reply = httpx.Response(
            200, json={"status": "sent", "address": "imessage", "duplicate": True}
        )

        content = await _call(_tool("send_message"), {"text": "hi"})

        assert content.startswith("status: sent\nto: imessage\n")
        assert "nothing was sent twice" in content


class TestNoAnswerIsStillAResult:
    """A gateway that is down, slow or misconfigured must not fail the turn,
    and must not leave the model guessing whether anything went out."""

    @pytest.mark.asyncio
    async def test_an_unreachable_gateway_sent_nothing(self, gateway):
        gateway.reply = httpx.ConnectError("refused")

        content = await _call(_tool("send_message"), {"text": "hi"})

        assert content.startswith("status: failed\ncode: unavailable")
        assert "Nothing was sent." in content

    @pytest.mark.asyncio
    async def test_a_timeout_leaves_delivery_unknown(self, gateway):
        gateway.reply = httpx.ReadTimeout("slow")

        content = await _call(_tool("send_message"), {"text": "hi"})

        assert content.startswith("status: unknown\ncode: unavailable")
        # It may yet land, so a second send could post it twice.
        assert "Do not send it again" in content
        assert "tell the user delivery couldn't be confirmed" in content

    @pytest.mark.asyncio
    async def test_a_broken_connection_leaves_delivery_unknown(self, gateway):
        gateway.reply = httpx.RemoteProtocolError("reset")

        content = await _call(_tool("send_message"), {"text": "hi"})

        assert content.startswith("status: unknown")

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", [401, 403])
    async def test_a_refused_token_sent_nothing(self, gateway, status):
        gateway.reply = httpx.Response(status, json={"detail": "bad token"})

        content = await _call(_tool("send_message"), {"text": "hi"})

        assert content.startswith("status: failed")
        assert "credentials" in content

    @pytest.mark.asyncio
    async def test_a_malformed_request_says_why(self, gateway):
        gateway.reply = httpx.Response(422, json={"detail": "text too long"})

        content = await _call(_tool("send_message"), {"text": "hi"})

        assert content.startswith("status: failed")
        assert "text too long" in content

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", [404, 405])
    async def test_a_missing_route_sent_nothing(self, gateway, status):
        gateway.reply = httpx.Response(status, json={"detail": "Not Found"})

        content = await _call(_tool("send_message"), {"text": "hi"})

        assert content.startswith("status: failed\ncode: unavailable")
        assert f"failed ({status}). Nothing was sent." in content

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "reply",
        [
            httpx.Response(500, text="boom"),
            httpx.Response(200, text="not json"),
            httpx.Response(200, json=["not", "an", "object"]),
        ],
    )
    async def test_a_gateway_fault_leaves_delivery_unknown(self, gateway, reply):
        gateway.reply = reply

        content = await _call(_tool("send_message"), {"text": "hi"})

        assert content.startswith("status: unknown")

    @pytest.mark.asyncio
    async def test_a_listing_that_fails_says_so(self, gateway):
        gateway.reply = httpx.ConnectError("refused")

        content = await _call(_tool("list_message_targets"), {})

        assert "could not be reached" in content
        assert "unknown right now" in content


class TestTheDeliveryArtifact:
    """The client renders the outcome from ``ToolMessage.artifact``; the model
    reads only the content, so the artifact must not change what it reads."""

    @pytest.mark.asyncio
    async def test_a_send_into_this_conversation(self, gateway):
        gateway.reply = httpx.Response(
            200,
            json={
                "status": "sent",
                "message": "Sent here.",
                "address": "slack:T1/C1/1.2",
                "current": True,
            },
        )

        message = await _message(_tool("send_message"), {"text": "hi"})

        assert message.artifact == {
            "type": "message_delivery",
            "status": "sent",
            "code": None,
            "address": "slack:T1/C1/1.2",
            "platform": "slack",
            "current": True,
            "duplicate": False,
            "message": "Sent here.",
            "files": [],
        }
        assert message.content == (
            "status: sent\nto: slack:T1/C1/1.2 (this conversation)\nSent here."
        )

    @pytest.mark.asyncio
    async def test_a_partial_send_carries_each_file_as_the_gateway_said(self, gateway):
        gateway.reply = httpx.Response(
            200,
            json={
                "status": "partial",
                "code": "file_error",
                "message": "The text went; one file did not.",
                "address": "imessage",
                "files": [
                    {"path": "a.xlsx", "status": "linked", "reason": None},
                    {"path": "b.csv", "status": "failed", "reason": "not found"},
                    "junk",
                    {"path": "c.png"},
                ],
            },
        )

        artifact = (await _message(_tool("send_message"), {"text": "hi"})).artifact

        assert artifact["status"] == "partial"
        assert artifact["code"] == "file_error"
        assert artifact["files"] == [
            {"path": "a.xlsx", "status": "linked", "reason": None},
            {"path": "b.csv", "status": "failed", "reason": "not found"},
            {"path": "c.png", "status": "failed", "reason": None},
        ]

    @pytest.mark.asyncio
    async def test_a_refusal_carries_its_code(self, gateway):
        gateway.reply = httpx.Response(
            200,
            json={"status": "failed", "code": "not_allowed", "message": "Not allowed."},
        )

        artifact = (await _message(_tool("send_message"), {"text": "hi"})).artifact

        assert artifact["status"] == "failed"
        assert artifact["code"] == "not_allowed"
        assert artifact["message"] == "Not allowed."
        assert artifact["address"] is None
        assert artifact["platform"] is None
        assert artifact["current"] is False

    @pytest.mark.asyncio
    async def test_a_replayed_call_is_marked_duplicate(self, gateway):
        gateway.reply = httpx.Response(
            200, json={"status": "sent", "address": "imessage", "duplicate": True}
        )

        artifact = (await _message(_tool("send_message"), {"text": "hi"})).artifact

        assert artifact["duplicate"] is True
        assert artifact["status"] == "sent"

    @pytest.mark.asyncio
    async def test_an_unreachable_gateway_failed_with_no_files(self, gateway):
        gateway.reply = httpx.ConnectError("refused")

        message = await _message(_tool("send_message"), {"text": "hi"})

        assert message.artifact["status"] == "failed"
        assert message.artifact["code"] == "unavailable"
        assert message.artifact["files"] == []
        assert message.artifact["message"] == (
            "The messaging service could not be reached. Nothing was sent."
        )
        assert message.artifact["message"] in message.content

    @pytest.mark.asyncio
    async def test_a_timeout_is_unknown(self, gateway):
        gateway.reply = httpx.ReadTimeout("slow")

        message = await _message(_tool("send_message"), {"text": "hi"})
        artifact = message.artifact

        assert artifact["status"] == "unknown"
        assert artifact["code"] == "unavailable"
        assert artifact["files"] == []
        # The model is told not to send again; the person reading the card is not.
        assert "Do not send it again" in message.content
        assert artifact["message"] == (
            "The messaging service did not answer in time. "
            "Whether the message went out is unknown."
        )

    @pytest.mark.asyncio
    async def test_a_local_refusal_never_reaches_the_gateway(self, gateway):
        message = await _message(_tool("send_message"), {"text": "  "})

        assert gateway.requests == []
        assert message.artifact["status"] == "failed"
        assert message.artifact["code"] == "invalid_request"
        assert message.artifact["message"] == (
            "Nothing to send: pass text, files, or both."
        )
        assert message.content == (
            "status: failed\ncode: invalid_request\n"
            "Nothing to send: pass text, files, or both."
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("address", "platform"),
        [
            ("slack:T1/C2", "slack"),
            ("imessage", "imessage"),
            ("Telegram:42", "telegram"),
        ],
    )
    async def test_the_platform_is_the_addresss_app(self, gateway, address, platform):
        gateway.reply = httpx.Response(200, json={"status": "sent", "address": address})

        artifact = (await _message(_tool("send_message"), {"text": "hi"})).artifact

        assert artifact["platform"] == platform

    @pytest.mark.parametrize("has_workspace_files", [True, False])
    def test_the_schema_the_model_sees_has_no_new_args(
        self, configured, has_workspace_files
    ):
        tool = _tool("send_message", has_workspace_files=has_workspace_files)

        expected = {"text", "files", "target", "reply", "new_thread"}
        if not has_workspace_files:
            expected.add("workspace_id")
        assert set(tool.args) == expected
        assert tool.response_format == "content_and_artifact"

    @pytest.mark.asyncio
    async def test_the_listing_has_no_artifact(self, gateway):
        message = await _message(_tool("list_message_targets"), {})

        assert message.artifact is None


class TestTheTargetsResult:
    @pytest.mark.asyncio
    async def test_the_full_listing(self, gateway):
        gateway.reply = httpx.Response(
            200,
            json={
                "current": {
                    "address": "slack:T1/C1/1.2",
                    "platform": "slack",
                    "name": "this conversation",
                },
                "targets": [
                    {
                        "address": "slack:T1",
                        "platform": "slack",
                        "kind": "dm",
                        "name": "Your Slack DM (Acme)",
                    },
                    {
                        "address": "slack:T1/C9",
                        "platform": "slack",
                        "kind": "channel",
                        "name": "#research (Acme)",
                    },
                ],
                "unavailable": [
                    {
                        "platform": "discord",
                        "reason": "disabled",
                        "message": "You switched Discord off.",
                    },
                ],
                "settings_url": "https://app.example.com/settings/integrations",
            },
        )

        content = await _call(_tool("list_message_targets"), {})

        assert content.splitlines() == [
            "This conversation: slack:T1/C1/1.2. Omit target to send here.",
            "Targets:",
            "- slack:T1 (dm): Your Slack DM (Acme)",
            "- slack:T1/C9 (channel): #research (Acme)",
            "Unavailable:",
            "- discord (disabled): You switched Discord off.",
            "Settings: https://app.example.com/settings/integrations",
        ]

    @pytest.mark.asyncio
    async def test_a_listing_that_failed_there_says_so(self, gateway):
        # What the messaging service answers when it could not build the list:
        # every app unavailable, so no conversation is named even in one.
        gateway.reply = httpx.Response(
            200,
            json={
                "current": None,
                "targets": [],
                "unavailable": [
                    {"platform": app, "reason": "unavailable", "message": "Try later."}
                    for app in ("slack", "discord", "telegram", "imessage")
                ],
                "settings_url": "https://app.example.com/settings/integrations",
            },
        )

        content = await _call(_tool("list_message_targets"), {})

        assert content == (
            "The messaging service could not list the targets. "
            "Where you can send is unknown right now."
        )

    @pytest.mark.asyncio
    async def test_one_app_down_is_still_a_listing(self, gateway):
        gateway.reply = httpx.Response(
            200,
            json={
                "current": None,
                "targets": [],
                "unavailable": [
                    {"platform": "slack", "reason": "unavailable", "message": "Try later."},
                    {"platform": "discord", "reason": "not_linked", "message": "Link it."},
                ],
            },
        )

        content = await _call(
            _tool("list_message_targets"), {}, {**CONFIGURABLE, "platform": None}
        )

        assert content.splitlines()[0] == (
            "This turn is not in a messaging conversation, so every send needs a target."
        )
        assert "- slack (unavailable): Try later." in content

    @pytest.mark.asyncio
    async def test_no_conversation_and_no_targets(self, gateway):
        gateway.reply = httpx.Response(
            200, json={"current": None, "targets": [], "unavailable": []}
        )

        content = await _call(
            _tool("list_message_targets"), {}, {**CONFIGURABLE, "platform": None}
        )

        assert content.splitlines() == [
            "This turn is not in a messaging conversation, so every send needs a target.",
            "Targets: none.",
        ]


NOW = 1_800_000_000


class TestTheThreadNote:
    @pytest.fixture(autouse=True)
    def _clock(self, monkeypatch):
        monkeypatch.setattr(messaging, "_now", lambda: float(NOW))

    async def _listing(self, gateway, targets):
        gateway.reply = httpx.Response(200, json={"current": None, "targets": targets})
        return (await _call(_tool("list_message_targets"), {})).splitlines()[2:]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("ago", "phrase"),
        [
            (0, "just now"),
            (59, "just now"),
            (60, "1m ago"),
            (3599, "59m ago"),
            (3600, "1h ago"),
            (3 * 3600 + 5, "3h ago"),
            (48 * 3600 - 1, "47h ago"),
            (48 * 3600, "2d ago"),
            (10 * 86400 + 99, "10d ago"),
            (-500, "just now"),
        ],
    )
    async def test_each_age_bucket(self, gateway, ago, phrase):
        lines = await self._listing(
            gateway,
            [
                {
                    "address": "slack:T1/C2",
                    "kind": "channel",
                    "name": "#research",
                    "thread": {"last_used_at": NOW - ago},
                }
            ],
        )

        assert lines == [
            "- slack:T1/C2 (channel): #research; this conversation's thread is here, "
            f"last used {phrase}"
        ]

    @pytest.mark.asyncio
    async def test_a_target_without_a_thread_is_unchanged(self, gateway):
        lines = await self._listing(
            gateway,
            [
                {"address": "slack:T1", "kind": "dm", "name": "DM", "thread": None},
                {"address": "slack:T1/C9", "kind": "channel", "name": "#a"},
            ],
        )

        assert lines == ["- slack:T1 (dm): DM", "- slack:T1/C9 (channel): #a"]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "thread",
        [
            "junk",
            [],
            {},
            {"last_used_at": None},
            {"last_used_at": "yesterday"},
            {"last_used_at": True},
            {"last_used_at": 1.5},
        ],
    )
    async def test_a_malformed_thread_leaves_the_line_alone(self, gateway, thread):
        lines = await self._listing(
            gateway,
            [{"address": "slack:T1", "kind": "dm", "name": "DM", "thread": thread}],
        )

        assert lines == ["- slack:T1 (dm): DM"]


class TestThePreferredMark:
    @pytest.fixture(autouse=True)
    def _clock(self, monkeypatch):
        monkeypatch.setattr(messaging, "_now", lambda: float(NOW))

    async def _listing(self, gateway, targets):
        gateway.reply = httpx.Response(200, json={"current": None, "targets": targets})
        return (await _call(_tool("list_message_targets"), {})).splitlines()[2:]

    @pytest.mark.asyncio
    async def test_the_preferred_chat_is_marked(self, gateway):
        lines = await self._listing(
            gateway,
            [
                {"address": "slack:T1", "kind": "dm", "name": "DM", "preferred": False},
                {
                    "address": "slack:T1/C2",
                    "kind": "channel",
                    "name": "#research",
                    "preferred": True,
                },
            ],
        )

        assert lines == [
            "- slack:T1 (dm): DM",
            "- slack:T1/C2 (channel): #research; preferred",
        ]

    @pytest.mark.asyncio
    async def test_preferred_goes_before_the_thread_note(self, gateway):
        lines = await self._listing(
            gateway,
            [
                {
                    "address": "slack:T1/C2",
                    "kind": "channel",
                    "name": "#research",
                    "preferred": True,
                    "thread": {"last_used_at": NOW - 3 * 3600},
                }
            ],
        )

        assert lines == [
            "- slack:T1/C2 (channel): #research; preferred; "
            "this conversation's thread is here, last used 3h ago"
        ]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("value", ["true", 1, None, {}])
    async def test_only_true_marks_it(self, gateway, value):
        lines = await self._listing(
            gateway,
            [{"address": "slack:T1", "kind": "dm", "name": "DM", "preferred": value}],
        )

        assert lines == ["- slack:T1 (dm): DM"]

    def test_the_descriptions_name_the_preferred_chat(self):
        assert "that app's preferred chat" in messaging.SEND_MESSAGE_DESCRIPTION
        assert "preferred chat marked" in messaging.LIST_MESSAGE_TARGETS_DESCRIPTION

    def test_the_descriptions_name_the_dm_by_address(self):
        assert (
            "only an app name, like `discord`, is the user's direct messages there, "
            "the same as `discord:@me`" in messaging.SEND_MESSAGE_DESCRIPTION
        )
        assert (
            "A direct message is listed as `<app>:@me`, or `slack:<team>` on Slack."
            in messaging.LIST_MESSAGE_TARGETS_DESCRIPTION
        )
