"""send_message and list_message_targets: when they exist, what they send, and
what the model reads back.

Delivery is the gateway's, so these tests pin the langalpha half of the
contract: the tools exist only with a gateway configured, carry the turn's
identity and exactly the documented fields, and turn every answer, including
no answer at all, into a result the model can act on without raising into the
graph.
"""

import json
import logging

import httpx
import pytest
from langchain_core.messages import ToolMessage

from src.config import env
from src.tools.messaging import build_messaging_tools, messaging_enabled
from src.tools.messaging import tools as messaging

GATEWAY = "http://gateway.test/api/prefix"
TOKEN = "svc-token"

NVDA = "6f1c2a9e-3b7d-4e5f-9a0b-1c2d3e4f5a6b"

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
        lambda: httpx.AsyncClient(transport=transport),
    )
    return fake


def _tool(name: str, role: str = "analyst"):
    tools = {t.name: t for t in build_messaging_tools(role)}
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
        assert build_messaging_tools("analyst") == []

    def test_a_gateway_without_a_token_is_not_enough(self, monkeypatch):
        monkeypatch.setattr(env, "CHANNEL_GATEWAY_URL", GATEWAY)
        monkeypatch.setenv("INTERNAL_SERVICE_TOKEN", "  ")

        assert build_messaging_tools("chief_of_staff") == []

    def test_a_token_without_a_gateway_is_not_enough(self, monkeypatch):
        monkeypatch.setattr(env, "CHANNEL_GATEWAY_URL", "")
        monkeypatch.setenv("INTERNAL_SERVICE_TOKEN", TOKEN)

        assert build_messaging_tools("chief_of_staff") == []

    @pytest.mark.usefixtures("configured")
    @pytest.mark.parametrize("role", ["analyst", "chief_of_staff"])
    def test_both_set_gives_both_tools(self, role):
        names = [t.name for t in build_messaging_tools(role)]
        assert names == ["send_message", "list_message_targets"]

    @pytest.mark.usefixtures("configured")
    def test_only_the_chief_of_staff_names_a_workspace(self):
        """An analyst attaches its own workspace's files; the Chief of Staff
        reads every workspace, so it says which one a file is in when it is
        not in Home."""
        analyst = _tool("send_message").tool_call_schema
        chief = _tool("send_message", "chief_of_staff").tool_call_schema

        assert set(analyst.model_json_schema()["properties"]) == {
            "text",
            "files",
            "target",
            "reply",
            "new_thread",
        }
        assert set(chief.model_json_schema()["properties"]) == {
            "text",
            "files",
            "workspace_id",
            "target",
            "reply",
            "new_thread",
        }

    @pytest.mark.usefixtures("configured")
    @pytest.mark.parametrize("role", ["analyst", "chief_of_staff"])
    def test_the_turn_identity_never_reaches_the_schema(self, role):
        for tool in build_messaging_tools(role):
            props = tool.tool_call_schema.model_json_schema().get("properties", {})
            assert not {"config", "tool_call_id"} & set(props)


class TestBothRolesBindThem:
    def _ptc_build(self, role: str = "analyst"):
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
            llm=LLMConfig(name="test-model", background="test-model"),
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

        from ptc_agent.agent import agent as agent_module

        resolve = agent_module.resolve_identity_gates

        def _gates(**kwargs):
            self.gates = resolve(**kwargs)
            return self.gates

        with (
            patch.object(agent_module, "resolve_identity_gates", side_effect=_gates),
            patch("ptc_agent.agent.agent.create_agent", side_effect=_capture),
            patch("ptc_agent.agent.agent.CompactionMiddleware"),
            patch("ptc_agent.agent.subagents.assembly.SubAgentMiddleware") as subagents,
        ):
            PTCAgent(config).create_agent(
                sandbox=MagicMock(),
                mcp_registry=MagicMock(),
                tool_summary="",
                user_id="u",
                role=role,
            )
        self.captured = captured
        main = {getattr(t, "name", None): t for t in captured["tools"]}
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

    def test_the_chief_of_staff_binds_them_and_names_a_files_workspace(
        self, configured
    ):
        main, inherited = self._ptc_build("chief_of_staff")

        assert {"send_message", "list_message_targets"} <= set(main)
        assert "workspace_id" in main["send_message"].args
        assert not {"send_message", "list_message_targets"} & set(inherited)
        # And the channel settings files, the same as an Analyst.
        assert self.gates.channels is True

    def test_an_analyst_attaches_only_its_own_files(self, configured):
        main, _ = self._ptc_build()

        assert "workspace_id" not in main["send_message"].args

    @pytest.mark.parametrize("role", ["analyst", "chief_of_staff"])
    def test_the_prompt_says_how_to_talk_over_chat(self, configured, role):
        from ptc_agent.agent.middleware.runtime_context.frozen_prompt import (
            FrozenPromptMiddleware,
        )

        self._ptc_build(role)

        assert "## Over Chat" in self.captured["system_prompt"]
        # The prompt an older epoch is sent with keeps the section too.
        (frozen,) = [
            m
            for m in self.captured["middleware"]
            if isinstance(m, FrozenPromptMiddleware)
        ]
        assert "## Over Chat" in frozen._render(not frozen._built)

    def test_the_prompt_has_no_chat_section_by_default(self, monkeypatch):
        monkeypatch.setattr(env, "CHANNEL_GATEWAY_URL", "")
        self._ptc_build()

        assert "## Over Chat" not in self.captured["system_prompt"]


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
    @pytest.mark.parametrize("role", ["analyst", "chief_of_staff"])
    async def test_an_automations_turn_names_its_run(self, gateway, role):
        await _call(
            _tool("send_message", role),
            {"text": "Done.", "target": "slack:T1/C2"},
            {**CONFIGURABLE, "automation_execution_id": "exec-9"},
        )

        assert (
            json.loads(gateway.requests[0].content)["automation_execution_id"]
            == "exec-9"
        )

    @pytest.mark.asyncio
    async def test_a_report_back_of_several_runs_names_each_sends_own(self, gateway):
        """Each outcome goes out under the run its request came from, since
        the messaging service counts a send as a run's own only at that run's
        targets."""
        from src.server.services import automation_delivery
        from src.server.services.automation_delivery import Delivery, Target

        morning = Delivery(
            "exec-1", [Target(entry="slack:T1/C2", address="slack:T1/C2", name="#desk", ok=True)]
        )
        evening = Delivery(
            "exec-2", [Target(entry="discord", address="discord:@me", name=None, ok=True)]
        )
        configurable = {
            **CONFIGURABLE,
            **automation_delivery.turn_configurable(evening, [morning]),
        }
        for target in ("slack:T1/C2", "discord:@me", "telegram:@me"):
            await _call(
                _tool("send_message", "chief_of_staff"),
                {"text": "Done.", "target": target},
                configurable,
            )

        assert [
            json.loads(r.content)["automation_execution_id"] for r in gateway.requests
        ] == ["exec-1", "exec-2", "exec-2"]

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
    @pytest.mark.parametrize("role", ["analyst", "chief_of_staff"])
    async def test_a_new_thread_is_asked_for_by_name(self, gateway, role):
        await _call(
            _tool("send_message", role),
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
    async def test_the_chief_of_staff_sends_homes_files_by_default(self, gateway):
        await _call(
            _tool("send_message", "chief_of_staff"),
            {"text": "x", "files": ["nvda_amd/digest.md"]},
            {**CONFIGURABLE, "workspace_id": "ws-home"},
        )

        body = json.loads(gateway.requests[0].content)
        assert body["workspace_id"] == "ws-home"
        # Null is the request's own workspace: Home.
        assert body["files"] == [{"path": "nvda_amd/digest.md", "workspace_id": None}]

    @pytest.mark.asyncio
    async def test_the_chief_of_staff_names_a_workspace_on_every_file(self, gateway):
        await _call(
            _tool("send_message", "chief_of_staff"),
            {"text": "", "files": ["a.csv", "b.csv"], "workspace_id": f" {NVDA} "},
            {**CONFIGURABLE, "workspace_id": "ws-home"},
        )

        body = json.loads(gateway.requests[0].content)
        assert body["workspace_id"] == "ws-home"
        assert body["files"] == [
            {"path": "a.csv", "workspace_id": NVDA},
            {"path": "b.csv", "workspace_id": NVDA},
        ]

    @pytest.mark.asyncio
    async def test_a_workspace_id_is_sent_in_its_own_form(self, gateway):
        await _call(
            _tool("send_message", "chief_of_staff"),
            {"text": "", "files": ["a.csv"], "workspace_id": NVDA.upper().replace("-", "")},
        )

        body = json.loads(gateway.requests[0].content)
        assert body["files"] == [{"path": "a.csv", "workspace_id": NVDA}]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "workspace_id",
        [
            "x/../../mcp/vault/secrets/NAME/reveal#",
            f"{NVDA}/../other",
            f"{NVDA}?x=1",
            "ws-nvda",
        ],
    )
    async def test_anything_but_a_workspace_id_sends_nothing(self, gateway, workspace_id):
        message = await _message(
            _tool("send_message", "chief_of_staff"),
            {"text": "hi", "files": ["a.csv"], "workspace_id": workspace_id},
        )

        assert gateway.requests == []
        assert message.content.startswith("status: failed\ncode: invalid_request")
        assert "Nothing was sent." in message.content
        assert message.artifact["status"] == "failed"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("args", "phrase"),
        [
            ({"text": "   "}, "Nothing to send"),
            ({"text": "x" * (messaging.MAX_TEXT_CHARS + 1)}, "Split it"),
            ({"text": "x", "files": [f"f{i}.csv" for i in range(11)]}, "at most 10"),
            (
                {"text": "x", "files": ["a" * (messaging.MAX_PATH_CHARS + 1)]},
                "1025 characters; the limit is 1024",
            ),
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
    async def test_a_path_at_the_limit_is_sent(self, gateway):
        path = "a" * messaging.MAX_PATH_CHARS

        await _call(_tool("send_message"), {"text": "x", "files": [path]})

        (request,) = gateway.requests
        assert json.loads(request.content)["files"] == [{"path": path, "workspace_id": None}]

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

    @pytest.mark.asyncio
    async def test_a_replay_while_the_first_is_still_delivering_says_not_to_resend(
        self, gateway
    ):
        gateway.reply = httpx.Response(
            200,
            json={
                "status": "unknown",
                "code": "in_flight",
                "message": "Still going.",
                "address": "discord:@me",
                "current": False,
                "duplicate": False,
                "files": [],
            },
        )

        content = await _call(_tool("send_message"), {"text": "hi"})

        assert content.splitlines() == [
            "status: unknown",
            "code: in_flight",
            "to: discord:@me",
            "An earlier attempt of this same call is still delivering it, and it will "
            "arrive or fail on its own. Do not send it again, and do not tell the user "
            "it was sent.",
        ]

    @pytest.mark.asyncio
    async def test_a_code_this_build_does_not_know_shows_the_words(self, gateway):
        gateway.reply = httpx.Response(
            200,
            json={
                "status": "unknown",
                "code": "some_new_code",
                "message": "Here is what happened.",
            },
        )

        content = await _call(_tool("send_message"), {"text": "hi"})

        assert content == "status: unknown\ncode: some_new_code\nHere is what happened."

    @pytest.mark.asyncio
    async def test_a_repeat_answered_as_before_the_in_flight_code_reads_as_before(
        self, gateway
    ):
        # The same repeat, answered by a messaging service without the code.
        words = (
            "This same message is still being delivered. Don't send it again; it "
            "will arrive or fail on its own."
        )
        gateway.reply = httpx.Response(
            200,
            json={
                "status": "failed",
                "code": "unavailable",
                "message": words,
                "address": "discord:@me",
            },
        )

        content = await _call(_tool("send_message"), {"text": "hi"})

        assert content == f"status: failed\ncode: unavailable\nto: discord:@me\n{words}"


class TestNoAnswerIsStillAResult:
    """A gateway that is down, slow or misconfigured must not fail the turn,
    and must not leave the model guessing whether anything went out."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "error",
        [
            httpx.ConnectError("refused"),
            httpx.ConnectTimeout("slow to connect"),
            # Waiting for a free connection: the request was never written.
            httpx.PoolTimeout("no free connection"),
        ],
    )
    async def test_an_unreachable_gateway_sent_nothing(self, gateway, error):
        gateway.reply = error

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
    @pytest.mark.parametrize("status", [400, 404, 405, 408, 413, 429])
    async def test_a_refused_request_sent_nothing(self, gateway, status):
        # A 4xx, from the service or a proxy in front of it, acted on nothing.
        gateway.reply = httpx.Response(status, json={"detail": "Not Found"})

        content = await _call(_tool("send_message"), {"text": "hi"})

        assert content.startswith("status: failed\ncode: unavailable")
        assert f"failed ({status}). Nothing was sent." in content

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", [301, 302, 307, 308])
    async def test_a_redirect_sent_nothing(self, gateway, status):
        # Redirects are not followed, so the gateway's send never ran.
        gateway.reply = httpx.Response(status, headers={"location": "http://elsewhere.test/"})

        content = await _call(_tool("send_message"), {"text": "hi"})

        assert content.startswith("status: failed\ncode: unavailable")
        assert f"failed ({status}). Nothing was sent." in content

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "reply",
        [
            httpx.Response(500, text="boom"),
            httpx.Response(502, text="bad gateway"),
            httpx.Response(503, text="unavailable"),
            httpx.Response(200, text="not json"),
            httpx.Response(200, json=["not", "an", "object"]),
        ],
    )
    async def test_a_gateway_fault_leaves_delivery_unknown(self, gateway, reply):
        gateway.reply = reply

        content = await _call(_tool("send_message"), {"text": "hi"})

        assert content.startswith("status: unknown")

    @pytest.mark.asyncio
    async def test_a_send_without_a_call_id_says_so_in_the_log(self, gateway, caplog):
        """The messaging service answers a repeat by the call's id, so a send
        without one would go out again if replayed."""
        with caplog.at_level(logging.WARNING, logger=messaging.__name__):
            await messaging._send(
                text="hi",
                files=None,
                target=None,
                reply=False,
                new_thread=False,
                files_workspace_id=None,
                config={"configurable": CONFIGURABLE},
                tool_call_id="",
            )

        (request,) = gateway.requests
        assert json.loads(request.content)["tool_call_id"] is None
        assert "send without a tool call id" in caplog.text

    @pytest.mark.asyncio
    async def test_a_listing_that_fails_says_so(self, gateway):
        gateway.reply = httpx.ConnectError("refused")

        content = await _call(_tool("list_message_targets"), {})

        assert "could not be reached" in content
        assert "unknown right now" in content


class TestTheClient:
    """One client per worker, so calls reuse the gateway's connections."""

    @pytest.mark.asyncio
    async def test_calls_share_one_client_until_it_is_closed(self):
        first = messaging._client()
        assert messaging._client() is first

        await messaging.close_client()

        assert first.is_closed
        second = messaging._client()
        assert second is not first and not second.is_closed
        await messaging.close_client()

    @pytest.mark.asyncio
    async def test_a_cookie_one_answer_sets_is_not_kept(self):
        """Every user's calls share the client, so nothing one answer sets may
        ride the next call."""
        client = messaging._client()
        request = httpx.Request("GET", f"{GATEWAY}/agent/targets")
        answer = httpx.Response(
            200, headers={"set-cookie": "session=abc; Path=/"}, request=request
        )

        client.cookies.extract_cookies(answer)

        assert len(client.cookies.jar) == 0
        await messaging.close_client()


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
    async def test_a_replay_still_being_delivered_is_not_a_failure(self, gateway):
        gateway.reply = httpx.Response(
            200,
            json={
                "status": "unknown",
                "code": "in_flight",
                "message": "Still going.",
                "address": "discord:@me",
            },
        )

        artifact = (await _message(_tool("send_message"), {"text": "hi"})).artifact

        assert artifact["status"] == "unknown"
        assert artifact["code"] == "in_flight"
        # The messaging service's words are for the model; the card gets the
        # sentence a local unknown shows.
        assert artifact["message"] == "Whether the message went out is unknown."
        assert artifact["platform"] == "discord"

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

    @pytest.mark.parametrize("role", ["analyst", "chief_of_staff"])
    def test_the_schema_the_model_sees_has_no_new_args(self, configured, role):
        tool = _tool("send_message", role)

        expected = {"text", "files", "target", "reply", "new_thread"}
        if role == "chief_of_staff":
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


# -- files on the user's computers ----------------------------------------------


class _Computers:
    """Stands in for ``TurnComputers``: a path under ``/root/lithium`` is in
    that workspace's folder; anything else is copied into Home."""

    def __init__(self):
        self.asked: list[list[tuple[str, str]]] = []

    async def attach(self, files):
        from ptc_agent.agent.tools.computer_reach import Attached, ComputerUnreachable

        self.asked.append(list(files))
        if any(computer == "Basement" for computer, _ in files):
            raise ComputerUnreachable('The user has no computer named "Basement".')
        return [
            Attached(path.removeprefix("/root/lithium/"), NVDA)
            if path.startswith("/root/lithium/")
            else Attached(f"sent/{path.rsplit('/', 1)[-1]}", None, copied=1000)
            for _, path in files
        ]


def _anywhere(computers):
    tools = build_messaging_tools("chief_of_staff", computers=computers)
    return {t.name: t for t in tools}["send_message"]


class TestFilesOnComputers:
    @pytest.mark.asyncio
    async def test_a_named_file_goes_from_its_workspace_or_as_a_copy_in_home(self, gateway):
        computers = _Computers()
        gateway.reply = httpx.Response(
            200,
            json={
                "status": "sent",
                "address": "imessage:@me",
                "files": [
                    {"path": "reports/alb.md", "status": "sent"},
                    {"path": "sent/budget.pdf", "status": "sent"},
                    {"path": "digest.md", "status": "sent"},
                ],
            },
        )
        text = await _call(
            _anywhere(computers),
            {
                "text": "here",
                "target": "imessage:@me",
                "files": [
                    "Studio Mac:/root/lithium/reports/alb.md",
                    "Studio Mac:~/Documents/budget.pdf",
                    "digest.md",
                ],
            },
        )

        assert json.loads(gateway.requests[0].content)["files"] == [
            {"path": "reports/alb.md", "workspace_id": NVDA},
            {"path": "sent/budget.pdf", "workspace_id": None},
            {"path": "digest.md", "workspace_id": None},
        ]
        # Asked once for every file named on a computer, so none is copied
        # before all are found.
        assert computers.asked == [
            [
                ("Studio Mac", "/root/lithium/reports/alb.md"),
                ("Studio Mac", "~/Documents/budget.pdf"),
            ]
        ]
        assert "- Studio Mac:/root/lithium/reports/alb.md: sent" in text
        assert "- Studio Mac:~/Documents/budget.pdf (copied to Home as sent/budget.pdf): sent" in text
        assert "- digest.md: sent" in text

    @pytest.mark.asyncio
    async def test_a_file_that_cannot_go_sends_nothing(self, gateway):
        text = await _call(
            _anywhere(_Computers()),
            {"text": "x", "target": "imessage:@me", "files": ["Basement:/a.pdf"]},
        )
        assert gateway.requests == []
        assert "code: invalid_request" in text
        assert 'no computer named "Basement". Nothing was sent.' in text

    @pytest.mark.asyncio
    @pytest.mark.parametrize("path", ["reports/q3:final.md", "/tmp/x:/y", "~/x:/y"])
    async def test_a_path_with_a_colon_is_still_a_path(self, gateway, path):
        computers = _Computers()
        await _call(_anywhere(computers), {"text": "x", "files": [path]})
        assert computers.asked == []
        assert json.loads(gateway.requests[0].content)["files"] == [
            {"path": path, "workspace_id": None}
        ]

    @pytest.mark.asyncio
    async def test_a_computer_whose_name_holds_a_slash_is_still_a_computer(self, gateway):
        """Names may hold a slash, and the parse refused one in the name, so
        the spec went to the messaging service as a path in Home."""
        computers = _Computers()
        await _call(_anywhere(computers), {"text": "x", "files": ["Work/Lab:~/a.pdf"]})
        assert computers.asked == [[("Work/Lab", "~/a.pdf")]]

    def test_only_a_chief_of_staff_with_several_computers_is_told_of_it(self, configured):
        def files_doc(tool):
            return tool.tool_call_schema.model_json_schema()["properties"]["files"]["description"]

        assert "a colon" in files_doc(_anywhere(_Computers()))
        assert "a colon" not in files_doc(_tool("send_message", "chief_of_staff"))
        analyst = {t.name: t for t in build_messaging_tools("analyst", computers=_Computers())}
        assert "a colon" not in files_doc(analyst["send_message"])
