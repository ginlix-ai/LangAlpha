"""An automation run that delivers through the messaging service: the start
that hands the run over, what the agent is told, and the finish whose answer
becomes the execution's ``delivery_result``.

The messaging service is faked at the HTTP transport, so these pin the
requests this server makes and how it reads every answer, including none.
"""

from __future__ import annotations

import json

import httpx
import pytest

from src.config import env
from src.server.services import automation_delivery as delivery
from src.server.services.automation_delivery import Target
from src.tools.messaging import tools as messaging

SERVICE = "http://messaging.test/api/prefix"
TOKEN = "svc-token"
EXECUTION_ID = "exec-1"

AUTOMATION = {
    "automation_id": "auto-1",
    "user_id": "user-1",
    "name": "Morning brief",
    "delivery_config": {"methods": ["slack:T/C", "discord", "telegram"]},
}

DEMO = Target(entry="slack:T/C", address="slack:T/C", name="#demo", ok=True)
DISCORD_DM = Target(entry="discord", address="discord:@me", name=None, ok=True)
UNLINKED = Target(
    entry="telegram", address=None, name=None, ok=False, message="Telegram isn't linked"
)


class FakeService:
    """Answers ``reply`` (a response, or an exception to raise) and keeps
    every request."""

    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self.reply: httpx.Response | Exception = httpx.Response(200, json={})

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if isinstance(self.reply, Exception):
            raise self.reply
        return self.reply

    @property
    def body(self) -> dict:
        (request,) = self.requests
        return json.loads(request.content)


@pytest.fixture
def service(monkeypatch) -> FakeService:
    fake = FakeService()
    transport = httpx.MockTransport(fake.handle)
    monkeypatch.setattr(env, "CHANNEL_GATEWAY_URL", SERVICE)
    monkeypatch.setenv("INTERNAL_SERVICE_TOKEN", TOKEN)
    monkeypatch.setattr(
        messaging,
        "_client",
        lambda timeout: httpx.AsyncClient(transport=transport, timeout=timeout),
    )
    return fake


def _started(*targets: Target) -> httpx.Response:
    return httpx.Response(200, json={"targets": [t.as_dict() for t in targets]})


# -- start --------------------------------------------------------------------


class TestStart:
    @pytest.mark.asyncio
    async def test_it_hands_the_run_over_and_answers_every_target(self, service):
        service.reply = _started(DEMO, DISCORD_DM, UNLINKED)

        targets = await delivery.start_run(AUTOMATION, EXECUTION_ID, "ws-1")

        assert targets == [DEMO, DISCORD_DM, UNLINKED]
        (request,) = service.requests
        assert request.method == "POST"
        assert str(request.url) == f"{SERVICE}/agent/automation-runs"
        assert request.headers["X-Service-Token"] == TOKEN
        assert request.headers["X-User-Id"] == "user-1"
        assert service.body == {
            "execution_id": EXECUTION_ID,
            "workspace_id": "ws-1",
            "automation_name": "Morning brief",
            "entries": ["slack:T/C", "discord", "telegram"],
        }

    @pytest.mark.asyncio
    async def test_no_entries_asks_nothing(self, service):
        automation = {**AUTOMATION, "delivery_config": {"methods": []}}

        assert await delivery.start_run(automation, EXECUTION_ID, "ws-1") is None
        assert await delivery.start_run(
            {**AUTOMATION, "delivery_config": None}, EXECUTION_ID, "ws-1"
        ) is None
        assert service.requests == []

    @pytest.mark.asyncio
    async def test_no_messaging_service_asks_nothing(self, service, monkeypatch):
        monkeypatch.setattr(env, "CHANNEL_GATEWAY_URL", "")

        assert await delivery.start_run(AUTOMATION, EXECUTION_ID, "ws-1") is None
        assert service.requests == []

    @pytest.mark.asyncio
    async def test_no_entry_resolved_leaves_the_run_to_the_webhook(self, service):
        service.reply = _started(UNLINKED)

        assert await delivery.start_run(AUTOMATION, EXECUTION_ID, "ws-1") is None

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "reply",
        [
            httpx.ConnectError("refused"),
            httpx.ReadTimeout("slow"),
            httpx.Response(401, json={}),
            httpx.Response(503, json={"code": "unavailable", "message": "down"}),
            # Only a 200 is an answer, whatever the body says.
            httpx.Response(500, json={"targets": [DEMO.as_dict()]}),
            httpx.Response(200, json={"targets": "nope"}),
            httpx.Response(200, text="not json"),
            RuntimeError("anything else"),
        ],
    )
    async def test_a_start_that_fails_leaves_the_run_to_the_webhook(self, service, reply):
        service.reply = reply

        assert await delivery.start_run(AUTOMATION, EXECUTION_ID, "ws-1") is None

    @pytest.mark.asyncio
    async def test_an_unreadable_target_is_dropped(self, service):
        service.reply = httpx.Response(
            200, json={"targets": [DEMO.as_dict(), {"address": "x"}, "junk"]}
        )

        assert await delivery.start_run(AUTOMATION, EXECUTION_ID, "ws-1") == [DEMO]


class TestTheRunRowStamp:
    def test_the_targets_come_back_from_the_run_row(self):
        run = {"metadata": {"other": 1, **delivery.run_metadata([DEMO, UNLINKED])}}

        assert delivery.targets_of_run(run) == [DEMO, UNLINKED]

    @pytest.mark.parametrize(
        "run",
        [None, {}, {"metadata": None}, {"metadata": {"other": 1}}, {"metadata": "x"}],
    )
    def test_a_run_with_no_stamp_delivers_by_webhook(self, run):
        assert delivery.targets_of_run(run) is None


# -- what the agent is told ------------------------------------------------------


class TestTheReminder:
    def test_it_names_each_chat_the_run_reaches(self):
        assert delivery.reminder([DEMO, DISCORD_DM]) == (
            "When you finish, send the result with send_message to: "
            "#demo (Slack) `slack:T/C`, your Discord DM `discord:@me`. "
            "Write it for chat; attach files for detail."
        )

    def test_an_entry_that_did_not_resolve_is_left_out(self):
        refused = Target(
            entry="slack:T/X", address="slack:T/X", name="#x", ok=False, message="no bot"
        )

        text = delivery.reminder([DEMO, UNLINKED, refused])

        assert "telegram" not in text.lower()
        assert "slack:T/X" not in text

    @pytest.mark.parametrize(
        "address, name, described",
        [
            ("slack:T1", None, "your Slack DM `slack:T1`"),
            ("imessage", None, "your iMessage DM `imessage`"),
            ("telegram:@me", None, "your Telegram DM `telegram:@me`"),
            ("telegram:-100", None, "Telegram `telegram:-100`"),
            ("discord:G/C", "#ops", "#ops (Discord) `discord:G/C`"),
        ],
    )
    def test_each_kind_of_chat(self, address, name, described):
        target = Target(entry=address, address=address, name=name, ok=True)

        assert f"to: {described}." in delivery.reminder([target])


# -- finish -------------------------------------------------------------------


def _finished(*targets: dict) -> httpx.Response:
    return httpx.Response(200, json={"targets": list(targets)})


def _landed(entry, address, name, *, reached, via, error=None) -> dict:
    return {
        "entry": entry,
        "address": address,
        "name": name,
        "reached": reached,
        "via": via,
        "error": error,
    }


async def _finish(status="completed", *, targets=(DEMO, DISCORD_DM), final_text="The brief."):
    return await delivery.finish_run(
        AUTOMATION, EXECUTION_ID, status, targets=list(targets), final_text=final_text
    )


class TestTheFinishRequest:
    @pytest.mark.asyncio
    async def test_a_completed_run_hands_over_its_answer(self, service):
        service.reply = _finished()

        await _finish("completed", final_text="The brief.")

        (request,) = service.requests
        assert request.method == "POST"
        assert str(request.url) == f"{SERVICE}/agent/automation-runs/{EXECUTION_ID}/finish"
        assert request.headers["X-User-Id"] == "user-1"
        assert service.body == {"status": "completed", "final_text": "The brief."}

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", ["failed", "stopped"])
    async def test_a_run_that_did_not_complete_hands_over_no_answer(self, service, status):
        service.reply = _finished()

        await _finish(status, final_text="half an answer")

        assert service.body == {"status": status}

    @pytest.mark.asyncio
    @pytest.mark.parametrize("final_text", [None, "", "  \n"])
    async def test_a_blank_answer_is_not_sent(self, service, final_text):
        service.reply = _finished()

        await _finish("completed", final_text=final_text)

        assert service.body == {"status": "completed"}

    @pytest.mark.asyncio
    async def test_a_long_answer_is_cut_to_what_a_message_carries(self, service):
        service.reply = _finished()
        cap = messaging.MAX_TEXT_CHARS

        await _finish("completed", final_text="x" * (cap + 50))
        sent = service.body["final_text"]

        assert len(sent) == cap
        assert sent.endswith("x…")

    @pytest.mark.asyncio
    async def test_an_answer_at_the_cap_is_sent_whole(self, service):
        service.reply = _finished()
        text = "y" * messaging.MAX_TEXT_CHARS

        await _finish("completed", final_text=text)

        assert service.body["final_text"] == text


class TestTheDeliveryResult:
    @pytest.mark.asyncio
    async def test_each_target_says_where_it_landed(self, service):
        service.reply = _finished(
            _landed("slack:T/C", "slack:T/C", "#demo", reached=True, via="agent"),
            _landed("discord", "discord:@me", None, reached=False, via="fallback"),
            _landed("imessage", "imessage", None, reached=False, via="notice"),
            _landed(
                "telegram", None, None, reached=False, via=None, error="Telegram isn't linked"
            ),
        )

        result = await _finish()

        assert result == [
            {
                "method": "slack:T/C",
                "address": "slack:T/C",
                "name": "#demo",
                "success": True,
                "via": "agent",
                "error": None,
            },
            {
                "method": "discord",
                "address": "discord:@me",
                "name": None,
                "success": True,
                "via": "fallback",
                "error": None,
            },
            {
                "method": "imessage",
                "address": "imessage",
                "name": None,
                "success": True,
                "via": "notice",
                "error": None,
            },
            {
                "method": "telegram",
                "address": None,
                "name": None,
                "success": False,
                "via": None,
                "error": "Telegram isn't linked",
            },
        ]

    @pytest.mark.asyncio
    async def test_reached_alone_is_a_success(self, service):
        service.reply = _finished(
            _landed("slack:T/C", "slack:T/C", "#demo", reached=True, via=None)
        )

        (attempt,) = await _finish()

        assert attempt["success"] is True

    @pytest.mark.asyncio
    async def test_an_unknown_via_is_not_trusted(self, service):
        service.reply = _finished(
            _landed("slack:T/C", "slack:T/C", "#demo", reached=False, via="carrier pigeon")
        )

        (attempt,) = await _finish()

        assert attempt["via"] is None
        assert attempt["success"] is False

    @pytest.mark.asyncio
    async def test_an_unreadable_item_is_dropped(self, service):
        service.reply = _finished(
            "junk", _landed("discord", "discord:@me", None, reached=True, via="agent")
        )

        result = await _finish()

        assert [a["method"] for a in result] == ["discord"]


class TestAFinishWithNoAnswer:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "reply, why",
        [
            (
                httpx.Response(404, json={"detail": "Not Found"}),
                "The messaging service has no record of this run.",
            ),
            (httpx.Response(500, text="boom"), "The messaging service failed (500)."),
            (
                httpx.Response(200, json={"targets": None}),
                "The messaging service sent an answer that could not be read.",
            ),
            (httpx.ConnectError("refused"), "The messaging service could not be reached."),
            (httpx.ReadTimeout("slow"), "The messaging service did not answer in time."),
            (RuntimeError("anything else"), "The finish could not be sent."),
        ],
    )
    async def test_every_target_is_recorded_as_failed(self, service, reply, why):
        service.reply = reply

        result = await _finish(targets=(DEMO, DISCORD_DM, UNLINKED))

        error = f"Delivery couldn't be confirmed. {why}"
        assert result == [
            {
                "method": "slack:T/C",
                "address": "slack:T/C",
                "name": "#demo",
                "success": False,
                "via": None,
                "error": error,
            },
            {
                "method": "discord",
                "address": "discord:@me",
                "name": None,
                "success": False,
                "via": None,
                "error": error,
            },
            # A target the start refused failed for its own reason.
            {
                "method": "telegram",
                "address": None,
                "name": None,
                "success": False,
                "via": None,
                "error": "Telegram isn't linked",
            },
        ]

    @pytest.mark.asyncio
    async def test_with_no_targets_held_each_entry_is_recorded(self, service):
        service.reply = httpx.ConnectError("refused")

        result = await _finish(targets=())

        assert [(a["method"], a["success"], a["address"]) for a in result] == [
            ("slack:T/C", False, None),
            ("discord", False, None),
            ("telegram", False, None),
        ]
        assert all(a["error"].startswith("Delivery couldn't be confirmed.") for a in result)
