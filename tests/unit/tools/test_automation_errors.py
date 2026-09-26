import pytest
from pydantic import ValidationError

from src.server.models.automation import AutomationCreate
from src.tools.automation.tools import _error_text


def _refusal(**trigger):
    with pytest.raises(ValidationError) as exc:
        AutomationCreate(name="A", instruction="x", trigger_type="price", **trigger)
    return _error_text(exc.value)


def test_a_refused_trigger_reads_as_the_models_sentence():
    """The agent acts on this text, so pydantic's framing stays out of it."""
    assert _refusal() == "trigger_config is required for trigger_type='price'"


def test_a_refused_price_config_names_what_in_it_is_wrong():
    text = _refusal(trigger_config={"symbol": "AAPL", "conditions": []})

    assert text.startswith("trigger_config: Invalid price trigger config: conditions: ")
    assert "pydantic" not in text and "input_value" not in text


@pytest.mark.asyncio
async def test_an_ownership_refusal_reaches_the_agent_as_its_reason(monkeypatch):
    """The handler refuses a foreign target with an HTTP error; the agent gets its detail."""
    from fastapi import HTTPException

    from src.tools.automation import tools

    async def refuse(*_args, **_kwargs):
        raise HTTPException(status_code=403, detail="Forbidden")

    monkeypatch.setattr(tools.auto_handler, "pause_automation", refuse)

    result = await tools.manage_automation.coroutine(
        automation_id="00000000-0000-4000-8000-0000000000a1", action="pause", config={"configurable": {"user_id": "u1"}}
    )

    assert result == {"error": "Forbidden"}


@pytest.mark.asyncio
async def test_an_id_that_cannot_name_an_automation_is_not_found(monkeypatch):
    """A malformed id would reach the uuid cast; the agent gets the not-found error instead."""
    from src.tools.automation import tools

    async def unreachable(*_args, **_kwargs):
        raise AssertionError("a malformed id reached the database")

    monkeypatch.setattr(tools.auto_db, "get_automation", unreachable)
    monkeypatch.setattr(tools.auto_handler, "pause_automation", unreachable)
    config = {"configurable": {"user_id": "u1"}}

    content, _ = await tools.check_automations.coroutine(config=config, automation_id="abc")
    managed = await tools.manage_automation.coroutine(
        automation_id="abc", action="pause", config=config
    )

    assert content == '{"error": "Automation \'abc\' not found."}'
    assert managed == {"error": "Automation 'abc' not found."}
