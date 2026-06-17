from datetime import UTC, datetime

from ptc_agent.agent.prompts import PromptLoader


def _loader() -> PromptLoader:
    return PromptLoader(session_start_time=datetime(2026, 6, 7, tzinfo=UTC))


def _assert_a_share_lot_rule(prompt: str) -> None:
    assert "Mainland China A-shares" in prompt
    assert "multiple of 100 shares" in prompt
    assert "50-70 shares" in prompt
    assert "301189.SZ" in prompt


def test_ptc_system_prompt_includes_a_share_lot_rule() -> None:
    prompt = _loader().get_system_prompt(
        tool_summary="",
        subagent_summary="",
        current_time="2026-06-07 09:00",
        thread_id="test-thread",
    )

    _assert_a_share_lot_rule(prompt)


def test_flash_system_prompt_includes_a_share_lot_rule() -> None:
    prompt = _loader().render(
        "flash_system.md.j2",
        current_time="2026-06-07 09:00",
        user_profile=None,
    )

    _assert_a_share_lot_rule(prompt)


def test_subagent_base_prompt_includes_a_share_lot_rule() -> None:
    prompt = _loader().get_subagent_base_prompt(
        identity_line="You are a test subagent.",
        current_time="2026-06-07 09:00",
        user_profile=None,
        sections={},
    )

    _assert_a_share_lot_rule(prompt)
