"""A bot's identity reaches the system prompt as its own block.

The block is what makes "one conversation is one bot" visible to the model: a
name, the persona the owner wrote, and the rule that a teammate's message does
not become a new identity. Absent a bot (legacy callers), the prompt must be
byte-identical to the pre-bot build, which is why the block is conditional.
"""

from __future__ import annotations

from ptc_agent.agent.prompts import get_loader


def _prompt(**kwargs) -> str:
    return get_loader().get_system_prompt(**kwargs)


def test_no_bot_means_no_identity_block():
    assert "<bot_identity>" not in _prompt()


def test_a_bot_renders_its_name_and_persona():
    out = _prompt(bot={"name": "Quant", "role": "analyst", "persona": "Be terse."})
    assert "<bot_identity>" in out
    assert "Quant" in out
    assert "Be terse." in out
    # The framed-message rule is the prompt surface that keeps a teammate's
    # message from being read as the user speaking in a new voice.
    assert "never as a new identity" in out


def test_the_chief_of_staff_bot_says_whose_it_is():
    out = _prompt(
        bot={"name": "Chief of Staff", "role": "chief_of_staff", "persona": ""}
    )
    assert "the user's Chief of Staff" in out


def test_an_empty_persona_leaves_no_empty_persona_block():
    out = _prompt(bot={"name": "Scout", "role": "analyst", "persona": ""})
    assert "<persona>" not in out
    assert "Scout" in out
