"""The scratchpad is opt-in: a user who never turned it on sees none of it."""

import pytest

from ptc_agent.agent.prompts import guidance_template_vars, init_loader
from ptc_agent.agent.prompts.formatter import workspace_path_vars


@pytest.mark.parametrize("guidance", ["detailed", "lean"])
def test_off_the_system_prompt_carries_no_scratchpad_guidance(guidance):
    prompt = init_loader().get_system_prompt(
        **guidance_template_vars(guidance),
        **workspace_path_vars(None, root="/home/workspace"),
        subagent_summary="STUB",
    )

    assert "<thread_scratchpad>" not in prompt
    assert ".agents/scratchpad" not in prompt
    assert "All intermediate files go in your task directory." in prompt


@pytest.mark.parametrize("guidance", ["detailed", "lean"])
def test_on_each_task_starts_a_checkpoint_note_by_its_absolute_path(guidance):
    prompt = init_loader().get_system_prompt(
        **guidance_template_vars(guidance),
        **workspace_path_vars(None, root="/home/workspace"),
        subagent_summary="STUB",
        scratchpad_enabled=True,
    )

    assert "<thread_scratchpad>\n# Scratchpad" in prompt
    assert "/home/workspace/.agents/scratchpad/<thread>/note/<task_name>.md" in prompt
    assert "All intermediate files go in your task directory." not in prompt
