"""Where past threads reach only the file tools, both roles' prompts say so."""

import pytest

from ptc_agent.agent.prompts import get_loader

_BASE = dict(current_time="2026-10-03 11:00 ET", subagent_summary="", workspace_folder="NVDA")
_ONLY_FILE_TOOLS = "only Read, Grep and Glob reach them"


@pytest.mark.parametrize("role", ["analyst", "chief_of_staff"])
def test_only_a_computer_without_the_mount_sends_history_to_the_file_tools(role):
    loader = get_loader()

    def prompt(**flags):
        return loader.get_system_prompt(**_BASE, role=role, **flags)

    unmounted = prompt(history_files=True)
    assert "../.agents/threads.jsonl" in unmounted and _ONLY_FILE_TOOLS in unmounted
    mounted = prompt(files_mounted=True)
    assert "../.agents/threads.jsonl" in mounted and _ONLY_FILE_TOOLS not in mounted
    assert "../.agents/threads.jsonl" not in prompt()
