"""A subagent's context: a static prompt shared by every user of its
definition, and a frozen baseline after it holding what is theirs."""

from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from ptc_agent.agent.context_stack import SubagentContext
from ptc_agent.agent.middleware.runtime_context import BaselineSources, MemoryTierSource
from ptc_agent.agent.subagents.compiler import SubagentCompiler
from ptc_agent.agent.subagents.definition import SubagentDefinition
from tests.unit.ptc_agent.agent._subagent_baseline import baseline_of, baseline_text

PROFILE = {
    "name": "Dana",
    "timezone": "Europe/London",
    "locale": "en-GB",
    "agent_preference": {"output_format": "html", "tone": "terse"},
}
FOLDER = "/home/workspace/w/.agents/scratchpad/0a1b2c3d/"


class _Store:
    def __init__(self, index: str | None) -> None:
        self._index = index

    async def aget(self, _namespace, _key):
        if self._index is None:
            return None
        return SimpleNamespace(value={"content": self._index})


def _sources(index: str | None = None) -> BaselineSources:
    return BaselineSources(
        store=_Store(index),
        memory={
            "user": MemoryTierSource(
                namespace_factory=lambda: ("u",), display_path=".agents/user/memory/memory.md"
            )
        },
    )


def _context(*, memory_index: str | None = None, scratchpad: str | None = FOLDER, sources=True):
    return SubagentContext(
        timezone="Europe/London",
        preferred_market="UK",
        sources=_sources(memory_index) if sources else None,
        skills=lambda _state: "## Available Skills\n\n- **xlsx**: Spreadsheets.",
        scratchpad=scratchpad,
    )


def _compile(context=None, profile=PROFILE, sandbox=None, drive=False, **fields):
    fields.setdefault("tools", ["execute_code", "filesystem", "bash"])
    defn = SubagentDefinition(
        name="helper", description="d", role_prompt="Do the task.", **fields
    )
    tool_sets = {name: [SimpleNamespace(name=name)] for name in ("execute_code", "filesystem", "bash", "web_search")}
    return SubagentCompiler(
        sandbox=sandbox, tool_sets=tool_sets, user_profile=profile, context=context, drive=drive
    ).compile(defn)


def _in_task(task_id: str = "k7Xm2p"):
    return patch(
        "ptc_agent.agent.context_stack.get_config",
        return_value={"configurable": {"checkpoint_ns": f"task:{task_id}|model:1"}},
    )


class TestStaticPrompt:
    def test_holds_nothing_of_the_user(self):
        prompt = _compile(_context(), memory=True)["system_prompt"]
        assert "Dana" not in prompt and "Europe/London" not in prompt
        assert "<user_profile>" not in prompt

    def test_is_the_same_for_two_users(self):
        one = _compile(_context(), memory=True)["system_prompt"]
        two = _compile(_context(), profile={"name": "Eli", "timezone": "Asia/Tokyo"}, memory=True)
        assert one == two["system_prompt"]

    @pytest.mark.parametrize(
        ("memory", "sources", "expected"),
        [(True, True, True), (False, True, False), (True, False, False)],
        ids=["on", "switched-off", "no-memory-this-turn"],
    )
    def test_memory_section_follows_the_switch_and_the_turn(self, memory, sources, expected):
        prompt = _compile(_context(sources=sources), memory=memory)["system_prompt"]
        assert ("<memory_system>" in prompt) is expected

    def test_scratchpad_section_only_for_a_subagent_that_writes_files(self):
        assert "<thread_scratchpad>" in _compile(_context())["system_prompt"]
        assert "<thread_scratchpad>" not in _compile(_context(), tools=["web_search"])["system_prompt"]
        assert "<thread_scratchpad>" not in _compile(_context(scratchpad=None))["system_prompt"]

    def test_security_policy_only_with_code_or_shell(self):
        assert "<security_policy>" in _compile(_context(), tools=["bash"])["system_prompt"]
        assert "<security_policy>" not in _compile(_context(), tools=["filesystem", "web_search"])["system_prompt"]

    def test_no_context_means_no_baseline(self):
        compiled = _compile(None, memory=True)
        assert baseline_of(compiled) is None
        assert "<memory_system>" not in compiled["system_prompt"]


class TestBaseline:
    def test_states_the_profile_without_the_main_agents_steering(self):
        with _in_task():
            text = baseline_text(_compile(_context()))
        assert "<user_profile>" in text and "Dana" in text and "terse" in text
        assert "Styled HTML" not in text
        assert "<user_identity>" in text and "Preferred market: UK" in text
        assert "<workspace>" not in text

    def test_withholds_the_trading_permission(self):
        # A subagent is bound no order tools, so a trading level would describe
        # calls it cannot make.
        with _in_task():
            text = baseline_text(
                _compile(_context(), profile={**PROFILE, "trading_permission": "autonomous"})
            )
        assert "Dana" in text
        assert "Full autonomy" not in text

    def test_states_the_skills(self):
        with _in_task():
            assert "<skills>" in baseline_text(_compile(_context()))

    def test_scratchpad_is_the_tasks_own_with_no_notes(self):
        with _in_task("k7Xm2p"):
            text = baseline_text(_compile(_context()))
        assert f'<scratchpad path="{FOLDER}k7Xm2p/"/>' in text

    def test_memory_index_is_read_and_never_asked_to_be_written(self):
        with _in_task():
            empty = baseline_text(_compile(_context(), memory=True))
            kept = baseline_text(_compile(_context(memory_index="- [Style](style.md) terse"), memory=True))
        assert "No .agents/user/memory/memory.md exists yet." in empty
        assert "create a typed detail file" not in empty
        assert "- [Style](style.md) terse" in kept

    @pytest.mark.parametrize(
        ("tools", "mounted", "opens"),
        [
            (["execute_code", "bash"], True, True),
            (["execute_code", "bash"], False, False),
            (["filesystem"], False, True),
        ],
        ids=["code-mounted", "code-unmounted", "file-tools"],
    )
    def test_memory_pointers_open_wherever_the_subagent_reaches_the_files(self, tools, mounted, opens):
        # File tools reach the memory files in process; code and shell only
        # through the file mount, so without it the index is read as text.
        sandbox = SimpleNamespace(livefs=object() if mounted else None)
        with _in_task():
            text = baseline_text(_compile(
                _context(memory_index="- [Style](style.md) terse"), sandbox=sandbox,
                tools=tools, memory=True,
            ))
        assert "- [Style](style.md) terse" in text
        assert ("cannot be opened" not in text) is opens

    @pytest.mark.parametrize(
        ("tools", "shown"),
        [
            (["execute_code", "bash"], False),
            (["bash"], False),
            (["execute_code"], False),
            (["filesystem"], True),
            (["execute_code", "filesystem", "bash"], True),
        ],
    )
    def test_the_drive_is_stated_only_to_a_subagent_with_the_file_tools(self, tools, shown):
        # Its section says to Read, Write, Edit, Glob and Grep it, and the
        # Drive is not on the file mount, so a mount up does not stand in.
        sandbox = SimpleNamespace(livefs=object())
        on = _compile(_context(), sandbox=sandbox, tools=tools, drive=True)
        off = _compile(_context(), sandbox=sandbox, tools=tools, drive=False)
        assert ("<drive_system>" in on["system_prompt"]) is shown
        assert "<drive_system>" not in off["system_prompt"]

    def test_no_render_carries_a_drive_block(self):
        # The agent looks in the Drive when a task needs it; nothing hands it
        # a map, so neither the main agent nor a subagent has one to read.
        from ptc_agent.agent.middleware.runtime_context.baseline import BLOCK_KINDS
        from ptc_agent.agent.prompts import init_loader

        main = init_loader().get_system_prompt(drive_enabled=True)
        with _in_task():
            compiled = _compile(_context(), sandbox=SimpleNamespace(livefs=object()), drive=True)
            text = baseline_text(compiled)
        assert "<drive_system>" in main and "<drive " not in main
        assert "<drive_system>" in compiled["system_prompt"]
        assert "<drive " not in compiled["system_prompt"] and "<drive " not in text
        assert "drive" not in BLOCK_KINDS

    def test_the_memos_ride_the_drive_section_on_the_main_agent_alone(self):
        # One section, never both: with the Drive on, the memos are its bullet.
        from ptc_agent.agent.prompts import init_loader

        memo_bullet = "- The user's memos, documents they wrote or uploaded"
        loader = init_loader()
        on = loader.get_system_prompt(drive_enabled=True, memo_enabled=True)
        off = loader.get_system_prompt(drive_enabled=False, memo_enabled=True)
        no_memo = loader.get_system_prompt(drive_enabled=True, memo_enabled=False)
        sub = _compile(_context(), sandbox=SimpleNamespace(livefs=object()), drive=True)
        assert memo_bullet in on and "<memo_system>" not in on
        assert "<memo_system>" in off and memo_bullet not in off
        assert memo_bullet not in no_memo and "<memo_system>" not in no_memo
        assert "<drive_system>" in sub["system_prompt"]
        assert memo_bullet not in sub["system_prompt"]

    def test_memory_switched_off_reads_no_index(self):
        with _in_task():
            text = baseline_text(_compile(_context(memory_index="- [Style](style.md)"), memory=False))
        assert "<memory" not in text


class _Config:
    """A turn config whose only resolved client is the one a test pins."""

    llm = None

    def __init__(self, client=None) -> None:
        self._client = client

    def client_for_role(self, role, *, fallback_to_main=False):
        return self._client

    def prompt_guidance_for_role(self, role):
        return "detailed"

    def feature_enabled(self, key):
        return False


class TestTheModel:
    """A subagent states the model it runs, which is not always the turn's."""

    def _text(self, *, client=None, turn="Claude Opus 5.5, reasoning effort high", **fields):
        context = replace(_context(), model=turn)
        defn = SubagentDefinition(
            name="helper", description="d", role_prompt="Do the task.",
            tools=["execute_code"], **fields,
        )
        compiler = SubagentCompiler(
            tool_sets={"execute_code": [SimpleNamespace(name="execute_code")]},
            user_profile=PROFILE, context=context, config=_Config(client),
        )
        with _in_task():
            return baseline_text(compiler.compile(defn))

    def test_inheriting_the_turn_s_model_states_the_turn_s(self):
        text = self._text()
        assert "<model>\nYou run on Claude Opus 5.5, reasoning effort high.\n</model>" in text
        # It cannot pick its model, so it is not told who does.
        assert "la models list" not in text

    def test_its_own_client_states_the_model_that_client_calls(self):
        client = SimpleNamespace(
            metadata={"manifest_model": "my-own-model", "reasoning_effort": "low"}
        )
        text = self._text(client=client)
        assert "You run on my-own-model, reasoning effort low." in text
        assert "Claude Opus 5.5" not in text

    def test_a_pinned_name_with_no_client_states_that_name(self):
        text = self._text(model="my-own-model")
        assert "You run on my-own-model." in text
        assert "Claude Opus 5.5" not in text
