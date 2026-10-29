"""Where the two subagent capture middleware sit in the stack every subagent
is built on.

The result capture has to see the message the subagent's checkpoint is
written with, since replay projects from that checkpoint: outermost of the
tool wrappers, so it also streams the time-up stop's refusal, with nothing
between it and eviction that changes a result. The tool-call capture and
metrics stay innermost.
"""

from unittest.mock import MagicMock, patch

import pytest
from langchain_core.language_models.fake_chat_models import GenericFakeChatModel

from ptc_agent.agent.middleware import (
    LargeResultEvictionMiddleware,
    SubAgentMiddleware,
)
from ptc_agent.agent.middleware.background_subagent import (
    SubagentEventCaptureMiddleware,
    SubagentToolResultCaptureMiddleware,
)
from ptc_agent.agent.middleware.runtime_context import TimeUpToolStopMiddleware
from ptc_agent.config.agent import AgentConfig
from tests.unit.ptc_agent.agent.test_time_up_stop_placement import (
    _config,
    _fake_graph,
    _wraps_tools,
)


@pytest.fixture(scope="module")
def subagent_stack() -> list:
    from ptc_agent.agent.agent import PTCAgent

    llm = GenericFakeChatModel(messages=iter([]))
    handed: list[list] = []
    real = SubAgentMiddleware

    def spy(*args, **kwargs):
        handed.append(list(kwargs["default_middleware"]))
        return real(*args, **kwargs)

    with (
        patch.object(AgentConfig, "get_llm_client", return_value=llm),
        patch(
            "ptc_agent.agent.middleware.compaction.compact.create_llm",
            return_value=llm,
        ),
        patch("ptc_agent.agent.agent.create_agent", side_effect=_fake_graph([])),
        patch("ptc_agent.agent.subagents.assembly.SubAgentMiddleware", side_effect=spy),
    ):
        PTCAgent(_config()).create_agent(
            llm=llm, thread_id="t-1", sandbox=MagicMock(), mcp_registry=MagicMock()
        )
    return handed[0]


def test_results_are_captured_outside_the_time_up_stop_and_eviction(subagent_stack):
    wrappers = [type(m) for m in subagent_stack if _wraps_tools(m)]
    assert wrappers[:3] == [
        SubagentToolResultCaptureMiddleware,
        TimeUpToolStopMiddleware,
        LargeResultEvictionMiddleware,
    ]
    assert wrappers.count(SubagentToolResultCaptureMiddleware) == 1


def test_tool_calls_and_metrics_are_captured_innermost(subagent_stack):
    assert isinstance(subagent_stack[-1], SubagentEventCaptureMiddleware)
    assert sum(isinstance(m, SubagentEventCaptureMiddleware) for m in subagent_stack) == 1
