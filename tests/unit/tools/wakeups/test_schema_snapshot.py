"""The schema ``schedule_wakeup`` shows the model is prompt surface: every
field name, description and limit in it is read at call time. It is stored
as a snapshot so a change is a decision, not a side effect.

To change it on purpose, regenerate ``schema_snapshot.json`` from the two
expressions below and review the diff like a prompt edit.
"""

import json
from pathlib import Path

from langchain_core.utils.function_calling import convert_to_openai_tool

from src.tools.wakeups.models import ScheduleWakeupArgs
from src.tools.wakeups.tool import build_schedule_wakeup_tool

SNAPSHOT = json.loads((Path(__file__).parent / "schema_snapshot.json").read_text())


def test_the_args_model_schema_is_the_snapshot():
    assert ScheduleWakeupArgs.model_json_schema() == SNAPSHOT["ScheduleWakeupArgs"]


def test_the_tool_as_the_model_sees_it_is_the_snapshot():
    # name, description and parameters, after the tool node's injected
    # arguments (state, tool_call_id) are left out.
    seen = convert_to_openai_tool(build_schedule_wakeup_tool())
    assert seen == SNAPSHOT["as_the_model_sees_it"]
    assert set(seen["function"]["parameters"]["properties"]) == {
        "triggers",
        "cancel",
        "quiet",
    }
