from typing import Any

import pytest


class _RecordingClient:
    """Stands in for the LangSmith client: records the runs it would update."""

    def __init__(self) -> None:
        self.updated: list[dict[str, Any]] = []

    def create_run(self, **kwargs: Any) -> None:
        return None

    def update_run(self, **kwargs: Any) -> None:
        self.updated.append(kwargs)

    def flush(self) -> None:
        return None


@pytest.fixture
def traced_tool():
    """Invoke a tool under the real tracer: its result, and its tool run's metadata."""
    from src.observability.langsmith_tracer import NonRetainingTracer

    async def invoke(tool: Any, call: Any) -> tuple[Any, dict[str, Any]]:
        client = _RecordingClient()
        tracer = NonRetainingTracer(project_name="proj", client=client)
        result = await tool.ainvoke(call, {"callbacks": [tracer]})
        (update,) = [u for u in client.updated if u["run_type"] == "tool"]
        return result, update["extra"]["metadata"]

    return invoke
