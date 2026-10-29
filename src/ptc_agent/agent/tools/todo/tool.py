import logging
from typing import List, Annotated
from langchain_core.tools import tool
from pydantic import Field

from .types import TodoItem, TodoStatus

logger = logging.getLogger(__name__)


@tool(parse_docstring=True)
def TodoWrite(
    todos: Annotated[
        List[TodoItem],
        Field(
            description=(
                "Full replacement of the todo list. Pass the ENTIRE collection every "
                "call, not just changes. Each item must have content, activeForm, and status."
            )
        ),
    ],
) -> str:
    """Replace the agent's todo list with the provided collection.

    Each call fully replaces the previous list. The tool has no read interface,
    so track the list in memory and pass the complete set every time. Mark an
    item `completed` as soon as it is done rather than batching.

    Args:
        todos: Full replacement of the todo list. Pass the ENTIRE collection every
            call, not just changes. Each item must have content, activeForm, and status.

    Returns:
        str: Confirmation message describing the resulting list state.

    Example:
        TodoWrite(todos=[
            {"content": "Fetch Q3 earnings", "activeForm": "Fetching Q3 earnings", "status": "in_progress"},
            {"content": "Summarize key metrics", "activeForm": "Summarizing key metrics", "status": "pending"},
        ])
    """
    logger.info(f"Todo list updated with {len(todos)} items")

    status_counts = {
        TodoStatus.PENDING: 0,
        TodoStatus.IN_PROGRESS: 0,
        TodoStatus.COMPLETED: 0,
    }
    for todo in todos:
        status_counts[todo.status] += 1
        logger.debug(f"  - [{todo.status.value}] {todo.content}")

    total = len(todos)
    completed = status_counts[TodoStatus.COMPLETED]
    in_progress = status_counts[TodoStatus.IN_PROGRESS]
    pending = status_counts[TodoStatus.PENDING]
    remaining = pending + in_progress

    if total > 0 and completed == total:
        logger.info("All todos completed")
        return (
            "✓ All tasks completed! You can now proceed to the next stage "
            "or add more tasks if needed."
        )

    if total > 1 and remaining == 1:
        logger.info("One task remaining")
        return (
            "Todos have been modified successfully. Ensure that you continue to use "
            "the todo list to track your progress. Please proceed with the current "
            "tasks if applicable\n\n"
            "💡 Reminder: One task remaining — remember to mark it as completed after "
            "finishing it."
        )

    return (
        "Todos have been modified successfully. Ensure that you continue to use the "
        "todo list to track your progress. Please proceed with the current tasks if "
        "applicable"
    )
