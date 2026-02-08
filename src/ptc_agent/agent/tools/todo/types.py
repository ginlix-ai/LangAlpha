"""Todo tracking data models for agent task management.

This module provides data structures for tracking agent todos throughout workflow execution.
Follows Anthropic's Claude Code best practices for todo management.
"""

from datetime import datetime
from enum import Enum
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field, field_validator


class TodoStatus(str, Enum):
    """Status of a todo item."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"


class TodoItem(BaseModel):
    """Individual todo item with tracking metadata."""

    content: str = Field(description="Description of the task (e.g., 'Run the build')")
    activeForm: str = Field(description="Present continuous form (e.g., 'Running the build')")
    status: TodoStatus = Field(description="Current status of the todo")
    id: Optional[str] = Field(default=None, description="Unique identifier for the todo")
    created_at: datetime = Field(default_factory=datetime.now, description="When the todo was created")
    updated_at: datetime = Field(default_factory=datetime.now, description="When the todo was last updated")

    @field_validator("status", mode="before")
    @classmethod
    def normalize_status(cls, v: Any) -> Any:
        """Normalize status values to TodoStatus enum."""
        if isinstance(v, TodoStatus):
            return v
        if isinstance(v, str):
            # Handle string values
            status_map = {
                "pending": TodoStatus.PENDING,
                "in_progress": TodoStatus.IN_PROGRESS,
                "completed": TodoStatus.COMPLETED,
            }
            return status_map.get(v.lower(), v)
        return v

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format for tool calls."""
        return {
            "content": self.content,
            "activeForm": self.activeForm,
            "status": self.status.value if isinstance(self.status, TodoStatus) else self.status,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TodoItem":
        """Create TodoItem from dictionary."""
        return cls(
            content=data.get("content", ""),
            activeForm=data.get("activeForm", ""),
            status=data.get("status", "pending"),
            id=data.get("id"),
            created_at=data.get("created_at", datetime.now()),
            updated_at=data.get("updated_at", datetime.now()),
        )


def validate_single_in_progress(todos: List[TodoItem]) -> tuple[bool, List[str]]:
    """Validate that only one todo is in_progress.

    Args:
        todos: List of TodoItem objects to validate

    Returns:
        Tuple of (is_valid, list_of_errors)
    """
    in_progress_count = sum(1 for todo in todos if todo.status == TodoStatus.IN_PROGRESS)

    if in_progress_count == 0:
        return False, ["No todo is marked as in_progress. Exactly one todo should be in_progress."]
    elif in_progress_count > 1:
        in_progress_todos = [todo.content for todo in todos if todo.status == TodoStatus.IN_PROGRESS]
        return False, [
            f"Multiple todos marked as in_progress: {', '.join(in_progress_todos)}. "
            "Only ONE todo should be in_progress at a time."
        ]

    return True, []


def validate_todo_list_dict(todos: List[Dict[str, Any]]) -> tuple[bool, List[str]]:
    """Validate a list of todos in dictionary format.

    Only validates format requirements (missing fields, invalid values).
    Does NOT enforce workflow rules like "only one in_progress" since
    concurrent scenarios (e.g., parallel research) legitimately allow
    multiple tasks in progress simultaneously.

    Args:
        todos: List of todo dictionaries to validate

    Returns:
        Tuple of (is_valid, list_of_errors)
    """
    if not todos:
        return True, []  # Empty list is valid

    errors = []

    # Valid status values
    valid_statuses = {
        "pending", "in_progress", "completed",
    }

    # Validate required fields and value formats
    for i, todo in enumerate(todos):
        if not todo.get("content"):
            errors.append(f"Todo at index {i} is missing 'content' field")
        if not todo.get("activeForm"):
            errors.append(f"Todo at index {i} is missing 'activeForm' field")
        if not todo.get("status"):
            errors.append(f"Todo at index {i} is missing 'status' field")
        elif todo.get("status") not in valid_statuses:
            errors.append(
                f"Todo at index {i} has invalid status: '{todo.get('status')}'. "
                f"Expected one of: {', '.join(sorted(valid_statuses))}"
            )

    return len(errors) == 0, errors
