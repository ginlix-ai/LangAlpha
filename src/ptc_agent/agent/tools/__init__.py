"""Open PTC Agent Tools Package.

This package contains all tools available to the PTC agent:
- bash: Bash command execution
- code_execution: Python code execution with MCP tool access
- file_ops: File read/write/edit operations
- glob: File pattern matching
- grep: Content search (ripgrep-based)
- think: Strategic reflection for research
- todo: Task tracking and progress management

Note: Web search tools are provided via src/tools/search.py (configurable: Tavily, Bocha, Serper).

Note: With deepagent, most filesystem tools (ls, read_file, write_file, edit_file,
glob, grep) are provided by the FilesystemMiddleware. These LangChain tool wrappers
are available for alternative agent configurations.
"""

from .bash import create_execute_bash_tool
from .code_execution import create_execute_code_tool
from .file_ops import create_filesystem_tools
from .glob import create_glob_tool
from .grep import create_grep_tool
from .preview_url import create_preview_url_tool
from .show_widget import create_show_widget_tool
from .think import think_tool

# Todo tracking
from .todo import (
    TodoWrite,
    TodoItem,
    TodoStatus,
    checkout_todos,
    checkin_todos,
    extract_todos_from_messages,
    mark_in_progress,
    mark_completed,
    add_todo,
    remove_todo,
    get_next_pending_todo,
    validate_single_in_progress,
    validate_todo_list_dict,
)

__all__ = [
    # Bash
    "create_execute_bash_tool",
    # Code execution
    "create_execute_code_tool",
    # Filesystem
    "create_filesystem_tools",
    # Search
    "create_glob_tool",
    "create_grep_tool",
    # Preview URL
    "create_preview_url_tool",
    # Show Widget
    "create_show_widget_tool",
    # Research
    "think_tool",
    # Todo tracking
    "TodoWrite",
    "TodoItem",
    "TodoStatus",
    "checkout_todos",
    "checkin_todos",
    "extract_todos_from_messages",
    "mark_in_progress",
    "mark_completed",
    "add_todo",
    "remove_todo",
    "get_next_pending_todo",
    "validate_single_in_progress",
    "validate_todo_list_dict",
]

