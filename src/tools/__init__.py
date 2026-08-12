
from .web.fetch import web_fetch_tool, web_fetch

# Backwards compatibility: re-export from new location
from ptc_agent.agent.tools.todo import TodoWrite


def get_web_search_tool(*args, **kwargs):
    from .web.search import get_web_search_tool as _get_web_search_tool

    return _get_web_search_tool(*args, **kwargs)


__all__ = [
    "web_fetch_tool",
    "web_fetch",
    "get_web_search_tool",
    "TodoWrite",
]
