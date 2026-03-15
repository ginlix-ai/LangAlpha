
import enum


class SearchEngine(enum.Enum):
    TAVILY = "tavily"
    BOCHA = "bocha"
    SERPER = "serper"


def _get_search_api() -> str:
    """Get search API from agent_config.yaml via shared YAML cache."""
    from src.config.core import load_yaml_config, find_config_file
    path = find_config_file("agent_config.yaml")
    if path is None:
        return "tavily"
    config = load_yaml_config(str(path))
    return str(config.get("search_api", "tavily"))


# Tool configuration loaded from agent_config.yaml
SELECTED_SEARCH_ENGINE = _get_search_api()
