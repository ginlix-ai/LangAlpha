
def _get_search_api() -> str:
    """Get search API from agent_config.yaml via shared YAML cache."""
    from src.config.tool_settings import _get_agent_config_dict
    config = _get_agent_config_dict()
    return str(config.get("search_api", "tavily"))


# Tool configuration loaded from agent_config.yaml
SELECTED_SEARCH_ENGINE = _get_search_api()
