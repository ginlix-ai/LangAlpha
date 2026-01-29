
import enum
from dotenv import load_dotenv

load_dotenv()


class SearchEngine(enum.Enum):
    TAVILY = "tavily"
    BOCHA = "bocha"
    SERPER = "serper"



# Tool configuration loaded from agent_config.yaml
from src.config.settings import get_search_api

SELECTED_SEARCH_ENGINE = get_search_api()
