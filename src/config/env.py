"""
Environment variable constants.

These are resolved once at import time from .env / process environment.
No YAML dependency — pure os.getenv.
"""

import os

# Auth / Login Service (Supabase)
SUPABASE_URL: str = os.getenv("SUPABASE_URL", "")
AUTH_ENABLED: bool = bool(SUPABASE_URL)
LOCAL_DEV_USER_ID: str = os.getenv("AUTH_USER_ID", "local-dev-user")

# Quota enforcement service (ginlix-auth)
AUTH_SERVICE_URL: str = os.getenv("AUTH_SERVICE_URL", "")

# ginlix-data (real-time market data proxy)
GINLIX_DATA_URL: str = os.getenv("GINLIX_DATA_URL", "")
GINLIX_DATA_WS_URL: str = os.getenv("GINLIX_DATA_WS_URL", "") or (
    GINLIX_DATA_URL.replace("http://", "ws://").replace("https://", "wss://")
    if GINLIX_DATA_URL
    else ""
)
GINLIX_DATA_ENABLED: bool = bool(GINLIX_DATA_URL)

# Public base URL of this server (used in agent-generated URLs like preview links)
SERVER_BASE_URL: str = os.getenv("SERVER_BASE_URL", "http://localhost:8000")

# Automation webhook delivery (ginlix-integration)
AUTOMATION_WEBHOOK_URL: str = os.getenv("AUTOMATION_WEBHOOK_URL", "")
AUTOMATION_WEBHOOK_SECRET: str = os.getenv("AUTOMATION_WEBHOOK_SECRET", "")
