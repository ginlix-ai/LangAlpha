"""
User profile tools for managing watchlists, portfolios, and preferences.

These tools are designed to be dynamically loaded via the load_skill mechanism
and run on the HOST (not in sandbox) with direct database access.

Provides 3 unified tools:
- get_user_data: Read user data
- update_user_data: Create or update user data
- remove_user_data: Delete user data
"""

from src.tools.user_profile.tools import (
    get_user_data,
    update_user_data,
    remove_user_data,
)

USER_PROFILE_TOOLS = [
    get_user_data,
    update_user_data,
    remove_user_data,
]

__all__ = [
    "get_user_data",
    "update_user_data",
    "remove_user_data",
    "USER_PROFILE_TOOLS",
]
