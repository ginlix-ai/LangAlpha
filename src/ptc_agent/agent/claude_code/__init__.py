"""The Claude Code runtime: thread turns run by Claude Code on the user's Claude plan.

Everything here imports ``claude_agent_sdk``, which ships in the ``claude-code``
extra with the CLI it bundles. Nothing outside this package imports it, so a
backend without the extra, and every turn that is not a Claude Code turn,
never loads the SDK.
"""
