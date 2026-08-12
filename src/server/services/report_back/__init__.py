"""Report-back subsystem: background runs delivering results as new turns.

``flash/`` owns the flash-thread report-back lifecycle (PTC dispatches
reporting back as ordered flash turns); ``subagent.py`` owns the
background-subagent variant. They are distinct agent-type pipelines —
never merge them.
"""
