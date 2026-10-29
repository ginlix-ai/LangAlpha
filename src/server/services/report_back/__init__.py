"""Report-back subsystem: background runs delivering results as new turns.

``handoff/`` owns the report-back lifecycle of the Chief of Staff's hand-offs
(an analyst's PTC thread reporting back as ordered turns on the thread that
handed it the work); ``subagent.py`` owns the background-subagent variant.
They are distinct agent-type pipelines; never merge them.
"""
