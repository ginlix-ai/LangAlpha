"""The automation files: how a row reads as its file, and how a save lands.

The PTC agent manages automations by editing one file per automation, so the
file's shape and the save semantics are the agent's whole interface to its
automations. A save goes through ``AutomationsBackend`` as the agent's Write
does, and runs the real lifecycle rules against an in-memory table whose
transactions roll back; only what those rules read from elsewhere (workspaces,
threads, models, the scheduler) is faked. What is pinned is the file, the
changes a save makes, the refusals, and the report the agent reads back.
"""
