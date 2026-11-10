"""Names chartkit shares with the server: the job env it reads.

Plain constants with no imports, so the server reads them without loading chartkit's
pandas modules, and both sides of the boundary spell each name once.
"""

#: File a job's published layers are appended to, one JSON line each; read when the job ends.
CHART_FILE_ENV = "PTC_CHART_FILE"
#: Folder snapshots are saved in when the call names no path.
SNAPSHOT_DIR_ENV = "PTC_CHART_SNAPSHOT_DIR"
#: The user's charts on screen when they sent the turn's messages, a JSON list.
CHART_VIEWS_ENV = "PTC_CHART_VIEWS"
