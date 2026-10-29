"""Size and count caps for user-tier skills.

These sit deliberately below the sandbox-scan constants in
``middleware/skills/discovery.py``: ``MAX_SKILL_FILE_SIZE`` (10 MB) guards a
filesystem walk, whereas ``MAX_SKILL_MD_BYTES`` here guards a *prompt* — a
SKILL.md body is injected verbatim into the model's context.
"""

# Compressed archive accepted over the wire.
MAX_SKILL_ARCHIVE_BYTES = 2 * 1024 * 1024

# Total uncompressed size across all entries — the zip-bomb ceiling.
MAX_SKILL_UNCOMPRESSED_BYTES = 8 * 1024 * 1024

# Any single file inside the archive.
MAX_SKILL_SINGLE_FILE_BYTES = 1 * 1024 * 1024

# SKILL.md specifically: ~16k tokens, already generous for a skill body.
MAX_SKILL_MD_BYTES = 64 * 1024

# Entries in one archive (after dropping ignored paths).
MAX_SKILL_FILES = 64

# One skill's description. Every enabled skill's description is listed in the
# manifest on every model call, so this is a per-turn cost, not a per-use one.
MAX_SKILL_DESCRIPTION_CHARS = 1024

# The per-user caps (count and total bytes) live beside their enforcement in
# database/user_skills.py, because the database layer must not import services.

# Inline-blob ceiling for deployments with no object storage configured. Well
# under MAX_SKILL_ARCHIVE_BYTES on purpose — a 2 MB blob per row would make
# every metadata read expensive if it ever leaked into the default column set.
MAX_SKILL_INLINE_BLOB_BYTES = 512 * 1024

# Archive round trips in flight for one fan-out. The right ceiling is a
# property of the object storage, not of how many skills a package ships or an
# account holds, so anything iterating skills gates on this rather than
# running the set serially and growing with the per-user cap.
MAX_CONCURRENT_ARCHIVE_OPS = 8
