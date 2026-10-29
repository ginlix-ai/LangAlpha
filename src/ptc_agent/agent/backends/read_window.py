"""What one Read call shows of a file.

The Read tool renders a window of lines as ``cat -n`` and clips it at a
character cap. A DB-backed route that deletes whatever a whole-document write
leaves out has to know whether the agent was shown every line, so it uses the
same arithmetic as the tool.
"""

from __future__ import annotations

# The line cap covers the common case; the char cap is the floor that catches
# files with very long lines (OCR markdown, minified JSON) that would otherwise
# sneak past the line cap. The char cap matches `LargeResultEvictionMiddleware`'s
# 40k-token/~160KB budget so a Read result can never single-handedly bust the
# context window the eviction middleware is otherwise responsible for protecting.
DEFAULT_READ_LINES = 2000
MAX_READ_CHARS = 160_000

# How Read's note opens when it showed less than the file may hold: a window
# clipped at the character cap, or one that filled its line limit.
READ_CLIPPED_NOTE = "\n\n[Read truncated"
READ_FULL_WINDOW_NOTE = "\n\n[Read stopped at the "


def format_cat_n(lines: list[str], *, start_line_number: int) -> str:
    return "\n".join(f"{i:6}\t{line}" for i, line in enumerate(lines, start=start_line_number))


def shows_whole_file(content: str, offset: int, limit: int) -> bool:
    """Whether Read(offset, limit) of ``content`` displays every line, unclipped."""
    lines = content.splitlines()
    if offset > 0 or limit < len(lines):
        return False
    return len(format_cat_n(lines, start_line_number=1)) <= MAX_READ_CHARS
