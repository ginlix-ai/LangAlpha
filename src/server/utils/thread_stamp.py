"""The title a thread carries until one is generated for it."""

import re

_TITLE_CHARS = 255
# Enough of the message to find its own words. Bounds the patterns below too,
# which are lazy and run on text a client sends.
_SCANNED_CHARS = 8000
_QUOTE = re.compile(
    r'<quote source="[^"]*">(.*?)</quote>(?:\s*<reply>(.*?)</reply>)?', re.DOTALL
)
# Cut short, by this scan or by what a client clips a first query to.
_UNCLOSED_QUOTE = re.compile(r'<quote source="[^"]*(?:">|$)')
_CUT_TAG = re.compile(r"</?[a-z]*$")


def stamped_title(first_query: str) -> str:
    """The first message as a line of words, not the tags a quote travels in.

    What the user said, else what they quoted. A message with no quote stamps as
    itself, which is also what the title generator's compare-and-swap expects.
    """
    if "<quote " not in first_query[:_SCANNED_CHARS]:
        return first_query[:_TITLE_CHARS]
    scanned = first_query[:_SCANNED_CHARS]
    quoted: list[str] = []

    def lift(match: re.Match[str]) -> str:
        quoted.append(match[1])
        return f" {match[2] or ''} "

    # A chip trails the message, and its tags and contents are no one's words.
    said = _QUOTE.sub(lift, scanned).partition("<details>")[0]
    said = _CUT_TAG.sub("", _UNCLOSED_QUOTE.sub(" ", said)).replace("<reply>", " ")
    title = said.strip() or (quoted[0].strip() if quoted else "")
    return " ".join(title.split())[:_TITLE_CHARS]
