"""A first message read past its quote tags, for the title it stamps and the title generated from it."""

import re

_TITLE_CHARS = 255
# Enough of the message to find its own words. Bounds the patterns below too,
# which are lazy and run on text a client sends.
SCANNED_CHARS = 8000
# A chip trails the message and may hold what looks like a quote, while a quote's
# own text may hold what looks like a chip, so whichever opens first decides. A
# chip's summary is a `[label]` or an `@path`, as the composer writes it; any
# other `<details>` is what the person typed.
_QUOTE_OR_CHIP = re.compile(
    r'<quote source="[^"]*">(.*?)</quote>(?:\s*<reply>(.*?)</reply>)?'
    r"|<details>\n<summary>(?:\[[^\n]*\]|@[^\n]*)</summary>\n\n",
    re.DOTALL,
)
# Cut short, by this scan or by what a client clips a first query to.
_UNCLOSED_QUOTE = re.compile(r'<quote source="[^"]*(?:">|$)')
_CUT_TAG = re.compile(r"</?[a-z]*$")


def _read(scanned: str) -> tuple[str, list[str], str]:
    """What the user said, with their replies in place, what they quoted, and
    the chips the message trails."""
    said: list[str] = []
    quoted: list[str] = []
    at = 0
    for match in _QUOTE_OR_CHIP.finditer(scanned):
        typed = scanned[at : match.start()]
        # Inside a code fence the person typed, a quote or a chip is an example of one.
        if ("".join(said) + typed).count("```") % 2:
            continue
        if match[1] is None:
            said.append(typed)
            at = len(scanned)
            chips = scanned[match.start() :].strip()
            break
        said.append(f"{typed} {match[2] or ''} ")
        if passage := match[1].strip():
            quoted.append(passage)
        at = match.end()
    else:
        chips = ""
    rest = "".join([*said, scanned[at:]])
    # Only an opener with no closer after it was cut short; one before a closer is
    # part of an example the person typed.
    cut = rest.rfind("</quote>") + 1
    rest = rest[:cut] + _UNCLOSED_QUOTE.sub(" ", rest[cut:])
    rest = _CUT_TAG.sub("", rest).replace("<reply>", " ")
    return rest.strip(), quoted, chips


def stamped_title(first_query: str) -> str:
    """The first message as a line of words, not the tags a quote travels in.

    What the user said, else what they quoted. A message with no quote stamps as
    itself, which is also what the title generator's compare-and-swap expects.
    """
    scanned = first_query[:SCANNED_CHARS]
    if "<quote " not in scanned:
        return first_query[:_TITLE_CHARS]
    said, quoted, _ = _read(scanned)
    title = said or (quoted[0] if quoted else first_query)
    return " ".join(title.split())[:_TITLE_CHARS]


def message_to_title(first_query: str) -> str:
    """The first message as the title generator reads it.

    What the user said comes first, so a long passage cannot push it past the
    generator's cut, and the chips the message carries come last, as sent. A
    message with no quote stays exactly itself, which includes one that only
    mentions the tag, or a quote cut short before it closed.
    """
    said, quoted, chips = _read(first_query[:SCANNED_CHARS])
    if not quoted:
        return first_query
    passages = [f"Quoted passage:\n{passage}" for passage in quoted]
    return "\n\n".join(part for part in (said, *passages, chips) if part)
