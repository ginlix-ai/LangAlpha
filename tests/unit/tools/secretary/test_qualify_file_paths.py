"""Links relayed into Home must name the workspace that holds the file."""

import pytest

from src.tools.secretary.utils import _qualify_file_paths

WID = "20cc68e8-d057-41f4-9bb1-57aa8d310704"
Q = f"__wsref__/{WID}"


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("[r](results/report.md)", f"[r]({Q}/results/report.md)"),
        ("![c](work/t/charts/r.png)", f"![c]({Q}/work/t/charts/r.png)"),
        ("[r](file:///home/workspace/results/report.md)", f"[r]({Q}/results/report.md)"),
        ("[r](/home/daytona/results/report.md)", f"[r]({Q}/results/report.md)"),
        ("[m](model.py)", f"[m]({Q}/model.py)"),
        ("[m](./work/model.py:42)", f"[m]({Q}/work/model.py:42)"),
        ("[m](model.py:40-55)", f"[m]({Q}/model.py:40-55)"),
        ("[v](results/notes.md#valuation)", f"[v]({Q}/results/notes.md#valuation)"),
        ("[d](<results/Q3 deck.pptx>)", f"[d](<{Q}/results/Q3 deck.pptx>)"),
        ("[d](results/Q3 deck.pptx)", f"[d](<{Q}/results/Q3 deck.pptx>)"),
        ("[d](<file:///home/workspace/Q3 deck.pptx#page=2>)", f"[d](<{Q}/Q3 deck.pptx#page=2>)"),
        # A title is not part of the path. Leaving these alone did not leave them
        # harmless: the parser hands the client the bare destination either way,
        # so an unqualified one resolves against Home and opens nothing. All
        # three CommonMark title forms, and the bracketed destination, which
        # previously did not match the pattern at all.
        ('[t](results/a.md "a title")', f'[t]({Q}/results/a.md "a title")'),
        ("[t](results/a.md 'a title')", f"[t]({Q}/results/a.md 'a title')"),
        ("[t](results/a.md (a title))", f"[t]({Q}/results/a.md (a title))"),
        ('[d](<results/Q3 deck.pptx> "Deck")', f'[d](<{Q}/results/Q3 deck.pptx> "Deck")'),
        ('[d](results/Q3 deck.pptx "Deck")', f'[d](<{Q}/results/Q3 deck.pptx> "Deck")'),
        ('[v](results/notes.md#valuation "Notes")', f'[v]({Q}/results/notes.md#valuation "Notes")'),
        # A heading fragment may hold spaces, because the panel's
        # `findHeadingIndex` matches a heading as written and not only as a
        # slug. Cutting the location at the first space left a head ending in
        # `Assumptions`, no file, so these relayed unqualified and opened
        # nothing in Home.
        (
            "[s](<results/report.md#Valuation Assumptions>)",
            f"[s](<{Q}/results/report.md#Valuation Assumptions>)",
        ),
        (
            "[s](results/report.md#Valuation Assumptions)",
            f"[s](<{Q}/results/report.md#Valuation Assumptions>)",
        ),
        (
            '[s](results/notes.md#Valuation Assumptions "Notes")',
            f'[s](<{Q}/results/notes.md#Valuation Assumptions> "Notes")',
        ),
        # A `#` in a name travels percent-encoded, which is how a destination
        # carries one and what `normalizeAgentHref` decodes after its own split.
        # It arrives here whole, so the extension is the name's own.
        ("[i](results/issue%231.md)", f"[i]({Q}/results/issue%231.md)"),
        ("[i](results/issue%231.md#Notes)", f"[i]({Q}/results/issue%231.md#Notes)"),
        # A `?query` trails a path the way a fragment does, and the client drops
        # one, so the link opens there and has to carry a workspace to open at all.
        ("[r](results/report.md?download=1)", f"[r]({Q}/results/report.md?download=1)"),
        ("[r](results/report.md?x=1#sec)", f"[r]({Q}/results/report.md?x=1#sec)"),
        (
            "[d](<results/Q3 deck.pptx?v=2>)",
            f"[d](<{Q}/results/Q3 deck.pptx?v=2>)",
        ),
        # A colon is a scheme only before the first slash. The sandbox allows
        # one in a name and `clean_path` accepts it, so rejecting every colon
        # left a real deliverable relaying unqualified.
        ("[r](results/Q3:final.pdf)", f"[r]({Q}/results/Q3:final.pdf)"),
        ("[r](<results/Q3:final.pdf>)", f"[r](<{Q}/results/Q3:final.pdf>)"),
        ("[r](./work/a:b/notes.md)", f"[r]({Q}/work/a:b/notes.md)"),
        # `.jsonl` is its own extension, not a longer `.json`. The crawl tool
        # writes `index.jsonl` and names it in agent-facing text, so matching
        # only the shorter one left a real deliverable relaying as a bare path.
        ("[i](results/index.jsonl)", f"[i]({Q}/results/index.jsonl)"),
    ],
)
def test_qualifies_workspace_file_links(text, expected):
    assert _qualify_file_paths(text, WID) == expected


@pytest.mark.parametrize(
    "text",
    [
        "[g](https://example.com/report.md)",
        "[e](mailto:a@example.com)",
        "[s](#summary)",
        "[s](localhost:8000)",
        "[s](127.0.0.1:8000)",
        "[t](/etc/hosts.txt)",
        "[x](notes about things)",
        "[g](https://example.com/report.md \"a title\")",
        # A query does not make a URL local.
        "[g](https://example.com/report.md?x=1)",
        # A colon before the first slash is a scheme, whatever follows it.
        "[x](example.com:8080/report.md)",
        "[d](data:text/html,a.md)",
        # No slash ahead of the colon, so the client's URL filter reads `Q3:` as
        # a scheme and drops the href. Qualifying it would mint a reference
        # nothing can open.
        "[r](Q3:final.pdf)",
        # An unterminated title is not a title, so the quote stays in the
        # destination and the name no longer ends in an extension.
        "[r](report.pdf \"unterminated)",
        # No extension on either side of the `#`, so there is no file to name.
        "[x](notes#Some Heading)",
        # A literal `#` opens the fragment, the reading the client does too, so
        # the head is `results/issue` and names no file. A name that really
        # carries a `#` is written `%23` and is pinned above.
        "[i](results/issue#1.md)",
        "[i](results/issue#1 draft.md)",
        "[i](results/issue#1.md#Notes)",
        f"[r]({Q}/results/report.md)",
        "[other](__wsref__/11111111-2222-3333-4444-555555555555/a.md)",
    ],
)
def test_leaves_non_workspace_links_alone(text):
    assert _qualify_file_paths(text, WID) == text


def test_a_computer_root_of_its_own_strips_like_the_stock_one():
    """A computer whose user cannot create the stock root keeps its workspaces
    under another, and its analyst writes absolute links under that one."""
    root = "/home/someone/langalpha"
    text = f"[r]({root}/results/report.md) [s](file://{root}/notes.md)"
    assert _qualify_file_paths(text, WID, root) == (
        f"[r]({Q}/results/report.md) [s]({Q}/notes.md)"
    )
    # Without the root it is an absolute path outside every workspace.
    assert _qualify_file_paths(text, WID) == text


def test_a_root_the_workspace_moved_off_strips_with_its_folder():
    """A turn from before a move links under the computer it ran on, which the
    live computer list stops naming once that computer is removed."""
    left = "/home/someone/langalpha"
    text = (
        f"[r]({left}/Research/results/report.md) [s](file://{left}/Old%20Name/notes.md)"
    )
    assert _qualify_file_paths(
        text, WID, "/home/workspace", (left,), "Research", ("Old Name",)
    ) == f"[r]({Q}/results/report.md) [s]({Q}/notes.md)"
    # Unless the workspace recorded leaving it, that root is no workspace's.
    assert _qualify_file_paths(text, WID, "/home/workspace", (), "Research") == text


def test_another_folder_under_a_root_left_is_a_siblings():
    """Qualified, a sibling's file would open this workspace's namesake."""
    text = "[o](/srv/Other/report.md) [r](/srv/report.md)"
    assert _qualify_file_paths(text, WID, "/other", ("/srv",), "Research") == text


def test_a_root_left_inside_the_current_one_is_read_with_the_folder():
    """Moved from ``~/langalpha`` to ``~``, the shorter root alone would leave
    ``langalpha/`` in front of the folder."""
    left = "/home/dev/langalpha"
    text = f"[r]({left}/Research/report.md) [s](/home/dev/Research/notes.md)"
    assert _qualify_file_paths(text, WID, "/home/dev", (left,), "Research") == (
        f"[r]({Q}/report.md) [s]({Q}/notes.md)"
    )
    # A root the workspace left above a stock one does not take its paths.
    assert _qualify_file_paths(
        "[r](/home/workspace/Research/report.md)", WID, None, ("/home",), "Research"
    ) == f"[r]({Q}/report.md)"


def test_the_current_folder_goes_with_the_current_root():
    """Kept, it reads as a subfolder of the workspace, which opens the file
    only by its name, or not at all beside a namesake."""
    text = "[r](/home/dev/Research/results/report.md)"
    assert _qualify_file_paths(text, WID, "/home/dev", (), "Research") == (
        f"[r]({Q}/results/report.md)"
    )


def test_the_current_folder_reads_as_itself_before_a_root_left():
    """Folder ``old`` on ``/srv`` after a computer rooted at ``/srv/old``: the
    path names a file in the folder now, as the file routes read it."""
    text = "[r](/srv/old/Research/a.md)"
    assert _qualify_file_paths(
        text, WID, "/srv", ("/srv/old",), "old", ("Research",)
    ) == f"[r]({Q}/Research/a.md)"


def test_roots_left_inside_one_another_read_the_longer_first():
    """Moved from ``/srv/old`` to ``/srv`` and on, neither root left is inside
    the current one, and ``/srv`` would leave ``old/`` in front of the folder."""
    text = "[r](/srv/old/Research/report.md) [s](/srv/Research/notes.md)"
    assert _qualify_file_paths(
        text, WID, "/other", ("/srv", "/srv/old"), "Research"
    ) == f"[r]({Q}/report.md) [s]({Q}/notes.md)"


def test_a_root_that_travels_encoded_strips_in_that_spelling_too():
    """A link is a URL, so a root with a space reaches the relay as ``%20``."""
    root = "/Users/someone/My Data"
    text = "[r](file:///Users/someone/My%20Data/results/Q3%20deck.pptx)"
    assert _qualify_file_paths(text, WID, root) == f"[r]({Q}/results/Q3%20deck.pptx)"


@pytest.mark.parametrize(
    ("fence", "closer"),
    [("```", "```"), ("```", "````"), ("````", "````"), ("~~~", "~~~"), ("~~~", "~~~~")],
)
def test_a_longer_closing_fence_still_ends_the_block(fence, closer):
    """CommonMark closes a block on a fence at least as long as the opener.

    Requiring an exact match let a block opened with three backticks and closed
    with four run to the end of the reply, so every deliverable link after it
    relayed unqualified.
    """
    text = f"{fence}py\nprint(1)\n{closer}\n\n[r](results/report.md)"
    assert _qualify_file_paths(text, WID) == (
        f"{fence}py\nprint(1)\n{closer}\n\n[r]({Q}/results/report.md)"
    )


@pytest.mark.parametrize(
    "text",
    [
        "```\n[r](results/report.md)\n```",
        "````\n```\n[r](results/report.md)\n```\n````",
        "~~~\n[r](results/report.md)\n~~~",
        "a `[r](results/report.md)` span",
        "```\n[r](results/report.md)",
        # A span opened by a run of two or more closes on the next run of the
        # same length, so the shorter runs around the link are its content.
        # Pairing backticks one at a time protected those inner runs instead
        # and rewrote the link between them, which is the one thing an agent
        # demonstrating the citation syntax writes.
        "a `` `[r](results/report.md)` `` span",
        "a ``` `[r](results/report.md)` ``` span",
        "a ```` `[r](results/report.md)` ```` span",
        "a ``x ` [r](results/report.md)`` span",
        "a ``[r](results/report.md)`` span",
    ],
)
def test_leaves_links_inside_code_alone(text):
    """A link in a fence is syntax on display, and this text is persisted."""
    assert _qualify_file_paths(text, WID) == text


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        # A lone backtick pairs with the next one, so the span ends before the
        # link and the link is prose. The control for the run pairing above:
        # widening it to any run would have swallowed these.
        ("Use ` as a sep, and [r](a.md) is it.", f"Use ` as a sep, and [r]({Q}/a.md) is it."),
        ("The `` operator and [r](a.md).", f"The `` operator and [r]({Q}/a.md)."),
    ],
)
def test_an_unclosed_run_is_not_a_code_span(text, expected):
    assert _qualify_file_paths(text, WID) == expected


def test_rewrites_every_link_in_a_reply():
    text = "See [a](a.md) and [b](https://x.io) then ![c](charts/c.png)."
    assert _qualify_file_paths(text, WID) == (
        f"See [a]({Q}/a.md) and [b](https://x.io) then ![c]({Q}/charts/c.png)."
    )
