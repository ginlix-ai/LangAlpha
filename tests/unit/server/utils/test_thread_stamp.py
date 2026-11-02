import time

from src.server.utils.thread_stamp import stamped_title

QUOTE = '<quote source="Analyst\'s response">Revenue grew</quote>'


def test_a_message_with_no_quote_stamps_as_itself():
    message = "  plain\nwords  " + "x" * 400
    assert stamped_title(message) == message[:255]


def test_prefers_what_the_user_said():
    assert stamped_title(f"{QUOTE}\n<reply>by how much?</reply> and why") == "by how much? and why"


def test_falls_back_to_the_quote():
    assert stamped_title(QUOTE) == "Revenue grew"


def test_a_quote_too_long_to_scan_keeps_no_tag():
    title = stamped_title('<quote source="Analyst">' + "word " * 5000 + "</quote>")
    assert title.startswith("word word")
    assert "<quote" not in title
    assert len(title) <= 255


def test_a_reply_cut_short_keeps_no_tag():
    assert stamped_title(f"{QUOTE}\n<reply>please expl") == "please expl"
    assert stamped_title(f"{QUOTE}\n<rep") == "Revenue grew"


def test_a_chip_is_nobody_s_words():
    chip = "\n<details>\n<summary>[From chart]</summary>\n\nbody\n</details>"
    assert stamped_title(f"{QUOTE}{chip}") == "Revenue grew"
    assert stamped_title(f"{QUOTE} why{chip}") == "why"


def test_unclosed_openers_do_not_stall():
    start = time.monotonic()
    stamped_title('<quote source="">' * 100_000)
    assert time.monotonic() - start < 1
