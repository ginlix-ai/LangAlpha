import time

from src.server.utils.thread_stamp import message_to_title, stamped_title

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


def test_the_title_model_reads_what_was_said_before_what_was_quoted():
    second = '<quote source="@a.md">Costs fell</quote>'
    assert message_to_title(f"{QUOTE}\n<reply>by how much?</reply>{second}") == (
        "by how much?\n\nQuoted passage:\nRevenue grew\n\nQuoted passage:\nCosts fell"
    )


def test_the_title_model_reads_a_lone_quote_as_its_passage():
    assert message_to_title(QUOTE) == "Quoted passage:\nRevenue grew"


def test_the_title_model_reads_a_message_with_no_quote_as_it_was_sent():
    message = "  plain\nwords  " + "x" * 9000
    assert message_to_title(message) == message


def test_the_title_model_reads_a_tag_mentioned_in_a_chip_as_it_was_sent():
    chip = '\n<details>\n<summary>[Pasted]</summary>\n\n```\n<quote symbol="AAPL"/>\n```\n</details>'
    assert message_to_title(chip) == chip
    assert message_to_title(f"summarize{chip}") == f"summarize{chip}"


def test_a_quote_inside_a_chip_stays_the_chip_s_text():
    chip = '\n<details>\n<summary>[Pasted]</summary>\n\n```\n<quote source="x">inner</quote> and more\n```\n</details>'
    assert message_to_title(chip) == chip
    assert message_to_title(f"summarize{chip}") == f"summarize{chip}"
    assert stamped_title(f"summarize{chip}") == "summarize"
    assert message_to_title(f"{QUOTE}{chip}") == f"Quoted passage:\nRevenue grew\n\n{chip.strip()}"


def test_a_chip_inside_a_quote_stays_the_quote_s_text():
    message = '<quote source="x">see\n<details>\n<summary>a</summary></quote> why'
    assert stamped_title(message) == "why"
    assert message_to_title(message) == "why\n\nQuoted passage:\nsee\n<details>\n<summary>a</summary>"


def test_details_markup_the_user_typed_is_their_words():
    typed = "why does <details>\n<summary>More</summary>\n\nbody</details> fold"
    assert stamped_title(f"{QUOTE} {typed}") == " ".join(typed.split())
    assert message_to_title(f"{QUOTE} {typed}") == f"{typed}\n\nQuoted passage:\nRevenue grew"


def test_a_chip_shaped_example_in_code_is_the_user_s_words():
    example = "and\n```html\n<details>\n<summary>[example]</summary>\n\nbody\n</details>\n```\nWhy?"
    assert stamped_title(f"{QUOTE} {example}").endswith("``` Why?")
    assert message_to_title(f"{QUOTE} {example}") == f"{example}\n\nQuoted passage:\nRevenue grew"


def test_a_quote_shaped_example_in_code_is_the_user_s_words():
    example = 'what is\n```html\n<quote source="x">foo</quote>\n```\nfor?'
    assert message_to_title(example) == example
    assert message_to_title(f"{QUOTE} {example}") == f"{example}\n\nQuoted passage:\nRevenue grew"


def test_a_details_tag_the_user_typed_is_their_words():
    message = f"{QUOTE} what does <details> do"
    assert stamped_title(message) == "what does <details> do"
    assert message_to_title(message) == "what does <details> do\n\nQuoted passage:\nRevenue grew"


def test_an_empty_quote_is_no_passage():
    empty = '<quote source="a"></quote>'
    assert stamped_title(f"{empty}{QUOTE}") == "Revenue grew"
    assert message_to_title(f"{empty}{QUOTE}") == "Quoted passage:\nRevenue grew"
