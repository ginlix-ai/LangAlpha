"""What a file names, read from its text. The scanners are pure, so each form a
reference takes, and how long a hostile file can hold one up, is locked directly.
"""

from __future__ import annotations

import html
import re
import time

import pytest

from src.server.services.artifacts import references as rf
from src.server.services.artifacts.references import (
    css_refs,
    file_references,
    markdown_refs,
    script_refs,
)


def html_refs(text: str) -> tuple[list[str], list[str], list[str]]:
    found = rf._collect(text)
    return [url for url, _ in found.page], found.style, found.script


def test_html_refs_split_page_style_and_script():
    page, style, script = html_refs(
        """
        <html><head>
          <link rel="stylesheet" href="style.css">
          <style>body { background: url("bg.png") }</style>
          <script src="app.js"></script>
          <script>fetch("data/chart.json")</script>
          <meta http-equiv="refresh" content="0; url=next.html">
        </head><body>
          <img src="a.png" srcset="a@2x.png 2x, a@3x.png 3x">
          <div style="background-image: url(inline.png)"></div>
          <video poster="poster.jpg"></video>
          <object data="doc.pdf"></object>
          <use xlink:href="icons.svg#x"/>
        </body></html>
        """
    )
    assert page == [
        "style.css",
        "app.js",
        "next.html",
        "a.png",
        "a@2x.png",
        "a@3x.png",
        "poster.jpg",
        "doc.pdf",
        "icons.svg#x",
    ]
    assert style == ["bg.png", "inline.png"]
    assert script == ["data/chart.json"]


@pytest.mark.parametrize(
    "markup, links",
    [
        # An unknown method is a GET, as the browser reads it.
        ('<form method="foo" action="odd.html"></form>', ["odd.html"]),
        ('<form method="post" action="p.html"><button formmethod="x">b</button></form>', ["p.html"]),
        ('<form method="post" action="p.html"><button>b</button></form>', []),
        ('<form method="dialog" action="d.html"></form>', []),
    ],
)
def test_a_form_method_the_browser_does_not_know_submits_as_get(markup, links):
    assert [url for url, _ in file_references("html", markup.encode(), True)] == links


def test_a_url_attribute_counts_only_where_the_element_uses_it():
    found = dict(
        file_references(
            "html",
            b'<div href="private.csv"></div><span src="asset.png"></span>'
            b'<lottie-player src="anim.json"></lottie-player><div srcset="x.png 2x"></div>'
            b'<img src="a.png" srcset="a2.png 2x"><svg><use href="i.svg#i"/>'
            b'<image xlink:href="pic.png"/></svg><video poster="p.jpg"></video>'
            b'<object data="d.pdf"></object><input type="text" src="field.csv">'
            b'<input type="IMAGE" src="go.png">',
            True,
        )
    )
    # Nothing fetches these, though a custom element's script may: a guess.
    guessed = ("private.csv", "asset.png", "anim.json", "x.png", "field.csv")
    assert {u: found[u] for u in guessed} == dict.fromkeys(guessed, "script")
    loaded = ("a.png", "a2.png", "i.svg#i", "pic.png", "p.jpg", "d.pdf", "go.png")
    assert {u: found[u] for u in loaded} == dict.fromkeys(loaded, "page")


@pytest.mark.parametrize(
    "value, urls",
    [
        # A URL runs to whitespace, so a data URL's comma is its own.
        ("data:,secret.png 1x", ["data:,secret.png"]),
        ("a.png 1x, b.png 2x", ["a.png", "b.png"]),
        ("a.png,b.png", ["a.png,b.png"]),
        ("a.png, b.png", ["a.png", "b.png"]),
        ("a.png 100w,b.png (x, y) 2x , c.png", ["a.png", "b.png", "c.png"]),
    ],
)
def test_srcset_is_read_by_its_candidate_grammar(value, urls):
    assert rf.srcset_urls(value) == urls


def test_a_data_url_in_srcset_names_no_file():
    # The data URL whole, which the walk then drops as no file; never the
    # ``secret.png`` a comma split would cut from it.
    found = file_references("html", b'<img srcset="data:,secret.png 1x, b.png 2x">', True)
    assert [url for url, _ in found] == ["data:,secret.png", "b.png"]


def test_a_rule_in_a_css_comment_loads_nothing():
    assert css_refs(
        'a{background:url(x.png)} /* b{background:url(private.png)} @import "no.css"; */'
        ' c{content:"/*"; background:url(y.png)} /* unclosed url(z.png)'
    ) == ["x.png", "y.png"]


def test_an_image_in_markdown_code_is_an_example():
    text = (
        "![real](real.png)\n`![ex](inline.png)`\n"
        '```html\n<img src="fenced.png">\n![f](fenced2.png)\n```\n'
        "~~~~\n![t](tilde.png)\n~~~\nstill code\n~~~~\n"
        "![after](after.png) ``a ` ![b](span.png) ``"
    )
    assert file_references("markdown", text.encode(), True) == [
        ("real.png", "markdown"),
        ("after.png", "markdown"),
    ]


def test_what_a_template_holds_is_a_guess():
    # Inert until a script stamps it out, and its <base> moves nothing.
    assert file_references(
        "html",
        b'<template><base href="x/"><img src="t.png"><template><img src="t2.png">'
        b'</template><a href="t.html">t</a></template><img src="shown.png">',
        True,
    ) == [
        ("shown.png", "page"),
        ("t.png", "script"),
        ("t.html", "script"),
        ("t2.png", "script"),
    ]


@pytest.mark.parametrize(
    "base, resolved",
    [
        ("../..", "../../root.png"),
        ("..", "../root.png"),
        (".", "./root.png"),
        ("sub/..", "sub/../root.png"),
        ("sub/page.html", "sub/root.png"),
        ("sub/", "sub/root.png"),
        ("page.html", "root.png"),
    ],
)
def test_a_base_ending_in_a_dot_segment_names_a_folder(base, resolved):
    assert rf._against_base(base)("root.png") == resolved


@pytest.mark.parametrize(
    "base, ref, resolved",
    [
        # Only a query or a fragment: the base's own file.
        ("sub/details.html", "?tab=one", "sub/details.html?tab=one"),
        ("sub/details.html", "#part", "sub/details.html#part"),
        ("details.html?x=1", "#part", "details.html#part"),
        # A base naming a folder has no file to keep: the document itself.
        ("sub/", "#part", "#part"),
        ("..", "?q", "?q"),
    ],
)
def test_a_query_or_fragment_alone_keeps_the_bases_file(base, ref, resolved):
    assert rf._against_base(base)(ref) == resolved


def test_css_refs_take_url_and_import_in_every_spelling():
    assert css_refs(
        """
        @import "base.css";
        @import url(theme.css);
        a { background: url( 'x.png' ) } b { src: url(fonts/a.woff2) }
        c { background: url("charts/revenue (1).png") }
        """
    ) == ["theme.css", "x.png", "fonts/a.woff2", "charts/revenue (1).png", "base.css"]


def test_markdown_refs_take_images_links_and_the_definitions_they_use():
    # The viewer draws a link to an image file in place; a definition nothing
    # uses draws nothing.
    assert markdown_refs(
        "![chart](img/c.png)\n[doc](other.md)\n[download](private.png)\n"
        "![t](<e.png>)\n![logo][Brand  Mark]\n![two]\n[see][notes]\n\n"
        "[brand mark]: <img/d.png>\n[Two]: img/two.png\n[notes]: secret.png\n"
        "[unused]: unused.png"
    ) == ["img/c.png", "other.md", "private.png", "e.png", "img/d.png", "img/two.png", "secret.png"]


def test_a_markdown_document_lists_the_image_files_it_links_to_and_nothing_else():
    md = b"[chart](chart.png) [notes](notes.md) [raw](raw.csv)\n![drawn](drawn.png)"
    assert file_references("markdown", md, True) == [
        ("chart.png", "markdown"),
        ("drawn.png", "markdown"),
    ]


def test_an_image_preload_offers_every_candidate():
    page = (
        b'<link rel="preload" as="image" href="fallback.jpg" imagesrcset="small.jpg 1x, large.jpg 2x">'
        b'<link rel="icon" href="i.png" imagesrcset="unused.png 2x">'
    )
    assert sorted(file_references("html", page, True)) == [
        ("fallback.jpg", "page"),
        ("i.png", "page"),
        ("large.jpg", "page"),
        ("small.jpg", "page"),
        # Only a preload fetches one: elsewhere a script is all that reads it.
        ("unused.png", "script"),
    ]


def test_a_string_that_reads_like_a_url_loads_nothing():
    css = (
        'a::after { content: "url(private.png)" } b::before { content: \'@import "x.css";\' }'
        ' c { background: url("real (1).png") } @import "theme.css";'
    )
    assert css_refs(css) == ["real (1).png", "theme.css"]


def test_every_image_set_candidate_is_listed():
    css = (
        'a { background: image-set("low.png" 1x, url(mid.png) 1.5x,'
        ' "high.png" type("image/png") 2x) }'
        " b { background: -webkit-image-set('w\\ 1.png' 1x) }"
    )
    assert sorted(css_refs(css)) == ["high.png", "low.png", "mid.png", "w 1.png"]


def test_a_markdown_destination_holds_its_balanced_parentheses():
    text = "![a](chart(1).png) ![b](a\\(b.png) ![c](q((r)).png \"t\") ![d](u.png)![e](v.png)"
    assert markdown_refs(text) == ["chart(1).png", "a(b.png", "q((r)).png", "u.png", "v.png"]


def test_a_markdown_destination_of_unmatched_parentheses_reads_in_one_pass():
    started = time.monotonic()
    for text in ("![a](" + "(" * 50_000, "![a](" + "(a" * 30_000, "![a](" + "((a)" * 20_000):
        markdown_refs(text)
    assert time.monotonic() - started < 2


@pytest.mark.parametrize(
    "rel, reason",
    [
        ("stylesheet", "page"),
        ("alternate stylesheet", "page"),
        ("shortcut icon", "page"),
        ("apple-touch-icon-precomposed", "page"),
        ("modulepreload", "page"),
        ("next", "link"),
        # Nothing fetches it: only a script that reads the attribute could.
        (None, "script"),
        ("pingback", "script"),
        ("preconnect", "script"),
    ],
)
def test_a_link_loads_its_href_only_under_a_relation_that_fetches_it(rel, reason):
    rel_attr = f' rel="{rel}"' if rel else ""
    html = f'<link{rel_attr} href="data/x.csv">'.encode()
    assert rf.file_references("html", html, True) == [("data/x.csv", reason)]


def test_a_markdown_destination_drops_only_the_backslashes_that_escape():
    assert markdown_refs("![a](img\\_a.png)\n![b](img\\b.png)") == ["img_a.png", "img\\b.png"]


def test_a_base_written_with_backslashes_names_its_folder():
    assert rf._against_base("sub\\page.html")("x.png") == "sub/x.png"
    assert rf._against_base("sub\\page.html")("\\x.png") == "\\x.png"


def test_a_raw_html_link_to_an_image_in_markdown_is_drawn():
    md = b'<a href="raw.png">raw</a> <a href="doc.md">doc</a> <img src="drawn.png">'
    assert file_references("markdown", md, True) == [
        ("raw.png", "markdown"),
        ("drawn.png", "markdown"),
    ]


@pytest.mark.parametrize(
    "css, expected",
    [
        ('a { background: url("mobile\\ 1.png") }', "mobile 1.png"),
        ("a { background: url(m\\ 2.png) }", "m 2.png"),
        ('@import "theme\\ 1.css";', "theme 1.css"),
        ("a { background: url('\\31 x.png') }", "1x.png"),
        ('a { background: url("q\\"uote.png") }', 'q"uote.png'),
        ('a { background: url("long\\\nname.png") }', "longname.png"),
    ],
    ids=["quoted-space", "bare-space", "import", "hex", "quote", "line-break"],
)
def test_a_css_escape_is_read_as_the_browser_reads_it(css, expected):
    assert css_refs(css) == [expected]


def test_markdown_refs_read_bracketed_names_and_titled_definitions():
    assert markdown_refs("![c](<img/c 1.png>)") == ["img/c 1.png"]
    assert markdown_refs('![a](x.png "T")') == ["x.png"]
    assert markdown_refs('![chart][ref]\n\n[ref]: chart.png "Title"') == ["chart.png"]


def test_markdown_refs_stay_linear_on_unmatched_brackets():
    # Quadratic before: every "[" rescanned to the end of the file.
    started = time.monotonic()
    assert markdown_refs("[" * 400_000) == []
    assert time.monotonic() - started < 30


def test_a_quoted_number_in_inlined_data_is_not_a_file():
    """Each candidate costs a probe, and past the cap the publish is refused."""
    refs = script_refs("""["12.5", "1.2.5", "0.75", "BRK.B", "model.h5", "a.mp4"]""")
    assert refs == ["BRK.B", "model.h5", "a.mp4"]


def test_script_refs_are_quoted_relative_paths_with_an_extension():
    refs = script_refs(
        """
        const a = "data/chart.json"; const b = 'x.csv'; const c = `y.png`;
        const url = "https://cdn.example.com/lib.js";
        const tpl = `${base}/z.json`; const glob = "*.csv"; const word = "hello";
        img.src = "chart.png?v=2"; const d = 'data/a.json#top'; const q = "?v=2";
        """
    )
    assert refs == ["data/chart.json", "x.csv", "y.png", "chart.png", "data/a.json"]


def test_inline_event_attributes_are_read_as_script():
    _, _, script = html_refs(
        """
        <button onclick="fetch('a/x.json')">x</button>
        <a ONCLICK="location.href='a/y.html'">y</a>
        <img onerror="this.src=&quot;a/z.png&quot;" src="missing.png">
        """
    )
    assert script == ["a/x.json", "a/y.html", "a/z.png"]


@pytest.mark.parametrize(
    "value, expected",
    [
        ("a/z.svg", ["a/z.svg"]),
        ("z.png?v=2", ["z.png"]),
        ("hello", []),
        ("12.5", []),
        ("https://cdn.example.com/z.svg", []),
        ("data:image/png;base64,AAAA", []),
        ("""{"src": "a/j.png", "n": "12.5"}""", ["a/j.png"]),
    ],
)
def test_a_data_attribute_holding_a_path_is_a_guess(value, expected):
    page, style, script = html_refs(f"<div data-x='{value}'></div>")
    assert (page, style, script) == ([], [], expected)


def test_an_iframe_srcdoc_is_read_as_html_of_the_page_holding_it():
    page, style, script = html_refs(
        """
        <iframe srcdoc="<img src='in/a.png'>
          <style>b { background: url(in/b.svg) }</style>
          <script>fetch(&quot;in/c&quot;)</script>
          <a href='in/d.html'>d</a>"></iframe>
        """
    )
    assert (page, style, script) == (["in/a.png", "in/d.html"], ["in/b.svg"], ["in/c"])


def test_a_srcdoc_base_moves_what_that_document_names():
    page = (
        b'<base href="reports/">'
        b"<iframe srcdoc=\"<base href='assets/'><img src='chart.png'>"
        b"<img src='https://cdn.example/x.png'>\"></iframe>"
        b"<iframe srcdoc=\"<base href='https://cdn.example/'><img src='y.png'>\"></iframe>"
    )
    assert file_references("html", page, follow_links=True) == [
        ("reports/assets/chart.png", "page"),
        ("https://cdn.example/x.png", "page"),
    ]


def _nested_srcdoc(levels: int) -> str:
    document = '<img src="deep.png">'
    for _ in range(levels):
        document = f'<iframe srcdoc="{html.escape(document)}"></iframe>'
    return document


def test_srcdoc_nesting_stops_at_its_limit():
    # Each level parses the whole document again, so an unbounded one multiplies
    # the work by its depth.
    assert html_refs(_nested_srcdoc(rf._MAX_SRCDOC_DEPTH))[0] == ["deep.png"]
    assert html_refs(_nested_srcdoc(rf._MAX_SRCDOC_DEPTH + 1))[0] == []


@pytest.mark.parametrize(
    "source, expected",
    [
        ("s.textContent='h2{background:url(a/x.svg)}'", "a/x.svg"),
        ("s.textContent='h2{background:url(\"a/x y.svg\")}'", "a/x y.svg"),
        (r"s.textContent='h2{background:url(\'a/x y.svg\')}'", "a/x y.svg"),
        (r"""JSON.parse("{\"image\":\"a/x.svg\"}")""", "a/x.svg"),
        (r"""const j = {"image": "a\/x.svg"}""", "a/x.svg"),
    ],
)
def test_css_and_json_held_in_a_script_string_are_read(source, expected):
    assert script_refs(source) == [expected]


def test_a_script_url_call_is_not_a_stylesheet_reference():
    assert script_refs("createObjectURL(blob); new URL(href); url(e); url(`${b}/y.png`)") == []


@pytest.mark.parametrize(
    "source",
    [
        "fetch('a/t')",
        'fetch("a/t", {method: "POST"})',
        "fetch(`a/t`)",
        "fetch ( 'a/t' )",
        "window.fetch('a/t').then(done)",
        "d3.csv('a/t')",
        "d3.json('a/t', row)",
        "$.getJSON('a/t', done)",
        "jQuery.getJSON('a/t')",
        "xhr.open('GET', 'a/t')",
        'xhr.open("POST","a/t",true)',
        "new Worker('a/t')",
        "new SharedWorker('a/t')",
        "import('a/t')",
        "importScripts('a/t')",
    ],
)
def test_a_literal_passed_to_a_loading_call_is_a_path_whatever_it_looks_like(source):
    assert script_refs(source) == ["a/t"]
    assert script_refs(source.replace("a/t", "a/t u.json")) == ["a/t u.json"]


@pytest.mark.parametrize(
    "source",
    [
        "fetch('a/' + name)",
        "fetch(`a/${name}`)",
        "fetch(url)",
        "log('a/t')",
        "cache.get('a/t')",
        "$.get('key')",
    ],
)
def test_a_string_elsewhere_or_a_built_path_is_not_a_load(source):
    assert script_refs(source) == []


def test_a_script_names_each_path_once():
    assert script_refs("""fetch('a.json'); fetch("a.json"); x = 'a.json'""") == ["a.json"]


# The shape each went quadratic or worse in, or would without its bound. Past
# these sizes a super-linear pattern takes minutes.
@pytest.mark.parametrize(
    "scan, text",
    [
        (script_refs, '"' + "a.p#" * 1_000_000 + ' x"'),
        (script_refs, '"' + "a.b" * 1_000_000 + ' x"'),
        (script_refs, "fetch(" * 500_000),
        (script_refs, "fetch('" * 500_000),
        (script_refs, ".open('GET'," * 300_000),
        (script_refs, "url(" * 1_000_000),
        (script_refs, "url('x " * 500_000),
        (css_refs, "url(" * 1_000_000),
        (css_refs, "@import " * 500_000),
    ],
    # Named by what repeats: the text itself is megabytes, and a test's name
    # is printed whole to every run's log.
    ids=[
        "script-path-hash", "script-dotted", "script-fetch", "script-fetch-quote",
        "script-open", "script-url", "script-url-quote", "css-url", "css-import",
    ],
)
def test_scans_stay_linear_on_a_run_of_near_matches(scan, text):
    started = time.monotonic()
    scan(text)
    assert time.monotonic() - started < 30


def test_a_parse_gone_quadratic_stops_at_its_budget_with_what_it_has(monkeypatch):
    # html.parser re-reads to the end of the file from each unterminated "<!--".
    monkeypatch.setattr(rf, "_PARSE_BUDGET_SECONDS", 0.2)
    started = time.monotonic()
    page, _, _ = html_refs('<img src="before.png">' + "<!--" * 1_000_000)
    assert page == ["before.png"]
    assert time.monotonic() - started < 30


@pytest.mark.parametrize("padding", range(0, 80, 3))
def test_a_scan_reads_a_match_across_a_window_edge_exactly_once(monkeypatch, padding):
    monkeypatch.setattr(rf, "_SCAN_WINDOW", 64)
    text = " " * padding + '"a/x.png"' + " " * 200 + '"b/y.png"'
    assert [m.group(1) for m in rf._scan(rf._SCRIPT_PATH_RE, text)] == ["a/x.png", "b/y.png"]


@pytest.mark.parametrize(
    "name",
    [
        "_CSS_URL_RE",
        "_CSS_IMPORT_RE",
        "_CSS_IMAGE_SET_RE",
        "_MD_INLINE_RE",
        "_MD_LABEL_USE_RE",
        "_MD_REF_RE",
        "_SCRIPT_PATH_RE",
        "_SCRIPT_LOAD_RE",
    ],
)
def test_no_scanned_pattern_can_match_past_the_window_overlap(name):
    # A longer match would be cut at the window's end and read as no match. What
    # a pattern reads ahead counts too: it would be cut short.
    pattern = getattr(rf, name)
    read = pattern.pattern.replace("(?=", "(?:")
    _, longest = re._parser.parse(read, pattern.flags).getwidth()
    assert longest <= rf._SCAN_OVERLAP


def test_a_base_href_moves_where_the_page_relative_urls_point():
    page = (
        b'<base href="assets/"><base href="ignored/">'
        b'<img src="chart.png"><link rel="stylesheet" href="../site.css">'
        b'<a href="#top">top</a><img src="https://cdn.example/x.png">'
        b"<script>fetch('data/q3.json')</script>"
    )
    assert file_references("html", page, follow_links=True) == [
        ("assets/chart.png", "page"),
        ("assets/../site.css", "page"),
        ("https://cdn.example/x.png", "page"),
        ("#top", "link"),
        ("assets/data/q3.json", "script"),
    ]


def test_a_get_form_leads_to_the_file_it_names_as_a_link_does():
    page = (
        b'<form action="details.html"><button formaction="other.html">o</button></form>'
        b'<form method="post" action="post.html"><button formaction="kept.html">k</button>'
        b'<button formaction="got.html" formmethod="get">g</button></form>'
        b'<form action=""></form><input formaction="x.html" formmethod="dialog">'
        b'<button formaction="alone.html">a</button>'
        # A GET of its own takes the form's action; one that cannot submit does not.
        b'<form method="post" action="inherited.html"><button formmethod="get">g</button>'
        b'<button type="button" formmethod="get">n</button><button>p</button></form>'
        # A form named by id, before or after the button, and not the one around it.
        b'<form id="f" method="post" action="owned.html"></form>'
        b'<form method="post" action="around.html"><button form="f" formmethod="get">o</button>'
        b'<button form="g" formmethod="get">a</button></form>'
        b'<form id="g" method="post" action="ahead.html"></form>'
    )
    assert file_references("html", page, follow_links=True) == [
        ("details.html", "link"),
        ("other.html", "link"),
        ("got.html", "link"),
        ("alone.html", "link"),
        ("inherited.html", "link"),
        ("owned.html", "link"),
        ("ahead.html", "link"),
    ]


@pytest.mark.parametrize("base", ["https://cdn.example/", "/static/"])
def test_a_base_off_the_workspace_takes_relative_urls_with_it(base):
    page = f'<base href="{base}"><img src="chart.png"><img src="https://a.example/b.png">'
    assert file_references("html", page.encode(), follow_links=True) == [
        ("https://a.example/b.png", "page"),
    ]


def test_a_base_naming_a_file_beside_the_page_changes_nothing():
    page = b'<base href="index.html"><img src="chart.png">'
    assert file_references("html", page, follow_links=True) == [("chart.png", "page")]

