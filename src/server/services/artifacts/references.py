"""What a file names, read from its text alone.

Page references, stylesheet ``url()``s and Markdown links are exact; a path
found as a string in a script, a JSON file or a data attribute is a guess,
marked ``script`` so the dialog can say so.
"""

from __future__ import annotations

import logging
import re
import time
from collections.abc import Callable, Iterator
from html.parser import HTMLParser
from typing import Literal

logger = logging.getLogger(__name__)

REASON_PAGE = "page"
REASON_LINK = "link"
REASON_STYLE = "style"
REASON_MARKDOWN = "markdown"
REASON_SCRIPT = "script"

_HTML_SUFFIXES = (".html", ".htm", ".xhtml", ".svg")
_CSS_SUFFIXES = (".css",)
_MARKDOWN_SUFFIXES = (".md", ".markdown")
# JSON is read like a script: a page fetches it and loads the paths inside it.
_SCRIPT_SUFFIXES = (".js", ".mjs", ".cjs", ".json", ".geojson")
# The shared page renders Markdown with the app's viewer, which draws these,
# a link to one in place as an image, and turns every other file link into
# plain text, so an image is the only thing a Markdown file can show a visitor.
_IMAGE_SUFFIXES = (".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".bmp")

# html.parser goes quadratic on a run of an unterminated construct ("<!--"
# repeated), which no real page holds and no thread can be told to stop. A parse
# that spends this stops with what it has, well past what a real page takes.
_PARSE_BUDGET_SECONDS = 60.0

# An escaped document nests in one more level of quoting each time, so a few
# levels are all a real page has and more only multiplies the parsing.
_MAX_SRCDOC_DEPTH = 4

# sre holds the GIL for a whole search, so one that finds nothing in a page near
# the walk's byte cap stalls every stream on this worker even from a thread.
# Windows let other threads run between searches. The overlap outruns the longest
# match the bounded patterns below can make.
_SCAN_WINDOW = 1024 * 1024
_SCAN_OVERLAP = 16 * 1024

# A quoted URL runs to its closing quote, so a name holding parentheses or
# spaces ("revenue (1).png") is read whole; an unquoted one stops at either. A
# run past any path is a data: URI, which names no file. Spelled without
# IGNORECASE so the search can skip on the first letter: this one reads whole
# bundles.
# A backslash escapes the next character in each form, a space or a quote
# included, so an escaped one neither ends the URL nor its string.
_CSS_URL_RE = re.compile(
    r"""[uU][rR][lL]\(\s{0,64}(?:"((?:\\[\s\S]|[^"\\]){1,2048})"|'((?:\\[\s\S]|[^'\\]){1,2048})'|((?:\\[\s\S]|[^'"()\s\\]){1,2048}))\s{0,64}\)"""
)
_CSS_IMPORT_RE = re.compile(
    r"""@import\s{1,64}(?!url\()(?:"((?:\\[\s\S]|[^"\\]){1,2048})"|'((?:\\[\s\S]|[^'\\]){1,2048})')""",
    re.IGNORECASE,
)
_CSS_ESCAPE_RE = re.compile(r"\\(?:([0-9a-fA-F]{1,6})[ \t\n\r\f]?|(\r\n|[\n\r\f])|([\s\S]))")
# A label holds no "[", so a run of unmatched ones costs one scan each to the
# next bracket instead of one to the end of the file. A destination is either
# bracketed, where a space is part of the name, or the run of non-space after
# the "(", read only ahead so an image right after this one is still found:
# ``_md_destination`` ends it where Markdown does. An image or a link alike.
_MD_INLINE_RE = re.compile(
    r"!?\[[^\[\]]{0,2048}\]\(\s{0,64}(?:<([^<>\n]{1,2048})>|(?=(\S{1,2048})))"
)
# ``[text][label]``, ``[label][]`` and ``[label]``, an image's or a link's: a
# use of the definition under that label.
_MD_LABEL_USE_RE = re.compile(r"!?\[([^\[\]\n]{0,2048})\](?:\[([^\[\]\n]{0,2048})\])?")
_MD_ESCAPE_RE = re.compile(r"\\([!-/:-@\[-`{-~])")
_MD_REF_RE = re.compile(
    r"^ {0,3}\[([^\[\]\n]{1,2048})\]:[ \t]{0,64}(?:<([^<>\n]{1,2048})>|(\S{1,2048}))",
    re.MULTILINE,
)
_META_REFRESH_RE = re.compile(r"url\s*=\s*(['\"]?)([^'\"]+)\1", re.IGNORECASE)
# A comment, or a string kept whole so a "/*" inside one opens nothing. An
# unclosed comment runs to the end, as the CSS tokenizer reads it.
_CSS_STRING = r""""(?:\\.|[^"\\\n])*"|'(?:\\.|[^'\\\n])*'"""
_CSS_COMMENT_RE = re.compile(rf"({_CSS_STRING})|/\*.*?(?:\*/|\Z)", re.DOTALL)
_CSS_STRING_RE = re.compile(_CSS_STRING, re.DOTALL)
# Each candidate an image-set() offers is a url() or a string, read to its
# closing ")" at most this far on.
_CSS_IMAGE_SET_RE = re.compile(r"image-set\(", re.IGNORECASE)
_MAX_IMAGE_SET = 16 * 1024
# A fence opens with three or more of one mark; a backtick fence's info string
# holds no backtick.
_MD_FENCE_RE = re.compile(r"^ {0,3}(`{3,}|~{3,})")
# A code span: a run of backticks closed by a run of the same length.
_MD_CODE_SPAN_RE = re.compile(r"(?<!`)(`+)(?!`)(.{1,4096}?)(?<!`)\1(?!`)", re.DOTALL)
# A quoted string that could be a relative file: no scheme, no whitespace, no
# template or glob punctuation, and an extension with a letter in it ("12.5" in
# inlined data is a number), which a query or fragment may follow
# ("chart.png?v=2"). "#" stays out of the name because a browser reads it as the
# fragment, and because a name that could hold one re-reads the rest of the
# string from every "." in it, which goes quadratic.
_SCRIPT_PATH_RE = re.compile(
    r"""['"`]([^'"`\s:{}$<>*?|#]{0,2048}[^'"`\s:{}$<>*?|#/.]\."""
    r"""(?=[0-9]{0,7}[A-Za-z])[A-Za-z0-9]{1,8})"""
    r"""(?:[?#][^'"`\s]{0,2048})?['"`]"""
)
# The argument of a call that loads a URL is a path whatever it looks like, so
# here a name may have no extension or hold a space. It must be the whole
# argument: 'a/' + name is not a file. Each callee opens on a literal so the
# search can skip on the first letter.
_LOAD_CALLEES = (
    r"fetch",
    r"d3\.(?:csv|tsv|json|text|xml|html|image|blob|buffer)",
    r"\$\.(?:getJSON|getScript)",
    r"jQuery\.(?:getJSON|getScript)",
    r"new\s{1,8}(?:Shared)?Worker",
    r"import(?:Scripts)?",
)
_SCRIPT_LOAD_RE = re.compile(
    "(?:"
    + "|".join(callee + r"\s{0,64}\(" for callee in _LOAD_CALLEES)
    + r"""|\.open\(\s{0,64}['"][A-Za-z]{1,16}['"]\s{0,64},)\s{0,64}"""
    + r"""(?:'([^'\n]{1,2048})'|"([^"\n]{1,2048})"|`([^`$\n]{1,2048})`)\s{0,64}[,)]"""
)
# Where a <link> names somewhere to go rather than something the page loads: a
# documentation site chains every page together with next and prev.
_NAVIGATION_RELS = frozenset(
    "alternate author bookmark canonical contents first help index last license "
    "next prev previous search start up".split()
)
#: The relations a browser fetches a ``<link>``'s href for. Under any other,
#: or none, nothing asks for the file but a script that reads the attribute.
_FETCH_RELS = frozenset(
    "stylesheet icon manifest preload modulepreload prefetch prerender "
    "compression-dictionary apple-touch-icon apple-touch-icon-precomposed "
    "apple-touch-startup-image mask-icon".split()
)


def _scan(pattern: re.Pattern[str], text: str) -> Iterator[re.Match[str]]:
    """``pattern.finditer(text)``, giving other threads a turn between windows."""
    start = 0
    while start < len(text):
        window_end = start + _SCAN_WINDOW
        resume = window_end
        for match in pattern.finditer(text, start, window_end + _SCAN_OVERLAP):
            if match.start() >= window_end:
                break
            resume = max(resume, match.end())
            yield match
        start = resume


class _ParseBudgetSpent(Exception):
    pass


# What a page reference is to the page holding it. A page past the parse depth
# keeps only what it loads.
_Role = Literal["load", "link"]

_SCHEME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*:")


def url_path(path: str) -> str:
    """A URL's path as the browser's parser reads it on an http(s) page, where a
    backslash separates segments as a slash does."""
    return path.replace("\\", "/")


def _against_base(base: str | None) -> Callable[[str], str | None]:
    """Where a document's ``<base href>`` sends its relative URLs, or None for
    one it sends off the workspace: to another site, or to the root of the
    origin serving it, which no workspace path names."""
    if not base or not base.strip():
        return lambda ref: ref

    def kept(ref: str) -> bool:
        return (
            not ref
            or url_path(ref).startswith(("/", "#", "?"))
            or bool(_SCHEME_RE.match(ref))
        )

    base = url_path(base.strip())
    if base.startswith("/") or _SCHEME_RE.match(base):
        return lambda ref: ref if kept(ref) else None
    path = base.split("#", 1)[0].split("?", 1)[0]
    # A last segment of ``.`` or ``..`` names a folder, as the URL parser
    # reads it: ``../..`` is two folders up, not one up and a file.
    last = path.rpartition("/")[2]
    folder = path.rstrip("/") if last in ("", ".", "..") else path.rpartition("/")[0]

    def against(ref: str) -> str:
        if ref.startswith(("#", "?")):
            # Only a query or a fragment: the base's own file, as a browser
            # reads it, which a base naming a folder has none of.
            return path + ref if last not in ("", ".", "..") else ref
        if kept(ref) or not folder:
            return ref
        return f"{folder}/{ref}"

    return against


def _navigates(tag: str, attributes: dict[str, str]) -> bool:
    if tag in ("a", "area"):
        return True
    if tag != "link":
        return False
    rels = attributes.get("rel", "").lower().split()
    return "stylesheet" not in rels and not _NAVIGATION_RELS.isdisjoint(rels)


def _submits(tag: str, attributes: dict[str, str]) -> bool:
    kind = (attributes.get("type") or "").strip().lower()
    if tag == "button":
        return kind not in ("button", "reset")
    return kind in ("submit", "image")


def _form_target(
    tag: str, attributes: dict[str, str], form_action: str | None, form_method: str
) -> str | None:
    """The file a GET submission lands on. A button's own ``formaction`` and
    ``formmethod`` win over its form's, as they do in a browser, so one that
    turns a POST form's submission into a GET goes to the form's action, which
    the form itself never names as a GET."""
    if tag == "form":
        url, method = attributes.get("action"), attributes.get("method")
    elif tag in ("button", "input"):
        url, method = attributes.get("formaction"), attributes.get("formmethod", form_method)
        if url is None and _submits(tag, attributes) and not _gets(form_method):
            url = form_action
    else:
        return None
    if not url or not url.strip() or not _gets(method):
        return None
    return url


def _gets(method: str | None) -> bool:
    """Whether a form submits as a GET: a missing or unknown method does, as
    the browser reads it; only ``post`` and ``dialog`` do not."""
    return (method or "get").strip().lower() not in ("post", "dialog")


class _RefCollector(HTMLParser):
    """Every URL an HTML or SVG document asks the browser to fetch or follow."""

    _URL_ATTRS = ("src", "href", "poster", "data", "xlink:href")
    _SVG_HREF = frozenset(
        {
            "a",
            "feimage",
            "filter",
            "image",
            "lineargradient",
            "mpath",
            "pattern",
            "radialgradient",
            "script",
            "textpath",
            "tref",
            "use",
        }
    )
    #: The elements that fetch or follow each URL attribute, HTML and SVG by
    #: the parser's lowercase names. On any other element only a script could
    #: take the value (a custom element's ``src``), so it is a guess.
    _CONSUMERS = {
        "src": frozenset(
            {"audio", "embed", "frame", "iframe", "img", "input", "script", "source", "track", "video"}
        ),
        "href": frozenset({"area", "link"}) | _SVG_HREF,
        "xlink:href": _SVG_HREF,
        "poster": frozenset({"video"}),
        "data": frozenset({"object"}),
        "srcset": frozenset({"img", "source"}),
    }

    def __init__(self, deadline: float, srcdoc_depth: int = 0) -> None:
        super().__init__(convert_charrefs=True)
        self.page: list[tuple[str, _Role]] = []
        self.style: list[str] = []
        self.script: list[str] = []
        # The first <base href> wins, as in a browser.
        self.base: str | None = None
        #: The action and method of the form being read, which its buttons inherit.
        self._form_action: str | None = None
        self._form_method = "get"
        #: Each form's action and method by its id, the first with an id
        #: winning, and the buttons that name a form by id, read at the end
        #: since a button may come before the form it names.
        self._forms: dict[str, tuple[str | None, str]] = {}
        self._owned: list[tuple[str, dict[str, str]]] = []
        #: What a ``<template>`` holds, read apart: inert until a script
        #: stamps it out, so what it names is a guess, and a ``<base>`` in it
        #: moves nothing.
        self._inert: _RefCollector | None = None
        self._template_depth = 0
        self._block: str | None = None
        self._deadline = deadline
        self._srcdoc_depth = srcdoc_depth

    def _check_budget(self) -> None:
        if time.monotonic() > self._deadline:
            raise _ParseBudgetSpent

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self._check_budget()
        if self._inert is not None:
            if tag == "template":
                self._template_depth += 1
            self._inert.handle_starttag(tag, attrs)
            return
        if tag == "template":
            self._inert = _RefCollector(self._deadline, self._srcdoc_depth)
            self._template_depth = 1
            return
        attributes = {k.lower(): v for k, v in attrs if v is not None}
        if tag == "base":
            if self.base is None and "href" in attributes:
                self.base = attributes["href"]
            return
        if tag == "style":
            self._block = "style"
        elif tag == "script" and "src" not in attributes:
            self._block = "script"
        role: _Role = "link" if _navigates(tag, attributes) else "load"
        for name in self._URL_ATTRS:
            if name in attributes:
                if self._consumes(tag, name, attributes):
                    self.page.append((attributes[name], role))
                else:
                    self.script.append(attributes[name])
        if "srcset" in attributes:
            for url in srcset_urls(attributes["srcset"]):
                if self._consumes(tag, "srcset", attributes):
                    self.page.append((url, "load"))
                else:
                    self.script.append(url)
        if tag == "link" and "imagesrcset" in attributes:
            # An image preload offers every candidate, as an <img> srcset does:
            # a visitor on another screen asks for another one.
            preloads = "preload" in attributes.get("rel", "").lower().split()
            for url in srcset_urls(attributes["imagesrcset"]):
                if preloads:
                    self.page.append((url, "load"))
                else:
                    self.script.append(url)
        if "style" in attributes:
            self.style.extend(css_refs(attributes["style"]))
        if tag == "meta" and attributes.get("http-equiv", "").lower() == "refresh":
            match = _META_REFRESH_RE.search(attributes.get("content", ""))
            if match:
                # It navigates by itself, so its target goes along as a load
                # does: not the owner's to leave out, and named at any depth.
                self.page.append((match.group(2), "load"))
        # A GET form leads to the file it names, as a link does, and the bridge
        # carries the view there (``links.js``).
        if tag == "form":
            self._form_action = attributes.get("action")
            self._form_method = attributes.get("method") or "get"
            if form_id := attributes.get("id"):
                self._forms.setdefault(form_id, (self._form_action, self._form_method))
        if tag in ("button", "input") and "form" in attributes:
            # Its form is the one with that id, not the one around it.
            self._owned.append((tag, attributes))
        else:
            self._take_form_target(tag, attributes, self._form_action, self._form_method)
        for name, value in attributes.items():
            if name.startswith("on") or value[:11].lower() == "javascript:":
                self.script.extend(script_refs(value))
            elif name.startswith("data-"):
                # Quoted so that a bare path reads as the string it is to the
                # script that takes it; JSON here carries strings of its own.
                self.script.extend(script_refs(f'"{value}"'))
        if tag == "iframe" and "srcdoc" in attributes:
            self._take_srcdoc(attributes["srcdoc"])

    def _consumes(self, tag: str, name: str, attributes: dict[str, str]) -> bool:
        if tag not in self._CONSUMERS[name]:
            return False
        if tag == "link":
            rels = attributes.get("rel", "").lower().split()
            return not _FETCH_RELS.isdisjoint(rels) or _navigates(tag, attributes)
        # An input fetches its src only as an image button.
        return tag != "input" or attributes.get("type", "").strip().lower() == "image"

    def _take_srcdoc(self, html: str) -> None:
        # A srcdoc document resolves URLs against its own <base>, and that
        # against the page holding it, so what it names joins this document's
        # own references once its base has moved them.
        if self._srcdoc_depth >= _MAX_SRCDOC_DEPTH:
            return
        inner = _collect(html, self._deadline, self._srcdoc_depth + 1)
        rebased = _against_base(inner.base)
        self.page.extend(
            (ref, role) for url, role in inner.page if (ref := rebased(url)) is not None
        )
        self.style.extend(ref for url in inner.style if (ref := rebased(url)) is not None)
        self.script.extend(ref for url in inner.script if (ref := rebased(url)) is not None)

    def _take_form_target(
        self, tag: str, attributes: dict[str, str], action: str | None, method: str
    ) -> None:
        target = _form_target(tag, attributes, action, method)
        if target:
            self.page.append((target, "link"))

    def close(self) -> None:
        super().close()
        # A template left open at the end still holds only guesses.
        self._take_inert()
        for tag, attributes in self._owned:
            action, method = self._forms.get(attributes["form"], (None, "get"))
            self._take_form_target(tag, attributes, action, method)

    def _take_inert(self) -> None:
        inert, self._inert = self._inert, None
        if inert is None:
            return
        inert.close()
        self.script.extend(url for url, _ in inert.page)
        self.script.extend(inert.style)
        self.script.extend(inert.script)

    def handle_endtag(self, tag: str) -> None:
        if self._inert is not None:
            if tag == "template":
                self._template_depth -= 1
                if self._template_depth == 0:
                    self._take_inert()
                    return
            self._inert.handle_endtag(tag)
            return
        if tag in ("style", "script"):
            self._block = None
        elif tag == "form":
            self._form_action = None
            self._form_method = "get"

    def handle_data(self, data: str) -> None:
        self._check_budget()
        if self._inert is not None:
            self._inert.handle_data(data)
            return
        if self._block == "style":
            self.style.extend(css_refs(data))
        elif self._block == "script":
            self.script.extend(script_refs(data))


def _collect(
    text: str, deadline: float | None = None, srcdoc_depth: int = 0
) -> _RefCollector:
    if deadline is None:
        deadline = time.monotonic() + _PARSE_BUDGET_SECONDS
    collector = _RefCollector(deadline, srcdoc_depth)
    try:
        collector.feed(text)
        collector.close()
    except _ParseBudgetSpent:
        logger.warning(
            "HTML reference scan stopped at its budget with %d references",
            len(collector.page) + len(collector.style) + len(collector.script),
        )
    except Exception:
        logger.debug("HTML reference scan stopped early", exc_info=True)
    return collector


def css_refs(text: str) -> list[str]:
    """What a stylesheet loads, each as the browser reads it: its ``url()``s,
    its imports and every candidate an ``image-set()`` offers, since a visitor
    on another screen asks for another one. A rule in a comment loads nothing,
    and nor does text in a string that only reads like one
    (``content: "url(x.png)"``)."""
    stripped = _CSS_COMMENT_RE.sub(lambda m: m.group(1) or " ", text)
    # Each string's insides blanked, the same length, so a pattern finds only
    # what stands outside one and its match still marks where to read.
    masked = _CSS_STRING_RE.sub(
        lambda m: m.group()[0] + "x" * (len(m.group()) - 2) + m.group()[-1], stripped
    )
    refs = [_css_group(stripped, m) for m in _scan(_CSS_URL_RE, masked)]
    refs.extend(_css_group(stripped, m) for m in _scan(_CSS_IMPORT_RE, masked))
    refs.extend(_image_set_strings(stripped, masked))
    return [_css_unescaped(ref) for ref in refs]


def _css_url_refs(text: str) -> list[str]:
    """Every ``url()`` and ``@import`` in ``text``, strings not set apart: a
    script holds its CSS in strings of its own."""
    refs = [m.group(1) or m.group(2) or m.group(3) for m in _scan(_CSS_URL_RE, text)]
    refs.extend(m.group(1) or m.group(2) for m in _scan(_CSS_IMPORT_RE, text))
    return refs


def _css_group(text: str, match: re.Match[str]) -> str:
    """The group ``match`` took, read from ``text`` rather than the blanked
    copy it ran on."""
    group = next(g for g in range(1, match.re.groups + 1) if match.group(g) is not None)
    return text[match.start(group) : match.end(group)]


def _image_set_strings(text: str, masked: str) -> list[str]:
    """The strings an ``image-set()`` offers as candidates: those it holds
    directly, not one inside ``type("image/png")``. Its ``url()``s are any
    other's."""
    found: list[str] = []
    for match in _scan(_CSS_IMAGE_SET_RE, masked):
        depth, i = 1, match.end()
        end = min(len(masked), i + _MAX_IMAGE_SET)
        while i < end and depth:
            char = masked[i]
            if char in "\"'":
                close = masked.find(char, i + 1)
                if close < 0:
                    break
                if depth == 1:
                    found.append(text[i + 1 : close])
                i = close + 1
                continue
            depth += {"(": 1, ")": -1}.get(char, 0)
            i += 1
    return found


def _css_unescaped(value: str) -> str:
    """A CSS URL or string as the browser reads it: ``\\ `` is a space, ``\\31 ``
    the character with that code point, and an escaped line break in a string
    continues it."""

    def one(m: re.Match[str]) -> str:
        if m.group(1):
            point = int(m.group(1), 16)
            if 0 < point <= 0x10FFFF and not 0xD800 <= point <= 0xDFFF:
                return chr(point)
            return "\ufffd"
        if m.group(2):
            return ""
        return m.group(3)

    return _CSS_ESCAPE_RE.sub(one, value)


def _without_markdown_code(text: str) -> str:
    """The document less its code blocks and spans, which the viewer shows as
    text: an image written in one is an example, never loaded."""
    kept: list[str] = []
    fence: str | None = None
    for line in text.splitlines(keepends=True):
        mark = _MD_FENCE_RE.match(line)
        if fence is None:
            if mark and not (mark.group(1)[0] == "`" and "`" in line[mark.end():]):
                fence = mark.group(1)
                kept.append("\n")
            else:
                kept.append(line)
            continue
        if (
            mark
            and mark.group(1)[0] == fence[0]
            and len(mark.group(1)) >= len(fence)
            and not line[mark.end():].strip()
        ):
            fence = None
        kept.append("\n")
    return _MD_CODE_SPAN_RE.sub(" ", "".join(kept))


def srcset_urls(value: str) -> list[str]:
    """Each candidate's URL, read by the HTML candidate grammar: a URL runs to
    whitespace, so a comma inside one (a ``data:`` URL) is part of it, and only a
    comma outside the descriptors' parentheses ends a candidate."""
    urls: list[str] = []
    i, n = 0, len(value)
    while i < n:
        while i < n and (value[i].isspace() or value[i] == ","):
            i += 1
        start = i
        while i < n and not value[i].isspace():
            i += 1
        url = value[start:i]
        if url.endswith(","):
            url = url.rstrip(",")
        else:
            depth = 0
            while i < n:
                char = value[i]
                i += 1
                if char == "(":
                    depth += 1
                elif char == ")" and depth:
                    depth -= 1
                elif char == "," and not depth:
                    break
        if url:
            urls.append(url)
    return urls


def markdown_refs(text: str) -> list[str]:
    """What a Markdown document may draw: its images and its links, written
    inline or through a definition one of them uses. The app's viewer draws a
    link to an image file in place, so the caller keeps the images by name;
    a definition nothing uses draws nothing."""
    refs = [
        _md_unescaped(m.group(1) if m.group(1) is not None else _md_destination(m.group(2)))
        for m in _scan(_MD_INLINE_RE, text)
    ]
    definitions = list(_scan(_MD_REF_RE, text))
    # A definition's own "[label]:" is not a use of it.
    defined_at = {m.start(1) - 1 for m in definitions}
    labels = {
        _md_label(m.group(2) or m.group(1))
        for m in _scan(_MD_LABEL_USE_RE, text)
        if not text.startswith("(", m.end()) and m.start() not in defined_at
    }
    refs.extend(
        _md_unescaped(m.group(2) or m.group(3))
        for m in definitions
        if _md_label(m.group(1)) in labels
    )
    return refs


def _md_destination(run: str) -> str:
    """An unbracketed destination out of the run of non-space it starts: up to
    the first ")" that no "(" in it opened (``chart(1).png``), a backslash
    escaping either."""
    depth = 0
    i = 0
    while i < len(run):
        char = run[i]
        if char == "\\":
            i += 2
            continue
        if char == "(":
            depth += 1
        elif char == ")":
            if not depth:
                return run[:i]
            depth -= 1
        i += 1
    return run


def _md_unescaped(destination: str) -> str:
    """A destination as Markdown reads it: a backslash before punctuation
    escapes it (``a\\_b.png`` is ``a_b.png``); any other is the URL's own."""
    return _MD_ESCAPE_RE.sub(r"\1", destination)


def _md_label(label: str) -> str:
    """A link label as Markdown matches it: case and inner whitespace aside."""
    return " ".join(label.split()).casefold()


def script_refs(text: str) -> list[str]:
    """The paths a script's or a data file's source could name, each once."""
    # An escaped string is the same path: JSON may write "a\/b.png", and CSS or
    # JSON held in a string writes its own quotes as \" or \'.
    text = text.replace("\\/", "/").replace('\\"', '"').replace("\\'", "'")
    refs = [m.group(1) for m in _scan(_SCRIPT_PATH_RE, text)]
    refs.extend(
        m.group(1) or m.group(2) or m.group(3) for m in _scan(_SCRIPT_LOAD_RE, text)
    )
    # Most ``url(x)`` in a script is a call, whose argument is a plain word.
    refs.extend(
        ref
        for ref in _css_url_refs(text)
        if ("/" in ref or "." in ref) and "$" not in ref and "{" not in ref
    )
    return list(dict.fromkeys(refs))


def file_kind(path: str) -> str | None:
    lower = path.lower()
    if lower.endswith(_HTML_SUFFIXES):
        return "html"
    if lower.endswith(_CSS_SUFFIXES):
        return "css"
    if lower.endswith(_MARKDOWN_SUFFIXES):
        return "markdown"
    if lower.endswith(_SCRIPT_SUFFIXES):
        return "script"
    return None


def file_references(
    kind: str, content: bytes, follow_links: bool
) -> list[tuple[str, str]]:
    """``(ref, reason)`` for each reference a file of this kind makes, what it
    names outright first so that decides a path it also merely hints at."""
    text = content.decode("utf-8", errors="replace")
    if kind == "html":
        found = _collect(text)
        rebased = _against_base(found.base)
        named = (
            # Loads first: a file the page shows as well as links to is there for
            # the page's own sake.
            [(url, REASON_PAGE) for url, role in found.page if role == "load"]
            + [
                (url, REASON_LINK)
                for url, role in found.page
                if role == "link" and follow_links
            ]
            + [(r, REASON_STYLE) for r in found.style]
            + [(r, REASON_SCRIPT) for r in found.script]
        )
        return [
            (ref, reason)
            for ref, reason in ((rebased(url), reason) for url, reason in named)
            if ref is not None
        ]
    if kind == "css":
        return [(r, REASON_STYLE) for r in css_refs(text)]
    if kind == "markdown":
        text = _without_markdown_code(text)
        # Raw HTML goes through the same viewer, which draws an <a> to an image
        # file as the image, as it does a Markdown link.
        page = [url for url, _role in _collect(text).page]
        return [
            (r, REASON_MARKDOWN)
            for r in markdown_refs(text) + page
            if r.split("#", 1)[0].split("?", 1)[0].lower().endswith(_IMAGE_SUFFIXES)
        ]
    return [(r, REASON_SCRIPT) for r in script_refs(text)]
