// @ts-check
// The comment half of the bridge, served after core.js and requests.js only
// to the owner's live view of a file (?inject=comments): a published version
// and a file read elsewhere are read only, so their frames neither hear the
// viewer's comment messages nor report what the reader points at.
//
// The viewer draws everything (the hover ring, pins, the composer) over the
// frame, in its own document. This part only reports what the reader
// pointed at, picked, selected or dragged an area around, and where the
// anchors it was given sit, in the frame's viewport coordinates. The page's
// own code never sees a comment.

/** @typedef {import('../../../../web/src/types/artifacts').ElementAnchor} ElementAnchor */
/** @typedef {import('../../../../web/src/types/artifacts').TextAnchor} TextAnchor */
/** @typedef {import('../../../../web/src/types/artifacts').AreaAnchor} AreaAnchor */
/** @typedef {{ left: number, top: number, right: number, bottom: number }} BridgeEdges */
/** @typedef {{ node: Text, offset: number, box: DOMRect }} BridgeGlyph */
/** @typedef {{ anchor: TextAnchor, rect: { x: number, y: number, width: number, height: number }, range: Range }} BridgeSelection */
// A node the code has checked is text, by its nodeType.
/** @typedef {Node & { data: string }} TextLike */
/** @typedef {{ x: number, y: number, width: number, height: number }} BridgeBox */
/** @typedef {{ clientX: number, clientY: number }} BridgePoint */
/** @typedef {{ node: Text, box: DOMRect | null }} BridgeRun */
// The viewer's anchor as the page-side code reads it: its fields are checked
// where they are used, since a stored anchor may be of any shape.
/** @typedef {{ kind: string, path?: any, quote?: any, prefix?: any, suffix?: any, elements?: any }} BridgeAnchor */
/** @typedef {{ el?: Element, els?: Element[], range?: Range }} BridgeHit */
/** @typedef {{ id: string, anchor: BridgeAnchor, hit: BridgeHit | null, mark: boolean }} BridgeTracked */
/** @typedef {{ id: number, press: number, on: boolean, moved: boolean, text: boolean, glyph: BridgeGlyph | null, x: number, y: number, cx: number, cy: number, selecting?: boolean, dropped?: boolean }} BridgeDrag */
/** @typedef {{ id: number, target: EventTarget | null, cx: number, cy: number }} BridgeHeld */

var MODE_STYLE_ID = '__page_comment_mode__';
var DRAG_STYLE_ID = '__page_area_drag__';

// The app's accent, read from its theme rather than the page's variables,
// which a page may define for itself: the mark on words being commented on
// is the app's, as the pins are.
var accent = '';
var ACCENT = /--color-accent-primary:\s*([^;]+);/;
var COLOUR_VALUE = /^[#a-zA-Z0-9(),.%\s\/-]+$/;

onTheme.push(function (css) {
  var match = ACCENT.exec(css);
  accent = match && COLOUR_VALUE.test(match[1]) ? match[1] : '';
  tintMark();
});

// ---- Text and geometry ----

// The field limits the comment API stores (CommentAnchor in
// app/artifacts/models.py), tighter where a shorter value reads better. The
// ones typed against pageProtocol must equal the viewer's limit; the rest
// are shorter by choice and cannot be pinned by equality.
/** @type {typeof import('../../../../web/src/pages/ChatAgent/components/viewers/html/pageProtocol').ANCHOR_LIMITS['path']} */
var MAX_PATH = 1000;
var MAX_LABEL = 120;
var MAX_QUOTE = 300;
/** @type {typeof import('../../../../web/src/pages/ChatAgent/components/viewers/html/pageProtocol').ANCHOR_LIMITS['quote']} */
var MAX_SELECTION = 2000;
var MAX_AROUND = 60;

/** @type {typeof import('../../../../web/src/pages/ChatAgent/components/viewers/html/pageProtocol').MAX_POSITIONS} */
var MAX_ANCHORS = 500;
// An area lists its first few elements in full and counts the rest.
/** @type {typeof import('../../../../web/src/pages/ChatAgent/components/viewers/html/pageProtocol').MAX_AREA_ELEMENTS} */
var MAX_AREA_ELEMENTS = 8;
/** @type {typeof import('../../../../web/src/pages/ChatAgent/components/viewers/html/pageProtocol').MAX_AREA_COUNT} */
var MAX_AREA_COUNT = 500;
var MAX_AREA_QUOTE = 160;
/** @type {typeof import('../../../../web/src/pages/ChatAgent/components/viewers/html/pageProtocol').MAX_TITLE} */
var MAX_TITLE = 300;

// A comment keeps the title its document had when it was made, which names
// the file to the agent and in the chat. Read at the pick, not at the hello:
// the bridge runs before the document's <title> is parsed.
/** @param {Extract<PageToViewerWire, { anchor: any }>} message */
function sendSpot(message) {
  var title = clip(squash(document.title), MAX_TITLE);
  if (title) message.title = title;
  send(message);
}

/** @param {unknown} text */
function squash(text) {
  return String(text == null ? '' : text).replace(/\s+/g, ' ').replace(/^ | $/g, '');
}

/** @param {string} text @param {number} limit */
function clip(text, limit) {
  return text.length > limit ? keptHead(text, limit - 1) + '\u2026' : text;
}

// A cut never leaves half a surrogate pair: the server cannot store one alone.
/** @param {string} text @param {number} length */
function keptHead(text, length) {
  var last = text.charCodeAt(length - 1);
  return text.slice(0, last >= 0xd800 && last <= 0xdbff ? length - 1 : length);
}

/** @param {string} text @param {number} length */
function keptTail(text, length) {
  var from = Math.max(0, text.length - length);
  var first = text.charCodeAt(from);
  return text.slice(first >= 0xdc00 && first <= 0xdfff ? from + 1 : from);
}

/** @param {{ left: number, top: number, width: number, height: number }} r */
function box(r) {
  return { x: round(r.left), y: round(r.top), width: round(r.width), height: round(r.height) };
}

/** @param {number} n */
function fraction(n) {
  return Math.round(Math.min(1, Math.max(0, n)) * 1000) / 1000;
}

/** @param {object} target @param {string} key @param {unknown} value */
function put(target, key, value) {
  if (value) /** @type {Record<string, unknown>} */ (target)[key] = value;
}

/** @param {(Element & { isContentEditable?: boolean, type?: string }) | null} el */
function isTextEntry(el) {
  if (!el || el.nodeType !== 1) return false;
  if (el.isContentEditable) return true;
  var name = el.localName;
  if (name === 'textarea' || name === 'select') return true;
  return name === 'input' && !/^(button|checkbox|color|file|hidden|image|radio|range|reset|submit)$/.test(/** @type {string} */ (el.type));
}

// ---- What a click or a selection is about ----

/** @param {EventTarget | null} node */
function elementOf(node) {
  var n = /** @type {Node | null} */ (node);
  return n && n.nodeType === 1 ? /** @type {Element} */ (n) : n && n.parentElement;
}

/** @param {EventTarget | null} node */
function refused(node) {
  var el = elementOf(node);
  return !!(el && el.closest && el.closest('[data-uncommentable]'));
}

// The element a comment at this node pins to, or null where none can.
// A page lifts a spot to a bigger one with data-comment-target. A chart's
// marks are redrawn by its script, so a drawing anchors whole; the pick
// point keeps where on it the reader meant.
/** @param {EventTarget | null} node */
function commentable(node) {
  var el = elementOf(node);
  if (!el || !el.closest || refused(el)) return null;
  var lifted = el.closest('[data-comment-target]');
  if (lifted) return lifted;
  for (var n = /** @type {Element | null} */ (el); n && n.nodeType === 1; n = n.parentElement) {
    if (n.localName === 'svg') el = n;
  }
  if (el === document.documentElement || el === document.body) return null;
  return el;
}

/** @param {string} name */
function cssIdent(name) {
  if (window.CSS && CSS.escape) return CSS.escape(name);
  return name.replace(/[^a-zA-Z0-9_-]/g, function (c) { return '\\' + c; });
}

/** @param {Element} el */
function step(el) {
  var name = el.localName;
  var index = 1;
  var others = false;
  var s;
  for (s = el.previousElementSibling; s; s = s.previousElementSibling) {
    if (s.localName === name) index++;
  }
  for (s = el.nextElementSibling; s && !others; s = s.nextElementSibling) {
    if (s.localName === name) others = true;
  }
  var tag = cssIdent(name);
  return index > 1 || others ? tag + ':nth-of-type(' + index + ')' : tag;
}

// A CSS path from the nearest ancestor with an id the document holds once.
/** @param {Element} el */
function pathOf(el) {
  var steps = [];
  for (var n = /** @type {Element | null} */ (el); n && n.nodeType === 1; n = n.parentElement) {
    if (n.id) {
      var id = '#' + cssIdent(n.id);
      /** @type {NodeListOf<Element> | null} */
      var found = null;
      try {
        found = document.querySelectorAll(id);
      } catch (e) {
        found = null;
      }
      if (found && found.length === 1) {
        steps.unshift(id);
        break;
      }
    }
    if (n === document.body || n === document.documentElement) {
      steps.unshift(n.localName);
      break;
    }
    steps.unshift(step(n));
  }
  var path = steps.join(' > ');
  return path && path.length <= MAX_PATH ? path : null;
}

/** @param {unknown} path */
function byPath(path) {
  if (typeof path !== 'string' || !path) return null;
  try {
    return document.querySelector(path);
  } catch (e) {
    return null;
  }
}

/** @param {Element} el @param {string} name */
function attr(el, name) {
  return el.getAttribute ? squash(el.getAttribute(name)) : '';
}

// A gap between two runs of text on one line reads as a space past this
// fraction of the smaller font beside it. A word space is about a quarter
// of an em in most faces; a unit set tight after its number sits nearer,
// as typesetting's thin space (a fifth of an em) does: "$59.69B" is 2 px
// at 15 px, 0.13 em, and "B" to "62.0% margin" 9 px at 12 px, 0.75 em.
// The smaller font, since the gap shows against the smaller text.
var GAP_SPACE = 0.25;

/** @param {string} a @param {string} b */
function sameChar(a, b) {
  return a === b || a.toLowerCase() === b.toLowerCase();
}

// Where a character of a text node is drawn, or null where it is not.
/** @param {Text} node @param {number} at */
function charBox(node, at) {
  var code = node.data.charCodeAt(at);
  if (code >= 0xdc00 && code <= 0xdfff && at > 0) at--;
  var range = document.createRange();
  range.setStart(node, at);
  range.setEnd(node, charEnd(node.data, at));
  var r = range.getBoundingClientRect();
  return r && (r.width || r.height) ? r : null;
}

/** @param {BridgeRun} run */
function fontSizeOf(run) {
  var size = parseFloat(window.getComputedStyle(/** @type {Element} */ (run.node.parentNode)).fontSize);
  return size > 0 ? size : /** @type {DOMRect} */ (run.box).height;
}

// Whether two runs, the end of one and the start of the next, sit on one
// line with a gap a reader takes for a space.
/** @param {BridgeRun} a @param {BridgeRun} b */
function drawnApart(a, b) {
  if (!a.box || !b.box) return false;
  var shared = Math.min(a.box.bottom, b.box.bottom) - Math.max(a.box.top, b.box.top);
  if (shared < Math.min(a.box.height, b.box.height) / 2) return false;
  // Either way round, for right-to-left text.
  var gap = Math.max(b.box.left - a.box.right, a.box.left - b.box.right);
  return gap > 0 && gap >= GAP_SPACE * Math.min(fontSizeOf(a), fontSizeOf(b));
}

// An element's innerText with a space where the page draws one and its
// text has none. innerText breaks only at a block's edges and at real
// spaces, so runs a margin sets apart run together ("$59.69B62.0%
// margin"). Each run of text is found in innerText by its characters,
// and its gap from the run before is measured where they meet. Only the
// first `reach` characters are read, as a label needs no more and a big
// container holds far more; past them, and past a run innerText does not
// hold as written (hidden, or a transform that changes its letters), the
// text is innerText's. Spaces only: the lines stay innerText's.
/** @param {Element & { innerText?: string }} el @param {number} reach */
function spacedText(el, reach) {
  if (typeof el.innerText !== 'string') return String(el.textContent == null ? '' : el.textContent);
  var text = el.innerText;
  if (!reach || !window.Range || !Range.prototype.getBoundingClientRect) return text;
  var out = '';
  var copied = 0;
  var pos = 0;
  var seen = 0;
  var runs = 0;
  /** @type {BridgeRun | null} */
  var last = null;
  var walker = document.createTreeWalker(el, 4);
  /** @type {Text | null} */
  var node;
  try {
    walk: while (seen <= reach && runs <= reach && (node = /** @type {Text | null} */ (walker.nextNode()))) {
      var data = node.data;
      var i = 0;
      while (i < data.length && isSpace(data.charCodeAt(i))) i++;
      if (i === data.length || SKIPPED.test(/** @type {Element} */ (node.parentNode).localName || '')) continue;
      runs++;
      var head = charBox(node, i);
      var at = pos;
      while (at < text.length && isSpace(text.charCodeAt(at))) at++;
      // Not drawn, or drawn and left out of innerText: no run of it.
      if (!head || at === text.length || !sameChar(text.charAt(at), data.charAt(i))) continue;
      var run = { node: node, box: head };
      if (last && !isSpace(text.charCodeAt(at - 1)) && drawnApart(last, run)) {
        out += text.slice(copied, at) + ' ';
        copied = at;
      }
      var end = i;
      for (; i < data.length; i++) {
        if (isSpace(data.charCodeAt(i))) continue;
        while (at < text.length && isSpace(text.charCodeAt(at))) at++;
        if (seen > reach || at === text.length || !sameChar(text.charAt(at), data.charAt(i))) break walk;
        at++;
        seen++;
        end = i;
      }
      pos = at;
      last = { node: node, box: charBox(node, end) };
    }
  } catch (e) {
    return text;
  }
  return out + text.slice(copied);
}

// What the reader saw of an element: an image is its alt, anything else
// its rendered text (an SVG has no innerText, so its text content),
// spaced as drawn through its first `reach` characters, a label's unless
// given.
/** @param {Element} el @param {number} [reach] */
function visibleText(el, reach) {
  if (el.localName === 'img') return attr(el, 'alt');
  return squash(spacedText(el, reach === undefined ? MAX_LABEL : reach));
}

var HEADINGS = 'h1,h2,h3,h4,h5,h6,[role="heading"]';

// The heading a reader would say the element is under: one inside it, or
// else the last one before it in the document.
/** @param {Element} el */
function headingElement(el) {
  var inner = el.querySelector && el.querySelector(HEADINGS);
  if (inner) return inner;
  var all = document.querySelectorAll(HEADINGS);
  /** @type {Element | null} */
  var best = null;
  for (var i = 0; i < all.length; i++) {
    // 2: the heading precedes the element (an ancestor heading does too).
    if (el.compareDocumentPosition(all[i]) & 2) best = all[i];
    else break;
  }
  return best;
}

/** @param {Element} el */
function headingFor(el) {
  var heading = headingElement(el);
  return heading ? visibleText(heading) : '';
}

var SHORT_LABEL = 60;

// Cut at a word boundary, the ellipsis included in the limit. A text with
// no space late enough to cut at (one long token, or CJK, which has none)
// is cut where it reaches the limit.
/** @param {string} text @param {number} limit */
function cutAtWord(text, limit) {
  var room = limit - 1;
  var cut = text.slice(0, room);
  if (text.charAt(room) !== ' ') {
    var space = cut.lastIndexOf(' ');
    if (space >= limit / 3) cut = cut.slice(0, space);
  }
  return (cut.replace(/[\s,.;:\u2013\u2014-]+$/, '') || cut) + '\u2026';
}

/** @param {string} text */
function short(text) {
  return text.length <= SHORT_LABEL ? text : cutAtWord(text, SHORT_LABEL);
}

// The element's first `count` rendered lines, whole, from innerText,
// where a block's edges are still line breaks.
/** @param {Element} el @param {number} count */
function linesOf(el, count) {
  var all = spacedText(el, MAX_LABEL).split(/[\n\r]+/);
  var lines = [];
  for (var i = 0; i < all.length && lines.length < count; i++) {
    var line = squash(all[i]);
    if (line) lines.push(line);
  }
  return lines;
}

/** @param {Element} el */
function firstLine(el) {
  return linesOf(el, 1)[0] || '';
}

var LABEL_PARTS = 3;

// A container named by what it holds: the first lines of its first few
// children that show text, or '' for an element that is not one. Its own
// first line is its first child's, and when that child is a block of
// several lines (a tile, in a grid of them) the line names only the child:
// the grid read as its first tile. A first child of one line is a
// caption, which names the element as a heading would: a tile by its
// label. Text of its own, even a word between two links, makes it a
// passage rather than a container. A child not drawn is skipped: its
// innerText is all its text.
/** @param {Element} el */
function partsOf(el) {
  var parts = [];
  var more = false;
  for (var n = el.firstChild; n; n = n.nextSibling) {
    if (n.nodeType === 3) {
      if (squash(/** @type {Text} */ (n).data)) return '';
    } else if (n.nodeType === 1 && !more && /** @type {Element} */ (n).getClientRects().length) {
      var lines = linesOf(/** @type {Element} */ (n), 2);
      if (!lines.length) continue;
      if (!parts.length && lines.length < 2) return '';
      if (parts.length < LABEL_PARTS) parts.push(lines[0]);
      else more = true;
    }
  }
  if (parts.length < 2 || firstLine(el) !== parts[0]) return '';
  var text = parts.join(', ');
  return short(more ? text + '\u2026' : text);
}

// What the hover chip shows and the agent reads as the comment's place.
// An element with text of its own is named by it (its heading, else its
// first line), and a container by its parts: the heading before it would
// name every tile under a page's title after that title.
/** @param {Element} el */
function labelOf(el) {
  var label = attr(el, 'aria-label') || attr(el, 'title') || attr(el, 'alt');
  if (!label && el.localName === 'svg') {
    var title = el.querySelector('title');
    label = title ? squash(title.textContent) : '';
  }
  if (!label) {
    var own = visibleText(el);
    if (own && own.length <= SHORT_LABEL) {
      label = own;
    } else if (own) {
      var inner = el.querySelector(HEADINGS);
      label = (inner && visibleText(inner)) || partsOf(el) || short(firstLine(el));
    }
  }
  if (!label) label = headingFor(el);
  return label ? clip(label, MAX_LABEL) : null;
}

/** @param {Element} el @param {BridgePoint | null} [event] */
function elementAnchor(el, event) {
  var r = el.getBoundingClientRect();
  /** @type {ElementAnchor} */
  var anchor = { kind: 'element' };
  put(anchor, 'path', pathOf(el));
  put(anchor, 'label', labelOf(el));
  // Spaced as the label is: a quote is found again only through normal(),
  // which drops every space.
  var quote = visibleText(el, MAX_QUOTE);
  put(anchor, 'quote', quote && clip(quote, MAX_QUOTE));
  if (event && r.width > 0 && r.height > 0) {
    anchor.point = {
      x: fraction((event.clientX - r.left) / r.width),
      y: fraction((event.clientY - r.top) / r.height),
    };
  }
  return { anchor: anchor, rect: box(r) };
}

// A block around the selection big enough to find it again by its
// neighbours, without reading the whole document for every selection.
/** @param {Element} el */
function blockAround(el) {
  var block = el;
  while (block.parentElement && block !== document.body && squash(block.textContent).length < 240) {
    block = block.parentElement;
  }
  return block;
}

function selectedRange() {
  var sel = window.getSelection ? window.getSelection() : null;
  if (!sel || sel.isCollapsed || !sel.rangeCount) return null;
  return sel.getRangeAt(0);
}

// Letters of a word in scripts that space their words; CJK is left out,
// since each of its characters already stands alone.
var WORD = /[0-9A-Za-z_'\u2019\u00c0-\u024f\u0370-\u03ff\u0400-\u04ff]/;
var SNAP = 40;

/** @param {string} ch */
function isWord(ch) {
  return !!ch && WORD.test(ch);
}

// The selection grown to whole words where it starts or ends inside one,
// so the quote never opens on a fragment ("arter ended"). A copy: the
// reader's own selection stays as they made it.
/** @param {Range} live */
function wholeWords(live) {
  var range = live.cloneRange();
  var node = /** @type {TextLike} */ (range.startContainer);
  var at = range.startOffset;
  if (node.nodeType === 3 && isWord(node.data.charAt(at - 1)) && isWord(node.data.charAt(at))) {
    var first = Math.max(0, at - SNAP);
    while (at > first && isWord(node.data.charAt(at - 1))) at--;
    range.setStart(node, at);
  }
  node = /** @type {TextLike} */ (range.endContainer);
  at = range.endOffset;
  if (node.nodeType === 3 && isWord(node.data.charAt(at - 1)) && isWord(node.data.charAt(at))) {
    var last = Math.min(node.data.length, at + SNAP);
    while (at < last && isWord(node.data.charAt(at))) at++;
    range.setEnd(node, at);
  }
  return range;
}

// The selection's words without the spaces at either end, as the anchor
// finds them again, so the mark drawn at once is the one that stays.
/** @param {Range} range */
function trimmed(range) {
  var r = range.cloneRange();
  var node = /** @type {TextLike} */ (r.startContainer);
  var at = r.startOffset;
  if (node.nodeType === 3) {
    while (at < node.data.length && isSpace(node.data.charCodeAt(at))) at++;
    r.setStart(node, at);
  }
  node = /** @type {TextLike} */ (r.endContainer);
  at = r.endOffset;
  if (node.nodeType === 3) {
    while (at > 0 && isSpace(node.data.charCodeAt(at - 1))) at--;
    if (node !== r.startContainer || at > r.startOffset) r.setEnd(node, at);
  }
  return r;
}

var TABLE_PART = /^(table|thead|tbody|tfoot|tr)$/;
// Past this much text a block is a section rather than a tile, named by
// its heading.
var CAPTIONED = 240;

// A block's caption: its aria-label or title, or a first child of one line
// over more lines below it (a tile's label over its value), unless the
// selected words start in that child.
/** @param {Element} el @param {Range} range */
function captionOf(el, range) {
  var name = attr(el, 'aria-label') || attr(el, 'title');
  if (name) return { text: name, at: el };
  if (linesOf(el, 2).length < 2) return null;
  for (var c = el.firstElementChild; c; c = c.nextElementSibling) {
    if (!c.getClientRects().length) continue;
    var lines = linesOf(c, 2);
    if (!lines.length) continue;
    if (lines.length > 1 || c.contains(range.startContainer)) return null;
    return { text: short(lines[0]), at: c };
  }
  return null;
}

// What selected words are on, for the box's head and the agent's label:
// the caption of the small block around them, as "Revenue" names the
// "$46.7B" under it, else the nearest heading. A heading standing between
// that caption and the words is nearer, and names them instead. A table
// goes by its heading: its first cell names nothing else.
/** @param {Element} host @param {Range} range */
function textLabel(host, range) {
  var heading = headingElement(host);
  for (var el = /** @type {Element | null} */ (host), depth = 0; el && el !== document.body && depth < 4; el = el.parentElement, depth++) {
    if (TABLE_PART.test(el.localName) || squash(el.textContent).length > CAPTIONED) break;
    if (ownText(el)) continue;
    var named = captionOf(el, range);
    if (!named) continue;
    // 4: the heading follows the caption, so it is nearer the words.
    if (heading && named.at !== heading && named.at.compareDocumentPosition(heading) & 4) break;
    return named.text;
  }
  return heading ? visibleText(heading) : '';
}

// A text anchor for selected words, grown to whole words. `release`, a
// point the pointer let go or clicked at, keeps where on the words the
// pin stands, as a click's point does on an element.
/** @param {Range | null} live @param {BridgePoint | null} release */
function rangeAnchor(live, release) {
  if (!live || live.collapsed) return null;
  var range = wholeWords(live);
  var quote = squash(range.toString());
  if (!quote) return null;
  var host = elementOf(range.commonAncestorContainer);
  if (!host || !document.body || !document.body.contains(host) || refused(host)) return null;
  var rect = range.getBoundingClientRect();
  if (!rect.width && !rect.height) return null;
  /** @type {TextAnchor} */
  var anchor = { kind: 'text', quote: clip(quote, MAX_SELECTION) };
  put(anchor, 'path', pathOf(host));
  var label = textLabel(host, range);
  put(anchor, 'label', label && clip(label, MAX_LABEL));
  if (release && rect.width > 0 && rect.height > 0) {
    anchor.point = {
      x: fraction((release.clientX - rect.left) / rect.width),
      y: fraction((release.clientY - rect.top) / rect.height),
    };
  }
  var block = blockAround(host);
  try {
    var before = document.createRange();
    before.setStart(block, 0);
    before.setEnd(range.startContainer, range.startOffset);
    var after = document.createRange();
    after.setStart(range.endContainer, range.endOffset);
    after.setEnd(block, block.childNodes.length);
    put(anchor, 'prefix', keptTail(squash(before.toString()), MAX_AROUND));
    put(anchor, 'suffix', keptHead(squash(after.toString()), MAX_AROUND));
  } catch (e) {
    // The selection reached outside the block; its words alone still find it.
  }
  return { anchor: anchor, rect: box(rect), range: range };
}

function selectionAnchor() {
  return rangeAnchor(selectedRange(), null);
}

