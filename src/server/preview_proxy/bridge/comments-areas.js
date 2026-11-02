// @ts-check
// The comment half of the bridge, continued: glyphs under the pointer and the
// areas dragged out. Shares one function with the parts before it.

// ---- Glyphs under the pointer, in comment mode ----

// How far outside a character's box, in px, still counts as on it.
var GLYPH_SLOP = 2;
// Text in these is a control's or a drawing's, not words a reader
// selects: the element takes the comment, as a click anywhere on it does.
var NOT_TEXT = 'svg,button,input,textarea,select,option,optgroup';

// The caret position the browser finds at a point, by whichever of the
// two APIs it has.
/** @param {number} x @param {number} y */
function caretAt(x, y) {
  if (document.caretPositionFromPoint) {
    var pos = document.caretPositionFromPoint(x, y);
    return pos && pos.offsetNode ? { node: pos.offsetNode, offset: pos.offset } : null;
  }
  if (document.caretRangeFromPoint) {
    var range = document.caretRangeFromPoint(x, y);
    return range ? { node: range.startContainer, offset: range.startOffset } : null;
  }
  return null;
}

// Whether a reader could select the text in an element: not in a field,
// an editor or a control, and not made unselectable. Chrome inherits
// user-select and Firefox does not, so the nearest element that says
// anything but auto decides.
/** @param {HTMLElement | null} el */
function selectable(el) {
  if (!el || refused(el) || el.isContentEditable) return false;
  if (el.closest && el.closest(NOT_TEXT)) return false;
  for (var n = /** @type {HTMLElement | null} */ (el); n && n.nodeType === 1; n = n.parentElement) {
    var style = window.getComputedStyle(n);
    var value = style.userSelect || style.webkitUserSelect;
    if (value === 'none') return false;
    if (value && value !== 'auto') return true;
  }
  return true;
}

/** @param {string} data @param {number} at */
function charEnd(data, at) {
  var code = data.charCodeAt(at);
  return code >= 0xd800 && code <= 0xdbff && at + 1 < data.length ? at + 2 : at + 1;
}

// The first of `offsets` whose character's box holds the point, in a text
// node a reader could select.
/**
 * @param {Text | null} node
 * @param {number[]} offsets
 * @param {number} x
 * @param {number} y
 * @returns {BridgeGlyph | null}
 */
function glyphIn(node, offsets, x, y) {
  if (!node || node.nodeType !== 3 || !/\S/.test(node.data) || !selectable(node.parentElement)) return null;
  for (var i = 0; i < offsets.length; i++) {
    var at = offsets[i];
    if (at < 0 || at >= node.data.length) continue;
    var range = document.createRange();
    range.setStart(node, at);
    range.setEnd(node, charEnd(node.data, at));
    var rects = range.getClientRects();
    for (var k = 0; k < rects.length; k++) {
      var r = rects[k];
      if (r.width > 0 && r.height > 0 && x >= r.left - GLYPH_SLOP && x <= r.right + GLYPH_SLOP &&
          y >= r.top - GLYPH_SLOP && y <= r.bottom + GLYPH_SLOP) {
        return { node: node, offset: at, box: r };
      }
    }
  }
  return null;
}

// The text node before or after this one, for a caret at its very edge:
// the glyph before a caret at the start of a styled run is the last of
// the run before it.
/** @param {Node} node @param {boolean} forward */
function textBeside(node, forward) {
  if (!document.body || !document.body.contains(node)) return null;
  var walker = document.createTreeWalker(document.body, 4);
  walker.currentNode = node;
  return /** @type {Text | null} */ (forward ? walker.nextNode() : walker.previousNode());
}

// The one hit-test for text in comment mode: whether the point is over
// glyphs a reader could select, not just inside an element that holds
// some. The caret the browser places at the point snaps there from blank
// space too, so it counts only once the box of the character at it, or
// just before it, holds the point. Null off glyphs, and in a browser with
// neither caret API, where every press is the element's or an area's.
/** @param {number} x @param {number} y */
function glyphAt(x, y) {
  /** @type {{ node: Node, offset: number } | null} */
  var caret = null;
  try {
    caret = caretAt(x, y);
  } catch (e) {
    caret = null;
  }
  if (!caret || caret.node.nodeType !== 3) return null;
  var node = /** @type {Text} */ (caret.node);
  var at = caret.offset;
  var hit = glyphIn(node, [at, at - 1], x, y);
  if (hit || (at > 0 && at < node.data.length)) return hit;
  var beside = textBeside(node, at > 0);
  return beside ? glyphIn(beside, [at > 0 ? 0 : beside.data.length - 1], x, y) : null;
}

// Signs that belong to the number beside them, which a word reaches over:
// "$46.7B" and "72.4%" are one word each.
var SIGNS = /^[$#%+\u00a2-\u00a5\u20ac\u20b9\u2030\u2212]+$/;
// How much of a text node around the pointer is read for its word.
var WORD_REACH = 120;
/** @type {Intl.Segmenter | null | undefined} */
var segmenter;

function wordSegmenter() {
  if (segmenter !== undefined) return segmenter;
  segmenter = null;
  if (!window.Intl || typeof Intl.Segmenter !== 'function') return segmenter;
  var langs = [document.documentElement.lang || undefined, undefined];
  for (var i = 0; i < langs.length && !segmenter; i++) {
    try {
      segmenter = new Intl.Segmenter(langs[i], { granularity: 'word' });
    } catch (e) {
      // A lang attribute that is no locale; the default one reads words too.
    }
  }
  return segmenter;
}

// [start, end) of the word at `at` in `text` by Intl.Segmenter, which
// splits Chinese into words too. On a space or a mark between words, the
// word on the pointer's side of it (`left`), if one touches it.
/** @param {Intl.Segments} segments @param {number} at @param {boolean} left */
function segmentWord(segments, at, left) {
  /** @type {Intl.SegmentData | null | undefined} */
  var hit = segments.containing(at);
  if (!hit) return null;
  if (!hit.isWordLike && !SIGNS.test(hit.segment)) {
    hit = left ? (hit.index > 0 ? segments.containing(hit.index - 1) : null) : segments.containing(hit.index + hit.segment.length);
    if (!hit || !hit.isWordLike) return null;
  }
  var start = hit.index;
  var end = hit.index + hit.segment.length;
  if (!hit.isWordLike) {
    // A sign alone: the number it belongs to.
    var next = segments.containing(end);
    var prev = start > 0 ? segments.containing(start - 1) : null;
    if (next && next.isWordLike) end = next.index + next.segment.length;
    else if (prev && prev.isWordLike) start = prev.index;
    else return null;
  }
  var s;
  for (s = start > 0 ? segments.containing(start - 1) : null; s && !s.isWordLike && SIGNS.test(s.segment);
    s = s.index > 0 ? segments.containing(s.index - 1) : null) {
    start = s.index;
  }
  for (s = segments.containing(end); s && !s.isWordLike && SIGNS.test(s.segment); s = segments.containing(s.index + s.segment.length)) {
    end = s.index + s.segment.length;
  }
  return [start, end];
}

// The same without a segmenter: the run of characters between spaces.
/** @param {string} text @param {number} at @param {boolean} left */
function spacedWord(text, at, left) {
  if (isSpace(text.charCodeAt(at))) {
    at = left ? at - 1 : at + 1;
    if (at < 0 || at >= text.length || isSpace(text.charCodeAt(at))) return null;
  }
  var start = at;
  var end = at + 1;
  while (start > 0 && !isSpace(text.charCodeAt(start - 1))) start--;
  while (end < text.length && !isSpace(text.charCodeAt(end))) end++;
  return [start, end];
}

// The word a glyph is part of, as a range in its text node, or null where
// the pointer is on a mark no word touches.
/** @param {BridgeGlyph} glyph @param {number} x */
function wordAt(glyph, x) {
  var node = glyph.node;
  var from = Math.max(0, glyph.offset - WORD_REACH);
  var text = node.data.slice(from, Math.min(node.data.length, glyph.offset + WORD_REACH));
  var at = glyph.offset - from;
  var left = x < glyph.box.left + glyph.box.width / 2;
  var seg = wordSegmenter();
  /** @type {number[] | null} */
  var span = null;
  if (seg) {
    try {
      span = segmentWord(seg.segment(text), at, left);
    } catch (e) {
      span = spacedWord(text, at, left);
    }
  } else {
    span = spacedWord(text, at, left);
  }
  if (!span || span[1] <= span[0]) return null;
  var range = document.createRange();
  range.setStart(node, from + span[0]);
  range.setEnd(node, from + span[1]);
  return range;
}

// Where a word is drawn: its box on the pointer's line, as one broken
// across two lines has a box on each.
/** @param {Range} range @param {number} y */
function wordRect(range, y) {
  var rects = range.getClientRects();
  var best = null;
  for (var i = 0; i < rects.length; i++) {
    var r = rects[i];
    if (!r.width || !r.height) continue;
    if (y >= r.top - GLYPH_SLOP && y <= r.bottom + GLYPH_SLOP) return box(r);
    if (!best) best = r;
  }
  return best ? box(best) : null;
}

// ---- What an area dragged out holds ----

// How much of an element's box lies inside the area for it to be in it.
var AREA_INSIDE = 0.7;
// A page of tens of thousands of nodes is read in part rather than stalling
// the release.
var MAX_AREA_VISITS = 20000;
// Elements that mean something with no text of their own, and that a
// comment takes whole: nothing inside one is a part of its own.
var MEDIA = /^(img|svg|canvas|video|audio|picture|iframe|embed|object|input|select|textarea|button|meter|progress)$/;
var HAS_MEDIA = 'img,svg,canvas,video,picture,iframe,embed,object';
var NOT_SHOWN = /^(script|style|noscript|template|title|head|meta|link)$/;

/** @param {BridgeEdges} r @param {BridgeEdges} area */
function overlap(r, area) {
  var w = Math.min(r.right, area.right) - Math.max(r.left, area.left);
  var h = Math.min(r.bottom, area.bottom) - Math.max(r.top, area.top);
  return w > 0 && h > 0 ? w * h : 0;
}

/** @param {BridgeEdges | null} a @param {BridgeEdges} r */
function union(a, r) {
  if (!a) return { left: r.left, top: r.top, right: r.right, bottom: r.bottom };
  return {
    left: Math.min(a.left, r.left), top: Math.min(a.top, r.top),
    right: Math.max(a.right, r.right), bottom: Math.max(a.bottom, r.bottom),
  };
}

/** @param {BridgeEdges} u */
function sized(u) {
  return { left: u.left, top: u.top, width: u.right - u.left, height: u.bottom - u.top };
}

/** @param {Element} el */
function ownText(el) {
  for (var n = el.firstChild; n; n = n.nextSibling) {
    if (n.nodeType === 3 && /\S/.test(/** @type {Text} */ (n).data)) return true;
  }
  return false;
}

// A spacer, or a wrapper around nothing to read or see, is no part of an
// area. textContent, not innerText: this runs for every element the area
// touches, and innerText lays the page out each time it is read.
/** @param {Element} el */
function meaningful(el) {
  if (MEDIA.test(el.localName) || el.hasAttribute('data-comment-target')) return true;
  if (attr(el, 'aria-label') || attr(el, 'title') || attr(el, 'alt')) return true;
  return /\S/.test(el.textContent) || !!el.querySelector(HAS_MEDIA);
}

/** @param {Element} el */
function atomic(el) {
  return MEDIA.test(el.localName) || el.hasAttribute('data-comment-target') || ownText(el);
}

// The children an area could hold: drawn, and something to read or see.
/** @param {Element} el */
function itemsOf(el) {
  var parts = [];
  for (var c = el.firstElementChild; c; c = c.nextElementSibling) {
    if (NOT_SHOWN.test(c.localName) || refused(c) || !c.getClientRects().length || !meaningful(c)) continue;
    var r = c.getBoundingClientRect();
    if (r.width * r.height > 0) parts.push({ el: c, r: r });
  }
  return parts;
}

// What kind of item an element is among its siblings: its tag and class.
/** @param {Element} el */
function kindOf(el) {
  return el.localName + '.' + squash(el.getAttribute('class'));
}

// Whether the area leaves out one of a container's items: a child of the
// same kind as one it took, lying mostly outside. Only that splits a
// container mostly inside. A tile whose footnote line falls outside is
// still the tile, since the line is no item of it; a list with its last
// item outside is its other items. No measure of area tells the two
// apart: either way part of the container is left out.
/** @param {Element} el @param {BridgeEdges} area */
function leavesOutAnItem(el, area) {
  var parts = itemsOf(el);
  /** @type {Record<string, boolean>} */
  var taken = {};
  var left = [];
  for (var i = 0; i < parts.length; i++) {
    var size = parts[i].r.width * parts[i].r.height;
    var inside = overlap(parts[i].r, area) / size;
    if (inside >= AREA_INSIDE) taken['#' + kindOf(parts[i].el)] = true;
    else if (inside <= 1 - AREA_INSIDE) left.push('#' + kindOf(parts[i].el));
  }
  for (var j = 0; j < left.length; j++) {
    if (taken[left[j]]) return true;
  }
  return false;
}

// An element the area takes, or the one element it wraps: a wrapper whose
// box is its one child's says the same less precisely.
/** @param {Element} el */
function unwrap(el) {
  while (!atomic(el)) {
    var parts = itemsOf(el);
    if (parts.length !== 1) break;
    var outer = el.getBoundingClientRect();
    if (parts[0].r.width * parts[0].r.height < 0.9 * outer.width * outer.height) break;
    el = parts[0].el;
  }
  return el;
}

// Collects, in document order, the outermost elements under `el` that lie
// mostly inside the area (AREA_INSIDE of their box), each taken whole
// unless the area leaves out one of its items. Text, a lifted spot and a
// drawing are never split.
/**
 * @param {Element} el
 * @param {BridgeEdges} area
 * @param {{ visits: number, most: { el: Element | null, inside: number, size: number } }} walk
 * @param {Element[]} found
 */
function areaParts(el, area, walk, found) {
  if (walk.visits++ > MAX_AREA_VISITS) return;
  if (NOT_SHOWN.test(el.localName) || el.hasAttribute('data-uncommentable')) return;
  var r = el.getClientRects().length ? el.getBoundingClientRect() : null;
  var size = r ? r.width * r.height : 0;
  var inside = size > 0 ? overlap(/** @type {DOMRect} */ (r), area) : 0;
  // Wholly outside, its children are too; one with no box of its own
  // (display: contents, a collapsed float parent) may still hold some.
  if (size > 0 && !inside) return;
  var top = el === document.body || el === document.documentElement;
  if (!top && size > 0) {
    if (!meaningful(el)) return;
    if (inside > walk.most.inside || (inside === walk.most.inside && size < walk.most.size)) {
      walk.most = { el: el, inside: inside, size: size };
    }
    if (inside >= AREA_INSIDE * size) {
      if (atomic(el) || !leavesOutAnItem(el, area)) {
        found.push(unwrap(el));
        return;
      }
    } else if (atomic(el)) {
      return;
    }
  }
  for (var c = el.firstElementChild; c; c = c.nextElementSibling) areaParts(c, area, walk, found);
}

// The elements an area holds, outermost first and in document order, and
// the element with the most of it, for when the area holds none.
/** @param {BridgeEdges} area */
function areaElements(area) {
  var walk = { visits: 0, most: { el: null, inside: 0, size: Infinity } };
  /** @type {Element[]} */
  var found = [];
  if (document.body) areaParts(document.body, area, walk, found);
  return { els: found, most: walk.most.el };
}

// An area's label lists every part, so each is named briefly: a block of
// several lines with no text of its own (a tile) by its first, as its
// caption names it, rather than by all it says. Anything else is named
// as a click on it would be.
/** @param {Element} el */
function partLabel(el) {
  if (!ownText(el) && linesOf(el, 2).length > 1) {
    var inner = el.querySelector(HEADINGS);
    var label = attr(el, 'aria-label') || attr(el, 'title') || (inner && visibleText(inner)) || short(firstLine(el));
    if (label) return clip(label, MAX_LABEL);
  }
  return labelOf(el);
}

// Words cut to `limit` at a word, as a label is.
/** @param {string} text @param {number} limit */
function clipWords(text, limit) {
  return text.length <= limit ? text : cutAtWord(text, limit);
}

/** @param {number} n */
function fine(n) {
  return Math.round(Math.min(1, Math.max(0, n)) * 10000) / 10000;
}

// An area's anchor: each element as an element anchor names it, and a
// label naming the parts. Only the elements a later version could find
// again count: by a path, or by their words. Null when fewer than two can.
// Its pin stands where the drag was let go (`release`), kept as a point
// on the area as a click's is on an element: a corner, mostly.
/** @param {Element[]} els @param {BridgePoint} release */
function areaAnchor(els, release) {
  var elements = [];
  var count = 0;
  var labels = [];
  /** @type {Record<string, boolean>} */
  var seen = {};
  var length = 0;
  /** @type {BridgeEdges | null} */
  var bounds = null;
  for (var i = 0; i < els.length && count < MAX_AREA_COUNT; i++) {
    var el = els[i];
    var path = pathOf(el);
    // Spaced only where it is kept as a quote: an area may hold hundreds.
    var text = visibleText(el, elements.length < MAX_AREA_ELEMENTS ? MAX_AREA_QUOTE : 0);
    if (!path && !text) continue;
    count++;
    bounds = union(bounds, el.getBoundingClientRect());
    var label = count <= MAX_AREA_ELEMENTS || length <= MAX_LABEL ? partLabel(el) : null;
    if (label && !seen['#' + label] && length <= MAX_LABEL) {
      seen['#' + label] = true;
      labels.push(label);
      length += label.length + 2;
    }
    if (elements.length < MAX_AREA_ELEMENTS) {
      var part = {};
      put(part, 'path', path);
      put(part, 'label', label);
      put(part, 'quote', text && clipWords(text, MAX_AREA_QUOTE));
      elements.push(part);
    }
  }
  if (count < 2) return null;
  // Filled in below, so it only becomes a whole AreaAnchor at the return.
  var anchor = /** @type {AreaAnchor} */ (/** @type {unknown} */ ({ kind: 'area' }));
  var named = labels.join(', ');
  put(anchor, 'label', named && clipWords(named, MAX_LABEL));
  anchor.elements = elements;
  anchor.count = count;
  var r = sized(/** @type {BridgeEdges} */ (bounds));
  if (r.width > 0 && r.height > 0) {
    anchor.point = {
      x: fraction((release.clientX - r.left) / r.width),
      y: fraction((release.clientY - r.top) / r.height),
    };
  }
  // Where the area is as fractions of the document, for a version on
  // which none of its elements can be found.
  var doc = document.documentElement;
  var width = Math.max(1, doc.scrollWidth);
  var height = Math.max(1, doc.scrollHeight);
  anchor.rect = {
    x: fine((r.left + window.scrollX) / width),
    y: fine((r.top + window.scrollY) / height),
    w: fine(r.width / width),
    h: fine(r.height / height),
  };
  return { anchor: anchor, rect: box(r) };
}

