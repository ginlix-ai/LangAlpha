// @ts-check
// The comment half of the bridge, continued: finding an anchor again, what is
// reported to the viewer, and the words being commented on. Shares one function
// with the parts before it.

// ---- Finding an anchor again ----

/** @typedef {{ text: string, nodes: Text[], nodeAt: number[], offsetAt: number[] }} TextIndex */

var SKIPPED = /^(script|style|noscript|template|title)$/;

/** @param {number} code */
function isSpace(code) {
  return code <= 32 || code === 160 || code === 0x2028 || code === 0x2029 || code === 0x3000 || code === 0xfeff ||
    (code >= 0x2000 && code <= 0x200b);
}

// Lower case with every space dropped: the stored quote is the rendered
// text, which spaces table cells and capitalises with CSS, and the text
// nodes are neither.
/** @param {unknown} text */
function normal(text) {
  var out = '';
  var s = String(text || '');
  for (var i = 0; i < s.length; i++) {
    if (!isSpace(s.charCodeAt(i))) out += s.charAt(i);
  }
  return out.toLowerCase();
}

// The body's text in that same form, each character mapped back to the
// text node and offset it came from.
function textIndex() {
  var body = document.body;
  if (!body) return null;
  var chars = [];
  var nodeAt = [];
  var offsetAt = [];
  var nodes = [];
  var walker = document.createTreeWalker(body, 4);
  /** @type {Text | null} */
  var node;
  while ((node = /** @type {Text | null} */ (walker.nextNode()))) {
    var parent = /** @type {Element | null} */ (node.parentNode);
    if (parent && parent.localName && SKIPPED.test(parent.localName)) continue;
    var data = node.data;
    var at = nodes.length;
    nodes.push(node);
    for (var i = 0; i < data.length; i++) {
      if (isSpace(data.charCodeAt(i))) continue;
      var lower = data.charAt(i).toLowerCase();
      chars.push(lower.length === 1 ? lower : data.charAt(i));
      nodeAt.push(at);
      offsetAt.push(i);
    }
  }
  return { text: chars.join(''), nodes: nodes, nodeAt: nodeAt, offsetAt: offsetAt };
}

/** @param {string} a @param {string} b */
function sharedTail(a, b) {
  var n = 0;
  while (n < a.length && n < b.length && a.charAt(a.length - 1 - n) === b.charAt(b.length - 1 - n)) n++;
  return n;
}

/** @param {string} a @param {string} b */
function sharedHead(a, b) {
  var n = 0;
  while (n < a.length && n < b.length && a.charAt(n) === b.charAt(n)) n++;
  return n;
}

// The occurrence of `quote` its stored neighbours fit best, as a range;
// only inside `within` when given.
/**
 * @param {TextIndex | null} index
 * @param {string} quote
 * @param {string} prefix
 * @param {string} suffix
 * @param {Element | null} within
 */
function findText(index, quote, prefix, suffix, within) {
  var q = normal(quote);
  if (!index || q.length < 2) return null;
  var p = normal(prefix);
  var s = normal(suffix);
  var hay = index.text;
  var best = -1;
  var bestScore = -1;
  var from = 0;
  var tries = 0;
  var at;
  while ((at = hay.indexOf(q, from)) !== -1 && tries++ < 200) {
    if (!within || within.contains(index.nodes[index.nodeAt[at]])) {
      var score = sharedTail(hay.slice(Math.max(0, at - p.length), at), p) +
        sharedHead(hay.slice(at + q.length, at + q.length + s.length), s);
      if (score > bestScore) {
        bestScore = score;
        best = at;
      }
    }
    from = at + 1;
  }
  return best < 0 ? null : rangeAt(index, best, q.length);
}

/** @param {TextIndex} index @param {number} at @param {number} length */
function rangeAt(index, at, length) {
  var last = at + length - 1;
  var range = document.createRange();
  range.setStart(index.nodes[index.nodeAt[at]], index.offsetAt[at]);
  range.setEnd(index.nodes[index.nodeAt[last]], index.offsetAt[last] + 1);
  return range;
}

// A path without its positions: the kinds of element it walks through,
// which a block added or removed beside them leaves as they were.
/** @param {unknown} path */
function shapeOf(path) {
  return typeof path === 'string' ? path.replace(/:nth-of-type\([0-9]+\)/g, '') : '';
}

// The element that reads most nearly as `quote` and nothing more, first
// among those that stand where the commented one stood (`shape`). A short
// quote ("Revenue") also turns up in headings, table heads and sentences,
// and the first of those is seldom the element the comment was on. One that
// only mentions the words, in the middle of its own ("GAAP diluted EPS came
// in at..." for a tile labelled "Diluted EPS"), is never it: a pin moved
// there would sit on a stranger, which `readsLike` keeps a path from doing.
/**
 * @param {TextIndex | null} index
 * @param {string} quote
 * @param {string} shape
 * @param {boolean} whole
 */
function elementByQuote(index, quote, shape, whole) {
  var q = normal(quote);
  if (!index || q.length < 2) return null;
  var best = null;
  var bestRank = Infinity;
  var from = 0;
  var tries = 0;
  var at;
  while ((at = index.text.indexOf(q, from)) !== -1 && tries++ < 200) {
    from = at + 1;
    var el = elementOf(rangeAt(index, at, q.length).commonAncestorContainer);
    if (!el || !readsLike(el, quote, whole) || !stillShort(el, quote, whole)) continue;
    var rank = normal(el.textContent).length - q.length;
    if (!shape || shapeOf(pathOf(el)) !== shape) rank += 1e6;
    if (rank < bestRank) {
      bestRank = rank;
      best = el;
    }
  }
  return best;
}

// An element's stored text, up to where it was cut for storage.
/** @param {unknown} quote */
function quoteHead(quote) {
  if (typeof quote !== 'string') return '';
  var head = quote.split('\u2026')[0];
  return normal(head).length >= 4 ? head : '';
}

// The words a stored quote read off an element: an image's are its alt.
/** @param {Element} el */
function wordsOf(el) {
  return el.localName === 'img' ? attr(el, 'alt') : el.textContent;
}

// A selection past the cap was stored cut, so only its opening words can be
// found; a quote that merely ends in an ellipsis is searched as it stands.
/** @param {string} quote */
function selectedWords(quote) {
  return quote.length >= MAX_SELECTION - 1 && quote.charAt(quote.length - 1) === '\u2026'
    ? quote.slice(0, -1)
    : quote;
}

// Whether an element could be the quoted one, edited: it keeps the
// quote's opening words, or its closing ones when the quote was stored
// whole. One that reads nothing like it is another element that came to
// stand at the same path, as the next block does when one is removed.
/** @param {Element} el @param {string} quote @param {boolean} whole */
function readsLike(el, quote, whole) {
  var q = normal(quote);
  var text = normal(wordsOf(el));
  var need = Math.min(8, Math.ceil(q.length / 2));
  return sharedHead(text, q) >= need || (whole && sharedTail(text, q) >= need);
}

// A label stored whole ("Revenue") is not the sentence that begins with it
// ("Revenue beat the consensus by 4%"): past twice its length an element
// has become something else. A clipped quote says nothing of the length.
/** @param {Element} el @param {string} quote @param {boolean} whole */
function stillShort(el, quote, whole) {
  return !whole || normal(el.textContent).length <= 2 * normal(quote).length;
}

// An area is found as its elements are, each by an element's rules, and
// stands where those still found stand.
/**
 * @param {BridgeAnchor} anchor
 * @param {() => TextIndex | null} index
 * @returns {BridgeHit | null}
 */
function areaHit(anchor, index) {
  var parts = Object.prototype.toString.call(anchor.elements) === '[object Array]' ? anchor.elements : [];
  var els = [];
  for (var i = 0; i < parts.length && i < MAX_AREA_ELEMENTS; i++) {
    var part = parts[i];
    if (!part || typeof part !== 'object') continue;
    var hit = hitOf({ kind: 'element', path: part.path, quote: part.quote }, index);
    if (hit && hit.el && els.indexOf(hit.el) === -1) els.push(hit.el);
  }
  return els.length ? { els: els } : null;
}

/**
 * @param {BridgeAnchor} anchor
 * @param {() => TextIndex | null} index
 * @returns {BridgeHit | null}
 */
function hitOf(anchor, index) {
  if (anchor.kind === 'area') return areaHit(anchor, index);
  var el = byPath(anchor.path);
  if (anchor.kind === 'text') {
    if (!anchor.quote) return null;
    var words = selectedWords(anchor.quote);
    var range = (el && findText(index(), words, anchor.prefix, anchor.suffix, el)) ||
      findText(index(), words, anchor.prefix, anchor.suffix, null);
    return range ? { range: range } : null;
  }
  // A path still names an element on a later version, but maybe another
  // one: when that element no longer reads as the quote and some other
  // element does, the quote wins. When none does, an element named by its
  // own id is still itself; one reached by position keeps the pin only if
  // it reads like an edit of the quote, and otherwise the comment shows as
  // on its own version rather than on a stranger.
  var quote = quoteHead(anchor.quote);
  if (el && (!quote || normal(wordsOf(el)).indexOf(normal(quote)) !== -1)) return { el: el };
  var found = quote ? elementByQuote(index(), quote, shapeOf(anchor.path), quote === anchor.quote) : null;
  if (found) return { el: found };
  if (!el) return null;
  var byId = anchor.path.indexOf(' > ') === -1;
  return byId || readsLike(el, quote, quote === anchor.quote) ? { el: el } : null;
}

/**
 * @param {BridgeHit | null} hit
 * @returns {BridgeBox | null | undefined} undefined: the node went with a re-render
 */
function rectOfHit(hit) {
  if (!hit) return null;
  if (hit.els) {
    /** @type {BridgeEdges | null} */
    var bounds = null;
    for (var i = 0; i < hit.els.length; i++) {
      if (!hit.els[i].isConnected) return undefined;
      if (hit.els[i].getClientRects().length) bounds = union(bounds, hit.els[i].getBoundingClientRect());
    }
    return bounds ? box(sized(bounds)) : null;
  }
  if (hit.el) {
    if (!hit.el.isConnected) return undefined;
    if (!hit.el.getClientRects().length) return null;
    return box(hit.el.getBoundingClientRect());
  }
  var range = /** @type {Range} */ (hit.range);
  if (!range.startContainer.isConnected) return undefined;
  var r = range.getBoundingClientRect();
  return r.width || r.height ? box(r) : null;
}

// ---- Reporting ----

var mode = false;
/** @type {BridgeTracked[]} */
var tracked = [];
var stale = true;
var lastPositions = '';
/** @type {Element | null} */
var hovered = null;
/** @type {string | null} */
var hoverLabel = null;
var lastHover = '';
/** @type {BridgeSelection | null} */
var selection = null;
var lastSelection = '';
var pointerDown = false;
// In comment mode, where the pointer is over the page, and while it is
// over glyphs, the word there (null on a mark no word touches).
/** @type {{ x: number, y: number } | null} */
var pointer = null;
/** @type {{ word: Range | null, y: number } | null} */
var hoverText = null;

function resolveAll() {
  /** @type {TextIndex | null | undefined} */
  var cached;
  /** @type {() => TextIndex | null} */
  var index = function () {
    if (cached === undefined) cached = textIndex();
    return cached;
  };
  for (var i = 0; i < tracked.length; i++) {
    try {
      tracked[i].hit = hitOf(tracked[i].anchor, index);
    } catch (e) {
      tracked[i].hit = null;
    }
  }
  stale = false;
  paintMarks();
}

function reportPositions() {
  if (stale) resolveAll();
  var items = [];
  for (var i = 0; i < tracked.length; i++) {
    var rect = rectOfHit(tracked[i].hit);
    if (rect === undefined) {
      // The node went with a re-render; find it again next frame.
      stale = true;
      schedule();
      rect = null;
    }
    items.push({ id: tracked[i].id, rect: rect });
  }
  var key = JSON.stringify(items);
  if (key === lastPositions) return;
  lastPositions = key;
  send({ type: 'page:positions', items: items });
}

// Over glyphs the word under the pointer, which a click takes, and no
// element; elsewhere the element a click would pick.
function reportHover() {
  /** @type {PageToViewerWire} */
  var message;
  if (mode && hoverText) {
    var word = hoverText.word;
    var rect = word && word.startContainer.isConnected ? wordRect(word, hoverText.y) : null;
    message = rect ? { type: 'page:hover', word: true, rect: rect } : { type: 'page:hover', clear: true };
  } else if (mode && hovered && hovered.isConnected) {
    message = { type: 'page:hover', rect: box(hovered.getBoundingClientRect()), label: hoverLabel };
  } else {
    hovered = null;
    message = { type: 'page:hover', clear: true };
  }
  var key = JSON.stringify(message);
  if (key === lastHover) return;
  lastHover = key;
  send(message);
}

function reportSelection() {
  var range = selection ? selectedRange() : null;
  /** @type {PageToViewerWire} */
  var message;
  if (range && selection) {
    message = { type: 'page:selection', anchor: selection.anchor, rect: box(range.getBoundingClientRect()) };
  } else {
    selection = null;
    message = { type: 'page:selection', clear: true };
  }
  var key = JSON.stringify(message);
  if (key === lastSelection) return;
  lastSelection = key;
  if ('anchor' in message) sendSpot(message);
  else send(message);
}

var frame = 0;
function tick() {
  frame = 0;
  if (tracked.length) reportPositions();
  // The page moved under a pointer that did not: what is under it now.
  if (pointer && (hovered || hoverText) && !pressing()) hoverAt(pointer.x, pointer.y);
  if (hovered || hoverText) reportHover();
  if (selection) reportSelection();
}

function schedule() {
  if (!frame) frame = window.requestAnimationFrame(tick);
}

// A page that rebuilds itself (a chart animating, a feed) finds its anchors
// again at most every 200 ms: once at the first change, once after.
var settle = 0;
var unsettled = false;
function onMutate() {
  if (settle) {
    unsettled = true;
    return;
  }
  stale = true;
  schedule();
  settle = window.setTimeout(function () {
    settle = 0;
    if (unsettled) {
      unsettled = false;
      stale = true;
      schedule();
    }
  }, 200);
}

var watching = false;
function watch() {
  if (watching) return;
  watching = true;
  window.addEventListener('resize', schedule);
  window.addEventListener('load', onMutate);
  // Capture: a scroll inside the page's own panes does not bubble.
  document.addEventListener('scroll', schedule, true);
  if (window.MutationObserver) {
    new MutationObserver(onMutate).observe(document.documentElement, {
      childList: true, subtree: true, characterData: true, attributes: true,
    });
  }
  if (window.ResizeObserver) {
    var sizes = new ResizeObserver(schedule);
    sizes.observe(document.documentElement);
    if (document.body) sizes.observe(document.body);
    else document.addEventListener('DOMContentLoaded', function () { if (document.body) sizes.observe(document.body); });
  }
}

// ---- The words a comment is being written on ----

// They stay marked in the app's accent while the comment's box is open.
// The viewer flags the draft's anchor (`mark`), found again like any
// other; words just selected or clicked in comment mode are marked at
// once, until the viewer answers. The selection itself is let go then,
// since it greys out once the box takes the focus. A browser without the
// highlight API keeps the selection instead, which the viewer clears when
// the box closes.
var MARK = '__page_comment_mark__';
// The amber the app's accent is, for a theme that did not say.
var MARK_ACCENT = 'hsl(28 78% 60%)';
/** @type {Range | null} */
var pendingMark = null;

function canMark() {
  return !!(window.CSS && CSS.highlights && typeof window.Highlight === 'function');
}

function markFill() {
  return 'color-mix(in srgb, ' + (accent || MARK_ACCENT) + ' 32%, transparent)';
}

// Through the style sheet, not the element: a change to the page's DOM
// would have every observer of it, the page's own too, look again.
function tintMark() {
  var style = /** @type {HTMLStyleElement | null} */ (document.getElementById(MARK));
  var rules = style && style.sheet ? style.sheet.cssRules : null;
  if (rules && rules.length) /** @type {CSSStyleRule} */ (rules[0]).style.setProperty('background-color', markFill());
}

function paintMarks() {
  if (!canMark()) return;
  /** @type {Range[]} */
  var ranges = pendingMark ? [pendingMark] : [];
  for (var i = 0; i < tracked.length; i++) {
    var hit = tracked[i].mark ? tracked[i].hit : null;
    if (hit && hit.range) ranges.push(hit.range);
  }
  if (!ranges.length) {
    CSS.highlights['delete'](MARK);
    return;
  }
  if (!document.getElementById(MARK)) {
    var style = document.createElement('style');
    style.id = MARK;
    style.textContent = '::highlight(' + MARK + '){background-color:' + markFill() + '}';
    headOf().appendChild(style);
  }
  var mark = new Highlight();
  for (var j = 0; j < ranges.length; j++) mark.add(ranges[j]);
  CSS.highlights.set(MARK, mark);
}

function clearNativeSelection() {
  var sel = window.getSelection ? window.getSelection() : null;
  if (sel && sel.rangeCount) sel.removeAllRanges();
}

// Words selected or clicked in comment mode, offered to the viewer to
// comment on at once (`compose`: no chip), with the press that made them,
// which the viewer pairs as it pairs a pick with its click. `select` puts
// clicked words in the selection, for a browser that cannot mark them.
/**
 * @param {Range | null} range
 * @param {number} press
 * @param {BridgePoint} release
 * @param {boolean} select
 */
function composeText(range, press, release, select) {
  /** @type {BridgeSelection | null} */
  var found = null;
  try {
    found = rangeAnchor(range, release);
  } catch (e) {
    found = null;
  }
  if (!found) return false;
  sendSpot({ type: 'page:selection', press: press, compose: true, anchor: found.anchor, rect: found.rect });
  if (canMark()) {
    pendingMark = trimmed(found.range);
    paintMarks();
    // After the release's own handling, which still reads the selection.
    window.setTimeout(clearNativeSelection, 0);
  } else if (select) {
    var sel = /** @type {Selection} */ (window.getSelection());
    sel.removeAllRanges();
    sel.addRange(/** @type {Range} */ (range));
  }
  return true;
}


// The pointer back over the page, once the viewer asks. What the viewer
// draws over the frame never hears the pointer leave it for the page (nor
// does the page hear it arrive), so a pin's preview would stay up; the
// viewer asks when one opens, and the next move here answers.
var pointerWatched = false;
window.addEventListener('pointermove', function () {
  if (!pointerWatched) return;
  pointerWatched = false;
  send({ type: 'page:pointer-in' });
}, { capture: true, passive: true });


