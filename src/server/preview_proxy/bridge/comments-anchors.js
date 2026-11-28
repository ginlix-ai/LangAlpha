// @ts-check
// The comment half of the bridge, continued: finding an anchor again, what is
// reported to the viewer, and the words being commented on. Shares one function
// with the parts before it.

// ---- Finding an anchor again ----

// How a quote is found again (`normal`, `indexText`, `findQuoteIn`) is the
// app's own code, compiled into `comments-shared.generated.js`.

var SKIPPED = /^(script|style|noscript|template|title)$/;

/** @param {Text} node */
function shownText(node) {
  var parent = /** @type {Element | null} */ (node.parentNode);
  return !(parent && parent.localName && SKIPPED.test(parent.localName));
}

// The body's text as `normal` reads it.
function textIndex() {
  return document.body ? indexText(document.body, shownText) : null;
}

// The occurrence of `quote` its stored neighbours fit best, as a range;
// only inside `within` when given. A page's text is anyone's, so a single
// character, found anywhere, is not looked for.
/**
 * @param {() => TextIndex | null} index
 * @param {string} quote
 * @param {unknown} prefix
 * @param {unknown} suffix
 * @param {Element | null} within
 */
function findText(index, quote, prefix, suffix, within) {
  return findQuoteIn(index, quote, prefix, suffix, { within: within, shortest: 2 });
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

// Whether an element could be the quoted one, edited: it keeps the
// quote's opening words, or its closing ones when the quote was stored
// whole. One that reads nothing like it is another element that came to
// stand at the same path, as the next block does when one is removed.
/** @param {Element} el @param {string} quote @param {boolean} whole */
function readsLike(el, quote, whole) {
  var q = normal(quote);
  var text = normal(wordsOf(el));
  var need = Math.min(8, Math.ceil(q.length / 2));
  return sharedHead(text, 0, text.length, q) >= need || (whole && sharedTail(text, 0, text.length, q) >= need);
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
  for (var i = 0; i < parts.length && i < MAX_PARTS; i++) {
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
    var words = searchedQuote(String(anchor.quote), MAX_SELECTION);
    var range = (el && findText(index, words, anchor.prefix, anchor.suffix, el)) ||
      findText(index, words, anchor.prefix, anchor.suffix, null);
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

// Where a hit is: its box as reported, rounded, and its edges as measured,
// which a kept pin moves with (`keptWordsPin`).
/**
 * @param {BridgeHit | null} hit
 * @returns {{ rect: BridgeBox, edges: BridgeEdges } | null | undefined} undefined: the node went with a re-render
 */
function spotOfHit(hit) {
  if (!hit) return null;
  if (hit.els) {
    /** @type {BridgeEdges | null} */
    var bounds = null;
    for (var i = 0; i < hit.els.length; i++) {
      if (!hit.els[i].isConnected) return undefined;
      if (hit.els[i].getClientRects().length) bounds = union(bounds, hit.els[i].getBoundingClientRect());
    }
    return bounds ? { rect: box(sized(bounds)), edges: bounds } : null;
  }
  if (hit.el) {
    if (!hit.el.isConnected) return undefined;
    if (!hit.el.getClientRects().length) return null;
    var own = hit.el.getBoundingClientRect();
    return { rect: box(own), edges: own };
  }
  var range = /** @type {Range} */ (hit.range);
  if (!range.startContainer.isConnected) return undefined;
  var r = range.getBoundingClientRect();
  return r.width || r.height ? { rect: box(r), edges: r } : null;
}

// ---- Where a pin goes ----

// The rule is the app's own (`wordsPin`, `blockPin`, `pinDefault`), compiled
// into `comments-shared.generated.js`; here it is given the page's root and
// viewport, and its answer rounded as every box the page reports is.

function pinView() {
  var root = document.documentElement;
  return { left: 0, top: 0, right: root.clientWidth || innerWidth, bottom: root.clientHeight || innerHeight };
}

/** @param {{ x: number, y: number }} at */
function roundedPoint(at) {
  return { x: round(at.x), y: round(at.y) };
}

/**
 * The pin on words, past the end of their last line, or null when they show nowhere.
 * @param {Range} range @returns {{ x: number, y: number } | null}
 */
function pinWords(range) {
  var at = wordsPin(range, document.body || document.documentElement, pinView());
  return at && roundedPoint(at);
}

/**
 * Where a found spot's pin goes, said for every spot so the viewer places
 * none itself, with what keeps it for the next report: `kept`, from the last,
 * while the spot has only scrolled, and the text `pass` measured.
 * @param {BridgeTracked} t @param {BridgeBox} rect @param {BridgeEdges} edges
 * @param {PinPass} pass @param {PinMemo | null} kept
 * @returns {{ at: { x: number, y: number }, memo: PinMemo | null }}
 */
function pinFor(t, rect, edges, pass, kept) {
  var hit = t.hit;
  var at = pinDefault(t.anchor, rect);
  var root = document.body || document.documentElement;
  if (hit && hit.range) {
    var words = keptWordsPin(hit.range, root, pinView(), edges, pass, kept);
    return { at: roundedPoint(words.at || at), memo: words.memo };
  }
  var spots = hit ? hit.els || (hit.el ? [hit.el] : []) : [];
  if (!spots.length) return { at: roundedPoint(at), memo: null };
  var bounds = { left: rect.x, top: rect.y, right: rect.x + rect.width, bottom: rect.y + rect.height };
  var placed = keptBlockPin(spots, bounds, at, pinView(), edges, pass, kept);
  return { at: roundedPoint(placed.at || at), memo: placed.memo };
}

// ---- Reporting ----

var mode = false;
/** @type {BridgeTracked[]} */
var tracked = [];
var stale = true;
var lastPositions = '';
// Each tracked anchor's pin as the last report placed it, by its place in
// `tracked`, dropped whenever anchors are found again (`resolveAll`).
/** @type {(PinMemo | null)[]} */
var pinsKept = [];
// The document's scroll size at the last report. A change is a resize,
// which can move text without a mutation, so no kept pin outlives it.
var pinLayout = '';
// The comment whose card is open: beside the draft, the only spot whose
// parts the viewer outlines, so the only other one they are measured for.
/** @type {string | null} */
var outlined = null;
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
  // Found again, maybe in another layout: every pin is placed afresh.
  pinsKept = [];
  stale = false;
  paintMarks();
}

function reportPositions() {
  if (stale) resolveAll();
  var root = document.documentElement;
  var layout = root.scrollWidth + ' ' + root.scrollHeight;
  if (layout !== pinLayout) {
    pinLayout = layout;
    pinsKept = [];
  }
  var pass = pinPass();
  var items = [];
  for (var i = 0; i < tracked.length; i++) {
    var found = spotOfHit(tracked[i].hit);
    if (found === undefined) {
      // The node went with a re-render; find it again next frame.
      stale = true;
      schedule();
      found = null;
    }
    var rect = found ? found.rect : null;
    var parts = rect && (tracked[i].draft || tracked[i].id === outlined) ? partRects(tracked[i]) : null;
    /** @type {BridgeSpotBox | null} */
    var spot = null;
    if (found) {
      var placed = pinFor(tracked[i], found.rect, found.edges, pass, pinsKept[i] || null);
      pinsKept[i] = placed.memo;
      spot = { x: found.rect.x, y: found.rect.y, width: found.rect.width, height: found.rect.height, pin: placed.at };
    }
    items.push(parts ? { id: tracked[i].id, rect: spot, parts: parts } : { id: tracked[i].id, rect: spot });
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
    message = { type: 'page:selection', anchor: selection.anchor, rect: spotBox(range.getBoundingClientRect(), pinWords(range)) };
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
  // A font that loads late lays the text out again without a mutation, and
  // often without a resize: kept pins are placed afresh.
  if (document.fonts && document.fonts.addEventListener) {
    document.fonts.addEventListener('loadingdone', function () {
      pinsKept = [];
      schedule();
    });
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
// Words that lost their mark fade on the exit beat the box over them leaves
// on, as the app's viewers fade theirs (`textMarks.ts`), under a highlight of
// their own so words still marked keep their fill.
var LEAVING = '__page_comment_leaving__';
/** @type {typeof import('../../../../web/src/lib/motion').DURATION.exit} */
var FADE_S = 0.14;
var MARK_STRENGTH = 32;
// The amber the app's accent is, for a theme that did not say.
var MARK_ACCENT = 'hsl(28 78% 60%)';
/** @type {Range | null} */
var pendingMark = null;
/** @type {Range[]} */
var shownMarks = [];
var fadeFrame = 0;

function canMark() {
  return !!(window.CSS && CSS.highlights && typeof window.Highlight === 'function');
}

/** @param {number} [percent] */
function markFill(percent) {
  return 'color-mix(in srgb, ' + (accent || MARK_ACCENT) + ' ' + (percent === undefined ? MARK_STRENGTH : percent) + '%, transparent)';
}

// Through the style sheet, not the element: a change to the page's DOM
// would have every observer of it, the page's own too, look again. The
// first rule fills marked words, the second fading ones.
/** @returns {CSSRuleList | null} */
function markRules() {
  var style = /** @type {HTMLStyleElement | null} */ (document.getElementById(MARK));
  if (!style) {
    style = document.createElement('style');
    style.id = MARK;
    style.textContent = '::highlight(' + MARK + '){background-color:' + markFill() + '}' +
      '::highlight(' + LEAVING + '){background-color:' + markFill() + '}';
    headOf().appendChild(style);
  }
  return style.sheet ? style.sheet.cssRules : null;
}

function tintMark() {
  var style = document.getElementById(MARK);
  var rules = style ? markRules() : null;
  if (rules && rules.length) /** @type {CSSStyleRule} */ (rules[0]).style.setProperty('background-color', markFill());
}

/** @param {Range[]} gone */
function fadeMarks(gone) {
  if (fadeFrame) window.cancelAnimationFrame(fadeFrame);
  fadeFrame = 0;
  var rules = markRules();
  var rule = rules && rules.length > 1 ? /** @type {CSSStyleRule} */ (rules[1]) : null;
  var still = window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  if (!rule || still) {
    CSS.highlights['delete'](LEAVING);
    return;
  }
  var leaving = new Highlight();
  for (var i = 0; i < gone.length; i++) leaving.add(gone[i]);
  CSS.highlights.set(LEAVING, leaving);
  var fading = rule;
  var start = -1;
  /** @param {number} now */
  var step = function (now) {
    if (start < 0) start = now;
    var t = Math.min(1, (now - start) / (FADE_S * 1000));
    // Eased out, as the app's exits are.
    fading.style.setProperty('background-color', markFill(MARK_STRENGTH * Math.pow(1 - t, 3)));
    if (t < 1) {
      fadeFrame = window.requestAnimationFrame(step);
      return;
    }
    fadeFrame = 0;
    CSS.highlights['delete'](LEAVING);
  };
  fading.style.setProperty('background-color', markFill());
  fadeFrame = window.requestAnimationFrame(step);
}

function paintMarks() {
  if (!canMark()) return;
  /** @type {Range[]} */
  var ranges = pendingMark ? [pendingMark] : [];
  for (var i = 0; i < tracked.length; i++) {
    var hit = tracked[i].mark ? tracked[i].hit : null;
    if (hit && hit.range) ranges.push(hit.range);
  }
  var gone = shownMarks.filter(function (was) {
    return !ranges.some(function (now) { return rangesOverlap(was, now); });
  });
  shownMarks = ranges;
  if (gone.length) fadeMarks(gone);
  if (!ranges.length) {
    CSS.highlights['delete'](MARK);
    return;
  }
  markRules();
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
 * @param {boolean} select
 */
function composeText(range, press, select) {
  /** @type {BridgeSelection | null} */
  var found = null;
  try {
    found = rangeAnchor(range);
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


