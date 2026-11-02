// @ts-check
// The document's end of the viewer's postMessage bridge, the part every
// bridged document carries: the hello, the theme, the ground, the scroll and
// putting it back, settling, links, and a section the viewer asks for. The
// server inlines it as the first script of each HTML file it frames for the
// app (page_bridge.py: a published version, the owner's live view, a file
// read in the panel or on a share), inside one function with the other parts
// that document gets, so it runs before any of the document's own code and
// nothing it declares is the document's. It speaks only to the frame's
// parent, in the page:* namespace, whose messages are typed in
// web/src/pages/ChatAgent/components/viewers/html/pageProtocol.ts.

/** @typedef {import('../../../../web/src/pages/ChatAgent/components/viewers/html/pageProtocol').ViewerToPage} ViewerToPage */
/** @typedef {import('../../../../web/src/pages/ChatAgent/components/viewers/html/pageProtocol').PageToViewerWire} PageToViewerWire */

var VERSION = 1;
var viewer = window.parent;

// The viewer's origin, learned from its first message. A page cannot know
// it in advance: the app can be served from any origin.
/** @type {string | null} */
var viewerOrigin = null;

function hello() {
  // '*' only because this message carries nothing the URL did not: it says
  // the bridge is listening and which file this document is, and the viewer
  // answers with its first message, whose origin every later message is
  // addressed to. The path tells the viewer whose comments these are after
  // a link in the document took the frame to another file.
  /** @type {PageToViewerWire} */
  var message = { type: 'page:ready', v: VERSION };
  var path = docPath();
  if (path) message.path = path;
  viewer.postMessage(message, '*');
}

// For every message after the hello. Dropped until the viewer has spoken,
// so nothing the page holds reaches a parent that never named itself.
/** @param {PageToViewerWire} message */
function send(message) {
  if (viewerOrigin === null) return;
  /** @type {{ v?: number }} */ (message).v = VERSION;
  viewer.postMessage(message, viewerOrigin);
}

/** @type {{ [type: string]: Array<(data: any) => void> }} */
var handlers = {};

/**
 * Answer one type of the viewer's messages. A type no part of this bridge
 * listens for is dropped unread: a read-only document never hears the
 * viewer's comment messages.
 * @template {ViewerToPage['type']} T
 * @param {T} type
 * @param {(data: Extract<ViewerToPage, { type: T }>) => void} handler
 */
function listen(type, handler) {
  (handlers[type] = handlers[type] || []).push(handler);
}

// What another part of the bridge does with a theme the viewer sent, before
// the page hears of it; and what it reports once the document settles,
// before the viewer hears that it has.
/** @type {Array<(css: string) => void>} */
var onTheme = [];
/** @type {Array<() => void>} */
var onSettled = [];

var THEME_STYLE_ID = '__page_theme__';

function headOf() {
  return document.head || document.documentElement;
}

// The app's CSS variables as one block of declarations, the shape the
// viewer's resolveThemeVars() produces. Applied as a :root rule ahead of
// the page's own styles, so a variable the page defines itself wins.
/** @param {string} css */
function applyTheme(css) {
  var style = document.getElementById(THEME_STYLE_ID);
  if (!style) {
    style = document.createElement('style');
    style.id = THEME_STYLE_ID;
    var head = headOf();
    head.insertBefore(style, head.firstChild);
  }
  style.textContent = ':root{\n' + css + '\n}';
  for (var i = 0; i < onTheme.length; i++) onTheme[i](css);
  announceTheme();
}

// Tells the page, once the block is in place, so a listener reading
// getComputedStyle sees the new values and can repaint what it drew from
// them: a canvas keeps the colours it was painted with. The first theme
// fires too, since it can land after the page's own script has run.
function announceTheme() {
  var event;
  try {
    event = new Event('themechange');
  } catch (e) {
    event = document.createEvent('Event');
    event.initEvent('themechange', false, false);
  }
  window.dispatchEvent(event);
}

/** @param {number} n */
function round(n) {
  return Math.round(n * 10) / 10;
}

// The longest workspace path a comment or a version can name.
/** @type {typeof import('../../../../web/src/pages/ChatAgent/components/viewers/html/pageProtocol').MAX_DOC_PATH} */
var MAX_DOC_PATH = 1024;

// The file this document is, workspace-relative: the part of the URL after
// a version's /v/<seq>/, or after the grant of the owner's live view
// (/wsfiles/g/<grant>/). The first such step is the prefix; a later one is
// part of the path. A path too long to be a file's is left out, not cut.
var DOC_PATH = /\/(?:v\/[0-9]+|wsfiles\/g\/[^\/]+)\/(.+)$/;

function docPath() {
  var match = DOC_PATH.exec(location.pathname);
  if (!match) return null;
  var path;
  try {
    path = decodeURIComponent(match[1]);
  } catch (e) {
    return null;
  }
  return path.length <= MAX_DOC_PATH ? path : null;
}

// ---- Scrolling, the ground, settling ----

// Where the window is scrolled and whether it is still moving, for the
// viewer's layer: once a frame while anything scrolls, then once when it
// stops. Capture, so a scroll inside one of the page's panes counts too.
var scrollFrame = 0;
var scrollStill = 0;
// Where the bridge's own jump back to the reader's place landed. That jump
// is one step, not a scroll in motion, so its event reports a stop, once a
// frame has been drawn there: the viewer may show the page on it.
/** @type {{ x: number, y: number } | null} */
var jumpedTo = null;

/** @param {() => void} fn */
function afterPaint(fn) {
  window.requestAnimationFrame(function () {
    window.requestAnimationFrame(fn);
  });
}

/** @param {boolean} moving */
function reportScroll(moving) {
  send({ type: 'page:scroll', x: round(window.scrollX), y: round(window.scrollY), moving: moving });
}

function onScroll() {
  var jump = jumpedTo;
  jumpedTo = null;
  if (jump && !scrollFrame && !scrollStill && round(window.scrollX) === jump.x && round(window.scrollY) === jump.y) {
    afterPaint(function () {
      reportScroll(false);
    });
    return;
  }
  if (!scrollFrame) {
    scrollFrame = window.requestAnimationFrame(function () {
      scrollFrame = 0;
      reportScroll(true);
    });
  }
  window.clearTimeout(scrollStill);
  scrollStill = window.setTimeout(function () {
    scrollStill = 0;
    // A frame still waiting would report movement after the stop.
    if (scrollFrame) window.cancelAnimationFrame(scrollFrame);
    scrollFrame = 0;
    reportScroll(false);
  }, 150);
}

document.addEventListener('scroll', onScroll, { capture: true, passive: true });

// Where the reader was on the page's previous version, applied again as
// the document grows: a page that renders after load is short at first,
// and an early scroll stops at its end. Any input from the reader ends it
// (not a scroll event, which the bridge's own scrollTo fires too), so
// their own scrolling is never undone.
/** @type {{ x: number, y: number } | null} */
var restoring = null;

function restoreScroll() {
  if (!restoring || !document.body) return;
  try {
    // Not 'auto': a page that sets scroll-behavior: smooth would glide
    // there from the top.
    window.scrollTo({ left: restoring.x, top: restoring.y, behavior: 'instant' });
  } catch (e) {
    window.scrollTo(restoring.x, restoring.y);
  }
  jumpedTo = { x: round(window.scrollX), y: round(window.scrollY) };
}

function readerTookOver() {
  restoring = null;
  jumpedTo = null;
}

// Before the comment-mode guards (the comment parts come after this one), which
// stop every listener after them.
var READER_INPUT = ['wheel', 'touchstart', 'keydown', 'pointerdown'];
for (var k = 0; k < READER_INPUT.length; k++) {
  window.addEventListener(READER_INPUT[k], readerTookOver, { capture: true, passive: true });
}

// The colour behind the page's content, for the viewer to paint around
// the frame: the root's background, else the body's, which the canvas
// takes when the root has none. Only rgb() goes out; anything else is null.
var RGB = /^rgba?\(\s*[0-9.]+%?(?:\s*,\s*|\s+)[0-9.]+%?(?:\s*,\s*|\s+)[0-9.]+%?\s*(?:[,\/]\s*([0-9.]+%?)\s*)?\)$/;
/** @type {string | null | undefined} */
var lastGround;

// '' for a background that shows what is behind it.
/** @param {Element | null} el */
function backgroundOf(el) {
  var color = el ? String(window.getComputedStyle(el).backgroundColor || '') : '';
  if (!color || color === 'transparent') return '';
  var match = RGB.exec(color);
  if (!match) return null;
  return match[1] && parseFloat(match[1]) === 0 ? '' : color;
}

function reportGround() {
  // Not marked as sent while send() would drop it.
  if (viewerOrigin === null) return;
  var color = backgroundOf(document.documentElement);
  if (color === '') color = backgroundOf(document.body);
  color = color || null;
  if (color === lastGround) return;
  lastGround = color;
  send({ type: 'page:ground', color: color });
}

// Settled: loaded, the page's requests back (requests.js), then 250 ms
// without a change, so an anchor still not found is missing rather than
// late. Sticky: a change after that is the page living, which positions
// follow. A page that never stops changing (an animation driven from
// script) settles this long after load regardless; the viewer, swapping
// documents, waits a little longer.
/** @type {typeof import('../../../../web/src/pages/ChatAgent/components/viewers/html/pageProtocol').SETTLE_CAP_MS} */
var SETTLE_CAP_MS = 5000;
var settled = false;
// Told to the viewer: after settling, once a frame is drawn.
var settledSaid = false;
var quietTimer = 0;
var quietCap = 0;
/** @type {MutationObserver | null} */
var quietWatch = null;

function settleSoon() {
  if (settled) return;
  window.clearTimeout(quietTimer);
  quietTimer = window.setTimeout(function () {
    quietTimer = 0;
    if (document.readyState === 'complete' && inFlight === 0) settleNow();
  }, 250);
}

function settleNow() {
  if (settled) return;
  settled = true;
  window.clearTimeout(quietTimer);
  window.clearTimeout(quietCap);
  if (quietWatch) quietWatch.disconnect();
  quietWatch = null;
  restoreScroll();
  restoring = null;
  reportGround();
  // Said once a frame has been drawn where the page was put back: the
  // viewer shows the document on this, and a frame still drawn at the old
  // offset would show it from the top for a moment. postMessage keeps
  // order, so what the other parts report first, the viewer has by then.
  afterPaint(function () {
    for (var i = 0; i < onSettled.length; i++) onSettled[i]();
    settledSaid = true;
    send({ type: 'page:settled' });
  });
}

if (window.MutationObserver && document.documentElement) {
  quietWatch = new MutationObserver(settleSoon);
  quietWatch.observe(document.documentElement, {
    childList: true, subtree: true, characterData: true, attributes: true,
  });
}

document.addEventListener('DOMContentLoaded', function () {
  restoreScroll();
  reportGround();
});

function onLoaded() {
  restoreScroll();
  reportGround();
  settleSoon();
  quietCap = window.setTimeout(settleNow, SETTLE_CAP_MS);
}

if (document.readyState === 'complete') onLoaded();
else window.addEventListener('load', onLoaded);

// ---- The viewer's messages ----

listen('page:theme', function (data) {
  if (typeof data.css !== 'string') return;
  applyTheme(data.css);
  reportGround();
});

listen('page:restore-scroll', function (data) {
  if (typeof data.x !== 'number' || typeof data.y !== 'number' || !isFinite(data.x) || !isFinite(data.y)) return;
  restoring = { x: data.x, y: data.y };
  restoreScroll();
  // Nothing left to grow into.
  if (settled) restoring = null;
});

// A section a reference points at. One the URL already names moves neither
// the frame's address nor its location, so the browser has nothing to
// navigate to and a second click would read as dead: the viewer asks the
// document instead. By id, then by name, the two things a fragment matches;
// no selector parsing, so any anchor text is safe.
listen('page:scroll-to', function (data) {
  if (!('fragment' in data) || typeof data.fragment !== 'string' || !data.fragment) return;
  var el = document.getElementById(data.fragment) || document.getElementsByName(data.fragment)[0];
  if (el && el.scrollIntoView) el.scrollIntoView();
});

window.addEventListener('message', function (event) {
  if (event.source !== viewer) return;
  var data = event.data;
  if (!data || typeof data.type !== 'string') return;
  if (!Object.prototype.hasOwnProperty.call(handlers, data.type)) return;
  // An opaque origin cannot be addressed; such a viewer gets the hello only.
  var first = viewerOrigin === null && event.origin !== 'null';
  if (first) viewerOrigin = event.origin;
  var answer = handlers[data.type];
  for (var i = 0; i < answer.length; i++) answer[i](data);
  // The ground found before the viewer spoke went nowhere.
  if (first) reportGround();
});

// ---- Links ----

// The view rides on its URL (?inject=), which a link in the document does
// not carry: followed as written, it would land on the next file with no
// bridge, unthemed, and a reader of the owner's live view could not comment
// on it. So a link that stays under this view's prefix (the owner's file
// grant, or a shared thread's serve route) gets this document's inject value
// before the browser follows it, its fragment and other parameters kept.
// Other origins and prefixes are left as written. Forms need nothing: the CSP
// has form-action 'none'. A script that sets location itself is not covered.
var LIVE_PREFIX = /^.*?\/(?:wsfiles\/g\/[^\/]+|api\/v1\/public\/shared\/[^\/]+\/files\/serve)\//.exec(location.pathname);
/** @type {string | null} */
var LIVE_MODE = null;
try {
  LIVE_MODE = new URLSearchParams(location.search).get('inject');
} catch (e) {
  LIVE_MODE = null;
}

/**
 * @param {Element} link
 * @param {RegExpExecArray} prefix
 * @param {string} mode
 */
function carryMode(link, prefix, mode) {
  var href = link.getAttribute('href');
  if (href === null) return;
  var url;
  try {
    url = new URL(href, document.baseURI);
  } catch (e) {
    return;
  }
  if (url.protocol !== location.protocol || url.host !== location.host) return;
  if (url.pathname.indexOf(prefix[0]) !== 0) return;
  // Already carried, a section of this same document included: rewritten,
  // the jump would reload the page.
  if (url.searchParams.get('inject') === mode) return;
  url.searchParams.set('inject', mode);
  link.setAttribute('href', url.href);
}

/**
 * The link a click is on, if any.
 * @param {Event} event
 */
function linkOf(event) {
  var target = /** @type {Element | null} */ (event.target);
  return target && target.closest ? target.closest('a[href], area[href]') : null;
}

if (LIVE_PREFIX && LIVE_MODE) {
  // Capturing, so the address is set before any handler, the page's own
  // included, reads it or follows it.
  window.addEventListener('click', function (event) {
    var link = linkOf(event);
    if (link && LIVE_PREFIX && LIVE_MODE) carryMode(link, LIVE_PREFIX, LIVE_MODE);
  }, true);
}

// A link to another site opens in a tab of its own: the viewer lets a
// page's frame load only the page's own origin and the mail and phone
// hand-offs (web/src/lib/framePolicy.ts), so followed in place it would be a
// dead click. A link within the page, a section of it included, and any
// other scheme are the browser's. On window and bubbling, so the page's own
// handlers hear the click first and one that took it has said so.
window.addEventListener('click', function (event) {
  if (event.defaultPrevented || event.button !== 0) return;
  var link = /** @type {HTMLAnchorElement | HTMLAreaElement | null} */ (linkOf(event));
  var href = link ? link.href : null;
  if (!link || typeof href !== 'string' || !/^https?:/i.test(href)) return;
  if (link.host === location.host) return;
  event.preventDefault();
  window.open(href, '_blank', 'noopener,noreferrer');
});
