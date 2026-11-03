// @ts-check
// The document's end of the viewer's postMessage bridge, the part every
// document the viewer frames carries: the hello, the theme, the ground, the
// scroll and putting it back, settling, a section the viewer asks for, and
// which windows are heard at all. The server inlines it as the first script of each HTML file
// it frames for the app (page_bridge.py: a published version, the owner's live
// view, a file read in the panel or on a share), inside one function with the
// other parts that document gets, so it runs before any of the document's own
// code and nothing it declares is the document's. It speaks only to the frame's
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
// the page hears of it; what it reports once the document settles, before the
// viewer hears that it has; and what it reports once the viewer has named
// itself, since anything found earlier went nowhere.
/** @type {Array<(css: string) => void>} */
var onTheme = [];
/** @type {Array<() => void>} */
var onSettled = [];
/** @type {Array<() => void>} */
var onSpoke = [];

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
onSpoke.push(reportGround);

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

// A window the document has no tie to is not heard, by this bridge or the
// document's own listeners. Anyone holding this page's link can open it and
// reach its frames with a message, so a listener that writes what it hears into
// the page would run a stranger's script with the reach of this document's URL.
// This listener is the first the window gets, as the bridge runs first.
/** @param {MessageEventSource | null} from */
function isKin(from) {
  /** @type {any} */
  var at = from;
  try {
    for (var hops = 0; at && hops < 32; hops++) {
      if (at === window || at === viewer) return true;
      at = at.parent !== at ? at.parent : at.opener;
    }
  } catch (e) {
    // A browser withholding a property ends the walk: no tie found.
  }
  return false;
}

// A frame of this page's own whose file is the workspace's sends what it loaded
// (nested.js), which goes out with what this document loaded, and the page
// never hears it. Only a frame whose parent is this window: the viewer and a
// stranger never pass for one.
/** @param {MessageEventSource | null} from */
function isChildFrame(from) {
  try {
    return !!from && from !== window && /** @type {Window} */ (from).parent === window;
  } catch (e) {
    return false;
  }
}

window.addEventListener('message', function (event) {
  if (!isKin(event.source)) {
    event.stopImmediatePropagation();
    return;
  }
  var data = event.data;
  if (data && data.type === 'page:frame-loads' && isChildFrame(event.source)) {
    event.stopImmediatePropagation();
    if (Array.isArray(data.urls)) heardLoads(data.urls);
  }
}, true);

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
  if (first) for (i = 0; i < onSpoke.length; i++) onSpoke[i]();
});
