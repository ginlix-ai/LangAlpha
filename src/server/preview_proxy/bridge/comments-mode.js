// @ts-check
// The comment half of the bridge, last part: comment mode, its drags and
// selections, and the viewer's messages. Shares one function with the parts
// before it.

// ---- Comment mode ----

// The mode's cursor, which the one rule of its style sheet sets: the
// crosshair, or the I-beam over glyphs. Links and images do not drag in
// the mode, so a press on a link's words selects them as any others.
var cursorShown = '';

/** @param {unknown} on */
function setMode(on) {
  mode = !!on;
  var style = document.getElementById(MODE_STYLE_ID);
  if (mode && !style) {
    style = document.createElement('style');
    style.id = MODE_STYLE_ID;
    style.textContent = ':not([data-uncommentable]):not([data-uncommentable] *)' +
      '{cursor:crosshair!important;-webkit-user-drag:none!important}';
    headOf().appendChild(style);
    cursorShown = 'crosshair';
  } else if (!mode && style) {
    /** @type {Node} */ (style.parentNode).removeChild(style);
    cursorShown = '';
  }
  if (mode) {
    watch();
  } else {
    endDrag();
    holdHover(null);
    hovered = null;
    hoverText = null;
    pointer = null;
    reportHover();
  }
}

// Through the style sheet, not the element: the cursor changes on most
// moves over text, and each change to the page's DOM would have every
// observer of it, the page's own too, look again.
/** @param {string} name */
function showCursor(name) {
  if (name === cursorShown) return;
  var style = /** @type {HTMLStyleElement | null} */ (document.getElementById(MODE_STYLE_ID));
  var rules = style && style.sheet ? style.sheet.cssRules : null;
  if (!rules || !rules.length) return;
  /** @type {CSSStyleRule} */ (rules[0]).style.setProperty('cursor', name, 'important');
  cursorShown = name;
}

// What the pointer is over: glyphs, with the I-beam and the word's box,
// or not, with the crosshair and the outline of the element pointerover
// found.
/** @param {number} x @param {number} y */
function hoverAt(x, y) {
  var glyph = glyphAt(x, y);
  showCursor(glyph ? 'text' : 'crosshair');
  hoverText = glyph ? { word: wordAt(glyph, x), y: y } : null;
}

// A press under way that the hover stands aside for: an area, or words
// being selected.
function pressing() {
  return !!drag && (drag.on || drag.selecting || drag.dropped);
}

// Crossing the few pixels between two cards puts their shared container
// under the pointer for a frame, and the ring would flash around the whole
// grid. A move out to an ancestor waits 60 ms for the next card first.
var hoverHold = 0;

/** @param {Element | null} el */
function holdHover(el) {
  window.clearTimeout(hoverHold);
  hoverHold = 0;
  if (!el) return;
  hoverHold = window.setTimeout(function () {
    hoverHold = 0;
    hovered = el;
    hoverLabel = labelOf(el);
    reportHover();
  }, 60);
}

// A press on the page, in either mode, first closes whatever the viewer
// has open over it. Each press is numbered, and the pick its own click
// reports carries the number, so the viewer passes over that pick alone:
// a key's click, or the next press's, still picks. A touch counts once it
// lifts as a tap: one that scrolls the page is cancelled first, and
// leaves the box where it is.
var presses = 0;
var tapping = false;

function press() {
  presses++;
  send({ type: 'page:press', press: presses });
}

// ---- Drags in comment mode: an area, or words ----

// In comment mode a primary press that travels this far, in px, is a
// drag rather than a click. One that began on glyphs selects words, as
// anywhere else, and its release opens a comment on them; one that began
// off them draws an area, the comment's spot. A double click selects
// nothing in the mode. Touch keeps its drag for scrolling, so both are
// the pointer's.
var DRAG_START = 6;
// The press under way: where it went down, in document coordinates so a
// wheel turned mid-drag grows the area, where the pointer is now, and
// whether it began on glyphs (`text`, `glyph`).
/** @type {BridgeDrag | null} */
var drag = null;
var dragFrame = 0;
// The press whose click a drag ate: the browser still fires one at the
// release, and it would pick the element the drag ended on.
var swallowed = 0;
// The last press that was a click on glyphs, for the click that follows.
/** @type {{ press: number, glyph: BridgeGlyph | null } | null} */
var textPress = null;

// What a click on glyphs takes: 'word', the word under the pointer, as a
// text comment on it, which the hover's word box promises (claude.ai's
// comment mode does this); or 'element', the element, as a click off
// glyphs does.
var TEXT_CLICK = 'word';

/** @param {number} n @param {number} low @param {number} high */
function clamp(n, low, high) {
  return Math.min(high, Math.max(low, n));
}

function dragArea() {
  var d = /** @type {BridgeDrag} */ (drag);
  var x0 = d.x - window.scrollX;
  var y0 = d.y - window.scrollY;
  // The pointer may have left the frame; the area stops at its edge.
  var x1 = clamp(d.cx, 0, window.innerWidth);
  var y1 = clamp(d.cy, 0, window.innerHeight);
  return {
    left: Math.min(x0, x1), top: Math.min(y0, y1),
    right: Math.max(x0, x1), bottom: Math.max(y0, y1),
  };
}

// No text is selected while an area is drawn: the browser began selecting
// at the press, before it could be told this is a drag.
/** @param {boolean} on */
function holdSelection(on) {
  var style = document.getElementById(DRAG_STYLE_ID);
  if (on && !style) {
    style = document.createElement('style');
    style.id = DRAG_STYLE_ID;
    style.textContent = '*{-webkit-user-select:none!important;user-select:none!important}';
    headOf().appendChild(style);
  } else if (!on && style) {
    /** @type {Node} */ (style.parentNode).removeChild(style);
  }
  var sel = on && window.getSelection ? window.getSelection() : null;
  if (sel && !sel.isCollapsed) sel.removeAllRanges();
}

function reportDrag() {
  dragFrame = 0;
  if (!drag || !drag.on) return;
  holdSelection(true);
  send({ type: 'page:area-drag', press: drag.press, rect: box(sized(dragArea())) });
}

function reportDragSoon() {
  if (!dragFrame) dragFrame = window.requestAnimationFrame(reportDrag);
}

function beginDrag() {
  var d = /** @type {BridgeDrag} */ (drag);
  d.on = true;
  holdSelection(true);
  holdHover(null);
  hovered = null;
  hoverText = null;
  reportHover();
  // The pointer's moves keep coming here once it leaves the frame.
  try {
    document.documentElement.setPointerCapture(d.id);
  } catch (e) {
    // Captured or not, the release still ends the drag.
  }
}

// A press on glyphs that travels is the reader selecting words, which the
// browser does as it would anywhere: nothing is cancelled, and the
// outline steps aside until the release.
function beginSelecting() {
  /** @type {BridgeDrag} */ (drag).selecting = true;
  holdHover(null);
  hovered = null;
  hoverText = null;
  reportHover();
  showCursor('text');
}

// Esc while selecting: the words go, and the rest of the press selects
// nothing more, nor opens anything at its release.
function dropSelecting() {
  var d = /** @type {BridgeDrag} */ (drag);
  d.selecting = false;
  d.dropped = true;
  holdSelection(true);
}

// Ends the drag under way, cleared from the viewer, and returns it.
/** @returns {BridgeDrag | null} */
function endDrag() {
  var was = drag;
  drag = null;
  if (dragFrame) window.cancelAnimationFrame(dragFrame);
  dragFrame = 0;
  if (was && was.dropped) holdSelection(false);
  if (!was || !was.on) return was;
  holdSelection(false);
  try {
    document.documentElement.releasePointerCapture(was.id);
  } catch (e) {
    // Released already, with the pointer.
  }
  send({ type: 'page:area-drag', clear: true });
  return was;
}

// What the released area is about. Two elements or more are an area; one,
// or none, is that element's comment as a click makes it: the one the
// area holds, else the one the drag began on, else the one most of the
// area covers. A lasso around one tile is that tile's comment. Either
// way the pin stands where the drag was let go, the nearest it can on
// what was taken.
/** @param {BridgeDrag} was */
function reportArea(was) {
  var area = dragArea();
  var release = { clientX: clamp(was.cx, 0, window.innerWidth), clientY: clamp(was.cy, 0, window.innerHeight) };
  var found = areaElements(area);
  var made = found.els.length > 1 ? areaAnchor(found.els, release) : null;
  if (made) {
    sendSpot({ type: 'page:area', press: was.press, anchor: made.anchor, rect: made.rect });
    return;
  }
  var el = found.els.length === 1 ? found.els[0] : null;
  if (!el) {
    var under = document.elementFromPoint(was.x - window.scrollX, was.y - window.scrollY);
    el = (under && commentable(under)) || (found.most && commentable(found.most));
  }
  if (!el) return;
  var picked = elementAnchor(el, release);
  sendSpot({ type: 'page:pick', press: was.press, anchor: picked.anchor, rect: picked.rect });
}

// The second and later press of a multi-click belong to the first: they
// close nothing, pick nothing and start no area. Only mousedown carries
// the click count (a pointerdown's detail is 0), and it follows its
// pointerdown in the same task, so the press waits for it. A pointerdown
// the page cancelled gets no mousedown, so a timer settles it alone.
/** @type {BridgeHeld | null} */
var held = null;
var heldTimer = 0;

/** @param {boolean} continuation */
function releaseHeld(continuation) {
  var h = held;
  held = null;
  window.clearTimeout(heldTimer);
  if (!h || continuation) return;
  press();
  endDrag();
  textPress = null;
  if (mode && !refused(h.target)) {
    var glyph = glyphAt(h.cx, h.cy);
    drag = {
      id: h.id, press: presses, on: false, moved: false, text: !!glyph, glyph: glyph,
      x: h.cx + window.scrollX, y: h.cy + window.scrollY,
      cx: h.cx, cy: h.cy,
    };
    // Words pressed on begin a selection of their own, never a drag of the
    // words selected before, and the mark of words not yet answered for
    // goes: the viewer has had its chance.
    if (glyph) clearNativeSelection();
    if (pendingMark) {
      pendingMark = null;
      paintMarks();
    }
  }
}

// Registered before the guards below, which stop everything after them.
window.addEventListener('pointerdown', function (event) {
  pointerDown = true;
  if (event.button !== 0) return;
  if (event.pointerType === 'touch') {
    tapping = true;
    return;
  }
  releaseHeld(false);
  held = { id: event.pointerId, target: event.target, cx: event.clientX, cy: event.clientY };
  heldTimer = window.setTimeout(function () { releaseHeld(false); }, 0);
}, true);
window.addEventListener('mousedown', function (event) {
  if (event.button !== 0) return;
  releaseHeld(event.detail > 1);
  // A multi-click selects nothing in comment mode: its default is a word
  // or line selection, which the press's own click has already taken.
  if (event.detail > 1 && mode && !refused(event.target)) event.preventDefault();
}, true);
window.addEventListener('pointermove', function (event) {
  if (!drag || event.pointerId !== drag.id || drag.dropped) return;
  drag.cx = event.clientX;
  drag.cy = event.clientY;
  if (drag.selecting) return;
  if (!drag.on) {
    var dx = event.clientX + window.scrollX - drag.x;
    var dy = event.clientY + window.scrollY - drag.y;
    if (dx || dy) drag.moved = true;
    if (dx * dx + dy * dy < DRAG_START * DRAG_START) return;
    if (drag.text) {
      beginSelecting();
      return;
    }
    beginDrag();
  }
  reportDragSoon();
}, { capture: true, passive: true });

// The I-beam and the word box follow the pointer, once a frame.
var hoverFrame = 0;

function hoverNow() {
  hoverFrame = 0;
  if (!mode || !pointer || pressing()) return;
  hoverAt(pointer.x, pointer.y);
  reportHover();
}

/** @param {PointerEvent} event */
function hoverSoon(event) {
  pointer = { x: event.clientX, y: event.clientY };
  if (!hoverFrame) hoverFrame = window.requestAnimationFrame(hoverNow);
}

window.addEventListener('pointermove', function (event) {
  if (mode && event.pointerType !== 'touch') hoverSoon(event);
}, { capture: true, passive: true });
window.addEventListener('pointerup', function (event) {
  pointerDown = false;
  if (tapping) {
    tapping = false;
    press();
  }
  if (drag && event.pointerId === drag.id) {
    // The area before the clear, so the viewer's outline goes straight
    // from the rectangle to what it took.
    if (drag.on) {
      drag.cx = event.clientX;
      drag.cy = event.clientY;
      swallowed = drag.press;
      try {
        reportArea(drag);
      } finally {
        endDrag();
      }
    } else if (drag.selecting || drag.dropped) {
      // Words selected: their comment opens, its pin where the pointer let go.
      swallowed = drag.press;
      var was = /** @type {BridgeDrag} */ (endDrag());
      if (was.selecting) composeText(selectedRange(), was.press, event, false);
      // Called off: Chrome keeps the press's selection under the hold, and
      // shows it again once the hold lifts, after this release.
      else window.setTimeout(clearNativeSelection, 0);
    } else {
      // A press that moved a little is still a click: what the browser
      // selected on the way is not the reader's. One that did not move (a
      // double click) keeps its word.
      var sel = window.getSelection ? window.getSelection() : null;
      if (sel && !sel.isCollapsed && drag.moved) sel.removeAllRanges();
      if (drag.text) textPress = { press: drag.press, glyph: drag.glyph };
      endDrag();
    }
  }
  if (mode && event.pointerType !== 'touch') hoverSoon(event);
  checkSelectionSoon();
}, true);
window.addEventListener('pointercancel', function () {
  pointerDown = false;
  tapping = false;
  endDrag();
}, true);
// A link or an image would carry the drag off as the browser's own
// drag-and-drop, and the area would never be released.
window.addEventListener('dragstart', function (event) {
  if (mode && drag) event.preventDefault();
}, true);
// A wheel turned mid-drag moves the page under the area's far corner.
document.addEventListener('scroll', function () {
  if (drag && drag.on) reportDragSoon();
}, { capture: true, passive: true });

// In comment mode a press is the reader's, not the page's: nothing the page
// listens for hears it, and a drag selects words or draws an area.
// A refused element keeps working, so a page can leave its own controls
// (a tab strip, a slide picker) usable while comments are being placed.
/** @param {Event} event */
function guard(event) {
  if (!mode || refused(event.target)) return;
  event.stopPropagation();
  event.stopImmediatePropagation();
}
var GUARDED = ['pointerdown', 'pointerup', 'mousedown', 'mouseup', 'touchstart', 'touchend', 'dblclick', 'auxclick', 'contextmenu'];
for (var g = 0; g < GUARDED.length; g++) window.addEventListener(GUARDED[g], guard, true);

window.addEventListener('click', function (event) {
  if (!mode || refused(event.target)) return;
  event.preventDefault();
  event.stopPropagation();
  event.stopImmediatePropagation();
  // A drag reported its area, or was called off, at the release.
  if (event.detail > 0 && swallowed && swallowed === presses) return;
  // The later clicks of a multi-click pick nothing: the first one did.
  if (event.detail > 1) return;
  // A double click that selected words is a selection, reported as one.
  if (event.button !== 0 || selectedRange()) return;
  if (event.detail > 0 && textPress && textPress.press === presses && clickText(event)) return;
  var el = commentable(event.target);
  if (!el) return;
  var picked = elementAnchor(el, event);
  /** @type {PageToViewerWire} */
  var message = { type: 'page:pick', anchor: picked.anchor, rect: picked.rect };
  // A click the pointer made ends the latest press; a key's counts none.
  if (event.detail > 0 && presses) message.press = presses;
  sendSpot(message);
}, true);

// A click on glyphs, by TEXT_CLICK: whether it made a text comment's
// words, its pin where it was clicked, rather than picking the element.
/** @param {MouseEvent} event */
function clickText(event) {
  if (TEXT_CLICK !== 'word') return false;
  var glyph = glyphAt(event.clientX, event.clientY) || (textPress && textPress.glyph);
  var word = glyph && glyph.node.isConnected ? wordAt(glyph, event.clientX) : null;
  return !!word && composeText(word, presses, event, true);
}

window.addEventListener('pointerover', function (event) {
  holdHover(null);
  if (!mode || event.pointerType === 'touch' || pressing()) return;
  // Glyphs first, so a move onto words never flashes the element's outline.
  pointer = { x: event.clientX, y: event.clientY };
  hoverAt(pointer.x, pointer.y);
  var el = commentable(event.target);
  if (el === hovered) {
    reportHover();
    return;
  }
  if (el && hovered && el.contains(hovered)) {
    holdHover(el);
    if (hoverText) reportHover();
    return;
  }
  hovered = el;
  hoverLabel = el ? labelOf(el) : null;
  reportHover();
}, true);

document.addEventListener('mouseout', function (event) {
  if (!mode || event.relatedTarget) return;
  holdHover(null);
  hovered = null;
  hoverText = null;
  pointer = null;
  reportHover();
});

// In comment mode the page has handed the reader's keys to the viewer, so
// Esc (and C, as in the viewer) leave the mode from inside the frame too.
window.addEventListener('keydown', function (event) {
  if (!mode || event.metaKey || event.ctrlKey || event.altKey) return;
  var key = event.key;
  var isC = key === 'c' || key === 'C';
  if (key !== 'Escape' && !isC) return;
  if (isC && isTextEntry(document.activeElement)) return;
  event.preventDefault();
  event.stopPropagation();
  event.stopImmediatePropagation();
  // Esc mid-drag calls the area or the selection off, and only that: the
  // release that follows picks nothing.
  if (pressing()) {
    var d = /** @type {BridgeDrag} */ (drag);
    if (key === 'Escape' && !d.dropped) {
      swallowed = d.press;
      if (d.on) endDrag();
      else dropSelecting();
    }
    return;
  }
  send({ type: 'page:key', key: isC ? 'c' : 'Escape' });
}, true);

// Outside the mode the page keeps its keys. A C it left alone, typed
// anywhere but a field, turns the mode on, as it does in the viewer.
window.addEventListener('keydown', function (event) {
  if (mode || event.defaultPrevented || event.repeat || event.isComposing) return;
  if (event.metaKey || event.ctrlKey || event.altKey) return;
  if ((event.key !== 'c' && event.key !== 'C') || isTextEntry(document.activeElement)) return;
  send({ type: 'page:key', key: 'c' });
});

// ---- Selections, with or without the mode ----

var selectionTimer = 0;
function checkSelectionSoon() {
  window.clearTimeout(selectionTimer);
  selectionTimer = window.setTimeout(checkSelection, 120);
}

function checkSelection() {
  // Still dragging: the release checks again, so the offer does not chase
  // the pointer.
  if (pointerDown) return;
  /** @type {BridgeSelection | null} */
  var found = null;
  try {
    found = selectionAnchor();
  } catch (e) {
    found = null;
  }
  selection = found;
  if (found) watch();
  reportSelection();
}

// A selection is offered as a comment's words.
document.addEventListener('selectionchange', checkSelectionSoon);

// Positions first, fresh, before the viewer hears the document settled.
onSettled.push(function () {
  if (!tracked.length) return;
  stale = true;
  lastPositions = '';
  reportPositions();
});

// ---- The viewer's messages ----

listen('page:comment-mode', function (data) {
  setMode(data.on === true);
});

listen('page:anchors', function (data) {
  var items = Object.prototype.toString.call(data.items) === '[object Array]' ? data.items : [];
  tracked = [];
  for (var i = 0; i < items.length && tracked.length < MAX_ANCHORS; i++) {
    var item = items[i];
    if (item && typeof item.id === 'string' && item.anchor && typeof item.anchor === 'object') {
      var marked = item.mark === true;
      tracked.push({ id: item.id, anchor: item.anchor, hit: null, mark: marked });
      // The viewer has opened the words' box, whose anchor marks them now.
      if (marked) pendingMark = null;
    }
  }
  stale = true;
  lastPositions = '';
  watch();
  reportPositions();
  // The viewer may first speak after the document settled, when the
  // page:settled it was sent went nowhere.
  if (settledSaid) send({ type: 'page:settled' });
});

// A comment's spot, by its id; a section by its fragment is core.js's.
listen('page:scroll-to', function (data) {
  if (!('id' in data) || typeof data.id !== 'string') return;
  if (stale) resolveAll();
  for (var i = 0; i < tracked.length; i++) {
    if (tracked[i].id !== data.id || !tracked[i].hit) continue;
    var hit = /** @type {BridgeHit} */ (tracked[i].hit);
    var target = hit.el || (hit.els && hit.els[0]) || elementOf(/** @type {Range} */ (hit.range).startContainer);
    if (target && target.scrollIntoView) {
      var still = window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches;
      target.scrollIntoView({ block: 'center', inline: 'nearest', behavior: still ? 'auto' : 'smooth' });
    }
    return;
  }
});

listen('page:pointer-watch', function () {
  pointerWatched = true;
});

listen('page:clear-selection', function () {
  var sel = window.getSelection ? window.getSelection() : null;
  if (sel) sel.removeAllRanges();
  if (pendingMark) {
    pendingMark = null;
    paintMarks();
  }
});
