// @ts-check
// The comment half of the bridge, continued: one comment on several parts.
// In comment mode a click with Cmd (Ctrl elsewhere) held adds the element to
// the comment being written, or takes it out if it is already there, and a
// drag with it held adds what the area holds; the box stays open with its
// words. A touch screen has no modifier: the box's "Add a part" asks for the
// next tap to add. Shares one function with the parts before and after it.

/** @typedef {import('../../../../web/src/types/artifacts').ArtifactCommentAreaElement} AreaPart */

// ---- Adding: Cmd held, or the next tap ----

// Either way a click adds a part, so words are not offered and the cursor
// says it adds. The page owns both and the viewer hears each change: its box
// fades for a click through it while Cmd is held, and "Add a part" shows
// whether the next tap adds. Keys pressed in here never reach it.
var modHeld = false;
var extendNext = false;

/** @param {boolean} held @param {boolean} next @returns {boolean} whether it changed */
function setAdding(held, next) {
  if (held === modHeld && next === extendNext) return false;
  modHeld = held;
  extendNext = next;
  if (mode) send({ type: 'page:adding', held: held, next: next });
  return true;
}

function adding() {
  return modHeld || extendNext;
}

// Cmd as a pointer event over the page carries it, which is how the page
// learns of a key pressed with the focus outside it. What the pointer is
// over is then looked at again: on a part, a click now takes all of it.
/** @param {MouseEvent | PointerEvent} event */
function readModifier(event) {
  if (setAdding(event.metaKey || event.ctrlKey, extendNext)) hoverAgain();
}

/** @param {MouseEvent | PointerEvent} event */
function extending(event) {
  return mode && (event.metaKey || event.ctrlKey || extendNext);
}

// What a click under a still pointer takes changed: the word or the
// element, or the part around it.
function hoverAgain() {
  if (!pointer || pressing()) return;
  hoverAt(pointer.x, pointer.y);
  var el = pickable(commentable(document.elementFromPoint(pointer.x, pointer.y)));
  if (el !== hovered) {
    hovered = el;
    hoverLabel = el ? labelOf(el) : null;
  }
  reportHover();
}

/** @param {KeyboardEvent} event */
function modifierMoved(event) {
  if (!mode || (event.key !== 'Meta' && event.key !== 'Control')) return;
  var held = event.type === 'keydown';
  if (held === modHeld) return;
  setAdding(held, extendNext);
  hoverAgain();
}
window.addEventListener('keydown', modifierMoved, true);
window.addEventListener('keyup', modifierMoved, true);
window.addEventListener('blur', function () { setAdding(false, extendNext); });

listen('page:extend-next', function (data) {
  setAdding(modHeld, mode && data.on === true);
  hoverAgain();
});

// ---- The draft's parts ----

// The parts this page sent for the draft, in the order its anchor lists
// them (`ownListed`, which a plain drag keeps to its first few). They hold
// until the page sends a spot that starts a new comment (`sendSpot`) or the
// viewer says none is being written: found again from the anchor, a part
// could be another element with the same words, and the viewer's echo of
// the new draft lags a quick second click.
/** @type {Element[] | null} */
var ownParts = null;
/** @type {AreaPart[] | null} */
var ownListed = null;

/** @param {Element[] | null} els @param {AreaPart[] | null} listed */
function keepParts(els, listed) {
  ownParts = els;
  ownListed = listed;
}

// An area that lists every part it holds: parts picked one by one, or a
// drag around a few.
/** @param {BridgeAnchor} anchor */
function listsEvery(anchor) {
  var parts = anchor.elements;
  return anchor.kind === 'area' && Object.prototype.toString.call(parts) === '[object Array]' &&
    parts.length >= 2 && anchor.count === parts.length;
}

// The elements a spot is a list of parts of, in the order its anchor lists
// them: an element's comment is a list of one, an area that lists every
// part a list of those. A part the page no longer has is left out, so the
// rest can still be added to or taken out of. Null for words, and for a
// drag that lists only its first few.
/** @param {BridgeTracked} item @returns {Element[] | null} */
function trackedParts(item) {
  var hit = item.hit;
  if (!hit) return null;
  if (item.anchor.kind === 'element') return hit.el ? [hit.el] : null;
  return listsEvery(item.anchor) && hit.els && hit.els.length ? hit.els.slice() : null;
}

// Where each part of an area that lists every part shows now, which the
// viewer outlines one by one; null for any other spot.
/** @param {BridgeTracked} item */
function partRects(item) {
  var els = item.anchor.kind === 'area' ? trackedParts(item) : null;
  if (!els) return null;
  var rects = [];
  for (var i = 0; i < els.length; i++) {
    if (els[i].isConnected && els[i].getClientRects().length) rects.push(box(els[i].getBoundingClientRect()));
  }
  return rects.length ? rects : null;
}

/** @returns {BridgeTracked | null} */
function draftItem() {
  for (var i = 0; i < tracked.length; i++) {
    if (tracked[i].draft) return tracked[i];
  }
  return null;
}

// The draft's parts: what this page sent for it, else found again from its
// anchor. Empty when nothing is being written, or the draft is not a list
// of parts, so a modifier click there starts the parts afresh.
/** @returns {Element[]} */
function draftParts() {
  if (ownParts) {
    if (ownParts.every(function (el) { return el.isConnected; })) return ownParts.slice();
    keepParts(null, null);
  }
  if (stale) resolveAll();
  var draft = draftItem();
  return draft ? trackedParts(draft) || [] : [];
}

/** @param {AreaPart | undefined} a @param {AreaPart} b */
function samePart(a, b) {
  return !!a && typeof a === 'object' && a.path === b.path && a.quote === b.quote;
}

// The draft's parts with `part`, an entry its anchor lists, taken out. None
// when the draft lists no such part (a chip pressed on a list the page has
// since changed), so a press before the box catches up takes nothing out.
// This page's own parts line up with what it listed; a draft found again
// has each part looked for alone, and one the page lost goes with the chip,
// as on any change to the parts, since only what is here can be sent.
/** @param {AreaPart} part @returns {Element[]} */
function partsWithout(part) {
  if (ownParts && !ownParts.every(function (el) { return el.isConnected; })) keepParts(null, null);
  /** @type {Element[]} */
  var left = [];
  var draft = draftItem();
  var listed = ownParts ? ownListed : draft && listsEvery(draft.anchor) ? draft.anchor.elements : null;
  if (!listed || (ownParts && ownParts.length !== listed.length)) return left;
  var at = -1;
  for (var i = 0; i < listed.length && at === -1; i++) {
    if (samePart(listed[i], part)) at = i;
  }
  if (at === -1) return left;
  for (var j = 0; j < listed.length; j++) {
    if (j === at) continue;
    if (ownParts) {
      left.push(ownParts[j]);
      continue;
    }
    var entry = listed[j];
    var hit = entry && typeof entry === 'object' ? hitOf({ kind: 'element', path: entry.path, quote: entry.quote }, textIndex) : null;
    if (hit && hit.el) left.push(hit.el);
  }
  return left;
}

// The viewer has no comment being written: there are no parts to add to.
function draftGone() {
  keepParts(null, null);
  setAdding(modHeld, false);
}

// The parts with `added` joined, in document order. A part around others,
// or one a drag found inside another, takes their place: one comment does
// not hold a card and a line of it both. Past the limit the newest are the
// ones left out, never a part already picked.
/** @param {Element[]} base @param {Element[]} added */
function joined(base, added) {
  var out = base.slice();
  for (var i = 0; i < added.length; i++) {
    var el = added[i];
    if (out.indexOf(el) !== -1) continue;
    var rest = out.filter(function (p) { return !p.contains(el) && !el.contains(p); });
    if (rest.length >= MAX_PARTS) break;
    out = rest;
    out.push(el);
  }
  out.sort(function (a, b) {
    return a.compareDocumentPosition(b) & Node.DOCUMENT_POSITION_FOLLOWING ? -1 : 1;
  });
  return out;
}

// Where the pin of parts `els` stands: the click or drag that changed them
// when it was on one of them, else the first part's top-right corner. Left
// to the viewer, a pin goes to the corner of the box around every part,
// which on parts far apart is over something none of them is; a part just
// taken out is under the click that took it.
/** @param {Element[]} els @param {BridgePoint | null} point @returns {BridgePoint | null} */
function pinOnParts(els, point) {
  for (var i = 0; point && i < els.length; i++) {
    var r = els[i].getBoundingClientRect();
    if (point.clientX >= r.left && point.clientX <= r.right && point.clientY >= r.top && point.clientY <= r.bottom) return point;
  }
  if (els.length === 1) return null;
  var first = els[0].getBoundingClientRect();
  return { clientX: first.right, clientY: first.top };
}

// The draft's parts are now `els`: one is that element's comment, several an
// area listing each. Sent with `extend`, so the viewer keeps the box and its
// words on them rather than opening a new one. What a click under the
// pointer takes may have changed with them.
/** @param {Element[]} els @param {BridgePoint | null} release */
function sendParts(els, release) {
  if (!els.length) return;
  var point = pinOnParts(els, release);
  if (els.length === 1) {
    var one = elementAnchor(els[0], point);
    keepParts(els, null);
    sendSpot({ type: 'page:pick', extend: true, anchor: one.anchor, rect: one.rect });
  } else {
    var made = areaAnchor(els, point, MAX_PARTS);
    if (!made) return;
    keepParts(made.els, made.anchor.elements);
    sendSpot({ type: 'page:area', extend: true, anchor: made.anchor, rect: made.rect });
  }
  hoverAgain();
}

// While a click adds parts, the part of the draft `el` is in, if any: a
// click anywhere on a part takes that part out, so the outline shows all of
// it rather than the line under the pointer. The last part cannot go, so a
// click inside it narrows it to what was clicked, and that is outlined.
/** @param {Element | null} el @returns {Element | null} */
function pickable(el) {
  if (!el || !adding()) return el;
  var parts = draftParts();
  if (parts.length < 2) return el;
  for (var i = 0; i < parts.length; i++) {
    if (parts[i].contains(el)) return parts[i];
  }
  return el;
}

// A modifier click on `el`: the part it is on goes out of the parts, else
// `el` comes into them. The last part stays, since a comment is on
// something: a click inside it narrows it to `el` (`joined`), and one on it
// does nothing.
/** @param {Element} el @param {BridgePoint} point */
function togglePart(el, point) {
  var base = draftParts();
  var at = -1;
  for (var i = 0; i < base.length && at === -1; i++) {
    if (base[i].contains(el)) at = i;
  }
  if (at !== -1 && base.length > 1) {
    base.splice(at, 1);
    sendParts(base, point);
  } else if (base.indexOf(el) === -1) {
    sendParts(joined(base, [el]), point);
  }
}

// A chip's close button in the box: the part it names leaves the selection.
listen('page:drop-part', function (data) {
  var part = data.part;
  if (!part || typeof part !== 'object') return;
  sendParts(partsWithout({ path: part.path, quote: part.quote }), null);
});
