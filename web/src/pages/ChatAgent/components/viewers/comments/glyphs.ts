/**
 * Whether a point is over glyphs a reader could select, not just inside an
 * element that holds some: a press there selects words rather than drawing
 * an area. The caret the browser places at a point snaps there from blank
 * space too, so it counts only once the box of the character at it, or just
 * before it, holds the point.
 */

/** How far outside a character's box, in px, still counts as on it. */
const GLYPH_SLOP = 2;
/** Text in these is a control's or a drawing's, not words a reader selects. */
const NOT_TEXT = 'svg,button,input,textarea,select,option';

function caretAt(x: number, y: number): { node: Node; offset: number } | null {
  if (typeof document.caretPositionFromPoint === 'function') {
    const pos = document.caretPositionFromPoint(x, y);
    return pos?.offsetNode ? { node: pos.offsetNode, offset: pos.offset } : null;
  }
  const legacy = document as Document & { caretRangeFromPoint?: (x: number, y: number) => Range | null };
  const range = legacy.caretRangeFromPoint?.(x, y);
  return range ? { node: range.startContainer, offset: range.startOffset } : null;
}

export function glyphAt(x: number, y: number, within: Element): boolean {
  let caret: ReturnType<typeof caretAt> = null;
  try {
    caret = caretAt(x, y);
  } catch {
    // A point the browser cannot place a caret at is on no glyph.
  }
  const node = caret?.node;
  if (!(node instanceof Text) || !within.contains(node) || !/\S/.test(node.data)) return false;
  const parent = node.parentElement;
  if (!parent || parent.closest(NOT_TEXT) || getComputedStyle(parent).userSelect === 'none') return false;
  for (const at of [caret!.offset, caret!.offset - 1]) {
    if (at < 0 || at >= node.data.length) continue;
    const range = document.createRange();
    range.setStart(node, at);
    range.setEnd(node, Math.min(node.data.length, at + 1));
    for (const r of range.getClientRects()) {
      if (r.width > 0 && r.height > 0 && x >= r.left - GLYPH_SLOP && x <= r.right + GLYPH_SLOP
        && y >= r.top - GLYPH_SLOP && y <= r.bottom + GLYPH_SLOP) return true;
    }
  }
  return false;
}
