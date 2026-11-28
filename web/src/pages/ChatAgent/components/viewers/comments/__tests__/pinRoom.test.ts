import { afterEach, describe, expect, it, vi } from 'vitest';
import { PIN_GAP as GAP, blockPin, wordsPin } from '../pinRoom';

const SIZE = 22;
const VIEW = { left: 0, top: 0, right: 400, bottom: 600 };

type Box = { left: number; top: number; right: number; bottom: number };
const rect = ({ left, top, right, bottom }: Box) =>
  ({ left, top, right, bottom, x: left, y: top, width: right - left, height: bottom - top, toJSON: () => ({}) }) as DOMRect;

/**
 * A paragraph laid out by hand: each text node's line boxes, and the block's
 * own box. jsdom lays nothing out, so the rule is tested against these.
 */
function layout(lines: Box[][], block: Box) {
  const root = document.createElement('div');
  const p = document.createElement('p');
  const nodes = lines.map((_, i) => p.appendChild(document.createTextNode(`line ${i} `)));
  root.appendChild(p);
  document.body.appendChild(root);
  const boxes = new Map<Node, DOMRect[]>(nodes.map((n, i) => [n, lines[i].map(rect)]));
  // jsdom has no line boxes at all.
  Object.defineProperty(Range.prototype, 'getClientRects', {
    configurable: true,
    value(this: Range) {
      return (boxes.get(this.startContainer) ?? []) as unknown as DOMRectList;
    },
  });
  vi.spyOn(p, 'getBoundingClientRect').mockReturnValue(rect(block));
  return { root, p, nodes };
}

/** A selection whose line boxes are `boxes`, ending in `end`. */
const selection = (end: Node, boxes: Box[]) =>
  ({ getClientRects: () => boxes.map(rect), endContainer: end }) as unknown as Range;

const covers = (at: { x: number; y: number }, r: Box) =>
  r.left < at.x + SIZE && r.right > at.x && r.top < at.y && r.bottom > at.y - SIZE;

afterEach(() => {
  vi.restoreAllMocks();
  delete (Range.prototype as { getClientRects?: unknown }).getClientRects;
  document.body.innerHTML = '';
});

describe('wordsPin', () => {
  it('stands just past the end of the line the words end on, level with it, never on the words after them', () => {
    // Three lines; the words are the start of the second, which runs on to 300.
    const lines = [[{ left: 20, top: 0, right: 380, bottom: 24 }], [{ left: 20, top: 24, right: 300, bottom: 48 }], [{ left: 20, top: 48, right: 370, bottom: 72 }]];
    const { root, nodes } = layout(lines, { left: 20, top: 0, right: 380, bottom: 72 });
    const at = wordsPin(selection(nodes[1], [{ left: 20, top: 24, right: 90, bottom: 48 }]), root, VIEW);
    expect(at).toEqual({ x: 310, y: 47 });
    for (const line of lines.flat()) expect(covers(at!, line)).toBe(false);
  });

  it('reaches into the pane\'s padding past its block rather than onto words, as on a phone', () => {
    const lines = [[{ left: 16, top: 0, right: 360, bottom: 24 }]];
    const { root, nodes } = layout(lines, { left: 16, top: 0, right: 374, bottom: 24 });
    expect(wordsPin(selection(nodes[0], [{ left: 16, top: 0, right: 80, bottom: 24 }]), root, VIEW)).toEqual({ x: 370, y: 23 });
  });

  it('takes the margin beside the block where its line runs to the pane\'s edge', () => {
    const full = [[{ left: 40, top: 0, right: 395, bottom: 24 }]];
    const { root, nodes } = layout(full, { left: 40, top: 0, right: 395, bottom: 24 });
    const at = wordsPin(selection(nodes[0], [{ left: 300, top: 0, right: 395, bottom: 24 }]), root, VIEW);
    // No room after the line nor right of the block: the left margin.
    expect(at).toEqual({ x: 8, y: 23 });
    expect(covers(at!, full[0][0])).toBe(false);
  });

  it('stands far enough past the last letter that the open pin\'s rings keep about 6 px clear of it', () => {
    // The open pin's outer ring reaches 4.5 px past its edge (PinBadge.css).
    const lines = [[{ left: 20, top: 0, right: 200, bottom: 24 }]];
    const { root, nodes } = layout(lines, { left: 20, top: 0, right: 380, bottom: 24 });
    const at = wordsPin(selection(nodes[0], [{ left: 120, top: 0, right: 200, bottom: 24 }]), root, VIEW);
    expect(at!.x - 4.5 - 200).toBeGreaterThanOrEqual(5.5);
    expect(GAP).toBe(10);
  });

  it('keeps clear of a shorter line\'s neighbours by standing on its foot or hanging from its top', () => {
    // A 12px line between two long ones: centred, the pin would reach both.
    const lines = [[{ left: 20, top: 0, right: 390, bottom: 12 }], [{ left: 20, top: 12, right: 150, bottom: 24 }], [{ left: 20, top: 24, right: 200, bottom: 36 }]];
    const { root, nodes } = layout(lines, { left: 20, top: 0, right: 390, bottom: 36 });
    const at = wordsPin(selection(nodes[1], [{ left: 20, top: 12, right: 80, bottom: 24 }]), root, VIEW);
    for (const line of lines.flat()) expect(covers(at!, line)).toBe(false);
  });
});

describe('blockPin', () => {
  it('leaves a block\'s pin where it stands unless that covers the block\'s own text', () => {
    const lines = [[{ left: 20, top: 0, right: 120, bottom: 24 }]];
    const { p } = layout(lines, { left: 20, top: 0, right: 380, bottom: 24 });
    expect(blockPin([p], { left: 20, top: 0, right: 380, bottom: 24 }, { x: 356, y: 24 }, VIEW)).toBeNull();
    const moved = blockPin([p], { left: 20, top: 0, right: 380, bottom: 24 }, { x: 100, y: 24 }, VIEW);
    expect(moved).toEqual({ x: 130, y: 23 });
  });
});
