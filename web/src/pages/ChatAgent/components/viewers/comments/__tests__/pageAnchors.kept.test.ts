/**
 * A report on a scroll frame places each pin where a fresh look would, but
 * measures no text for a spot that has only scrolled since the last report.
 */
import { afterEach, describe, expect, it } from 'vitest';
import type { PageToViewerWire, SpotRect } from '../../html/pageProtocol';
import { PageAnchors } from '../pageAnchors';
import type { SpotStrategy } from '../spotStrategy';

type Box = { left: number; top: number; right: number; bottom: number };
const rect = ({ left, top, right, bottom }: Box) =>
  ({ left, top, right, bottom, x: left, y: top, width: right - left, height: bottom - top, toJSON: () => ({}) }) as DOMRect;

// The viewport at (10, 20), 500 wide; boxes below are where the content lays
// them out unscrolled, in client coordinates.
const VIEW = { left: 10, top: 20, width: 500, height: 300 };
const LINE = { left: 30, top: 100, right: 330, bottom: 124 };
const WORDS = { left: 30, top: 100, right: 90, bottom: 124 };
// A block whose text runs under its top-right corner, so its pin moves off it.
const BLOCK_TEXT = { left: 30, top: 160, right: 380, bottom: 184 };
const BLOCK = { left: 30, top: 160, right: 400, bottom: 184 };

const original = Object.getOwnPropertyDescriptors(Range.prototype);

afterEach(() => {
  for (const name of ['getClientRects', 'getBoundingClientRect'] as const) {
    if (original[name]) Object.defineProperty(Range.prototype, name, original[name]);
    else delete (Range.prototype as Partial<Range>)[name];
  }
  document.body.replaceChildren();
});

/** A viewer page scrolled `scrolled` px down, with a comment on words and one on a block. */
function scene(scrolled: number) {
  let y = scrolled;
  const at = (b: Box) => rect({ left: b.left, top: b.top - y, right: b.right, bottom: b.bottom - y });
  const scroller = document.createElement('div');
  const p = document.createElement('p');
  p.textContent = 'Revenue rose to a record this quarter';
  const block = document.createElement('p');
  block.textContent = 'Gross margin held at seventy two percent';
  scroller.append(p, block);
  document.body.append(scroller);
  scroller.getBoundingClientRect = () => rect({ left: VIEW.left, top: VIEW.top, right: VIEW.left + VIEW.width, bottom: VIEW.top + VIEW.height });
  Object.defineProperty(scroller, 'clientWidth', { value: VIEW.width });
  Object.defineProperty(scroller, 'clientHeight', { value: VIEW.height });
  p.getBoundingClientRect = () => at(LINE);
  block.getBoundingClientRect = () => at(BLOCK);
  const words = document.createRange();
  words.setStart(p.firstChild!, 0);
  words.setEnd(p.firstChild!, 7);

  let measured = 0;
  Object.defineProperty(Range.prototype, 'getClientRects', {
    configurable: true,
    value(this: Range) {
      measured += 1;
      if (this === words) return [at(WORDS)];
      if (this.startContainer === p.firstChild) return [at(LINE)];
      if (this.startContainer === block.firstChild) return [at(BLOCK_TEXT)];
      return [];
    },
  });
  Object.defineProperty(Range.prototype, 'getBoundingClientRect', {
    configurable: true,
    value(this: Range) {
      return this === words ? at(WORDS) : rect({ left: 0, top: 0, right: 0, bottom: 0 });
    },
  });

  const strategy: SpotStrategy = {
    find: (anchor) => (anchor.kind === 'text' ? { range: words } : { parts: [[block]] }),
    spots: {
      spotAt: () => null, spotsIn: () => [], labelOf: () => null, anchorOf: () => null,
      rectsOf: (spot) => [spot.getBoundingClientRect()],
    },
  };
  const sent: PageToViewerWire[] = [];
  const anchors = new PageAnchors(scroller, strategy, (wire) => sent.push(wire));
  anchors.set([
    { id: 'words', anchor: { kind: 'text', quote: 'Revenue' } },
    { id: 'block', anchor: { kind: 'block', lines: '2', quote: 'Gross margin' } },
  ]);

  /** The pins the next report says, and how many line boxes it measured for them. */
  const report = () => {
    measured = 0;
    sent.length = 0;
    anchors.report();
    const positions = sent.find((m) => m.type === 'page:positions') as { items: { id: string; rect: SpotRect | null }[] } | undefined;
    return { pins: positions?.items.map((item) => item.rect?.pin), measured };
  };
  return { report, anchors, scroll: (to: number) => (y = to) };
}

describe('pins on a scroll frame', () => {
  it('measures no text for spots that only scrolled, and moves each pin by exactly the scroll', () => {
    const page = scene(0);
    const first = page.report();
    expect(first.measured).toBeGreaterThan(0);
    // The block's pin moved off its corner, past the end of its text.
    expect(first.pins![1]).toEqual({ x: 380 - VIEW.left + 10, y: expect.any(Number) });
    page.scroll(40);
    const second = page.report();
    expect(second.measured).toBe(0);
    expect(second.pins).toEqual(first.pins!.map((pin) => ({ x: pin!.x, y: pin!.y - 40 })));
    expect(second.pins).toEqual(scene(40).report().pins);
  });

  it('measures again once the anchors are found again, since the layout may have changed', () => {
    const page = scene(0);
    page.report();
    page.scroll(40);
    page.anchors.changed();
    const again = page.report();
    expect(again.measured).toBeGreaterThan(0);
    expect(again.pins).toEqual(scene(40).report().pins);
  });

  it('measures again once a font has loaded, which lays the text out again in place', () => {
    const page = scene(0);
    page.report();
    page.scroll(40);
    page.anchors.relaid();
    const again = page.report();
    expect(again.measured).toBeGreaterThan(0);
    expect(again.pins).toEqual(scene(40).report().pins);
  });

  it('places a pin the view\'s top edge holds down as a fresh look would', () => {
    const page = scene(0);
    page.report();
    // The words' line runs above the view's top: their pin is held below it.
    page.scroll(LINE.top - VIEW.top + 6);
    const held = page.report();
    const fresh = scene(LINE.top - VIEW.top + 6).report();
    expect(held.pins).toEqual(fresh.pins);
    expect(held.pins![0]!.y).toBeGreaterThanOrEqual(22);
  });

  it('lets a pin go with words scrolled out above the view, measuring nothing for it', () => {
    const page = scene(0);
    const first = page.report();
    // Both lines are wholly above the view's top edge.
    const out = BLOCK.bottom - VIEW.top + 30;
    page.scroll(out);
    const gone = page.report();
    expect(gone.measured).toBe(0);
    expect(gone.pins).toEqual(first.pins!.map((pin) => ({ x: pin!.x, y: pin!.y - out })));
    expect(gone.pins![0]!.y).toBeLessThan(0);
    expect(gone.pins).toEqual(scene(out).report().pins);
  });
});
