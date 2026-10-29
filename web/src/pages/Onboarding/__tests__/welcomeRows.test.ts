import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { riseInRows, rowDelays, rowsOf, unitsOf } from '../welcome/rows';

// jsdom lays nothing out, so each element's top is read from its data-top and
// every element counts as drawn.
function layOut() {
  vi.spyOn(Element.prototype, 'getBoundingClientRect').mockImplementation(function (this: Element) {
    const top = Number((this as HTMLElement).dataset.top ?? 0);
    return { top, bottom: top, left: 0, right: 0, width: 0, height: 0, x: 0, y: top, toJSON: () => ({}) } as DOMRect;
  });
  vi.spyOn(Element.prototype, 'getClientRects').mockImplementation(() => [{}] as unknown as DOMRectList);
}

// A turn's card: stacked options, a section with a heading over a two-column
// grid, the footer, and a row of chips that fits on one line.
function card() {
  const root = document.createElement('div');
  root.innerHTML = `
    <div>
      <div role="radiogroup">
        <label data-top="0">A</label>
        <label data-top="60">B</label>
      </div>
      <section data-top="140">
        <h2 data-top="140">Hold</h2>
        <div role="radiogroup" data-top="170">
          <label data-top="170">x</label><label data-top="170">y</label>
          <label data-top="220">z</label><label data-top="220">w</label>
        </div>
      </section>
      <div data-top="280"><button data-top="280">Continue</button></div>
      <div data-top="330" id="chips"><span data-top="330">a</span><span data-top="330">b</span></div>
    </div>`;
  document.body.append(root);
  return root;
}

describe('welcome card rows', () => {
  beforeEach(layOut);
  afterEach(() => {
    vi.restoreAllMocks();
    document.body.innerHTML = '';
  });

  it('opens up what stacks and keeps what sits on one line whole', () => {
    const units = unitsOf(card()).map((el) => el.id || el.textContent?.trim());
    expect(units).toEqual(['A', 'B', 'Hold', 'x', 'y', 'z', 'w', 'Continue', 'chips']);
  });

  it('groups units that start on the same line into one row', () => {
    const rows = rowsOf(unitsOf(card()), (el) => el.getBoundingClientRect().top);
    expect(rows.map((row) => row.map((el) => el.id || el.textContent?.trim()))).toEqual([
      ['A'],
      ['B'],
      ['Hold'],
      ['x', 'y'],
      ['z', 'w'],
      ['Continue'],
      ['chips'],
    ]);
  });

  it('steps a tall card faster, so its last row starts no later than a short one would allow', () => {
    expect(rowDelays(3, 100)).toEqual([100, 140, 180]);
    const tall = rowDelays(15, 0);
    expect(tall[14]).toBe(280);
    expect(tall[1]).toBe(20);
    expect(rowDelays(1, 50)).toEqual([50]);
  });

  it('brings each row up after the one above it, the first after the lead', () => {
    const animate = vi.fn();
    Object.defineProperty(HTMLElement.prototype, 'animate', { value: animate, configurable: true });
    try {
      riseInRows(card(), 120);
      const delays = animate.mock.calls.map(([, timing]) => (timing as KeyframeAnimationOptions).delay);
      expect(delays).toEqual([120, 160, 200, 240, 240, 280, 280, 320, 360]);
    } finally {
      delete (HTMLElement.prototype as Partial<HTMLElement>).animate;
    }
  });
});
