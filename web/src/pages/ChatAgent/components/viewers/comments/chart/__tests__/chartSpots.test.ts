import { afterEach, describe, expect, it } from 'vitest';
import type { ChartMark, ChartTime } from '@/types/artifacts';
import type { ElementIdentity } from '@/pages/MarketView/utils/chartLayer/elements';
import { ViewerPage } from '../../viewerPage';
import { MAX_CHART_MARKS } from '../../../html/pageProtocol';
import { markKey } from '../chartMarks';
import { chartSpots, type ChartPlane } from '../chartSpots';

const DAY_PX = 10;
const START = Date.UTC(2026, 9, 1);

/** A day per 10 px from Oct 1, a price per px down from 500, on a 300 by 300 plot at (100, 50). */
function plane(shape: ElementIdentity | null = null): ChartPlane {
  const dayOf = (time: ChartTime) => (Date.parse(`${time}T00:00:00Z`) - START) / 86_400_000;
  return {
    area: () => ({ width: 300, height: 300 }),
    x: (time) => dayOf(time) * DAY_PX,
    y: (price) => 500 - price,
    halfBar: () => DAY_PX / 2,
    timeAt: (x) => new Date(START + Math.round(x / DAY_PX) * 86_400_000).toISOString().slice(0, 10),
    priceAt: (y) => 500 - y,
    elementAt: (x, y) => (shape && Math.abs(x - 120) < 3 && Math.abs(y - 200) < 3 ? shape : null),
    holds: () => true,
    show: () => {},
    watch: () => () => {},
  };
}

function content(): HTMLElement {
  const el = document.createElement('div');
  el.getBoundingClientRect = () => new DOMRect(100, 50, 400, 300);
  document.body.appendChild(el);
  return el;
}

const neckline: ElementIdentity = {
  layer: 'nvda-hs', layer_title: 'Head and shoulders', element_kind: 'drawing',
  element_type: 'segment', element_name: 'Neckline', index: 1,
};

afterEach(() => {
  document.body.innerHTML = '';
});

describe('chartSpots', () => {
  it('saves a drag as a region in bar times and prices, its edges snapped to the bars under them', () => {
    const { spots } = chartSpots(plane(), content());
    // Content x 21 to 64 is days 2.1 to 6.4; y 100 to 250 is 400 down to 250.
    const drawn = spots!.draw!({ left: 121, top: 150, right: 164, bottom: 300 }, { x: 121, y: 150 });
    expect(spots!.anchorOf(drawn)).toEqual({
      kind: 'chart',
      marks: [{ type: 'region', start: '2026-10-03', end: '2026-10-07', low: 250, high: 400 }],
    });
  });

  it('takes a drag too small to be a region as a click where it began', () => {
    const { spots } = chartSpots(plane(), content());
    const drawn = spots!.draw!({ left: 150, top: 150, right: 152, bottom: 170 }, { x: 150, y: 150 });
    expect(spots!.anchorOf(drawn)).toEqual({ kind: 'chart', marks: [{ type: 'point', time: '2026-10-06', price: 400 }] });
  });

  it('pins a point off any shape, and takes the shape a click lands on', () => {
    const { spots } = chartSpots(plane(neckline), content());
    const point = spots!.pointAt!(300, 250);
    const shape = spots!.pointAt!(220, 250);
    expect(spots!.anchorOf([point!, shape!])).toEqual({
      kind: 'chart',
      marks: [
        { type: 'point', time: '2026-10-21', price: 300 },
        { type: 'element', ...neckline, time: '2026-10-13', price: 300 },
      ],
    });
  });

  it('keeps marks off the plot area out', () => {
    const { spots } = chartSpots(plane(), content());
    expect(spots!.pointAt!(450, 100)).toBeNull();
    expect(spots!.draw!({ left: 450, top: 60, right: 480, bottom: 90 }, { x: 450, y: 60 })).toEqual([]);
  });

  it("lets go of its marks with its page, and never picks another page's", () => {
    const el = content();
    const before = chartSpots(plane(), el);
    const [old] = before.spots!.draw!({ left: 121, top: 150, right: 164, bottom: 300 }, { x: 121, y: 150 });
    // The chart kept while another was shown: a new page's strategy over the same content.
    const after = chartSpots(plane(), el);
    expect(before.spots!.spotAt(old!)).toBe(old);
    expect(after.spots!.spotAt(old!)).toBeNull();
    const page = new ViewerPage(el, el, before, { path: 'NVDA:1day', emit: () => {}, schedule: () => () => {} });
    page.attach()();
    expect(old!.isConnected).toBe(false);
  });

  describe('a draft with marks out of view', () => {
    const a = { type: 'region' as const, start: '2026-10-03', end: '2026-10-07', low: 250, high: 400 };
    const b = { type: 'region' as const, start: '2026-10-10', end: '2026-10-12', low: 300, high: 350 };
    const p1 = { type: 'point' as const, time: '2027-01-15', price: 300 };
    const p2 = { type: 'point' as const, time: '2027-02-15', price: 320 };

    /** A page over the chart whose draft is `marks`, and what it sends after `part` is taken out. */
    function drop(marks: ChartMark[], part: ChartMark): unknown[] {
      const el = content();
      const sent: unknown[] = [];
      const page = new ViewerPage(el, el, chartSpots(plane(), el), {
        path: 'NVDA:1day', emit: (m) => sent.push(m), schedule: (run) => { run(); return () => {}; },
      });
      const detach = page.attach();
      page.handle({ type: 'page:anchors', items: [{ id: 'draft', anchor: { kind: 'chart', marks }, draft: true }] });
      sent.length = 0;
      page.handle({ type: 'page:drop-part', part });
      detach();
      return sent.filter((m) => (m as { type: string }).type === 'page:pick');
    }

    it('keeps them when another part leaves', () => {
      expect(drop([a, b, p1], a)).toMatchObject([{ extend: true, anchor: { kind: 'chart', marks: [b, p1] } }]);
    });

    it('is them alone once the last part in view leaves', () => {
      expect(drop([a, p1, p2], a)).toMatchObject([{ extend: true, anchor: { kind: 'chart', marks: [p1, p2] } }]);
    });

    it('takes one out with none in view', () => {
      expect(drop([p1, p2], p1)).toMatchObject([{ extend: true, anchor: { kind: 'chart', marks: [p2] } }]);
    });

    it('adds a part with every mark out of view, and keeps them', () => {
      const el = content();
      const sent: { type: string; anchor?: unknown }[] = [];
      const page = new ViewerPage(el, el, chartSpots(plane(), el), {
        path: 'NVDA:1day', emit: (m) => sent.push(m as never), schedule: (run) => { run(); return () => {}; },
      });
      const detach = page.attach();
      page.handle({ type: 'page:comment-mode', on: true });
      page.handle({ type: 'page:anchors', items: [{ id: 'draft', anchor: { kind: 'chart', marks: [p1] }, draft: true }] });
      page.handle({ type: 'page:extend-next', on: true });
      el.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, clientX: 300, clientY: 250, detail: 0 }));
      detach();
      const picks = sent.filter((m) => m.type === 'page:pick');
      expect(picks.at(-1)).toMatchObject({ extend: true, anchor: { kind: 'chart', marks: [{ type: 'point', time: '2026-10-21', price: 300 }, p1] } });
    });

    it('keeps its last part, as a document does', () => {
      expect(drop([p1], p1)).toEqual([]);
      expect(drop([a], a)).toEqual([]);
    });
  });

  describe('withAway', () => {
    const at = { layer: 'nvda-hs', layer_title: 'Head and shoulders', element_kind: 'drawing' as const, index: 2, time: '2026-10-13', price: 300 };
    const segment: ChartMark = { type: 'element', ...at, element_type: 'segment' };
    const box: ChartMark = { type: 'element', ...at, element_type: 'box' };

    it('tells apart shapes with no id in one place by their type and name, and keeps a shape with an id keyed as before', () => {
      expect(markKey(segment)).not.toBe(markKey(box));
      expect(markKey({ ...segment, element_name: 'Neckline' })).not.toBe(markKey(segment));
      expect(markKey({ type: 'element', ...at, element_id: 'neck', element_type: 'segment', element_name: 'Neckline' }))
        .toBe('element|nvda-hs|drawing|neck|2|2026-10-13|300');
      const { spots } = chartSpots(plane(), content());
      expect(spots!.withAway!({ kind: 'chart', marks: [segment] }, [box])).toEqual({ kind: 'chart', marks: [segment, box] });
    });

    it('keeps every part out of view at the limit, refusing the newest mark in view', () => {
      const away = Array.from({ length: MAX_CHART_MARKS - 1 }, (_, i): ChartMark => ({ type: 'point', time: '2027-01-15', price: 100 + i }));
      const kept: ChartMark = { type: 'point', time: '2026-10-05', price: 300 };
      const added: ChartMark = { type: 'point', time: '2026-10-06', price: 300 };
      const { spots } = chartSpots(plane(), content());
      expect(spots!.withAway!({ kind: 'chart', marks: [kept, added] }, away)).toEqual({ kind: 'chart', marks: [kept, ...away] });
    });
  });

  it('parks a comment whose marks are all out of view, and lays the one in view', () => {
    const el = content();
    const strategy = chartSpots(plane(), el);
    const late = { type: 'point' as const, time: '2027-01-15', price: 300 };
    expect(strategy.find({ kind: 'chart', marks: [late] })).toMatchObject({ parked: el });
    const found = strategy.find({ kind: 'chart', marks: [late, { type: 'point', time: '2026-10-05', price: 300 }] });
    expect(found).toMatchObject({ parts: [null, [expect.any(HTMLElement)]] });
  });
});
