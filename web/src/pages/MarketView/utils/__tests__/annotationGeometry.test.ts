import { describe, expect, it } from 'vitest';

import type { ChartDataPoint } from '@/types/market';

import type { StoredAnnotation } from '../../stores/chartAnnotationStore';
import {
  DEFAULT_EVENT_COLOR,
  FIB_RATIOS,
  buildEvents,
  buildMarkers,
  buildPrimitiveData,
  dashForStyle,
  isEvent,
  resolveBarTime,
  resolveTrendlineData,
  snapToNearestBar,
  toUnixSeconds,
} from '../annotationGeometry';

const T = (iso: string): number => Math.floor(Date.parse(iso) / 1000);

// Four daily bars the annotations below reference.
const CHART: ChartDataPoint[] = [
  { time: T('2024-10-16T00:00:00Z'), open: 100, high: 105, low: 99, close: 104, volume: 1 },
  { time: T('2024-11-14T00:00:00Z'), open: 110, high: 115, low: 108, close: 112, volume: 1 },
  { time: T('2024-11-20T00:00:00Z'), open: 120, high: 125, low: 118, close: 122, volume: 1 },
  { time: T('2024-12-20T00:00:00Z'), open: 200, high: 205, low: 198, close: 202, volume: 1 },
];

describe('time helpers', () => {
  it('toUnixSeconds parses ISO and rejects garbage', () => {
    expect(toUnixSeconds('2024-10-16T00:00:00Z')).toBe(T('2024-10-16T00:00:00Z'));
    expect(toUnixSeconds('not-a-date')).toBeNull();
  });

  it('snapToNearestBar returns the closest bar time', () => {
    // 2024-11-13 is closest to the 11-14 bar
    expect(snapToNearestBar(CHART, T('2024-11-13T06:00:00Z'))).toBe(T('2024-11-14T00:00:00Z'));
    expect(snapToNearestBar([], 123)).toBeNull();
  });

  it('resolveBarTime falls back to raw seconds without chart data', () => {
    expect(resolveBarTime(null, '2024-10-16T00:00:00Z')).toBe(T('2024-10-16T00:00:00Z'));
  });

  it('dashForStyle maps styles to canvas dash patterns', () => {
    expect(dashForStyle('solid')).toEqual([]);
    expect(dashForStyle('dotted')).toEqual([1, 3]);
    expect(dashForStyle('dashed')).toEqual([4, 4]);
    expect(dashForStyle(undefined)).toEqual([4, 4]);
  });
});

describe('buildMarkers', () => {
  it('builds sorted markers and maps square/circle shapes', () => {
    const anns: StoredAnnotation[] = [
      {
        annotation_id: 'm2',
        symbol: 'NVDA',
        type: 'marker',
        time: '2024-12-20T00:00:00Z',
        shape: 'square',
      },
      {
        annotation_id: 'm1',
        symbol: 'NVDA',
        type: 'marker',
        time: '2024-11-14T00:00:00Z',
        shape: 'arrowUp',
        text: 'Earnings',
      },
    ];
    const markers = buildMarkers(anns, CHART);
    expect(markers).toHaveLength(2);
    // sorted by time ascending
    expect(markers[0].time).toBe(T('2024-11-14T00:00:00Z'));
    expect(markers[0].shape).toBe('arrowUp');
    // square is a native LWC SeriesMarkerShape — passed through, not downgraded.
    expect(markers[1].shape).toBe('square');
  });

  it('ignores non-marker annotations', () => {
    const anns: StoredAnnotation[] = [
      { annotation_id: 'p1', symbol: 'NVDA', type: 'price_line', price: 200 },
    ];
    expect(buildMarkers(anns, CHART)).toEqual([]);
  });
});

describe('buildPrimitiveData', () => {
  it('routes each variant to the right bucket and computes fib levels', () => {
    const anns: StoredAnnotation[] = [
      {
        annotation_id: 'r1',
        symbol: 'NVDA',
        type: 'rectangle',
        point1: { time: '2024-10-16T00:00:00Z', price: 150 },
        point2: { time: '2024-11-20T00:00:00Z', price: 140 },
      },
      {
        annotation_id: 'v1',
        symbol: 'NVDA',
        type: 'vertical_line',
        time: '2024-11-14T00:00:00Z',
        style: 'dotted',
      },
      {
        annotation_id: 't1',
        symbol: 'NVDA',
        type: 'text',
        time: '2024-11-14T00:00:00Z',
        price: 205,
        text: 'Breakout',
      },
      {
        annotation_id: 'f1',
        symbol: 'NVDA',
        type: 'fib_retracement',
        point1: { time: '2024-10-16T00:00:00Z', price: 100 },
        point2: { time: '2024-12-20T00:00:00Z', price: 200 },
      },
      // price lines / trendlines are NOT primitive shapes — must be ignored
      { annotation_id: 'p1', symbol: 'NVDA', type: 'price_line', price: 200 },
    ];

    const data = buildPrimitiveData(anns, CHART);
    expect(data.rects).toHaveLength(1);
    expect(data.vlines).toHaveLength(1);
    expect(data.vlines[0].dash).toEqual([1, 3]); // dotted
    expect(data.texts).toHaveLength(1);
    expect(data.texts[0].text).toBe('Breakout');
    expect(data.fibs).toHaveLength(1);

    const fib = data.fibs[0];
    expect(fib.levels).toHaveLength(FIB_RATIOS.length);
    // price = p2 + (p1 - p2) * ratio; with p1=100, p2=200:
    // ratio 0 -> 200, ratio 1 -> 100, ratio 0.5 -> 150
    const byRatio = Object.fromEntries(fib.levels.map((l) => [l.ratio, l.price]));
    expect(byRatio[0]).toBe(200);
    expect(byRatio[1]).toBe(100);
    expect(byRatio[0.5]).toBe(150);
  });

  it('orders rect and fib x-anchors by time even when points arrive reversed', () => {
    // point1 is LATER than point2 for both shapes. The off-screen clamp in the
    // primitive assumes time1 is the earlier (left) edge, so buildPrimitiveData
    // must reorder the span — otherwise a corner outside the loaded range paints
    // a phantom shape spanning the whole chart.
    const anns: StoredAnnotation[] = [
      {
        annotation_id: 'r-rev',
        symbol: 'NVDA',
        type: 'rectangle',
        point1: { time: '2024-12-20T00:00:00Z', price: 140 },
        point2: { time: '2024-10-16T00:00:00Z', price: 150 },
      },
      {
        annotation_id: 'f-rev',
        symbol: 'NVDA',
        type: 'fib_retracement',
        point1: { time: '2024-12-20T00:00:00Z', price: 200 },
        point2: { time: '2024-10-16T00:00:00Z', price: 100 },
      },
    ];

    const data = buildPrimitiveData(anns, CHART);

    const rect = data.rects[0];
    expect(rect.time1).toBeLessThanOrEqual(rect.time2);
    expect(rect.time1).toBe(T('2024-10-16T00:00:00Z'));
    expect(rect.time2).toBe(T('2024-12-20T00:00:00Z'));

    const fib = data.fibs[0];
    expect(fib.time1).toBeLessThanOrEqual(fib.time2);
    expect(fib.time1).toBe(T('2024-10-16T00:00:00Z'));
    expect(fib.time2).toBe(T('2024-12-20T00:00:00Z'));
    // Direction is preserved: level prices stay tied to point identity, NOT the
    // reordered time span. point1.price=200 -> ratio 1.0, point2.price=100 -> 0.0.
    const byRatio = Object.fromEntries(fib.levels.map((l) => [l.ratio, l.price]));
    expect(byRatio[1]).toBe(200);
    expect(byRatio[0]).toBe(100);
  });

  it('renders a trendline label as a chip anchored at the later endpoint', () => {
    const anns: StoredAnnotation[] = [
      {
        annotation_id: 'tl1',
        symbol: 'NVDA',
        type: 'trendline',
        label: 'Uptrend Support',
        color: '#22c55e',
        point1: { time: '2024-10-16T00:00:00Z', price: 100 },
        point2: { time: '2024-11-20T00:00:00Z', price: 140 },
      },
    ];
    const data = buildPrimitiveData(anns, CHART);
    // The line is drawn natively; only its label becomes a primitive chip.
    expect(data.texts).toHaveLength(1);
    expect(data.texts[0].text).toBe('Uptrend Support');
    expect(data.texts[0].price).toBe(140); // later point (2024-11-20)
    expect(data.texts[0].color).toBe('#22c55e');
  });

  it('omits event annotations from primitive data by default (DOM overlay owns them)', () => {
    const anns: StoredAnnotation[] = [
      {
        annotation_id: 'e1',
        symbol: 'NVDA',
        type: 'event',
        time: '2024-11-14T00:00:00Z',
        price: 205,
        title: 'Earnings',
        detail: 'Beat and raised.',
      },
    ];
    expect(buildPrimitiveData(anns, CHART).texts).toHaveLength(0);
  });

  it('renders the event title as a canvas chip when eventsAsText is set (inline card)', () => {
    const anns: StoredAnnotation[] = [
      {
        annotation_id: 'e1',
        symbol: 'NVDA',
        type: 'event',
        time: '2024-11-14T00:00:00Z',
        price: 205,
        title: 'Earnings',
        detail: 'Beat and raised.',
        color: '#abcdef',
      },
    ];
    const data = buildPrimitiveData(anns, CHART, { eventsAsText: true });
    expect(data.texts).toHaveLength(1);
    expect(data.texts[0].text).toBe('Earnings'); // title, not detail
    expect(data.texts[0].color).toBe('#abcdef');
  });

  it('omits the trendline chip when the line has no label', () => {
    const anns: StoredAnnotation[] = [
      {
        annotation_id: 'tl2',
        symbol: 'NVDA',
        type: 'trendline',
        point1: { time: '2024-10-16T00:00:00Z', price: 100 },
        point2: { time: '2024-11-20T00:00:00Z', price: 140 },
      },
    ];
    expect(buildPrimitiveData(anns, CHART).texts).toHaveLength(0);
  });
});

describe('buildEvents', () => {
  it('resolves events to snapped bar times, sorted, with default color', () => {
    const anns: StoredAnnotation[] = [
      {
        annotation_id: 'e2',
        symbol: 'NVDA',
        type: 'event',
        time: '2024-12-20T00:00:00Z',
        price: 202,
        title: 'Later',
        detail: 'd2',
      },
      {
        annotation_id: 'e1',
        symbol: 'NVDA',
        type: 'event',
        time: '2024-11-13T06:00:00Z', // snaps to the 11-14 bar
        price: 112,
        title: 'Earlier',
        detail: 'd1',
        color: '#123456',
      },
      // non-event annotations are ignored
      { annotation_id: 'p1', symbol: 'NVDA', type: 'price_line', price: 200 },
    ];
    const events = buildEvents(anns, CHART);
    expect(events).toHaveLength(2);
    // sorted ascending by resolved time
    expect(events[0].title).toBe('Earlier');
    expect(events[0].time).toBe(T('2024-11-14T00:00:00Z'));
    expect(events[0].color).toBe('#123456');
    // default color applied when omitted
    expect(events[1].title).toBe('Later');
    expect(events[1].color).toBe(DEFAULT_EVENT_COLOR);
  });

  it('skips events whose time cannot be parsed', () => {
    const anns: StoredAnnotation[] = [
      {
        annotation_id: 'e1',
        symbol: 'NVDA',
        type: 'event',
        time: 'not-a-date',
        price: 100,
        title: 'Bad',
        detail: 'd',
      },
    ];
    // resolveBarTime returns null only when toUnixSeconds fails AND no chart
    // data; with chart data a bad ISO yields null seconds → skipped.
    expect(buildEvents(anns, null)).toHaveLength(0);
  });

  it('isEvent narrows the event type', () => {
    expect(
      isEvent({
        annotation_id: 'e1',
        symbol: 'NVDA',
        type: 'event',
        time: '2024-11-14T00:00:00Z',
        price: 1,
        title: 't',
        detail: 'd',
      }),
    ).toBe(true);
    expect(isEvent({ annotation_id: 'p1', symbol: 'NVDA', type: 'price_line', price: 1 })).toBe(
      false,
    );
  });
});

describe('resolveTrendlineData', () => {
  it('returns two points sorted by time', () => {
    const line = resolveTrendlineData(
      {
        annotation_id: 'tl',
        symbol: 'NVDA',
        type: 'trendline',
        point1: { time: '2024-12-20T00:00:00Z', price: 200 },
        point2: { time: '2024-10-16T00:00:00Z', price: 100 },
      },
      CHART,
    );
    expect(line).not.toBeNull();
    expect(line!).toHaveLength(2);
    // earlier bar first, carrying its own price (100)
    expect(line![0].time).toBe(T('2024-10-16T00:00:00Z'));
    expect(line![0].value).toBe(100);
    expect(line![1].value).toBe(200);
  });

  it('returns null for a degenerate same-time line', () => {
    const line = resolveTrendlineData(
      {
        annotation_id: 'tl',
        symbol: 'NVDA',
        type: 'trendline',
        point1: { time: '2024-10-16T00:00:00Z', price: 100 },
        point2: { time: '2024-10-16T00:00:00Z', price: 120 },
      },
      null,
    );
    expect(line).toBeNull();
  });
});
