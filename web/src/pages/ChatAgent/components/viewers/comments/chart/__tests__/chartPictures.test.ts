import { describe, expect, it, vi } from 'vitest';
import type { ArtifactCommentRegionBox, ChartAnchor, ChartTime } from '@/types/artifacts';
import { chartPictureName, chartPictures } from '../chartPictures';
import type { ChartPlane } from '../chartSpots';

const written: ArtifactCommentRegionBox[] = [];
let shift = 0;

vi.mock('../../region/regionPictures', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../../region/regionPictures')>()),
  // Each upload takes a while, and the chart moves meanwhile.
  writePicture: vi.fn(async (_ws: string, _source: unknown, box: ArtifactCommentRegionBox, name: string) => {
    written.push(box);
    shift += 50;
    await Promise.resolve();
    return `.agents/comments/${name}`;
  }),
}));

const START = Date.UTC(2026, 9, 1);
const dayOf = (time: ChartTime) => (Date.parse(`${time}T00:00:00Z`) - START) / 86_400_000;

/** A day per 10 px from Oct 1, shifted right by `shift`, a price per px down from 500, on a 300 by 300 plot. */
const plane: ChartPlane = {
  area: () => ({ width: 300, height: 300 }),
  x: (time) => dayOf(time) * 10 + shift,
  y: (price) => 500 - price,
  halfBar: () => 5,
  timeAt: () => null,
  priceAt: () => null,
  elementAt: () => null,
  holds: () => true,
  show: () => {},
  watch: () => () => {},
};

describe('chartPictures', () => {
  it('places every region with the screenshot, before any upload moves the chart', async () => {
    const days = ['2026-10-02', '2026-10-04', '2026-10-06', '2026-10-08', '2026-10-10', '2026-10-12'];
    const anchor: ChartAnchor = { kind: 'chart', marks: days.map((d) => ({ type: 'region', start: d, end: d, low: 300, high: 400 })) };
    const write = chartPictures(plane, () => ({ canvas: document.createElement('canvas'), scale: 1 }));
    const saved = await write(anchor, { workspaceId: 'ws', path: 'NVDA:1day' });
    expect(written).toEqual(days.map((d) => ({ x: (dayOf(d) * 10 - 5) / 300, y: 100 / 300, w: 10 / 300, h: 100 / 300 })));
    expect(saved.kind === 'chart' && saved.marks.every((m) => m.type === 'region' && m.picture)).toBe(true);
  });
});

describe('chartPictureName', () => {
  const at = new Date(2026, 10, 24, 14, 12, 5);

  it("names a region's picture by its chart, when it was saved, and the save", () => {
    expect(chartPictureName('NVDA:1day', at, 'k3', null)).toBe('NVDA-1day-20261124-141205-k3.png');
    expect(chartPictureName('BRK.B:5min', at, 'k3', 1)).toBe('BRK.B-5min-20261124-141205-k3-2.png');
  });

  it('keeps a symbol to the letters a picture name takes', () => {
    expect(chartPictureName('^GSPC:1day', at, 'k3', null)).toBe('GSPC-1day-20261124-141205-k3.png');
  });
});
