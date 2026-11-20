import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { RegionAnchor } from '@/types/artifacts';

const upload = vi.fn();
vi.mock('../../../../../utils/api/files', () => ({ uploadWorkspaceFile: (...args: unknown[]) => upload(...args) }));

const { cropOf, imageSource, pictureName, withPictures } = await import('../regionPictures');

const AT = new Date(2026, 10, 18, 14, 12, 5);

describe('cropOf', () => {
  it('keeps a quarter of the box around it, at least 3% of the surface, within the surface', () => {
    const crop = cropOf({ x: 0.2, y: 0.2, w: 0.4, h: 0.4 });
    for (const [key, want] of [['x', 0.1], ['y', 0.1], ['w', 0.6], ['h', 0.6]] as const) expect(crop[key]).toBeCloseTo(want);
    const small = cropOf({ x: 0.5, y: 0.5, w: 0.02, h: 0.02 });
    expect(small.x).toBeCloseTo(0.47);
    expect(small.w).toBeCloseTo(0.08);
    const edge = cropOf({ x: 0, y: 0.9, w: 0.5, h: 0.1 });
    expect(edge.x).toBe(0);
    expect(edge.w).toBeCloseTo(0.625);
    expect(edge.y + edge.h).toBeCloseTo(1);
  });
});

describe('pictureName', () => {
  it('names the file, the page, when, and which of several, in what the API takes', () => {
    const box = { page: 3, x: 0, y: 0, w: 0.1, h: 0.1 };
    expect(pictureName('reports/Q3 report (final).pdf', box, AT, 'k3x9', null)).toBe('Q3-report-final-p3-20261118-141205-k3x9.png');
    expect(pictureName('chart.png', { ...box, page: undefined }, AT, 'k3x9', 1)).toBe('chart-20261118-141205-k3x9-2.png');
    expect(pictureName('报告.pdf', box, AT, 'k3x9', null)).toBe('file-p3-20261118-141205-k3x9.png');
  });
});

describe('withPictures', () => {
  const anchor: RegionAnchor = {
    kind: 'region',
    boxes: [{ x: 0.1, y: 0.1, w: 0.2, h: 0.2 }, { x: 0.5, y: 0.5, w: 0, h: 0 }],
  };

  beforeEach(() => {
    upload.mockReset();
  });

  it('leaves an anchor of words alone', async () => {
    const words = { kind: 'text', quote: 'net revenue' } as const;
    await expect(withPictures(words, { workspaceId: 'w', path: 'a.pdf', source: { maxScale: 1, size: vi.fn(), paint: vi.fn() } })).resolves.toBe(words);
  });

  it('saves a box whose picture cannot be read, or written, without one', async () => {
    const unread = { maxScale: 1, size: async () => null, paint: vi.fn() };
    await expect(withPictures(anchor, { workspaceId: 'w', path: 'a.png', source: unread })).resolves.toEqual(anchor);
    expect(upload).not.toHaveBeenCalled();
  });
});

describe('imageSource', () => {
  it('reads only the image it was made for, not the next one shown in the same element', async () => {
    const img = document.createElement('img');
    img.setAttribute('src', 'blob:first');
    Object.defineProperty(img, 'complete', { value: true });
    Object.defineProperty(img, 'naturalWidth', { value: 400 });
    Object.defineProperty(img, 'naturalHeight', { value: 200 });
    const source = imageSource(img, 'blob:first');
    const box = { x: 0.1, y: 0.1, w: 0.5, h: 0.5 };
    expect(await source.size(box)).toEqual({ width: 400, height: 200 });
    img.setAttribute('src', 'blob:second');
    expect(await source.size(box)).toBeNull();
    await expect(source.paint(document.createElement('canvas'), box, cropOf(box))).rejects.toThrow();
  });
});
