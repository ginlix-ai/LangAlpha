// @vitest-environment node
import { describe, expect, it } from 'vitest';
import type { ArtifactCommentAnchor } from '@/types/api';
import type { FrameRect } from '../../pageProtocol';
import { spotOutlines, type OutlinedSpot } from '../spotOutlines';

const rect = (x: number): FrameRect => ({ x, y: 10, width: 40, height: 20 });
const ELEMENT: ArtifactCommentAnchor = { kind: 'element', path: '#a' };
const AREA: ArtifactCommentAnchor = { kind: 'area', elements: [{ path: '#a' }, { path: '#b' }], count: 2 };
const spot = (anchor: ArtifactCommentAnchor, parts?: FrameRect[]): OutlinedSpot => ({ anchor, rect: rect(0), parts });
const base = { draft: null, draftPinned: true, overPage: true, shown: null, hover: null, addingParts: false, hidden: false };

describe('what outlines a comment spot over the page', () => {
  it('settles the hover ring on the draft element, until a part elsewhere is under the pointer', () => {
    expect(spotOutlines({ ...base, draft: spot(ELEMENT) })).toEqual({ picked: rect(0), area: null, parts: null });
    const elsewhere = spotOutlines({
      ...base, draft: spot(ELEMENT), addingParts: true, hover: { rect: rect(100), label: 'B', word: false },
    });
    expect(elsewhere).toEqual({ picked: null, area: null, parts: { rects: [rect(0)], tone: 'picked' } });
  });

  it('outlines each part of an area the page reports one by one, else the area once it is pinned', () => {
    expect(spotOutlines({ ...base, draft: spot(AREA, [rect(0), rect(50)]) }).parts)
      .toEqual({ rects: [rect(0), rect(50)], tone: 'picked' });
    expect(spotOutlines({ ...base, draft: spot(AREA) }).area).toEqual(rect(0));
    expect(spotOutlines({ ...base, draft: spot(AREA), draftPinned: false }).area).toBeNull();
  });

  it('marks the open comment\'s parts beside an element draft, and nothing while the document is replaced', () => {
    expect(spotOutlines({ ...base, draft: spot(ELEMENT), shown: spot(AREA, [rect(60)]) }).parts)
      .toEqual({ rects: [rect(60)], tone: 'marked' });
    expect(spotOutlines({ ...base, shown: spot(AREA, [rect(60)]), hidden: true })).toMatchObject({ area: null, parts: null });
  });
});
