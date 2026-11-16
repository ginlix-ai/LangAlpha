import type { ArtifactCommentAnchor } from '@/types/api';
import type { FrameRect, PageHover } from '../pageProtocol';

/** A spot to outline: its anchor, where it shows, and where each of its parts shows when the page reports them. */
export interface OutlinedSpot {
  anchor: ArtifactCommentAnchor;
  rect: FrameRect | null;
  parts: FrameRect[] | undefined;
}

export interface SpotOutlines {
  /** The draft's element, which the hover ring settles on. */
  picked: FrameRect | null;
  /** One outline around an area: the draft's, or that of the comment whose card is open. */
  area: FrameRect | null;
  /** Each part on its own: the draft's as picked, or the open comment's. Drawn instead of the area's outline. */
  parts: { rects: FrameRect[]; tone: 'picked' | 'marked' } | null;
}

export function sameRect(a: FrameRect, b: FrameRect): boolean {
  return a.x === b.x && a.y === b.y && a.width === b.width && a.height === b.height;
}

/**
 * What outlines the comment's spot over the page. An area's outline settles
 * on the draft's area, or on that of the comment whose card is open, here or
 * in the list, and a page that reports an area's parts one by one has each
 * outlined instead. While a draft takes parts, what a click would add keeps
 * the hover ring, and the picked element then shows as a part, so the two
 * can differ. Areas and parts go out with the pins while the document under
 * them is being replaced (`hidden`).
 */
export function spotOutlines({ draft, draftPinned, overPage, shown, hover, addingParts, hidden }: {
  /** The draft, over the page or in the list. */
  draft: OutlinedSpot | null;
  /** The page has said where the draft's spot is; an area is outlined only then. */
  draftPinned: boolean;
  /** The draft's box is over the page. */
  overPage: boolean;
  /** The comment whose card is open. */
  shown: OutlinedSpot | null;
  hover: PageHover | null;
  /** In comment mode, with a draft that takes parts. */
  addingParts: boolean;
  hidden: boolean;
}): SpotOutlines {
  const draftArea = draft?.anchor.kind === 'area' && draftPinned ? draft : null;
  const shownArea = shown?.anchor.kind === 'area' ? shown : null;
  const element = overPage && draft?.anchor.kind === 'element' ? draft.rect : null;
  const hoverElsewhere = addingParts && hover !== null && !hover.word && !(element && sameRect(hover.rect, element));

  const parts: SpotOutlines['parts'] = hidden ? null
    : draftArea?.parts ? { rects: draftArea.parts, tone: 'picked' }
      : element && hoverElsewhere ? { rects: [element], tone: 'picked' }
        : draft?.anchor.kind !== 'area' && shownArea?.parts ? { rects: shownArea.parts, tone: 'marked' }
          : null;
  const area = hidden ? null
    : (draftArea && !draftArea.parts ? draftArea.rect : null)
      ?? (shownArea && !shownArea.parts ? shownArea.rect : null);
  return { picked: element && !hoverElsewhere ? element : null, area, parts };
}
