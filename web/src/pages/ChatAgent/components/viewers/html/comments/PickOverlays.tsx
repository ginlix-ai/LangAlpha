import React, { useEffect, useState } from 'react';
import { MessageSquarePlus } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { AnimatePresence, motion } from '@/lib/framer';
import { TWEEN } from '@/lib/motion';
import type { FrameRect, PageHover, PagePick } from '../pageProtocol';
import { useKeptWhileFading } from './useKeptWhileFading';

/** How far a settled area's outline stands off the elements it holds, in px. */
const AREA_PAD = 3;
/** How far the box around a word under the pointer stands off its glyphs at either side, in px. */
const WORD_PAD = 2;

// The selection's chip fades when the words are deselected, and just goes
// when it becomes the draft, whose box pops from the same spot.
const CHIP_VARIANTS = {
  gone: (becameDraft: boolean) => ({ opacity: 0, transition: becameDraft ? { duration: 0 } : TWEEN.exit }),
};

type Outline = { rect: FrameRect; label: string | null; picked: boolean };
type Area = { rect: FrameRect; live: boolean; key: number };

function sameRect(a: FrameRect, b: FrameRect): boolean {
  return a.x === b.x && a.y === b.y && a.width === b.width && a.height === b.height;
}

function sameOutline(a: Outline, b: Outline): boolean {
  return sameRect(a.rect, b.rect) && a.label === b.label && a.picked === b.picked;
}

function sameArea(a: Area, b: Area): boolean {
  return sameRect(a.rect, b.rect) && a.live === b.live && a.key === b.key;
}

/** How many times `shown` has turned on: each is a fresh appearance, drawn in place rather than glided to. */
function useAppearances(shown: boolean): number {
  const [seen, setSeen] = useState({ shown, count: shown ? 1 : 0 });
  const count = seen.shown === shown || !shown ? seen.count : seen.count + 1;
  if (seen.shown !== shown) setSeen({ shown, count });
  return count;
}

interface PickOverlaysProps {
  /** Clicks pick: the outline follows the pointer, and words open their box with no chip. */
  mode: boolean;
  /** A box is open over the page, so nothing under the pointer is outlined. */
  boxOpen: boolean;
  hover: PageHover | null;
  /** The rectangle being dragged out, while it is. */
  areaDrag: FrameRect | null;
  /** The element a draft's box is open on, where the page says it is now. */
  picked: FrameRect | null;
  /** The area of the draft, or of the comment whose card is open; null while the pins are out. */
  area: FrameRect | null;
  scrolling: boolean;
  /** Words selected outside comment mode, which the chip offers; null while a draft's box is open. */
  selection: PagePick | null;
  size: { width: number; height: number };
  /** A comment is being written: the chip that became it goes at once. */
  drafting: boolean;
  onComment: (selection: PagePick) => void;
}

/**
 * What is drawn over the page for picking a spot: the outline of what a click
 * would pick, the box around the word under the pointer, an area's outline,
 * and the chip that offers to comment on words selected outside the mode.
 */
export function PickOverlays({
  mode, boxOpen, hover, areaDrag, picked, area: settledArea, scrolling, selection, size, drafting, onComment,
}: PickOverlaysProps): React.ReactElement {
  const { t } = useTranslation();
  const liveArea = mode ? areaDrag : null;
  const hovering = mode && !boxOpen && !liveArea && hover ? hover : null;
  // Over text the word a click would take is boxed, and no element.
  const hoverElement = hovering && !hovering.word ? hovering : null;
  const hoverWord = hovering?.word ? hovering.rect : null;

  // The outline of what a click would pick, which becomes the picked spot's
  // outline in place: one element that glides from box to box as the pointer
  // moves, fades in where it first shows, and settles a step darker and
  // further out when picked. Picked, it keeps the label it had, which fades.
  const lastHover = useKeptWhileFading(hoverElement);
  const outlineRect = picked ?? hoverElement?.rect ?? null;
  const outlineShown = outlineRect !== null;
  const outline = useKeptWhileFading<Outline>(
    outlineRect && { rect: outlineRect, label: lastHover?.label ?? null, picked: !!picked },
    sameOutline,
  );
  const outlineShows = useAppearances(outlineShown);
  const [stillShows, setStillShows] = useState(0);
  const outlineFresh = outlineShows !== stillShows;
  useEffect(() => {
    if (!outlineFresh) return;
    // Two frames: the first paints the new spot with no glide, the second
    // lets the next move glide.
    let second = 0;
    const first = requestAnimationFrame(() => {
      second = requestAnimationFrame(() => setStillShows(outlineShows));
    });
    return () => {
      cancelAnimationFrame(first);
      cancelAnimationFrame(second);
    };
  }, [outlineFresh, outlineShows]);

  // An area's outline: the rectangle being dragged out, which glides onto the
  // elements it took once released; then the area of the draft, or of the
  // comment whose card is open, here or in the list. Kept once it goes, so it
  // fades where it was. Each fresh show is a new outline (`key`), drawn in
  // place rather than gliding from the last one.
  const areaRect = liveArea ?? settledArea;
  const areaShown = areaRect !== null;
  const areaShows = useAppearances(areaShown);
  const area = useKeptWhileFading<Area>(areaRect && { rect: areaRect, live: liveArea !== null, key: areaShows }, sameArea);

  // Outside comment mode only: in it, words picked open their box at once.
  let chip: { right: number; top: number; above: boolean } | null = null;
  if (selection && !mode && size.width > 0) {
    const r = selection.rect;
    const below = r.y + r.height + 6;
    const above = below + 30 > size.height;
    const left = Math.min(Math.max(r.x + r.width, 96), size.width - 8);
    chip = { right: size.width - left, top: above ? Math.max(4, r.y - 34) : below, above };
  }

  return (
    <>
      {outline && (
        <div
          className="page-comments-outline"
          data-shown={outlineShown || undefined}
          data-picked={outline.picked || undefined}
          data-still={outlineFresh || scrolling || undefined}
          style={{
            transform: `translate(${outline.rect.x}px, ${outline.rect.y}px)`,
            width: outline.rect.width,
            height: outline.rect.height,
          }}
        >
          {outline.label && <span className="page-comments-outline-label">{outline.label}</span>}
        </div>
      )}

      {hoverWord && (
        <div
          className="page-comments-word"
          style={{
            transform: `translate(${hoverWord.x - WORD_PAD}px, ${hoverWord.y}px)`,
            width: hoverWord.width + 2 * WORD_PAD,
            height: hoverWord.height,
          }}
        />
      )}

      {area && (
        <div
          key={area.key}
          className="page-comments-area"
          data-shown={areaShown || undefined}
          data-live={area.live || undefined}
          data-still={scrolling || undefined}
          style={area.live ? {
            transform: `translate(${area.rect.x}px, ${area.rect.y}px)`,
            width: area.rect.width,
            height: area.rect.height,
          } : {
            transform: `translate(${area.rect.x - AREA_PAD}px, ${area.rect.y - AREA_PAD}px)`,
            width: area.rect.width + 2 * AREA_PAD,
            height: area.rect.height + 2 * AREA_PAD,
          }}
        />
      )}

      <AnimatePresence custom={drafting}>
        {chip && selection && (
          <motion.button
            key="select"
            type="button"
            className="page-comments-select"
            data-side={chip.above ? 'top' : 'bottom'}
            style={{ right: chip.right, top: chip.top }}
            initial={false}
            exit="gone"
            variants={CHIP_VARIANTS}
            onClick={() => onComment(selection)}
          >
            <MessageSquarePlus className="h-3.5 w-3.5" />
            {t('pageViewer.comments.comment')}
          </motion.button>
        )}
      </AnimatePresence>
    </>
  );
}
