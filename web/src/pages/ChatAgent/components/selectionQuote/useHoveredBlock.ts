import { useEffect, type RefObject } from 'react';
import { isQuoteSurface, makeQuoteAnchor, type BlockAnchor } from './quoteAnchor';
import { useHoverAnchor } from './useHoverAnchor';

/** Text the stream has finished writing (`TextMessageContent`), in a reply: a user's own bubble draws its words the same way. */
const QUOTABLE = '[data-message-role="assistant"] .message-quotable';
const BLOCKS = 'p, li, h1, h2, h3, h4, h5, h6';
/** Long enough that crossing a reply on the way somewhere opens nothing. */
const HOVER_MS = 150;

function blockAt(target: Element, event: PointerEvent): HTMLElement | null {
  if (event.buttons !== 0 || !window.getSelection()?.isCollapsed) return null;
  // An image has its own way in (`ImageReply`), and a code block and a table
  // would make a long quote of something the user rarely means whole.
  if (target instanceof HTMLImageElement) return null;
  const block = target.closest<HTMLElement>(BLOCKS);
  if (!block?.closest(QUOTABLE) || block.closest('pre, table')) return null;
  return block.innerText.trim() ? block : null;
}

function blockAnchor(block: HTMLElement): BlockAnchor {
  const range = document.createRange();
  range.selectNodeContents(block);
  return makeQuoteAnchor<BlockAnchor>({ kind: 'block', text: block.innerText.trim(), block, range });
}

/** The paragraph, list item or heading the pointer rests on, until a press or a selection takes over. */
export function useHoveredBlock({ surfaceRef, enabled }: {
  surfaceRef: RefObject<HTMLElement | null>;
  enabled: boolean;
}) {
  const hovered = useHoverAnchor({ surfaceRef, enabled, delay: HOVER_MS, resolve: blockAt, makeAnchor: blockAnchor });
  const { clear } = hovered;

  useEffect(() => {
    if (!enabled) return;
    const onPointerDown = (event: PointerEvent) => {
      if (!isQuoteSurface(event.target)) clear();
    };
    const onSelectionChange = () => {
      if (!window.getSelection()?.isCollapsed) clear();
    };
    document.addEventListener('pointerdown', onPointerDown, true);
    document.addEventListener('selectionchange', onSelectionChange);
    return () => {
      document.removeEventListener('pointerdown', onPointerDown, true);
      document.removeEventListener('selectionchange', onSelectionChange);
    };
  }, [enabled, clear]);

  return hovered;
}
