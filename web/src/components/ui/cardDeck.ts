/**
 * The card-deck motion: a stack of cards that fans open on a click.
 *
 * Several decks in the app use it (a turn's deliverables, a turn's sources,
 * the chat input's context snapshots) and they are meant to move alike, so the
 * geometry and the dismiss behaviour live here rather than being restated per
 * deck. They were restated, and the copies drifted: a card whose offset rides
 * `top` teleports to its open slot while only the last few pixels animate,
 * because nothing transitions `top` here. The fix landed in one copy.
 *
 * Sizes differ between decks on purpose and travel in `DeckGeometry`; the
 * formulas do not.
 */
import { useEffect, useEffectEvent, useRef, type CSSProperties } from 'react';

export interface DeckGeometry {
  cardHeight: number;
  cardGap: number;
  /** Pixels a peek card sits below the one in front of it. */
  peekStep: number;
  /** Peek cards drawn behind the front; the rest park under the deepest one. */
  maxPeekLayers: number;
  /** How much each peek layer shrinks. */
  peekScaleStep: number;
  /** The smallest a peek card is allowed to get. */
  minPeekScale: number;
}

export interface DeckSlot {
  /** Spread onto the card's `style`. Placement only: the card sets its own height. */
  style: CSSProperties;
  /** Answers to pointer and keyboard: the front card, or any card once open. */
  interactive: boolean;
}

/** Peek layers actually drawn behind the front card. */
export function deckPeekLayers(count: number, g: DeckGeometry): number {
  return Math.min(count - 1, g.maxPeekLayers);
}

/** The stack's height, open or shut. */
export function deckHeight(count: number, fanned: boolean, g: DeckGeometry): number {
  return fanned
    ? count * (g.cardHeight + g.cardGap) - g.cardGap
    : g.cardHeight + deckPeekLayers(count, g) * g.peekStep;
}

/**
 * Where card `i` sits. The whole offset rides `transform`, which is the one
 * property the browser animates here.
 *
 * Shut, the stack shows at most `maxPeekLayers` behind the front and parks the
 * rest under the deepest peek at zero opacity. They stay mounted so opening is
 * one continuous transition rather than a slide for some cards and an abrupt
 * mount for the others.
 */
export function deckSlot(i: number, count: number, fanned: boolean, g: DeckGeometry): DeckSlot {
  const isTop = i === 0;
  const depth = Math.min(i, g.maxPeekLayers);
  const buried = !fanned && i > g.maxPeekLayers;
  const y = fanned ? i * (g.cardHeight + g.cardGap) : depth * g.peekStep;
  const scale = fanned ? 1 : Math.max(1 - depth * g.peekScaleStep, g.minPeekScale);
  const interactive = fanned || isTop;
  return {
    style: {
      transform: `translateY(${y}px) scale(${scale})`,
      opacity: fanned ? 1 : buried ? 0 : isTop ? 1 : Math.max(0.85 - (depth - 1) * 0.2, 0.25),
      zIndex: count - i,
      pointerEvents: interactive ? 'auto' : 'none',
    },
    interactive,
  };
}

/**
 * Shut the deck on an outside click or Escape, and hand back the ref that says
 * what "outside" means.
 *
 * The listeners are attached a frame late, so the click that opened the deck
 * cannot immediately shut it again, and `onCollapse` is read through an effect
 * event, so a caller passing an inline arrow does not re-register them on every
 * render.
 *
 * Escape takes the focus back with it. Shutting the deck unmounts the cards
 * behind the front one, so a reader who had tabbed onto one of them was left on
 * `document.body`, with the next Tab starting over from the top of the page.
 * Only the keyboard path restores: a click elsewhere is the reader putting the
 * focus somewhere themselves, and the deck has no business taking it back.
 */
export function useDeckCollapse({
  open,
  onCollapse,
  suspend = false,
  ignoreWithin,
  boundary,
}: {
  open: boolean;
  onCollapse: () => void;
  /** Hold the collapse while something this deck owns is open (a card's menu). */
  suspend?: boolean;
  /** Selector for a portal whose clicks belong to this deck (an opened detail dialog). */
  ignoreWithin?: string;
  /** Widens "outside" past the deck itself, to a surrounding rail the deck shares. */
  boundary?: React.RefObject<HTMLElement | null>;
}): React.RefObject<HTMLDivElement | null> {
  const rootRef = useRef<HTMLDivElement | null>(null);
  const collapse = useEffectEvent(onCollapse);

  useEffect(() => {
    if (!open || suspend) return;
    const onDown = (e: MouseEvent) => {
      const target = e.target as HTMLElement | null;
      if (!target) return;
      if (ignoreWithin && target.closest?.(ignoreWithin)) return;
      // A click on something already detached says nothing about intent.
      if (!document.body.contains(target)) return;
      if ((boundary?.current ?? rootRef.current)?.contains(target)) return;
      collapse();
    };
    const onKey = (e: KeyboardEvent) => {
      if (e.key !== 'Escape') return;
      const held = !!rootRef.current?.contains(document.activeElement);
      collapse();
      if (!held) return;
      // A frame late for the same reason the listeners are: the card holding
      // the focus is still mounted until the collapse has rendered, and the
      // front card's own control is the first one left standing.
      requestAnimationFrame(() => {
        rootRef.current?.querySelector<HTMLElement>('button:not([disabled]), [href], [tabindex]:not([tabindex="-1"])')?.focus();
      });
    };
    let attached = false;
    const raf = requestAnimationFrame(() => {
      document.addEventListener('mousedown', onDown);
      document.addEventListener('keydown', onKey);
      attached = true;
    });
    return () => {
      cancelAnimationFrame(raf);
      if (attached) {
        document.removeEventListener('mousedown', onDown);
        document.removeEventListener('keydown', onKey);
      }
    };
  }, [open, suspend, ignoreWithin, boundary]);

  return rootRef;
}
