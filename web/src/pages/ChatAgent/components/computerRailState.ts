import { useCallback, useEffect, useLayoutEffect, useRef, useState, type KeyboardEvent } from 'react';

import { scrollMotion } from '@/lib/reducedMotion';

/** A card's place on the rail, from 1, and whether it is the one in the middle. */
export interface RailPlace {
  index: number;
  active: boolean;
  onActivate: () => void;
}

/**
 * A card off the middle of the rail takes no input: its controls are inert,
 * out of reach of a click and of Tab, and a click anywhere on it brings it to
 * the middle instead. Nothing is pressed on a card the reader is not looking at.
 */
export function railCard(active: boolean, onActivate: () => void) {
  return {
    card: { onClick: active ? undefined : onActivate },
    content: { inert: !active },
  };
}

/** A place on the rail as its labels write it: "01". */
export function railOrdinal(n: number): string {
  return String(n).padStart(2, '0');
}

/**
 * The rail's position: which card is in the middle, kept as the reader
 * scrolls, swipes or steps with the arrow keys, and a way to bring one there.
 *
 * It starts at `start.index` as it mounts, there at once (on opening, or back
 * from a machine's details, where `start.focus` puts focus on that card's
 * Details), and puts the card of `arrivingId` there once a machine just made
 * lands: at once, since it is made under the New computer form, so the rail
 * comes back with it already in the middle. It follows that card by
 * `arrivingIndex`, gliding, if the list's refetch moves it, until the reader
 * moves the rail. Nothing else moves it, so a list that changes under it
 * leaves it be.
 */
export function useComputerRail(
  count: number,
  start: { index: number; focus: boolean },
  arrivingId: string | null,
  arrivingIndex: number,
) {
  const ref = useRef<HTMLDivElement>(null);
  const frame = useRef(0);
  const [first] = useState(start);
  const [current, setCurrent] = useState(first.index);
  // A removal can leave the middle past the end until the next scroll.
  const active = Math.min(current, count - 1);
  // What the arrow keys leave focus on, found on the card they step to.
  const focusNext = useRef<string | null>(null);
  // The card a glide is headed for. The cards it passes on the way are not
  // the middle: making them so would make the card headed for inert, and
  // take focus off it, for a moment.
  const glidingTo = useRef<number | null>(null);

  const goTo = useCallback((index: number, { smooth = true }: { smooth?: boolean } = {}) => {
    const rail = ref.current;
    if (!rail) return;
    const to = Math.max(0, Math.min(rail.children.length - 1, index));
    const card = rail.children[to] as HTMLElement | undefined;
    if (!card) return;
    setCurrent(to);
    glidingTo.current = smooth ? to : null;
    rail.scrollTo?.({
      left: card.offsetLeft - (rail.clientWidth - card.offsetWidth) / 2,
      behavior: smooth ? scrollMotion() : 'auto',
    });
  }, []);

  useLayoutEffect(() => {
    goTo(first.index, { smooth: false });
    if (first.focus) ref.current?.children[first.index]?.querySelector<HTMLElement>('[data-computer-details]')?.focus();
  }, [first, goTo]);

  // Whether the rail still follows the machine just made: until the reader
  // scrolls, swipes or steps it elsewhere.
  const following = useRef(false);
  useLayoutEffect(() => {
    following.current = !!arrivingId;
  }, [arrivingId]);
  const placed = useRef<string | null>(null);
  useLayoutEffect(() => {
    if (!following.current || arrivingIndex < 0) return;
    const first = placed.current !== arrivingId;
    placed.current = arrivingId;
    goTo(arrivingIndex, { smooth: !first });
  }, [arrivingId, arrivingIndex, goTo]);

  // The card stepped away from goes inert, which drops focus to the page and
  // with it the next arrow key: focus moves to the same control on the card
  // stepped to, else its first, else the rail itself.
  useLayoutEffect(() => {
    const selector = focusNext.current;
    if (!selector) return;
    focusNext.current = null;
    const card = ref.current?.children[active];
    const control =
      card?.querySelector<HTMLElement>(`${selector}:not(:disabled)`) ?? card?.querySelector<HTMLElement>('button:not(:disabled)');
    (control ?? ref.current)?.focus({ preventScroll: true });
  }, [active]);

  // Whichever card is nearest the middle once the scroll has moved.
  const onScroll = () => {
    cancelAnimationFrame(frame.current);
    frame.current = requestAnimationFrame(() => {
      const rail = ref.current;
      if (!rail) return;
      const middle = rail.scrollLeft + rail.clientWidth / 2;
      let nearest = 0;
      let distance = Infinity;
      Array.from(rail.children).forEach((child, i) => {
        const card = child as HTMLElement;
        const d = Math.abs(card.offsetLeft + card.offsetWidth / 2 - middle);
        if (d < distance) {
          distance = d;
          nearest = i;
        }
      });
      if (glidingTo.current !== null) {
        if (nearest !== glidingTo.current) return;
        glidingTo.current = null;
      }
      setCurrent(nearest);
    });
  };
  // A reader's own scroll takes over from a glide.
  const onTakeOver = () => {
    glidingTo.current = null;
    following.current = false;
  };
  useEffect(() => () => cancelAnimationFrame(frame.current), []);

  const onKeyDown = (e: KeyboardEvent) => {
    if (e.key !== 'ArrowLeft' && e.key !== 'ArrowRight') return;
    e.preventDefault();
    const to = Math.max(0, Math.min(count - 1, active + (e.key === 'ArrowLeft' ? -1 : 1)));
    if (to === active) return;
    const from = e.target instanceof Element ? e.target : null;
    focusNext.current = from?.closest('[data-computer-power]') ? '[data-computer-power]' : '[data-computer-details]';
    following.current = false;
    goTo(to);
  };

  return { ref, active, goTo, onScroll, onTakeOver, onKeyDown };
}
