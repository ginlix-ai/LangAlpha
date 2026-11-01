import React, { useEffect, useEffectEvent } from 'react';
import { HeldChildren } from '@/components/ui/Disclosure';
import { Popover, PopoverAnchor, PopoverContent } from '@/components/ui/popover';
import { DURATION } from '@/lib/motion';
import { PIN_SIZE } from './pinPlaces';
import { useKeptWhileFading } from './useKeptWhileFading';

/**
 * A box that stopped being the current one is dropped once its exit has
 * played. Radix unmounts the content itself at the exit's end, so this only
 * has to come after that; it is twice the beat so a busy frame cannot cut the
 * exit short.
 */
const LEAVE_MS = DURATION.exit * 2000;

type Point = { x: number; y: number };
type OutsideEvent = Parameters<NonNullable<React.ComponentProps<typeof PopoverContent>['onInteractOutside']>>[0];

/** A press or focus that landed on a pin: the pin's own click decides what opens. */
function onPin(e: OutsideEvent): boolean {
  return e.target instanceof Element && !!e.target.closest('[data-comment-pin]');
}

/**
 * A press or focus on the version picker, its menu included (portalled out of
 * the panel): the live file stays under a version picked, and the open box
 * with it, so the words in it are not lost to the pick.
 */
function onVersionPicker(e: OutsideEvent): boolean {
  return e.target instanceof Element && !!e.target.closest('[data-artifact-version-picker]');
}

function samePoint(a: Point, b: Point): boolean {
  return a.x === b.x && a.y === b.y;
}

// Radix's collisionPadding and sideOffset below.
const EDGE = 8;
const GAP = 8;

/**
 * Whether the box fits beside its pin on either side; Radix flips it to the
 * one that has room. Beside a pin it can only move up and down, so with room
 * on neither side it would run off the frame's edge: below the pin it can
 * also slide along the row to stay inside.
 */
function fitsBeside(at: Point, width: number, boundary: HTMLElement | null): boolean {
  if (!boundary) return true;
  const right = boundary.clientWidth - (at.x + PIN_SIZE + GAP) - EDGE;
  const left = at.x - GAP - EDGE;
  return Math.max(left, right) >= width;
}

export interface CommentPopoverProps {
  /** The current box. False while it leaves, holding what it showed and where. */
  open: boolean;
  /** Where its pin's tail is, or null while the page has not said; the box stays where it last was. */
  point: Point | null;
  narrow: boolean;
  boundary: HTMLElement | null;
  width: number;
  label: string;
  /** A card takes focus on its own box rather than on its first button, which a stray Enter would press. */
  focusSelf: boolean;
  /** Plays the one-shot ring that says "here it is", for a card the chat brought forward. */
  arrival: boolean;
  /** Another box took this one's place, so focus is not handed back when it goes. */
  replaced: boolean;
  /**
   * The current box is a new comment being written: Esc leaves it as its
   * own close does, and words written in it are not let go for a press or
   * focus elsewhere (`onAskDiscard` asks instead).
   */
  draft?: { words: boolean; onLeave: () => void; onAskDiscard: () => void } | null;
  onDismiss: () => void;
  /** Its exit has played; whoever holds it can let it go. */
  onGone: () => void;
  children: React.ReactNode;
}

/**
 * One box over the page: a new comment being written, or a comment's card.
 * Each open target has its own, so a box that closes plays its exit where it
 * was, with what it showed, while the next one opens at its own pin. One box
 * shared between targets lost both the moment its target went: it emptied
 * and its anchor fell to the frame's corner, where the exit played on a
 * sliver.
 *
 * The box comes out of its pin and goes back into it: `.pop-in` scales about
 * the corner Radix places beside the anchor.
 */
export function CommentPopover({
  open, point, narrow, boundary, width, label, focusSelf, arrival, replaced, draft, onDismiss, onGone, children,
}: CommentPopoverProps): React.ReactElement | null {
  const at = useKeptWhileFading(open ? point : null, samePoint);

  const gone = useEffectEvent(onGone);
  useEffect(() => {
    if (open) return;
    const timer = setTimeout(() => gone(), LEAVE_MS);
    return () => clearTimeout(timer);
  }, [open]);

  if (!at) return null;
  return (
    <Popover open={open} onOpenChange={(next) => { if (!next && open) onDismiss(); }}>
      <PopoverAnchor asChild>
        {/* The pin's box: a bubble whose tail is the point, its body up and to the right. */}
        <div className="page-comments-anchor" style={{ left: at.x, top: at.y - PIN_SIZE }} />
      </PopoverAnchor>
      <PopoverContent
        side={narrow || !fitsBeside(at, width, boundary) ? 'bottom' : 'right'}
        align="start"
        sideOffset={GAP}
        collisionBoundary={boundary}
        collisionPadding={EDGE}
        hideWhenDetached
        updatePositionStrategy="always"
        aria-label={label}
        data-arrival={arrival || undefined}
        className="page-comment-box flex flex-col overflow-hidden rounded-xl p-0 shadow-lg"
        style={{
          width,
          maxHeight: 'min(30rem, var(--radix-popover-content-available-height))',
          backgroundColor: 'var(--color-bg-popover)',
          borderColor: 'var(--color-border-default)',
          transformOrigin: 'var(--radix-popover-content-transform-origin)',
        }}
        onOpenAutoFocus={focusSelf ? (e) => {
          e.preventDefault();
          if (e.target instanceof HTMLElement) e.target.focus({ preventScroll: true });
        } : undefined}
        onCloseAutoFocus={(e) => { if (replaced) e.preventDefault(); }}
        onEscapeKeyDown={draft ? (e) => {
          e.preventDefault();
          draft.onLeave();
        } : undefined}
        onPointerDownOutside={(e) => {
          if (onVersionPicker(e)) e.preventDefault();
          else if (draft?.words) {
            e.preventDefault();
            draft.onAskDiscard();
          }
        }}
        // Words already written are not thrown away for focus going
        // elsewhere in the app. Focus going into the page never lands
        // here: no browser reports it on the frame, so a press there
        // reaches the layer only as the page's `page:press`.
        onFocusOutside={(e) => {
          if (onVersionPicker(e) || draft?.words) e.preventDefault();
        }}
        onInteractOutside={(e) => { if (onPin(e)) e.preventDefault(); }}
      >
        <HeldChildren held={!open}>{children}</HeldChildren>
      </PopoverContent>
    </Popover>
  );
}
