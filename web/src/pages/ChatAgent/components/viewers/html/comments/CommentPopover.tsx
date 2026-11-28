import React, { useEffectEvent, useLayoutEffect, useRef, useState } from 'react';
import { HeldChildren } from '@/components/ui/Disclosure';
import { Popover, PopoverAnchor, PopoverContent } from '@/components/ui/popover';
import { animate } from '@/lib/framer';
import { isImeComposing } from '@/lib/ime';
import { TWEEN } from '@/lib/motion';
import { prefersReducedMotion } from '@/lib/reducedMotion';
import { PIN_SIZE } from './pinPlaces';
import { useKeptWhileFading } from './useKeptWhileFading';

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

type Band = { top: number; bottom: number };
/** Where the box stands: its pin's tail, and the band it keeps clear. */
type Place = { at: Point; clear: Band | null };

function samePlace(a: Place, b: Place): boolean {
  const band = a.clear === b.clear || (!!a.clear && !!b.clear && a.clear.top === b.clear.top && a.clear.bottom === b.clear.bottom);
  return a.at.x === b.at.x && a.at.y === b.at.y && band;
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

/** The least room beside a band kept clear for the box to open there rather than beside its pin. */
const CLEAR_MIN = 180;

export interface CommentPopoverProps {
  /** Given the box's element while it is the current one, for what gives way to it. */
  boxRef?: React.RefObject<HTMLDivElement | null>;
  /** The current box. False while it leaves, holding what it showed and where. */
  open: boolean;
  /** Where its pin's tail is, or null while the page has not said; the box stays where it last was. */
  point: Point | null;
  narrow: boolean;
  /**
   * A band the box keeps clear, its top and bottom in the boundary's
   * coordinates: it opens under it, or over it, on the side with more room,
   * its far edge on its pin, so what the pin marks stays in view. Beside its
   * pin where neither side has room.
   */
  clear?: { top: number; bottom: number } | null;
  boundary: HTMLElement | null;
  width: number;
  label: string;
  /**
   * Where the focus goes as the box opens: a card's onto the box itself, not
   * its first button, which a stray Enter would press; a draft's onto its
   * field; none for a draft taken up again from where it was kept.
   */
  focus: 'self' | 'field' | 'none';
  /** Plays the one-shot ring that says "here it is", for a card the chat brought forward. */
  arrival: boolean;
  /** Another box took this one's place, so focus is not handed back when it goes. */
  replaced: boolean;
  /**
   * The current box is a new comment being written: words written in it are
   * not let go for a press or focus outside the box.
   */
  draft?: { words: boolean } | null;
  /** Fades and lets clicks through to the page under it: the add key is held to add a part the box may cover. */
  seeThrough?: boolean;
  onDismiss: () => void;
  /** Esc, which steps the layer back one layer (`CommentLayer`'s `escapeOneLayer`). */
  onEscape: () => void;
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
  open, point, narrow, clear, boundary, width, label, boxRef, focus, arrival, replaced, draft, seeThrough, onDismiss, onEscape, onGone, children,
}: CommentPopoverProps): React.ReactElement | null {
  // The pin and the band are kept together: the layer drops both once the
  // box stops being current, and a box that went back beside its pin for its
  // exit would leap there from under or over the band before it faded.
  const place = useKeptWhileFading(open && point ? { at: point, clear: clear ?? null } : null, samePlace);
  const at = place?.at ?? null;
  // The box has no trigger to hand the focus back to: it goes back to where
  // it was when the box opened, unless a press or focus elsewhere moved it on.
  const [returnTo] = useState(() => (typeof document === 'undefined' ? null : document.activeElement));
  const leftFor = useRef(false);

  // The box fades where it stands on framer's clock, the one its pin, outline
  // and marked words leave on, and is let go once the fade has played. Held
  // mounted for it (`forceMount`), so no CSS exit plays: one starts a frame
  // later than a framer one begun in the same commit, so the pin went first,
  // and the pop's shrink drew it toward the corner Radix placed it by.
  const content = useRef<HTMLDivElement>(null);
  const gone = useEffectEvent(onGone);
  useLayoutEffect(() => {
    const el = content.current;
    if (open) return;
    if (!el || prefersReducedMotion()) {
      gone();
      return;
    }
    const from = Number(getComputedStyle(el).opacity) || 1;
    const fade = animate(from, 0, { ...TWEEN.exit, onUpdate: (v) => { el.style.opacity = String(v); } });
    void fade.then(() => gone());
    return () => fade.stop();
  }, [open]);

  if (!at) return null;
  // Off a band kept clear, the box is placed against the band, ending at the
  // pin's far side, rather than against the pin: Radix still flips it and
  // keeps it inside.
  const band = place?.clear ?? null;
  const room = band && boundary
    ? { bottom: boundary.clientHeight - band.bottom - GAP - EDGE, top: band.top - GAP - EDGE }
    : null;
  const off: 'bottom' | 'top' | null = !room || !band || Math.max(room.bottom, room.top) < CLEAR_MIN ? null
    : room.bottom >= room.top ? 'bottom' : 'top';
  const anchor = off && band
    ? { left: at.x, width: PIN_SIZE, top: band.top, height: band.bottom - band.top }
    : { left: at.x, top: at.y - PIN_SIZE };
  return (
    <Popover open={open} onOpenChange={(next) => { if (!next && open) onDismiss(); }}>
      <PopoverAnchor asChild>
        {/* The pin's box: a bubble whose tail is the point, its body up and to the right. */}
        <div className="page-comments-anchor" style={anchor} />
      </PopoverAnchor>
      <PopoverContent
        ref={(el) => {
          content.current = el;
          if (boxRef) boxRef.current = el;
        }}
        forceMount
        side={off ?? (narrow || !fitsBeside(at, width, boundary) ? 'bottom' : 'right')}
        align={off ? 'end' : 'start'}
        sideOffset={GAP}
        collisionBoundary={boundary}
        collisionPadding={EDGE}
        hideWhenDetached
        updatePositionStrategy="always"
        aria-label={label}
        data-arrival={arrival || undefined}
        data-see-through={seeThrough || undefined}
        className="page-comment-box flex flex-col overflow-hidden rounded-xl p-0 shadow-lg"
        style={{
          width,
          maxHeight: 'min(30rem, var(--radix-popover-content-available-height))',
          backgroundColor: 'var(--color-bg-popover)',
          borderColor: 'var(--color-border-default)',
          transformOrigin: 'var(--radix-popover-content-transform-origin)',
        }}
        onOpenAutoFocus={focus === 'field' ? undefined : (e) => {
          e.preventDefault();
          if (focus === 'self' && e.target instanceof HTMLElement) e.target.focus({ preventScroll: true });
        }}
        onCloseAutoFocus={(e) => {
          e.preventDefault();
          if (replaced || leftFor.current) return;
          if (returnTo instanceof HTMLElement && returnTo !== document.body && returnTo.isConnected) returnTo.focus({ preventScroll: true });
        }}
        onEscapeKeyDown={(e) => {
          // Taken either way and closed here rather than by Radix, so one Esc
          // steps back one layer through `onEscape`. Esc ending an input
          // method's composition is the writer's, not a close. A box on its
          // way out has stepped back already.
          if (!open) return;
          e.preventDefault();
          if (!isImeComposing(e)) onEscape();
        }}
        // A press outside the box leaves words written as they are: on the
        // chat, a toolbar or a menu it is about something else. One on the
        // page asks whether to discard them, as a click away does in a design
        // tool, but that press reaches the layer as the page's own report
        // (`page:press`) and a pin's as its own click, never here.
        onPointerDownOutside={(e) => {
          if (onVersionPicker(e) || draft?.words) e.preventDefault();
        }}
        // Words already written are not thrown away for focus going
        // elsewhere in the app. Focus going into the page never lands
        // here: no browser reports it on the frame, so a press there
        // reaches the layer only as the page's `page:press`.
        onFocusOutside={(e) => {
          if (onVersionPicker(e) || draft?.words) e.preventDefault();
        }}
        onInteractOutside={(e) => {
          if (onPin(e)) e.preventDefault();
          else if (!e.defaultPrevented) leftFor.current = true;
        }}
      >
        <HeldChildren held={!open}>{children}</HeldChildren>
      </PopoverContent>
    </Popover>
  );
}
