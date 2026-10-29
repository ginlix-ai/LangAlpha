import { useEffect, useEffectEvent, useRef, type ReactNode, type RefObject } from 'react';
import { Dialog } from 'react-aria-components';
import { Popover } from './aria-popover';
import { useIsMobile } from '@/hooks/useIsMobile';
import { lastInputWasPointer } from '@/lib/inputModality';
import { cn } from '@/lib/utils';

/** A row of a composer popover's list. */
export const MENU_ROW = cn(
  // The list's p-1 plus px-2 puts each icon under the search field's, 12px in
  // from the panel edge.
  'flex cursor-default select-none items-center gap-2 rounded-sm px-2 py-1.5 text-[0.8125rem] text-(--color-text-primary) outline-hidden',
  // The tint every floating list marks its row with (dropdown-menu's
  // ITEM_HIGHLIGHT). The current row takes the check alone, never the tint, so
  // the row under the pointer is the only one that is lit.
  'data-focused:bg-accent/15 data-hovered:bg-accent/15',
  'data-focus-visible:outline-current',
);

/** A hairline between sections, drawn only above a section that follows
 *  another, so a search that empties the first section never leaves a rule on
 *  top. It runs edge to edge through the list's padding, as a menu separator
 *  does. */
export const MENU_SECTION = '[&+&]:-mx-1 [&+&]:mt-1 [&+&]:border-t [&+&]:border-(--color-border-muted) [&+&]:px-1 [&+&]:pt-1';

/**
 * The floating panel a composer pill opens.
 *
 * Non-modal, so the transcript keeps scrolling and the rest of the page
 * answers while it is open. Blur closes it when focus moves to another
 * control, but react-aria ignores focus falling to the page, so a press on
 * something that takes no focus is caught here; the pill's own press toggles
 * it. Its content unmounts when it closes, so whatever the content holds (a
 * query, a tab) starts over on the next opening.
 */
export function ComposerPopover({
  triggerRef, isOpen, onOpenChange, direction, align, container, label, className, children,
}: {
  triggerRef: RefObject<HTMLElement | null>;
  isOpen: boolean;
  onOpenChange: (open: boolean) => void;
  direction: 'up' | 'down';
  /** The pill edge the panel lines up with. */
  align: 'start' | 'end';
  /** The composer. A phone portals into it so the panel can't escape the sheet. */
  container?: HTMLElement | null;
  /** The panel's accessible name. */
  label: string;
  /** Width, chiefly. */
  className?: string;
  children: ReactNode;
}) {
  const isMobile = useIsMobile();
  const popoverRef = useRef<HTMLElement>(null);

  const pressedOutside = useEffectEvent((target: Node) => {
    if (popoverRef.current?.contains(target) || triggerRef.current?.contains(target)) return;
    onOpenChange(false);
  });
  useEffect(() => {
    if (!isOpen) return;
    const onPointerDown = (e: PointerEvent) => pressedOutside(e.target as Node);
    document.addEventListener('pointerdown', onPointerDown, true);
    return () => document.removeEventListener('pointerdown', onPointerDown, true);
  }, [isOpen]);

  // The pill can fold into the overflow menu or leave the toolbar while the
  // panel is open, which unmounts it with the open state still set. Closing on
  // the way out keeps it from springing open when the pill comes back.
  const closeIfOpen = useEffectEvent(() => {
    if (isOpen) onOpenChange(false);
  });
  useEffect(() => () => closeIfOpen(), []);

  return (
    <Popover
      ref={popoverRef}
      triggerRef={triggerRef}
      isOpen={isOpen}
      onOpenChange={onOpenChange}
      isNonModal
      placement={direction === 'down' ? `bottom ${align}` : `top ${align}`}
      offset={6}
      UNSTABLE_portalContainer={isMobile && container ? container : undefined}
      className={cn('flex max-w-[calc(100vw-32px)] flex-col overflow-hidden', className)}
    >
      {/* A click here still bubbles through the portal to the composer, which
          answers any click by focusing its textarea, and the panel closes
          when focus leaves it. */}
      <Dialog
        aria-label={label}
        className="flex min-h-0 flex-col outline-hidden"
        onClick={(e) => e.stopPropagation()}
      >
        {children}
      </Dialog>
    </Popover>
  );
}

/**
 * Where focus goes after a pick that closes the panel. A click on a row is a
 * click in the composer, which puts the caret in the draft; a keyboard pick
 * goes back to the pill, where the user left off. The draft takes focus once
 * the panel is shut, since until then the list keeps handing it back to the
 * row that was pressed.
 *
 * Returns the call to make on each pick, before closing.
 */
export function usePickToDraft(isOpen: boolean, draftRef?: RefObject<HTMLElement | null>) {
  const toDraft = useRef(false);
  useEffect(() => {
    if (isOpen || !toDraft.current) return;
    toDraft.current = false;
    draftRef?.current?.focus();
  }, [isOpen, draftRef]);
  return () => {
    toDraft.current = lastInputWasPointer();
  };
}

/**
 * Sends focus that lands on a menu itself on to its checked row, so arrowing
 * starts from the current choice. Opened by a click the browser made up (Enter
 * on the pill, a screen reader), react-aria holds each focus move for a frame
 * and drops any that finds focus already moved, and its move to the row can be
 * the one dropped, leaving focus on the menu.
 */
export function useFocusCheckedRow(menuRef: RefObject<HTMLElement | null>, enabled = true) {
  useEffect(() => {
    const menu = menuRef.current;
    if (!enabled || !menu) return;
    const onFocusIn = (e: FocusEvent) => {
      if (e.target === menu) menu.querySelector<HTMLElement>('[aria-checked=true]')?.focus();
    };
    menu.addEventListener('focusin', onFocusIn);
    return () => menu.removeEventListener('focusin', onFocusIn);
  }, [menuRef, enabled]);
}
