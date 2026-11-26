import { useCallback, useEffect, useRef, useState } from 'react';

type Hover = { onPointerEnter: () => void; onPointerLeave: () => void };

export interface SidebarPeek {
  /** The hidden panel is over the page. */
  open: boolean;
  /** For the toggle, which brings the panel in; absent while it is shown. */
  toggleHover?: Hover;
  /** For the panel, and the top bar's corner over it, while it peeks. */
  panelHover?: Hover;
}

/**
 * Hidden, resting the pointer on the toggle slides the panel in over the page,
 * usable as it is, and leaving it slides it away; nothing else brings it up,
 * so it stays out of the way until asked for. The toggle lives in the top
 * bar and the panel in the sidebar, so the shell holds this for both.
 */
export function useSidebarPeek(hidden: boolean): {
  peek: SidebarPeek;
  /** The panel, and the top bar's corner that holds its toggle. */
  panelRef: React.RefObject<HTMLElement | null>;
  cornerRef: React.RefObject<HTMLDivElement | null>;
} {
  const panelRef = useRef<HTMLElement>(null);
  const cornerRef = useRef<HTMLDivElement>(null);
  const [peeking, setPeeking] = useState(false);
  const [wasHidden, setWasHidden] = useState(hidden);
  if (wasHidden !== hidden) {
    setWasHidden(hidden);
    setPeeking(false);
  }
  const open = hidden && peeking;

  const timer = useRef<number | undefined>(undefined);
  const clear = useCallback(() => window.clearTimeout(timer.current), []);
  useEffect(() => clear, [clear]);

  // A brief rest, so a pointer crossing the toggle on its way elsewhere opens nothing.
  const arm = useCallback(() => {
    clear();
    timer.current = window.setTimeout(() => setPeeking(true), 150);
  }, [clear]);

  // Leaving the panel closes it, unless the pointer is still over the panel
  // or its corner, or something in it is still in use: a menu (portalled
  // outside it, so reaching the menu leaves the panel; a context menu's
  // trigger is marked by no attribute, so the menu itself is what counts), a
  // field being typed in, a resize. Those hold it until they end. Where the
  // pointer is is asked, not tracked from enter and leave: one control moving
  // under a still pointer, or the toggle left for the rest of the corner,
  // sends a leave with no enter to answer it.
  const scheduleClose = useCallback(() => {
    clear();
    const check = () => {
      const panel = panelRef.current;
      const active = document.activeElement;
      const held =
        !!panel &&
        (panel.matches(':hover') ||
          !!cornerRef.current?.matches(':hover') ||
          document.querySelector('[role="menu"]') != null ||
          (active instanceof HTMLElement && panel.contains(active) && active.matches('input, textarea, [contenteditable="true"]')) ||
          document.body.classList.contains('sidebar-resizing'));
      if (held) timer.current = window.setTimeout(check, 280);
      else setPeeking(false);
    };
    timer.current = window.setTimeout(check, 280);
  }, [clear]);

  // A press elsewhere on the page, or Escape, closes it at once.
  useEffect(() => {
    if (!open) return;
    const onPointerDown = (e: PointerEvent) => {
      const target = e.target as Element | null;
      if (!target || panelRef.current?.contains(target) || cornerRef.current?.contains(target)) return;
      if (target.closest('[data-radix-popper-content-wrapper], [role="menu"], [role="dialog"]')) return;
      setPeeking(false);
    };
    const onKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape' && !e.defaultPrevented) setPeeking(false);
    };
    document.addEventListener('pointerdown', onPointerDown, true);
    document.addEventListener('keydown', onKeyDown);
    return () => {
      document.removeEventListener('pointerdown', onPointerDown, true);
      document.removeEventListener('keydown', onKeyDown);
    };
  }, [open]);

  const stay: Hover = { onPointerEnter: clear, onPointerLeave: scheduleClose };
  return {
    peek: {
      open,
      toggleHover: !hidden ? undefined : open ? stay : { onPointerEnter: arm, onPointerLeave: clear },
      panelHover: open ? stay : undefined,
    },
    panelRef,
    cornerRef,
  };
}
