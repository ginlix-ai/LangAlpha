import { useCallback, useEffect, useRef, useState } from 'react';
import { getIsMobileSnapshot } from '@/hooks/useIsMobile';
import { NAV_PIN_KEY, _sharedNav } from './navPin';

/** Nav-panel controller (carved out of ChatView, 5.9c): hover/pin/minimize
 * state shared across ChatView instances via _sharedNav. */
export function useNavPanel({ isMobile, isActiveRef }: {
  isMobile: boolean;
  isActiveRef: { current: boolean };
}) {
  // Navigation panel visibility (hover-triggered overlay, or docked when pinned)
  // Initialize from shared state so thread switches inherit the panel's open/closed state.
  // Pinned (desktop only) forces the panel visible from first paint.
  const initialNavOpen = _sharedNav.visible || (_sharedNav.pinned && !isMobile);
  const [navPanelVisible, setNavPanelVisible] = useState(initialNavOpen);
  const navPanelVisibleRef = useRef(initialNavOpen);
  const [navPinned, setNavPinned] = useState(_sharedNav.pinned);
  const navPinnedRef = useRef(_sharedNav.pinned);
  const navHideTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const navLockedRef = useRef(_sharedNav.locked);
  const contentAreaRef = useRef<HTMLDivElement>(null);
  const contentAreaWidthRef = useRef<number>(0);
  // True when the content area is too narrow for the docked push layout; a
  // pinned panel then stays visible but overlays without pushing content.
  const [contentNarrow, setContentNarrow] = useState(false);
  // Skip nav panel slide-in on mount if already open (inherited from previous thread or pinned).
  const skipNavAnimRef = useRef(initialNavOpen);
  useEffect(() => { skipNavAnimRef.current = false; return () => { if (navHideTimerRef.current) clearTimeout(navHideTimerRef.current); }; }, []);
  // Auto-close nav panel when content area shrinks below threshold (e.g., right panel opens)
  useEffect(() => {
    const container = contentAreaRef.current;
    if (!container) return;
    const observer = new ResizeObserver((entries: ResizeObserverEntry[]) => {
      const width = entries[0].contentRect.width;
      contentAreaWidthRef.current = width;
      // Skip auto-hide on mobile — hamburger controls nav drawer
      if (getIsMobileSnapshot()) return;
      // Skip when view is hidden (display:none reports width 0) to avoid
      // corrupting _sharedNav for the incoming active view.
      if (!isActiveRef.current) return;
      setContentNarrow(width < 1100);
      // Pinned panels never auto-collapse — they fall back to overlay-without-push instead.
      if (width < 1100 && navPanelVisibleRef.current && !navPinnedRef.current) {
        if (navHideTimerRef.current) clearTimeout(navHideTimerRef.current);
        navPanelVisibleRef.current = false;
        _sharedNav.visible = false;
        setNavPanelVisible(false);
      }
    });
    observer.observe(container);
    return () => observer.disconnect();
  }, [isActiveRef]);

  // Navigation panel hover handlers with 30s hide delay
  const handleNavEnter = useCallback(() => {
    if (navPinnedRef.current) return; // pinned panel ignores the hover dance
    if (navLockedRef.current) return; // locked after explicit minimize
    // Don't open if content area is too narrow (e.g., right panel consuming space)
    if ((contentAreaRef.current?.offsetWidth ?? Infinity) < 1100) return;
    if (navHideTimerRef.current) clearTimeout(navHideTimerRef.current);
    navPanelVisibleRef.current = true;
    _sharedNav.visible = true;
    setNavPanelVisible(true);
  }, []);

  const handleNavLeave = useCallback(() => {
    if (navPinnedRef.current) return; // pinned panel never auto-hides
    if (navLockedRef.current) return;
    navHideTimerRef.current = setTimeout(() => {
      if (!isActiveRef.current) return;
      navPanelVisibleRef.current = false;
      _sharedNav.visible = false;
      setNavPanelVisible(false);
    }, 30000);
  }, [isActiveRef]);

  const handleNavMinimize = useCallback(() => {
    if (navHideTimerRef.current) clearTimeout(navHideTimerRef.current);
    navLockedRef.current = true;
    navPanelVisibleRef.current = false;
    _sharedNav.visible = false;
    _sharedNav.locked = true;
    setNavPanelVisible(false);
  }, []);

  // Pin toggle: pin docks the panel open (persisted); unpin returns to hover mode.
  const handleTogglePin = useCallback(() => {
    const next = !navPinnedRef.current;
    navPinnedRef.current = next;
    _sharedNav.pinned = next;
    try {
      localStorage.setItem(NAV_PIN_KEY, String(next));
    } catch {
      // localStorage unavailable (private mode) — pin still works for the session
    }
    setNavPinned(next);
    if (next) {
      if (navHideTimerRef.current) clearTimeout(navHideTimerRef.current);
      navLockedRef.current = false;
      _sharedNav.locked = false;
      navPanelVisibleRef.current = true;
      _sharedNav.visible = true;
      setNavPanelVisible(true);
    } else {
      navPanelVisibleRef.current = false;
      _sharedNav.visible = false;
      setNavPanelVisible(false);
    }
  }, []);

  // Expand button explicitly unlocks and opens the panel
  const handleNavExpand = useCallback(() => {
    navLockedRef.current = false;
    _sharedNav.locked = false;
    if (navHideTimerRef.current) clearTimeout(navHideTimerRef.current);
    navPanelVisibleRef.current = true;
    _sharedNav.visible = true;
    setNavPanelVisible(true);
  }, []);

  // On view activation: inherit the shared nav state (see the become-active
  // effect in ChatView). Returns wantNavVisible so the caller's rAF can clear
  // the skip-animation flag it set here.
  const inheritNavOnActivate = useCallback((): boolean => {
    // Clear stale nav-hide timer from a previous activation period
    if (navHideTimerRef.current) clearTimeout(navHideTimerRef.current);
    // Reset nav lock — matches the old per-thread-change reset so the
    // hover trigger zone works again after a minimize + thread switch.
    navLockedRef.current = false;
    _sharedNav.locked = false;
    // Sync pin state — it may have been toggled in another instance.
    // Pinned forces visibility on desktop; mobile ignores it (drawer flow).
    navPinnedRef.current = _sharedNav.pinned;
    setNavPinned(_sharedNav.pinned);
    const wantNavVisible = _sharedNav.visible || (_sharedNav.pinned && !getIsMobileSnapshot());
    // Sync nav panel from shared state
    navPanelVisibleRef.current = wantNavVisible;
    setNavPanelVisible(wantNavVisible);
    // Skip slide-in animation if inheriting open state
    if (wantNavVisible) skipNavAnimRef.current = true;
    return wantNavVisible;
  }, []);

  return {
    navPanelVisible,
    navPinned,
    contentNarrow,
    contentAreaRef,
    navPanelVisibleRef,
    skipNavAnimRef,
    handleNavEnter,
    handleNavLeave,
    handleNavMinimize,
    handleTogglePin,
    handleNavExpand,
    inheritNavOnActivate,
  };
}
