import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { isNearBottom } from '../../utils/scrollHelpers';

// Scroll/pin tuning. Distance from the bottom (px) still counted as "at bottom";
// settle window the pin re-applies through as async media expands; fallback for
// engines without a `scrollend` event.
const NEAR_BOTTOM_PX = 120;
const SETTLE_QUIET_MS = 1500;
const SETTLE_HARD_CAP_MS = 8000;
const SCROLLEND_FALLBACK_MS = 600;

/** Chat transcript scroll controller + tab scroll memory (carved out of
 * ChatView, 5.9c): bottom pin with async-settle re-apply, streaming follow,
 * thread-entry restore, jump-to-latest pill, and per-tab scroll memory. */
export function useChatScroll({ activeAgentId, messages, isActive, isActiveRef, isLoadingHistory, currentThreadId, threadId }: {
  activeAgentId: string;
  messages: unknown[];
  isActive: boolean;
  isActiveRef: { current: boolean };
  isLoadingHistory: boolean;
  currentThreadId: string;
  threadId: string;
}) {
  const scrollAreaRef = useRef<HTMLDivElement>(null);
  const subagentScrollAreaRef = useRef<HTMLDivElement>(null);

  // --- Scroll position memory for tab switching ---
  // Stores scrollTop per agentId so switching tabs preserves position
  const scrollPositionsRef = useRef<Record<string, number>>({});
  const activeAgentIdRef = useRef(activeAgentId);
  activeAgentIdRef.current = activeAgentId;
  // Flag to skip subagent auto-scroll when restoring a saved position
  const skipSubagentAutoScrollRef = useRef(false);

  // Helper: get the scrollable container from a ScrollArea ref
  const getScrollContainer = useCallback((ref: React.RefObject<HTMLDivElement | null>): HTMLElement | null => {
    if (!ref?.current) return null;
    return ref.current.querySelector('[data-radix-scroll-area-viewport]') ||
           ref.current.querySelector('.overflow-auto') ||
           ref.current;
  }, []);

  // Save scroll position of the currently active tab
  const saveScrollPosition = useCallback(() => {
    const currentId = activeAgentIdRef.current;
    const ref = currentId === 'main' ? scrollAreaRef : subagentScrollAreaRef;
    const container = getScrollContainer(ref);
    if (container) {
      scrollPositionsRef.current[currentId] = container.scrollTop;
    }
  }, [getScrollContainer]);

  // Restore scroll position after the new tab mounts
  useEffect(() => {
    const savedPosition = scrollPositionsRef.current[activeAgentId];
    if (savedPosition == null) return;

    // requestAnimationFrame waits for DOM commit + layout
    requestAnimationFrame(() => {
      const ref = activeAgentId === 'main' ? scrollAreaRef : subagentScrollAreaRef;
      const container = getScrollContainer(ref);
      if (container) {
        // Mark as programmatic so the main-tab scroll listener doesn't treat
        // this restore as a user scroll (which would cancel the pin / save).
        programmaticScrollRef.current = true;
        container.scrollTop = savedPosition;
        requestAnimationFrame(() =>
          requestAnimationFrame(() => {
            programmaticScrollRef.current = false;
          }),
        );
      }
    });
  }, [activeAgentId, getScrollContainer]);

  // ==========================================================================
  // Chat transcript scroll controller
  // Reliable land-at-bottom that survives async content (charts/code/images)
  // expanding after the initial scroll, plus a jump-to-latest affordance.
  // See utils/scrollHelpers.
  // ==========================================================================

  // "Near bottom" trackers (used by streaming follow + the pin controller).
  const isNearBottomRef = useRef(true);
  const isSubagentNearBottomRef = useRef(true);

  // Pin controller state.
  type PinTarget = { mode: 'bottom' };
  const pinTargetRef = useRef<PinTarget | null>(null);
  const programmaticScrollRef = useRef(false);
  const settleQuietTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const settleHardCapRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const reapplyRafRef = useRef<number | null>(null);
  const restoredForThreadRef = useRef<string | null>(null);
  // Streaming auto-follow's deferred scroll, and the entry-restore frame —
  // tracked so a thread switch / unmount cancels a pending scroll instead of
  // yanking a now-stale view.
  const streamFollowTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const entryRestoreRafRef = useRef<number | null>(null);

  // Jump-to-latest pill.
  const messagesLenRef = useRef(0);
  messagesLenRef.current = messages.length;
  const pillBaselineLenRef = useRef(0);
  const [jumpPill, setJumpPill] = useState<{ visible: boolean; hasNew: boolean; newCount: number }>({
    visible: false,
    hasNew: false,
    newCount: 0,
  });
  const setPillState = useCallback((next: { visible: boolean; hasNew: boolean; newCount: number }) => {
    setJumpPill((prev) =>
      prev.visible === next.visible && prev.hasNew === next.hasNew && prev.newCount === next.newCount
        ? prev
        : next,
    );
  }, []);
  const userMsgCount = useMemo(
    () => (messages as Array<{ role?: string }>).filter((m) => m?.role === 'user').length,
    [messages],
  );

  // Wrap a programmatic scroll so the scroll listener doesn't mistake it for a
  // user scroll (which cancels the pin). Smooth scrolls clear on `scrollend`
  // (600ms fallback for engines without it); instant clears after the scroll
  // event flushes (double rAF).
  const withProgrammaticScroll = useCallback(
    (fn: () => void, behavior: 'auto' | 'smooth' = 'auto') => {
      programmaticScrollRef.current = true;
      fn();
      if (behavior === 'smooth') {
        const c = getScrollContainer(scrollAreaRef);
        let cleared = false;
        const clear = () => {
          if (cleared) return;
          cleared = true;
          c?.removeEventListener('scrollend', clear);
          programmaticScrollRef.current = false;
        };
        c?.addEventListener('scrollend', clear, { once: true });
        setTimeout(clear, SCROLLEND_FALLBACK_MS);
      } else {
        requestAnimationFrame(() =>
          requestAnimationFrame(() => {
            programmaticScrollRef.current = false;
          }),
        );
      }
    },
    [getScrollContainer],
  );

  // The growing content node inside the fixed-height Radix viewport. The viewport
  // height is fixed (h-full); only its content grows as async media expands, so
  // that is what the ResizeObserver must watch.
  const getScrollContent = useCallback(
    (c: HTMLElement): HTMLElement =>
      c.querySelector<HTMLElement>('.max-w-3xl') ?? (c.firstElementChild as HTMLElement) ?? c,
    [],
  );

  const clearSettleTimers = useCallback(() => {
    if (settleQuietTimerRef.current) {
      clearTimeout(settleQuietTimerRef.current);
      settleQuietTimerRef.current = null;
    }
    if (settleHardCapRef.current) {
      clearTimeout(settleHardCapRef.current);
      settleHardCapRef.current = null;
    }
  }, []);

  // Arm the settle window: re-pin while content keeps growing, give up after a
  // 1.5s quiet window (reset on each settle resize) or an 8s hard cap.
  const armSettleTimers = useCallback(() => {
    if (settleQuietTimerRef.current) clearTimeout(settleQuietTimerRef.current);
    settleQuietTimerRef.current = setTimeout(() => {
      // Quiet window elapsed — the settle session is over. Tear down BOTH timers
      // so the next pin session arms a fresh hard cap; otherwise it inherits this
      // session's stale (shortened or already-elapsed) one and gives up early.
      pinTargetRef.current = null;
      settleQuietTimerRef.current = null;
      if (settleHardCapRef.current) {
        clearTimeout(settleHardCapRef.current);
        settleHardCapRef.current = null;
      }
    }, SETTLE_QUIET_MS);
    if (!settleHardCapRef.current) {
      settleHardCapRef.current = setTimeout(() => {
        pinTargetRef.current = null;
        settleHardCapRef.current = null;
        if (settleQuietTimerRef.current) {
          clearTimeout(settleQuietTimerRef.current);
          settleQuietTimerRef.current = null;
        }
      }, SETTLE_HARD_CAP_MS);
    }
  }, []);

  const pinToBottom = useCallback(
    (behavior: 'auto' | 'smooth' = 'auto') => {
      const c = getScrollContainer(scrollAreaRef);
      if (!c) return;
      pinTargetRef.current = { mode: 'bottom' };
      isNearBottomRef.current = true;
      pillBaselineLenRef.current = messagesLenRef.current;
      setPillState({ visible: false, hasNew: false, newCount: 0 });
      withProgrammaticScroll(() => c.scrollTo({ top: c.scrollHeight, behavior }), behavior);
      armSettleTimers();
    },
    [getScrollContainer, withProgrammaticScroll, armSettleTimers, setPillState],
  );

  // Re-apply the bottom pin (rAF-coalesced); called by the ResizeObserver each
  // time content settles, so async media finishing layout can't strand the user
  // mid-thread.
  const reapplyPin = useCallback(() => {
    if (reapplyRafRef.current != null) return;
    reapplyRafRef.current = requestAnimationFrame(() => {
      reapplyRafRef.current = null;
      const c = getScrollContainer(scrollAreaRef);
      if (!pinTargetRef.current || !c) return;
      withProgrammaticScroll(() => c.scrollTo({ top: c.scrollHeight }), 'auto');
      armSettleTimers();
    });
  }, [getScrollContainer, withProgrammaticScroll, armSettleTimers]);

  // Scroll listener + settle-aware ResizeObserver.
  // Re-attaches when activeAgentId changes (ScrollArea remounts on tab switch).
  useEffect(() => {
    const isMain = activeAgentId === 'main';
    const ref = isMain ? scrollAreaRef : subagentScrollAreaRef;
    const nearBottomRef = isMain ? isNearBottomRef : isSubagentNearBottomRef;
    const c = getScrollContainer(ref);
    if (!c) return;

    // Reset to near-bottom when switching tabs
    nearBottomRef.current = true;

    const handleScroll = () => {
      nearBottomRef.current = isNearBottom(
        { scrollTop: c.scrollTop, scrollHeight: c.scrollHeight, clientHeight: c.clientHeight },
        NEAR_BOTTOM_PX,
      );
      if (!isMain) return;
      if (programmaticScrollRef.current) return; // ignore our own scrolls
      // A genuine user scroll takes control away from the pin controller.
      pinTargetRef.current = null;
      clearSettleTimers();
      // Update jump-to-latest pill.
      const atBottom = nearBottomRef.current;
      setJumpPill((prev) => {
        if (atBottom) {
          return prev.visible || prev.hasNew ? { visible: false, hasNew: false, newCount: 0 } : prev;
        }
        if (prev.visible) return prev; // keep hasNew/newCount once shown
        pillBaselineLenRef.current = messagesLenRef.current;
        return { visible: true, hasNew: false, newCount: 0 };
      });
    };
    c.addEventListener('scroll', handleScroll, { passive: true });

    // A real user gesture (wheel / touch) reclaims scroll control even mid
    // programmatic smooth-scroll. Without this, those scroll events are flagged
    // programmatic and ignored above, so the pin keeps yanking against the user.
    const handleUserIntent = () => {
      if (!isMain) return;
      programmaticScrollRef.current = false;
      pinTargetRef.current = null;
      clearSettleTimers();
    };
    c.addEventListener('wheel', handleUserIntent, { passive: true });
    c.addEventListener('touchstart', handleUserIntent, { passive: true });

    // While a pin target is set, re-apply it whenever the transcript grows
    // (charts/code/images finishing layout) — the fix for landing mid-thread.
    let ro: ResizeObserver | null = null;
    if (isMain) {
      ro = new ResizeObserver(() => {
        if (pinTargetRef.current) reapplyPin();
      });
      ro.observe(getScrollContent(c));
    }
    return () => {
      c.removeEventListener('scroll', handleScroll);
      c.removeEventListener('wheel', handleUserIntent);
      c.removeEventListener('touchstart', handleUserIntent);
      ro?.disconnect();
      if (reapplyRafRef.current != null) {
        cancelAnimationFrame(reapplyRafRef.current);
        reapplyRafRef.current = null;
      }
    };
  }, [activeAgentId, getScrollContainer, getScrollContent, reapplyPin, clearSettleTimers]);

  // Auto-scroll main chat to bottom when messages change, but only if the user is
  // near the bottom and the pin controller isn't currently owning the scroll.
  useEffect(() => {
    if (pinTargetRef.current) return; // pin controller owns scroll during settle
    if (!isNearBottomRef.current) {
      // User is reading earlier turns — surface "N new" instead of yanking them down.
      const delta = messagesLenRef.current - pillBaselineLenRef.current;
      if (delta > 0) {
        setJumpPill((prev) => (prev.visible ? { visible: true, hasNew: true, newCount: delta } : prev));
      }
      return;
    }
    const c = getScrollContainer(scrollAreaRef);
    if (!c) return;
    if (streamFollowTimerRef.current) clearTimeout(streamFollowTimerRef.current);
    streamFollowTimerRef.current = setTimeout(() => {
      streamFollowTimerRef.current = null;
      // Re-check at fire time: if a pin took over or the user scrolled up
      // between scheduling and firing, do not yank them to the bottom. Wrap as
      // programmatic so this scroll isn't misread as the user scrolling away.
      if (pinTargetRef.current || !isNearBottomRef.current) return;
      const el = getScrollContainer(scrollAreaRef);
      if (!el) return;
      withProgrammaticScroll(() => el.scrollTo({ top: el.scrollHeight, behavior: 'smooth' }), 'smooth');
    }, 0);
    return () => {
      if (streamFollowTimerRef.current) {
        clearTimeout(streamFollowTimerRef.current);
        streamFollowTimerRef.current = null;
      }
    };
  }, [messages, getScrollContainer, withProgrammaticScroll]);

  // Thread-entry restore — the core fix. Fires on the real "history is present"
  // signal (isLoadingHistory flips false), not on an empty/partial list, then
  // pins to bottom through the async settle window.
  useEffect(() => {
    if (!isActive) return;
    const tid = currentThreadId || threadId;
    if (!tid || tid === '__default__') return;
    if (isLoadingHistory) return;
    if (restoredForThreadRef.current === tid) return;
    restoredForThreadRef.current = tid;
    entryRestoreRafRef.current = requestAnimationFrame(() => {
      entryRestoreRafRef.current = null;
      // The instance may have gone inactive (cached/hidden) before this frame.
      if (!isActiveRef.current) return;
      pinToBottom('auto');
    });
    return () => {
      if (entryRestoreRafRef.current != null) {
        cancelAnimationFrame(entryRestoreRafRef.current);
        entryRestoreRafRef.current = null;
      }
    };
  }, [isActive, isLoadingHistory, currentThreadId, threadId, pinToBottom, isActiveRef]);

  // Cleanup pending scroll timers/rAF on unmount.
  useEffect(() => {
    return () => {
      if (settleQuietTimerRef.current) clearTimeout(settleQuietTimerRef.current);
      if (settleHardCapRef.current) clearTimeout(settleHardCapRef.current);
      if (reapplyRafRef.current != null) cancelAnimationFrame(reapplyRafRef.current);
      if (streamFollowTimerRef.current) clearTimeout(streamFollowTimerRef.current);
      if (entryRestoreRafRef.current != null) cancelAnimationFrame(entryRestoreRafRef.current);
    };
  }, []);

  return {
    scrollAreaRef,
    subagentScrollAreaRef,
    getScrollContainer,
    withProgrammaticScroll,
    pinToBottom,
    saveScrollPosition,
    jumpPill,
    userMsgCount,
    scrollPositionsRef,
    skipSubagentAutoScrollRef,
    activeAgentIdRef,
    isNearBottomRef,
    isSubagentNearBottomRef,
    restoredForThreadRef,
  };
}
