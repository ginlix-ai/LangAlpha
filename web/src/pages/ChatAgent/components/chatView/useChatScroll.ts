import React, { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';
import { AT_BOTTOM_PX, NEAR_BOTTOM_PX, isNearBottom } from '../../utils/scrollHelpers';
import { findMessageElement, resolveScrollContent, resolveScrollViewport } from '../../utils/scrollDom';
import { scrollMemory } from '@/lib/scrollMemory';
import { ANCHORED_TOGGLE_EVENT } from '../../utils/anchoredToggle';
import { useLatestRef } from '@/hooks/useLatestRef';
import { TRANSCRIPT_STATUS_SELECTOR } from './TranscriptStatus';
import { INTENT_EVENTS, type StreamFollowControls, type TurnStart } from './streamFollow';
import { blockSize, createTranscriptFollow, type PinTarget, type TabPlace, type TranscriptFollow } from './transcriptFollow';
import { createTurnPin, type TurnPin } from './turnPin';

export type { PinTarget };

// How long a toggled row is held in place while its disclosure animates open
// or shut; longer than the fold spring, shorter than the next streamed chunk.
const TOGGLE_HOLD_MS = 1000;

/** The bubble at the viewport top and how far into it the view starts. With
 *  `orFirst`, a view that starts above every bubble (the top of the transcript)
 *  is placed by the first one, `delta` negative by the gap over it. */
function readPlace(c: HTMLElement, orFirst = false): { id: string; delta: number } | null {
  const bubbles = c.querySelectorAll<HTMLElement>('[data-message-id]');
  const top = c.getBoundingClientRect().top;
  let hit: HTMLElement | null = orFirst ? (bubbles[0] ?? null) : null;
  for (let lo = 0, hi = bubbles.length - 1; lo <= hi; ) {
    const mid = (lo + hi) >> 1;
    if (bubbles[mid].getBoundingClientRect().top <= top) {
      hit = bubbles[mid];
      lo = mid + 1;
    } else {
      hi = mid - 1;
    }
  }
  if (!hit?.dataset.messageId) return null;
  return { id: hit.dataset.messageId, delta: top - hit.getBoundingClientRect().top };
}

/** Where the reader was, for a thread: on a bubble, or at the end. */
interface ShownPlace {
  tid: string;
  place: 'bottom' | { id: string; delta: number };
}

// A replayed turn's bubbles, its steering replies included, are named by its
// turn index, so one a replay no longer renders is a turn a fork removed. Live
// bubbles are renamed by the replay, so their absence says nothing.
const REPLAYED_TURN_ID = /^history-(?:(?:user|assistant)-\d+|steering-user-\d+-\d+-\d+|assistant-steering-\d+-\d+)$/;

/**
 * The main transcript's scroll controller, over the follow both chat surfaces
 * share (transcriptFollow): a send's message pinned under the top (turnPin), a
 * thread opening where the reader left it (scrollMemory) or at its end, older
 * pages landing above the reader without moving them, a toggled row held
 * under the pointer, the jump-to-latest pill, and each tab's place across a
 * switch to a subagent and back.
 */
export function useChatScroll({
  activeAgentId,
  messages,
  isActive,
  isActiveRef,
  isLoadingHistory,
  historyLoadFailed,
  isStreaming,
  currentThreadId,
  threadId,
}: {
  activeAgentId: string;
  messages: unknown[];
  isActive: boolean;
  isActiveRef: { current: boolean };
  isLoadingHistory: boolean;
  /** The last replay of the thread failed, so its bubbles are missing for a
   *  reason that says nothing about the turns. */
  historyLoadFailed: boolean;
  /** A turn is open: the transcript grows on its own, so a reader at the end is carried along. */
  isStreaming: boolean;
  currentThreadId: string;
  threadId: string;
}) {
  const scrollAreaRef = useRef<HTMLDivElement>(null);
  // These latest-value refs are read by the observer, listeners, callbacks and
  // effects below, never by render. Each holds the last committed value,
  // written in a layout effect that runs ahead of this hook's own.
  const isLoadingHistoryRef = useLatestRef(isLoadingHistory);
  const activeAgentIdRef = useLatestRef(activeAgentId);
  // Resolved thread id for the cross-unmount scroll store (scrollMemory): a
  // ref, so the scroll listener always stamps the current thread without
  // re-binding. '__default__' (unresolved new thread) is never stored.
  const resolvedTid = currentThreadId || threadId;
  const memoryTidRef = useLatestRef(resolvedTid && resolvedTid !== '__default__' ? resolvedTid : null);
  const messagesLenRef = useLatestRef(messages.length);

  const getScrollContainer = useCallback(
    (ref: React.RefObject<HTMLDivElement | null>): HTMLElement | null => resolveScrollViewport(ref?.current ?? null),
    [],
  );

  // Jump-to-latest pill.
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

  // Whether the reader rides the end, and the place held for them: the
  // follow's own, kept here so they outlive the transcript remounting on a tab
  // switch and so ChatView and the minimap can read them.
  const isNearBottomRef = useRef(true);
  const pinTargetRef = useRef<PinTarget | null>(null);
  const followRef = useRef<TranscriptFollow | null>(null);
  const turnPinRef = useRef<TurnPin | null>(null);
  // Made on first use, not in render. A placement decides the pill: shown
  // unless it left the reader at the end.
  const getFollow = useCallback(() => (followRef.current ??= createTranscriptFollow({
    following: isNearBottomRef,
    pin: pinTargetRef,
    placed: (atEnd) => {
      pillBaselineLenRef.current = messagesLenRef.current;
      setPillState({ visible: !atEnd, hasNew: false, newCount: 0 });
    },
  })), [messagesLenRef, setPillState]);
  const getTurnPin = useCallback(() => (turnPinRef.current ??= createTurnPin()), []);
  /** The wrapper ChatView puts between the scroller and the padded transcript,
   *  which carries the pinned turn's floor. */
  const transcriptFloorRef = useCallback((el: HTMLDivElement | null) => getTurnPin().setFloor(el), [getTurnPin]);
  // A disclosure toggle holds the toggled row at the viewport position it had
  // when clicked, for as long as its animation resizes the transcript.
  const toggleHoldRef = useRef<{ el: HTMLElement; top: number; until: number } | null>(null);

  useLayoutEffect(() => {
    getFollow().setLive(isStreaming);
  }, [getFollow, isStreaming]);

  // --- Each tab's place, across a switch to another and back ---
  // A tab switch remounts the transcripts, so the place a tab was left at is
  // saved on the way out and put back by its follow when it attaches again.
  const scrollPositionsRef = useRef<Record<string, TabPlace>>({});
  const subagentScrollAreaRef = useRef<HTMLDivElement | null>(null);
  // The subagent tab's scroller and content, for its follow (useStreamFollow).
  // Its ScrollArea mounts only once its agent is known, which can be after the
  // tab is chosen, so they are state, resolved on the element.
  const [subagentPane, setSubagentPane] = useState<{ scroller: HTMLElement; content: HTMLElement } | null>(null);
  const subagentScrollRoot = useCallback((el: HTMLDivElement | null) => {
    subagentScrollAreaRef.current = el;
    const scroller = resolveScrollViewport(el);
    const content = scroller?.firstElementChild;
    setSubagentPane(scroller && content instanceof HTMLElement ? { scroller, content } : null);
  }, []);
  // A reader at the end is saved as there, by the band the thread's own
  // memory uses, so a reply that grew while they were away is followed from
  // its new end rather than reopened above it.
  const saveScrollPosition = useCallback(() => {
    const id = activeAgentIdRef.current;
    const c = getScrollContainer(id === 'main' ? scrollAreaRef : subagentScrollAreaRef);
    if (!c) return;
    const atEnd = isNearBottom({ scrollTop: c.scrollTop, scrollHeight: c.scrollHeight, clientHeight: c.clientHeight }, NEAR_BOTTOM_PX);
    scrollPositionsRef.current[id] = atEnd ? 'end' : c.scrollTop;
  }, [getScrollContainer, activeAgentIdRef]);
  /** The place tab `id` was left at, once. */
  const takeTabPlace = useCallback((id: string): TabPlace | null => {
    const top = scrollPositionsRef.current[id];
    delete scrollPositionsRef.current[id];
    return top ?? null;
  }, []);

  // --- Thread entry ---
  const restoredForThreadRef = useRef<string | null>(null);
  // Where a mid-thread reader was, by bubble, for a reload of the thread on
  // screen: the replay drops the bubbles in the render that starts it, so the
  // place is read while scrolling. reloadingRef marks the next restore as that
  // reload's.
  const readerPlaceRef = useRef<{ tid: string; id: string; delta: number } | null>(null);
  const reloadingRef = useRef(false);
  // Where the reader last was in the main transcript, by bubble or at its end.
  // An older page that lands while the transcript is not shown moves what sits
  // at its offset and cannot be measured, so that place is owed to the next
  // time the transcript is shown.
  const shownPlaceRef = useRef<ShownPlace | null>(null);
  const owedPlaceRef = useRef<ShownPlace | null>(null);
  // The entry-restore frame, tracked so a thread switch / unmount cancels a
  // pending scroll instead of yanking a now-stale view.
  const entryRestoreRafRef = useRef<number | null>(null);
  const visibilityRafRef = useRef<number | null>(null);
  // The thread whose entry restore has been applied, as state for render: the
  // older-history row waits on it. Until the restore lands the view sits at
  // the top of the transcript, which is where the row asks for a page.
  const [restoredTid, setRestoredTid] = useState<string | null>(null);

  /** The entry restore for this thread has landed, so an automatic scroll may move the view. */
  const entryRestoreSettled = useCallback(
    () => !memoryTidRef.current || restoredForThreadRef.current === memoryTidRef.current,
    [memoryTidRef],
  );
  /** The follow is carrying the reader, and the thread's entry is settled. */
  const isFollowing = useCallback(() => getFollow().carrying && entryRestoreSettled(), [getFollow, entryRestoreSettled]);

  // A jump to the end still gliding there. The pill hides at the click, but
  // the reader reaches the end only when the glide lands, and chrome that
  // grows on arrival (the composer's tray) waits for that.
  const [jumping, setJumping] = useState(false);
  const pinToBottom = useCallback(
    (behavior: 'auto' | 'smooth' = 'auto') => {
      const follow = getFollow();
      // A glide to a bottom re-read every frame, so a reply growing under it
      // moves where it lands rather than cutting it short. Starting it ends a
      // landing it replaces, and that one's jump with it, so the flag is set
      // after.
      if (!follow.place({ mode: 'bottom' }, { smooth: behavior === 'smooth', onEnd: () => setJumping(false) })) return;
      setJumping(follow.engine?.landing ?? false);
    },
    [getFollow],
  );

  // For a turn the reader starts: wherever they had scrolled, the turn is
  // theirs to watch, so the jump pill goes and the follow claims the view.
  // A send, an edit, a regenerate or a retry pins their message under the top
  // (turnPin); a steer adds to a turn already on screen and goes to its end,
  // as the pill does. A turn the main transcript is not on screen for leaves
  // its place alone.
  const rejoin = useCallback((start: TurnStart) => {
    if (activeAgentIdRef.current !== 'main' || !isActiveRef.current) return;
    const c = getScrollContainer(scrollAreaRef);
    const follow = getFollow();
    const engine = follow.engine;
    if (!c || !engine) return;
    // Placed anew, the reader is no longer held on a row they toggled.
    toggleHoldRef.current = null;
    if (start === 'steer') return follow.rejoin();
    follow.claim();
    engine.stop();
    const turnPin = getTurnPin();
    turnPin.start(c, start === 'restart');
    requestAnimationFrame(() => {
      if (follow.engine === engine) turnPin.settle(c, engine);
    });
  }, [activeAgentIdRef, isActiveRef, getScrollContainer, getFollow, getTurnPin]);

  // A minimap jump: the bubble under the viewport top, held through the settle
  // window. Anything short of the newest turn hands the reader the jump pill
  // and stops the follow, so a streaming turn can't carry them off it.
  const pinToMessage = useCallback(
    (id: string, behavior: 'auto' | 'smooth' = 'auto', isLatest = false) => {
      getFollow().place({ mode: 'anchor', id }, { smooth: behavior === 'smooth', newest: isLatest });
    },
    [getFollow],
  );

  // A link to a message further on (a hand-off's report), landed as the
  // minimap's jump lands, then focused, so the next Tab reaches what it holds.
  const revealMessage = useCallback((id: string) => getFollow().reveal(id), [getFollow]);

  // For a turn that finished under 'reply_start': the first line of the reply
  // under the viewport top (transcriptFollow), once the thread's entry is.
  const landOnReply = useCallback(
    (id: string) => {
      if (activeAgentIdRef.current !== 'main' || !isActiveRef.current || !entryRestoreSettled()) return;
      getFollow().landOnReply(id);
    },
    [activeAgentIdRef, isActiveRef, entryRestoreSettled, getFollow],
  );
  const controls = useMemo<StreamFollowControls>(() => ({ rejoin, landOnReply }), [rejoin, landOnReply]);

  // Bring a turn's deliverables deck into view as it unfolds. The deck cannot
  // do this for itself: its height animates over 260ms, and the observer below
  // re-applies the held place on every one of those growth frames, so a scroll
  // the deck set would be overwritten before it painted. Held as a place
  // instead, the unfold is followed by the very machinery that was overwriting
  // it, and the settle window releases it once growth stops.
  const revealFiles = useCallback(
    (id: string) => {
      const c = getScrollContainer(scrollAreaRef);
      const drawn = c?.querySelectorAll('[data-message-id]');
      const newest = !!c && !!drawn?.length && drawn[drawn.length - 1] === findMessageElement(c, id);
      getFollow().place({ mode: 'reveal', id }, { newest });
    },
    [getScrollContainer, getFollow],
  );

  const toTop = useCallback(() => getFollow().toTop(), [getFollow]);

  // An older page lands above the reader: hold the bubble at the viewport top
  // where it is, so the page grows the transcript upward out of sight instead
  // of pushing what they are reading down by its own height. `commit` renders
  // the page before it returns, so the place is read and applied around it in
  // one task, and the settle window keeps it while the page's media lays out.
  // A bottom pin or the follow already holds the end, and an anchor or a
  // reveal names a bubble the page cannot move. `prepended` keeps the jump
  // pill from counting the page as new messages.
  const holdPlace = useCallback(
    (prepended: number, commit: () => void) => {
      pillBaselineLenRef.current += prepended;
      const c = isActiveRef.current ? getScrollContainer(scrollAreaRef) : null;
      if (!c) {
        // Hidden, or behind a subagent tab: the reader's last place is put
        // back when the transcript is next shown, in place of the tab's own
        // offset, which the page has left a page short.
        const shown = shownPlaceRef.current;
        if (shown && shown.tid === memoryTidRef.current) {
          owedPlaceRef.current = shown;
          delete scrollPositionsRef.current.main;
        }
        return commit();
      }
      const follow = getFollow();
      const mode = pinTargetRef.current?.mode;
      if (mode === 'bottom' || isFollowing()) return commit();
      if (mode !== 'anchor' && mode !== 'reveal') {
        const place = readPlace(c, true);
        if (!place) return commit();
        follow.hold({ mode: 'anchor', id: place.id, delta: place.delta });
      }
      commit();
      follow.reapply(0);
    },
    [isActiveRef, getScrollContainer, getFollow, isFollowing, memoryTidRef],
  );

  // The main transcript's follow, attached while its tab is shown: the scroll
  // listener, the reader's input, the toggle hold and the observer. A layout
  // effect, so it is attached before anything below scrolls the view.
  useLayoutEffect(() => {
    if (activeAgentId !== 'main') return;
    const c = getScrollContainer(scrollAreaRef);
    if (!c) return;
    const follow = getFollow();
    const turnPin = getTurnPin();
    const detach = follow.attach(c);
    const engine = follow.engine;
    if (!engine) return detach;
    const saved = takeTabPlace('main');
    if (saved != null) follow.restore(saved);

    const handleScroll = () => {
      const byReader = follow.scrolled();
      // Record every settle (user scrolls AND pins/follows) so the cross-unmount
      // store always reflects where the transcript actually is: a bottom pin
      // after send must overwrite a stale mid-thread offset. 'bottom' is sticky:
      // re-entry pins to the (possibly taller) new bottom. Offset sessions are
      // the exception: their intermediate scrolls clamp against still-short
      // content and would overwrite the very offset being restored.
      // The band, not the upward rule: a nudge that pauses the follow is not a
      // place worth coming back to, and a numeric save re-opens with the pill.
      // A history load is the same exception, save for the reader's own
      // scrolls: the list holds what it drew through the load (MessageList),
      // so a place they scroll to there is theirs, and the reload's restore
      // lands on it; anything else moving the view then is not where they were.
      // A landing's frames on the way are skipped too: each would read the
      // place across the whole transcript on a frame the glide needs, and the
      // scroll its last write makes arrives once it has ended, so where it
      // stopped is still recorded.
      if (memoryTidRef.current && pinTargetRef.current?.mode !== 'offset' && (byReader || !isLoadingHistoryRef.current) && !engine.landing) {
        const atBottom = isNearBottom({ scrollTop: c.scrollTop, scrollHeight: c.scrollHeight, clientHeight: c.clientHeight }, NEAR_BOTTOM_PX);
        scrollMemory.set(`thread:${memoryTidRef.current}`, atBottom ? 'bottom' : c.scrollTop);
        const place = atBottom ? null : readPlace(c, true);
        readerPlaceRef.current = place && place.delta >= 0 ? { tid: memoryTidRef.current, ...place } : null;
        shownPlaceRef.current = { tid: memoryTidRef.current, place: place ?? 'bottom' };
      }
      if (!byReader) return;
      // The reader's own scroll has taken the view from any held place, and
      // takes it from the pinned turn's floor.
      turnPin.yieldFloor(c);
      const atBottom = isNearBottomRef.current;
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

    // A real user gesture (a wheel, a touch, a press on the scrollbar or the
    // transcript, a key) takes the view from the follow, a held place, a
    // toggle's hold and a pending entry restore, before the scroll it makes
    // arrives.
    const handleUserIntent = (e: Event) => {
      follow.intent(e);
      toggleHoldRef.current = null;
      if (entryRestoreRafRef.current != null) {
        cancelAnimationFrame(entryRestoreRafRef.current);
        entryRestoreRafRef.current = null;
      }
    };
    for (const type of INTENT_EVENTS) c.addEventListener(type, handleUserIntent, { passive: true });

    // A disclosure toggle: the reader has just chosen a place, the toggled row.
    // The pinned turn's floor stays as it is, but the pin no longer places the
    // view: the row does.
    const handleAnchoredToggle = (e: Event) => {
      const el = e.target as HTMLElement | null;
      if (!el) return;
      follow.intent(e);
      turnPin.drop();
      toggleHoldRef.current = { el, top: el.getBoundingClientRect().top, until: performance.now() + TOGGLE_HOLD_MS };
    };
    c.addEventListener(ANCHORED_TOGGLE_EVENT, handleAnchoredToggle);

    // Content growth, observed after layout and before paint. A held place is
    // re-applied (charts, code, images finishing layout: the fix for landing
    // mid-thread); otherwise growth is the follow's to carry. Only growth is
    // followed: the observer also fires once on attach and on every shrink,
    // and a follow there would move a reader whose position a tab return has
    // just restored, or whom a fold just left exactly where they were.
    const content = resolveScrollContent(c);
    const padded = content.parentElement ?? content;
    const status = content.querySelector<HTMLElement>(TRANSCRIPT_STATUS_SELECTOR);
    let lastHeight = -1;
    let lastPad = -1;
    let lastStatusHeight = -1;
    const ro = new ResizeObserver((entries) => {
      // A cached view is display:none and measures 0. Coming back from that
      // is not growth: the composer and the status lines would read as grown
      // and carry a reader near the end to it. A hidden view keeps the sizes
      // it last showed.
      if (entries.some((e) => e.target !== status && blockSize(e) === 0)) return;
      // Where this layout left the view, before anything here moves it.
      engine.resized();
      let entry: ResizeObserverEntry | undefined;
      let statusGrew = false;
      for (const e of entries) {
        if (e.target !== status) {
          entry = e;
          continue;
        }
        const h = blockSize(e);
        statusGrew = lastStatusHeight >= 0 && h > lastStatusHeight;
        lastStatusHeight = h;
      }
      // The pinned turn's floor follows the transcript before anything reads
      // where its bottom is.
      if (turnPin.active) turnPin.settle(c, engine);
      if (!entry) return;
      const height = blockSize(entry);
      const growth = lastHeight >= 0 ? Math.max(0, height - lastHeight) : 0;
      lastHeight = height;
      // The wrapper's bottom padding is the composer's height
      // (TRANSCRIPT_BOTTOM_PAD), so what its border box has over its content
      // box grows with the composer and nothing else.
      const pad = height - (entry.contentBoxSize?.[0]?.blockSize ?? entry.contentRect.height);
      const composerGrew = lastPad >= 0 && pad > lastPad;
      lastPad = pad;
      const atEndBefore = () => isNearBottom(
        { scrollTop: c.scrollTop, scrollHeight: c.scrollHeight - growth, clientHeight: c.clientHeight },
        AT_BOTTOM_PX,
      );
      if (follow.reapply(growth)) {
        // A toggle drops every held place, so one held now was set since (the
        // jump pill, a link) and has placed the reader anew: holding the row
        // would scroll them back to it.
        toggleHoldRef.current = null;
        return;
      }
      const hold = toggleHoldRef.current;
      if (hold && performance.now() <= hold.until && c.contains(hold.el)) {
        const delta = hold.el.getBoundingClientRect().top - hold.top;
        if (Math.abs(delta) >= 1) engine.jump(c.scrollTop + delta);
        return;
      }
      if (hold) {
        toggleHoldRef.current = null;
        // An unfold grows below the held row, so holding it never scrolls and
        // the band is never asked again: the reader who opened a row to read
        // it still counted as following and was carried to the bottom on the
        // next growth. Ask where the hold actually left them, before the
        // growth this callback is reporting, or a reader at the end is read as
        // that growth short of it.
        isNearBottomRef.current = atEndBefore();
      }
      // The composer carries only a reader at the very end, measured before
      // the growth as for the toggle above: one in the band short of the end
      // is reading there. A status line arriving under the last reply is the
      // thread's news.
      if (growth > 0 && entryRestoreSettled()) follow.grew(statusGrew || (composerGrew && atEndBefore()));
    });
    // The padded wrapper's border box, not the content's: its bottom padding
    // is the floating composer's height, and a taller composer (the tray
    // unfolding, a todo tab arriving, the input wrapping) must re-pin and
    // follow like content, or the last line ends up under it. The status
    // lines are watched on their own so their growth can be told from the
    // reader's.
    ro.observe(padded, { box: 'border-box' });
    if (status) ro.observe(status, { box: 'border-box' });

    // A hidden tab gets no rendering updates, so the observer above does not
    // fire and the view sits still while the transcript grows; the animations
    // the tab queued apply their final layout only on return. Close whatever
    // gap that leaves in one instant jump, only for a reader who was following:
    // a user who scrolled up keeps their place. The jump is made twice: at the
    // event, and again inside the first frame, after those animations have
    // applied (see lib/framer) but before that frame paints.
    const handleVisibility = () => {
      if (document.visibilityState !== 'visible') return;
      const jump = () => {
        if (!isFollowing() || c.scrollHeight - c.scrollTop - c.clientHeight <= 1) return;
        engine.jump(c.scrollHeight - c.clientHeight);
      };
      jump();
      if (visibilityRafRef.current != null) cancelAnimationFrame(visibilityRafRef.current);
      visibilityRafRef.current = requestAnimationFrame(() => {
        visibilityRafRef.current = null;
        jump();
      });
    };
    document.addEventListener('visibilitychange', handleVisibility);
    return () => {
      detach();
      c.removeEventListener('scroll', handleScroll);
      for (const type of INTENT_EVENTS) c.removeEventListener(type, handleUserIntent);
      c.removeEventListener(ANCHORED_TOGGLE_EVENT, handleAnchoredToggle);
      document.removeEventListener('visibilitychange', handleVisibility);
      if (visibilityRafRef.current != null) {
        cancelAnimationFrame(visibilityRafRef.current);
        visibilityRafRef.current = null;
      }
      ro.disconnect();
    };
  }, [activeAgentId, getScrollContainer, getFollow, getTurnPin, takeTabPlace, isFollowing, entryRestoreSettled, isLoadingHistoryRef, memoryTidRef, messagesLenRef]);

  // A page that landed while the transcript was not shown, now that it can be
  // measured again.
  useLayoutEffect(() => {
    const owed = owedPlaceRef.current;
    if (!owed || !isActive || activeAgentId !== 'main') return;
    const c = getScrollContainer(scrollAreaRef);
    if (!c) return;
    owedPlaceRef.current = null;
    if (owed.tid !== memoryTidRef.current) return;
    if (owed.place === 'bottom') {
      pinToBottom('auto');
    } else if (findMessageElement(c, owed.place.id)) {
      const follow = getFollow();
      follow.hold({ mode: 'anchor', ...owed.place });
      follow.reapply(0);
    }
  }, [isActive, activeAgentId, getScrollContainer, memoryTidRef, pinToBottom, getFollow]);

  // New messages for a reader who is not following: the ResizeObserver above
  // keeps a following reader at the bottom, so all that is left here is the
  // "N new" count on the jump pill. Held until the thread-entry decision
  // (below) has landed, as messages render while history is still hydrating.
  useEffect(() => {
    if (pinTargetRef.current) return; // a held place owns the scroll during settle
    if (!entryRestoreSettled()) return;
    if (isNearBottomRef.current) return;
    const delta = messagesLenRef.current - pillBaselineLenRef.current;
    if (delta > 0) {
      setJumpPill((prev) => (prev.visible ? { visible: true, hasNew: true, newCount: delta } : prev));
    }
  }, [messages, entryRestoreSettled, messagesLenRef]);

  // A thread switched to, or a history load replaying this one, starts with no
  // floor. A new thread taking its id from the send that created it keeps the
  // floor that send is landing on. Ahead of the entry restore, which measures.
  const floorTidRef = useRef(resolvedTid);
  useLayoutEffect(() => {
    const prev = floorTidRef.current;
    floorTidRef.current = resolvedTid;
    const named = !!prev && prev !== '__default__';
    if (isLoadingHistory || (named && prev !== resolvedTid)) getTurnPin().reset();
  }, [resolvedTid, isLoadingHistory, getTurnPin]);

  // Thread-entry restore, the core fix. Fires on the real "history is present"
  // signal (isLoadingHistory flips false), not on an empty/partial list. A
  // remembered mid-thread offset (scrollMemory, survives route unmounts) wins
  // over the default bottom pin, so tabbing away and back lands where the user
  // left; 'bottom' / no memory pins to bottom through the async settle window.
  // A layout effect, applied in the same commit that renders the history: the
  // first frame the user sees of the thread is already in position. The
  // deferred frame remains only for a viewport that is not mounted yet.
  useLayoutEffect(() => {
    const tid = currentThreadId || threadId;
    if (!tid || tid === '__default__') return;
    if (isLoadingHistory) {
      // A reload of this thread replays it from scratch, so the restore runs
      // again when the replay lands, or when a hidden view is next shown.
      if (restoredForThreadRef.current === tid) {
        restoredForThreadRef.current = null;
        reloadingRef.current = true;
      }
      return;
    }
    if (!isActive) return;
    if (restoredForThreadRef.current === tid) return;
    restoredForThreadRef.current = tid;
    if (getTurnPin().active) {
      // A new thread named by the send that created it: that send's landing
      // already owns the view.
      setRestoredTid(tid);
      return;
    }
    // A reload looks for the bubble the reader was on, since the replay may
    // have changed what sits at their offset. Entering a thread has only the
    // offset: the bubbles it was saved against are gone.
    const place = reloadingRef.current && readerPlaceRef.current?.tid === tid ? readerPlaceRef.current : null;
    reloadingRef.current = false;
    const saved = scrollMemory.get(`thread:${tid}`);
    const apply = () => {
      // The instance may have gone inactive (cached/hidden) before the frame,
      // or opened on a subagent tab, which leaves no main viewport mounted.
      // Nothing is applied then, and the restore is owed to the next time the
      // transcript is shown.
      const c = isActiveRef.current ? getScrollContainer(scrollAreaRef) : null;
      if (!c) {
        if (restoredForThreadRef.current === tid) restoredForThreadRef.current = null;
        return;
      }
      setRestoredTid(tid);
      if (typeof saved !== 'number') return pinToBottom('auto');
      // A numeric save is by construction mid-thread (near-bottom saves record
      // 'bottom'), so it is never the end: the follow stays off and the jump
      // pill shows without waiting for a scroll of the reader's. Held through
      // the settle window, so async content (charts, markdown, images) still
      // growing the transcript cannot clamp it short.
      const follow = getFollow();
      if (place && findMessageElement(c, place.id)) {
        follow.place({ mode: 'anchor', id: place.id, delta: place.delta });
      } else if (place && !historyLoadFailed && REPLAYED_TURN_ID.test(place.id)) {
        // A fork cut the turn they were reading, and the offset would hold
        // them over whatever streams in to replace it. A replay that failed
        // cut nothing: the offset holds, and so does the place, for the retry.
        pinToBottom('auto');
      } else {
        follow.place({ mode: 'offset', top: saved });
      }
    };
    if (getScrollContainer(scrollAreaRef)) {
      apply();
    } else {
      entryRestoreRafRef.current = requestAnimationFrame(() => {
        entryRestoreRafRef.current = null;
        apply();
      });
    }
    return () => {
      if (entryRestoreRafRef.current != null) {
        cancelAnimationFrame(entryRestoreRafRef.current);
        entryRestoreRafRef.current = null;
        // The restore itself is still owed, and restoredTid waits on it.
        if (restoredForThreadRef.current === tid) restoredForThreadRef.current = null;
      }
    };
  }, [isActive, activeAgentId, isLoadingHistory, historyLoadFailed, currentThreadId, threadId, pinToBottom, isActiveRef, getScrollContainer, getFollow, getTurnPin]);

  // Cancel the entry-restore frame on unmount.
  useEffect(() => () => {
    if (entryRestoreRafRef.current != null) cancelAnimationFrame(entryRestoreRafRef.current);
  }, []);

  return {
    scrollAreaRef,
    transcriptFloorRef,
    subagentScrollRoot,
    subagentPane,
    takeTabPlace,
    getScrollContainer,
    pinToBottom,
    follow: controls,
    pinToMessage,
    revealFiles,
    revealMessage,
    holdPlace,
    toTop,
    pinTargetRef,
    saveScrollPosition,
    jumpPill,
    jumping,
    activeAgentIdRef,
    isNearBottomRef,
    restoredForThreadRef,
    entryRestored: restoredTid !== null && restoredTid === resolvedTid,
  };
}
