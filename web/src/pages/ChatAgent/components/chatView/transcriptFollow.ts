import { useCallback, useEffectEvent, useLayoutEffect, useRef } from 'react';
import { NEAR_BOTTOM_PX, createSettleWindow, isNearBottom } from '../../utils/scrollHelpers';
import { anchorTop, findMessageElement, revealTop, type AnchorPart } from '../../utils/scrollDom';
import { INTENT_EVENTS, RELEASE_EVENTS, createStreamFollow, movesView, towardStart, type StreamFollow, type StreamFollowControls } from './streamFollow';

/**
 * A place the view is held on through the settle window, re-applied as the
 * transcript resizes so async content (charts, markdown, images) finishing
 * layout cannot strand it. 'bottom' rides the growing end; 'offset' converges
 * on a remembered scrollTop that is not reachable yet; 'anchor' keeps a bubble
 * under the viewport top (`delta` px into it, or its reply's start); 'reveal'
 * keeps a turn's deliverables deck in view as it unfolds.
 */
export type PinTarget =
  | { mode: 'bottom' }
  | { mode: 'offset'; top: number }
  | { mode: 'anchor'; id: string; part?: AnchorPart; delta?: number }
  | { mode: 'reveal'; id: string };

/** Where a tab was left: an offset, or the end of a transcript the reader was
 *  riding, which may have grown while the tab was away. */
export type TabPlace = number | 'end';

/** scrollTop that puts the view on `pin`, or null once what it names has left the transcript. */
export function pinTop(c: HTMLElement, pin: PinTarget): number | null {
  switch (pin.mode) {
    case 'bottom': return Math.max(0, c.scrollHeight - c.clientHeight);
    case 'offset': return pin.top;
    case 'reveal': return revealTop(c, pin.id);
    case 'anchor': return anchorTop(c, pin.id, pin.part, pin.delta);
  }
}

/** A ResizeObserver entry's border-box height. */
export function blockSize(e: ResizeObserverEntry): number {
  return e.borderBoxSize?.[0]?.blockSize ?? e.contentRect.height;
}

/**
 * The follow both chat surfaces share, over one scroll engine
 * (createStreamFollow) at a time: whether the reader rides the end, a place
 * held through the settle window, the tail of a turn that has just ended, and
 * where a reply, a link or a turn the reader starts takes the view. The main
 * transcript keeps one across its scroller remounting on a tab switch
 * (`attach`) and builds its pinned send, scroll memory and jump pill on top
 * (useChatScroll); the market panel and the subagent pane use it as it is, one
 * per transcript shown (useStreamFollow).
 */
export interface TranscriptFollow {
  /** The reader rides the end. The engine judges it on their scrolls, and a
   *  placement or a restore on where it leaves them. */
  readonly following: { current: boolean };
  /** The attached scroller's engine. */
  readonly engine: StreamFollow | null;
  /** Follows scroller `c` until the returned detach. */
  attach(c: HTMLElement): () => void;
  /** Whether a turn is running, on each change. Its growth is news while it
   *  runs, and for one settle window after it ends: the commit that ends it
   *  adds the reply's actions, then the typewriter types out what it still
   *  held and late media lands, all after the turn has closed. */
  setLive(live: boolean): void;
  /** A scroll event: true for the reader's own move, which takes the view from
   *  any held place. */
  scrolled(): boolean;
  /** The reader's input on the scroller (INTENT_EVENTS). It ends every claim
   *  the follow made on the view: the glide, a held place, and a finished
   *  turn's tail, so a row they open as a turn ends stays where it opened. */
  intent(e: Event): void;
  /** Holds the view on `pin` through the settle window. */
  hold(pin: PinTarget): void;
  unpin(): void;
  /** Re-applies the held place for a resize carrying `growth` px of growth;
   *  false when no place is held. */
  reapply(growth: number): boolean;
  /** Carries a following reader toward the end for growth that is news: a
   *  turn running, its tail, or `chrome` (the main transcript's status lines
   *  and composer, which grow around the transcript rather than in it). */
  grew(chrome?: boolean): void;
  /** The follow is carrying the reader: they ride the end, nothing holds a
   *  place, and a turn is running or has just ended. */
  readonly carrying: boolean;
  /** Takes the view to `pin` and holds it there, gliding when `smooth` (the
   *  engine lands at once for a reader who asked for less motion). The reader
   *  rides the end from there only if it is the end: the bottom, or a place
   *  in the `newest` turn that clamps to it. False when `pin` names nothing
   *  drawn. */
  place(pin: PinTarget, opts?: { smooth?: boolean; newest?: boolean; onEnd?: () => void }): boolean;
  /** Puts the view back where the reader left the tab: at `top`, asking
   *  whether that is the end, or at the end they rode, still following. */
  restore(place: TabPlace): void;
  /** A turn the reader starts claims the view: nothing held, and the follow
   *  carries them from here. */
  claim(): void;
  /** `claim`, and glides to the end. */
  rejoin(): void;
  /** The first line of reply `id` under the viewport top, for a turn that
   *  finished under 'reply_start'. */
  landOnReply(id: string): void;
  /** A link's jump to message `id`, under the viewport top as a placement
   *  lands it, then focus there. False when this transcript does not draw it. */
  reveal(id: string): boolean;
  /** The mobile top bar's tap: to the top, which is a place like any other. */
  toTop(): void;
}

export function createTranscriptFollow({
  following = { current: true },
  pin = { current: null },
  placed,
}: {
  following?: { current: boolean };
  pin?: { current: PinTarget | null };
  /** Told whether each placement left the reader at the end. */
  placed?: (atEnd: boolean) => void;
} = {}): TranscriptFollow {
  let c: HTMLElement | null = null;
  let engine: StreamFollow | null = null;
  let live = false;
  let tailing = false;
  const tail = createSettleWindow(() => {
    tailing = false;
  });
  const settle = createSettleWindow(() => {
    pin.current = null;
  });
  const maxTop = (el: HTMLElement) => Math.max(0, el.scrollHeight - el.clientHeight);

  const follow: TranscriptFollow = {
    following,
    get engine() {
      return engine;
    },
    attach(el) {
      engine?.stop();
      const own = createStreamFollow(el, following);
      c = el;
      engine = own;
      // Wherever the pointer or touch lifts: a scrollbar drag ends off the
      // scroller too, and a window that loses focus mid-drag hears no lift.
      const release = () => own.pressed(false);
      for (const type of RELEASE_EVENTS) window.addEventListener(type, release, { capture: true, passive: true });
      window.addEventListener('blur', release);
      return () => {
        own.stop();
        for (const type of RELEASE_EVENTS) window.removeEventListener(type, release, { capture: true });
        window.removeEventListener('blur', release);
        if (engine !== own) return;
        engine = null;
        c = null;
      };
    },
    setLive(now) {
      const ended = live && !now;
      live = now;
      tailing = ended;
      if (!ended) return tail.clear();
      tail.arm();
      // The commit that ends the turn carries its last growth, which the
      // observer reports only after this: carried from here as well.
      if (!pin.current || pin.current.mode === 'bottom') engine?.follow();
    },
    scrolled() {
      if (!engine?.scrolled()) return false;
      follow.unpin();
      return true;
    },
    intent(e) {
      engine?.stop();
      if (movesView(e)) {
        engine?.touched();
        if (e.type === 'pointerdown' || e.type === 'touchstart') engine?.pressed(true);
      }
      if (towardStart(e)) engine?.pulled();
      follow.unpin();
      tailing = false;
      tail.clear();
    },
    hold(target) {
      pin.current = target;
      settle.arm();
    },
    unpin() {
      pin.current = null;
      settle.clear();
    },
    reapply(growth) {
      const target = pin.current;
      if (!target) return false;
      if (!c || !engine) return true;
      // A landing under way re-reads its target every frame.
      if (engine.landing) {
        settle.arm();
        return true;
      }
      // A bottom pin over a turn still running is the follow's to carry: its
      // glide, not a jump per growth. A pin on a settled transcript re-applies
      // at once: what grows there is late layout, not news.
      if (target.mode === 'bottom' && growth > 0 && (live || tailing)) {
        engine.follow();
        settle.arm();
        return true;
      }
      const top = pinTop(c, target);
      if (top == null) {
        // What it named left the transcript (an edit or a regenerate cut it).
        follow.unpin();
        return true;
      }
      engine.jump(top);
      settle.arm();
      return true;
    },
    grew(chrome = false) {
      if (!engine || pin.current || !following.current || !(chrome || live || tailing)) return;
      engine.follow();
      if (tailing) tail.arm();
    },
    get carrying() {
      return following.current && !pin.current && (live || tailing);
    },
    place(target, { smooth = false, newest = false, onEnd } = {}) {
      const top = c && engine ? pinTop(c, target) : null;
      if (!c || !engine || top == null) return false;
      const el = c;
      follow.hold(target);
      // A request past the maximum clamps, so any turn near enough to the end
      // reads as the bottom by position alone. Only the newest one really is:
      // under an earlier turn the transcript still has room to grow, and
      // calling that the bottom re-arms the follow that carries the reader off
      // the turn they picked.
      const atEnd = target.mode === 'bottom' || (newest && top >= maxTop(el) - 1);
      following.current = atEnd;
      placed?.(atEnd);
      if (Math.abs(Math.min(top, maxTop(el)) - el.scrollTop) < 1) {
        // Already there: a reply that fits on screen moves nothing.
        engine.stop();
        onEnd?.();
      } else if (smooth) {
        // Re-measured every frame, so a fold settling above the place while
        // the view travels moves where it lands.
        engine.land(() => pinTop(el, target), onEnd);
      } else {
        engine.jump(top);
      }
      return true;
    },
    restore(place) {
      if (!c || !engine) return;
      if (place === 'end') {
        follow.place({ mode: 'bottom' });
        return;
      }
      engine.jump(place);
      following.current = isNearBottom({ scrollTop: c.scrollTop, scrollHeight: c.scrollHeight, clientHeight: c.clientHeight }, NEAR_BOTTOM_PX);
      placed?.(following.current);
    },
    claim() {
      follow.unpin();
      following.current = true;
      placed?.(true);
    },
    rejoin() {
      follow.claim();
      const el = c;
      if (el && engine && maxTop(el) - el.scrollTop > 1) engine.land(() => maxTop(el));
    },
    landOnReply(id) {
      // A bottom pin is the follow inside its settle window (a thread opening,
      // the jump pill) and hands over; any other holds a place the reader
      // chose. Only a reader the follow was carrying is moved: one who
      // scrolled up keeps their place. A reply shorter than the view clamps
      // to the end and nothing moves, and the hold re-measures the reply's
      // start as the settled turn folds its work away above it.
      if ((pin.current && pin.current.mode !== 'bottom') || !following.current) return;
      follow.place({ mode: 'anchor', id, part: 'reply' }, { smooth: true, newest: true });
    },
    reveal(id) {
      // Only this transcript's own messages count: the threads kept mounted
      // behind it are hidden, and a message loaded from history is named by
      // its place there, so another can share its name.
      const el = c && findMessageElement(c, id);
      if (!c || !el) return false;
      const drawn = c.querySelectorAll('[data-message-id]');
      if (!follow.place({ mode: 'anchor', id }, { smooth: true, newest: drawn[drawn.length - 1] === el })) return false;
      el.focus({ preventScroll: true });
      return true;
    },
    toTop() {
      if (c && c.scrollTop > 0) follow.place({ mode: 'offset', top: 0 }, { smooth: true });
    },
  };
  return follow;
}

/**
 * The follow for a transcript pane with no thread memory of its own: the
 * market panel, and the subagent pane beside the main transcript. Each
 * transcript it shows gets a follow of its own (`view` names the one a reused
 * scroller shows), which opens at the end and holds it through the settle
 * window, as a thread does, or at `openAt`, a tab's place kept across a
 * switch. Growth is followed while `live`, and a history `loading` lands a
 * following reader at the end once it is in. A send pins nothing here: these
 * are narrow columns, where a reader's message under the top would leave most
 * of the view blank.
 */
export function useStreamFollow(
  scroller: HTMLElement | null,
  content: HTMLElement | null,
  { live, loading = false, view, openAt }: {
    live: boolean;
    loading?: boolean;
    view?: string;
    openAt?: () => TabPlace | null;
  },
): StreamFollowControls & { reveal(id: string): boolean; toTop(): void } {
  const followRef = useRef<TranscriptFollow | null>(null);
  useLayoutEffect(() => {
    followRef.current?.setLive(live);
  }, [live]);
  const wasLoadingRef = useRef(loading);
  useLayoutEffect(() => {
    const loaded = wasLoadingRef.current && !loading;
    wasLoadingRef.current = loading;
    const follow = followRef.current;
    if (loaded && follow?.following.current) follow.place({ mode: 'bottom' });
  }, [loading]);
  const opening = useEffectEvent(() => ({ live, saved: openAt?.() ?? null }));
  useLayoutEffect(() => {
    if (!scroller || !content) return;
    const follow = createTranscriptFollow();
    followRef.current = follow;
    const detach = follow.attach(scroller);
    const { live: liveNow, saved } = opening();
    follow.setLive(liveNow);
    if (saved != null) follow.restore(saved);
    else follow.place({ mode: 'bottom' });
    let lastHeight = -1;
    const ro = new ResizeObserver(([entry]) => {
      const height = entry ? blockSize(entry) : 0;
      // A hidden pane measures 0, and coming back from that is not growth.
      if (height === 0) return;
      follow.engine?.resized();
      const growth = lastHeight >= 0 ? Math.max(0, height - lastHeight) : 0;
      lastHeight = height;
      if (!follow.reapply(growth) && growth > 0) follow.grew();
    });
    const onScroll = () => {
      follow.scrolled();
    };
    const onIntent = (e: Event) => follow.intent(e);
    scroller.addEventListener('scroll', onScroll, { passive: true });
    for (const type of INTENT_EVENTS) scroller.addEventListener(type, onIntent, { passive: true });
    ro.observe(content, { box: 'border-box' });
    return () => {
      // Its settle window and tail lapse into a follow nothing reads.
      detach();
      if (followRef.current === follow) followRef.current = null;
      scroller.removeEventListener('scroll', onScroll);
      for (const type of INTENT_EVENTS) scroller.removeEventListener(type, onIntent);
      ro.disconnect();
    };
  }, [scroller, content, view]);
  const rejoin = useCallback(() => followRef.current?.rejoin(), []);
  const landOnReply = useCallback((id: string) => followRef.current?.landOnReply(id), []);
  const reveal = useCallback((id: string) => followRef.current?.reveal(id) ?? false, []);
  const toTop = useCallback(() => followRef.current?.toTop(), []);
  return { rejoin, landOnReply, reveal, toTop };
}
