import { memo, useCallback, useEffect, useId, useLayoutEffect, useMemo, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Loader } from '@/components/ui/loader';
import { useStableArray } from '@/hooks/useStableArray';
import { isNearBottom } from '../utils/scrollHelpers';
import { resolveScrollContent, resolveScrollViewport } from '../utils/scrollDom';
import type { MessageRecord } from './messageList/types';
import { buildEntries, entriesEqual } from './minimapEntries';
import type { PinTarget } from './chatView/useChatScroll';
import './ChatMinimap.css';

interface ChatMinimapProps {
  messages: MessageRecord[];
  scrollAreaRef: React.RefObject<HTMLDivElement | null>;
  /** The newest prompt with no reply text yet shows a skeleton only while a turn is actually running. */
  turnInFlight: boolean;
  /** Navigation goes through the scroll controller so settle re-apply and streaming follow honour the landing. */
  pinToMessage: (id: string, behavior?: 'auto' | 'smooth', isLatest?: boolean) => void;
  /** Read to tell a landing the reader asked for from one they scrolled to; see the scrollspy. */
  pinTargetRef: React.RefObject<PinTarget | null>;
}

/** Same band the scroll controller's jump pill uses, so "at the bottom" agrees. */
const NEAR_BOTTOM_PX = 120;
/** A turn becomes active once its prompt crosses this line below the viewport top; on short viewports the line sits at a fraction of the height instead. */
const ACTIVE_LINE_PX = 96;
const ACTIVE_LINE_MAX_FRACTION = 0.3;
/** Vertical pitch of one tick. It shrinks to fit the column; past the minimum the rail scrolls. */
const PITCH_MAX = 16;
const PITCH_MIN = 4;
/** Rail padding, top plus bottom; must match the stylesheet. */
const RAIL_PADDING_PX = 16;
const CARD_MARGIN_PX = 8;
/** Tick width / opacity by index distance to the focus (hovered, else active); the last bucket is the resting look. */
const TICK_FALLOFF = [
  { width: 28, opacity: 0.5 },
  { width: 20, opacity: 0.36 },
  { width: 15, opacity: 0.28 },
  { width: 12, opacity: 0.22 },
] as const;
const ACTIVE_OPACITY = 0.92;

function stableIds(prev: string[], next: string[]): boolean {
  return next.length === prev.length && next.every((id, i) => id === prev[i]);
}

/** A mouse click focuses the button too; only keyboard focus should hold the card up after the pointer leaves. */
function keyboardFocused(el: HTMLElement): boolean {
  try {
    return el.matches(':focus-visible');
  } catch {
    return true;
  }
}

function prefersReducedMotion(): boolean {
  return typeof window !== 'undefined' && window.matchMedia?.('(prefers-reduced-motion: reduce)').matches;
}

interface TickProps {
  id: string;
  label: string;
  pitch: number;
  width: number;
  opacity: number;
  active: boolean;
  describedBy?: string;
  onHover: (id: string) => void;
  onFocus: (id: string) => void;
  onBlur: (id: string) => void;
  onActivate: (id: string) => void;
}

// Memoised so a streamed chunk (which rebuilds the entries) re-renders the
// card, not every tick in the rail.
const Tick = memo(function Tick({
  id, label, pitch, width, opacity, active, describedBy, onHover, onFocus, onBlur, onActivate,
}: TickProps) {
  return (
    <button
      type="button"
      className="chat-minimap-tick"
      data-minimap-id={id}
      aria-label={label}
      aria-current={active ? 'true' : undefined}
      aria-describedby={describedBy}
      style={{ height: pitch, ['--tick-w' as string]: `${width}px`, ['--tick-o' as string]: opacity }}
      onMouseEnter={() => onHover(id)}
      onFocus={(e) => {
        if (keyboardFocused(e.currentTarget)) onFocus(id);
      }}
      onBlur={() => onBlur(id)}
      onClick={() => onActivate(id)}
    >
      <span aria-hidden="true" />
    </button>
  );
});

export default function ChatMinimap({ messages, scrollAreaRef, turnInFlight, pinToMessage, pinTargetRef }: ChatMinimapProps) {
  const { t } = useTranslation();
  const rootRef = useRef<HTMLDivElement>(null);
  const railRef = useRef<HTMLDivElement>(null);
  const cardRef = useRef<HTMLDivElement>(null);
  const cardId = useId();

  const entries = useStableArray(
    useMemo(() => buildEntries(messages ?? [], turnInFlight), [messages, turnInFlight]),
    entriesEqual,
  );
  // Ids alone drive the observers: the reply text changes on every streamed
  // chunk, and the scroll bookkeeping must not tear down for that.
  const ids = useStableArray(
    useMemo(() => entries.map((e) => e.id), [entries]),
    stableIds,
  );

  const [activeId, setActiveId] = useState<string | null>(null);
  // Pointer and keyboard each own a slot: leaving with the mouse must not
  // dismiss the card a focused tick is showing.
  const [hoverId, setHoverId] = useState<string | null>(null);
  const [focusId, setFocusId] = useState<string | null>(null);
  const [railHeight, setRailHeight] = useState(0);

  // Scrollspy: the active turn is whichever one the reader pinned, else the
  // last prompt above the reading line, else the final turn once they are at
  // the bottom. Anchors are resolved once per id set and re-queried only if one
  // leaves the DOM; a binary search over them keeps a frame to a handful of
  // rect reads however long the thread.
  useEffect(() => {
    const viewport = resolveScrollViewport(scrollAreaRef.current);
    if (!viewport || ids.length < 2) return;
    let anchors: { index: number; el: HTMLElement }[] = [];
    let raf = 0;

    const resolveAnchors = () => {
      const byId = new Map<string, HTMLElement>();
      for (const el of viewport.querySelectorAll<HTMLElement>('[data-message-id]')) {
        byId.set(el.dataset.messageId ?? '', el);
      }
      anchors = [];
      ids.forEach((id, index) => {
        const el = byId.get(id);
        if (el) anchors.push({ index, el });
      });
    };
    const compute = () => {
      raf = 0;
      // A held anchor is the reader's own answer to "which turn am I on", so it
      // outranks anything read off the scroll position. Without this, landing on
      // one of the last turns lights up the final tick instead of the one just
      // clicked: the landing falls inside the bottom band below, and the rail
      // then magnifies around the wrong neighbourhood.
      const anchored = pinTargetRef.current?.mode === 'anchor' ? pinTargetRef.current.id : null;
      if (anchored) {
        setActiveId(anchored);
        return;
      }
      const metrics = { scrollTop: viewport.scrollTop, scrollHeight: viewport.scrollHeight, clientHeight: viewport.clientHeight };
      // A transcript too short to scroll at all would otherwise read as "at the
      // bottom" forever and pin the last tick. One that overflows by less than
      // the band still has a real bottom the reader can reach, and there the
      // band never opens, so recognise that case by position instead.
      const range = metrics.scrollHeight - metrics.clientHeight;
      const atBottom =
        range > 0 &&
        (range <= NEAR_BOTTOM_PX ? metrics.scrollTop >= range - 1 : isNearBottom(metrics, NEAR_BOTTOM_PX));
      if (atBottom) {
        setActiveId(ids[ids.length - 1]);
        return;
      }
      if (anchors.length < ids.length || anchors.some((a) => !a.el.isConnected)) resolveAnchors();
      const vpRect = viewport.getBoundingClientRect();
      const line = vpRect.top + Math.min(ACTIVE_LINE_PX, vpRect.height * ACTIVE_LINE_MAX_FRACTION);
      let lo = 0;
      let hi = anchors.length - 1;
      let hit = -1;
      while (lo <= hi) {
        const mid = (lo + hi) >> 1;
        if (anchors[mid].el.getBoundingClientRect().top <= line) {
          hit = mid;
          lo = mid + 1;
        } else {
          hi = mid - 1;
        }
      }
      setActiveId(hit >= 0 ? ids[anchors[hit].index] : ids[0]);
    };
    const schedule = () => {
      if (!raf) raf = requestAnimationFrame(compute);
    };

    compute();
    viewport.addEventListener('scroll', schedule, { passive: true });
    // Content growth moves the anchors under a fixed scrollTop; a viewport
    // resize (composer growing, window height) moves the reading line.
    const resize = typeof ResizeObserver !== 'undefined' ? new ResizeObserver(schedule) : null;
    resize?.observe(resolveScrollContent(viewport));
    resize?.observe(viewport);
    return () => {
      viewport.removeEventListener('scroll', schedule);
      resize?.disconnect();
      if (raf) cancelAnimationFrame(raf);
    };
  }, [scrollAreaRef, ids, pinTargetRef]);

  // Keyed on the rail being mounted: the component renders nothing until the
  // second turn exists, and an effect that ran only once would have found no
  // element to observe.
  const railMounted = entries.length >= 2;
  useEffect(() => {
    const root = rootRef.current;
    if (!railMounted || !root || typeof ResizeObserver === 'undefined') return;
    const measure = () => setRailHeight(root.clientHeight);
    measure();
    const observer = new ResizeObserver(measure);
    observer.observe(root);
    return () => observer.disconnect();
  }, [railMounted]);

  const pitch = useMemo(() => {
    if (!railHeight || ids.length === 0) return PITCH_MAX;
    const fit = Math.floor((railHeight - RAIL_PADDING_PX) / ids.length);
    return Math.max(PITCH_MIN, Math.min(PITCH_MAX, fit));
  }, [railHeight, ids.length]);

  const shownId = hoverId ?? focusId;
  const shownEntry = shownId ? entries.find((e) => e.id === shownId) ?? null : null;

  // Keep the card centred on its tick, clamped to the column. Positioned
  // imperatively: the first placement must not animate, or the card slides in
  // from wherever the element mounted.
  useLayoutEffect(() => {
    const root = rootRef.current;
    const rail = railRef.current;
    const card = cardRef.current;
    if (!shownEntry || !root || !rail || !card) return;
    const tick = rail.children[ids.indexOf(shownEntry.id)] as HTMLElement | undefined;
    if (!tick) return;
    const place = () => {
      const center = tick.offsetTop - rail.scrollTop + tick.offsetHeight / 2;
      const maxTop = Math.max(CARD_MARGIN_PX, root.clientHeight - card.offsetHeight - CARD_MARGIN_PX);
      card.style.top = `${Math.min(maxTop, Math.max(CARD_MARGIN_PX, center - card.offsetHeight / 2))}px`;
    };
    if (card.dataset.placed) {
      place();
    } else {
      card.style.transition = 'none';
      place();
      void card.offsetHeight;
      card.style.transition = '';
      card.dataset.placed = '1';
    }
    // An overflowing rail scrolls under a card that is already up: a wheel over
    // the rail, or the active tick being kept in view as the transcript streams.
    // Neither touches a dependency here, so the card would keep its old offset
    // and drift away from the tick it describes.
    rail.addEventListener('scroll', place, { passive: true });
    return () => rail.removeEventListener('scroll', place);
  }, [shownEntry, ids, pitch, railHeight]);

  // A rail long enough to scroll keeps the active tick in view. A resize is a
  // trigger as much as a new active turn: the column shrinking re-pitches every
  // tick under an unchanged scrollTop, which is how the tick this is meant to
  // keep visible ends up outside the rail.
  useEffect(() => {
    const rail = railRef.current;
    if (!rail || !activeId || rail.scrollHeight <= rail.clientHeight) return;
    const tick = rail.children[ids.indexOf(activeId)] as HTMLElement | undefined;
    if (!tick) return;
    const top = tick.offsetTop - rail.offsetTop;
    const bottom = top + tick.offsetHeight;
    if (top < rail.scrollTop) rail.scrollTop = top;
    else if (bottom > rail.scrollTop + rail.clientHeight) rail.scrollTop = bottom - rail.clientHeight;
  }, [activeId, ids, pitch, railHeight]);

  const hover = useCallback((id: string) => setHoverId(id), []);
  const focus = useCallback((id: string) => {
    // Keyboard focus is the newer interaction, and a pointer resting on another
    // tick never fires mouseleave. Without dropping it here the card and the
    // aria-describedby it carries would keep naming the hovered tick while the
    // keyboard is somewhere else.
    setHoverId(null);
    setFocusId(id);
  }, []);
  const blur = useCallback((id: string) => setFocusId((cur) => (cur === id ? null : cur)), []);
  const leave = useCallback(() => setHoverId(null), []);
  const dismiss = useCallback((e: React.KeyboardEvent<HTMLDivElement>) => {
    // Escape closes the preview without moving focus, so a reader on a tick can
    // see the transcript the card is covering. Only swallow the key when there
    // was something to close.
    if (e.key !== 'Escape' || !shownId) return;
    e.stopPropagation();
    setHoverId(null);
    setFocusId(null);
  }, [shownId]);
  const activate = useCallback(
    (id: string) => {
      // A turn already as far up as the transcript goes lands clamped: the
      // scroll never moves, so no scroll event arrives and the scrollspy, which
      // only recomputes on one, would leave the click unanswered.
      setActiveId(id);
      // Whether this is the newest turn is the rail's to answer, and it decides
      // whether the landing keeps following the stream.
      pinToMessage(id, prefersReducedMotion() ? 'auto' : 'smooth', id === ids[ids.length - 1]);
    },
    [pinToMessage, ids],
  );
  // The rail sits over the transcript's right edge where the scrollbar used to
  // be, so a wheel over it must still scroll the transcript unless the rail
  // itself can spend the gesture. The transcript is a sibling, not an ancestor,
  // so nothing chains to it once the rail hits an end: forward from there too.
  const wheel = useCallback(
    (e: React.WheelEvent<HTMLDivElement>) => {
      const rail = e.currentTarget;
      const slack = rail.scrollHeight - rail.clientHeight;
      if (slack > 0 && (e.deltaY < 0 ? rail.scrollTop > 0 : rail.scrollTop < slack - 1)) return;
      const viewport = resolveScrollViewport(scrollAreaRef.current);
      if (!viewport) return;
      // deltaY is pixels, lines or pages depending on the device. The last two
      // are counts, so a one-page gesture forwarded raw moves one pixel.
      const step = e.deltaMode === 1 ? 16 : e.deltaMode === 2 ? viewport.clientHeight : 1;
      viewport.scrollBy({ top: e.deltaY * step });
    },
    [scrollAreaRef],
  );

  if (!railMounted) return null;

  const focusIndex = ids.indexOf(shownEntry?.id ?? activeId ?? '');
  const resting = TICK_FALLOFF.length - 1;
  const fallbackLabel = t('chat.minimap.untitledTurn');

  return (
    <div ref={rootRef} className="chat-minimap">
      <div ref={railRef} className="chat-minimap-rail" onMouseLeave={leave} onWheel={wheel} onKeyDown={dismiss}>
        {entries.map((entry, index) => {
          const bucket = focusIndex < 0 ? resting : Math.min(Math.abs(index - focusIndex), resting);
          const isActive = entry.id === activeId;
          return (
            <Tick
              key={entry.id}
              id={entry.id}
              label={entry.prompt || fallbackLabel}
              pitch={pitch}
              width={TICK_FALLOFF[bucket].width}
              opacity={isActive ? ACTIVE_OPACITY : TICK_FALLOFF[bucket].opacity}
              active={isActive}
              describedBy={entry.id === shownEntry?.id ? cardId : undefined}
              onHover={hover}
              onFocus={focus}
              onBlur={blur}
              onActivate={activate}
            />
          );
        })}
      </div>

      {shownEntry && (
        <div ref={cardRef} id={cardId} className="chat-minimap-card" role="tooltip">
          <div className="chat-minimap-card-prompt">{shownEntry.prompt || fallbackLabel}</div>
          {shownEntry.pending ? (
            // The Loader carries the announcement (role="status" + label), so
            // the visible word is decoration beside it rather than a second
            // thing to read out.
            <div className="chat-minimap-card-live">
              <Loader size={11} label={t('chat.minimap.replyPending')} style={{ color: 'inherit' }} />
              <span aria-hidden="true">{t('chat.minimap.replyPending')}</span>
            </div>
          ) : shownEntry.reply ? (
            <div className="chat-minimap-card-reply">{shownEntry.reply}</div>
          ) : null}
        </div>
      )}
    </div>
  );
}
