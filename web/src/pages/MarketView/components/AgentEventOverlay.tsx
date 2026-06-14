/**
 * Interactive DOM overlay for ``event`` (news-event) annotations on the live
 * MarketView chart.
 *
 * Canvas chips (drawn by ``AgentAnnotationsPrimitive``) can't receive
 * hover/click, so news events render as DOM badges layered over the chart:
 * each badge is anchored at its ``(time, price)`` point via the chart's
 * coordinate API and repositioned on pan/zoom/resize. The always-visible badge
 * shows the event title; the few-sentence ``detail`` is revealed on hover
 * (hover-capable devices) or tap/click (touch) via a Radix Popover.
 *
 * Mounts inside ``chart-wrapper`` (alongside the crosshair tooltip) and only in
 * the custom/Light chart mode — the TradingView iframe has no surface to
 * overlay. Styling rides the theme-aware CSS tokens, so it follows light/dark
 * without per-instance work.
 */

import React, {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type RefObject,
} from 'react';
import type { IChartApi, ISeriesApi, Time } from 'lightweight-charts';

import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover';
import type { ChartDataPoint } from '@/types/market';

import { useAnnotationsForView } from '../stores/chartAnnotationStore';
import { buildEvents, type EventItem } from '../utils/annotationGeometry';
import './AgentEventOverlay.css';

// Keep a badge's center this far from the pane edges so it stays readable.
const EDGE_X = 30;
// Below this anchor-y the badge flips under the point (no room above it).
const FLIP_Y = 44;
// Grace period before a hover-opened popover closes, so the pointer can travel
// from the badge into the detail card without it vanishing.
const HOVER_CLOSE_MS = 120;

const hoverMql =
  typeof window !== 'undefined' && typeof window.matchMedia === 'function'
    ? window.matchMedia('(hover: hover)')
    : null;

/** True on pointer devices that can hover (desktop), false on touch. */
function useHoverCapable(): boolean {
  const [hoverable, setHoverable] = useState(() => hoverMql?.matches ?? false);
  useEffect(() => {
    if (!hoverMql) return;
    const onChange = () => setHoverable(hoverMql.matches);
    hoverMql.addEventListener('change', onChange);
    return () => hoverMql.removeEventListener('change', onChange);
  }, []);
  return hoverable;
}

interface PlacedEvent extends EventItem {
  x: number;
  y: number;
  below: boolean;
}

interface EventBadgeProps {
  ev: PlacedEvent;
  hoverCapable: boolean;
}

/** One news badge + its hover/tap detail popover. */
function EventBadge({ ev, hoverCapable }: EventBadgeProps): React.ReactElement {
  const [open, setOpen] = useState(false);
  const closeTimer = useRef<number | null>(null);

  const clearClose = useCallback(() => {
    if (closeTimer.current != null) {
      clearTimeout(closeTimer.current);
      closeTimer.current = null;
    }
  }, []);
  const scheduleClose = useCallback(() => {
    clearClose();
    closeTimer.current = window.setTimeout(() => setOpen(false), HOVER_CLOSE_MS);
  }, [clearClose]);
  useEffect(() => clearClose, [clearClose]);

  // Hover handlers only on hover-capable devices; touch falls back to the
  // trigger's native click toggle (Radix calls onOpenChange on tap).
  const triggerHover = hoverCapable
    ? {
        onPointerEnter: () => {
          clearClose();
          setOpen(true);
        },
        onPointerLeave: scheduleClose,
      }
    : {};
  const contentHover = hoverCapable
    ? { onPointerEnter: clearClose, onPointerLeave: scheduleClose }
    : {};

  const side = ev.below ? 'bottom' : 'top';

  return (
    <Popover
      open={open}
      onOpenChange={(o) => {
        clearClose();
        setOpen(o);
      }}
    >
      <PopoverTrigger asChild>
        <button
          type="button"
          className={`agent-event-badge${ev.below ? ' agent-event-badge--below' : ''}`}
          style={{ left: `${ev.x}px`, top: `${ev.y}px` }}
          aria-label={`News event: ${ev.title}`}
          {...triggerHover}
        >
          <span className="agent-event-badge-dot" style={{ background: ev.color }} />
          <span className="agent-event-badge-title">{ev.title}</span>
        </button>
      </PopoverTrigger>
      <PopoverContent
        side={side}
        align="center"
        sideOffset={10}
        collisionPadding={12}
        className="agent-event-pop"
        onOpenAutoFocus={(e) => {
          // Don't yank focus on hover-open; let click-open focus normally.
          if (hoverCapable) e.preventDefault();
        }}
        {...contentHover}
      >
        <div className="agent-event-pop-head">
          <span className="agent-event-pop-dot" style={{ background: ev.color }} />
          <span className="agent-event-pop-title">{ev.title}</span>
        </div>
        <p className="agent-event-pop-detail">{ev.detail}</p>
      </PopoverContent>
    </Popover>
  );
}

interface AgentEventOverlayProps {
  chartRef: RefObject<IChartApi | null>;
  seriesRef: RefObject<ISeriesApi<'Candlestick'> | null>;
  chartData: ChartDataPoint[] | null;
  /** Light/dark — drives re-placement on toggle (styling rides CSS tokens). */
  theme: 'light' | 'dark';
  /** False when the user cleared the drawing; keeps the store intact. */
  visible: boolean;
  workspaceId: string | null | undefined;
  symbol: string | null | undefined;
  timeframe: string | null | undefined;
}

export function AgentEventOverlay({
  chartRef,
  seriesRef,
  chartData,
  theme,
  visible,
  workspaceId,
  symbol,
  timeframe,
}: AgentEventOverlayProps): React.ReactElement {
  const hoverCapable = useHoverCapable();
  const annotations = useAnnotationsForView(workspaceId ?? null, symbol ?? null, timeframe ?? null);
  const events = useMemo(() => buildEvents(annotations, chartData), [annotations, chartData]);

  const hostRef = useRef<HTMLDivElement | null>(null);
  const [placed, setPlaced] = useState<PlacedEvent[]>([]);

  // Latest events read inside recompute without re-subscribing every change.
  const eventsRef = useRef<EventItem[]>(events);
  eventsRef.current = visible ? events : [];

  const recompute = useCallback(() => {
    const chart = chartRef.current;
    const series = seriesRef.current;
    const evs = eventsRef.current;
    if (!chart || !series || evs.length === 0) {
      setPlaced((prev) => (prev.length ? [] : prev));
      return;
    }
    let timeScale: ReturnType<IChartApi['timeScale']>;
    try {
      timeScale = chart.timeScale();
    } catch {
      return;
    }
    const host = hostRef.current;
    const w = host?.clientWidth ?? 0;
    const next: PlacedEvent[] = [];
    for (const ev of evs) {
      let x: number | null;
      let y: number | null;
      try {
        x = timeScale.timeToCoordinate(ev.time as unknown as Time);
        y = series.priceToCoordinate(ev.price);
      } catch {
        continue;
      }
      if (x == null || y == null) continue;
      const cx = w > 0 ? Math.max(EDGE_X, Math.min(x, w - EDGE_X)) : x;
      next.push({ ...ev, x: cx, y, below: y < FLIP_Y });
    }
    setPlaced(next);
  }, [chartRef, seriesRef]);

  // Reposition on pan/zoom (logical range), resize (ResizeObserver), and chart
  // rebuilds (symbol/theme/data deps re-grab the current chart instance). The
  // initial placement waits a frame so the chart has laid out.
  useEffect(() => {
    const chart = chartRef.current;
    if (!chart) return;
    let timeScale: ReturnType<IChartApi['timeScale']>;
    try {
      timeScale = chart.timeScale();
    } catch {
      return;
    }
    const onRange = () => recompute();
    try {
      timeScale.subscribeVisibleLogicalRangeChange(onRange);
    } catch {
      /* chart disposed */
    }

    let ro: ResizeObserver | null = null;
    const host = hostRef.current;
    if (host && typeof ResizeObserver !== 'undefined') {
      ro = new ResizeObserver(() => recompute());
      ro.observe(host);
    }
    const raf = requestAnimationFrame(() => recompute());

    return () => {
      try {
        timeScale.unsubscribeVisibleLogicalRangeChange(onRange);
      } catch {
        /* already disposed */
      }
      ro?.disconnect();
      cancelAnimationFrame(raf);
    };
  }, [recompute, symbol, theme, visible, events, chartData, chartRef]);

  return (
    <div ref={hostRef} className="agent-event-overlay" aria-hidden={placed.length === 0}>
      {placed.map((ev) => (
        <React.Fragment key={ev.id}>
          <span
            className="agent-event-anchor"
            style={{ left: `${ev.x}px`, top: `${ev.y}px`, background: ev.color }}
          />
          <EventBadge ev={ev} hoverCapable={hoverCapable} />
        </React.Fragment>
      ))}
    </div>
  );
}

export default AgentEventOverlay;
