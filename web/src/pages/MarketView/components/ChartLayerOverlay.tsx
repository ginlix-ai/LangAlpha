/**
 * The explain layer over an agent's chart layers: hover an element that
 * carries a note or a label and a small card says what it is and why the agent
 * drew it; click pins the card, with "Ask about this", which hands the element
 * to the composer beside the chart (`useChartComposer`), as the layer legend
 * does.
 *
 * What is under the pointer comes from lightweight-charts' own hover test: the
 * layer primitive reports its drawings (`hitTest`), markers and price lines
 * carry the element key as their id, and a hovered series maps back through the
 * hook's series table. The card is DOM, positioned from the crosshair point and
 * re-anchored to its (logical, price) point on every redraw of its pane while
 * pinned.
 */

import React, { useEffect, useEffectEvent, useLayoutEffect, useRef, useState, type RefObject } from 'react';
import { useTranslation } from 'react-i18next';
import { Check, MessageSquarePlus, X } from 'lucide-react';
import type { IChartApi, IPanePrimitive, ISeriesApi, Logical, MouseEventParams, SeriesType, Time } from 'lightweight-charts';

import { useChartComposer } from '@/components/chart/chartComposer';
import { buttonSlot } from '@/components/ui/button-class';
import { useToast } from '@/components/ui/use-toast';
import { cn } from '@/lib/utils';

import type { LayerTime } from '../types/chartLayer';
import { nearestBarIndex } from '../utils/barTime';
import { withAlpha } from '../utils/canvasKit';
import type { ChartLayerPrimitive } from '../utils/chartLayer/ChartLayerPrimitive';
import { cleanAnchors, type LayerElementInfo } from '../utils/chartLayer/elements';
import { layerTimeToSeconds } from '../utils/chartLayer/timeGrid';
import { cardPosition, type CardMetrics } from '../utils/layerCardPosition';
import './ChartLayerOverlay.css';

/** Dwell before a hover card opens, so sweeping across the chart does not strobe cards. */
const HOVER_OPEN_MS = 140;
/** How long "Added to chat" shows before the pinned card closes. */
const ASKED_CLOSE_MS = 900;
const CARD_W = 280;
/** The card as placed before it is measured. */
const CARD: CardMetrics = { width: CARD_W, height: 120, gap: 14, edge: 8 };

interface Bar {
  time: number;
  close: number;
}

interface CardState {
  key: string;
  /** Host-relative pointer position. */
  x: number;
  y: number;
}

interface PinState extends CardState {
  logical: number;
  /** Price under the click, when it was on the price pane (re-anchors y on zoom). */
  price: number | null;
  paneIndex: number;
  paneTop: number;
}

interface ChartLayerOverlayProps {
  /** The chart, once its host has made it. */
  chart: IChartApi | null;
  seriesRef: RefObject<ISeriesApi<'Candlestick'> | null>;
  primitiveRef: RefObject<ChartLayerPrimitive | null>;
  seriesKeysRef: RefObject<Map<ISeriesApi<SeriesType>, string>>;
  elements: Map<string, LayerElementInfo>;
  chartData: readonly Bar[] | null;
  /** Off while a selection tool is armed: its gestures own the chart. */
  enabled: boolean;
  /** Whether a card is open: it stands in for the host's OHLC tooltip meanwhile. */
  onCardOpenChange: (open: boolean) => void;
}

function paneTopOf(chart: IChartApi, paneIndex: number, host: HTMLElement | null): number {
  if (!host || paneIndex <= 0) return 0;
  try {
    const el = chart.panes()[paneIndex]?.getHTMLElement();
    if (!el) return 0;
    return el.getBoundingClientRect().top - host.getBoundingClientRect().top;
  } catch {
    return 0;
  }
}

/** The element key under the pointer, if it is one this overlay explains. */
function hoveredKey(
  param: MouseEventParams<Time>,
  elements: Map<string, LayerElementInfo>,
  seriesKeys: Map<ISeriesApi<SeriesType>, string>,
  prim: ChartLayerPrimitive | null,
): string | null {
  const info = param.hoveredInfo;
  const id = info?.objectId ?? param.hoveredObjectId;
  if (typeof id === 'string' && elements.has(id)) return id;
  const series = info?.series ?? param.hoveredSeries;
  if (series) {
    const key = seriesKeys.get(series);
    if (key && elements.has(key)) return key;
  }
  // Fallback for the price pane: the primitive's own last-frame shapes.
  if (prim && param.point && (param.paneIndex ?? 0) === 0) {
    const hit = prim.hitAt(param.point.x, param.point.y);
    if (hit && elements.has(hit.key)) return hit.key;
  }
  return null;
}

export const ChartLayerOverlay = React.memo(function ChartLayerOverlay({
  chart,
  seriesRef,
  primitiveRef,
  seriesKeysRef,
  elements,
  chartData,
  enabled,
  onCardOpenChange,
}: ChartLayerOverlayProps): React.ReactElement | null {
  const { t } = useTranslation();
  const { toast } = useToast();
  const composer = useChartComposer();
  const [host, setHost] = useState<HTMLDivElement | null>(null);
  const [hostSize, setHostSize] = useState({ w: 800, h: 500 });
  const cardRef = useRef<HTMLDivElement | null>(null);
  const hoverTimer = useRef<number | null>(null);
  const [hover, setHover] = useState<CardState | null>(null);
  const [pin, setPin] = useState<PinState | null>(null);
  const [asked, setAsked] = useState(false);
  const [cardH, setCardH] = useState(CARD.height);

  // The host's size places the card; it follows the chart's.
  useLayoutEffect(() => {
    if (!host) return;
    const read = () => setHostSize({ w: host.clientWidth, h: host.clientHeight });
    read();
    const ro = new ResizeObserver(read);
    ro.observe(host);
    return () => ro.disconnect();
  }, [host]);

  // Elements went away (layer hidden or replaced): drop a card that points at one.
  useEffect(() => {
    if (pin && !elements.has(pin.key)) setPin(null);
    if (hover && !elements.has(hover.key)) setHover(null);
  }, [elements, pin, hover]);

  // Emphasize the element the card is about.
  useEffect(() => {
    primitiveRef.current?.setHovered(pin?.key ?? hover?.key ?? null);
  }, [pin, hover, primitiveRef]);

  const cardOpen = !!(pin ?? hover);
  useEffect(() => {
    onCardOpenChange(cardOpen);
  }, [cardOpen, onCardOpenChange]);
  useEffect(() => () => onCardOpenChange(false), [onCardOpenChange]);

  // ── Hover + click ──
  const stopHoverTimer = () => {
    if (hoverTimer.current != null) window.clearTimeout(hoverTimer.current);
    hoverTimer.current = null;
  };
  const keyAt = (param: MouseEventParams<Time>) =>
    hoveredKey(param, elements, seriesKeysRef.current, primitiveRef.current);

  const onMove = useEffectEvent((param: MouseEventParams<Time>) => {
    if (!chart || !enabled || !param.point) {
      stopHoverTimer();
      setHover(null);
      return;
    }
    const key = keyAt(param);
    if (!key) {
      stopHoverTimer();
      if (hover) setHover(null);
      return;
    }
    const top = paneTopOf(chart, param.paneIndex ?? 0, host);
    const next = { key, x: param.point.x, y: param.point.y + top };
    if (hover?.key === key) {
      setHover(next);
      return;
    }
    stopHoverTimer();
    hoverTimer.current = window.setTimeout(() => setHover(next), HOVER_OPEN_MS);
  });

  const onClick = useEffectEvent((param: MouseEventParams<Time>) => {
    if (!chart || !enabled) return;
    const key = param.point ? keyAt(param) : null;
    if (!key || !param.point) {
      setPin(null);
      return;
    }
    const paneIndex = param.paneIndex ?? 0;
    const paneTop = paneTopOf(chart, paneIndex, host);
    let price: number | null = null;
    if (paneIndex === 0) {
      try {
        price = seriesRef.current?.coordinateToPrice(param.point.y) ?? null;
      } catch {
        price = null;
      }
    }
    setAsked(false);
    setHover(null);
    setPin({
      key,
      x: param.point.x,
      y: param.point.y + paneTop,
      logical: (param.logical as number | undefined) ?? 0,
      price,
      paneIndex,
      paneTop,
    });
  });

  const hasElements = elements.size > 0;
  useEffect(() => {
    if (!chart || !enabled || !hasElements) {
      // A pinned card would stay over the chart, taking the gestures a
      // selection tool now owns.
      setHover(null);
      setPin(null);
      return;
    }
    const move = (param: MouseEventParams<Time>) => onMove(param);
    const click = (param: MouseEventParams<Time>) => onClick(param);
    chart.subscribeCrosshairMove(move);
    chart.subscribeClick(click);
    return () => {
      stopHoverTimer();
      try {
        chart.unsubscribeCrosshairMove(move);
        chart.unsubscribeClick(click);
      } catch {
        /* disposed */
      }
    };
  }, [chart, enabled, hasElements]);

  // ── Keep a pinned card on its point as the chart moves under it ──
  const reanchor = useEffectEvent(() => {
    if (!chart || !pin) return;
    let x: number | null = null;
    let y: number | null = null;
    try {
      x = chart.timeScale().logicalToCoordinate(pin.logical as Logical);
      if (pin.price != null) y = seriesRef.current?.priceToCoordinate(pin.price) ?? null;
    } catch {
      return;
    }
    const top = paneTopOf(chart, pin.paneIndex, host);
    setPin((p) => {
      if (!p || p.key !== pin.key) return p;
      const nx = x ?? p.x;
      // Off the price pane there is no price to follow, only the pane's own top.
      const ny = y != null ? y + top : p.y - p.paneTop + top;
      return nx === p.x && ny === p.y && top === p.paneTop ? p : { ...p, x: nx, y: ny, paneTop: top };
    });
  });
  const pinned = pin !== null;
  const pinPane = pin?.paneIndex ?? null;
  useEffect(() => {
    if (!chart || pinPane == null) return;
    const pane = chart.panes()[pinPane];
    if (!pane) return;
    let raf = 0;
    // lightweight-charts has no event for a pan or zoom of a price scale, a pane
    // resized, or an autoscale on a new bar, but it updates every primitive's
    // views on each redraw of the pane, which is when the layer moves too. A
    // primitive that draws nothing hears all of them, the time scale's included,
    // and nothing at all while the chart is still.
    const watcher: IPanePrimitive<Time> = {
      updateAllViews: () => {
        if (raf) return;
        raf = requestAnimationFrame(() => {
          raf = 0;
          reanchor();
        });
      },
    };
    pane.attachPrimitive(watcher);
    return () => {
      if (raf) cancelAnimationFrame(raf);
      try {
        pane.detachPrimitive(watcher);
      } catch {
        /* disposed */
      }
    };
  }, [chart, pinPane]);

  // Esc closes a pinned card.
  useEffect(() => {
    if (!pinned) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') setPin(null);
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [pinned]);

  // "Added to chat" shows a moment, then the card closes.
  useEffect(() => {
    if (!asked) return;
    const id = window.setTimeout(() => {
      setPin(null);
      setAsked(false);
    }, ASKED_CLOSE_MS);
    return () => window.clearTimeout(id);
  }, [asked]);

  // Measure the card so it flips cleanly near the edges.
  useLayoutEffect(() => {
    const h = cardRef.current?.offsetHeight;
    if (h && Math.abs(h - cardH) > 1) setCardH(h);
  }, [pin, hover, asked, cardH]);

  const closeAt = (time: LayerTime): number | null => {
    const secs = layerTimeToSeconds(time);
    const k = secs == null ? -1 : nearestBarIndex(chartData, secs);
    return k < 0 ? null : chartData![k].close;
  };

  const ask = (info: LayerElementInfo) => {
    const handled = composer.ask({
      symbol: info.symbol,
      timeframe: info.timeframe,
      layerId: info.ref.layerId,
      layerTitle: info.ref.layerTitle,
      summary: info.layerSummary,
      element: { ...info.ref, anchors: cleanAnchors(info.ref.anchors, closeAt) },
    });
    if (!handled) {
      toast({ title: t('marketView.layers.askUnavailable') });
      return;
    }
    setAsked(true);
  };

  const active = pin ?? hover;
  const info = active ? elements.get(active.key) : undefined;
  if (!hasElements) return null;

  const pos = active ? cardPosition(active.x, active.y, hostSize.w, hostSize.h, { ...CARD, height: cardH }) : null;
  const typeName = info ? t(info.typeKey) : '';
  const title = info ? (info.label ?? typeName) : '';

  return (
    <div ref={setHost} className="chart-layer-overlay">
      {info && pos && (
        <div
          ref={cardRef}
          className={cn('chart-layer-card', pin && 'chart-layer-card--pinned')}
          style={{ left: pos.left, top: pos.top, width: CARD_W }}
          role={pin ? 'dialog' : 'tooltip'}
          aria-label={title}
        >
          <div className="chart-layer-card-kicker">
            <span
              className="chart-layer-card-dot"
              style={info.color ? { backgroundColor: withAlpha(info.color, 1) } : undefined}
            />
            <span className="chart-layer-card-type">{typeName}</span>
            <span className="chart-layer-card-sep">·</span>
            <span className="chart-layer-card-layer">{info.ref.layerTitle}</span>
            {pin && (
              <button
                type="button"
                {...buttonSlot('ghost')}
                className="chart-layer-card-close tap-target"
                aria-label={t('marketView.layerExplain.close')}
                onClick={() => setPin(null)}
              >
                <X size={13} />
              </button>
            )}
          </div>
          <div className="chart-layer-card-title">{title}</div>
          {info.ref.note && <p className="chart-layer-card-note">{info.ref.note}</p>}
          {pin ? (
            <div className="chart-layer-card-actions">
              <button
                type="button"
                {...buttonSlot('outline')}
                className="chart-layer-card-ask press tap-target"
                onClick={() => ask(info)}
                disabled={asked}
              >
                {asked ? <Check size={13} /> : <MessageSquarePlus size={13} />}
                {asked ? t('marketView.layerExplain.asked') : t('marketView.layerExplain.ask')}
              </button>
            </div>
          ) : (
            <div className="chart-layer-card-hint">{t('marketView.layerExplain.pinHint')}</div>
          )}
        </div>
      )}
    </div>
  );
});

export default ChartLayerOverlay;
