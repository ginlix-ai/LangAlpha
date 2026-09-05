/**
 * Comment markers + composer for chart selections (Figma-style commenting).
 *
 * Each confirmed selection gets a numbered **pin** anchored to its top-right
 * corner; clicking a pin re-opens its note. The selection whose editor is open
 * shows a **composer pill** ("Add a comment…") next to its pin — a send button
 * (or Enter) confirms. For a freshly-drawn (pending) selection, "send" adds it
 * to the context; for a confirmed one re-opened from its pin/chip, it saves the
 * edited note. The ✕ button **discards** the whole selection; clicking outside
 * the composer (or Esc) just closes the editor, keeping a confirmed selection.
 *
 * DOM overlays anchored via the chart's coordinate API and repositioned on
 * pan/zoom/resize. Mounts inside `chart-wrapper` (Light/custom mode only),
 * alongside the crosshair tooltip and the agent event overlay.
 */

import React, {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type RefObject,
} from 'react';
import { useTranslation } from 'react-i18next';
import { Check, Plus, X } from 'lucide-react';
import type { IChartApi, ISeriesApi, Time } from 'lightweight-charts';

import { chartSelectionStore, isConfirmedFor, useChartSelections, type ChartSelection } from '../stores/chartSelectionStore';
import { toUnixSeconds } from '../utils/annotationGeometry';
import './SelectionCommentOverlay.css';

const GAP = 8;
const EDGE = 4;
const EDGE_Y = 12;
const PIN = 26; // pin box used for placement
const EST_W = 308; // composer estimate before it is measured
const EST_H = 40;
const MAX_COMMENT = 500;

interface SelectionCommentOverlayProps {
  chartRef: RefObject<IChartApi | null>;
  seriesRef: RefObject<ISeriesApi<'Candlestick'> | null>;
  /** Uppercased symbol currently on screen. */
  symbol: string;
  /** Normalized timeframe currently on screen. */
  timeframe: string;
}

interface PinLayout {
  id: string;
  n: number;
  left: number;
  top: number;
}

interface ComposerLayout {
  n: number;
  left: number;
  top: number;
}

// Memoized — all props are stable refs or primitives; pin/composer updates
// arrive through the selection store subscription, not through the parent
// chart's renders.
export const SelectionCommentOverlay = React.memo(function SelectionCommentOverlay({
  chartRef,
  seriesRef,
  symbol,
  timeframe,
}: SelectionCommentOverlayProps): React.ReactElement {
  const { t } = useTranslation();
  const { selections, activeId } = useChartSelections();

  // Confirmed selections on the current chart, in draw order → their numbers.
  const confirmed = useMemo(
    () => selections.filter((s) => isConfirmedFor(s, symbol, timeframe)),
    [selections, symbol, timeframe],
  );

  // The selection being edited, when it belongs to the chart on screen.
  const active = useMemo(() => {
    if (!activeId) return null;
    const sel = selections.find((s) => s.id === activeId);
    if (!sel || sel.symbol !== symbol || sel.timeframe !== timeframe) return null;
    return sel;
  }, [selections, activeId, symbol, timeframe]);

  // A pending draft has no confirmed index yet — it gets the next number.
  const numberOf = useCallback(
    (id: string) => {
      const i = confirmed.findIndex((s) => s.id === id);
      return i >= 0 ? i + 1 : confirmed.length + 1;
    },
    [confirmed],
  );

  const hostRef = useRef<HTMLDivElement | null>(null);
  const cardRef = useRef<HTMLDivElement | null>(null);
  const inputRef = useRef<HTMLInputElement | null>(null);
  const confirmedRef = useRef(confirmed);
  confirmedRef.current = confirmed;
  const activeRef = useRef<ChartSelection | null>(active);
  activeRef.current = active;
  const numberOfRef = useRef(numberOf);
  numberOfRef.current = numberOf;

  const [pins, setPins] = useState<PinLayout[]>([]);
  const [composer, setComposer] = useState<ComposerLayout | null>(null);
  const [text, setText] = useState('');

  // Seed the local draft + focus whenever a different selection becomes active.
  const activeKey = active?.id ?? null;
  useEffect(() => {
    if (!activeKey) return;
    setText(activeRef.current?.comment ?? '');
    const raf = requestAnimationFrame(() => {
      const el = inputRef.current;
      if (el) {
        el.focus();
        const len = el.value.length;
        el.setSelectionRange(len, len);
      }
    });
    return () => cancelAnimationFrame(raf);
  }, [activeKey]);

  const recompute = useCallback(() => {
    const chart = chartRef.current;
    const series = seriesRef.current;
    if (!chart || !series) {
      setPins([]);
      setComposer(null);
      return;
    }
    let timeScale: ReturnType<IChartApi['timeScale']>;
    try {
      timeScale = chart.timeScale();
    } catch {
      return;
    }
    const host = hostRef.current;
    const paneW = host?.clientWidth ?? 0;
    const paneH = host?.clientHeight ?? 0;

    // The selection's right + left (time) edges and top (high price) edge. A
    // price level spans full width, so both edges are the right side.
    const anchorOf = (sel: ChartSelection): { axRight: number; axLeft: number; ay: number } => {
      const coord = (iso?: string): number | null => {
        const u = iso ? toUnixSeconds(iso) : null;
        if (u == null) return null;
        try { return timeScale.timeToCoordinate(u as unknown as Time); } catch { return null; }
      };
      let axRight: number;
      let axLeft: number;
      if (sel.selectionType === 'region' && sel.timeStart && sel.timeEnd) {
        axRight = coord(sel.timeEnd) ?? paneW;
        axLeft = coord(sel.timeStart) ?? 0;
      } else {
        axRight = paneW;
        axLeft = paneW;
      }
      axRight = Math.max(0, Math.min(axRight, paneW));
      axLeft = Math.max(0, Math.min(axLeft, paneW));
      const ay = Math.max(0, Math.min(series.priceToCoordinate(sel.priceHigh) ?? EDGE_Y, paneH));
      return { axRight, axLeft, ay };
    };

    // Sit beside the region, just past its right edge and top-aligned with its
    // top edge. Flip to the left of the region when there's no room on the right.
    const place = (axRight: number, axLeft: number, ay: number, w: number, h: number) => {
      let left = axRight + GAP;
      if (left + w + EDGE > paneW) left = axLeft - GAP - w;
      const top = ay;
      return {
        left: Math.max(EDGE, Math.min(left, Math.max(EDGE, paneW - w - EDGE))),
        top: Math.max(EDGE, Math.min(top, Math.max(EDGE, paneH - h - EDGE))),
      };
    };

    const aId = activeRef.current?.id ?? null;
    const nextPins: PinLayout[] = [];
    confirmedRef.current.forEach((sel, i) => {
      if (sel.id === aId) return; // the active one shows the composer instead
      const { axRight, axLeft, ay } = anchorOf(sel);
      const { left, top } = place(axRight, axLeft, ay, PIN, PIN);
      nextPins.push({ id: sel.id, n: i + 1, left, top });
    });
    setPins(nextPins);

    const act = activeRef.current;
    if (act) {
      const { axRight, axLeft, ay } = anchorOf(act);
      const w = cardRef.current?.offsetWidth || EST_W;
      const h = cardRef.current?.offsetHeight || EST_H;
      const { left, top } = place(axRight, axLeft, ay, w, h);
      setComposer({ left, top, n: numberOfRef.current(act.id) });
    } else {
      setComposer(null);
    }
  }, [chartRef, seriesRef]);

  // Reposition on pan/zoom + resize + selection changes, coalesced per frame.
  useEffect(() => {
    const chart = chartRef.current;
    if (!chart) {
      setPins([]);
      setComposer(null);
      return;
    }
    let timeScale: ReturnType<IChartApi['timeScale']>;
    try {
      timeScale = chart.timeScale();
    } catch {
      return;
    }
    let scheduled = 0;
    const schedule = () => {
      if (scheduled) return;
      scheduled = requestAnimationFrame(() => {
        scheduled = 0;
        recompute();
      });
    };
    const onRange = () => schedule();
    try {
      timeScale.subscribeVisibleLogicalRangeChange(onRange);
    } catch {
      /* disposed */
    }
    let ro: ResizeObserver | null = null;
    const host = hostRef.current;
    if (host && typeof ResizeObserver !== 'undefined') {
      ro = new ResizeObserver(() => schedule());
      ro.observe(host);
    }
    const raf = requestAnimationFrame(() => recompute());
    return () => {
      try {
        timeScale.unsubscribeVisibleLogicalRangeChange(onRange);
      } catch {
        /* disposed */
      }
      ro?.disconnect();
      cancelAnimationFrame(raf);
      if (scheduled) cancelAnimationFrame(scheduled);
    };
    // Key on the primitive activeKey, not the memo'd `active` object: recompute
    // reads selections via refs, so this only needs to re-run when the active
    // *identity* changes. Same reasoning as the outside-click effect below.
  }, [recompute, confirmed, activeKey, chartRef]);

  const onPrimary = useCallback(() => {
    const sel = activeRef.current;
    if (!sel) return;
    const note = text.trim();
    if (sel.status === 'pending') {
      chartSelectionStore.confirm(sel.id, note);
    } else {
      chartSelectionStore.setComment(sel.id, note);
      chartSelectionStore.closeEditor();
    }
  }, [text]);

  // ✕ — always discard the whole selection (region + note).
  const onDiscard = useCallback(() => {
    const sel = activeRef.current;
    if (sel) chartSelectionStore.remove(sel.id);
  }, []);

  // Click-outside / Esc — just close the editor. A confirmed selection is kept
  // (with its saved note); a never-confirmed draft has nothing to keep, so it
  // is discarded rather than orphaned on the chart.
  const onDismiss = useCallback(() => {
    const sel = activeRef.current;
    if (!sel) return;
    if (sel.status === 'pending') chartSelectionStore.remove(sel.id);
    else chartSelectionStore.closeEditor();
  }, []);

  const onKeyDown = useCallback(
    (e: React.KeyboardEvent<HTMLInputElement>) => {
      e.stopPropagation(); // keep chart-level Esc-to-disarm from firing
      if (e.key === 'Enter' && !e.shiftKey) {
        e.preventDefault();
        onPrimary();
      } else if (e.key === 'Escape') {
        e.preventDefault();
        onDismiss();
      }
    },
    [onPrimary, onDismiss],
  );

  // Dismiss the editor when the user interacts anywhere outside the composer
  // card. Capture phase so it runs before the chart's own pointer handling; the
  // opening click already fired before this listener attaches, so it can't
  // self-dismiss.
  useEffect(() => {
    if (!activeKey) return;
    const handler = (e: PointerEvent) => {
      const card = cardRef.current;
      if (card && e.target instanceof Node && card.contains(e.target)) return;
      onDismiss();
    };
    document.addEventListener('pointerdown', handler, true);
    return () => document.removeEventListener('pointerdown', handler, true);
    // Key on the primitive activeKey, not the memo'd `active` object, so the
    // listener isn't re-subscribed on unrelated re-renders (pan/zoom).
  }, [activeKey, onDismiss]);

  return (
    <div ref={hostRef} className="selection-comment-overlay">
      {pins.map((p) => (
        <button
          key={p.id}
          type="button"
          className="selection-pin"
          style={{ left: `${p.left}px`, top: `${p.top}px` }}
          title={t('marketView.selection.editChip')}
          onPointerDown={(e) => e.stopPropagation()}
          onClick={() => chartSelectionStore.openEditor(p.id)}
        >
          {p.n}
        </button>
      ))}
      {active && composer && (
        <div
          ref={cardRef}
          className="selection-composer"
          style={{ left: `${composer.left}px`, top: `${composer.top}px` }}
          onPointerDown={(e) => e.stopPropagation()}
        >
          <span className="selection-pin selection-pin--composer">{composer.n}</span>
          <div className="selection-composer-pill clips-focus-ring">
            <input
              ref={inputRef}
              className="selection-composer-input"
              type="text"
              maxLength={MAX_COMMENT}
              value={text}
              onChange={(e) => setText(e.target.value)}
              onKeyDown={onKeyDown}
              placeholder={t('marketView.selection.commentPlaceholder')}
            />
            <button
              type="button"
              className="selection-composer-send"
              aria-label={active.status === 'pending'
                ? t('marketView.selection.commentAdd')
                : t('marketView.selection.commentSave')}
              onClick={onPrimary}
            >
              {active.status === 'pending' ? <Plus size={16} /> : <Check size={15} />}
            </button>
          </div>
          <button
            type="button"
            className="selection-composer-cancel"
            aria-label={t('marketView.selection.removeChip')}
            onClick={onDiscard}
          >
            <X size={14} />
          </button>
        </div>
      )}
    </div>
  );
});

export default SelectionCommentOverlay;
