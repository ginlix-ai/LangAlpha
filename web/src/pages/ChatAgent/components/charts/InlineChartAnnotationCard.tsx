/**
 * Inline preview for an agent ``chart_annotation`` artifact in the chat
 * transcript.
 *
 * Two-stage on the standalone ChatAgent page (no live chart present):
 *   1. a full-bleed "spotlight" card — the symbol's real (clean) price chart;
 *      ticker, latest price, window change and an annotation legend float over
 *      soft scrims, all from the same bars (annotations are listed, not drawn);
 *   2. clicking opens a roomy modal with the full interactive chart (candles,
 *      MA, volume, RSI, the annotations) plus a button to open it in MarketView.
 *
 * Inside the MarketView desktop panel the real chart already shows the drawing
 * live, so the card collapses to a one-line confirmation chip (see
 * ChartSurfaceContext).
 */

import React, { Suspense, lazy, useCallback, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useLocation, useNavigate, useParams } from 'react-router-dom';
import * as DialogPrimitive from '@radix-ui/react-dialog';
import { AnimatePresence, motion, useReducedMotion } from 'framer-motion';
import { LineChart, ExternalLink, Check, ArrowRight, X } from 'lucide-react';

import {
  Dialog,
  DialogOverlay,
  DialogPortal,
  DialogClose,
  DialogTitle,
} from '@/components/ui/dialog';
import { useIsMobile } from '@/hooks/useIsMobile';
import type { StoredAnnotation } from '@/pages/MarketView/stores/chartAnnotationStore';
import {
  chartAnnotationStore,
  makeChartId,
  useDisplayCleared,
} from '@/pages/MarketView/stores/chartAnnotationStore';
import { useStockBars } from '@/pages/MarketView/hooks/useStockBars';
import { AUTO_FIT_BARS, INTERVAL_LABEL } from '@/lib/bars';
import { describeAnnotationVisual } from '@/pages/MarketView/utils/annotationGeometry';

import { useWorkspaceId } from '../../contexts/WorkspaceContext';
import { useChartSurface } from '../../contexts/ChartSurfaceContext';
import { AnnotationPreviewChart } from './AnnotationPreviewChart';

// Lazy: the surface pulls in the whole MarketView chart stack (lightweight-charts,
// html2canvas, TradingView). Keep it out of the chat bundle until a chart opens.
const MarketChartSurface = lazy(() =>
  import('@/pages/MarketView/components/MarketChartSurface').then((m) => ({
    default: m.MarketChartSurface,
  })),
);

const CARD_BG = 'var(--color-bg-tool-card)';
const CARD_BORDER = 'var(--color-border-muted)';
const TEXT_COLOR = 'var(--color-text-tertiary)';
const ACCENT = 'var(--color-accent-primary)';
const ACCENT_SOFT = 'var(--color-accent-soft)';

// Centered overlay for the chart's loading / empty states.
const CENTERED: React.CSSProperties = {
  position: 'absolute',
  inset: 0,
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'center',
};


// Translucent chrome that floats over the chart (theme-aware via color-mix).
const GLASS_BG = 'color-mix(in srgb, var(--color-bg-tool-card) 62%, transparent)';
const GLASS_BORDER = 'color-mix(in srgb, var(--color-text-primary) 16%, transparent)';
const SCRIM_TOP =
  'linear-gradient(to bottom, color-mix(in srgb, var(--color-bg-tool-card) 92%, transparent), transparent)';
const SCRIM_BOTTOM =
  'linear-gradient(to top, color-mix(in srgb, var(--color-bg-tool-card) 94%, transparent), transparent)';

// How many annotation chips to show in the floating legend before "+N".
const MAX_LEGEND = 3;

interface InlineChartAnnotationCardProps {
  artifact: Record<string, unknown> | null | undefined;
  onClick?: () => void;
}

export function InlineChartAnnotationCard({
  artifact,
}: InlineChartAnnotationCardProps): React.ReactElement | null {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const location = useLocation();
  const params = useParams();
  const ctxWorkspaceId = useWorkspaceId();
  const { chartPresent, activeSymbol, activeTimeframe, onJumpToChart } = useChartSurface();
  const isMobile = useIsMobile();
  const reduceMotion = useReducedMotion();

  const symbol = ((artifact?.symbol as string) || '').toUpperCase();
  const timeframe = (artifact?.timeframe as string) || '1day';
  const annotations = useMemo(
    () => (artifact?.annotations as StoredAnnotation[] | undefined) ?? [],
    [artifact],
  );
  const workspaceId = (artifact?.workspace_id as string | undefined) || ctxWorkspaceId || undefined;
  const threadId = params.threadId as string | undefined;

  // Whether this instance is currently cleared from the chart (MarketView only).
  const displayCleared = useDisplayCleared(workspaceId, symbol, timeframe);

  // Stage-2 modal open state (standalone ChatAgent page only).
  const [open, setOpen] = useState(false);
  // Card hover drives the CTA fill (glass → accent).
  const [hover, setHover] = useState(false);

  // Resting-card price preview: cached bars for this symbol/timeframe. Skipped
  // inside MarketView (chartPresent) where the card collapses to a chip.
  const { bars, isLoading: barsLoading } = useStockBars(symbol, timeframe, {
    enabled: !chartPresent,
  });

  // Re-apply a cleared drawing to the adjacent MarketView chart.
  const handleRestore = useCallback(() => {
    if (!workspaceId || !symbol) return;
    chartAnnotationStore.restoreDisplay(workspaceId, makeChartId(symbol, timeframe));
  }, [workspaceId, symbol, timeframe]);

  const handleOpenInMarketView = useCallback(() => {
    if (!symbol) return;
    const sp = new URLSearchParams();
    sp.set('symbol', symbol);
    sp.set('tf', timeframe);
    sp.set('mode', 'ptc');
    if (workspaceId) sp.set('ws', workspaceId);
    if (threadId && threadId !== '__default__') sp.set('thread', threadId);
    sp.set('returnTo', location.pathname + location.search);
    navigate(`/market?${sp.toString()}`);
  }, [symbol, timeframe, workspaceId, threadId, location, navigate]);

  if (!artifact || !symbol) return null;

  const count = annotations.length;

  // Inside MarketView: the real chart shows the drawing — collapse to a chip.
  // The chip is clickable. Three states:
  //  - different instance than what's on screen → jump the chart to it;
  //  - this instance but cleared from the chart → re-apply it;
  //  - this instance and showing → a passive confirmation.
  if (chartPresent) {
    const isActiveInstance =
      (activeSymbol ?? '').toUpperCase() === symbol &&
      (!activeTimeframe || activeTimeframe === timeframe);
    const canJump = !!onJumpToChart && !isActiveInstance;
    // Accent border invites a click whenever one would change the chart.
    const accented = canJump || displayCleared;

    const handleChipClick = (): void => {
      if (canJump) {
        onJumpToChart?.(symbol, timeframe);
        // Un-clear so the drawing shows once the chart switches to it.
        if (workspaceId) {
          chartAnnotationStore.restoreDisplay(workspaceId, makeChartId(symbol, timeframe));
        }
      } else {
        handleRestore();
      }
    };

    const title = canJump
      ? t('chat.chartAnnotationCard.chipJumpTitle', {
          symbol,
          timeframe: INTERVAL_LABEL[timeframe] ?? timeframe,
        })
      : displayCleared
        ? t('chat.chartAnnotationCard.chipShowTitle')
        : t('chat.chartAnnotationCard.chipShownTitle');

    return (
      <button
        type="button"
        onClick={handleChipClick}
        title={title}
        style={{
          display: 'inline-flex',
          alignItems: 'center',
          gap: 8,
          background: CARD_BG,
          border: `1px solid ${accented ? ACCENT : CARD_BORDER}`,
          borderRadius: 999,
          padding: '6px 12px',
          fontSize: 12,
          color: TEXT_COLOR,
          cursor: 'pointer',
          transition: 'border-color 0.15s',
        }}
        onMouseEnter={(e) => (e.currentTarget.style.borderColor = ACCENT)}
        onMouseLeave={(e) =>
          (e.currentTarget.style.borderColor = accented ? ACCENT : CARD_BORDER)
        }
      >
        {accented
          ? <LineChart size={14} style={{ color: ACCENT, flexShrink: 0 }} />
          : <Check size={14} style={{ color: 'var(--color-profit)', flexShrink: 0 }} />}
        <span>
          <span style={{ color: 'var(--color-text-primary)', fontWeight: 600 }}>{symbol}</span>
          <span style={{ color: TEXT_COLOR }}>{` · ${INTERVAL_LABEL[timeframe] ?? timeframe}`}</span>
          {' · '}
          {canJump
            ? t('chat.chartAnnotationCard.chipViewCount', { count })
            : displayCleared
              ? t('chat.chartAnnotationCard.chipShowCount', { count })
              : t('chat.chartAnnotationCard.chipShownCount', { count })}
        </span>
        {canJump && <ArrowRight size={13} style={{ color: ACCENT, flexShrink: 0 }} />}
      </button>
    );
  }

  // Render the same recent window the live chart auto-fits to (not the whole
  // fetched history), so the header %-change and curve match what opens.
  const fitBars = AUTO_FIT_BARS[timeframe] ?? 180;
  const viewBars = bars.length > fitBars ? bars.slice(-fitBars) : bars;

  // Price + window change from those same bars, so the header never disagrees
  // with the curve underneath it.
  const lastClose = viewBars.length ? viewBars[viewBars.length - 1].close : null;
  const firstClose = viewBars.length ? viewBars[0].close : null;
  const pct =
    lastClose != null && firstClose ? ((lastClose - firstClose) / firstClose) * 100 : null;
  const up = pct == null || pct >= 0;
  const trendColor = up ? 'var(--color-profit)' : 'var(--color-loss)';
  const hasChart = !barsLoading && viewBars.length >= 2;
  const plotHeight = isMobile ? 200 : 248;

  // The floating legend names the real annotations (they're listed here, not
  // drawn over the preview curve — the full set shows when the chart opens).
  const visuals = annotations.map(describeAnnotationVisual);
  const shownVisuals = visuals.slice(0, MAX_LEGEND);
  const extraCount = visuals.length - shownVisuals.length;

  // Stage 1 — the "spotlight" card: a clean full-bleed price chart; ticker /
  // price float over soft scrims, an annotation legend sits bottom-left, and the
  // CTA opens the full interactive chart (where the annotations are drawn).
  return (
    <>
      <div
        role="button"
        tabIndex={0}
        aria-label={t('chat.chartAnnotationCard.cardAria', { symbol, timeframe, count })}
        onClick={() => setOpen(true)}
        onKeyDown={(e) => {
          if (e.key === 'Enter' || e.key === ' ') {
            e.preventDefault();
            setOpen(true);
          }
        }}
        onMouseEnter={(e) => {
          setHover(true);
          e.currentTarget.style.borderColor = ACCENT;
          e.currentTarget.style.transform = 'translateY(-2px)';
          e.currentTarget.style.boxShadow =
            '0 2px 6px rgba(0,0,0,0.06), 0 26px 50px -20px rgba(0,0,0,0.6)';
        }}
        onMouseLeave={(e) => {
          setHover(false);
          e.currentTarget.style.borderColor = CARD_BORDER;
          e.currentTarget.style.transform = 'none';
          e.currentTarget.style.boxShadow =
            '0 1px 2px rgba(0,0,0,0.05), 0 16px 36px -18px rgba(0,0,0,0.5)';
        }}
        // Keyboard focus needs a visible ring (outline is suppressed for the
        // rounded card). Gate on :focus-visible so a mouse click stays clean.
        onFocus={(e) => {
          if (!e.currentTarget.matches(':focus-visible')) return;
          setHover(true);
          e.currentTarget.style.borderColor = ACCENT;
          e.currentTarget.style.transform = 'translateY(-2px)';
          e.currentTarget.style.boxShadow =
            `0 0 0 2px ${ACCENT}, 0 2px 6px rgba(0,0,0,0.06), 0 26px 50px -20px rgba(0,0,0,0.6)`;
        }}
        onBlur={(e) => {
          setHover(false);
          e.currentTarget.style.borderColor = CARD_BORDER;
          e.currentTarget.style.transform = 'none';
          e.currentTarget.style.boxShadow =
            '0 1px 2px rgba(0,0,0,0.05), 0 16px 36px -18px rgba(0,0,0,0.5)';
        }}
        style={{
          position: 'relative',
          background: CARD_BG,
          border: `1px solid ${CARD_BORDER}`,
          borderRadius: 20,
          overflow: 'hidden',
          cursor: 'pointer',
          outline: 'none',
          userSelect: 'none',
          boxShadow: '0 1px 2px rgba(0,0,0,0.05), 0 16px 36px -18px rgba(0,0,0,0.5)',
          transition: 'border-color 0.16s, box-shadow 0.16s, transform 0.16s',
        }}
      >
        {/* Full-bleed plot — the real chart, or a loading / empty fallback. */}
        <div style={{ position: 'relative', height: plotHeight }}>
          {barsLoading ? (
            <div style={CENTERED}>
              <span style={{ fontSize: 12, color: TEXT_COLOR }}>
                {t('chat.chartAnnotationCard.loadingChart')}
              </span>
            </div>
          ) : hasChart ? (
            // Clean price line only — the legend below conveys the annotations.
            <AnnotationPreviewChart
              bars={viewBars}
              trendColor={trendColor}
              showLastPrice
            />
          ) : (
            <div style={CENTERED}>
              <span
                style={{
                  display: 'inline-flex',
                  flexDirection: 'column',
                  alignItems: 'center',
                  gap: 6,
                  color: TEXT_COLOR,
                }}
              >
                <LineChart size={26} style={{ opacity: 0.5 }} />
                <span style={{ fontSize: 12 }}>
                  {t('chat.chartAnnotationCard.previewUnavailable')}
                </span>
              </span>
            </div>
          )}

          {/* Scrims keep the floating chrome legible over the chart. */}
          {hasChart && (
            <>
              <div
                style={{
                  position: 'absolute',
                  top: 0,
                  left: 0,
                  right: 0,
                  height: 78,
                  background: SCRIM_TOP,
                  pointerEvents: 'none',
                }}
              />
              <div
                style={{
                  position: 'absolute',
                  bottom: 0,
                  left: 0,
                  right: 0,
                  height: 92,
                  background: SCRIM_BOTTOM,
                  pointerEvents: 'none',
                }}
              />
            </>
          )}

          {/* Top-left — ticker, latest price, window change. */}
          <div
            style={{
              position: 'absolute',
              top: 15,
              left: 17,
              display: 'flex',
              alignItems: 'baseline',
              gap: 9,
              minWidth: 0,
            }}
          >
            <span
              style={{
                fontSize: 21,
                fontWeight: 700,
                color: 'var(--color-text-primary)',
                letterSpacing: '-0.01em',
              }}
            >
              {symbol}
            </span>
            {lastClose != null && (
              <span style={{ fontSize: 16, fontWeight: 700, color: 'var(--color-text-primary)' }}>
                ${lastClose.toLocaleString('en-US', {
                  minimumFractionDigits: 2,
                  maximumFractionDigits: 2,
                })}
              </span>
            )}
            {pct != null && (
              <span
                style={{
                  display: 'inline-flex',
                  alignItems: 'center',
                  gap: 3,
                  fontSize: 13,
                  fontWeight: 600,
                  color: trendColor,
                }}
              >
                <span
                  style={{
                    width: 0,
                    height: 0,
                    borderLeft: '3.5px solid transparent',
                    borderRight: '3.5px solid transparent',
                    ...(up
                      ? { borderBottom: `5px solid ${trendColor}` }
                      : { borderTop: `5px solid ${trendColor}` }),
                  }}
                />
                {pct >= 0 ? '+' : ''}
                {pct.toFixed(2)}%
              </span>
            )}
          </div>

          {/* Top-right — timeframe pill. */}
          <span
            style={{
              position: 'absolute',
              top: 16,
              right: 16,
              fontSize: 11,
              fontWeight: 700,
              letterSpacing: '0.03em',
              color: TEXT_COLOR,
              background: GLASS_BG,
              border: `1px solid ${GLASS_BORDER}`,
              backdropFilter: 'blur(8px)',
              padding: '4px 9px',
              borderRadius: 7,
            }}
          >
            {INTERVAL_LABEL[timeframe] ?? timeframe}
          </span>

          {/* Bottom-left — annotation legend (the real annotations). */}
          {hasChart && shownVisuals.length > 0 && (
            <div
              style={{
                position: 'absolute',
                bottom: 15,
                left: 17,
                display: 'flex',
                flexWrap: 'wrap',
                alignItems: 'center',
                gap: 14,
                maxWidth: '62%',
              }}
            >
              {/* `initial={false}` keeps the first paint (and history replay)
                  instant; only annotations that arrive while the pinned card is
                  already mounted animate in, so the legend grows smoothly as the
                  agent draws. */}
              <AnimatePresence initial={false}>
                {shownVisuals.map((v, i) => (
                  <motion.span
                    key={annotations[i]?.annotation_id || `legend-${i}`}
                    initial={reduceMotion ? false : { opacity: 0, y: 3, scale: 0.96 }}
                    animate={{ opacity: 1, y: 0, scale: 1 }}
                    exit={reduceMotion ? { opacity: 0 } : { opacity: 0, y: -3, scale: 0.96 }}
                    transition={{ duration: reduceMotion ? 0 : 0.2, ease: 'easeOut' }}
                    style={{
                      display: 'inline-flex',
                      alignItems: 'center',
                      gap: 6,
                      fontSize: 11.5,
                      fontWeight: 600,
                      color: 'var(--color-text-secondary)',
                    }}
                  >
                    <span
                      style={{ width: 8, height: 8, borderRadius: 2.5, backgroundColor: v.color, flexShrink: 0 }}
                    />
                    <span
                      style={{
                        overflow: 'hidden',
                        textOverflow: 'ellipsis',
                        whiteSpace: 'nowrap',
                        maxWidth: 130,
                      }}
                    >
                      {v.label}
                    </span>
                  </motion.span>
                ))}
              </AnimatePresence>
              {extraCount > 0 && (
                <span style={{ fontSize: 11.5, fontWeight: 600, color: TEXT_COLOR }}>+{extraCount}</span>
              )}
            </div>
          )}

          {/* Bottom-right — CTA (glass → accent on hover). */}
          <span
            style={{
              position: 'absolute',
              bottom: 14,
              right: 14,
              display: 'inline-flex',
              alignItems: 'center',
              gap: 7,
              fontSize: 13,
              fontWeight: 600,
              padding: '9px 15px',
              borderRadius: 11,
              backdropFilter: 'blur(8px)',
              background: hover ? ACCENT : GLASS_BG,
              border: `1px solid ${hover ? 'transparent' : GLASS_BORDER}`,
              color: hover ? '#fff' : 'var(--color-text-primary)',
              transition: 'background 0.16s, color 0.16s, border-color 0.16s',
            }}
          >
            {t('chat.chartAnnotationCard.openAnnotatedChart')}
            <ArrowRight
              size={14}
              style={{ transform: hover ? 'translateX(3px)' : 'none', transition: 'transform 0.16s' }}
            />
          </span>
        </div>
      </div>

      {/* Stage 2 — the modal: a ~3/4-viewport, self-contained replica of the
          MarketView chart surface (same header, toolbar, candles, MA, volume,
          RSI and the agent's annotations). */}
      <Dialog open={open} onOpenChange={setOpen}>
        <DialogPortal>
          <DialogOverlay />
          <DialogPrimitive.Content
            aria-describedby={undefined}
            className="fixed left-1/2 top-1/2 z-[1030] flex flex-col -translate-x-1/2 -translate-y-1/2 overflow-hidden rounded-xl border bg-background shadow-lg data-[state=open]:animate-in data-[state=closed]:animate-out data-[state=closed]:fade-out-0 data-[state=open]:fade-in-0 data-[state=closed]:zoom-out-95 data-[state=open]:zoom-in-95"
            style={{
              width: isMobile ? '96vw' : '75vw',
              height: isMobile ? '88vh' : '80vh',
              maxWidth: 'none',
              maxHeight: '94vh',
            }}
          >
            <DialogTitle className="sr-only">
              {t('chat.chartAnnotationCard.dialogTitle', {
                symbol,
                timeframe: INTERVAL_LABEL[timeframe] ?? timeframe,
                count,
              })}
            </DialogTitle>

            {/* Slim modal chrome — keeps the close + "Open in MarketView"
                buttons off the chart's own header (which has the price). */}
            <div
              style={{
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'flex-end',
                gap: 8,
                padding: '8px 10px',
                borderBottom: `1px solid ${CARD_BORDER}`,
                flexShrink: 0,
              }}
            >
              <button
                type="button"
                onClick={handleOpenInMarketView}
                style={{
                  display: 'inline-flex',
                  alignItems: 'center',
                  gap: 6,
                  padding: '6px 12px',
                  borderRadius: 8,
                  border: `1px solid ${CARD_BORDER}`,
                  background: ACCENT_SOFT,
                  color: ACCENT,
                  fontSize: 12,
                  fontWeight: 600,
                  cursor: 'pointer',
                  whiteSpace: 'nowrap',
                  transition: 'border-color 0.15s',
                }}
                onMouseEnter={(e) => (e.currentTarget.style.borderColor = ACCENT)}
                onMouseLeave={(e) => (e.currentTarget.style.borderColor = CARD_BORDER)}
              >
                {t('chat.chartAnnotationCard.openInMarketView')}
                <ExternalLink size={13} />
              </button>
              <DialogClose
                aria-label={t('chat.chartAnnotationCard.close')}
                style={{
                  display: 'inline-flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  width: 30,
                  height: 30,
                  borderRadius: 8,
                  border: `1px solid ${CARD_BORDER}`,
                  background: 'transparent',
                  color: TEXT_COLOR,
                  cursor: 'pointer',
                }}
              >
                <X size={16} />
              </DialogClose>
            </div>

            {/* The chart surface fills the rest — mounted only while open. */}
            <div style={{ flex: 1, minHeight: 0 }}>
              {open && (
                <Suspense
                  fallback={
                    <div
                      style={{
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'center',
                        height: '100%',
                        fontSize: 13,
                        color: TEXT_COLOR,
                      }}
                    >
                      {t('chat.chartAnnotationCard.loadingChart')}
                    </div>
                  }
                >
                  <MarketChartSurface
                    symbol={symbol}
                    timeframe={timeframe}
                    workspaceId={workspaceId ?? null}
                  />
                </Suspense>
              )}
            </div>
          </DialogPrimitive.Content>
        </DialogPortal>
      </Dialog>
    </>
  );
}

export default InlineChartAnnotationCard;
