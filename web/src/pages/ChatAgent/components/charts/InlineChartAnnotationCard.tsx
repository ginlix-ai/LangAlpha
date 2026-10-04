/**
 * An agent's annotated chart in the chat transcript, drawn as the same card as
 * a turn's files so everything a turn produced reads as one kind of object. It
 * fetches nothing: the artifact already names the symbol, timeframe and
 * annotations, and the drawing itself lives on the chart it opens.
 *
 * Inside the MarketView desktop panel the real chart already shows the drawing
 * live, so the card collapses to a one-line confirmation chip (see
 * ChartSurfaceContext).
 */

import React, { useCallback, useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import { LineChart, Check, ArrowRight, ChartCandlestick, PanelRight } from 'lucide-react';
import { useMessageActions } from '../messageList/MessageActionsContext';

import type { StoredAnnotation } from '@/pages/MarketView/stores/chartAnnotationStore';
import {
  chartAnnotationStore,
  makeChartId,
  useDisplayCleared,
} from '@/pages/MarketView/stores/chartAnnotationStore';
import { INTERVAL_LABEL, displaySpelling } from '@/lib/bars';
import { annotationLabel } from '@/pages/MarketView/utils/annotationGeometry';

import { useWorkspaceId } from '../../contexts/WorkspaceContext';
import { useChartSurface } from '../../contexts/ChartSurfaceContext';
import { CARD_BG, CARD_BORDER } from './inlineCardsShared';
import '../messageList/TurnFileCards.css';

const TEXT_COLOR = 'var(--color-text-tertiary)';
const ACCENT = 'var(--color-accent-primary)';

/** Stands on the tile where a file card shows its kind's icon. */
const CHART_GLYPH = ChartCandlestick;

interface InlineChartAnnotationCardProps {
  artifact: Record<string, unknown> | null | undefined;
}

export function InlineChartAnnotationCard({
  artifact,
}: InlineChartAnnotationCardProps): React.ReactElement | null {
  const { t } = useTranslation();
  const { onOpenChart } = useMessageActions();
  const ctxWorkspaceId = useWorkspaceId();
  const { chartPresent, activeSymbol, activeTimeframe, onJumpToChart } = useChartSurface();

  // An artifact from an older thread may spell Shanghai `.SS`; the chart, its
  // annotation keys and the tab all use the display spelling.
  const symbol = displaySpelling((artifact?.symbol as string) || '');
  const timeframe = (artifact?.timeframe as string) || '1day';
  const annotations = useMemo(
    () => (artifact?.annotations as StoredAnnotation[] | undefined) ?? [],
    [artifact],
  );
  const workspaceId = (artifact?.workspace_id as string | undefined) || ctxWorkspaceId || undefined;
  // A transcript mounted with no workspace is a share: the owner's hosts always
  // name one. The drawing lives in a workspace's store, so a chart opened from
  // here carries none, and the card must not promise it.
  const pricesOnly = ctxWorkspaceId === null;

  // Whether this instance is currently cleared from the chart (MarketView only).
  const displayCleared = useDisplayCleared(workspaceId, symbol, timeframe);

  // Re-apply a cleared drawing to the adjacent MarketView chart.
  const handleRestore = useCallback(() => {
    if (!workspaceId || !symbol) return;
    chartAnnotationStore.restoreDisplay(workspaceId, makeChartId(symbol, timeframe));
  }, [workspaceId, symbol, timeframe]);

  // Every host that draws this card can open the chart: the chat lands it in
  // a tab beside the transcript (MarketView on mobile) and a share in its
  // panel, so the card only says which one. It also asks for the drawing, so
  // one the user cleared from that chart comes back, as the MarketView chip
  // does it.
  const handleOpen = useCallback(() => {
    if (!symbol) return;
    if (workspaceId) chartAnnotationStore.restoreDisplay(workspaceId, makeChartId(symbol, timeframe));
    onOpenChart?.({ symbol, timeframe, workspaceId });
  }, [symbol, timeframe, workspaceId, onOpenChart]);

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
          fontSize: '0.75rem',
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

  const interval = INTERVAL_LABEL[timeframe] ?? timeframe;
  // Like a file's name carrying the extension its tile shows, so two charts of
  // one symbol stay apart in the transcript and in the accessible name.
  const name = t('chat.chartAnnotationCard.name', { symbol, interval });
  const labels = annotations
    .map(annotationLabel)
    .join(t('chat.chartAnnotationCard.labelSeparator'));
  const meta = [t('chat.chartAnnotationCard.metaCount', { count }), labels].filter(Boolean).join(' · ');
  const ariaArgs = { name, count };

  return (
    <div className="turn-file-card is-inline">
      <span className="turn-file-thumb" aria-hidden="true">
        <span className="turn-file-sheet" />
        <span className="turn-file-page">
          <CHART_GLYPH className="turn-file-glyph" />
          <span className="turn-file-ext">{interval}</span>
        </span>
      </span>
      <button
        type="button"
        className="turn-file-hit"
        aria-label={pricesOnly
          ? t('chat.chartAnnotationCard.cardAriaPricesOnly', ariaArgs)
          : t('chat.chartAnnotationCard.cardAria', ariaArgs)}
        // The meta line truncates once a chart has a few annotations.
        title={meta}
        onClick={handleOpen}
      >
        <span className="turn-file-name">{name}</span>
        <span className="turn-file-meta">{meta}</span>
      </button>
      <span className="turn-file-actions">
        <span className="turn-file-open is-solo" aria-hidden="true">
          <PanelRight className="h-3.5 w-3.5" />
          {t('chat.turnFiles.open')}
        </span>
      </span>
    </div>
  );
}

export default InlineChartAnnotationCard;
