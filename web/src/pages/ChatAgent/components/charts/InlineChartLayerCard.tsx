/**
 * The layers an agent published on a chart, as one card per chart instance in
 * the transcript. It is drawn from the tool result's `chart_layers`, or a
 * background command's report of them (grouped by `chartLayerGrouping`), so it
 * fetches nothing and reads the same on a history
 * replay as it did live; the overlay itself lives on the chart it opens.
 *
 * Inside the MarketView desktop panel the real chart already shows the layers,
 * so the card collapses to a one-line chip (see ChartSurfaceContext).
 */

import React, { useId, useLayoutEffect, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { ArrowRight, Check, ChartCandlestick, ChevronDown, Layers } from 'lucide-react';

import { buttonSlot } from '@/components/ui/button-class';
import { INTERVAL_LABEL, displaySpelling } from '@/lib/bars';
import { useLocale } from '@/hooks/useLocale';
import { cn } from '@/lib/utils';
import { requestShowLayers } from '@/pages/MarketView/stores/layerViewStore';
import { chartKeyFor } from '@/pages/MarketView/utils/chartInstanceKey';
import type { PublishedLayer } from '@/pages/MarketView/types/chartLayer';
import { formatLayerAsOf } from '@/pages/MarketView/utils/layerDisplay';

import { useChartSurface } from '../../contexts/ChartSurfaceContext';
import { useWorkspaceId } from '../../contexts/WorkspaceContext';
import { useMessageActions } from '../messageList/MessageActionsContext';
import type { ChartLayersCard } from '../chartLayerGrouping';

/** The summary shows this tall before "Show more": about three lines. */
const CLAMP_LINES = 3;

const CLAMPED: React.CSSProperties = {
  display: '-webkit-box',
  WebkitLineClamp: CLAMP_LINES,
  WebkitBoxOrient: 'vertical',
  overflow: 'hidden',
};

/**
 * Whether the clamped summary cuts text off, measured rather than guessed
 * from its length, since the card's width varies with the column. Read only
 * while clamped: open, nothing overflows, and the last reading keeps the
 * button that closes it.
 */
function useClampOverflow(ref: React.RefObject<HTMLElement | null>, clamped: boolean, text: string): boolean {
  const [overflows, setOverflows] = useState(false);
  useLayoutEffect(() => {
    const el = ref.current;
    if (!el || !clamped) return;
    const measure = () => setOverflows(el.scrollHeight > el.clientHeight + 1);
    measure();
    if (typeof ResizeObserver === 'undefined') return;
    // The column narrowing (a side panel opening) rewraps the text.
    const observer = new ResizeObserver(measure);
    observer.observe(el);
    return () => observer.disconnect();
  }, [ref, clamped, text]);
  return overflows;
}

function LayerRow({ layer }: { layer: PublishedLayer }): React.ReactElement {
  const { t } = useTranslation();
  const locale = useLocale();
  const [open, setOpen] = useState(false);
  const bodyId = useId();
  const bodyRef = useRef<HTMLParagraphElement>(null);
  const overflows = useClampOverflow(bodyRef, !open, layer.summary ?? '');
  const asOf = formatLayerAsOf(layer.as_of, locale);
  return (
    <li className="rule-b px-3.5 py-2.5 first:pt-3 last:border-b-0 last:pb-3">
      <div className="flex items-baseline justify-between gap-3">
        <span className="min-w-0 truncate text-[0.8125rem] font-medium text-[var(--color-text-primary)]">
          {layer.title}
        </span>
        {asOf && (
          <span
            className="shrink-0 text-[0.6875rem] text-[var(--color-text-tertiary)]"
            style={{ fontFamily: 'var(--font-mono)', fontVariantNumeric: 'tabular-nums' }}
          >
            {t('marketView.layers.asOf', { date: asOf })}
          </span>
        )}
      </div>
      {layer.summary && (
        <>
          <p
            ref={bodyRef}
            id={bodyId}
            className="mt-1 text-xs leading-[1.55] text-[var(--color-text-secondary)]"
            style={open ? undefined : CLAMPED}
          >
            {layer.summary}
          </p>
          {overflows && (
            <button
              type="button"
              aria-expanded={open}
              aria-controls={bodyId}
              onClick={() => setOpen((v) => !v)}
              className="tap-target mt-1 inline-flex items-center gap-1 text-[0.6875rem] text-[var(--color-text-tertiary)] transition-colors hover:text-[var(--color-text-primary)]"
            >
              {open ? t('chat.chartLayerCard.showLess') : t('chat.chartLayerCard.showMore')}
              <ChevronDown size={11} className={cn('transition-transform', open && 'rotate-180')} />
            </button>
          )}
        </>
      )}
    </li>
  );
}

export function InlineChartLayerCard({ card }: { card: ChartLayersCard }): React.ReactElement | null {
  const { t } = useTranslation();
  const { onOpenChart } = useMessageActions();
  const ctxWorkspaceId = useWorkspaceId();
  const { chartPresent, activeSymbol, activeTimeframe, onJumpToChart } = useChartSurface();

  const { symbol, timeframe, layers } = card;
  const workspaceId = card.workspaceId || ctxWorkspaceId || undefined;
  // A card from an entry that named no workspace shows its layers on the
  // chart of the workspace it is read in.
  const key = card.key ?? chartKeyFor(workspaceId, symbol, timeframe);
  const interval = INTERVAL_LABEL[timeframe] ?? timeframe;

  // Un-hides the layers and has whichever chart renders the instance frame
  // them once its bars are in.
  const showLayers = () => {
    if (key) requestShowLayers(key, layers.map((l) => l.layer_id));
  };

  if (!symbol || layers.length === 0) return null;

  // Inside MarketView the real chart shows the layers: a chip, which jumps the
  // chart to this instance when another is on screen.
  if (chartPresent) {
    const isActiveInstance =
      displaySpelling(activeSymbol ?? '') === symbol &&
      (!activeTimeframe || activeTimeframe === timeframe);
    const canJump = !!onJumpToChart && !isActiveInstance;
    const title = canJump
      ? t('chat.chartLayerCard.chipJumpTitle', { symbol, timeframe: interval })
      : t('chat.chartLayerCard.chipShownTitle');
    const count = layers.length;
    return (
      <button
        type="button"
        // On the instance already on screen the chip still un-hides and frames
        // the layers (a layer the user hid, a view panned away); it only skips
        // the switch.
        onClick={() => {
          showLayers();
          if (canJump) onJumpToChart?.(symbol, timeframe);
        }}
        title={title}
        className={cn(
          'tap-target inline-flex cursor-pointer items-center gap-2 rounded-full border bg-[var(--color-bg-tool-card)] px-3 py-1.5 text-xs text-[var(--color-text-tertiary)] transition-colors',
          canJump ? 'border-[var(--color-accent-primary)]' : 'border-[var(--color-border-muted)]',
        )}
      >
        {canJump
          ? <Layers size={14} className="shrink-0 text-[var(--color-accent-primary)]" />
          : <Check size={14} className="shrink-0 text-[var(--color-profit)]" />}
        <span>
          <span className="font-semibold text-[var(--color-text-primary)]">{symbol}</span>
          {` · ${interval} · `}
          {canJump
            ? t('chat.chartLayerCard.chipViewCount', { count })
            : t('chat.chartLayerCard.chipShownCount', { count })}
        </span>
        {canJump && <ArrowRight size={13} className="shrink-0 text-[var(--color-accent-primary)]" />}
      </button>
    );
  }

  // A transcript mounted with no workspace is a share: the chart it opens
  // carries none of these layers, so the card must not promise them.
  const pricesOnly = ctxWorkspaceId === null;
  const name = t('chat.chartLayerCard.name', { symbol, interval });
  return (
    <section
      aria-label={name}
      className="shell-box overflow-hidden text-[var(--color-text-primary)]"
    >
      <header className="rule-b flex items-center gap-2.5 px-3.5 py-2.5">
        <ChartCandlestick size={16} className="shrink-0 text-[var(--color-text-tertiary)]" aria-hidden="true" />
        <span className="min-w-0 flex-1 truncate text-[0.8125rem]">
          <span className="font-semibold">{symbol}</span>
          <span className="text-[var(--color-text-tertiary)]">{` · ${interval} · `}</span>
          <span className="text-[var(--color-text-secondary)]">
            {t('marketView.layers.count', { count: layers.length })}
          </span>
        </span>
        <button
          type="button"
          onClick={() => {
            // A share opens the plain prices: the layers are the owner's, and
            // nothing here can read them.
            if (pricesOnly) return void onOpenChart?.({ symbol, timeframe });
            showLayers();
            onOpenChart?.({ symbol, timeframe, workspaceId });
          }}
          {...buttonSlot('outline')}
          aria-label={pricesOnly
            ? t('chat.chartLayerCard.openAriaPricesOnly', { name })
            : t('chat.chartLayerCard.openAria', { name })}
          className="press tap-target inline-flex shrink-0 items-center gap-1 rounded-md border border-[var(--color-border-muted)] px-2.5 py-1 text-xs text-[var(--color-text-primary)] hover:bg-[var(--color-bg-surface)]"
        >
          {t('chat.chartLayerCard.open')}
          <ArrowRight size={12} aria-hidden="true" />
        </button>
      </header>
      <ul>
        {layers.map((layer) => <LayerRow key={layer.layer_id} layer={layer} />)}
      </ul>
    </section>
  );
}

export default InlineChartLayerCard;
