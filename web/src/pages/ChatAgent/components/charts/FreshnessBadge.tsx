import React from 'react';
import { useTranslation } from 'react-i18next';
import type { TFunction } from 'i18next';
import { useIsMobile } from '@/hooks/useIsMobile';
import { useLocale } from '@/hooks/useLocale';
import { useNow } from '@/hooks/useNow';
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip';
import { providerLabel } from '@/lib/providerLabels';
import {
  FRESHNESS_TONE,
  chartFreshnessParts,
  freshnessBadgeText,
  resolveFreshnessBadge,
  type FreshnessFields,
  type FreshnessTone,
} from '@/lib/freshness';
import { SIZES_MOBILE, SIZES_DESKTOP, TEXT_COLOR, GREEN, CARD_BORDER } from './inlineCardsShared';

export interface FreshnessBadgeProps extends FreshnessFields {
  /** Publisher of the row; falls back to `freshness.source`. */
  source?: string | null;
  /** Venue-local stamp for a quote row: the print time, or the retrieval
   *  clock when the provider gave none (`printed` says which). */
  asOfLocal?: string | null;
  printed?: boolean;
  /** Chart surface: names the symbol so the last bar prints in venue time. */
  chartSymbol?: string | null;
  /** Dot only, fixed width, details on hover: for list rows where a label of
   *  varying length would push the price column around. */
  compact?: boolean;
}

/** The tooltip body: source, state, and the one time that locates the data. */
export function freshnessTooltipLines(
  t: TFunction,
  p: FreshnessBadgeProps,
  locale: string,
  now: number,
): string[] {
  const sourceId = p.source ?? p.freshness?.source;
  if (p.chartSymbol != null) {
    const parts = chartFreshnessParts(t, p.freshness && { ...p.freshness, source: sourceId }, p.chartSymbol, locale, now);
    // The state line repeats the pill's own words; the parts' state is the
    // header's mid-sentence phrasing.
    return [
      parts.source && `${t('marketView.header.source')}: ${parts.source}`,
      freshnessBadgeText(t, p),
      parts.lastBar,
    ].filter((l): l is string => !!l);
  }
  const lines: string[] = [];
  const src = providerLabel(sourceId, t);
  if (src) lines.push(`${t('marketView.header.source')}: ${src}`);
  const state = freshnessBadgeText(t, p);
  if (state) lines.push(state);
  if (p.asOfLocal) {
    lines.push(p.printed === false ? t('toolArtifact.retrievedAt', { time: p.asOfLocal }) : p.asOfLocal);
  }
  return lines;
}

const DOT_COLOR: Record<FreshnessTone, string> = {
  live: GREEN,
  neutral: TEXT_COLOR,
  warning: 'var(--color-warning)',
};

/**
 * Compact pill naming how current a row is, with the source and the exact
 * time behind a tooltip. Renders nothing for a row with no freshness fields
 * so pre-contract artifacts keep their old look.
 */
export function FreshnessBadge(props: FreshnessBadgeProps): React.ReactElement | null {
  const { t } = useTranslation();
  const locale = useLocale();
  // Only a chart's last bar is dated against today; a quote row's stamp is fixed.
  const now = useNow(60_000, props.chartSymbol != null);
  const isMobile = useIsMobile();
  const sz = isMobile ? SIZES_MOBILE : SIZES_DESKTOP;
  const spec = resolveFreshnessBadge(props);
  if (!spec) return null;
  const text = freshnessBadgeText(t, props);
  const lines = freshnessTooltipLines(t, props, locale, now);
  const dot = (
    <span
      aria-hidden
      style={{
        width: props.compact ? 7 : 5,
        height: props.compact ? 7 : 5,
        borderRadius: '50%',
        background: DOT_COLOR[FRESHNESS_TONE[spec.state]],
        flexShrink: 0,
        // Centered, and kept out of the baseline computation: a flex
        // container takes its baseline from the first item that has one, and
        // an empty box would hand the pill the dot's bottom edge as baseline.
        alignSelf: 'center',
      }}
    />
  );
  // The pill's own words name its state; the bare dot is a graphic, named by
  // the tooltip's lines. Only a pill with a tooltip to open takes focus.
  const hasTooltip = props.compact || lines.length > 1;
  const pill = props.compact ? (
    <span
      tabIndex={0}
      role="img"
      aria-label={lines.join('. ')}
      style={{
        display: 'inline-flex',
        alignItems: 'center',
        justifyContent: 'center',
        width: 16,
        height: 16,
        flexShrink: 0,
        cursor: 'default',
        alignSelf: 'center',
      }}
    >
      {dot}
    </span>
  ) : (
    <span
      tabIndex={hasTooltip ? 0 : undefined}
      style={{
        display: 'inline-flex',
        alignItems: 'baseline',
        gap: 4,
        padding: '1px 6px',
        borderRadius: 999,
        border: `1px solid ${CARD_BORDER}`,
        fontSize: sz.badgeFs,
        lineHeight: 1.4,
        // Secondary, not tertiary: at 9-10px only it clears 4.5:1 on a light card.
        color: 'var(--color-text-secondary)',
        whiteSpace: 'nowrap',
        flexShrink: 0,
        fontVariantNumeric: 'tabular-nums',
        cursor: 'default',
        // Header rows baseline-align text of three sizes; a bordered pill
        // reads as a box, and a box sits right when centered on the line.
        alignSelf: 'center',
      }}
    >
      {dot}
      <span>{text}</span>
    </span>
  );
  if (!hasTooltip) return pill;
  return (
    <TooltipProvider delayDuration={200}>
      <Tooltip>
        <TooltipTrigger asChild>{pill}</TooltipTrigger>
        <TooltipContent side="top" style={{ fontSize: '0.6875rem', lineHeight: 1.5, fontVariantNumeric: 'tabular-nums' }}>
          {lines.map((l) => (
            <div key={l}>{l}</div>
          ))}
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  );
}
