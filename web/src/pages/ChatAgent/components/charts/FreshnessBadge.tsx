import React from 'react';
import { useTranslation } from 'react-i18next';
import type { TFunction } from 'i18next';
import { useLocale } from '@/hooks/useLocale';
import { useNow } from '@/hooks/useNow';
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip';
import { providerLabel } from '@/lib/providerLabels';
import { cn } from '@/lib/utils';
import {
  FRESHNESS_TONE,
  chartFreshnessParts,
  freshnessBadgeText,
  resolveFreshnessBadge,
  type FreshnessFields,
} from '@/lib/freshness';

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
  /** Where the dot sits against the row's words. */
  className?: string;
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

/**
 * How current a row is, as a data card's bracket tag, `[● LAST CLOSE]`, with
 * the source and the exact time behind a tooltip. `compact` draws the dot
 * alone. Renders nothing for a row with no freshness fields, so pre-contract
 * artifacts keep their old look.
 */
export function FreshnessBadge(props: FreshnessBadgeProps): React.ReactElement | null {
  const { t } = useTranslation();
  const locale = useLocale();
  // Only a chart's last bar is dated against today; a quote row's stamp is fixed.
  const now = useNow(60_000, props.chartSymbol != null);
  const spec = resolveFreshnessBadge(props);
  if (!spec) return null;
  const tone = FRESHNESS_TONE[spec.state];
  const lines = freshnessTooltipLines(t, props, locale, now);
  // The tag's own words name its state; the bare dot is a graphic, named by
  // the tooltip's lines. Only a mark with a tooltip to open takes focus.
  const hasTooltip = props.compact || lines.length > 1;
  const mark = props.compact ? (
    <span tabIndex={0} role="img" aria-label={lines.join('. ')} className={cn('term-fresh-dot', props.className)}>
      <span aria-hidden data-tone={tone} />
    </span>
  ) : (
    <span className="term-tag" tabIndex={hasTooltip ? 0 : undefined} style={{ cursor: 'default' }}>
      <span className="term-dot" data-tone={tone} aria-hidden="true" />
      <span>{freshnessBadgeText(t, props)}</span>
    </span>
  );
  if (!hasTooltip) return mark;
  return (
    <TooltipProvider delayDuration={200}>
      <Tooltip>
        <TooltipTrigger asChild>{mark}</TooltipTrigger>
        <TooltipContent side="top" style={{ fontSize: '0.6875rem', lineHeight: 1.5, fontVariantNumeric: 'tabular-nums' }}>
          {lines.map((l) => (
            <div key={l}>{l}</div>
          ))}
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  );
}
