import React from 'react';
import { useTranslation } from 'react-i18next';
import { useLocale } from '@/hooks/useLocale';
import { formatCalendarDay, formatCalendarMonthDay, grouped2 } from '@/lib/format';
import { indexRows, sectorRows, text, unwrapMarketOverview, type IndexRow, type InlineCardProps } from './cardReaders';
import { FreshnessBadge } from './FreshnessBadge';
import { TermBars, TermCard, TermChange, TermDivider } from './TermCard';
import { commonFreshness } from './termFreshness';
import { TermTable } from './TermTable';

/** The header's freshness: what the rows share, said once. */
function indexFreshness(rows: IndexRow[]): React.ReactElement | undefined {
  const { common } = commonFreshness(rows);
  if (!common) return undefined;
  return <FreshnessBadge tier={common.tier} freshness={common.freshness} source={common.source} />;
}

export function IndexTable({ rows }: { rows: IndexRow[] }): React.ReactElement {
  const { t } = useTranslation();
  const locale = useLocale();
  const { differs } = commonFreshness(rows);
  // A basket across time zones can hold sessions of different days (Monday in
  // Asia beside Friday's US close): a row behind the newest names its own day.
  const newest = rows.reduce((max, row) => (row.day && row.day > max ? row.day : max), '');
  return (
    <TermTable
      rows={rows}
      rowKey={(row) => row.sym}
      columns={[
        {
          key: 'name',
          label: t('toolArtifact.termHead.name'),
          fill: 'clip',
          cell: (row) => (
            <>
              {/* Only a row whose freshness differs from the edge's keeps a mark. */}
              {differs(row) && (
                <FreshnessBadge tier={row.tier} freshness={row.freshness} source={row.source} compact className="mr-[4px]" />
              )}
              {row.day && row.day !== newest && <span className="term-t3 mr-[4px]">{formatCalendarMonthDay(row.day, locale)}</span>}
              {row.name}
            </>
          ),
        },
        { key: 'last', label: t('toolArtifact.termHead.last'), num: true, cell: (row) => (row.last != null ? grouped2(row.last, locale) : '') },
        { key: 'change', label: t('toolArtifact.termHead.change'), num: true, cell: (row) => <TermChange value={row.changePct} /> },
      ]}
    />
  );
}

/** `meta` leads the card's own freshness, for a caller that dates the rows. */
type DatedCardProps = InlineCardProps & { meta?: React.ReactNode };

export function InlineMarketIndicesCard({ artifact, onClick, meta }: DatedCardProps): React.ReactElement | null {
  const { t } = useTranslation();
  const rows = indexRows(artifact);
  if (!rows.length) return null;
  return (
    <TermCard title={t('toolArtifact.marketIndices')} meta={[meta, indexFreshness(rows)]} onClick={onClick}>
      <IndexTable rows={rows} />
    </TermCard>
  );
}

export function InlineSectorPerformanceCard({ artifact, onClick, meta }: DatedCardProps): React.ReactElement | null {
  const { t } = useTranslation();
  const rows = sectorRows(artifact);
  if (!rows.length) return null;
  return (
    <TermCard title={t('toolArtifact.sectorPerformance')} meta={meta} onClick={onClick}>
      <TermBars rows={rows} />
    </TermCard>
  );
}

/**
 * Composite card for the consolidated `get_market_overview` tool: the indices
 * and the sectors it carries (verbatim under `indices` / `sectors`) share one
 * box, the sectors under a divider, dated by the session they show. A request
 * for a day with no session names that day beside the one shown. It must never
 * return null, since a completed get_market_overview call routed here would
 * otherwise render nothing, so when neither part has data it falls back to the
 * region alone.
 */
export function InlineMarketOverviewCard({ artifact, onClick }: InlineCardProps): React.ReactElement {
  const { t } = useTranslation();
  const locale = useLocale();
  const { indicesArtifact, sectorsArtifact } = unwrapMarketOverview(artifact);
  const indices = indexRows(indicesArtifact);
  const sectors = sectorRows(sectorsArtifact);
  const day = text(artifact.date);
  const requested = text(artifact.requested_date);
  // The sectors name their own session, which can differ from the indices'
  // and is still forming until the regular close.
  const sectorDay = text(sectorsArtifact?.date);
  const sectorsForming = sectorsArtifact?.in_progress === true;
  const sectorDayLabel =
    sectorDay &&
    (sectorsForming
      ? t('toolArtifact.sessionInProgress', { day: formatCalendarDay(sectorDay, locale) })
      : formatCalendarDay(sectorDay, locale));
  const dayMeta =
    day &&
    (requested
      ? t('toolArtifact.overviewFallbackDay', { day: formatCalendarDay(day, locale), requested: formatCalendarDay(requested, locale) })
      : formatCalendarDay(day, locale));

  if (indices.length && sectors.length) {
    return (
      <TermCard title={t('toolArtifact.marketIndices')} meta={[dayMeta, indexFreshness(indices)]} onClick={onClick}>
        <IndexTable rows={indices} />
        <TermDivider>
          {t('toolArtifact.sectorPerformance')}
          {sectorDayLabel && (sectorDay !== day || sectorsForming) && ` · ${sectorDayLabel}`}
        </TermDivider>
        <TermBars rows={sectors} />
      </TermCard>
    );
  }
  if (indicesArtifact && indices.length) {
    return <InlineMarketIndicesCard artifact={indicesArtifact} meta={dayMeta} onClick={onClick} />;
  }
  if (sectorsArtifact && sectors.length) {
    // A fallback card keeps the day that was asked for beside the one shown.
    const sectorMeta = requested || !sectorDayLabel ? dayMeta : sectorDayLabel;
    return <InlineSectorPerformanceCard artifact={sectorsArtifact} meta={sectorMeta} onClick={onClick} />;
  }

  // Error / degenerate path (e.g. unknown region): keep the call visible.
  return <TermCard title={t('toolArtifact.marketOverview')} sub={text(artifact.region)?.toUpperCase()} meta={dayMeta} onClick={onClick} />;
}
