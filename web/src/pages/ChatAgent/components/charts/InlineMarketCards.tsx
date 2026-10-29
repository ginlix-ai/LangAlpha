import React from 'react';
import { useTranslation } from 'react-i18next';
import { useLocale } from '@/hooks/useLocale';
import { grouped2 } from '@/lib/format';
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

export function InlineMarketIndicesCard({ artifact, onClick }: InlineCardProps): React.ReactElement | null {
  const { t } = useTranslation();
  const rows = indexRows(artifact);
  if (!rows.length) return null;
  return (
    <TermCard title={t('toolArtifact.marketIndices')} meta={indexFreshness(rows)} onClick={onClick}>
      <IndexTable rows={rows} />
    </TermCard>
  );
}

export function InlineSectorPerformanceCard({ artifact, onClick }: InlineCardProps): React.ReactElement | null {
  const { t } = useTranslation();
  const rows = sectorRows(artifact);
  if (!rows.length) return null;
  return (
    <TermCard title={t('toolArtifact.sectorPerformance')} onClick={onClick}>
      <TermBars rows={rows} />
    </TermCard>
  );
}

/**
 * Composite card for the consolidated `get_market_overview` tool: the indices
 * and the sectors it carries (verbatim under `indices` / `sectors`) share one
 * box, the sectors under a divider. It must never return null, since a
 * completed get_market_overview call routed here would otherwise render
 * nothing, so when neither part has data it falls back to the region alone.
 */
export function InlineMarketOverviewCard({ artifact, onClick }: InlineCardProps): React.ReactElement {
  const { t } = useTranslation();
  const { indicesArtifact, sectorsArtifact } = unwrapMarketOverview(artifact);
  const indices = indexRows(indicesArtifact);
  const sectors = sectorRows(sectorsArtifact);

  if (indices.length && sectors.length) {
    return (
      <TermCard title={t('toolArtifact.marketIndices')} meta={indexFreshness(indices)} onClick={onClick}>
        <IndexTable rows={indices} />
        <TermDivider>{t('toolArtifact.sectorPerformance')}</TermDivider>
        <TermBars rows={sectors} />
      </TermCard>
    );
  }
  if (indicesArtifact && indices.length) return <InlineMarketIndicesCard artifact={indicesArtifact} onClick={onClick} />;
  if (sectorsArtifact && sectors.length) return <InlineSectorPerformanceCard artifact={sectorsArtifact} onClick={onClick} />;

  // Error / degenerate path (e.g. unknown region): keep the call visible.
  return <TermCard title={t('toolArtifact.marketOverview')} sub={text(artifact.region)?.toUpperCase()} onClick={onClick} />;
}
