import React from 'react';
import { useTranslation } from 'react-i18next';
import { formatMoney } from '@/lib/bars';
import { useLocale } from '@/hooks/useLocale';
import { readMarketMovers, type InlineCardProps } from './cardReaders';
import { formatVolume, moneyPlaces, moversTitle, retrievedTitle } from './cardWords';
import { TermCard, TermChange, TermStatus } from './TermCard';
import { useCardNarrow } from './termContext';
import { TermTable } from './TermTable';

const MAX_INLINE_ROWS = 5;

/** The day's biggest movers one way, in the order the tool ranked them. */
export function InlineMarketMoversCard({ artifact, onClick, toolArgs }: InlineCardProps): React.ReactElement {
  const { t } = useTranslation();
  const locale = useLocale();
  const narrow = useCardNarrow();
  const { direction, rows, asOfTs, failed } = readMarketMovers(artifact, toolArgs);
  const listed = rows.slice(0, MAX_INLINE_ROWS);

  return (
    <TermCard
      title={moversTitle(t, direction)}
      meta={[
        failed && <TermStatus key="failed" tone="loss">{t('common.failed')}</TermStatus>,
        rows.length > 0 && t('toolArtifact.nResults', { count: rows.length }),
      ]}
      metaTitle={retrievedTitle(t, asOfTs, locale)}
      onClick={onClick}
    >
      {/* An error or an empty list keeps its call in the transcript; the
          panel behind it holds the tool's own words for why. */}
      {!rows.length && <div className="term-t3">{t('toolArtifact.noMoversData')}</div>}
      <TermTable
        rows={rows}
        limit={MAX_INLINE_ROWS}
        more={(n) => t('toolArtifact.nMoreStocks', { count: n })}
        rowKey={(row, i) => row.symbol || i}
        columns={[
          // Without the name column, the symbol takes the spare width.
          { key: 'sym', label: t('toolArtifact.termHead.sym'), fill: 'grow', className: 'term-strong', cell: (r) => r.symbol },
          // The vendor names few US movers; a column blank on every row says nothing.
          { key: 'name', label: t('toolArtifact.termHead.name'), fill: 'clip', className: 'term-t3', show: !narrow && listed.some((r) => r.name), cell: (r) => r.name },
          {
            key: 'last',
            label: t('toolArtifact.termHead.last'),
            num: true,
            cell: (r) => (r.price != null ? formatMoney(r.price, r.currency, locale, { decimals: moneyPlaces(r.price) }) : ''),
          },
          { key: 'change', label: t('toolArtifact.termHead.change'), num: true, cell: (r) => <TermChange value={r.changePct} /> },
          {
            key: 'vol',
            label: t('toolArtifact.termHead.vol'),
            num: true,
            className: 'term-t2',
            show: listed.some((r) => r.volume != null),
            cell: (r) => (r.volume != null ? formatVolume(r.volume, locale) : ''),
          },
        ]}
      />
    </TermCard>
  );
}
