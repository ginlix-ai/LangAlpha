import React from 'react';
import type { TFunction } from 'i18next';
import { useTranslation } from 'react-i18next';
import { formatMoney } from '@/lib/bars';
import { useLocale } from '@/hooks/useLocale';
import { readScreener, type InlineCardProps } from './cardReaders';
import { TermCard, TermChange } from './TermCard';
import { useCardNarrow } from './termContext';
import { TermTable } from './TermTable';

const MAX_INLINE_ROWS = 5;
const MAX_FILTER_CHIPS = 4;

// The producer's filter names (screener.py) that the catalog already words.
const FILTER_LABEL_KEYS: Record<string, string> = {
  Sector: 'toolArtifact.sector',
  Industry: 'toolArtifact.industry',
  Exchange: 'toolArtifact.exchange',
  Country: 'toolArtifact.country',
  'Mkt Cap': 'toolArtifact.mktCap',
  Price: 'toolArtifact.price',
  Vol: 'toolArtifact.vol',
  Beta: 'toolArtifact.beta',
};

/**
 * The producer names a threshold with its operator (`Mkt Cap >`) and a match
 * without one (`Sector`), so a chip reads `MKT CAP > $10.00B` or
 * `SECTOR = Technology`.
 */
export function FilterChip({ name, value, t }: { name: string; value: unknown; t: TFunction }): React.ReactElement {
  const m = /^(.*?)\s*([<>]=?)$/.exec(name);
  const field = m ? m[1] : name;
  const op = m ? m[2] : '=';
  const key = FILTER_LABEL_KEYS[field];
  return (
    <span className="term-chip">
      <span className="term-caps">{key ? t(key) : field}</span> {op} {String(value)}
    </span>
  );
}

export function InlineStockScreenerCard({ artifact, onClick }: InlineCardProps): React.ReactElement | null {
  const { t } = useTranslation();
  const locale = useLocale();
  const narrow = useCardNarrow();
  const { rows, filters, count } = readScreener(artifact);
  if (!rows.length) return null;

  return (
    <TermCard title={t('toolArtifact.stockScreener')} meta={t('toolArtifact.nResults', { count })} onClick={onClick}>
      {filters.length > 0 && (
        <div className="term-chips term-filters">
          {filters.slice(0, MAX_FILTER_CHIPS).map(([name, value]) => (
            <FilterChip key={name} name={name} value={value} t={t} />
          ))}
          {filters.length > MAX_FILTER_CHIPS && (
            <span className="term-t3 term-filters-more">+{filters.length - MAX_FILTER_CHIPS}</span>
          )}
        </div>
      )}
      <TermTable
        rows={rows}
        limit={MAX_INLINE_ROWS}
        total={count}
        more={(n) => t('toolArtifact.nMoreStocks', { count: n })}
        rowKey={(r, i) => r.symbol ?? i}
        columns={[
          // Without the name column, the symbol takes the spare width.
          { key: 'sym', label: t('toolArtifact.termHead.sym'), fill: 'clip', className: 'term-code term-strong', cell: (r) => r.symbol },
          { key: 'name', label: t('toolArtifact.termHead.name'), fill: 'clip', className: 'term-t3', show: !narrow, cell: (r) => r.name },
          { key: 'last', label: t('toolArtifact.termHead.last'), num: true, cell: (r) => (r.price != null ? formatMoney(r.price, r.currency, locale) : '') },
          {
            key: 'mktCap',
            label: t('toolArtifact.termHead.mktCap'),
            num: true,
            className: 'term-t2',
            // As the panel prints it: two decimals keep 4.53T and 4.48T apart.
            cell: (r) => (r.marketCap != null ? formatMoney(r.marketCap, r.currency, locale, { compact: true }) : ''),
          },
          {
            key: 'change',
            label: t('toolArtifact.termHead.change'),
            num: true,
            // FMP's screen carries no change at all; a column blank on every row says nothing.
            show: rows.slice(0, MAX_INLINE_ROWS).some((r) => r.changePct != null),
            cell: (r) => <TermChange value={r.changePct} />,
          },
        ]}
      />
    </TermCard>
  );
}
