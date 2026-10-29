import React from 'react';
import { useTranslation } from 'react-i18next';
import { formatMoney } from '@/lib/bars';
import { compactNumberFixed2 } from '@/lib/format';
import { useLocale } from '@/hooks/useLocale';
import { readScreener, type PanelProps, type ScreenerRow } from './cardReaders';
import { FilterChip } from './InlineScreenerCard';
import { MarketLink } from './marketLinks';
import { TermCard, TermChange } from './TermCard';
import { SortTable, type SortColumn } from './TermTable';

/** The screener card a size up: every filter, every row, and every column a row fills. */
export function StockScreenerTable({ data }: PanelProps): React.ReactElement {
  const locale = useLocale();
  const { t } = useTranslation();
  const { rows, filters, count } = readScreener(data);

  if (!rows.length) {
    return <div className="term-t3" style={{ padding: 16 }}>{t('toolArtifact.noScreenerResults')}</div>;
  }

  const money = (n: number, row: ScreenerRow) => formatMoney(n, row.currency, locale);

  const columns: Array<SortColumn<ScreenerRow>> = [
    { key: 'symbol', label: t('toolArtifact.symbol'), className: 'term-strong', value: (r) => r.symbol, cell: (r) => <MarketLink symbol={r.symbol} inline /> },
    { key: 'companyName', label: t('toolArtifact.company'), className: 'term-t3 term-clip term-word', value: (r) => r.name },
    { key: 'price', label: t('toolArtifact.price'), num: true, value: (r) => r.price, cell: (r) => money(r.price!, r) },
    { key: 'change', label: t('toolArtifact.changePct'), num: true, value: (r) => r.changePct, cell: (r) => <TermChange value={r.changePct} /> },
    {
      key: 'marketCap', label: t('toolArtifact.mktCap'), num: true, className: 'term-t2', value: (r) => r.marketCap,
      cell: (r) => formatMoney(r.marketCap!, r.currency, locale, { compact: true }),
    },
    { key: 'volume', label: t('toolArtifact.volume'), num: true, className: 'term-t2', value: (r) => r.volume, cell: (r) => compactNumberFixed2(r.volume!, locale) },
    { key: 'beta', label: t('toolArtifact.beta'), num: true, className: 'term-t2', value: (r) => r.beta, cell: (r) => r.beta!.toFixed(2) },
    { key: 'lastAnnualDividend', label: t('toolArtifact.dividend'), num: true, className: 'term-t2', value: (r) => r.dividend, cell: (r) => money(r.dividend!, r) },
    { key: 'sector', label: t('toolArtifact.sector'), className: 'term-t2 term-word', value: (r) => r.sector },
    { key: 'industry', label: t('toolArtifact.industry'), className: 'term-t2 term-clip term-word', value: (r) => r.industry },
    { key: 'exchangeShortName', label: t('toolArtifact.exchange'), className: 'term-t2', value: (r) => r.exchange },
    { key: 'country', label: t('toolArtifact.country'), className: 'term-t2 term-word', value: (r) => r.country },
  ];

  return (
    <TermCard title={t('toolArtifact.stockScreener')} meta={t('toolArtifact.nResults', { count })}>
      {filters.length > 0 && (
        <div className="term-chips" style={{ marginBottom: 10 }}>
          {filters.map(([name, value]) => (
            <FilterChip key={name} name={name} value={value} t={t} />
          ))}
        </div>
      )}
      <SortTable rows={rows} columns={columns} rowKey={(r, i) => r.symbol || i} />
    </TermCard>
  );
}
