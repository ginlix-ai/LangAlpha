import React from 'react';
import { useTranslation } from 'react-i18next';
import { formatMoney } from '@/lib/bars';
import { useLocale } from '@/hooks/useLocale';
import { compactNumberFixed2, integer } from '@/lib/format';
import { readMarketMovers, readOptionsChain, type MoverRow, type OptionRow, type PanelProps } from './cardReaders';
import { expiryLabeler, moneyPlaces, moversTitle, optionsEdge, retrievedTitle } from './cardWords';
import { TermCard, TermChange, TermRange, TermTag } from './TermCard';
import { MarketLink } from './marketLinks';
import { SortTable, type SortColumn } from './TermTable';
import { useCardNarrow } from './termContext';

// ─── OptionsChainPanel ────────────────────────────────────────────

/** The options card a size up: every contract, its OCC ticker, and where it sits in its day's range. */
export function OptionsChainPanel({ data, toolArgs }: PanelProps): React.ReactElement {
  const { t } = useTranslation();
  const locale = useLocale();
  const narrow = useCardNarrow();
  const chain = readOptionsChain(data, toolArgs);
  const edge = optionsEdge(chain, t, locale, narrow);
  const expiryOf = expiryLabeler(chain.expiries, locale);
  const money = (n: number): string => formatMoney(n, chain.currency, locale, { decimals: moneyPlaces(n) });

  // A phone keeps the contract's terms, its price and its move; the money
  // move, the range and the OCC ticker need the width.
  const columns: Array<SortColumn<OptionRow> & { wide?: boolean }> = [
    // The row's name: kept when every contract shares it, unlike the type
    // and the expiry, which the edge says once.
    { key: 'strike', label: t('toolArtifact.termHead.strike'), value: (r) => r.strike, cell: (r) => money(r.strike!), className: 'term-strong', fill: 'grow', keep: true },
    { key: 'expiry', label: t('toolArtifact.expiration'), value: (r) => r.expiry, cell: (r) => expiryOf(r.expiry), className: 'term-t2', fill: 'grow' },
    {
      key: 'type', label: t('toolArtifact.termHead.type'), value: (r) => r.type, className: 'term-t2', fill: 'grow',
      cell: (r) => <span className="term-label">{t(`toolArtifact.optionType.${r.type}`)}</span>,
    },
    // The machine's name for the contract, which the terms before it spell
    // out: it sits where a screen puts a company's name, quiet, and the first
    // to give way.
    { key: 'ticker', label: t('toolArtifact.contract'), value: (r) => r.ticker, className: 'term-t3 term-code', fill: 'clip', wide: true },
    { key: 'last', label: t('toolArtifact.price'), num: true, value: (r) => r.last, cell: (r) => money(r.last!) },
    {
      key: 'change', label: t('toolArtifact.change'), num: true, className: 'term-t2', value: (r) => r.change, wide: true,
      cell: (r) => formatMoney(r.change!, chain.currency, locale, { signed: true, decimals: moneyPlaces(r.change) }),
    },
    { key: 'changePct', label: t('toolArtifact.changePct'), num: true, value: (r) => r.changePct, cell: (r) => <TermChange value={r.changePct} /> },
    { key: 'volume', label: t('toolArtifact.volume'), num: true, className: 'term-t2', value: (r) => r.volume, cell: (r) => integer(r.volume!, locale) },
    // Where the price sits in the day's range, when the day has one.
    {
      key: 'range', label: t('toolArtifact.termHead.dayRange'), num: true, wide: true,
      value: (r) => (r.low != null && r.high != null && r.high > r.low ? r.high - r.low : undefined),
      cell: (r) => <TermRange low={r.low} high={r.high} value={r.last} />,
    },
  ];

  return (
    <TermCard
      title={edge.title}
      sub={edge.sub}
      meta={[edge.status && <TermTag key="status">{edge.status}</TermTag>, edge.expiry]}
      metaTitle={retrievedTitle(t, chain.asOfTs, locale)}
    >
      <SortTable rows={chain.rows} columns={narrow ? columns.filter((col) => !col.wide) : columns} rowKey={(r, i) => r.ticker || i} />
    </TermCard>
  );
}

// ─── MarketMoversPanel ────────────────────────────────────────────

/** The movers card a size up: every row, each symbol a way into Market View. */
export function MarketMoversPanel({ data, toolArgs }: PanelProps): React.ReactElement {
  const { t } = useTranslation();
  const locale = useLocale();
  const { direction, rows, asOfTs } = readMarketMovers(data, toolArgs);
  const money = (n: number, row: MoverRow, signed = false): string =>
    formatMoney(n, row.currency, locale, { signed, decimals: moneyPlaces(n) });

  const columns: Array<SortColumn<MoverRow>> = [
    { key: 'symbol', label: t('toolArtifact.symbol'), value: (r) => r.symbol, cell: (r) => <MarketLink symbol={r.symbol} inline />, className: 'term-strong', fill: 'grow' },
    { key: 'name', label: t('toolArtifact.company'), value: (r) => r.name, className: 'term-t3', fill: 'clip' },
    { key: 'price', label: t('toolArtifact.price'), num: true, value: (r) => r.price, cell: (r) => money(r.price!, r) },
    { key: 'change', label: t('toolArtifact.change'), num: true, className: 'term-t2', value: (r) => r.change, cell: (r) => money(r.change!, r, true) },
    { key: 'changePct', label: t('toolArtifact.changePct'), num: true, value: (r) => r.changePct, cell: (r) => <TermChange value={r.changePct} /> },
    { key: 'volume', label: t('toolArtifact.volume'), num: true, className: 'term-t2', value: (r) => r.volume, cell: (r) => compactNumberFixed2(r.volume!, locale) },
  ];

  return (
    <TermCard
      title={moversTitle(t, direction)}
      meta={t('toolArtifact.nResults', { count: rows.length })}
      metaTitle={retrievedTitle(t, asOfTs, locale)}
    >
      <SortTable rows={rows} columns={columns} rowKey={(r, i) => r.symbol || i} />
    </TermCard>
  );
}

