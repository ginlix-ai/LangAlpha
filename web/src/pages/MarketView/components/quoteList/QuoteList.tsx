import React, { useRef, useState } from 'react';
import { ArrowDown, ArrowUp, Plus, Sunrise, Sunset, X } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { TickerLogo } from '@/components/TickerLogo';
import ConfirmDialog from '@/pages/Dashboard/components/ConfirmDialog';
import type { DeleteConfirmConfig } from '@/pages/Dashboard/hooks/usePortfolioData';
import { formatMoney } from '@/lib/bars';
import { DASH, createFormatter, signedFixed2 } from '@/lib/format';
import { useLocale } from '@/hooks/useLocale';
import { cn } from '@/lib/utils';
import { useQuoteList } from './useQuoteList';
import { QuoteListAdd } from './QuoteListAdd';
import { sameListing, type Column, type QuoteListRow, type QuoteListTab } from './quoteRows';
import './QuoteList.css';

interface QuoteListProps {
  /** The symbol on the chart, whose row is marked. */
  activeSymbol: string | null;
  onPick: (symbol: string) => void;
}

/** What the Value column's 3.75rem holds in the table's mono figures: `$184,320`. */
const MONEY_CHARS = 8;

// A move keeps the width of `-10.25%` however large it grows, the most a
// figure column holds: `+345.1%`, `+1234%`.
const signedFixed1 = createFormatter({ minimumFractionDigits: 1, maximumFractionDigits: 1, useGrouping: false, signDisplay: 'exceptZero' });
const signedWhole = createFormatter({ maximumFractionDigits: 0, useGrouping: false, signDisplay: 'exceptZero' });
const percentFigure = (v: number, locale: string) =>
  `${(Math.abs(v) >= 1000 ? signedWhole : Math.abs(v) >= 100 ? signedFixed1 : signedFixed2)(v, locale)}%`;

function tone(v: number | null): string {
  if (v == null || v === 0) return 'quote-list-flat';
  return v > 0 ? 'quote-list-up' : 'quote-list-down';
}

/**
 * The user's watchlist and holdings as a dense, sortable table: one line a
 * symbol, its figures in columns. It opens over a chart, on the market page
 * and in a thread's chart tab, and as the phone's side sheet; a pick moves
 * that chart. The session is said once, above the columns, rather than on
 * every row.
 */
export function QuoteList({ activeSymbol, onPick }: QuoteListProps): React.ReactElement {
  const { t } = useTranslation();
  const locale = useLocale();
  const q = useQuoteList();
  const { watchlist, portfolio } = q;
  const isWatchlist = q.tab === 'watchlist';
  const [confirm, setConfirm] = useState<DeleteConfirmConfig | null>(null);
  const [adding, setAdding] = useState(false);
  // The symbol just added, marked once as its row arrives.
  const [fresh, setFresh] = useState<string | null>(null);
  const addButton = useRef<HTMLButtonElement>(null);
  const stopAdding = () => {
    setAdding(false);
    addButton.current?.focus();
  };

  const price = (v: number | null, places: number) => (v == null ? DASH : formatMoney(v, null, locale, { upTo: places }));
  const pct = (v: number | null) => (v == null ? DASH : percentFigure(v, locale));
  // Whole units, shortened (`HK$1.2M`) where the full figure would outrun its column.
  const money = (v: number | null, currency: string | null) => {
    if (v == null) return DASH;
    const full = formatMoney(v, currency, locale, { decimals: 0 });
    return full.length > MONEY_CHARS ? formatMoney(v, currency, locale, { decimals: 0, upTo: 1, compact: true }) : full;
  };

  // Watchlist items go at once, as everywhere else; a holding asks first.
  const remove = (row: QuoteListRow) => {
    if (isWatchlist) void watchlist.handleDelete(row.id);
    else setConfirm(portfolio.handleDelete(row.id));
  };

  const figure = (column: Exclude<Column, { print: 'symbol' }>, row: QuoteListRow) => {
    const v = column.value(row);
    return column.print === 'price' ? price(v, row.places) : column.print === 'money' ? money(v, row.currency) : pct(v);
  };

  const cell = (column: Column, row: QuoteListRow) => {
    if (column.print === 'symbol') {
      return (
        <span key={column.key} className="quote-list-sym">
          <span className="quote-list-logo"><TickerLogo symbol={row.symbol} /></span>
          <span className="quote-list-ticker">{row.symbol}</span>
        </span>
      );
    }
    return <span key={column.key} className={cn('quote-list-num', column.print === 'pct' && tone(column.value(row)))}>{figure(column, row)}</span>;
  };

  const renderRow = (row: QuoteListRow) => {
    const active = activeSymbol != null && sameListing(row.symbol, activeSymbol);
    const isFresh = fresh != null && sameListing(row.symbol, fresh);
    // An extended figure keeps the official close one hover away.
    const title = row.ext ? t('marketView:quoteList.closeTip', { price: price(row.close.price, row.places), pct: pct(row.close.pct) }) : undefined;
    return (
      <li
        key={row.id}
        className={cn('quote-list-row', active && 'quote-list-row--active', isFresh && 'quote-list-row--fresh')}
        ref={isFresh ? (el) => el?.scrollIntoView({ block: 'nearest' }) : undefined}
        onAnimationEnd={isFresh ? () => setFresh(null) : undefined}
      >
        <button
          type="button"
          className="quote-list-row-main"
          onClick={() => onPick(row.symbol)}
          title={title}
          aria-current={active || undefined}
          // Each figure said with its heading, which the row's cells cannot carry.
          aria-label={q.columns.map((c) => (c.print === 'symbol' ? row.symbol : `${t(c.label)} ${figure(c, row)}`)).join(', ')}
        >
          {q.columns.map((c) => cell(c, row))}
        </button>
        <button
          type="button"
          className="quote-list-remove"
          onClick={() => remove(row)}
          title={t('marketView:sidebar.remove')}
          aria-label={`${t('marketView:sidebar.remove')} ${row.symbol}`}
        >
          <X size={12} />
        </button>
      </li>
    );
  };

  const tabButton = (tab: QuoteListTab, label: string, count: number) => (
    <button
      type="button"
      aria-pressed={q.tab === tab}
      className={cn('quote-list-tab tap-target', q.tab === tab && 'quote-list-tab--on')}
      onClick={() => q.setTab(tab)}
    >
      {label}
      {count > 0 && <span className="quote-list-count">{count}</span>}
    </button>
  );

  const addLabel = adding
    ? t('marketView:quoteList.add.close')
    : isWatchlist ? t('marketView:sidebar.addToWatchlist') : t('marketView:sidebar.addHolding');

  return (
    <div className={cn('quote-list', !isWatchlist && 'quote-list--holdings')}>
      <div className="quote-list-head">
        <div className="quote-list-tabs">
          {tabButton('watchlist', t('marketView:header.watchlist'), q.counts.watchlist)}
          {tabButton('holdings', t('marketView:quoteList.holdings'), q.counts.holdings)}
        </div>
        <span className="quote-list-spacer" />
        <button
          ref={addButton}
          type="button"
          className={cn('quote-list-icon-btn tap-target', adding && 'quote-list-icon-btn--on')}
          onClick={() => (adding ? stopAdding() : setAdding(true))}
          title={addLabel}
          aria-label={addLabel}
          aria-expanded={adding}
        >
          {adding ? <X size={14} /> : <Plus size={14} />}
        </button>
      </div>

      {adding ? (
        <QuoteListAdd
          // A tab's search is its own: a half-filled holding never carries to the watchlist.
          key={q.tab}
          tab={q.tab}
          watching={watchlist.rows.map((r) => r.symbol)}
          addToWatchlist={(hit) => watchlist.handleAdd(
            { symbol: hit.symbol, instrument_type: 'stock', exchange: hit.exchangeShortName || '', name: hit.name || '' },
            watchlist.currentWatchlistId,
          )}
          addHolding={portfolio.handleAdd}
          onAdded={(symbol) => {
            setFresh(symbol);
            stopAdding();
          }}
          onCancel={stopAdding}
        />
      ) : (
        <>
          <div className="quote-list-meta">
            {q.session && (
              <span className={cn('quote-list-session', `quote-list-session--${q.session}`)}>
                {q.session === 'pre' && <Sunrise size={12} aria-hidden="true" />}
                {q.session === 'post' && <Sunset size={12} aria-hidden="true" />}
                {t(`marketView:quoteList.session.${q.session}`)}
              </span>
            )}
            {q.live && <span className="quote-list-live" title={t('marketView:sidebar.livePrices')} />}
          </div>
          {!isWatchlist && q.summaries.length > 0 && (
            <div className="quote-list-summary">
              {q.summaries.map((s) => (
                <span key={s.currency} className="quote-list-summary-item">
                  <span className="quote-list-num">{money(s.totalValue, s.currency)}</span>
                  {s.totalCost > 0 && (
                    <>
                      <span className="quote-list-summary-label">{t('marketView:quoteList.col.pl')}</span>
                      <span className={cn('quote-list-num', tone(s.totalPlPct))}>{pct(s.totalPlPct)}</span>
                    </>
                  )}
                </span>
              ))}
            </div>
          )}

          <div className="quote-list-cols">
            {q.columns.map((c) => {
              const sorted = q.sort?.key === c.key ? q.sort.dir : null;
              const label = t(c.label);
              return (
                <button
                  key={c.key}
                  type="button"
                  className={cn('quote-list-col', sorted && 'quote-list-col--sorted')}
                  onClick={() => q.toggleSort(c.key)}
                  // The rows are whole-row pick buttons, not table cells, so the
                  // sort is said in the heading's name rather than aria-sort.
                  aria-label={sorted ? `${label}, ${t(`marketView:quoteList.sorted.${sorted}`)}` : label}
                  title={t('marketView:quoteList.sortBy', { column: label })}
                >
                  {label}
                  {sorted === 'asc' && <ArrowUp size={10} aria-hidden="true" />}
                  {sorted === 'desc' && <ArrowDown size={10} aria-hidden="true" />}
                </button>
              );
            })}
          </div>

          <div className="quote-list-body">
            {q.loading ? (
              Array.from({ length: 5 }, (_, i) => (
                <div key={i} className="quote-list-skeleton">
                  <span style={{ width: 52 }} />
                  <span style={{ width: 48, marginLeft: 'auto' }} />
                  <span style={{ width: 44 }} />
                </div>
              ))
            ) : q.rows.length === 0 ? (
              <p className="quote-list-empty">
                {isWatchlist ? t('marketView:sidebar.emptyWatchlist') : t('marketView:sidebar.emptyPortfolio')}
              </p>
            ) : (
              <ul className="quote-list-rows">{q.rows.map(renderRow)}</ul>
            )}
          </div>
        </>
      )}

      <ConfirmDialog
        // Closing keeps the text, so the dialog does not empty as it leaves.
        open={confirm?.open ?? false}
        title={confirm?.title ?? ''}
        message={confirm?.message ?? ''}
        confirmLabel={t('common.delete')}
        onConfirm={async () => {
          await confirm?.onConfirm();
          setConfirm((c) => c && { ...c, open: false });
        }}
        onOpenChange={(open: boolean) => !open && setConfirm((c) => c && { ...c, open: false })}
      />
    </div>
  );
}
