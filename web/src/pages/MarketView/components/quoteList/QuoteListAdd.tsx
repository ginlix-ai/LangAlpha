import React, { useEffect, useId, useRef, useState } from 'react';
import { Check, Search } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { TickerLogo } from '@/components/TickerLogo';
import { useSymbolSearch } from '@/hooks/useSymbolSearch';
import { isImeComposing } from '@/lib/ime';
import { resolveDualName } from '@/lib/displayName';
import { asksHoldingCurrency, holdingCurrency, isCurrencyCode, typedCurrencyCode } from '@/lib/holdingCurrency';
import type { StockSearchHit } from '@/lib/marketUtils';
import { cn } from '@/lib/utils';
import type { PortfolioHoldingPayload } from '@/pages/Dashboard/utils/portfolio';
import { isMinorUnitListingInMajor } from '@/pages/Dashboard/utils/portfolioSummary';
import { sameListing, type QuoteListTab } from './quoteRows';

/** Marks the panel, so the drawer leaves Escape to it rather than closing. */
export const ADD_PANEL_ATTR = 'data-quote-list-add';

interface QuoteListAddProps {
  tab: QuoteListTab;
  /** Symbols already on the watchlist, which are marked instead of offered. */
  watching: string[];
  addToWatchlist: (hit: StockSearchHit) => Promise<boolean>;
  addHolding: (payload: PortfolioHoldingPayload) => Promise<boolean>;
  /** Called with the symbol once it is on the list. */
  onAdded: (symbol: string) => void;
  onCancel: () => void;
}

const exchangeOf = (hit: StockSearchHit) => hit.exchangeShortName || hit.stockExchange || '';

/**
 * Adding from inside the list: a search where the rows were, where a watchlist
 * pick goes on at once and a holding pick asks only what a holding cannot do
 * without (quantity and average cost, and the currency where the listing does
 * not say). Notes, alerts and account names stay with the Dashboard's dialogs.
 */
export function QuoteListAdd({ tab, watching, addToWatchlist, addHolding, onAdded, onCancel }: QuoteListAddProps): React.ReactElement {
  const { t } = useTranslation();
  const [query, setQuery] = useState('');
  const { hits, loading } = useSymbolSearch(query, 20);
  const [cursor, setCursor] = useState(0);
  const [picked, setPicked] = useState<StockSearchHit | null>(null);
  const [pending, setPending] = useState<string | null>(null);
  const active = Math.min(cursor, hits.length - 1);
  // An add still in flight when the panel goes (cancelled, or another tab's
  // panel took its place) must not close whatever is open by then.
  const mounted = useRef(true);
  useEffect(() => {
    mounted.current = true;
    return () => { mounted.current = false; };
  }, []);
  const finish = (ok: boolean, symbol: string) => {
    if (!mounted.current) return;
    setPending(null);
    if (ok) onAdded(symbol);
  };

  const isWatched = (hit: StockSearchHit) => watching.some((s) => sameListing(s, hit.symbol));

  const listId = useId();
  const optionId = (i: number) => `${listId}-${i}`;

  const choose = async (hit: StockSearchHit | undefined) => {
    // The hits on screen still answer the last query until this one lands.
    if (!hit || pending || loading) return;
    if (tab === 'holdings') {
      setPicked(hit);
      return;
    }
    if (isWatched(hit)) return;
    setPending(hit.symbol);
    finish(await addToWatchlist(hit).catch(() => false), hit.symbol);
  };

  const onFieldKey = (e: React.KeyboardEvent<HTMLInputElement>) => {
    // A key that belongs to an input method edits the query; it never picks.
    if (isImeComposing(e)) return;
    if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
      e.preventDefault();
      if (hits.length) setCursor((active + (e.key === 'ArrowDown' ? 1 : hits.length - 1)) % hits.length);
    } else if (e.key === 'Enter') {
      e.preventDefault();
      void choose(hits[active]);
    }
  };

  const status = loading && hits.length === 0
    ? t('dashboard.addWatchlistDialog.searching')
    : hits.length === 0 && query.trim()
      ? t('dashboard.addWatchlistDialog.noResults')
      : null;

  return (
    <div
      className="quote-list-add"
      {...{ [ADD_PANEL_ATTR]: '' }}
      onKeyDown={(e) => {
        if (e.key !== 'Escape' || isImeComposing(e)) return;
        e.preventDefault();
        if (picked) setPicked(null);
        else onCancel();
      }}
    >
      {picked ? (
        <HoldingForm hit={picked} pending={pending != null} onBack={() => setPicked(null)} onSubmit={async (payload) => {
          setPending(payload.symbol);
          finish(await addHolding(payload).catch(() => false), payload.symbol);
        }} />
      ) : (
        <>
          <label className="quote-list-search rings-within owns-its-edge">
            <Search size={13} aria-hidden="true" />
            <input
              autoFocus
              type="text"
              value={query}
              onChange={(e) => {
                setQuery(e.target.value);
                setCursor(0);
              }}
              onKeyDown={onFieldKey}
              // Focus stays in the field; the arrowed result is announced from here.
              role="combobox"
              aria-autocomplete="list"
              aria-expanded={!status && hits.length > 0}
              aria-controls={listId}
              aria-activedescendant={!status && hits.length > 0 ? optionId(active) : undefined}
              placeholder={t('marketView.quoteList.add.placeholder')}
              aria-label={t('marketView.quoteList.add.placeholder')}
              spellCheck={false}
              autoComplete="off"
            />
          </label>
          <div className="quote-list-body">
            {status ? (
              <p className="quote-list-empty">{status}</p>
            ) : (
              <ul id={listId} className="quote-list-rows" role="listbox" aria-label={t('marketView.quoteList.add.results')} aria-busy={loading}>
                {hits.map((hit, i) => {
                  const watched = tab === 'watchlist' && isWatched(hit);
                  const exchange = exchangeOf(hit);
                  return (
                    <li key={`${hit.symbol}-${i}`} id={optionId(i)} role="option" aria-selected={i === active}>
                      <button
                        type="button"
                        className={cn('quote-list-hit', i === active && 'quote-list-hit--active')}
                        onClick={() => void choose(hit)}
                        onMouseMove={() => i !== active && setCursor(i)}
                        disabled={watched || pending != null}
                        ref={i === active ? (el) => el?.scrollIntoView({ block: 'nearest' }) : undefined}
                      >
                        <span className="quote-list-logo"><TickerLogo symbol={hit.symbol} /></span>
                        <span className="quote-list-hit-symbol">{hit.symbol}</span>
                        <span className="quote-list-hit-name">
                          {resolveDualName(hit, hit.symbol).primary}
                          {exchange && <span className="quote-list-hit-exchange">{exchange}</span>}
                        </span>
                        {watched ? (
                          <span className="quote-list-hit-done"><Check size={11} aria-hidden="true" />{t('marketView.quoteList.add.watching')}</span>
                        ) : tab === 'watchlist' ? (
                          <span className="quote-list-hit-add">{t('marketView.quoteList.add.add')}</span>
                        ) : null}
                      </button>
                    </li>
                  );
                })}
              </ul>
            )}
          </div>
        </>
      )}
    </div>
  );
}

interface HoldingFormProps {
  hit: StockSearchHit;
  pending: boolean;
  onBack: () => void;
  onSubmit: (payload: PortfolioHoldingPayload) => Promise<void>;
}

/** What a holding needs before it can be valued; mistakes are said under the fields. */
function HoldingForm({ hit, pending, onBack, onSubmit }: HoldingFormProps): React.ReactElement {
  const { t } = useTranslation();
  const [quantity, setQuantity] = useState('');
  const [cost, setCost] = useState('');
  const [typedCurrency, setTypedCurrency] = useState('');
  const [error, setError] = useState<string | null>(null);
  const asksCurrency = asksHoldingCurrency(hit);
  const currency = asksCurrency ? typedCurrencyCode(typedCurrency) : holdingCurrency(hit);
  const copy = (key: string, opts?: Record<string, string>) => t(`dashboard.addPortfolioDialog.${key}`, opts);

  const submit = (e: React.FormEvent) => {
    e.preventDefault();
    // The whole string or nothing: parseFloat would read `1,000` as 1.
    const qty = Number(quantity.trim());
    const avg = Number(cost.trim());
    const problem = !quantity.trim() ? 'alertQuantityMissing'
      : !(Number.isFinite(qty) && qty > 0) ? 'alertQuantityInvalid'
        : !cost.trim() ? 'alertAverageCostMissing'
          : !(Number.isFinite(avg) && avg > 0) ? 'alertAverageCostInvalid'
            : asksCurrency && !isCurrencyCode(currency) ? 'alertCurrencyMissing'
              : null;
    setError(problem && copy(problem));
    if (problem) return;
    void onSubmit({
      symbol: hit.symbol,
      instrument_type: 'stock',
      exchange: hit.exchangeShortName || '',
      name: hit.name || '',
      quantity: qty,
      average_cost: avg,
      currency,
      first_purchased_at: new Date().toISOString(),
    });
  };

  return (
    <form className="quote-list-form" onSubmit={submit} noValidate>
      <div className="quote-list-form-head">
        <span className="quote-list-logo"><TickerLogo symbol={hit.symbol} /></span>
        <span className="quote-list-hit-symbol">{hit.symbol}</span>
        <span className="quote-list-hit-name">{resolveDualName(hit, hit.symbol).primary}</span>
      </div>
      <div className="quote-list-form-fields">
        <label className="quote-list-field rings-within owns-its-edge">
          <span>{copy('quantityLabel')}</span>
          <input autoFocus inputMode="decimal" value={quantity} onChange={(e) => setQuantity(e.target.value)} placeholder="10" />
        </label>
        <label className="quote-list-field rings-within owns-its-edge">
          <span>{currency ? copy('averageCostIn', { currency }) : copy('averageCostLabel')}</span>
          <input
            inputMode="decimal"
            value={cost}
            onChange={(e) => setCost(e.target.value)}
            placeholder={isMinorUnitListingInMajor(hit.symbol, currency) ? '1.75' : '175.50'}
          />
        </label>
        {asksCurrency && (
          <label className="quote-list-field rings-within owns-its-edge">
            <span>{copy('currencyLabel')}</span>
            <input maxLength={3} value={typedCurrency} onChange={(e) => setTypedCurrency(e.target.value)} placeholder="BRL" />
          </label>
        )}
      </div>
      {error && <p className="quote-list-form-error" role="alert">{error}</p>}
      <button type="submit" className="quote-list-form-submit" disabled={pending}>
        {t('marketView.sidebar.addHolding')}
      </button>
      <button type="button" className="quote-list-form-back tap-target" onClick={onBack}>
        {t('marketView.quoteList.add.back')}
      </button>
    </form>
  );
}
