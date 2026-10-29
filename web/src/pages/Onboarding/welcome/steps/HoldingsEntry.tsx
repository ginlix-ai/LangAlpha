import { useEffect, useId, useRef, useState, type FormEvent, type KeyboardEvent } from 'react';
import { useTranslation } from 'react-i18next';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { X } from 'lucide-react';
import { Input } from '@/components/ui/input';
import { Loader } from '@/components/ui/loader';
import { toast } from '@/components/ui/use-toast';
import { useSymbolSearch } from '@/hooks/useSymbolSearch';
import type { StockSearchHit } from '@/lib/marketUtils';
import { apiErrorReason, apiErrorStatus } from '@/pages/ChatAgent/utils/api';
import { portfolioHoldingsQuery } from '@/pages/Dashboard/hooks/usePortfolioData';
import { addPortfolioHolding, deletePortfolioHolding } from '@/pages/Dashboard/utils/portfolio';
import { watchlistItemsQuery } from '@/pages/Dashboard/hooks/useWatchlistData';
import { addWatchlistItem, deleteWatchlistItem } from '@/pages/Dashboard/utils/watchlistItems';

const chipClass =
  'inline-flex h-7 items-center gap-1 rounded-full border border-(--color-border-muted) pl-2.5 pr-1 text-xs tabular-nums text-(--color-text-primary)';
const chipRemoveClass =
  'ml-0.5 rounded-full p-0.5 text-(--color-text-tertiary) transition-colors hover:bg-(--color-bg-hover) hover:text-(--color-text-primary)';

interface HoldingsEntryProps {
  /** Handed each add and remove as it starts, settling once the write is done
   *  and its list read again: the flow can be started from a later step while
   *  one is still out, and waits for it there. */
  onPreparing?: (work: Promise<void>) => void;
}

/**
 * What the agent starts from without a brokerage: the tickers the user holds,
 * and the ones they only follow. A symbol is found by search rather than typed, so
 * it is always one the market data knows. Each add is a portfolio or watchlist
 * row like any other, written as it happens, so the dashboard shows the same
 * lists and nothing here has to be saved later.
 */
export function HoldingsEntry({ onPreparing }: HoldingsEntryProps) {
  const { t, i18n } = useTranslation();
  const queryClient = useQueryClient();
  const { data: holdings = [] } = useQuery(portfolioHoldingsQuery);
  // The dashboard's own entry, so this shows the list the dashboard does and
  // writes to it too: the one the dashboard resolved, else the default.
  const { data: watchlistData } = useQuery(watchlistItemsQuery);
  const watchlist = watchlistData?.items ?? [];
  const watchlistId = watchlistData?.currentWatchlistId ?? 'default';
  const searchId = useId();
  const listId = useId();
  const quantityId = useId();
  const searchRef = useRef<HTMLInputElement>(null);
  const quantityRef = useRef<HTMLInputElement>(null);
  const [query, setQuery] = useState('');
  const [open, setOpen] = useState(false);
  const [active, setActive] = useState(0);
  const [picked, setPicked] = useState<StockSearchHit | null>(null);
  const [quantity, setQuantity] = useState('');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const mounted = useRef(true);
  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
    };
  }, []);

  const trimmed = query.trim();
  const { hits, loading } = useSymbolSearch(query, 8, { enabled: picked === null });
  const showList = open && picked === null && trimmed !== '';
  const zh = i18n.language?.toLowerCase().startsWith('zh');
  const optionId = (i: number) => `${listId}-${i}`;

  const amount = Number(quantity);
  const validQuantity = quantity.trim() !== '' && Number.isFinite(amount) && amount > 0;

  function nameOf(hit: StockSearchHit): string {
    return (zh ? hit.nameLocal || hit.name : hit.name || hit.nameEn || hit.nameLocal) || '';
  }

  function pick(hit: StockSearchHit) {
    setPicked(hit);
    setOpen(false);
    setQuery('');
    setError(null);
    // The next thing wanted is how many.
    requestAnimationFrame(() => quantityRef.current?.focus());
  }

  function clear() {
    setPicked(null);
    setQuantity('');
    setError(null);
    requestAnimationFrame(() => searchRef.current?.focus());
  }

  function onSearchKeyDown(event: KeyboardEvent<HTMLInputElement>) {
    if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
      if (hits.length === 0) return;
      event.preventDefault();
      setOpen(true);
      const step = event.key === 'ArrowDown' ? 1 : -1;
      setActive((i) => (i + step + hits.length) % hits.length);
    } else if (event.key === 'Enter') {
      // A form field: with the list open Enter picks, never submits.
      if (showList) {
        event.preventDefault();
        if (hits[active]) pick(hits[active]);
      }
    } else if (event.key === 'Escape' && showList) {
      event.preventDefault();
      event.stopPropagation();
      setOpen(false);
    }
  }

  function fail(key: string, values: Record<string, string>, err: unknown) {
    const reason = apiErrorReason(err);
    const message = t(key, values);
    const said = reason ? `${message} ${reason}` : message;
    // A write still out when the user went on from this turn fails after the
    // entry has left the screen, where a line under it would be read by no
    // one. The lists are then not as the user left them, so it is said in a
    // toast instead.
    if (mounted.current) setError(said);
    else toast({ variant: 'destructive', title: t('common.error'), description: said });
  }

  // Each write is handed to the flow as it starts. Start is a few Continues
  // past this turn and can be pressed while one is out, and the agent's first
  // move is to read these lists, so the flow waits for it there. None of the
  // writes throws: each says its own failure.
  function started(write: Promise<void>) {
    onPreparing?.(write);
  }

  async function toHoldings(event?: FormEvent) {
    // The form's own submit, which Enter in the quantity field also sends; the
    // flow's Enter-to-continue sees it handled and leaves it.
    event?.preventDefault();
    if (!picked || !validQuantity || busy) return;
    const symbol = picked.symbol.toUpperCase();
    setBusy(true);
    setError(null);
    try {
      await addPortfolioHolding({
        symbol,
        instrument_type: 'stock',
        quantity: amount,
        exchange: picked.exchangeShortName,
        currency: picked.currency,
      });
      await queryClient.invalidateQueries({ queryKey: portfolioHoldingsQuery.queryKey });
      clear();
    } catch (err) {
      fail('onboarding.welcome.holdings.failed', { symbol }, err);
    } finally {
      setBusy(false);
    }
  }

  async function toWatchlist() {
    if (!picked || busy) return;
    const symbol = picked.symbol.toUpperCase();
    setBusy(true);
    setError(null);
    try {
      await addWatchlistItem(watchlistId, {
        symbol,
        instrument_type: 'stock',
        exchange: picked.exchangeShortName,
        name: nameOf(picked) || undefined,
      });
      await queryClient.invalidateQueries({ queryKey: watchlistItemsQuery.queryKey });
      clear();
    } catch (err) {
      // Already on the list is the state the user asked for.
      if (apiErrorStatus(err) === 409) {
        clear();
      } else {
        fail('onboarding.welcome.holdings.watchlistFailed', { symbol }, err);
      }
    } finally {
      setBusy(false);
    }
  }

  // Removing is fired from a click, and nothing that waits on it reads its
  // result, so a failure is said here; the list is re-read either way, so a
  // row that did not go stays.
  async function removeHolding(id: string, symbol: string) {
    setError(null);
    try {
      await deletePortfolioHolding(id);
    } catch (err) {
      fail('plugins.bulk.notDone.remove', { what: symbol }, err);
    } finally {
      await queryClient.invalidateQueries({ queryKey: portfolioHoldingsQuery.queryKey });
    }
  }

  async function removeWatch(id: string, symbol: string) {
    setError(null);
    try {
      await deleteWatchlistItem(watchlistId, id);
    } catch (err) {
      fail('plugins.bulk.notDone.remove', { what: symbol }, err);
    } finally {
      await queryClient.invalidateQueries({ queryKey: watchlistItemsQuery.queryKey });
    }
  }

  const groupTitle = 'mb-1.5 text-xs font-medium text-(--color-text-secondary)';

  return (
    <div className="rounded-lg border border-(--color-border-muted) bg-(--color-bg-card) p-4">
      <form onSubmit={(event) => started(toHoldings(event))} className="flex flex-col gap-3">
        <div className="flex flex-col gap-1">
          <label htmlFor={searchId} className="text-xs font-medium text-(--color-text-secondary)">
            {t('onboarding.welcome.holdings.ticker')}
          </label>
          {picked ? (
            <div className="flex h-9 items-center gap-2 rounded-md border border-(--color-border-default) pl-3 pr-1.5 text-sm">
              <span className="font-medium text-(--color-text-primary)">{picked.symbol}</span>
              <span className="min-w-0 flex-1 truncate text-(--color-text-tertiary)">{nameOf(picked)}</span>
              <button
                type="button"
                onClick={clear}
                aria-label={t('onboarding.welcome.holdings.remove', { symbol: picked.symbol })}
                className={chipRemoveClass}
              >
                <X className="h-3.5 w-3.5" />
              </button>
            </div>
          ) : (
            <div className="relative">
              <Input
                ref={searchRef}
                id={searchId}
                role="combobox"
                aria-expanded={showList}
                aria-controls={listId}
                aria-autocomplete="list"
                aria-activedescendant={showList && hits[active] ? optionId(active) : undefined}
                value={query}
                onChange={(e) => {
                  setQuery(e.target.value);
                  setActive(0);
                  setOpen(true);
                }}
                onFocus={() => setOpen(true)}
                onBlur={() => setOpen(false)}
                onKeyDown={onSearchKeyDown}
                placeholder={t('onboarding.welcome.holdings.search')}
                autoComplete="off"
                spellCheck={false}
                maxLength={50}
                data-testid="welcome-holdings-search"
              />
              <div
                id={listId}
                role="listbox"
                hidden={!showList}
                className="absolute left-0 right-0 top-full z-10 mt-1 max-h-64 overflow-y-auto rounded-md border border-(--color-border-default) bg-(--color-bg-card) py-1 shadow-sm"
              >
                {showList && loading && (
                  <div className="flex items-center justify-center px-3 py-3 text-(--color-text-tertiary)">
                    <Loader size={12} className="text-current" label={t('common.loading')} />
                  </div>
                )}
                {showList && !loading && hits.length === 0 && (
                  <p className="px-3 py-2 text-sm text-(--color-text-tertiary)">
                    {t('onboarding.welcome.holdings.noResults', { query: trimmed })}
                  </p>
                )}
                {showList &&
                  !loading &&
                  hits.map((hit, i) => (
                    <div
                      key={`${hit.symbol}-${hit.exchangeShortName ?? ''}-${i}`}
                      id={optionId(i)}
                      role="option"
                      aria-selected={i === active}
                      // Before the input's blur closes the list, or the click is lost.
                      onMouseDown={(e) => {
                        e.preventDefault();
                        pick(hit);
                      }}
                      onMouseEnter={() => setActive(i)}
                      className={`flex cursor-pointer items-baseline gap-2 px-3 py-1.5 text-sm ${
                        i === active ? 'bg-(--color-bg-hover)' : ''
                      }`}
                    >
                      <span className="font-medium text-(--color-text-primary)">{hit.symbol}</span>
                      <span className="min-w-0 flex-1 truncate text-(--color-text-secondary)">{nameOf(hit)}</span>
                      {hit.exchangeShortName && (
                        <span className="shrink-0 text-xs text-(--color-text-tertiary)">{hit.exchangeShortName}</span>
                      )}
                    </div>
                  ))}
              </div>
            </div>
          )}
        </div>

        {picked && (
          <div className="flex flex-col gap-2 sm:flex-row sm:items-end">
            <div className="flex flex-col gap-1 sm:w-32">
              <label htmlFor={quantityId} className="text-xs font-medium text-(--color-text-secondary)">
                {t('onboarding.welcome.holdings.quantity')}
              </label>
              <Input
                ref={quantityRef}
                id={quantityId}
                value={quantity}
                onChange={(e) => setQuantity(e.target.value)}
                inputMode="decimal"
                placeholder="100"
                autoComplete="off"
                className="tabular-nums"
                data-testid="welcome-holdings-quantity"
              />
            </div>
            <button
              type="submit"
              disabled={!validQuantity || busy}
              aria-busy={busy || undefined}
              data-testid="welcome-holdings-to-holdings"
              className="inline-flex h-9 shrink-0 items-center justify-center rounded-md border border-(--color-border-default) px-4 text-sm font-medium text-(--color-text-primary) transition-colors hover:bg-(--color-bg-hover) disabled:cursor-not-allowed disabled:opacity-50"
            >
              {t('onboarding.welcome.holdings.toHoldings')}
            </button>
            <button
              type="button"
              onClick={() => started(toWatchlist())}
              disabled={busy}
              data-testid="welcome-holdings-to-watchlist"
              className="inline-flex h-9 shrink-0 items-center justify-center rounded-md px-3 text-sm text-(--color-text-secondary) transition-colors hover:bg-(--color-bg-hover) hover:text-(--color-text-primary) disabled:cursor-not-allowed disabled:opacity-50"
            >
              {t('onboarding.welcome.holdings.toWatchlist')}
            </button>
          </div>
        )}
      </form>
      {error && (
        <p role="alert" className="mt-2 text-xs text-(--color-loss)">
          {error}
        </p>
      )}
      {holdings.length > 0 && (
        <div className="mt-4">
          <p className={groupTitle}>{t('onboarding.welcome.holdings.holdingsTitle')}</p>
          <ul className="flex flex-wrap gap-1.5" data-testid="welcome-holdings-list">
            {holdings.map((h) => (
              <li key={h.user_portfolio_id} className={chipClass}>
                <span className="font-medium">{h.symbol}</span>
                <span className="text-(--color-text-tertiary)">{h.quantity}</span>
                <button
                  type="button"
                  onClick={() => started(removeHolding(h.user_portfolio_id, h.symbol))}
                  aria-label={t('onboarding.welcome.holdings.remove', { symbol: h.symbol })}
                  className={chipRemoveClass}
                >
                  <X className="h-3 w-3" />
                </button>
              </li>
            ))}
          </ul>
        </div>
      )}
      {watchlist.length > 0 && (
        <div className="mt-4">
          <p className={groupTitle}>{t('onboarding.welcome.holdings.watchlistTitle')}</p>
          <ul className="flex flex-wrap gap-1.5" data-testid="welcome-watchlist-list">
            {watchlist.map((w) => (
              <li key={w.watchlist_item_id} className={chipClass}>
                <span className="font-medium">{w.symbol}</span>
                <button
                  type="button"
                  onClick={() => started(removeWatch(w.watchlist_item_id, w.symbol))}
                  aria-label={t('onboarding.welcome.holdings.watchlistRemove', { symbol: w.symbol })}
                  className={chipRemoveClass}
                >
                  <X className="h-3 w-3" />
                </button>
              </li>
            ))}
          </ul>
        </div>
      )}
    </div>
  );
}
