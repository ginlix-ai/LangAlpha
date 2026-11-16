import { useCallback, useEffect, useMemo, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { useWatchlistData } from '@/pages/Dashboard/hooks/useWatchlistData';
import { usePortfolioData } from '@/pages/Dashboard/hooks/usePortfolioData';
import { marketStatusQuery } from '@/lib/marketUtils';
import { useMarketDataWSContext } from '../../contexts/MarketDataWSContext';
import { loadPref, savePref } from '../../utils/prefs';
import {
  COLUMNS, listSession, nextSort, quoteRow, sortRows, valueHoldings,
  type QuoteListTab, type SortKey, type SortState,
} from './quoteRows';

const TAB_PREF = 'quote-list-tab';
const SORT_PREF = (tab: QuoteListTab) => `quote-list-sort-${tab}`;

function readTab(): QuoteListTab {
  return loadPref<unknown>(TAB_PREF, 'watchlist') === 'holdings' ? 'holdings' : 'watchlist';
}

/** A saved sort, kept only while it still names one of the tab's columns. */
function readSort(tab: QuoteListTab): SortState | null {
  const raw = loadPref<Partial<SortState> | null>(SORT_PREF(tab), null);
  const known = COLUMNS[tab].some((c) => c.key === raw?.key);
  return known && raw?.key && (raw.dir === 'asc' || raw.dir === 'desc') ? { key: raw.key, dir: raw.dir } : null;
}

/**
 * The watchlist and holdings as the quote list shows them, with the tab and
 * sort the user left them on. Every symbol in either list is on the
 * market-data socket while the list is mounted, so switching tabs shows live
 * figures at once.
 */
export function useQuoteList() {
  const watchlist = useWatchlistData();
  const portfolio = usePortfolioData();
  const { prices: wsPrices, connectionStatus, subscribe, unsubscribe } = useMarketDataWSContext();
  // The same query the chart's header reads, so the two never disagree on the session.
  const { data: marketStatus = null } = useQuery({
    ...marketStatusQuery(),
    refetchInterval: 60000,
    refetchIntervalInBackground: false,
  });

  const [tab, setTabState] = useState<QuoteListTab>(readTab);
  const setTab = useCallback((next: QuoteListTab) => {
    setTabState(next);
    savePref(TAB_PREF, next);
  }, []);
  const [sorts, setSorts] = useState<Record<QuoteListTab, SortState | null>>(() => ({
    watchlist: readSort('watchlist'),
    holdings: readSort('holdings'),
  }));
  const toggleSort = (key: SortKey) => {
    const next = nextSort(sorts[tab], key);
    setSorts({ ...sorts, [tab]: next });
    savePref(SORT_PREF(tab), next);
  };

  // Only a change in which symbols are listed resubscribes, not each poll's new rows.
  const symbolsKey = useMemo(() => [...new Set([
    ...watchlist.rows.map((r) => r.symbol),
    ...portfolio.rows.map((r) => r.symbol),
  ])].filter(Boolean).sort().join(','), [watchlist.rows, portfolio.rows]);
  useEffect(() => {
    const symbols = symbolsKey ? symbolsKey.split(',') : [];
    if (symbols.length) subscribe(symbols);
    return () => { if (symbols.length) unsubscribe(symbols); };
  }, [symbolsKey, subscribe, unsubscribe]);

  const watchRows = useMemo(
    () => watchlist.rows.map((r) => quoteRow(r, String(r.watchlist_item_id), marketStatus, wsPrices.get(r.symbol))),
    [watchlist.rows, marketStatus, wsPrices],
  );
  const holdings = useMemo(
    () => valueHoldings(portfolio.rows, (h) => quoteRow(h, String(h.user_portfolio_id), marketStatus, wsPrices.get(h.symbol))),
    [portfolio.rows, marketStatus, wsPrices],
  );

  const shown = tab === 'watchlist' ? watchRows : holdings.rows;
  const columns = COLUMNS[tab];
  return {
    tab,
    setTab,
    columns,
    sort: sorts[tab],
    toggleSort,
    rows: sortRows(shown, sorts[tab], columns),
    counts: { watchlist: watchlist.rows.length, holdings: portfolio.rows.length },
    loading: tab === 'watchlist' ? watchlist.loading : portfolio.loading,
    summaries: holdings.summaries,
    session: listSession(shown),
    live: connectionStatus === 'connected' && wsPrices.size > 0,
    watchlist,
    portfolio,
  };
}
