import { createContext, useContext, useMemo, useCallback, useState } from 'react';
import type { ReactNode } from 'react';
import { useDashboardData } from '../../hooks/useDashboardData';
import { useWatchlistData } from '../../hooks/useWatchlistData';
import { usePortfolioData } from '../../hooks/usePortfolioData';
import { useTickerNews } from '../../hooks/useTickerNews';
import type { PortfolioRow } from '../../hooks/usePortfolioData';

export interface DeleteConfirmState {
  open: boolean;
  title: string;
  message: string;
  onConfirm: (() => Promise<void>) | null;
}

type DashboardData = ReturnType<typeof useDashboardData>;
type WatchlistData = ReturnType<typeof useWatchlistData>;
type PortfolioData = ReturnType<typeof usePortfolioData>;
type TickerNews = ReturnType<typeof useTickerNews>;

interface NewsModalSentiment {
  ticker: string;
  sentiment: string;
  reasoning?: string;
}

/** The clicked news row's full body — the list now inlines description/keywords/
 *  sentiments, so the detail modal renders straight from this with no by-id
 *  round-trip (and still survives one for any future richer-body provider). */
export interface NewsModalFallback {
  title?: string;
  source?: string;
  publishedAt?: string | null;
  tickers?: string[];
  articleUrl?: string | null;
  author?: string | null;
  description?: string | null;
  keywords?: string[];
  sentiments?: NewsModalSentiment[] | null;
  imageUrl?: string | null;
  favicon?: string | null;
}

interface ModalActions {
  selectedNewsId: string | null;
  selectedNewsFallback: NewsModalFallback | null;
  openNews: (id: string | number, fallback?: NewsModalFallback) => void;
  closeNews: () => void;
  selectedMarketInsightId: string | null;
  openInsight: (id: string) => void;
  closeInsight: () => void;
  deleteConfirm: DeleteConfirmState;
  requestDeleteConfirm: (state: Omit<DeleteConfirmState, 'open'>) => void;
  runDeleteConfirm: () => Promise<void>;
  cancelDeleteConfirm: () => void;
}

export interface DashboardDataContextValue {
  dashboard: DashboardData;
  watchlist: WatchlistData;
  portfolio: PortfolioData;
  portfolioNews: TickerNews;
  watchlistNews: TickerNews;
  portfolioHandlers: {
    onAdd: () => void;
    onDelete: (id: string) => void;
    onEdit: (row: PortfolioRow) => void;
  };
  watchlistHandlers: {
    onAdd: () => void;
    onDelete: (id: string) => void;
  };
  modals: ModalActions;
}

const Ctx = createContext<DashboardDataContextValue | null>(null);

export function DashboardDataProvider({ children }: { children: ReactNode }) {
  const dashboard = useDashboardData();
  const watchlist = useWatchlistData();
  const portfolio = usePortfolioData();
  const portfolioNews = useTickerNews(portfolio.rows, 'portfolio', 'tickertick');
  const watchlistNews = useTickerNews(watchlist.rows, 'watchlist', 'tickertick');

  const [selectedNewsId, setSelectedNewsId] = useState<string | null>(null);
  const [selectedNewsFallback, setSelectedNewsFallback] = useState<NewsModalFallback | null>(null);
  const [selectedMarketInsightId, setSelectedMarketInsightId] = useState<string | null>(null);
  const [deleteConfirm, setDeleteConfirm] = useState<DeleteConfirmState>({
    open: false,
    title: '',
    message: '',
    onConfirm: null,
  });

  const openNews = useCallback((id: string | number, fallback?: NewsModalFallback) => {
    setSelectedNewsId(String(id));
    setSelectedNewsFallback(fallback ?? null);
  }, []);
  const closeNews = useCallback(() => {
    setSelectedNewsId(null);
    setSelectedNewsFallback(null);
  }, []);

  const requestDeleteConfirm = useCallback((state: Omit<DeleteConfirmState, 'open'>) => {
    setDeleteConfirm({ ...state, open: true });
  }, []);
  const runDeleteConfirm = useCallback(async () => {
    const fn = deleteConfirm.onConfirm;
    if (fn) await fn();
    setDeleteConfirm((p) => ({ ...p, open: false }));
  }, [deleteConfirm]);
  const cancelDeleteConfirm = useCallback(() => {
    setDeleteConfirm((p) => ({ ...p, open: false }));
  }, []);

  const portfolioHandlers = useMemo(
    () => ({
      onAdd: () => portfolio.setModalOpen(true),
      onDelete: (id: string) => {
        const state = portfolio.handleDelete(id) as Omit<DeleteConfirmState, 'open'>;
        if (state) requestDeleteConfirm(state);
      },
      onEdit: (row: PortfolioRow) => portfolio.openEdit(row),
    }),
    [portfolio, requestDeleteConfirm]
  );

  const watchlistHandlers = useMemo(
    () => ({
      onAdd: () => watchlist.setModalOpen(true),
      onDelete: (id: string) => watchlist.handleDelete(id),
    }),
    [watchlist]
  );

  const closeInsight = useCallback(() => setSelectedMarketInsightId(null), []);

  const modals = useMemo<ModalActions>(
    () => ({
      selectedNewsId,
      selectedNewsFallback,
      openNews,
      closeNews,
      selectedMarketInsightId,
      openInsight: setSelectedMarketInsightId,
      closeInsight,
      deleteConfirm,
      requestDeleteConfirm,
      runDeleteConfirm,
      cancelDeleteConfirm,
    }),
    [
      selectedNewsId,
      selectedNewsFallback,
      openNews,
      closeNews,
      selectedMarketInsightId,
      closeInsight,
      deleteConfirm,
      requestDeleteConfirm,
      runDeleteConfirm,
      cancelDeleteConfirm,
    ]
  );

  // Memoize the context value so unrelated re-renders of the provider don't
  // invalidate every consuming widget.
  const value = useMemo<DashboardDataContextValue>(
    () => ({
      dashboard,
      watchlist,
      portfolio,
      portfolioNews,
      watchlistNews,
      portfolioHandlers,
      watchlistHandlers,
      modals,
    }),
    [
      dashboard,
      watchlist,
      portfolio,
      portfolioNews,
      watchlistNews,
      portfolioHandlers,
      watchlistHandlers,
      modals,
    ]
  );

  return <Ctx.Provider value={value}>{children}</Ctx.Provider>;
}

export function useDashboardContext(): DashboardDataContextValue {
  const v = useContext(Ctx);
  if (!v) throw new Error('useDashboardContext must be used within DashboardDataProvider');
  return v;
}
