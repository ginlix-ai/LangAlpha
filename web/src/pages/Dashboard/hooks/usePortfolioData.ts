import { useCallback, useMemo, useState } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { useToast } from '@/components/ui/use-toast';
import {
  addPortfolioHolding,
  deletePortfolioHolding,
  updatePortfolioHolding,
} from '../utils/api';
import { portfolioHoldingsQuery } from '@/hooks/membershipQueries';
import { useQuotes, snapshotToStockPrice } from '@/lib/quotes';
import { quotePlaces } from '@/lib/bars/currencyDisplay';
import type { PortfolioHoldingPayload, PortfolioHoldingUpdatePayload } from '../utils/portfolio';
import { costBasis } from '../utils/portfolioSummary';
import type { ExtendedSessionFields, StockPrice } from '@/types/market';

export interface PortfolioRow {
  user_portfolio_id?: string | number;
  symbol: string;
  quantity?: number | null;
  average_cost?: number | null;
  /** The currency its price and value are in: the quote's, else the holding's. */
  currency: string;
  /** The currency its average cost was entered in. */
  costCurrency: string;
  notes?: string;
  price: number;
  marketValue?: number | null;
  unrealizedPlPercent?: number | null;
  isPositive?: boolean;
  quoteAvailable?: boolean;
  /** Places its prices print up to (lib/bars `quotePlaces`), from the quote's own currency and asset class, not the holding's. */
  places: number;
  session?: ExtendedSessionFields;
  [key: string]: unknown;
}

interface PortfolioEditForm {
  quantity: string;
  averageCost: string;
  notes: string;
}

interface DeleteConfirmConfig {
  open: boolean;
  title: string;
  message: string;
  onConfirm: () => Promise<void>;
}

interface ApiError {
  response?: {
    status?: number;
    data?: {
      detail?: string;
      message?: string;
      [key: string]: unknown;
    };
  };
  message?: string;
}

/**
 * Shared hook for portfolio data fetching and CRUD operations.
 * Used by both Dashboard and MarketView sidebar.
 * Refactored to use TanStack Query for optimal polling and caching.
 */
export function usePortfolioData() {
  const { toast } = useToast();
  const queryClient = useQueryClient();

  const [modalOpen, setModalOpen] = useState(false);
  const [editRow, setEditRow] = useState<PortfolioRow | null>(null);
  const [editForm, setEditForm] = useState<PortfolioEditForm>({ quantity: '', averageCost: '', notes: '' });

  // Holdings membership is its own query (keyed ['portfolioData'] so CRUD
  // invalidation + polling are unchanged); per-symbol quotes come from the
  // shared quote layer so a symbol held here and watched elsewhere fetch once.
  const { data: holdings = [], isLoading: holdingsLoading, isSuccess: loaded, refetch: refetchHoldings } = useQuery({
    ...portfolioHoldingsQuery,
    refetchInterval: 60000,
    refetchIntervalInBackground: false,
  });

  const symbols = useMemo(
    () => holdings.map((h) => String(h.symbol || '').trim().toUpperCase()).filter(Boolean),
    [holdings]
  );

  const { quotes, isLoading: quotesLoading, refetch: refetchQuotes } = useQuotes(symbols, {
    staleTime: 1000 * 30,
    refetchInterval: 60000,
  });

  const rows = useMemo<PortfolioRow[]>(() => {
    if (!holdings.length) return [];
    const bySym: Record<string, StockPrice> = Object.fromEntries(
      symbols.map((s) => [s, snapshotToStockPrice(s, quotes[s])])
    );
    return holdings.map((h) => {
      const sym = String(h.symbol || '').trim().toUpperCase();
      const p = bySym[sym] || {} as Partial<StockPrice>;
      const q = Number(h.quantity || 0);
      const ac = h.average_cost != null ? Number(h.average_cost) : null;
      const quoteAvailable = p.quoteAvailable !== false && p.price != null;
      const price = quoteAvailable ? p.price ?? 0 : 0;
      const marketValue = quoteAvailable ? q * price : null;
      const costCurrency = h.currency ?? 'USD';
      const currency = p.currency ?? costCurrency;
      const basis = costBasis({ symbol: sym, average_cost: ac, quantity: q, currency, costCurrency });
      const plPct = marketValue != null && basis != null && basis > 0 ? ((marketValue - basis) / basis) * 100 : null;
      return {
        user_portfolio_id: h.user_portfolio_id,
        symbol: sym,
        quantity: q,
        average_cost: ac,
        currency,
        costCurrency,
        notes: h.notes ?? '',
        price,
        marketValue,
        quoteAvailable,
        unrealizedPlPercent: plPct,
        isPositive: quoteAvailable && plPct != null ? plPct >= 0 : true,
        session: p.session,
        places: p.places ?? quotePlaces(sym, {}),
      };
    });
  }, [holdings, symbols, quotes]);

  const hasRealHoldings = holdings.length > 0;
  const loading = holdingsLoading || (symbols.length > 0 && quotesLoading);

  const fetchPortfolio = useCallback(async () => {
    await Promise.all([refetchHoldings(), Promise.resolve(refetchQuotes())]);
  }, [refetchHoldings, refetchQuotes]);

  const handleAdd = useCallback(
    async (payload: PortfolioHoldingPayload) => {
      try {
        await addPortfolioHolding(payload);
        setModalOpen(false);
        queryClient.invalidateQueries({ queryKey: ['portfolioData'] });

        toast({
          title: 'Holding added',
          description: `${payload.symbol} has been added to your portfolio.`,
        });
      } catch (e: unknown) {
        const err = e as ApiError;
        console.error('Add portfolio holding failed:', err?.response?.status, err?.response?.data, err?.message);

        const msg = err?.response?.data?.detail || err?.response?.data?.message || '';

        if (msg.includes('NumericValueOutOfRange') || msg.includes('numeric overflow')) {
          toast({
            variant: 'destructive',
            title: 'Holding amount too large',
            description: 'The total position value exceeds system limits. Try reducing quantity or price.',
          });
        } else {
          toast({
            variant: 'destructive',
            title: 'Cannot add holding',
            description: msg || 'Failed to add holding. Please try again.',
          });
        }
      }
    },
    [queryClient, toast]
  );

  const handleDelete = useCallback(
    (holdingId: string): DeleteConfirmConfig => {
      // Returns the confirm config so the caller can use a ConfirmDialog
      return {
        open: true,
        title: 'Remove holding',
        message: 'Remove this holding from your portfolio?',
        onConfirm: async () => {
          try {
            await deletePortfolioHolding(holdingId);
            queryClient.invalidateQueries({ queryKey: ['portfolioData'] });
          } catch (e: unknown) {
            const err = e as ApiError;
            console.error('Delete portfolio holding failed:', err?.response?.status, err?.response?.data, err?.message);
          }
        },
      };
    },
    [queryClient]
  );

  const openEdit = useCallback((row: PortfolioRow | null) => {
    setEditRow(row);
    if (row) {
      setEditForm({
        quantity: row.quantity != null ? String(row.quantity) : '',
        averageCost: row.average_cost != null ? String(row.average_cost) : '',
        notes: row.notes ?? '',
      });
    }
  }, []);

  const handleUpdate = useCallback(async (): Promise<void> => {
    if (!editRow?.user_portfolio_id) return;
    const q = Number(editForm.quantity);
    const ac = Number(editForm.averageCost);
    if (!Number.isFinite(q) || q <= 0 || !Number.isFinite(ac) || ac <= 0) return;
    try {
      await updatePortfolioHolding(
        String(editRow.user_portfolio_id),
        {
          quantity: q,
          average_cost: ac,
          notes: editForm.notes.trim() || undefined,
        } as PortfolioHoldingUpdatePayload
      );
      setEditRow(null);
      queryClient.invalidateQueries({ queryKey: ['portfolioData'] });
    } catch (e: unknown) {
      const err = e as ApiError;
      console.error('Update portfolio holding failed:', err?.response?.status, err?.response?.data, err?.message);

      const msg = err?.response?.data?.detail || err?.response?.data?.message || '';

      if (msg.includes('NumericValueOutOfRange')) {
        toast({
          variant: 'destructive',
          title: 'Holding amount too large',
          description: 'The total position value exceeds system limits. Try reducing quantity or price.',
        });
      } else {
        toast({
          variant: 'destructive',
          title: 'Update failed',
          description: 'Something went wrong while saving your portfolio.',
        });
      }
    }
  }, [editRow, editForm, queryClient, toast]);

  return {
    rows,
    loading,
    /** The holdings arrived, as opposed to a read that failed or never ran. */
    loaded,
    hasRealHoldings,
    modalOpen,
    setModalOpen,
    editRow,
    editForm,
    setEditForm,
    openEdit,
    handleUpdate,
    handleAdd,
    handleDelete,
    fetchPortfolio,
  };
}
