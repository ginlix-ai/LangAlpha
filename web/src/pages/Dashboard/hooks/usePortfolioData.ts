import { useCallback, useState } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { useToast } from '@/components/ui/use-toast';
import {
  addPortfolioHolding,
  deletePortfolioHolding,
  getPortfolio,
  getStockPrices,
  updatePortfolioHolding,
} from '../utils/api';
import type { PortfolioHoldingPayload, PortfolioHoldingUpdatePayload } from '../utils/portfolio';
import type { StockPrice } from '@/types/market';

export interface PortfolioRow {
  user_portfolio_id?: string | number;
  symbol: string;
  quantity?: number | null;
  average_cost?: number | null;
  currency: string;
  notes?: string;
  price: number;
  marketValue?: number | null;
  unrealizedPlPercent?: number | null;
  isPositive?: boolean;
  quoteAvailable?: boolean;
  previousClose?: number | null;
  earlyTradingChangePercent?: number | null;
  lateTradingChangePercent?: number | null;
  [key: string]: unknown;
}

interface PortfolioQueryData {
  rows: PortfolioRow[];
  hasRealHoldings: boolean;
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

  const { data = { rows: [], hasRealHoldings: false }, isLoading: loading, refetch: fetchPortfolio } = useQuery<PortfolioQueryData>({
    queryKey: ['portfolioData'],
    queryFn: async (): Promise<PortfolioQueryData> => {
      const { holdings } = await getPortfolio() as { holdings?: Array<{ user_portfolio_id: string; symbol: string; quantity?: number; average_cost?: number | null; currency?: string | null; notes?: string; [key: string]: unknown }> };
      const symbols = holdings?.length
        ? holdings.map((h) => String(h.symbol || '').trim().toUpperCase())
        : [];
      const prices: StockPrice[] = symbols.length > 0 ? await getStockPrices(symbols) : [];
      const bySym: Record<string, StockPrice> = Object.fromEntries((prices || []).map((p) => [p.symbol, p]));

      if (holdings?.length) {
        const combined: PortfolioRow[] = holdings.map((h) => {
          const sym = String(h.symbol || '').trim().toUpperCase();
          const p = bySym[sym] || {} as Partial<StockPrice>;
          const q = Number(h.quantity || 0);
          const ac = h.average_cost != null ? Number(h.average_cost) : null;
          const quoteAvailable = p.quoteAvailable !== false && p.price != null;
          const price = quoteAvailable ? p.price ?? 0 : 0;
          const marketValue = quoteAvailable ? q * price : null;
          const plPct = quoteAvailable && ac != null && ac > 0 ? ((price - ac) / ac) * 100 : null;
          return {
            user_portfolio_id: h.user_portfolio_id,
            symbol: sym,
            quantity: q,
            average_cost: ac,
            currency: h.currency ?? 'USD',
            notes: h.notes ?? '',
            price,
            marketValue,
            quoteAvailable,
            unrealizedPlPercent: plPct,
            isPositive: quoteAvailable && plPct != null ? plPct >= 0 : true,
            previousClose: p.previousClose ?? null,
            earlyTradingChangePercent: p.earlyTradingChangePercent ?? null,
            lateTradingChangePercent: p.lateTradingChangePercent ?? null,
          };
        });
        return { rows: combined, hasRealHoldings: true };
      }
      return { rows: [], hasRealHoldings: false };
    },
    refetchInterval: 60000,
    refetchIntervalInBackground: false,
    staleTime: 1000 * 30, // 30s fresh cache
  });

  const { rows, hasRealHoldings } = data;

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
