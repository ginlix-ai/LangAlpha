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

/**
 * Shared hook for portfolio data fetching and CRUD operations.
 * Used by both Dashboard and MarketView sidebar.
 * Refactored to use TanStack Query for optimal polling and caching.
 */
export function usePortfolioData() {
  const { toast } = useToast();
  const queryClient = useQueryClient();

  const [modalOpen, setModalOpen] = useState(false);
  const [editRow, setEditRow] = useState(null);
  const [editForm, setEditForm] = useState({ quantity: '', averageCost: '', notes: '' });

  const { data = { rows: [], hasRealHoldings: false }, isLoading: loading, refetch: fetchPortfolio } = useQuery({
    queryKey: ['portfolioData'],
    queryFn: async () => {
      const { holdings } = await getPortfolio();
      const symbols = holdings?.length
        ? holdings.map((h) => String(h.symbol || '').trim().toUpperCase())
        : [];
      const prices = symbols.length > 0 ? await getStockPrices(symbols) : [];
      const bySym = Object.fromEntries((prices || []).map((p) => [p.symbol, p]));

      if (holdings?.length) {
        const combined = holdings.map((h) => {
          const sym = String(h.symbol || '').trim().toUpperCase();
          const p = bySym[sym] || {};
          const q = Number(h.quantity || 0);
          const ac = h.average_cost != null ? Number(h.average_cost) : null;
          const price = p.price ?? 0;
          const marketValue = q * price;
          const plPct = ac != null && ac > 0 ? ((price - ac) / ac) * 100 : null;
          return {
            user_portfolio_id: h.user_portfolio_id,
            symbol: sym,
            quantity: q,
            average_cost: ac,
            notes: h.notes ?? '',
            price,
            marketValue,
            unrealizedPlPercent: plPct,
            isPositive: plPct == null ? true : plPct >= 0,
            earlyTradingChangePercent: p.earlyTradingChangePercent ?? null,
            lateTradingChangePercent: p.lateTradingChangePercent ?? null,
          };
        });
        return { rows: combined, hasRealHoldings: true };
      }
      return { rows: [], hasRealHoldings: false };
    },
    refetchInterval: 60000,
    staleTime: 1000 * 30, // 30s fresh cache
  });

  const { rows, hasRealHoldings } = data;

  const handleAdd = useCallback(
    async (payload) => {
      try {
        await addPortfolioHolding(payload);
        setModalOpen(false);
        queryClient.invalidateQueries({ queryKey: ['portfolioData'] });

        toast({
          title: 'Holding added',
          description: `${payload.symbol} has been added to your portfolio.`,
        });
      } catch (e) {
        console.error('Add portfolio holding failed:', e?.response?.status, e?.response?.data, e?.message);

        const msg = e?.response?.data?.detail || e?.response?.data?.message || '';

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
    (holdingId) => {
      // Returns the confirm config so the caller can use a ConfirmDialog
      return {
        open: true,
        title: 'Remove holding',
        message: 'Remove this holding from your portfolio?',
        onConfirm: async () => {
          try {
            await deletePortfolioHolding(holdingId);
            queryClient.invalidateQueries({ queryKey: ['portfolioData'] });
          } catch (e) {
            console.error('Delete portfolio holding failed:', e?.response?.status, e?.response?.data, e?.message);
          }
        },
      };
    },
    [queryClient]
  );

  const openEdit = useCallback((row) => {
    setEditRow(row);
    setEditForm({
      quantity: row.quantity != null ? String(row.quantity) : '',
      averageCost: row.average_cost != null ? String(row.average_cost) : '',
      notes: row.notes ?? '',
    });
  }, []);

  const handleUpdate = useCallback(async () => {
    if (!editRow?.user_portfolio_id) return;
    const q = Number(editForm.quantity);
    const ac = Number(editForm.averageCost);
    if (!Number.isFinite(q) || q <= 0 || !Number.isFinite(ac) || ac <= 0) return;
    try {
      await updatePortfolioHolding(
        editRow.user_portfolio_id,
        {
          quantity: q,
          average_cost: ac,
          notes: editForm.notes.trim() || undefined,
        }
      );
      setEditRow(null);
      queryClient.invalidateQueries({ queryKey: ['portfolioData'] });
    } catch (e) {
      console.error('Update portfolio holding failed:', e?.response?.status, e?.response?.data, e?.message);

      const msg = e?.response?.data?.detail || e?.response?.data?.message || '';

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
