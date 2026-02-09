import { useCallback, useEffect, useState } from 'react';
import { useToast } from '@/components/ui/use-toast';
import {
  DEFAULT_USER_ID,
  addPortfolioHolding,
  deletePortfolioHolding,
  getPortfolio,
  getStockPrices,
  updatePortfolioHolding,
} from '../utils/api';

// Module-level cache (survives navigation, clears on page refresh)
let portfolioCache = null; // { rows, hasRealHoldings }

/**
 * Shared hook for portfolio data fetching and CRUD operations.
 * Used by both Dashboard and TradingCenter sidebar.
 */
export function usePortfolioData() {
  const { toast } = useToast();

  const [rows, setRows] = useState(() => portfolioCache?.rows || []);
  const [loading, setLoading] = useState(!portfolioCache);
  const [hasRealHoldings, setHasRealHoldings] = useState(() => portfolioCache?.hasRealHoldings || false);
  const [modalOpen, setModalOpen] = useState(false);
  const [editRow, setEditRow] = useState(null);
  const [editForm, setEditForm] = useState({ quantity: '', averageCost: '', notes: '' });

  const fetchPortfolio = useCallback(async () => {
    if (!portfolioCache) setLoading(true);
    try {
      const { holdings } = await getPortfolio(DEFAULT_USER_ID);
      const symbols = holdings?.length
        ? holdings.map((h) => String(h.symbol || '').trim().toUpperCase())
        : [];
      const prices = symbols.length > 0 ? await getStockPrices(symbols) : [];
      const bySym = Object.fromEntries((prices || []).map((p) => [p.symbol, p]));

      if (holdings?.length) {
        setHasRealHoldings(true);
        const combined = holdings.map((h) => {
          const sym = String(h.symbol || '').trim().toUpperCase();
          const p = bySym[sym] || {};
          const q = Number(h.quantity || 0);
          const ac = h.average_cost != null ? Number(h.average_cost) : null;
          const price = p.price ?? 0;
          const marketValue = q * price;
          const plPct = ac != null && ac > 0 ? ((price - ac) / ac) * 100 : null;
          return {
            holding_id: h.holding_id,
            symbol: sym,
            quantity: q,
            average_cost: ac,
            notes: h.notes ?? '',
            price,
            marketValue,
            unrealizedPlPercent: plPct,
            isPositive: plPct == null ? true : plPct >= 0,
          };
        });
        setRows(combined);
        portfolioCache = { rows: combined, hasRealHoldings: true };
      } else {
        setHasRealHoldings(false);
        setRows([]);
        portfolioCache = { rows: [], hasRealHoldings: false };
      }
    } catch (error) {
      console.error('[usePortfolioData] Error fetching portfolio:', error);
      if (!portfolioCache) {
        setHasRealHoldings(false);
        setRows([]);
      }
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchPortfolio();
    const intervalId = setInterval(() => {
      fetchPortfolio();
    }, 60000);
    return () => clearInterval(intervalId);
  }, [fetchPortfolio]);

  const handleAdd = useCallback(
    async (payload, userId) => {
      try {
        await addPortfolioHolding(payload, userId || DEFAULT_USER_ID);
        setModalOpen(false);
        portfolioCache = null;
        fetchPortfolio();

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
    [fetchPortfolio, toast]
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
            await deletePortfolioHolding(holdingId, DEFAULT_USER_ID);
            portfolioCache = null;
            fetchPortfolio();
          } catch (e) {
            console.error('Delete portfolio holding failed:', e?.response?.status, e?.response?.data, e?.message);
          }
        },
      };
    },
    [fetchPortfolio]
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
    if (!editRow?.holding_id) return;
    const q = Number(editForm.quantity);
    const ac = Number(editForm.averageCost);
    if (!Number.isFinite(q) || q <= 0 || !Number.isFinite(ac) || ac <= 0) return;
    try {
      await updatePortfolioHolding(
        editRow.holding_id,
        {
          quantity: q,
          average_cost: ac,
          notes: editForm.notes.trim() || undefined,
        },
        DEFAULT_USER_ID
      );
      setEditRow(null);
      portfolioCache = null;
      fetchPortfolio();
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
  }, [editRow, editForm, fetchPortfolio, toast]);

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
