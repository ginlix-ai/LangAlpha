import React, { useState } from 'react';
import { MoreVertical, Pencil, Plus, Trash2 } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '../../../components/ui/card';
import { Dialog, DialogContent, DialogHeader, DialogTitle } from '../../../components/ui/dialog';
import { Input } from '../../../components/ui/input';
import { ScrollArea } from '../../../components/ui/scroll-area';

/**
 * Portfolio panel: table + edit modal. Add modal is handled separately via AddPortfolioHoldingDialog.
 */
function PortfolioCard({
  rows = [],
  loading = false,
  hasRealHoldings = false,
  onHeaderAddClick,
  editRow = null,
  editForm = {},
  onEditFormChange,
  onEditSubmit,
  onEditClose,
  onDeleteItem,
  onEditItem,
}) {
  const [menuOpenId, setMenuOpenId] = useState(null);

  const handleDelete = (holdingId) => {
    setMenuOpenId(null);
    onDeleteItem?.(holdingId);
  };

  const handleEdit = (row) => {
    setMenuOpenId(null);
    onEditItem?.(row);
  };

  return (
    <Card className="panel flex flex-col flex-1 min-h-0">
        <CardHeader className="px-3 py-4 flex-shrink-0">
          <button type="button" onClick={onHeaderAddClick} className="flex items-center justify-between w-full text-left">
            <CardTitle className="dashboard-title-font text-base font-semibold" style={{ color: 'var(--color-text-primary)', letterSpacing: '0.15px' }}>
            Add Portfolio Holding
            </CardTitle>
            <Plus className="h-4 w-4 shrink-0" style={{ color: 'var(--color-text-primary)' }} />
          </button>
        </CardHeader>

      <Dialog open={!!editRow} onOpenChange={(open) => !open && onEditClose?.()}>
        <DialogContent className="sm:max-w-sm text-white border" style={{ backgroundColor: 'var(--color-bg-elevated)', borderColor: 'var(--color-border-elevated)' }}>
          <DialogHeader>
            <DialogTitle className="dashboard-title-font" style={{ color: 'var(--color-text-primary)' }}>Edit holding — {editRow?.symbol}</DialogTitle>
          </DialogHeader>
          <div className="grid gap-3 py-2" onKeyDown={(e) => { if (e.key === 'Enter') { e.preventDefault(); onEditSubmit?.(); } }}>
            <div>
              <label className="text-xs text-gray-400 block mb-1">Quantity *</label>
              <Input
                type="number"
                min="0"
                step="any"
                placeholder="e.g. 10.5"
                value={editForm.quantity ?? ''}
                onChange={(e) => onEditFormChange?.({ ...editForm, quantity: e.target.value })}
                className="text-white placeholder:text-gray-500 border"
                style={{ backgroundColor: 'var(--color-bg-card)', borderColor: 'var(--color-border-default)' }}
              />
            </div>
            <div>
              <label className="text-xs text-gray-400 block mb-1">Average Cost *</label>
              <Input
                type="number"
                min="0"
                step="any"
                placeholder="e.g. 175.50"
                value={editForm.averageCost ?? ''}
                onChange={(e) => onEditFormChange?.({ ...editForm, averageCost: e.target.value })}
                className="text-white placeholder:text-gray-500 border"
                style={{ backgroundColor: 'var(--color-bg-card)', borderColor: 'var(--color-border-default)' }}
              />
            </div>
            <div>
              <label className="text-xs text-gray-400 block mb-1">Notes</label>
              <Input
                placeholder="Optional"
                value={editForm.notes ?? ''}
                onChange={(e) => onEditFormChange?.({ ...editForm, notes: e.target.value })}
                className="text-white placeholder:text-gray-500 border"
                style={{ backgroundColor: 'var(--color-bg-card)', borderColor: 'var(--color-border-default)' }}
              />
            </div>
          </div>
          <div className="flex justify-end gap-2 pt-2">
            <button type="button" onClick={onEditClose} className="px-3 py-1.5 rounded text-sm border hover:bg-white/10" style={{ color: 'var(--color-text-primary)', borderColor: 'var(--color-border-default)' }}>
              Cancel
            </button>
            <button type="button" onClick={onEditSubmit} className="px-3 py-1.5 rounded text-sm font-medium hover:opacity-90" style={{ backgroundColor: 'var(--color-accent-primary)', color: 'var(--color-text-on-accent)' }}>
              Save
            </button>
          </div>
        </DialogContent>
      </Dialog>

      <CardContent className="px-2 pb-6 pt-0 flex-1 min-h-0">
        <ScrollArea className="h-full">
          <table className="w-full text-sm" style={{ tableLayout: 'fixed' }}>
            <thead>
              <tr style={{ borderBottom: '1px solid var(--color-border-subtle)' }}>
                <th className="text-left py-2 px-2 font-normal text-xs" style={{ color: 'var(--color-text-secondary)', width: '26%' }}>Symbol</th>
                <th className="text-left py-2 px-2 font-normal text-xs" style={{ color: 'var(--color-text-secondary)', width: '24%' }}>Quantity</th>
                <th className="text-left py-2 px-2 font-normal text-xs" style={{ color: 'var(--color-text-secondary)', width: '24%' }}>Current Price</th>
                <th className="text-left py-2 px-2 font-normal text-xs" style={{ color: 'var(--color-text-secondary)', width: '24%' }}>Unrealized P/L %</th>
                {hasRealHoldings ? <th className="w-8" style={{ width: '32px' }} /> : null}
              </tr>
            </thead>
            <tbody>
              {loading
                ? Array.from({ length: 5 }).map((_, i) => (
                    <tr key={i} style={{ borderBottom: '1px solid var(--color-border-subtle)' }}>
                      <td colSpan={hasRealHoldings ? 5 : 4} className="py-2.5 px-2">
                        <div className="h-4 w-3/4 rounded bg-white/10 animate-pulse" />
                      </td>
                    </tr>
                  ))
                : rows.map((row) => (
                    <tr key={row.holding_id ?? row.symbol} className="transition-colors" style={{ borderBottom: '1px solid var(--color-border-subtle)' }}>
                      <td className="py-2.5 px-2 font-normal" style={{ color: 'var(--color-text-primary)' }}>{row.symbol}</td>
                      <td className="py-2.5 px-2 font-normal tabular-nums" style={{ color: 'var(--color-text-primary)' }}>
                        {row.quantity != null ? Number(row.quantity).toLocaleString(undefined, { maximumFractionDigits: 4 }) : '—'}
                      </td>
                      <td className="py-2.5 px-2 font-normal tabular-nums" style={{ color: 'var(--color-text-primary)' }}>
                        {Number(row.price).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                      </td>
                      <td className="py-2.5 px-2 font-normal tabular-nums" style={{ color: row.unrealizedPlPercent != null ? (row.isPositive ? 'var(--color-profit)' : 'var(--color-loss)') : 'var(--color-text-primary)' }}>
                        {row.unrealizedPlPercent != null ? (row.isPositive ? '+' : '') + Number(row.unrealizedPlPercent).toFixed(2) + '%' : '—'}
                      </td>
                      {hasRealHoldings && row.holding_id ? (
                        <td className="py-2.5 px-2 relative">
                          <div className="relative inline-block">
                            <button
                              type="button"
                              onClick={(e) => { e.stopPropagation(); setMenuOpenId((id) => (id === row.holding_id ? null : row.holding_id)); }}
                              className="p-1 rounded hover:opacity-80"
                              style={{ color: 'var(--color-text-secondary)' }}
                              aria-label="More options"
                            >
                              <MoreVertical className="h-4 w-4" />
                            </button>
                            {menuOpenId === row.holding_id && (
                              <>
                                <div className="fixed inset-0 z-40" aria-hidden onClick={() => setMenuOpenId(null)} />
                                <div className="absolute right-0 top-full z-50 mt-0.5 min-w-[120px] rounded border py-1 shadow-lg" style={{ backgroundColor: 'var(--color-bg-elevated)', borderColor: 'var(--color-border-elevated)' }}>
                                  <button type="button" onClick={(e) => { e.stopPropagation(); handleEdit(row); }} className="flex w-full items-center gap-2 px-3 py-1.5 text-left text-sm hover:bg-white/10" style={{ color: 'var(--color-text-primary)' }}>
                                    <Pencil className="h-3.5 w-3.5" style={{ color: 'var(--color-text-secondary)' }} /> Edit
                                  </button>
                                  <button type="button" onClick={(e) => { e.stopPropagation(); handleDelete(String(row.holding_id)); }} className="flex w-full items-center gap-2 px-3 py-1.5 text-left text-sm hover:bg-white/10" style={{ color: 'var(--color-text-primary)' }}>
                                    <Trash2 className="h-3.5 w-3.5" style={{ color: 'var(--color-text-secondary)' }} /> Delete
                                  </button>
                                </div>
                              </>
                            )}
                          </div>
                        </td>
                      ) : hasRealHoldings ? <td className="py-2.5 px-2" /> : null}
                    </tr>
                  ))}
            </tbody>
          </table>
        </ScrollArea>
      </CardContent>
    </Card>
  );
}

export default PortfolioCard;
