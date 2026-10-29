import React, { useState, useEffect } from 'react';
import { useTranslation } from 'react-i18next';
import { ArrowLeft } from 'lucide-react';
import { Dialog, DialogContent, DialogHeader, DialogTitle } from '../../../components/ui/dialog';
import { Input } from '../../../components/ui/input';
import { FormCard } from './FormCard';
import { SymbolSearchCard } from './SymbolSearchCard';
import type { StockSearchHit } from '@/lib/marketUtils';
import { useSymbolSearch } from '@/hooks/useSymbolSearch';

interface PortfolioHoldingPayload {
  symbol: string;
  instrument_type: string;
  exchange: string;
  name: string;
  quantity: string;
  average_cost: string;
  currency?: string;  // absent: the server takes the listing's
  account_name?: string;
  notes?: string;
  first_purchased_at: string;
}

interface AddPortfolioHoldingDialogProps {
  open?: boolean;
  onClose?: () => void;
  onAdd: (payload: PortfolioHoldingPayload) => void;
}

/**
 * Two-page dialog for adding portfolio holdings:
 * Page 1: Search for stocks by keyword
 * Page 2: Fill in quantity, average cost, account name, and notes
 */
function AddPortfolioHoldingDialog({
  open = false,
  onClose,
  onAdd,
}: AddPortfolioHoldingDialogProps) {
  const { t } = useTranslation();
  const [page, setPage] = useState<1 | 2>(1); // 1 = search, 2 = details
  const [searchQuery, setSearchQuery] = useState('');
  // The API caps a page at 100; asking for it all lets a broad name resolve.
  const { hits: searchResults, loading: searchLoading } = useSymbolSearch(searchQuery, 100, { enabled: open && page === 1 });
  const [selectedStock, setSelectedStock] = useState<StockSearchHit | null>(null);

  // Form fields for page 2
  const [quantity, setQuantity] = useState('');
  const [averageCost, setAverageCost] = useState('');
  const [accountName, setAccountName] = useState('');
  const [notes, setNotes] = useState('');

  // Reset state when dialog closes
  useEffect(() => {
    if (!open) {
      setPage(1);
      setSearchQuery('');
      setSelectedStock(null);
      setQuantity('');
      setAverageCost('');
      setAccountName('');
      setNotes('');
    }
  }, [open]);

  const handleStockSelect = (stock: StockSearchHit) => {
    setSelectedStock(stock);
    setPage(2);
  };

  const handleBack = () => {
    setPage(1);
    setSelectedStock(null);
    setQuantity('');
    setAverageCost('');
    setAccountName('');
    setNotes('');
  };

  const handleAdd = () => {
    if (!selectedStock) return;

    // Validate required fields
    if (!quantity.trim()) {
      alert(t('dashboard.addPortfolioDialog.alertQuantityMissing'));
      return;
    }
    if (!averageCost.trim()) {
      alert(t('dashboard.addPortfolioDialog.alertAverageCostMissing'));
      return;
    }

    const quantityNum = parseFloat(quantity);
    const averageCostNum = parseFloat(averageCost);

    if (isNaN(quantityNum) || quantityNum <= 0) {
      alert(t('dashboard.addPortfolioDialog.alertQuantityInvalid'));
      return;
    }

    if (isNaN(averageCostNum) || averageCostNum <= 0) {
      alert(t('dashboard.addPortfolioDialog.alertAverageCostInvalid'));
      return;
    }

    // Build the payload according to API specification
    const payload = {
      symbol: selectedStock.symbol,
      instrument_type: 'stock',
      exchange: selectedStock.exchangeShortName || '',
      name: selectedStock.name || '',
      quantity: String(quantityNum),
      average_cost: String(averageCostNum),
      currency: selectedStock.currency || undefined,
      account_name: accountName.trim() || undefined,
      notes: notes.trim() || undefined,
      first_purchased_at: new Date().toISOString(),
    };

    onAdd(payload);
  };

  const exchange = selectedStock?.exchangeShortName || selectedStock?.stockExchange || '';

  return (
    <Dialog open={open} onOpenChange={(open) => !open && onClose?.()}>
      <DialogContent className="dash-form sm:max-w-md max-sm:min-h-[60dvh]">
        {page === 1 ? (
          <>
            <DialogHeader className="text-left">
              <DialogTitle className="title-font" style={{ color: 'var(--color-text-primary)' }}>
                {t('dashboard.addPortfolioDialog.titleSearch')}
              </DialogTitle>
            </DialogHeader>
            <SymbolSearchCard
              query={searchQuery}
              onQueryChange={setSearchQuery}
              hits={searchResults}
              loading={searchLoading}
              onSelect={handleStockSelect}
              copy={{
                searching: t('dashboard.addPortfolioDialog.searching'),
                noResults: t('dashboard.addPortfolioDialog.noResults'),
                searchHint: t('dashboard.addPortfolioDialog.searchHint'),
                searchPlaceholder: t('dashboard.addPortfolioDialog.searchPlaceholder'),
              }}
            />
          </>
        ) : (
          <>
            <DialogHeader className="text-left">
              <div className="flex items-center gap-2">
                <button
                  type="button"
                  onClick={handleBack}
                  className="p-1.5 -ml-1 rounded-lg transition-colors"
                  style={{ color: 'var(--color-text-secondary)' }}
                  onMouseEnter={(e) => { e.currentTarget.style.backgroundColor = 'var(--color-bg-hover)'; }}
                  onMouseLeave={(e) => { e.currentTarget.style.backgroundColor = 'transparent'; }}
                  aria-label={t('dashboard.addPortfolioDialog.back')}
                >
                  <ArrowLeft className="h-4 w-4" />
                </button>
                <DialogTitle className="title-font" style={{ color: 'var(--color-text-primary)' }}>
                  {t('dashboard.addPortfolioDialog.titleDetails')}
                </DialogTitle>
              </div>
            </DialogHeader>
            {selectedStock && (
              <div className="space-y-5">
                <FormCard title={<span className="term-code">{selectedStock.symbol}</span>} sub={exchange}>
                  {selectedStock.name && <div className="term-word dash-form-name">{selectedStock.name}</div>}

                  <div className="grid grid-cols-2 gap-3">
                    <label className="block">
                      <span className="term-label dash-form-label">
                        {t('dashboard.addPortfolioDialog.quantityLabel')} <span className="dash-form-required">*</span>
                      </span>
                      <Input
                        type="number"
                        min="0"
                        step="any"
                        placeholder={t('dashboard.addPortfolioDialog.quantityPlaceholder')}
                        value={quantity}
                        onChange={(e) => setQuantity(e.target.value)}
                        className="dash-form-input dash-form-figure border"
                      />
                    </label>
                    <label className="block">
                      <span className="term-label dash-form-label">
                        {t('dashboard.addPortfolioDialog.averageCostLabel')} <span className="dash-form-required">*</span>
                      </span>
                      <Input
                        type="number"
                        min="0"
                        step="any"
                        placeholder={t('dashboard.addPortfolioDialog.averageCostPlaceholder')}
                        value={averageCost}
                        onChange={(e) => setAverageCost(e.target.value)}
                        className="dash-form-input dash-form-figure border"
                      />
                    </label>
                  </div>

                  <label className="block">
                    <span className="term-label dash-form-label">{t('dashboard.addPortfolioDialog.accountNameLabel')}</span>
                    <Input
                      placeholder={t('dashboard.addPortfolioDialog.accountNamePlaceholder')}
                      value={accountName}
                      onChange={(e) => setAccountName(e.target.value)}
                      className="dash-form-input border"
                    />
                  </label>

                  <label className="block">
                    <span className="term-label dash-form-label">{t('dashboard.addPortfolioDialog.notesLabel')}</span>
                    <Input
                      placeholder={t('dashboard.addPortfolioDialog.notesPlaceholder')}
                      value={notes}
                      onChange={(e) => setNotes(e.target.value)}
                      className="dash-form-input border"
                    />
                  </label>
                </FormCard>

                <button
                  type="button"
                  onClick={handleAdd}
                  className="w-full px-4 py-3 rounded-xl text-sm font-semibold hover:opacity-90 transition-opacity"
                  style={{ backgroundColor: 'var(--color-btn-primary-bg)', color: 'var(--color-btn-primary-text)' }}
                >
                  {t('dashboard.addPortfolioDialog.addButton')}
                </button>
              </div>
            )}
          </>
        )}
      </DialogContent>
    </Dialog>
  );
}

export default AddPortfolioHoldingDialog;
