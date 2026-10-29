import React, { useState, useEffect } from 'react';
import { useTranslation } from 'react-i18next';
import { ArrowLeft } from 'lucide-react';
import { Dialog, DialogContent, DialogHeader, DialogTitle } from '../../../components/ui/dialog';
import { Input } from '../../../components/ui/input';
import { TermKV } from '@/pages/ChatAgent/components/charts/TermCard';
import { getStockPrices } from '../utils/api';
import { FormCard } from './FormCard';
import { SymbolSearchCard } from './SymbolSearchCard';
import type { StockSearchHit } from '@/lib/marketUtils';
import { useSymbolSearch } from '@/hooks/useSymbolSearch';
import { useLocale } from '@/hooks/useLocale';
import { formatMoney, quoteCurrency } from '@/lib/bars';

interface WatchlistItemData {
  symbol: string;
  instrument_type: string;
  exchange: string;
  name: string;
  notes?: string;
  alert_settings?: {
    price_above?: number;
    price_below?: number;
  };
}

interface AddWatchlistItemDialogProps {
  open?: boolean;
  onClose?: () => void;
  onAdd: (itemData: WatchlistItemData, watchlistId?: string) => void | Promise<void>;
  watchlistId?: string;
}

/**
 * Two-page dialog for adding watchlist items:
 * Page 1: Search for stocks by keyword
 * Page 2: Review stock details and add notes/alert settings
 */
function AddWatchlistItemDialog({
  open = false,
  onClose,
  onAdd,
  watchlistId,
}: AddWatchlistItemDialogProps) {
  const { t } = useTranslation();
  const locale = useLocale();
  const [page, setPage] = useState<1 | 2>(1); // 1 = search, 2 = details
  const [searchQuery, setSearchQuery] = useState('');
  const { hits: searchResults, loading: searchLoading } = useSymbolSearch(searchQuery, 50, { enabled: open && page === 1 });
  const [selectedStock, setSelectedStock] = useState<StockSearchHit | null>(null);
  const [currentPrice, setCurrentPrice] = useState<number | null>(null);
  const [priceCurrency, setPriceCurrency] = useState<string | null>(null);
  const [priceLoading, setPriceLoading] = useState(false);
  const [submitting, setSubmitting] = useState(false);

  // Form fields for page 2
  const [notes, setNotes] = useState('');
  const [priceAbove, setPriceAbove] = useState('');
  const [priceBelow, setPriceBelow] = useState('');

  // Fetch current price when stock is selected
  useEffect(() => {
    if (page === 2 && selectedStock?.symbol) {
      setPriceLoading(true);
      getStockPrices([selectedStock.symbol])
        .then((prices) => {
          const priceData = prices?.[0];
          if (priceData && priceData.quoteAvailable !== false) {
            setCurrentPrice(priceData.price);
            setPriceCurrency(priceData.currency ?? null);
          } else {
            setCurrentPrice(null);
            setPriceCurrency(null);
          }
        })
        .catch((error) => {
          console.error('Failed to fetch price:', error);
          setCurrentPrice(null);
        })
        .finally(() => {
          setPriceLoading(false);
        });
    }
  }, [page, selectedStock]);

  // Reset state when dialog closes
  useEffect(() => {
    if (!open) {
      setPage(1);
      setSearchQuery('');
      setSelectedStock(null);
      setCurrentPrice(null);
      setNotes('');
      setPriceAbove('');
      setPriceBelow('');
    }
  }, [open]);

  const handleStockSelect = (stock: StockSearchHit) => {
    setSelectedStock(stock);
    setPage(2);
  };

  const handleBack = () => {
    setPage(1);
    setSelectedStock(null);
    setCurrentPrice(null);
    setNotes('');
    setPriceAbove('');
    setPriceBelow('');
  };

  const handleAdd = () => {
    if (!selectedStock || submitting) return;

    const priceAboveNum = priceAbove.trim() ? parseFloat(priceAbove) : null;
    const priceBelowNum = priceBelow.trim() ? parseFloat(priceBelow) : null;

    if (priceAboveNum != null && priceBelowNum != null && priceAboveNum <= priceBelowNum) {
      alert(t('dashboard.addWatchlistDialog.alertPriceAboveBelow'));
      return;
    }

    const itemData = {
      symbol: selectedStock.symbol,
      instrument_type: 'stock',
      exchange: selectedStock.exchangeShortName || '',
      name: selectedStock.name || '',
      notes: notes.trim() || undefined,
      alert_settings: priceAboveNum != null || priceBelowNum != null
        ? {
            ...(priceAboveNum != null && { price_above: priceAboveNum }),
            ...(priceBelowNum != null && { price_below: priceBelowNum }),
          }
        : undefined,
    };

    // The parent's handler is async and closes the dialog on success; hold
    // the button while it runs so a double-click cannot post the row twice.
    setSubmitting(true);
    Promise.resolve(onAdd(itemData, watchlistId)).finally(() => setSubmitting(false));
  };

  // Prefer the currency the quote was served in; the search result's listing
  // currency, then the ticker suffix, cover the wait and older payloads. An
  // index level prints bare.
  const cur = quoteCurrency(priceCurrency || selectedStock?.currency, selectedStock?.symbol);

  const exchange = selectedStock?.exchangeShortName || selectedStock?.stockExchange || '';
  const priceCell = priceLoading ? (
    <span className="term-t3">...</span>
  ) : currentPrice !== null ? (
    <span className="term-code term-strong">{formatMoney(currentPrice, cur, locale)}</span>
  ) : (
    <span className="term-t3">{t('dashboard.addWatchlistDialog.priceNA')}</span>
  );

  return (
    <Dialog open={open} onOpenChange={(open) => !open && onClose?.()}>
      <DialogContent className="dash-form sm:max-w-md max-sm:min-h-[60dvh]">
        {page === 1 ? (
          <>
            <DialogHeader className="text-left">
              <DialogTitle className="title-font" style={{ color: 'var(--color-text-primary)' }}>
                {t('dashboard.addWatchlistDialog.titleSearch')}
              </DialogTitle>
            </DialogHeader>
            <SymbolSearchCard
              query={searchQuery}
              onQueryChange={setSearchQuery}
              hits={searchResults}
              loading={searchLoading}
              onSelect={handleStockSelect}
              copy={{
                searching: t('dashboard.addWatchlistDialog.searching'),
                noResults: t('dashboard.addWatchlistDialog.noResults'),
                searchHint: t('dashboard.addWatchlistDialog.searchHint'),
                searchPlaceholder: t('dashboard.addWatchlistDialog.searchPlaceholder'),
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
                  aria-label={t('dashboard.addWatchlistDialog.back')}
                >
                  <ArrowLeft className="h-4 w-4" />
                </button>
                <DialogTitle className="title-font" style={{ color: 'var(--color-text-primary)' }}>
                  {t('dashboard.addWatchlistDialog.titleDetails')}
                </DialogTitle>
              </div>
            </DialogHeader>
            {selectedStock && (
              <div className="space-y-5">
                <FormCard title={<span className="term-code">{selectedStock.symbol}</span>} sub={exchange}>
                  <div>
                    {selectedStock.name && <div className="term-word dash-form-name">{selectedStock.name}</div>}
                    <TermKV cols={1} pairs={[[t('dashboard.addWatchlistDialog.currentPrice'), priceCell]]} />
                  </div>
                  <label className="block">
                    <span className="term-label dash-form-label">{t('dashboard.addWatchlistDialog.notesLabel')}</span>
                    <Input
                      placeholder={t('dashboard.addWatchlistDialog.notesPlaceholder')}
                      value={notes}
                      onChange={(e) => setNotes(e.target.value)}
                      className="dash-form-input border"
                    />
                  </label>
                </FormCard>

                <FormCard title={t('dashboard.addWatchlistDialog.priceAlertsLabel')}>
                  <div className="grid grid-cols-2 gap-3">
                    <label className="block">
                      <span className="term-label dash-form-label">{t('dashboard.addWatchlistDialog.priceAboveLabel')}</span>
                      <Input
                        type="number"
                        step="0.01"
                        placeholder="200.00"
                        value={priceAbove}
                        onChange={(e) => setPriceAbove(e.target.value)}
                        className="dash-form-input dash-form-figure border"
                      />
                    </label>
                    <label className="block">
                      <span className="term-label dash-form-label">{t('dashboard.addWatchlistDialog.priceBelowLabel')}</span>
                      <Input
                        type="number"
                        step="0.01"
                        placeholder="150.00"
                        value={priceBelow}
                        onChange={(e) => setPriceBelow(e.target.value)}
                        className="dash-form-input dash-form-figure border"
                      />
                    </label>
                  </div>
                </FormCard>

                <button
                  type="button"
                  onClick={handleAdd}
                  disabled={submitting}
                  className="w-full px-4 py-3 rounded-xl text-sm font-semibold hover:opacity-90 transition-opacity disabled:opacity-60"
                  style={{ backgroundColor: 'var(--color-btn-primary-bg)', color: 'var(--color-btn-primary-text)' }}
                >
                  {t('dashboard.addWatchlistDialog.addButton')}
                </button>
              </div>
            )}
          </>
        )}
      </DialogContent>
    </Dialog>
  );
}

export default AddWatchlistItemDialog;
