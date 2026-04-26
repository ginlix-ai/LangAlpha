import React, { useState, useEffect } from 'react';
import { useTranslation } from 'react-i18next';
import i18n from '@/i18n';
import { ArrowLeft, Search } from 'lucide-react';
import { Dialog, DialogContent, DialogHeader, DialogTitle } from '../../../components/ui/dialog';
import { Input } from '../../../components/ui/input';
import { ScrollArea } from '../../../components/ui/scroll-area';
import { getStockPrices } from '../utils/api';
import { searchStocks } from '@/lib/marketUtils';

interface StockResult {
  symbol: string;
  name?: string;
  exchangeShortName?: string;
  stockExchange?: string;
}

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
  onAdd: (itemData: WatchlistItemData, watchlistId?: string) => void;
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
  const [page, setPage] = useState<1 | 2>(1); // 1 = search, 2 = details
  const [searchQuery, setSearchQuery] = useState('');
  const [searchResults, setSearchResults] = useState<StockResult[]>([]);
  const [searchLoading, setSearchLoading] = useState(false);
  const [selectedStock, setSelectedStock] = useState<StockResult | null>(null);
  const [currentPrice, setCurrentPrice] = useState<number | null>(null);
  const [priceLoading, setPriceLoading] = useState(false);

  // Form fields for page 2
  const [notes, setNotes] = useState('');
  const [priceAbove, setPriceAbove] = useState('');
  const [priceBelow, setPriceBelow] = useState('');

  // Debounced search
  useEffect(() => {
    if (!open || page !== 1) {
      setSearchResults([]);
      return;
    }

    const query = searchQuery.trim();
    if (!query || query.length < 1) {
      setSearchResults([]);
      return;
    }

    const timeoutId = setTimeout(async () => {
      setSearchLoading(true);
      try {
        const result = await searchStocks(query, 50);
        setSearchResults((result.results || []) as StockResult[]);
      } catch (error) {
        console.error('Search failed:', error);
        setSearchResults([]);
      } finally {
        setSearchLoading(false);
      }
    }, 300); // 300ms debounce

    return () => clearTimeout(timeoutId);
  }, [searchQuery, open, page]);

  // Fetch current price when stock is selected
  useEffect(() => {
    if (page === 2 && selectedStock?.symbol) {
      setPriceLoading(true);
      getStockPrices([selectedStock.symbol])
        .then((prices) => {
          const priceData = prices?.[0];
          if (priceData) {
            setCurrentPrice(priceData.price);
          } else {
            setCurrentPrice(null);
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
      setSearchResults([]);
      setSelectedStock(null);
      setCurrentPrice(null);
      setNotes('');
      setPriceAbove('');
      setPriceBelow('');
    }
  }, [open]);

  const handleStockSelect = (stock: StockResult) => {
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
    if (!selectedStock) return;

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

    onAdd(itemData, watchlistId);
  };

  const exchange = selectedStock?.exchangeShortName || selectedStock?.stockExchange || '';

  return (
    <Dialog open={open} onOpenChange={(open) => !open && onClose?.()}>
      <DialogContent className="sm:max-w-md border max-sm:min-h-[60dvh]" style={{ backgroundColor: 'var(--color-bg-elevated)', borderColor: 'var(--color-border-elevated)' }}>
        {page === 1 ? (
          <>
            <DialogHeader className="text-left">
              <DialogTitle className="title-font" style={{ color: 'var(--color-text-primary)' }}>
                {t('dashboard.addWatchlistDialog.titleSearch')}
              </DialogTitle>
            </DialogHeader>
            <div className="pt-1">
              <div className="relative">
                <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-4 w-4" style={{ color: 'var(--color-text-secondary)' }} />
                <Input
                  placeholder={t('dashboard.addWatchlistDialog.searchPlaceholder')}
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  className="pl-10 border"
                  style={{ backgroundColor: 'var(--color-bg-card)', borderColor: 'var(--color-border-default)', color: 'var(--color-text-primary)' }}
                  autoFocus
                />
              </div>
              <ScrollArea className="mt-3 max-h-[50dvh] sm:max-h-[400px]">
                {searchLoading ? (
                  <div className="py-8 text-center text-sm" style={{ color: 'var(--color-text-secondary)' }}>
                    {t('dashboard.addWatchlistDialog.searching')}
                  </div>
                ) : searchResults.length === 0 && searchQuery.trim().length >= 1 ? (
                  <div className="py-8 text-center text-sm" style={{ color: 'var(--color-text-secondary)' }}>
                    {t('dashboard.addWatchlistDialog.noResults')}
                  </div>
                ) : searchResults.length === 0 ? (
                  <div className="py-8 text-center text-sm" style={{ color: 'var(--color-text-secondary)' }}>
                    {t('dashboard.addWatchlistDialog.searchHint')}
                  </div>
                ) : (
                  <div className="divide-y" style={{ borderColor: 'var(--color-border-muted)' }}>
                    {searchResults.map((stock, index) => (
                      <button
                        key={`${stock.symbol}-${index}`}
                        type="button"
                        onClick={() => handleStockSelect(stock)}
                        className="w-full text-left px-2 py-2.5 transition-colors flex items-center gap-3"
                        style={{ color: 'var(--color-text-primary)' }}
                        onMouseEnter={(e) => { e.currentTarget.style.backgroundColor = 'var(--color-bg-hover)'; }}
                        onMouseLeave={(e) => { e.currentTarget.style.backgroundColor = 'transparent'; }}
                      >
                        <span
                          className="shrink-0 w-12 text-center text-xs font-bold py-1 rounded"
                          style={{ backgroundColor: 'var(--color-bg-surface, var(--color-bg-hover))', color: 'var(--color-text-primary)' }}
                        >
                          {stock.symbol}
                        </span>
                        <span className="text-sm truncate" style={{ color: 'var(--color-text-secondary)' }}>
                          {stock.name}
                        </span>
                      </button>
                    ))}
                  </div>
                )}
              </ScrollArea>
            </div>
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
                {/* Stock info card */}
                <div
                  className="rounded-xl p-4 border"
                  style={{ backgroundColor: 'var(--color-bg-card)', borderColor: 'var(--color-border-muted)' }}
                >
                  <div className="flex items-start justify-between gap-3">
                    <div className="min-w-0">
                      <div className="flex items-center gap-2">
                        <span className="text-lg font-bold" style={{ color: 'var(--color-text-primary)' }}>
                          {selectedStock.symbol}
                        </span>
                        {exchange && (
                          <span
                            className="text-[10px] font-medium px-1.5 py-0.5 rounded"
                            style={{ backgroundColor: 'var(--color-bg-surface, var(--color-bg-hover))', color: 'var(--color-text-secondary)' }}
                          >
                            {exchange}
                          </span>
                        )}
                      </div>
                      <div className="text-xs mt-1 truncate" style={{ color: 'var(--color-text-secondary)' }}>
                        {selectedStock.name}
                      </div>
                    </div>
                    <div className="text-right shrink-0">
                      {priceLoading ? (
                        <div className="text-sm" style={{ color: 'var(--color-text-secondary)' }}>...</div>
                      ) : currentPrice !== null ? (
                        <div className="text-lg font-bold tabular-nums" style={{ color: 'var(--color-text-primary)' }}>
                          ${currentPrice.toLocaleString(i18n.language, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                        </div>
                      ) : (
                        <div className="text-sm" style={{ color: 'var(--color-text-secondary)' }}>{t('dashboard.addWatchlistDialog.priceNA')}</div>
                      )}
                      <div className="text-[10px] mt-0.5" style={{ color: 'var(--color-text-tertiary)' }}>{t('dashboard.addWatchlistDialog.currentPrice')}</div>
                    </div>
                  </div>
                </div>

                {/* Notes Input */}
                <div>
                  <label className="block text-xs font-medium mb-1.5" style={{ color: 'var(--color-text-secondary)' }}>
                    {t('dashboard.addWatchlistDialog.notesLabel')}
                  </label>
                  <Input
                    placeholder={t('dashboard.addWatchlistDialog.notesPlaceholder')}
                    value={notes}
                    onChange={(e) => setNotes(e.target.value)}
                    className="border"
                    style={{ backgroundColor: 'var(--color-bg-card)', borderColor: 'var(--color-border-default)', color: 'var(--color-text-primary)' }}
                  />
                </div>

                {/* Alert Settings */}
                <div>
                  <div className="text-xs font-medium mb-1.5" style={{ color: 'var(--color-text-secondary)' }}>
                    {t('dashboard.addWatchlistDialog.priceAlertsLabel')}
                  </div>
                  <div className="grid grid-cols-2 gap-3">
                    <div>
                      <label className="block text-[11px] mb-1" style={{ color: 'var(--color-text-tertiary)' }}>
                        {t('dashboard.addWatchlistDialog.priceAboveLabel')}
                      </label>
                      <Input
                        type="number"
                        step="0.01"
                        placeholder="200.00"
                        value={priceAbove}
                        onChange={(e) => setPriceAbove(e.target.value)}
                        className="border"
                        style={{ backgroundColor: 'var(--color-bg-card)', borderColor: 'var(--color-border-default)', color: 'var(--color-text-primary)' }}
                      />
                    </div>
                    <div>
                      <label className="block text-[11px] mb-1" style={{ color: 'var(--color-text-tertiary)' }}>
                        {t('dashboard.addWatchlistDialog.priceBelowLabel')}
                      </label>
                      <Input
                        type="number"
                        step="0.01"
                        placeholder="150.00"
                        value={priceBelow}
                        onChange={(e) => setPriceBelow(e.target.value)}
                        className="border"
                        style={{ backgroundColor: 'var(--color-bg-card)', borderColor: 'var(--color-border-default)', color: 'var(--color-text-primary)' }}
                      />
                    </div>
                  </div>
                </div>

                {/* Add Button */}
                <button
                  type="button"
                  onClick={handleAdd}
                  className="w-full px-4 py-3 rounded-xl text-sm font-semibold hover:opacity-90 transition-opacity"
                  style={{ backgroundColor: 'var(--color-accent-primary)', color: 'var(--color-text-on-accent)' }}
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
