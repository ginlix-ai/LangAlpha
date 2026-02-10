import React, { useState, useEffect, useCallback } from 'react';
import { ArrowLeft, Search } from 'lucide-react';
import { Dialog, DialogContent, DialogHeader, DialogTitle } from '../../../components/ui/dialog';
import { Input } from '../../../components/ui/input';
import { ScrollArea } from '../../../components/ui/scroll-area';
import { searchStocks, getStockPrices } from '../utils/api';

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
}) {
  const [page, setPage] = useState(1); // 1 = search, 2 = details
  const [searchQuery, setSearchQuery] = useState('');
  const [searchResults, setSearchResults] = useState([]);
  const [searchLoading, setSearchLoading] = useState(false);
  const [selectedStock, setSelectedStock] = useState(null);
  const [currentPrice, setCurrentPrice] = useState(null);
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
        setSearchResults(result.results || []);
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

  const handleStockSelect = (stock) => {
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

    // Validate required fields
    if (!notes.trim()) {
      alert('Please enter notes for this stock.');
      return;
    }
    if (!priceAbove.trim() || !priceBelow.trim()) {
      alert('Please enter both price above and price below values.');
      return;
    }

    const priceAboveNum = parseFloat(priceAbove);
    const priceBelowNum = parseFloat(priceBelow);

    if (isNaN(priceAboveNum) || isNaN(priceBelowNum)) {
      alert('Please enter valid numbers for price alerts.');
      return;
    }

    if (priceAboveNum <= priceBelowNum) {
      alert('Price above must be greater than price below.');
      return;
    }

    const itemData = {
      symbol: selectedStock.symbol,
      instrument_type: 'stock',
      exchange: selectedStock.exchangeShortName || '',
      name: selectedStock.name || '',
      notes: notes.trim(),
      alert_settings: {
        price_above: priceAboveNum,
        price_below: priceBelowNum,
      },
    };

    onAdd(itemData, watchlistId);
  };

  return (
    <Dialog open={open} onOpenChange={(open) => !open && onClose?.()}>
      <DialogContent className="sm:max-w-md text-white border" style={{ backgroundColor: 'var(--color-bg-elevated)', borderColor: 'var(--color-border-elevated)' }}>
        {page === 1 ? (
          <>
            <DialogHeader>
              <DialogTitle className="dashboard-title-font" style={{ color: 'var(--color-text-primary)' }}>
                Add Watchlist Item
              </DialogTitle>
            </DialogHeader>
            <div className="pt-2">
              <div className="relative">
                <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-4 w-4" style={{ color: 'var(--color-text-secondary)' }} />
                <Input
                  placeholder="Search by symbol or company name..."
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  className="pl-10 text-white placeholder:text-gray-500 border"
                  style={{ backgroundColor: 'var(--color-bg-card)', borderColor: 'var(--color-border-default)' }}
                  autoFocus
                />
              </div>
              <ScrollArea className="mt-4 max-h-[400px]">
                {searchLoading ? (
                  <div className="py-8 text-center text-sm" style={{ color: 'var(--color-text-secondary)' }}>
                    Searching...
                  </div>
                ) : searchResults.length === 0 && searchQuery.trim().length >= 1 ? (
                  <div className="py-8 text-center text-sm" style={{ color: 'var(--color-text-secondary)' }}>
                    No results found
                  </div>
                ) : searchResults.length === 0 ? (
                  <div className="py-8 text-center text-sm" style={{ color: 'var(--color-text-secondary)' }}>
                    Type to search for stocks...
                  </div>
                ) : (
                  <div className="space-y-1">
                    {searchResults.map((stock, index) => (
                      <button
                        key={`${stock.symbol}-${index}`}
                        type="button"
                        onClick={() => handleStockSelect(stock)}
                        className="w-full text-left px-3 py-2 rounded hover:bg-white/10 transition-colors"
                        style={{ color: 'var(--color-text-primary)' }}
                      >
                        <div className="text-sm font-medium">{stock.name}</div>
                      </button>
                    ))}
                  </div>
                )}
              </ScrollArea>
            </div>
          </>
        ) : (
          <>
            <DialogHeader>
              <div className="flex items-center gap-2">
                <button
                  type="button"
                  onClick={handleBack}
                  className="p-1 rounded hover:bg-white/10"
                  style={{ color: 'var(--color-text-primary)' }}
                  aria-label="Back"
                >
                  <ArrowLeft className="h-4 w-4" />
                </button>
                <DialogTitle className="dashboard-title-font" style={{ color: 'var(--color-text-primary)' }}>
                  Add Watchlist Item
                </DialogTitle>
              </div>
            </DialogHeader>
            {selectedStock && (
              <div className="pt-2 space-y-4">
                {/* Stock Information */}
                <div className="space-y-2">
                  <div>
                    <div className="text-xs mb-1" style={{ color: 'var(--color-text-secondary)' }}>Symbol</div>
                    <div className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
                      {selectedStock.symbol}
                    </div>
                  </div>
                  <div>
                    <div className="text-xs mb-1" style={{ color: 'var(--color-text-secondary)' }}>Company Name</div>
                    <div className="text-sm" style={{ color: 'var(--color-text-primary)' }}>
                      {selectedStock.name}
                    </div>
                  </div>
                  <div>
                    <div className="text-xs mb-1" style={{ color: 'var(--color-text-secondary)' }}>Exchange</div>
                    <div className="text-sm" style={{ color: 'var(--color-text-primary)' }}>
                      {selectedStock.exchangeShortName || selectedStock.stockExchange || 'N/A'}
                    </div>
                  </div>
                  <div>
                    <div className="text-xs mb-1" style={{ color: 'var(--color-text-secondary)' }}>Current Price</div>
                    {priceLoading ? (
                      <div className="text-sm" style={{ color: 'var(--color-text-secondary)' }}>Loading...</div>
                    ) : currentPrice !== null ? (
                      <div className="text-sm font-medium tabular-nums" style={{ color: 'var(--color-text-primary)' }}>
                        ${currentPrice.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                      </div>
                    ) : (
                      <div className="text-sm" style={{ color: 'var(--color-text-secondary)' }}>N/A</div>
                    )}
                  </div>
                </div>

                {/* Notes Input */}
                <div>
                  <label className="block text-xs mb-1" style={{ color: 'var(--color-text-secondary)' }}>
                    Notes <span className="text-red-400">*</span>
                  </label>
                  <Input
                    placeholder="Enter notes about this stock..."
                    value={notes}
                    onChange={(e) => setNotes(e.target.value)}
                    className="text-white placeholder:text-gray-500 border"
                    style={{ backgroundColor: 'var(--color-bg-card)', borderColor: 'var(--color-border-default)' }}
                  />
                </div>

                {/* Alert Settings */}
                <div className="space-y-2">
                  <div className="text-xs" style={{ color: 'var(--color-text-secondary)' }}>
                    Alert Settings <span className="text-red-400">*</span>
                  </div>
                  <div className="grid grid-cols-2 gap-2">
                    <div>
                      <label className="block text-xs mb-1" style={{ color: 'var(--color-text-secondary)' }}>
                        Price Above
                      </label>
                      <Input
                        type="number"
                        step="0.01"
                        placeholder="200.00"
                        value={priceAbove}
                        onChange={(e) => setPriceAbove(e.target.value)}
                        className="text-white placeholder:text-gray-500 border"
                        style={{ backgroundColor: 'var(--color-bg-card)', borderColor: 'var(--color-border-default)' }}
                      />
                    </div>
                    <div>
                      <label className="block text-xs mb-1" style={{ color: 'var(--color-text-secondary)' }}>
                        Price Below
                      </label>
                      <Input
                        type="number"
                        step="0.01"
                        placeholder="150.00"
                        value={priceBelow}
                        onChange={(e) => setPriceBelow(e.target.value)}
                        className="text-white placeholder:text-gray-500 border"
                        style={{ backgroundColor: 'var(--color-bg-card)', borderColor: 'var(--color-border-default)' }}
                      />
                    </div>
                  </div>
                </div>

                {/* Add Button */}
                <button
                  type="button"
                  onClick={handleAdd}
                  className="w-full px-4 py-2 rounded font-medium hover:opacity-90"
                  style={{ backgroundColor: 'var(--color-accent-primary)', color: 'var(--color-text-on-accent)' }}
                >
                  Add to Watchlist
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
