import React, { useState, useEffect } from 'react';
import { ArrowLeft, Search } from 'lucide-react';
import { Dialog, DialogContent, DialogHeader, DialogTitle } from '../../../components/ui/dialog';
import { Input } from '../../../components/ui/input';
import { ScrollArea } from '../../../components/ui/scroll-area';
import { searchStocks } from '../utils/api';

/**
 * Two-page dialog for adding portfolio holdings:
 * Page 1: Search for stocks by keyword
 * Page 2: Fill in quantity, average cost, account name, and notes
 */
function AddPortfolioHoldingDialog({
  open = false,
  onClose,
  onAdd,
}) {
  const [page, setPage] = useState(1); // 1 = search, 2 = details
  const [searchQuery, setSearchQuery] = useState('');
  const [searchResults, setSearchResults] = useState([]);
  const [searchLoading, setSearchLoading] = useState(false);
  const [selectedStock, setSelectedStock] = useState(null);
  
  // Form fields for page 2
  const [quantity, setQuantity] = useState('');
  const [averageCost, setAverageCost] = useState('');
  const [accountName, setAccountName] = useState('');
  const [notes, setNotes] = useState('');

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
        // Use maximum limit of 100 to show more search results
        const result = await searchStocks(query, 100);
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

  // Reset state when dialog closes
  useEffect(() => {
    if (!open) {
      setPage(1);
      setSearchQuery('');
      setSearchResults([]);
      setSelectedStock(null);
      setQuantity('');
      setAverageCost('');
      setAccountName('');
      setNotes('');
    }
  }, [open]);

  const handleStockSelect = (stock) => {
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
      alert('Please enter quantity.');
      return;
    }
    if (!averageCost.trim()) {
      alert('Please enter average cost.');
      return;
    }

    const quantityNum = parseFloat(quantity);
    const averageCostNum = parseFloat(averageCost);

    if (isNaN(quantityNum) || quantityNum <= 0) {
      alert('Please enter a valid quantity greater than 0.');
      return;
    }

    if (isNaN(averageCostNum) || averageCostNum <= 0) {
      alert('Please enter a valid average cost greater than 0.');
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
      currency: selectedStock.currency || 'USD',
      account_name: accountName.trim() || undefined,
      notes: notes.trim() || undefined,
      first_purchased_at: new Date().toISOString(),
    };

    onAdd(payload);
  };

  return (
    <Dialog open={open} onOpenChange={(open) => !open && onClose?.()}>
      <DialogContent className="sm:max-w-md text-white border" style={{ backgroundColor: 'var(--color-bg-elevated)', borderColor: 'var(--color-border-elevated)' }}>
        {page === 1 ? (
          <>
            <DialogHeader>
              <DialogTitle className="dashboard-title-font" style={{ color: 'var(--color-text-primary)' }}>
                Add Portfolio Holding
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
                  Add Portfolio Holding
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
                </div>

                {/* Quantity Input */}
                <div>
                  <label className="block text-xs mb-1" style={{ color: 'var(--color-text-secondary)' }}>
                    Quantity <span className="text-red-400">*</span>
                  </label>
                  <Input
                    type="number"
                    min="0"
                    step="any"
                    placeholder="e.g. 10.5"
                    value={quantity}
                    onChange={(e) => setQuantity(e.target.value)}
                    className="text-white placeholder:text-gray-500 border"
                    style={{ backgroundColor: 'var(--color-bg-card)', borderColor: 'var(--color-border-default)' }}
                  />
                </div>

                {/* Average Cost Input */}
                <div>
                  <label className="block text-xs mb-1" style={{ color: 'var(--color-text-secondary)' }}>
                    Average Cost Per Share <span className="text-red-400">*</span>
                  </label>
                  <Input
                    type="number"
                    min="0"
                    step="any"
                    placeholder="e.g. 175.50"
                    value={averageCost}
                    onChange={(e) => setAverageCost(e.target.value)}
                    className="text-white placeholder:text-gray-500 border"
                    style={{ backgroundColor: 'var(--color-bg-card)', borderColor: 'var(--color-border-default)' }}
                  />
                </div>

                {/* Account Name Input */}
                <div>
                  <label className="block text-xs mb-1" style={{ color: 'var(--color-text-secondary)' }}>
                    Account Name
                  </label>
                  <Input
                    placeholder="e.g. Robinhood"
                    value={accountName}
                    onChange={(e) => setAccountName(e.target.value)}
                    className="text-white placeholder:text-gray-500 border"
                    style={{ backgroundColor: 'var(--color-bg-card)', borderColor: 'var(--color-border-default)' }}
                  />
                </div>

                {/* Notes Input */}
                <div>
                  <label className="block text-xs mb-1" style={{ color: 'var(--color-text-secondary)' }}>
                    Notes
                  </label>
                  <Input
                    placeholder="Optional"
                    value={notes}
                    onChange={(e) => setNotes(e.target.value)}
                    className="text-white placeholder:text-gray-500 border"
                    style={{ backgroundColor: 'var(--color-bg-card)', borderColor: 'var(--color-border-default)' }}
                  />
                </div>

                {/* Add Button */}
                <button
                  type="button"
                  onClick={handleAdd}
                  className="w-full px-4 py-2 rounded font-medium hover:opacity-90"
                  style={{ backgroundColor: 'var(--color-accent-primary)', color: 'var(--color-text-on-accent)' }}
                >
                  Add to Portfolio
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
