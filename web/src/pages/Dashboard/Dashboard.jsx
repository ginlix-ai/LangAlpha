import {
  DEFAULT_USER_ID,
  DEFAULT_WATCHLIST_NAMES,
  DEFAULT_WATCHLIST_SYMBOLS,
  INDEX_SYMBOLS,
  addPortfolioHolding,
  addWatchlistItem,
  deletePortfolioHolding,
  deleteWatchlistItem,
  fallbackIndex,
  getCurrentUser,
  getIndices,
  getPortfolio,
  getStockPrices,
  listWatchlists,
  listWatchlistItems,
  normalizeIndexSymbol,
  updatePortfolioHolding,
  getInfoFlowResults,
} from './utils/api';
import { useCallback, useEffect, useRef, useState } from 'react';
import { useToast } from '@/components/ui/use-toast';
import { getWorkspaces, createWorkspace } from '../ChatAgent/utils/api';
import { useNavigate } from 'react-router-dom';
import { findOrCreateDefaultWorkspace } from './utils/workspace';
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogDescription } from '../../components/ui/dialog';
import DashboardHeader from './components/DashboardHeader';
import ConfirmDialog from './components/ConfirmDialog';
import IndexMovementCard from './components/IndexMovementCard';
import PopularCard from './components/PopularCard';
import TopNewsCard from './components/TopNewsCard';
import TopResearchCard from './components/TopResearchCard';
import ChatInputCard from './components/ChatInputCard';
import WatchlistCard from './components/WatchlistCard';
import AddWatchlistItemDialog from './components/AddWatchlistItemDialog';
import AddPortfolioHoldingDialog from './components/AddPortfolioHoldingDialog';
import PortfolioCard from './components/PortfolioCard';
import './Dashboard.css';

const POPULAR_ITEMS = [
    { title: 'Comparison Report', description: 'A comprehensive analysis comparing industries.', duration: '20-30min', isHighlighted: true },
    { title: 'Comparison Report', description: 'A comprehensive analysis comparing industries.', duration: '20-30min', isHighlighted: false },
    { title: 'Comparison Report', description: 'A comprehensive analysis comparing industries.', duration: '20-30min', isHighlighted: false },
    { title: 'Comparison Report', description: 'A comprehensive analysis comparing industries.', duration: '20-30min', isHighlighted: false },
  ];

const NEWS_ITEMS = [
    { title: 'Federal Reserve Signals Potential Rate Cuts Amid Economic Uncertainty', time: '5 min ago', isHot: true },
    { title: 'Tech Stocks Rally as AI Companies Report Record Quarterly Earnings', time: '12 min ago', isHot: false },
    { title: 'Oil Prices Surge Following OPEC Production Cut Announcement', time: '18 min ago', isHot: true },
    { title: 'Cryptocurrency Market Volatility Increases as Regulatory News Emerges', time: '25 min ago', isHot: false },
    { title: 'Global Supply Chain Disruptions Impact Manufacturing Sector Performance', time: '1 hr ago', isHot: true },
    { title: 'Housing Market Shows Signs of Cooling as Mortgage Rates Climb', time: '2 hrs ago', isHot: false },
    { title: 'European Central Bank Maintains Current Interest Rate Policy', time: '3 hrs ago', isHot: false },
    { title: 'Renewable Energy Investments Reach All-Time High in Q4', time: '5 hrs ago', isHot: true },
  ];

const RESEARCH_ITEMS = [
    { title: 'Retail Sales Slump Takes Toll on Market, Stocks Dip', time: '10 min ago' },
    { title: 'Retail Sales Slump Takes Toll on Market, Stocks Dip', time: '10 min ago' },
    { title: 'Retail Sales Slump Takes Toll on Market, Stocks Dip', time: '10 min ago' },
    { title: 'Retail Sales Slump Takes Toll on Market, Stocks Dip', time: '10 min ago' },
  ];

// Module-level: user clicked Ignore on onboarding dialog this session
// Resets on page refresh (module reload)
let onboardingDismissedThisSession = false;

// Module-level caches (survive navigation, clear on page refresh)
let popularCache = null; // { items, hasMore, offset }
let newsCache = null;    // { items }
let researchCache = null; // { items }

function formatRelativeTime(timestamp) {
  if (!timestamp) return '';
  const now = new Date();
  const then = new Date(timestamp);
  const diffMs = now - then;
  const diffMin = Math.floor(diffMs / 60000);
  if (diffMin < 1) return 'just now';
  if (diffMin < 60) return `${diffMin} min ago`;
  const diffHr = Math.floor(diffMin / 60);
  if (diffHr < 24) return `${diffHr} hr${diffHr > 1 ? 's' : ''} ago`;
  const diffDay = Math.floor(diffHr / 24);
  return `${diffDay} day${diffDay > 1 ? 's' : ''} ago`;
}

function Dashboard() {
  const { toast } = useToast();
  const navigate = useNavigate();
  
  // Onboarding check state
  const [showOnboardingDialog, setShowOnboardingDialog] = useState(false);
  const [isCreatingWorkspace, setIsCreatingWorkspace] = useState(false);
  
  const [indices, setIndices] = useState(() =>
    INDEX_SYMBOLS.map((s) => fallbackIndex(normalizeIndexSymbol(s)))
  );
  const [indicesLoading, setIndicesLoading] = useState(true);

  const [popularItems, setPopularItems] = useState(() => popularCache?.items || POPULAR_ITEMS);
  const [popularLoading, setPopularLoading] = useState(!popularCache);
  const [popularHasMore, setPopularHasMore] = useState(() => popularCache?.hasMore || false);
  const [popularOffset, setPopularOffset] = useState(() => popularCache?.offset || 0);
  const POPULAR_PAGE_SIZE = 10;

  const [newsItems, setNewsItems] = useState(() => newsCache?.items || NEWS_ITEMS);
  const [newsLoading, setNewsLoading] = useState(!newsCache);

  const [researchItems, setResearchItems] = useState(() => researchCache?.items || RESEARCH_ITEMS);
  const [researchLoading, setResearchLoading] = useState(!researchCache);

  const fetchNews = useCallback(async () => {
    setNewsLoading(true);
    try {
      const data = await getInfoFlowResults('market', 50, 0);
      if (data.results && data.results.length > 0) {
        const mapped = data.results.map((r) => ({
          indexNumber: r.indexNumber,
          title: r.title,
          time: formatRelativeTime(r.event_timestamp),
          isHot: !!(r.tags && r.tags.length > 0),
        }));
        setNewsItems(mapped);
        newsCache = { items: mapped };
      } else {
        setNewsItems(NEWS_ITEMS);
      }
    } catch {
      setNewsItems(NEWS_ITEMS);
    } finally {
      setNewsLoading(false);
    }
  }, []);

  const fetchResearch = useCallback(async () => {
    setResearchLoading(true);
    try {
      const data = await getInfoFlowResults('industry', 50, 0);
      if (data.results && data.results.length > 0) {
        const mapped = data.results.map((r) => ({
          indexNumber: r.indexNumber,
          title: r.title,
          time: formatRelativeTime(r.event_timestamp),
          image: r.images?.[0]?.url || r.images?.[0] || null,
        }));
        setResearchItems(mapped);
        researchCache = { items: mapped };
      } else {
        setResearchItems(RESEARCH_ITEMS);
      }
    } catch {
      setResearchItems(RESEARCH_ITEMS);
    } finally {
      setResearchLoading(false);
    }
  }, []);

  const fetchPopular = useCallback(async (offset = 0, append = false) => {
    if (!append) setPopularLoading(true);
    try {
      const data = await getInfoFlowResults('hot_topic', POPULAR_PAGE_SIZE, offset);
      if (data.results && data.results.length > 0) {
        const newItems = data.results.map((r) => ({
          indexNumber: r.indexNumber,
          title: r.title,
          description: r.summary || '',
          tags: r.tags || [],
          event_timestamp: r.event_timestamp || '',
          image: r.images?.[0]?.url || r.images?.[0] || null,
        }));
        const newOffset = offset + data.results.length;
        setPopularItems((prev) => {
          const updated = append ? [...prev, ...newItems] : newItems;
          popularCache = { items: updated, hasMore: data.has_more, offset: newOffset };
          return updated;
        });
        setPopularHasMore(data.has_more);
        setPopularOffset(newOffset);
      } else if (!append) {
        setPopularItems(POPULAR_ITEMS);
        setPopularHasMore(false);
      }
    } catch {
      if (!append) setPopularItems(POPULAR_ITEMS);
    } finally {
      setPopularLoading(false);
    }
  }, []);

  const loadMorePopular = useCallback(() => {
    if (popularHasMore) {
      fetchPopular(popularOffset, true);
    }
  }, [popularHasMore, popularOffset, fetchPopular]);

  useEffect(() => {
    if (!popularCache) fetchPopular(0, false);
    if (!newsCache) fetchNews();
    if (!researchCache) fetchResearch();
  }, [fetchPopular, fetchNews, fetchResearch]);

  const fetchIndices = useCallback(async () => {
    setIndicesLoading(true);
    try {
      // API doesn't require from/to params - just call with symbols
      const { indices: next } = await getIndices(INDEX_SYMBOLS);
      setIndices(next);
    } catch (error) {
      console.error('[Dashboard] Error fetching indices:', error?.message);
      setIndices(INDEX_SYMBOLS.map((s) => fallbackIndex(normalizeIndexSymbol(s))));
    } finally {
      setIndicesLoading(false);
    }
  }, []);

  useEffect(() => {
    // Fetch immediately on mount
    fetchIndices();
    
    // Set up interval to fetch every minute (60000ms)
    const intervalId = setInterval(() => {
      console.log('[Dashboard] Refreshing Index Movement data');
      fetchIndices();
    }, 60000); // 60 seconds = 1 minute
    
    // Cleanup interval on unmount
    return () => {
      clearInterval(intervalId);
    };
  }, [fetchIndices]);

  /**
   * Check and create "Stealth Agent" default workspace on Dashboard load
   */
  useEffect(() => {
    const ensureDefaultWorkspace = async () => {
      try {
        const { workspaces } = await getWorkspaces(DEFAULT_USER_ID);
        const stealthAgentWorkspace = workspaces?.find(
          (ws) => ws.name === 'Stealth Agent'
        );
        
        if (!stealthAgentWorkspace) {
          // Create default workspace if it doesn't exist
          await createWorkspace(
            'Stealth Agent',
            'system default workspace, cannot be deleted'
          );
        }
      } catch (error) {
        // Silently fail - user can still use the app
        console.error('[Dashboard] Error ensuring default workspace:', error);
      }
    };

    ensureDefaultWorkspace();
  }, []);

  /**
   * Check onboarding completion status on every Dashboard mount.
   * Refetches so we pick up onboarding_completed after ChatAgent completes it.
   * Only shows dialog if onboarding is incomplete AND user hasn't clicked Ignore this session.
   */
  useEffect(() => {
    const checkOnboarding = async () => {
      try {
        const userData = await getCurrentUser(DEFAULT_USER_ID);
        const onboardingCompleted = userData?.user?.onboarding_completed;
        
        if (onboardingCompleted === true) {
          setShowOnboardingDialog(false);
          return;
        }
        if (onboardingCompleted === false && !onboardingDismissedThisSession) {
          setShowOnboardingDialog(true);
        }
      } catch (error) {
        console.error('[Dashboard] Error checking onboarding status:', error);
      }
    };

    checkOnboarding();
  }, []);

  const [watchlistRows, setWatchlistRows] = useState([]);
  const [watchlistLoading, setWatchlistLoading] = useState(true);
  const [watchlistModalOpen, setWatchlistModalOpen] = useState(false);
  const [currentWatchlistId, setCurrentWatchlistId] = useState(null);
  
  /**
   * Fetches watchlist data by:
   * 1. Getting all watchlists for the user
   * 2. Using the first watchlist's ID to fetch its items
   * 3. Getting stock prices for those symbols
   * 4. Combining the data for display
   */
  const fetchWatchlist = useCallback(async () => {
    setWatchlistLoading(true);
    try {
      // Step 1: Get all watchlists for the user
      const { watchlists } = await listWatchlists(DEFAULT_USER_ID);
      
      // Step 2: Get the first watchlist's ID (or use 'default' if no watchlists exist)
      const firstWatchlist = watchlists?.[0];
      const watchlistId = firstWatchlist?.watchlist_id || 'default';
      
      // Store the watchlist ID for use in add/delete operations
      setCurrentWatchlistId(watchlistId);
      
      // Step 3: Fetch items for the watchlist
      const { items } = await listWatchlistItems(watchlistId, DEFAULT_USER_ID);
      
      // Step 4: Extract symbols from items (empty array if no items)
      const symbols = items?.length ? items.map((i) => i.symbol) : [];
      
      // Step 5: Get stock prices for the symbols (only if there are symbols)
      const prices = symbols.length > 0 ? await getStockPrices(symbols) : [];
      const bySym = Object.fromEntries((prices || []).map((p) => [p.symbol, p]));
      
      // Step 6: Combine watchlist items with price data
      // If no items exist, set empty array
      const rows = items?.length
        ? items.map((i) => {
            const sym = String(i.symbol || '').trim().toUpperCase();
            const p = bySym[sym] || {};
            return {
              item_id: i.item_id,
              symbol: sym,
              price: p.price ?? 0,
              change: p.change ?? 0,
              changePercent: p.changePercent ?? 0,
              isPositive: p.isPositive ?? true,
            };
          })
        : [];
      setWatchlistRows(rows);
    } catch {
      // If any step fails, set empty rows (no fallback to default symbols)
      setWatchlistRows([]);
    } finally {
      setWatchlistLoading(false);
    }
  }, []);

  useEffect(() => {
    // Fetch immediately on mount
    fetchWatchlist();
    
    // Set up interval to fetch every minute (60000ms)
    const intervalId = setInterval(() => {
      console.log('[Dashboard] Refreshing Watchlist data');
      fetchWatchlist();
    }, 60000); // 60 seconds = 1 minute
    
    // Cleanup interval on unmount
    return () => {
      clearInterval(intervalId);
    };
  }, [fetchWatchlist]);

  const [deleteConfirm, setDeleteConfirm] = useState({
    open: false,
    title: '',
    message: '',
    onConfirm: null,
  });

  /**
   * Adds a stock to the current watchlist with full details
   * Uses the watchlist ID from the most recent fetch
   * @param {Object} itemData - Stock item data: { symbol, instrument_type, exchange, name, notes, alert_settings }
   * @param {string} watchlistId - The watchlist ID
   * @param {string} userId - The user ID
   */
  const handleAddWatchlist = useCallback(async (itemData, watchlistId, userId) => {
    try {
      // Ensure we have a watchlist ID
      let targetWatchlistId = watchlistId || currentWatchlistId;
      if (!targetWatchlistId) {
        const { watchlists } = await listWatchlists(DEFAULT_USER_ID);
        targetWatchlistId = watchlists?.[0]?.watchlist_id || 'default';
        setCurrentWatchlistId(targetWatchlistId);
      }
      
      await addWatchlistItem(itemData, targetWatchlistId, userId || DEFAULT_USER_ID);
      setWatchlistModalOpen(false);
      fetchWatchlist();
      
      toast({
        title: 'Stock added',
        description: `${itemData.symbol} has been added to your watchlist.`,
      });
    } catch (e) {
      console.error('Add watchlist item failed:', e?.response?.status, e?.response?.data, e?.message);
      
      const status = e?.response?.status;
      const msg = e?.response?.data?.detail || e?.response?.data?.message || '';
      
      if (status === 409 || msg.toLowerCase().includes('already exists')) {
        toast({
          variant: 'destructive',
          title: 'Already in watchlist',
          description: `${itemData.symbol} is already in your watchlist.`,
        });
      } else {
        toast({
          variant: 'destructive',
          title: 'Cannot add stock',
          description: msg || 'Failed to add to watchlist. Please try again.',
        });
      }
    }
  }, [currentWatchlistId, fetchWatchlist, toast]);

  /**
   * Deletes a watchlist item by ID
   * Uses the watchlist ID from the most recent fetch
   */
  const handleDeleteWatchlistItem = useCallback(
    async (itemId) => {
      try {
        // Get current watchlist ID (or fetch it if not available)
        let watchlistId = currentWatchlistId;
        if (!watchlistId) {
          const { watchlists } = await listWatchlists(DEFAULT_USER_ID);
          watchlistId = watchlists?.[0]?.watchlist_id || 'default';
          setCurrentWatchlistId(watchlistId);
        }
        
        await deleteWatchlistItem(itemId, watchlistId, DEFAULT_USER_ID);
        fetchWatchlist();
      } catch (e) {
        console.error('Delete watchlist item failed:', e?.response?.status, e?.response?.data, e?.message);
      }
    },
    [currentWatchlistId, fetchWatchlist]
  );

  const [portfolioRows, setPortfolioRows] = useState([]);
  const [portfolioLoading, setPortfolioLoading] = useState(true);
  const [portfolioHasRealHoldings, setPortfolioHasRealHoldings] = useState(false);
  const [portfolioModalOpen, setPortfolioModalOpen] = useState(false);

  /**
   * Fetches portfolio holdings data by:
   * 1. Getting all holdings for the user from /api/v1/users/me/portfolio
   * 2. Getting current stock prices for those symbols
   * 3. Calculating Unrealized P/L % using average_cost and current price
   * 4. Combining the data for display
   */
  const fetchPortfolio = useCallback(async () => {
    setPortfolioLoading(true);
    try {
      // Step 1: Get all portfolio holdings for the user
      const { holdings } = await getPortfolio(DEFAULT_USER_ID);
      
      // Step 2: Extract symbols from holdings (empty array if no holdings)
      const symbols = holdings?.length
        ? holdings.map((h) => String(h.symbol || '').trim().toUpperCase())
        : [];
      
      // Step 3: Get current stock prices for the symbols (only if there are symbols)
      const prices = symbols.length > 0 ? await getStockPrices(symbols) : [];
      const bySym = Object.fromEntries((prices || []).map((p) => [p.symbol, p]));
      
      // Step 4: Combine holdings with price data
      // If no holdings exist, set empty array
      if (holdings?.length) {
        setPortfolioHasRealHoldings(true);
        const rows = holdings.map((h) => {
          const sym = String(h.symbol || '').trim().toUpperCase();
          const p = bySym[sym] || {};
          const q = Number(h.quantity || 0);
          const ac = h.average_cost != null ? Number(h.average_cost) : null;
          const price = p.price ?? 0;
          const marketValue = q * price;
          // Calculate Unrealized P/L %: ((current_price - average_cost) / average_cost) * 100
          const plPct = ac != null && ac > 0 ? ((price - ac) / ac) * 100 : null;
          return {
            holding_id: h.holding_id,
            symbol: sym,
            quantity: q,
            average_cost: ac,
            notes: h.notes ?? '',
            price, // Current price from stock prices API
            marketValue,
            unrealizedPlPercent: plPct, // Unrealized P/L %
            isPositive: plPct == null ? true : plPct >= 0,
          };
        });
        setPortfolioRows(rows);
      } else {
        setPortfolioHasRealHoldings(false);
        setPortfolioRows([]);
      }
    } catch (error) {
      console.error('[Dashboard] Error fetching portfolio:', error);
      setPortfolioHasRealHoldings(false);
      setPortfolioRows([]);
    } finally {
      setPortfolioLoading(false);
    }
  }, []);

  useEffect(() => {
    // Fetch immediately on mount
    fetchPortfolio();
    
    // Set up interval to fetch every minute (60000ms)
    const intervalId = setInterval(() => {
      console.log('[Dashboard] Refreshing Portfolio data');
      fetchPortfolio();
    }, 60000); // 60 seconds = 1 minute
    
    // Cleanup interval on unmount
    return () => {
      clearInterval(intervalId);
    };
  }, [fetchPortfolio]);

  const handleDeletePortfolioItem = useCallback(
    (holdingId) => {
      setDeleteConfirm({
        open: true,
        title: 'Remove holding',
        message: 'Remove this holding from your portfolio?',
        onConfirm: async () => {
          try {
            await deletePortfolioHolding(holdingId, DEFAULT_USER_ID);
            fetchPortfolio();
          } catch (e) {
            console.error('Delete portfolio holding failed:', e?.response?.status, e?.response?.data, e?.message);
          }
        },
      });
    },
    [fetchPortfolio]
  );

  /**
   * Adds a portfolio holding with full details
   * @param {Object} payload - Portfolio holding data from AddPortfolioHoldingDialog
   * @param {string} userId - The user ID
   */
  const handleAddPortfolio = useCallback(async (payload, userId) => {
    try {
      await addPortfolioHolding(payload, userId || DEFAULT_USER_ID);
      setPortfolioModalOpen(false);
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
  }, [fetchPortfolio, toast]);

  const [portfolioEditRow, setPortfolioEditRow] = useState(null);
  const [portfolioEditForm, setPortfolioEditForm] = useState({ quantity: '', averageCost: '', notes: '' });

  const openPortfolioEdit = useCallback((row) => {
    setPortfolioEditRow(row);
    setPortfolioEditForm({
      quantity: row.quantity != null ? String(row.quantity) : '',
      averageCost: row.average_cost != null ? String(row.average_cost) : '',
      notes: row.notes ?? '',
    });
  }, []);

  const handleUpdatePortfolio = useCallback(async () => {
    if (!portfolioEditRow?.holding_id) return;
    const q = Number(portfolioEditForm.quantity);
    const ac = Number(portfolioEditForm.averageCost);
    if (!Number.isFinite(q) || q <= 0 || !Number.isFinite(ac) || ac <= 0) return;
    try {
      await updatePortfolioHolding(
        portfolioEditRow.holding_id,
        {
          quantity: q,
          average_cost: ac,
          notes: portfolioEditForm.notes.trim() || undefined,
        },
        DEFAULT_USER_ID
      );
      setPortfolioEditRow(null);
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
  }, [portfolioEditRow, portfolioEditForm, fetchPortfolio]);

  const runDeleteConfirm = useCallback(async () => {
    if (deleteConfirm.onConfirm) await deleteConfirm.onConfirm();
    setDeleteConfirm((p) => ({ ...p, open: false }));
  }, [deleteConfirm.onConfirm]);

  return (
    <div className="dashboard-container min-h-screen">
      <ConfirmDialog
        open={deleteConfirm.open}
        title={deleteConfirm.title}
        message={deleteConfirm.message}
        confirmLabel="Delete"
        onConfirm={runDeleteConfirm}
        onOpenChange={(open) => !open && setDeleteConfirm((p) => ({ ...p, open: false }))}
            />

      {/* Onboarding Incomplete Dialog */}
      <Dialog open={showOnboardingDialog} onOpenChange={setShowOnboardingDialog}>
        <DialogContent className="sm:max-w-md text-white border" style={{ backgroundColor: 'var(--color-bg-elevated)', borderColor: 'var(--color-border-elevated)' }}>
          <DialogHeader>
            <DialogTitle className="dashboard-title-font" style={{ color: 'var(--color-text-primary)' }}>
              Preference Information Incomplete
            </DialogTitle>
            <DialogDescription style={{ color: 'var(--color-text-secondary)' }}>
              Your preference information is not complete. Please complete your preferences to get the best experience with the agent.
            </DialogDescription>
          </DialogHeader>
          <div className="flex justify-end gap-2 pt-4">
            <button
              type="button"
              onClick={() => {
                onboardingDismissedThisSession = true;
                setShowOnboardingDialog(false);
              }}
              className="px-4 py-2 rounded-md text-sm font-medium transition-colors hover:bg-white/10"
              style={{ color: 'var(--color-text-primary)' }}
            >
              Ignore
            </button>
            <button
              type="button"
              onClick={async () => {
                setShowOnboardingDialog(false);
                setIsCreatingWorkspace(true);
                try {
                  // Find or create "Stealth Agent" workspace
                  const workspaceId = await findOrCreateDefaultWorkspace(
                    () => {}, // onCreating - already showing loading state
                    () => {}  // onCreated
                  );
                  
                  // Navigate to ChatAgent with onboarding flag
                  navigate(`/chat/${workspaceId}/__default__`, {
                    state: {
                      isOnboarding: true,
                    },
                  });
                } catch (error) {
                  console.error('Error setting up onboarding:', error);
                  toast({
                    variant: 'destructive',
                    title: 'Error',
                    description: 'Failed to set up onboarding. Please try again.',
                  });
                  setShowOnboardingDialog(true); // Re-open dialog on error
                } finally {
                  setIsCreatingWorkspace(false);
                }
              }}
              disabled={isCreatingWorkspace}
              className="px-4 py-2 rounded-md text-sm font-medium transition-colors hover:opacity-90 disabled:opacity-50 disabled:cursor-not-allowed"
              style={{ backgroundColor: 'var(--color-accent-primary)', color: 'var(--color-text-on-accent)' }}
            >
              {isCreatingWorkspace ? 'Setting up...' : 'Proceed'}
            </button>
          </div>
        </DialogContent>
      </Dialog>

      <DashboardHeader />

      <div className="flex-1 min-h-0 overflow-hidden flex flex-col">
        <div className="w-full flex-1 min-h-0 flex justify-center">
          <div className="w-full h-full min-h-0 max-w-[1400px] px-6 py-4 flex flex-col">
            <div className="grid grid-cols-[1fr_360px] gap-4 flex-1 min-h-0 h-full">
              <div className="w-full flex flex-col gap-4 h-full min-h-0 overflow-hidden">
                <IndexMovementCard indices={indices} loading={indicesLoading} />
                <PopularCard items={popularItems} loading={popularLoading} hasMore={popularHasMore} onLoadMore={loadMorePopular} />
                <div className="w-full grid grid-cols-2 gap-4 flex-1 min-h-0 overflow-hidden">
                  <TopNewsCard items={newsItems} loading={newsLoading} />
                  <TopResearchCard items={researchItems} loading={researchLoading} />
                </div>
                <ChatInputCard />
              </div>

              <div className="w-full flex flex-col gap-4 h-full min-h-0 overflow-hidden">
                <WatchlistCard
                  rows={watchlistRows}
                  loading={watchlistLoading}
                  onHeaderAddClick={() => setWatchlistModalOpen(true)}
                  onDeleteItem={handleDeleteWatchlistItem}
                />
                <AddWatchlistItemDialog
                  open={watchlistModalOpen}
                  onClose={() => setWatchlistModalOpen(false)}
                  onAdd={handleAddWatchlist}
                  watchlistId={currentWatchlistId}
                  userId={DEFAULT_USER_ID}
                />
                <PortfolioCard
                  rows={portfolioRows}
                  loading={portfolioLoading}
                  hasRealHoldings={portfolioHasRealHoldings}
                  onHeaderAddClick={() => setPortfolioModalOpen(true)}
                  editRow={portfolioEditRow}
                  editForm={portfolioEditForm}
                  onEditFormChange={setPortfolioEditForm}
                  onEditSubmit={handleUpdatePortfolio}
                  onEditClose={() => setPortfolioEditRow(null)}
                  onDeleteItem={handleDeletePortfolioItem}
                  onEditItem={openPortfolioEdit}
                />
                <AddPortfolioHoldingDialog
                  open={portfolioModalOpen}
                  onClose={() => setPortfolioModalOpen(false)}
                  onAdd={handleAddPortfolio}
                  userId={DEFAULT_USER_ID}
                />
                    </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

export default Dashboard;
