import {
  INDEX_SYMBOLS,
  fallbackIndex,
  getCurrentUser,
  getIndices,
  normalizeIndexSymbol,
  getInfoFlowResults,
} from './utils/api';
import { useCallback, useEffect, useState } from 'react';
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
import { useWatchlistData } from './hooks/useWatchlistData';
import { usePortfolioData } from './hooks/usePortfolioData';
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
let indicesCache = null; // [ index objects ]

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
    indicesCache || INDEX_SYMBOLS.map((s) => fallbackIndex(normalizeIndexSymbol(s)))
  );
  const [indicesLoading, setIndicesLoading] = useState(!indicesCache);

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
    if (!indicesCache) setIndicesLoading(true);
    try {
      const { indices: next } = await getIndices(INDEX_SYMBOLS);
      setIndices(next);
      indicesCache = next;
    } catch (error) {
      console.error('[Dashboard] Error fetching indices:', error?.message);
      if (!indicesCache) {
        setIndices(INDEX_SYMBOLS.map((s) => fallbackIndex(normalizeIndexSymbol(s))));
      }
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
   * Check and create "LangAlpha" default workspace on Dashboard load
   */
  useEffect(() => {
    const ensureDefaultWorkspace = async () => {
      try {
        const { workspaces } = await getWorkspaces();
        const stealthAgentWorkspace = workspaces?.find(
          (ws) => ws.name === 'LangAlpha'
        );
        
        if (!stealthAgentWorkspace) {
          // Create default workspace if it doesn't exist
          await createWorkspace(
            'LangAlpha',
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
        const userData = await getCurrentUser();
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

  const watchlist = useWatchlistData();
  const portfolio = usePortfolioData();

  const [deleteConfirm, setDeleteConfirm] = useState({
    open: false,
    title: '',
    message: '',
    onConfirm: null,
  });

  const handleDeletePortfolioItem = useCallback(
    (holdingId) => {
      setDeleteConfirm(portfolio.handleDelete(holdingId));
    },
    [portfolio.handleDelete]
  );

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
                  // Find or create "LangAlpha" workspace
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
                  rows={watchlist.rows}
                  loading={watchlist.loading}
                  onHeaderAddClick={() => watchlist.setModalOpen(true)}
                  onDeleteItem={watchlist.handleDelete}
                />
                <AddWatchlistItemDialog
                  open={watchlist.modalOpen}
                  onClose={() => watchlist.setModalOpen(false)}
                  onAdd={watchlist.handleAdd}
                  watchlistId={watchlist.currentWatchlistId}
                />
                <PortfolioCard
                  rows={portfolio.rows}
                  loading={portfolio.loading}
                  hasRealHoldings={portfolio.hasRealHoldings}
                  onHeaderAddClick={() => portfolio.setModalOpen(true)}
                  editRow={portfolio.editRow}
                  editForm={portfolio.editForm}
                  onEditFormChange={portfolio.setEditForm}
                  onEditSubmit={portfolio.handleUpdate}
                  onEditClose={() => portfolio.openEdit(null)}
                  onDeleteItem={handleDeletePortfolioItem}
                  onEditItem={portfolio.openEdit}
                />
                <AddPortfolioHoldingDialog
                  open={portfolio.modalOpen}
                  onClose={() => portfolio.setModalOpen(false)}
                  onAdd={portfolio.handleAdd}
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
