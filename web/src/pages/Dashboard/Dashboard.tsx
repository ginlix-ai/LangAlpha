import { useCallback, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { ListFilter, Sparkles, X } from 'lucide-react';
import { MobileBottomSheet } from '../../components/ui/mobile-bottom-sheet';
import { Dialog, DialogContent, DialogHeader, DialogTitle } from '../../components/ui/dialog';
import { Input } from '../../components/ui/input';
import { useIsMobile } from '@/hooks/useIsMobile';
import DashboardHeader from './components/DashboardHeader';
import ConfirmDialog from './components/ConfirmDialog';
import IndexMovementCard from './components/IndexMovementCard';
import AIDailyBriefCard from './components/AIDailyBriefCard';
import NewsFeedCard from './components/NewsFeedCard';
import ChatInputCard from './components/ChatInputCard';
import EarningsCalendarCard from './components/EarningsCalendarCard';
import PortfolioWatchlistCard from './components/PortfolioWatchlistCard';
import NewsDetailModal from './components/NewsDetailModal';
import InsightDetailModal from './components/InsightDetailModal';
import AddWatchlistItemDialog from './components/AddWatchlistItemDialog';
import AddPortfolioHoldingDialog from './components/AddPortfolioHoldingDialog';
import { useWatchlistData } from './hooks/useWatchlistData';
import { usePortfolioData, type PortfolioRow } from './hooks/usePortfolioData';
import { useTickerNews } from './hooks/useTickerNews';
import { useDashboardData } from './hooks/useDashboardData';
import { useOnboarding, snoozePersonalization } from './hooks/useOnboarding';
import './Dashboard.css';

interface DeleteConfirmState {
  open: boolean;
  title: string;
  message: string;
  onConfirm: (() => Promise<void>) | null;
}

interface DashboardProps {
  layoutToggle?: {
    mode: 'classic' | 'custom';
    onModeChange: (mode: 'classic' | 'custom') => void;
  };
}

function Dashboard({ layoutToggle }: DashboardProps = {}) {
  const { t } = useTranslation();
  const isMobile = useIsMobile();
  const mainRef = useRef<HTMLElement>(null);
  const handleScrollToTop = useCallback(() => {
    mainRef.current?.scrollTo({ top: 0, behavior: 'smooth' });
  }, []);
  // News modal state
  const [selectedNewsId, setSelectedNewsId] = useState<string | null>(null);
  const [selectedNewsFallbackUrl, setSelectedNewsFallbackUrl] = useState<string | null>(null);

  // Insight modal state
  const [selectedMarketInsightId, setSelectedMarketInsightId] = useState<string | null>(null);

  // Mobile watchlist bottom sheet
  const [showWatchlistSheet, setShowWatchlistSheet] = useState(false);

  const {
    indices,
    indicesLoading,
    newsItems,
    newsLoading,
    marketStatus,
  } = useDashboardData();

  const {
    showPersonalizationBanner,
    setShowPersonalizationBanner,
    isCreatingWorkspace,
    navigateToPersonalization,
  } = useOnboarding();

  const watchlist = useWatchlistData();
  const portfolio = usePortfolioData();

  const portfolioNews = useTickerNews(portfolio.rows, 'portfolio');
  const watchlistNews = useTickerNews(watchlist.rows, 'watchlist');

  const [deleteConfirm, setDeleteConfirm] = useState<DeleteConfirmState>({
    open: false,
    title: '',
    message: '',
    onConfirm: null,
  });

  const handleDeletePortfolioItem = useCallback(
    (holdingId: string) => {
      setDeleteConfirm(portfolio.handleDelete(holdingId) as DeleteConfirmState);
    },
    [portfolio.handleDelete]
  );

  const runDeleteConfirm = useCallback(async () => {
    if (deleteConfirm.onConfirm) await deleteConfirm.onConfirm();
    setDeleteConfirm((p) => ({ ...p, open: false }));
  }, [deleteConfirm.onConfirm]);

  const portfolioWatchlistProps = {
    watchlistRows: watchlist.rows,
    watchlistLoading: watchlist.loading,
    onWatchlistAdd: () => { setShowWatchlistSheet(false); watchlist.setModalOpen(true); },
    onWatchlistDelete: (id: string) => { setShowWatchlistSheet(false); watchlist.handleDelete(id); },
    portfolioRows: portfolio.rows,
    portfolioLoading: portfolio.loading,
    hasRealHoldings: portfolio.hasRealHoldings,
    onPortfolioAdd: () => { setShowWatchlistSheet(false); portfolio.setModalOpen(true); },
    onPortfolioDelete: (id: string) => { setShowWatchlistSheet(false); handleDeletePortfolioItem(id); },
    onPortfolioEdit: (item: PortfolioRow) => { setShowWatchlistSheet(false); portfolio.openEdit(item); },
    marketStatus,
  };

  return (
    <div className="dashboard-container min-h-screen">
      {/* Main content area */}
      <main ref={mainRef} className="flex-1 flex flex-col min-h-0 overflow-y-auto overflow-x-hidden">
        <DashboardHeader onScrollToTop={handleScrollToTop} layoutToggle={layoutToggle} />

        <div className="mx-auto max-w-[1920px] w-full p-3 sm:p-6 pb-32">
          {/* Market Overview heading + mobile watchlist tab */}
          <div className="flex items-center justify-between mb-6">
            <h1
              className="text-2xl font-bold"
              style={{ color: 'var(--color-text-primary)' }}
            >
              Market Overview
            </h1>
            {isMobile && (
              <button
                onClick={() => setShowWatchlistSheet(true)}
                className="flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs font-medium border transition-colors"
                style={{
                  borderColor: 'var(--color-border-muted)',
                  color: 'var(--color-text-secondary)',
                  backgroundColor: 'var(--color-bg-card)',
                }}
              >
                <ListFilter size={13} />
                {t('dashboard.watchlist')}
              </button>
            )}
          </div>

          {/* Personalize your experience — dismissible banner */}
          {showPersonalizationBanner && (
            <div
              className="mb-6 rounded-lg border px-4 py-3 flex items-center gap-3"
              style={{
                backgroundColor: 'var(--color-bg-card)',
                borderColor: 'var(--color-border-muted)',
              }}
            >
              <Sparkles size={18} className="shrink-0" style={{ color: 'var(--color-accent-primary)' }} />
              <div className="flex-1 min-w-0">
                <p className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
                  {t('dashboard.personalizeTitle')}
                </p>
                <p className="text-xs mt-0.5" style={{ color: 'var(--color-text-secondary)' }}>
                  {t('dashboard.personalizeDesc')}
                </p>
              </div>
              <button
                type="button"
                onClick={() => {
                  setShowPersonalizationBanner(false);
                  navigateToPersonalization();
                }}
                disabled={isCreatingWorkspace}
                className="shrink-0 px-3 py-1.5 rounded-md text-xs font-medium transition-colors hover:opacity-90 disabled:opacity-50 disabled:cursor-not-allowed"
                style={{ backgroundColor: 'var(--color-accent-primary)', color: 'var(--color-text-on-accent)' }}
              >
                {isCreatingWorkspace ? t('dashboard.settingUp') : t('dashboard.personalize')}
              </button>
              <button
                type="button"
                onClick={() => {
                  snoozePersonalization();
                  setShowPersonalizationBanner(false);
                }}
                className="shrink-0 p-1 rounded transition-colors hover:bg-foreground/10"
                style={{ color: 'var(--color-text-tertiary)' }}
                aria-label={t('common.close')}
              >
                <X size={14} />
              </button>
            </div>
          )}

          {/* Index Movement — full width */}
          <div className="mb-8">
            <IndexMovementCard indices={indices} loading={indicesLoading} />
          </div>

          {/* 3-column grid */}
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
            {/* Left 2/3 */}
            <div className="lg:col-span-2 space-y-8">
              <AIDailyBriefCard onReadFull={setSelectedMarketInsightId} />
              <NewsFeedCard
                marketItems={newsItems}
                marketLoading={newsLoading}
                portfolioItems={portfolioNews.items}
                portfolioLoading={portfolioNews.loading}
                watchlistItems={watchlistNews.items}
                watchlistLoading={watchlistNews.loading}
                onNewsClick={(id, articleUrl) => {
                  setSelectedNewsId(String(id));
                  setSelectedNewsFallbackUrl(articleUrl ?? null);
                }}
              />
            </div>

            {/* Right 1/3 — sticky sidebar (hidden on mobile, accessible via sheet) */}
            {!isMobile && (
              <div className="lg:col-span-1">
                <div className="lg:sticky lg:top-24 space-y-6">
                  <div>
                    <PortfolioWatchlistCard {...portfolioWatchlistProps} />
                  </div>
                  <EarningsCalendarCard />
                </div>
              </div>
            )}
          </div>
        </div>

        {/* Floating chat */}
        <ChatInputCard />
      </main>

      {/* News Detail Modal */}
      <NewsDetailModal newsId={selectedNewsId} onClose={() => { setSelectedNewsId(null); setSelectedNewsFallbackUrl(null); }} fallbackUrl={selectedNewsFallbackUrl} />

      {/* Insight Detail Modal */}
      <InsightDetailModal
        marketInsightId={selectedMarketInsightId}
        onClose={() => setSelectedMarketInsightId(null)}
      />

      {/* Dialogs */}
      <ConfirmDialog
        open={deleteConfirm.open}
        title={deleteConfirm.title}
        message={deleteConfirm.message}
        confirmLabel={t('common.delete')}
        onConfirm={runDeleteConfirm}
        onOpenChange={(open) => !open && setDeleteConfirm((p) => ({ ...p, open: false }))}
      />

      {/* Personalization banner is rendered inline above the market overview */}

      {/* Portfolio Edit Dialog */}
      <Dialog open={!!portfolio.editRow} onOpenChange={(open) => !open && portfolio.openEdit(null)}>
        <DialogContent className="sm:max-w-sm border" style={{ backgroundColor: 'var(--color-bg-elevated)', borderColor: 'var(--color-border-elevated)' }}>
          <DialogHeader>
            <DialogTitle className="title-font" style={{ color: 'var(--color-text-primary)' }}>Edit holding — {portfolio.editRow?.symbol}</DialogTitle>
          </DialogHeader>
          <div className="grid gap-3 py-2" onKeyDown={(e) => { if (e.key === 'Enter') { e.preventDefault(); portfolio.handleUpdate?.(); } }}>
            <div>
              <label className="text-xs block mb-1" style={{ color: 'var(--color-text-secondary)' }}>Quantity *</label>
              <Input
                type="number"
                min="0"
                step="any"
                placeholder="e.g. 10.5"
                value={portfolio.editForm.quantity ?? ''}
                onChange={(e) => portfolio.setEditForm?.({ ...portfolio.editForm, quantity: e.target.value })}
                className="border"
                style={{ backgroundColor: 'var(--color-bg-card)', borderColor: 'var(--color-border-default)', color: 'var(--color-text-primary)' }}
              />
            </div>
            <div>
              <label className="text-xs block mb-1" style={{ color: 'var(--color-text-secondary)' }}>Average Cost Per Share *</label>
              <Input
                type="number"
                min="0"
                step="any"
                placeholder="e.g. 175.50"
                value={portfolio.editForm.averageCost ?? ''}
                onChange={(e) => portfolio.setEditForm?.({ ...portfolio.editForm, averageCost: e.target.value })}
                className="border"
                style={{ backgroundColor: 'var(--color-bg-card)', borderColor: 'var(--color-border-default)', color: 'var(--color-text-primary)' }}
              />
            </div>
            <div>
              <label className="text-xs block mb-1" style={{ color: 'var(--color-text-secondary)' }}>Notes</label>
              <Input
                placeholder="Optional"
                value={portfolio.editForm.notes ?? ''}
                onChange={(e) => portfolio.setEditForm?.({ ...portfolio.editForm, notes: e.target.value })}
                className="border"
                style={{ backgroundColor: 'var(--color-bg-card)', borderColor: 'var(--color-border-default)', color: 'var(--color-text-primary)' }}
              />
            </div>
          </div>
          <div className="flex justify-end gap-2 pt-2">
            <button type="button" onClick={() => portfolio.openEdit(null)} className="px-3 py-1.5 rounded text-sm border hover:bg-foreground/10" style={{ color: 'var(--color-text-primary)', borderColor: 'var(--color-border-default)' }}>
              Cancel
            </button>
            <button type="button" onClick={portfolio.handleUpdate} className="px-3 py-1.5 rounded text-sm font-medium hover:opacity-90" style={{ backgroundColor: 'var(--color-accent-primary)', color: 'var(--color-text-on-accent)' }}>
              Save
            </button>
          </div>
        </DialogContent>
      </Dialog>

      <AddWatchlistItemDialog
        open={watchlist.modalOpen}
        onClose={() => watchlist.setModalOpen(false)}
        onAdd={watchlist.handleAdd as (...args: unknown[]) => void}
        watchlistId={watchlist.currentWatchlistId ?? undefined}
      />
      <AddPortfolioHoldingDialog
        open={portfolio.modalOpen}
        onClose={() => portfolio.setModalOpen(false)}
        onAdd={portfolio.handleAdd as (...args: unknown[]) => void}
      />

      {/* Mobile watchlist/portfolio bottom sheet */}
      <MobileBottomSheet open={showWatchlistSheet} onClose={() => setShowWatchlistSheet(false)} className="pb-8">
        <PortfolioWatchlistCard {...portfolioWatchlistProps} />
        <div className="mt-4">
          <EarningsCalendarCard />
        </div>
      </MobileBottomSheet>
    </div>
  );
}

export default Dashboard;
