import { use, useCallback, useEffect, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useQueryClient } from '@tanstack/react-query';
import { Plus, Layers, RotateCcw, X } from 'lucide-react';
import DashboardHeader from './components/DashboardHeader';
import ConfirmDialog from './components/ConfirmDialog';
import NewsDetailModal from './components/NewsDetailModal';
import InsightDetailModal from './components/InsightDetailModal';
import AddWatchlistItemDialog from './components/AddWatchlistItemDialog';
import AddPortfolioHoldingDialog from './components/AddPortfolioHoldingDialog';
import ChatInputCard from './components/ChatInputCard';
import { FormCard } from './components/FormCard';
import { Dialog, DialogContent, DialogHeader, DialogTitle } from '../../components/ui/dialog';
import { Input } from '../../components/ui/input';
import { DashboardDataProvider, useDashboardContext } from './widgets/framework/DashboardDataContext';
import { MarketDataWSProvider } from '@/pages/MarketView/contexts/MarketDataWSContext';
import { DashboardGrid } from './widgets/framework/DashboardGrid';
import { DashboardStack } from './widgets/framework/DashboardStack';
import { AddWidgetDialog } from './widgets/framework/AddWidgetDialog';
import { PresetsDialog } from './widgets/framework/PresetsDialog';
import { getWidget } from './widgets/framework/WidgetRegistry';
import { placeAtBottom } from './widgets/framework/reconcile';
import { newWidgetId } from './widgets/framework/ids';
import { useDashboardPrefs } from './widgets/framework/useDashboardPrefs';
import type { DashboardPrefs, WidgetInstance } from './widgets/types';
import type { PresetId } from './widgets/presets';
import './widgets/index';
import { useIsMobile } from '@/hooks/useIsMobile';
import { usePreferences } from '@/hooks/usePreferences';
import { queryKeys } from '@/lib/queryKeys';
import { useOnboarding, snoozePersonalization } from './hooks/useOnboarding';
import { WelcomeBanner } from '../Onboarding/welcome/WelcomeBanner';
import { getPreferences } from './utils/api';
import { useScrollMemory } from '@/lib/scrollMemory';
import { scrollMotion } from '@/lib/reducedMotion';
import './Dashboard.css';

function DashboardPage() {
  const { t } = useTranslation();
  const isMobile = useIsMobile();
  const { showPersonalizationBanner, setShowPersonalizationBanner } = useOnboarding();
  const { prefs, update, applyPreset, resetToDefault, editable } = useDashboardPrefs();
  // Editing is a desktop affordance, and needs the saved layout read: after a
  // failed read the page shows the default, which no edit may be saved over.
  const canEdit = !isMobile && editable;
  const [editMode, setEditMode] = useState(false);
  const [addOpen, setAddOpen] = useState(false);
  const [presetsOpen, setPresetsOpen] = useState(false);
  const [resetConfirmOpen, setResetConfirmOpen] = useState(false);
  const [settingsFor, setSettingsFor] = useState<string | null>(null);
  // A window narrowed to a phone's width leaves edit mode and closes the
  // dialogs that edit, or the toolbar would sit over a column it cannot edit
  // and a dialog left open would save a change from it.
  if (!canEdit && (editMode || addOpen || presetsOpen || resetConfirmOpen || settingsFor !== null)) {
    setEditMode(false);
    setAddOpen(false);
    setPresetsOpen(false);
    setResetConfirmOpen(false);
    setSettingsFor(null);
  }
  const mainRef = useRef<HTMLElement>(null);
  useScrollMemory(mainRef, 'page:dashboard');

  const ctx = useDashboardContext();
  const {
    portfolio,
    watchlist,
    modals,
  } = ctx;

  const handleScrollToTop = useCallback(() => {
    mainRef.current?.scrollTo({ top: 0, behavior: scrollMotion() });
  }, []);

  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === 'Escape' && editMode) setEditMode(false);
    };
    document.addEventListener('keydown', handler);
    return () => document.removeEventListener('keydown', handler);
  }, [editMode]);

  const handleAddWidget = useCallback(
    (type: string) => {
      const def = getWidget(type);
      if (!def) return;
      // Prefer the widget's context-aware initConfig factory when present so
      // e.g. a new TickerTape can default to the user's watchlist symbols.
      // Falls back to shallow-copying the static defaultConfig otherwise.
      const initialConfig = def.initConfig
        ? def.initConfig(ctx)
        : { ...(def.defaultConfig as object) };
      const newInstance: WidgetInstance = {
        id: newWidgetId(),
        type,
        config: initialConfig,
      };
      update((prev: DashboardPrefs) => {
        if (def.singleton && prev.widgets.some((w) => w.type === type)) return prev;
        const layouts = placeAtBottom(prev.layouts, newInstance.id, def.defaultSize.w, def.defaultSize.h);
        return { ...prev, widgets: [...prev.widgets, newInstance], layouts };
      });
    },
    [update, ctx]
  );

  const handleGridChange = useCallback(
    (next: DashboardPrefs) => {
      update(() => next);
    },
    [update]
  );
  const handleGridAdjust = useCallback(
    (next: DashboardPrefs) => {
      update(() => next, { adjust: true });
    },
    [update]
  );
  // From the latest prefs, not a render's copy, so two changes in a row keep
  // both.
  const handleConfigChange = useCallback(
    (id: string, patch: Partial<unknown>) => {
      update((prev) => ({
        ...prev,
        widgets: prev.widgets.map((w) => (w.id === id ? { ...w, config: { ...(w.config as object), ...(patch as object) } } : w)),
      }));
    },
    [update]
  );

  const settingsInstance = settingsFor ? prefs.widgets.find((w) => w.id === settingsFor) : null;
  const settingsDef = settingsInstance ? getWidget(settingsInstance.type) : null;
  const SettingsComp = settingsDef?.settingsComponent;

  // Hide the floating chat input if the user has placed the agent widget on the canvas,
  // or while the edit toolbar is open (they'd overlap).
  const hasAgentWidget = prefs.widgets.some((w) => w.type === 'agent.conversation');
  const showFloatingChat = !hasAgentWidget && !editMode;

  // Reserve a bottom gutter inside <main>'s scroll area when a floating overlay
  // is visible (edit-mode toolbar or floating chat card). Sitting inside the
  // scroll area means widgets end above the overlay without exposing a seam of
  // outer-container bg at the viewport bottom.
  // Toolbar sits at `bottom: 1.5rem` with ~2.5rem height, chat card is taller;
  // 6rem / 8rem give comfortable clearance without reserving dead space when
  // neither overlay is on screen.
  const bottomGutter = editMode ? '6rem' : showFloatingChat ? '8rem' : '0';

  return (
    <div className="dashboard-container min-h-full">
      <main
        ref={mainRef}
        className="flex-1 flex flex-col min-h-0 overflow-y-auto overflow-x-hidden"
        style={{ paddingBottom: bottomGutter }}
      >
        <DashboardHeader
          onScrollToTop={handleScrollToTop}
          edit={canEdit ? { editMode, onEditModeChange: setEditMode } : undefined}
        />
        <div className="mx-auto max-w-[1920px] w-full p-3 sm:p-6 pb-6">
          {/* The welcome flow, offered until an investing profile is saved */}
          {showPersonalizationBanner && (
            <WelcomeBanner
              onDismiss={() => {
                snoozePersonalization();
                setShowPersonalizationBanner(false);
              }}
            />
          )}
          {isMobile ? (
            <DashboardStack prefs={prefs} onConfigChange={handleConfigChange} />
          ) : (
            <DashboardGrid
              prefs={prefs}
              editMode={editMode}
              onChange={handleGridChange}
              onAdjust={handleGridAdjust}
              onOpenSettings={setSettingsFor}
            />
          )}
        </div>

        {showFloatingChat && <ChatInputCard />}
      </main>

      {/* Floating edit-mode toolbar */}
      {editMode && (
        <div
          className="fixed left-1/2 sidebar-tracking z-40 flex items-center gap-1 rounded-full border shadow-lg px-2 py-1.5"
          style={{
            // Centered on the content column rather than the viewport, whose
            // midpoint sits half a sidebar left of the grid below. Shifted by
            // transform, not by `left`: this box is width:auto, so moving `left`
            // would also shrink the space it can grow into and crush the row.
            transform: 'translateX(calc(-50% + var(--sidebar-width) / 2))',
            bottom: '1.5rem',
            backgroundColor: 'var(--color-bg-elevated)',
            borderColor: 'var(--color-border-elevated)',
          }}
        >
          <button
            type="button"
            onClick={() => setAddOpen(true)}
            className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium rounded-full transition-colors"
            style={{
              backgroundColor: 'var(--color-btn-primary-bg)',
              color: 'var(--color-btn-primary-text)',
            }}
          >
            <Plus size={12} /> {t('dashboard.widgets.shell.addWidget')}
          </button>
          <button
            type="button"
            onClick={() => setPresetsOpen(true)}
            className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium rounded-full transition-colors"
            style={{ color: 'var(--color-text-primary)' }}
          >
            <Layers size={12} /> {t('dashboard.widgets.shell.presets')}
          </button>
          <button
            type="button"
            onClick={() => setResetConfirmOpen(true)}
            className="flex items-center gap-1.5 px-3 py-1.5 text-xs rounded-full transition-colors"
            style={{ color: 'var(--color-text-secondary)' }}
          >
            <RotateCcw size={12} /> {t('dashboard.widgets.shell.reset')}
          </button>
          <div className="w-px h-5 mx-0.5" style={{ backgroundColor: 'var(--color-border-muted)' }} />
          <button
            type="button"
            onClick={() => setEditMode(false)}
            className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium rounded-full transition-colors"
            style={{ color: 'var(--color-text-secondary)' }}
          >
            <X size={12} /> {t('dashboard.widgets.shell.done')}
          </button>
        </div>
      )}

      {/* Reset confirmation */}
      <ConfirmDialog
        surface="page"
        open={resetConfirmOpen}
        title={t('dashboard.widgets.shell.resetTitle')}
        message={t('dashboard.widgets.shell.resetMessage')}
        confirmLabel={t('dashboard.widgets.shell.resetConfirm')}
        onConfirm={() => {
          setSettingsFor(null);
          resetToDefault();
        }}
        onOpenChange={setResetConfirmOpen}
      />

      {/* Widget picker */}
      <AddWidgetDialog
        open={addOpen}
        onOpenChange={setAddOpen}
        onAdd={handleAddWidget}
        existingWidgets={prefs.widgets}
      />

      {/* Presets */}
      <PresetsDialog
        open={presetsOpen}
        onOpenChange={setPresetsOpen}
        onApply={(id: PresetId) => {
          // Closing settings before swapping the widget set prevents the
          // settings dialog from referencing a stale id after the preset
          // replaces `prefs.widgets` (which would silently unmount the
          // dialog mid-edit). Same protection applies to the reset path —
          // resetToDefault calls applyPreset under the hood.
          setSettingsFor(null);
          applyPreset(id);
        }}
      />

      {/* Widget settings */}
      {settingsInstance && SettingsComp && (
        <Dialog open={!!settingsFor} onOpenChange={(open) => !open && setSettingsFor(null)}>
          <DialogContent className="dash-form sm:max-w-md">
            <DialogHeader className="text-left">
              <DialogTitle className="title-font" style={{ color: 'var(--color-text-primary)' }}>
                {settingsDef?.titleKey ? t(settingsDef.titleKey) : ''}
              </DialogTitle>
            </DialogHeader>
            <FormCard title={t('dashboard.widgets.frame.settingsTitle')}>
              <SettingsComp
                config={settingsInstance.config}
                onChange={(patch: Partial<unknown>) => {
                  update((prev) => ({
                    ...prev,
                    widgets: prev.widgets.map((w) =>
                      w.id === settingsInstance.id
                        ? { ...w, config: { ...(w.config as object), ...(patch as object) } }
                        : w
                    ),
                  }));
                }}
                onClose={() => setSettingsFor(null)}
              />
            </FormCard>
          </DialogContent>
        </Dialog>
      )}

      {/* Modals the widgets open, through DashboardDataContext */}
      <NewsDetailModal
        newsId={modals.selectedNewsId}
        onClose={modals.closeNews}
        fallback={modals.selectedNewsFallback}
      />
      <InsightDetailModal
        marketInsightId={modals.selectedMarketInsightId}
        onClose={modals.closeInsight}
      />
      <ConfirmDialog
        surface="page"
        open={modals.deleteConfirm.open}
        title={modals.deleteConfirm.title}
        message={modals.deleteConfirm.message}
        confirmLabel={t('dashboard.widgets.shell.deleteConfirm')}
        onConfirm={modals.runDeleteConfirm}
        onOpenChange={(open) => !open && modals.cancelDeleteConfirm()}
      />

      {/* Portfolio Edit Dialog */}
      <Dialog open={!!portfolio.editRow} onOpenChange={(open) => !open && portfolio.openEdit(null)}>
        <DialogContent className="dash-form sm:max-w-sm">
          <DialogHeader className="text-left">
            <DialogTitle className="title-font" style={{ color: 'var(--color-text-primary)' }}>
              {t('dashboard.widgets.shell.editHolding')}
              {/* The symbol is the card's title; the dialog's name says it too. */}
              <span className="sr-only"> {portfolio.editRow?.symbol ?? ''}</span>
            </DialogTitle>
          </DialogHeader>
          <FormCard title={<span className="term-code">{portfolio.editRow?.symbol ?? ''}</span>}>
            <div
              className="grid gap-3"
              onKeyDown={(e) => {
                if (e.key === 'Enter') {
                  e.preventDefault();
                  portfolio.handleUpdate?.();
                }
              }}
            >
              <label className="block">
                <span className="term-label dash-form-label">{t('dashboard.widgets.shell.quantityLabel')}</span>
                <Input
                  type="number"
                  min="0"
                  step="any"
                  placeholder={t('dashboard.widgets.shell.quantityPlaceholder')}
                  value={portfolio.editForm.quantity ?? ''}
                  onChange={(e) => portfolio.setEditForm?.({ ...portfolio.editForm, quantity: e.target.value })}
                  className="dash-form-input dash-form-figure border"
                />
              </label>
              <label className="block">
                <span className="term-label dash-form-label">{t('dashboard.widgets.shell.averageCostLabel')}</span>
                <Input
                  type="number"
                  min="0"
                  step="any"
                  placeholder={t('dashboard.widgets.shell.averageCostPlaceholder')}
                  value={portfolio.editForm.averageCost ?? ''}
                  onChange={(e) => portfolio.setEditForm?.({ ...portfolio.editForm, averageCost: e.target.value })}
                  className="dash-form-input dash-form-figure border"
                />
              </label>
              <label className="block">
                <span className="term-label dash-form-label">{t('dashboard.widgets.shell.notesLabel')}</span>
                <Input
                  placeholder={t('dashboard.widgets.shell.notesPlaceholder')}
                  value={portfolio.editForm.notes ?? ''}
                  onChange={(e) => portfolio.setEditForm?.({ ...portfolio.editForm, notes: e.target.value })}
                  className="dash-form-input border"
                />
              </label>
            </div>
          </FormCard>
          <div className="flex justify-end gap-2">
            <button
              type="button"
              onClick={() => portfolio.openEdit(null)}
              className="px-3 py-1.5 rounded text-sm border hover:bg-foreground/10"
              style={{ color: 'var(--color-text-primary)', borderColor: 'var(--color-border-default)' }}
            >
              {t('dashboard.widgets.shell.cancel')}
            </button>
            <button
              type="button"
              onClick={portfolio.handleUpdate}
              className="px-3 py-1.5 rounded text-sm font-medium hover:opacity-90"
              style={{ backgroundColor: 'var(--color-btn-primary-bg)', color: 'var(--color-btn-primary-text)' }}
            >
              {t('dashboard.widgets.shell.save')}
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
    </div>
  );
}

// A settled-either-way twin per in-flight read, so every render that suspends
// on one read hands `use` the same thenable, and a failed read ends the wait
// (the page then draws the default layout) instead of throwing.
const settledReads = new WeakMap<Promise<unknown>, Promise<void>>();
function settledOf(read: Promise<unknown>): Promise<void> {
  let settled = settledReads.get(read);
  if (!settled) {
    settled = read.then(() => undefined, () => undefined);
    settledReads.set(read, settled);
  }
  return settled;
}

/**
 * The dashboard: the user's widget layout, or the default one when they have
 * saved none. Until the preferences are read the page suspends, so the route's
 * loader stays up rather than a guessed layout that the saved one replaces.
 */
export default function Dashboard() {
  const { isLoading } = usePreferences();
  const queryClient = useQueryClient();
  // Not after a failed read: an observer still shows a failed read as loading,
  // so waiting again would start another read, fail, and wait again for good.
  // The page renders instead, and its own read retries once as it mounts.
  // Counted, not read off `status`: a refetch (on focus) of a read that never
  // landed sets it back to pending, which would take the page down until it
  // settles.
  const key = queryKeys.user.preferences();
  if (isLoading && !queryClient.getQueryState(key)?.errorUpdateCount) {
    // Starts the read when no reader has yet; joins the one in flight otherwise.
    void queryClient.prefetchQuery({ queryKey: key, queryFn: getPreferences, retry: false });
    const read = queryClient.getQueryCache().find({ queryKey: key })?.promise;
    if (read) use(settledOf(read));
  }
  return (
    <DashboardDataProvider>
      <MarketDataWSProvider>
        <DashboardPage />
      </MarketDataWSProvider>
    </DashboardDataProvider>
  );
}
