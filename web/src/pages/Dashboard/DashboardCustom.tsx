import { useCallback, useEffect, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Plus, Layers, RotateCcw, X } from 'lucide-react';
import DashboardHeader from './components/DashboardHeader';
import ConfirmDialog from './components/ConfirmDialog';
import NewsDetailModal from './components/NewsDetailModal';
import InsightDetailModal from './components/InsightDetailModal';
import AddWatchlistItemDialog from './components/AddWatchlistItemDialog';
import AddPortfolioHoldingDialog from './components/AddPortfolioHoldingDialog';
import ChatInputCard from './components/ChatInputCard';
import { Dialog, DialogContent, DialogHeader, DialogTitle } from '../../components/ui/dialog';
import { Input } from '../../components/ui/input';
import { DashboardDataProvider, useDashboardContext } from './widgets/framework/DashboardDataContext';
import { MarketDataWSProvider } from '@/pages/MarketView/contexts/MarketDataWSContext';
import { DashboardGrid } from './widgets/framework/DashboardGrid';
import { AddWidgetDialog } from './widgets/framework/AddWidgetDialog';
import { PresetsDialog } from './widgets/framework/PresetsDialog';
import { getWidget } from './widgets/framework/WidgetRegistry';
import { placeAtBottom } from './widgets/framework/reconcile';
import { newWidgetId } from './widgets/framework/ids';
import { useDashboardPrefs } from './widgets/framework/useDashboardPrefs';
import type { DashboardPrefs, WidgetInstance } from './widgets/types';
import type { PresetId } from './widgets/presets';
import './widgets/index';
import './Dashboard.css';

interface DashboardCustomProps {
  mode: 'classic' | 'custom';
  onModeChange: (mode: 'classic' | 'custom') => void;
}

function CustomInner({ mode, onModeChange }: DashboardCustomProps) {
  const { t } = useTranslation();
  const { prefs, update, applyPreset, resetToDefault } = useDashboardPrefs();
  const [editMode, setEditMode] = useState(false);
  const [addOpen, setAddOpen] = useState(false);
  const [presetsOpen, setPresetsOpen] = useState(false);
  const [resetConfirmOpen, setResetConfirmOpen] = useState(false);
  const [settingsFor, setSettingsFor] = useState<string | null>(null);
  const mainRef = useRef<HTMLElement>(null);

  const ctx = useDashboardContext();
  const {
    portfolio,
    watchlist,
    modals,
  } = ctx;

  const handleScrollToTop = useCallback(() => {
    mainRef.current?.scrollTo({ top: 0, behavior: 'smooth' });
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
    <div className="dashboard-container min-h-screen">
      <main
        ref={mainRef}
        className="flex-1 flex flex-col min-h-0 overflow-y-auto overflow-x-hidden"
        style={{ paddingBottom: bottomGutter }}
      >
        <DashboardHeader
          onScrollToTop={handleScrollToTop}
          layoutToggle={{
            mode,
            onModeChange,
            editMode,
            onEditModeChange: setEditMode,
          }}
        />
        <div className="mx-auto max-w-[1920px] w-full p-3 sm:p-6 pb-6">
          <DashboardGrid
            prefs={prefs}
            editMode={editMode}
            onChange={handleGridChange}
            onOpenSettings={setSettingsFor}
          />
        </div>

        {showFloatingChat && <ChatInputCard />}
      </main>

      {/* Floating edit-mode toolbar */}
      {editMode && (
        <div
          className="fixed left-1/2 -translate-x-1/2 z-40 flex items-center gap-1 rounded-full border shadow-lg px-2 py-1.5"
          style={{
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
              backgroundColor: 'var(--color-accent-primary)',
              color: 'var(--color-text-on-accent)',
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
          <DialogContent
            className="sm:max-w-md"
            style={{
              backgroundColor: 'var(--color-bg-elevated)',
              borderColor: 'var(--color-border-elevated)',
            }}
          >
            <DialogHeader>
              <DialogTitle style={{ color: 'var(--color-text-primary)' }}>
                {t('dashboard.widgets.shell.settingsDialogTitle', {
                  title: settingsDef?.titleKey ? t(settingsDef.titleKey) : '',
                })}
              </DialogTitle>
            </DialogHeader>
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
          </DialogContent>
        </Dialog>
      )}

      {/* Modals shared with Classic — pulled from DashboardDataContext */}
      <NewsDetailModal
        newsId={modals.selectedNewsId}
        onClose={modals.closeNews}
        fallbackUrl={modals.selectedNewsFallbackUrl}
      />
      <InsightDetailModal
        marketInsightId={modals.selectedMarketInsightId}
        onClose={modals.closeInsight}
      />
      <ConfirmDialog
        open={modals.deleteConfirm.open}
        title={modals.deleteConfirm.title}
        message={modals.deleteConfirm.message}
        confirmLabel={t('dashboard.widgets.shell.deleteConfirm')}
        onConfirm={modals.runDeleteConfirm}
        onOpenChange={(open) => !open && modals.cancelDeleteConfirm()}
      />

      {/* Portfolio Edit Dialog */}
      <Dialog open={!!portfolio.editRow} onOpenChange={(open) => !open && portfolio.openEdit(null)}>
        <DialogContent
          className="sm:max-w-sm border"
          style={{ backgroundColor: 'var(--color-bg-elevated)', borderColor: 'var(--color-border-elevated)' }}
        >
          <DialogHeader>
            <DialogTitle className="title-font" style={{ color: 'var(--color-text-primary)' }}>
              {t('dashboard.widgets.shell.editHoldingTitle', { symbol: portfolio.editRow?.symbol ?? '' })}
            </DialogTitle>
          </DialogHeader>
          <div
            className="grid gap-3 py-2"
            onKeyDown={(e) => {
              if (e.key === 'Enter') {
                e.preventDefault();
                portfolio.handleUpdate?.();
              }
            }}
          >
            <div>
              <label className="text-xs block mb-1" style={{ color: 'var(--color-text-secondary)' }}>
                {t('dashboard.widgets.shell.quantityLabel')}
              </label>
              <Input
                type="number"
                min="0"
                step="any"
                placeholder={t('dashboard.widgets.shell.quantityPlaceholder')}
                value={portfolio.editForm.quantity ?? ''}
                onChange={(e) => portfolio.setEditForm?.({ ...portfolio.editForm, quantity: e.target.value })}
                className="border"
                style={{
                  backgroundColor: 'var(--color-bg-card)',
                  borderColor: 'var(--color-border-default)',
                  color: 'var(--color-text-primary)',
                }}
              />
            </div>
            <div>
              <label className="text-xs block mb-1" style={{ color: 'var(--color-text-secondary)' }}>
                {t('dashboard.widgets.shell.averageCostLabel')}
              </label>
              <Input
                type="number"
                min="0"
                step="any"
                placeholder={t('dashboard.widgets.shell.averageCostPlaceholder')}
                value={portfolio.editForm.averageCost ?? ''}
                onChange={(e) => portfolio.setEditForm?.({ ...portfolio.editForm, averageCost: e.target.value })}
                className="border"
                style={{
                  backgroundColor: 'var(--color-bg-card)',
                  borderColor: 'var(--color-border-default)',
                  color: 'var(--color-text-primary)',
                }}
              />
            </div>
            <div>
              <label className="text-xs block mb-1" style={{ color: 'var(--color-text-secondary)' }}>
                {t('dashboard.widgets.shell.notesLabel')}
              </label>
              <Input
                placeholder={t('dashboard.widgets.shell.notesPlaceholder')}
                value={portfolio.editForm.notes ?? ''}
                onChange={(e) => portfolio.setEditForm?.({ ...portfolio.editForm, notes: e.target.value })}
                className="border"
                style={{
                  backgroundColor: 'var(--color-bg-card)',
                  borderColor: 'var(--color-border-default)',
                  color: 'var(--color-text-primary)',
                }}
              />
            </div>
          </div>
          <div className="flex justify-end gap-2 pt-2">
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
              style={{ backgroundColor: 'var(--color-accent-primary)', color: 'var(--color-text-on-accent)' }}
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

export default function DashboardCustom(props: DashboardCustomProps) {
  return (
    <DashboardDataProvider>
      <MarketDataWSProvider>
        <CustomInner {...props} />
      </MarketDataWSProvider>
    </DashboardDataProvider>
  );
}
