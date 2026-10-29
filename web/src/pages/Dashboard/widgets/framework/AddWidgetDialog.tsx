import { useEffect, useMemo, useState } from 'react';
import { Trans, useTranslation } from 'react-i18next';
import { X } from 'lucide-react';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogTitle,
} from '@/components/ui/dialog';
import { TermDivider, TermTag } from '@/pages/ChatAgent/components/charts/TermCard';
import '@/pages/ChatAgent/components/charts/termCard.css';
import '@/pages/ChatAgent/components/charts/termCardStyles.css';
import '../../components/dashWindow.css';
import { listWidgets, listWidgetsByCategory } from './WidgetRegistry';
import type { WidgetCategory, WidgetDefinition, WidgetInstance } from '../types';

type CategoryMeta = { labelKey: string; blurbKey: string; order: number };

const CATEGORY_META: Record<WidgetCategory, CategoryMeta> = {
  markets: {
    labelKey: 'dashboard.widgets.addDialog.category.marketsLabel',
    blurbKey: 'dashboard.widgets.addDialog.category.marketsBlurb',
    order: 1,
  },
  intel: {
    labelKey: 'dashboard.widgets.addDialog.category.intelligenceLabel',
    blurbKey: 'dashboard.widgets.addDialog.category.intelligenceBlurb',
    order: 2,
  },
  personal: {
    labelKey: 'dashboard.widgets.addDialog.category.personalLabel',
    blurbKey: 'dashboard.widgets.addDialog.category.personalBlurb',
    order: 3,
  },
  agent: {
    labelKey: 'dashboard.widgets.addDialog.category.agentLabel',
    blurbKey: 'dashboard.widgets.addDialog.category.agentBlurb',
    order: 4,
  },
  workspace: {
    labelKey: 'dashboard.widgets.addDialog.category.workspaceLabel',
    blurbKey: 'dashboard.widgets.addDialog.category.workspaceBlurb',
    order: 5,
  },
};

interface AddWidgetDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onAdd: (type: string) => void;
  existingWidgets: WidgetInstance[];
}

export function AddWidgetDialog({ open, onOpenChange, onAdd, existingWidgets }: AddWidgetDialogProps) {
  const { t } = useTranslation();
  const grouped = useMemo(() => listWidgetsByCategory(), []);
  const allWidgets = useMemo(() => listWidgets(), []);
  const existingTypes = useMemo(() => new Set(existingWidgets.map((w) => w.type)), [existingWidgets]);

  const categoriesSorted = useMemo(
    () => (Object.keys(grouped) as WidgetCategory[]).sort(
      (a, b) => (CATEGORY_META[a]?.order ?? 99) - (CATEGORY_META[b]?.order ?? 99),
    ),
    [grouped],
  );

  const [activeCat, setActiveCat] = useState<WidgetCategory>(() => categoriesSorted[0] ?? 'markets');
  const [search, setSearch] = useState('');
  const [selectedType, setSelectedType] = useState<string | null>(null);

  useEffect(() => {
    if (!open) {
      setSearch('');
      setSelectedType(null);
    }
  }, [open]);

  const searchActive = search.trim().length > 0;
  const searchLower = search.trim().toLowerCase();
  const matches = useMemo(() => {
    if (!searchActive) return null;
    return allWidgets.filter((w) => {
      const translatedTitle = t(w.titleKey);
      const translatedDesc = w.descriptionKey ? t(w.descriptionKey) : '';
      // Type stays English for power-user search ('chart.symbol', 'tv.movers');
      // chrome strings come from the active locale.
      return (
        w.type.toLowerCase().includes(searchLower) ||
        translatedTitle.toLowerCase().includes(searchLower) ||
        translatedDesc.toLowerCase().includes(searchLower)
      );
    });
  }, [allWidgets, searchActive, searchLower, t]);

  const ordered = useMemo(
    () => [activeCat, ...categoriesSorted.filter((c) => c !== activeCat)],
    [activeCat, categoriesSorted],
  );

  // Auto-select a sensible default so the CTA always has a target: first
  // non-disabled widget in the active category (or first search match).
  // Only re-runs when the current selection is empty or no longer visible.
  useEffect(() => {
    if (!open) return;
    const visible = searchActive ? matches ?? [] : grouped[activeCat] ?? [];
    if (visible.length === 0) {
      setSelectedType(null);
      return;
    }
    const isEnabled = (w: WidgetDefinition<unknown>) => !(w.singleton && existingTypes.has(w.type));
    setSelectedType((prev) => {
      if (prev && visible.some((w) => w.type === prev)) return prev;
      const firstEnabled = visible.find(isEnabled);
      return firstEnabled ? firstEnabled.type : visible[0].type;
    });
  }, [open, activeCat, searchActive, matches, grouped, existingTypes]);

  const selectedDef = selectedType
    ? allWidgets.find((w) => w.type === selectedType) ?? null
    : null;
  const selectedDisabled = !!(selectedDef && selectedDef.singleton && existingTypes.has(selectedDef.type));

  const handleAdd = (type: string) => {
    onAdd(type);
    onOpenChange(false);
  };

  const handleKey = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && selectedDef && !selectedDisabled) {
      // Enter on any other control does what that control does: it would
      // otherwise add the widget picked earlier, not the one in focus.
      const control = (e.target as HTMLElement).closest('button, input, a, select, textarea');
      if (control && !control.matches('[aria-pressed="true"]')) return;
      e.preventDefault();
      handleAdd(selectedDef.type);
    }
  };

  const cardFor = (def: WidgetDefinition<unknown>) => (
    <WidgetTile
      key={def.type}
      def={def}
      disabled={!!def.singleton && existingTypes.has(def.type)}
      selected={selectedType === def.type}
      onSelect={() => setSelectedType(def.type)}
      onAdd={() => handleAdd(def.type)}
    />
  );
  const categoryLabel = (cat: WidgetCategory) => (CATEGORY_META[cat]?.labelKey ? t(CATEGORY_META[cat].labelKey) : cat);

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent
        className="dash-window dash-window--radix dash-gallery max-w-[1080px]! w-[96vw] h-[86vh] max-h-[86vh]! p-0! gap-0! overflow-hidden! [&>button.absolute]:hidden"
        onKeyDown={handleKey}
      >
        <aside className="dash-gallery-nav">
          <div className="dash-gallery-eyebrow term-t3">{t('dashboard.widgets.addDialog.eyebrow')}</div>
          <DialogTitle className="dash-gallery-h">{t('dashboard.widgets.addDialog.title')}</DialogTitle>
          <input
            type="text"
            autoFocus
            className="dash-gallery-search"
            aria-label={t('dashboard.widgets.addDialog.searchPlaceholder')}
            placeholder={t('dashboard.widgets.addDialog.searchPlaceholder')}
            value={search}
            onChange={(e) => setSearch(e.target.value)}
          />
          <nav className="dash-gallery-cats">
            {categoriesSorted.map((cat) => (
              <button
                key={cat}
                type="button"
                className="dash-gallery-cat"
                aria-current={!searchActive && cat === activeCat ? 'true' : undefined}
                onClick={() => {
                  setActiveCat(cat);
                  setSearch('');
                }}
              >
                <span className="truncate">{categoryLabel(cat)}</span>
                <span className="term-t3">{grouped[cat]?.length ?? 0}</span>
              </button>
            ))}
          </nav>
        </aside>

        <div className="dash-gallery-main">
          <div className="dash-gallery-head">
            <span className="dash-gallery-title">
              {searchActive ? t('dashboard.widgets.addDialog.results') : categoryLabel(activeCat)}
            </span>
            <DialogDescription className="dash-gallery-blurb term-t3">
              {searchActive
                ? t('dashboard.widgets.addDialog.resultCount', { count: matches?.length ?? 0, search })
                : CATEGORY_META[activeCat]?.blurbKey
                  ? t(CATEGORY_META[activeCat].blurbKey)
                  : ''}
            </DialogDescription>
            <span className="term-push term-t3 whitespace-nowrap">
              {t('dashboard.widgets.addDialog.totalCount', { count: allWidgets.length })}
            </span>
            <button
              type="button"
              onClick={() => onOpenChange(false)}
              className="dash-window-x self-center"
              aria-label={t('dashboard.widgets.addDialog.closeAria')}
            >
              <X size={16} />
            </button>
          </div>

          <div className="dash-gallery-scroll">
            {searchActive ? (
              <div className="dash-gallery-grid">
                {(matches ?? []).map(cardFor)}
                {(matches?.length ?? 0) === 0 && (
                  <div className="dash-gallery-empty term-t3">{t('dashboard.widgets.addDialog.noMatches')}</div>
                )}
              </div>
            ) : (
              ordered.map((cat, i) => {
                const items = grouped[cat] ?? [];
                if (items.length === 0) return null;
                return (
                  <section key={cat} className="dash-gallery-section">
                    {i !== 0 && <TermDivider>{categoryLabel(cat)}</TermDivider>}
                    <div className="dash-gallery-grid">{items.map(cardFor)}</div>
                  </section>
                );
              })
            )}
          </div>

          <div className="dash-gallery-foot">
            <span className="term-t3">
              <Trans
                i18nKey="dashboard.widgets.addDialog.hintAddSentence"
                components={{ enter: <span className="dash-gallery-key">↵</span> }}
              />
            </span>
            <button type="button" className="dash-gallery-btn term-push" onClick={() => onOpenChange(false)}>
              {t('dashboard.widgets.addDialog.cancel')}
            </button>
            <button
              type="button"
              className="dash-gallery-btn dash-gallery-btn--primary"
              disabled={!selectedDef || selectedDisabled}
              onClick={() => selectedDef && !selectedDisabled && handleAdd(selectedDef.type)}
            >
              {selectedDef && !selectedDisabled
                ? t('dashboard.widgets.addDialog.addNamed', { title: t(selectedDef.titleKey) })
                : t('dashboard.widgets.addDialog.addWidget')}
            </button>
          </div>
        </div>
      </DialogContent>
    </Dialog>
  );
}

interface WidgetTileProps {
  def: WidgetDefinition<unknown>;
  disabled: boolean;
  selected: boolean;
  onSelect: () => void;
  onAdd: () => void;
}

/** A widget as a card: its name in the edge, its kind beside it, its default
 *  size at the far end. A button so the whole face selects; the head is drawn
 *  in spans, since a button holds phrasing content only. */
function WidgetTile({ def, disabled, selected, onSelect, onAdd }: WidgetTileProps) {
  const { t } = useTranslation();
  const { w, h } = def.defaultSize;
  return (
    <button
      type="button"
      className="term-card dash-gallery-tile"
      data-scale="panel"
      aria-pressed={selected}
      disabled={disabled}
      onClick={onSelect}
      onDoubleClick={() => !disabled && onAdd()}
    >
      <span className="term-card-head" data-keep="title">
        <span className="term-card-title">
          <span className="term-bracket" aria-hidden="true">[</span>
          <span className="term-card-title-text">{t(def.titleKey)}</span>
          <span className="term-bracket" aria-hidden="true">]</span>
        </span>
        <span className="term-card-sub">
          {def.singleton ? t('dashboard.widgets.addDialog.badgeSingleton') : t('dashboard.widgets.addDialog.badgeMulti')}
        </span>
        <span className="term-card-meta">{`${w}w × ${h}h`}</span>
      </span>
      {def.descriptionKey && <span className="dash-gallery-desc">{t(def.descriptionKey)}</span>}
      <span className="dash-gallery-tags">
        {def.source === 'tradingview' && <TermTag>{t('dashboard.widgets.addDialog.badgeTradingView')}</TermTag>}
        {def.settingsComponent && <TermTag>{t('dashboard.widgets.addDialog.tagSettings')}</TermTag>}
        {disabled && <TermTag>{t('dashboard.widgets.addDialog.tagOnDashboard')}</TermTag>}
      </span>
    </button>
  );
}
