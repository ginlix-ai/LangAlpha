import { useEffect, useMemo, useState } from 'react';
import { ArrowRight, Search } from 'lucide-react';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogTitle,
} from '@/components/ui/dialog';
import { listWidgets, listWidgetsByCategory } from './WidgetRegistry';
import type { WidgetCategory, WidgetDefinition, WidgetInstance } from '../types';

type CategoryMeta = { label: string; blurb: string; order: number; dot: string };

const CATEGORY_META: Record<WidgetCategory, CategoryMeta> = {
  markets: {
    label: 'Markets',
    blurb: 'Real-time quotes, indices, charts and calendars.',
    order: 1,
    dot: '#E0B341',
  },
  intel: {
    label: 'Intelligence',
    blurb: 'AI-generated briefs and curated news streams.',
    order: 2,
    dot: '#5BA47F',
  },
  personal: {
    label: 'Personal',
    blurb: 'Your portfolio, watchlist, and holdings.',
    order: 3,
    dot: '#C4A36B',
  },
  agent: {
    label: 'Agent',
    blurb: 'Kick off a research session from the dashboard.',
    order: 4,
    dot: '#C4574F',
  },
  workspace: {
    label: 'Workspace',
    blurb: 'Jump into a past conversation or workspace.',
    order: 5,
    dot: '#5DA372',
  },
};

interface AddWidgetDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onAdd: (type: string) => void;
  existingWidgets: WidgetInstance[];
}

export function AddWidgetDialog({ open, onOpenChange, onAdd, existingWidgets }: AddWidgetDialogProps) {
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
    return allWidgets.filter((w) =>
      w.title.toLowerCase().includes(searchLower) ||
      (w.description ?? '').toLowerCase().includes(searchLower) ||
      w.type.toLowerCase().includes(searchLower),
    );
  }, [allWidgets, searchActive, searchLower]);

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
      const target = e.target as HTMLElement;
      if (target.tagName === 'INPUT') return;
      e.preventDefault();
      handleAdd(selectedDef.type);
    }
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent
        className="!max-w-[1080px] w-[96vw] h-[86vh] !max-h-[86vh] !p-0 !gap-0 grid grid-cols-[280px_minmax(0,1fr)] grid-rows-[minmax(0,1fr)] !overflow-hidden [&>button.absolute]:hidden"
        style={{
          backgroundColor: 'var(--color-bg-card)',
          borderColor: 'var(--color-border-muted)',
          borderRadius: 20,
        }}
        onKeyDown={handleKey}
      >
        {/* Left rail */}
        <aside
          className="flex flex-col gap-5 p-6 min-w-0 min-h-0 overflow-y-auto"
          style={{
            backgroundColor: 'var(--color-bg-subtle)',
            borderRight: '1px solid var(--color-border-muted)',
          }}
        >
          <div>
            <div
              className="text-[11px] font-semibold uppercase tracking-[0.14em]"
              style={{ color: 'var(--color-text-tertiary)' }}
            >
              Widget Gallery
            </div>
            <DialogTitle
              className="title-font mt-1.5 text-[26px] leading-tight tracking-tight"
              style={{ color: 'var(--color-text-primary)', fontWeight: 500 }}
            >
              Add a widget
            </DialogTitle>
          </div>

          <label
            className="flex items-center gap-2 px-3 py-2 rounded-full border text-sm"
            style={{
              backgroundColor: 'var(--color-bg-card)',
              borderColor: 'var(--color-border-muted)',
              color: 'var(--color-text-tertiary)',
            }}
          >
            <Search size={14} />
            <input
              type="text"
              autoFocus
              placeholder="Search widgets..."
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              className="flex-1 min-w-0 bg-transparent outline-none"
              style={{ color: 'var(--color-text-primary)' }}
            />
          </label>

          <nav className="flex flex-col gap-1">
            {categoriesSorted.map((cat) => {
              const meta = CATEGORY_META[cat];
              const count = grouped[cat]?.length ?? 0;
              const active = !searchActive && cat === activeCat;
              return (
                <button
                  key={cat}
                  type="button"
                  onClick={() => {
                    setActiveCat(cat);
                    setSearch('');
                  }}
                  className="flex items-center justify-between gap-2 px-3 py-2 rounded-lg text-sm transition-colors"
                  style={{
                    backgroundColor: active ? 'var(--color-text-primary)' : 'transparent',
                    color: active ? 'var(--color-bg-card)' : 'var(--color-text-primary)',
                  }}
                  onMouseEnter={(e) => {
                    if (!active) e.currentTarget.style.backgroundColor = 'var(--color-bg-hover)';
                  }}
                  onMouseLeave={(e) => {
                    if (!active) e.currentTarget.style.backgroundColor = 'transparent';
                  }}
                >
                  <span className="flex items-center gap-2.5 min-w-0">
                    <span
                      className="inline-block rounded-full flex-shrink-0"
                      style={{ width: 8, height: 8, backgroundColor: meta?.dot ?? '#999' }}
                    />
                    <span className="font-medium truncate">{meta?.label ?? cat}</span>
                  </span>
                  <span
                    className="text-xs tabular-nums flex-shrink-0"
                    style={{
                      color: active ? 'var(--color-bg-card)' : 'var(--color-text-tertiary)',
                      opacity: active ? 0.7 : 1,
                    }}
                  >
                    {count}
                  </span>
                </button>
              );
            })}
          </nav>

          <div className="flex-1" />

          <div
            className="rounded-xl p-3 text-xs"
            style={{
              backgroundColor: 'var(--color-bg-card)',
              border: '1px solid var(--color-border-muted)',
            }}
          >
            <span
              className="inline-block text-[10px] font-semibold lowercase px-2 py-0.5 rounded-full mb-2"
              style={{
                backgroundColor: 'var(--color-profit-soft)',
                color: 'var(--color-profit)',
                letterSpacing: '0.05em',
              }}
            >
              tip
            </span>
            <div style={{ color: 'var(--color-text-secondary)', lineHeight: 1.5 }}>
              Click a widget to drop it at the next open slot on your dashboard.
            </div>
          </div>
        </aside>

        {/* Right content */}
        <div className="flex flex-col min-w-0 min-h-0" style={{ backgroundColor: 'var(--color-bg-card)' }}>
          <div className="flex items-start justify-between gap-6 px-8 pt-8 pb-5">
            <div className="flex-1 min-w-0">
              {searchActive ? (
                <>
                  <div
                    className="title-font text-[26px] leading-tight tracking-tight"
                    style={{ color: 'var(--color-text-primary)', fontWeight: 500 }}
                  >
                    Results
                  </div>
                  <DialogDescription
                    className="mt-1 text-sm"
                    style={{ color: 'var(--color-text-secondary)' }}
                  >
                    {matches?.length ?? 0} widget{(matches?.length ?? 0) === 1 ? '' : 's'} match &ldquo;{search}&rdquo;.
                  </DialogDescription>
                </>
              ) : (
                <>
                  <div className="flex items-baseline gap-3">
                    <div
                      className="title-font text-[26px] leading-tight tracking-tight"
                      style={{ color: 'var(--color-text-primary)', fontWeight: 500 }}
                    >
                      {CATEGORY_META[activeCat]?.label ?? activeCat}
                    </div>
                    <DialogDescription
                      className="text-sm"
                      style={{ color: 'var(--color-text-secondary)' }}
                    >
                      {CATEGORY_META[activeCat]?.blurb}
                    </DialogDescription>
                  </div>
                </>
              )}
            </div>
            <div className="flex items-center gap-2 flex-shrink-0">
              <span
                className="text-[11px] px-2.5 py-1 rounded-full whitespace-nowrap"
                style={{
                  backgroundColor: 'var(--color-bg-subtle)',
                  color: 'var(--color-text-tertiary)',
                }}
              >
                {allWidgets.length} total widgets
              </span>
              <button
                type="button"
                onClick={() => onOpenChange(false)}
                className="w-7 h-7 rounded-md flex items-center justify-center transition-colors"
                style={{ color: 'var(--color-text-tertiary)' }}
                onMouseEnter={(e) => {
                  e.currentTarget.style.backgroundColor = 'var(--color-bg-hover)';
                  e.currentTarget.style.color = 'var(--color-text-primary)';
                }}
                onMouseLeave={(e) => {
                  e.currentTarget.style.backgroundColor = 'transparent';
                  e.currentTarget.style.color = 'var(--color-text-tertiary)';
                }}
                aria-label="Close"
              >
                <svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" strokeWidth="1.75" strokeLinecap="round">
                  <path d="M3 3L11 11M11 3L3 11" />
                </svg>
              </button>
            </div>
          </div>

          <div className="flex-1 min-h-0 overflow-y-auto px-8 pb-6">
            {searchActive ? (
              <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                {(matches ?? []).map((def) => (
                  <WidgetCard
                    key={def.type}
                    def={def}
                    disabled={!!def.singleton && existingTypes.has(def.type)}
                    selected={selectedType === def.type}
                    onSelect={() => setSelectedType(def.type)}
                    onAdd={() => handleAdd(def.type)}
                  />
                ))}
                {(matches?.length ?? 0) === 0 && (
                  <div
                    className="col-span-full py-12 text-center text-sm"
                    style={{ color: 'var(--color-text-tertiary)' }}
                  >
                    No widgets match your search.
                  </div>
                )}
              </div>
            ) : (
              ordered.map((cat, i) => {
                const items = grouped[cat] ?? [];
                if (items.length === 0) return null;
                return (
                  <section key={cat} className={i === 0 ? '' : 'mt-8'}>
                    {i !== 0 && (
                      <div className="flex items-center gap-4 mb-4">
                        <div
                          className="flex-1 h-px"
                          style={{ backgroundColor: 'var(--color-border-muted)' }}
                        />
                        <div
                          className="text-[11px] font-semibold uppercase tracking-[0.14em]"
                          style={{ color: 'var(--color-text-tertiary)' }}
                        >
                          {CATEGORY_META[cat]?.label ?? cat}
                        </div>
                        <div
                          className="flex-1 h-px"
                          style={{ backgroundColor: 'var(--color-border-muted)' }}
                        />
                      </div>
                    )}
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                      {items.map((def) => (
                        <WidgetCard
                          key={def.type}
                          def={def}
                          disabled={!!def.singleton && existingTypes.has(def.type)}
                          selected={selectedType === def.type}
                          onSelect={() => setSelectedType(def.type)}
                          onAdd={() => handleAdd(def.type)}
                        />
                      ))}
                    </div>
                  </section>
                );
              })
            )}
          </div>

          <div
            className="flex items-center justify-between gap-4 px-8 py-4"
            style={{ borderTop: '1px solid var(--color-border-muted)' }}
          >
            <div className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
              Click to select · double-click or{' '}
              <span
                className="inline-flex items-center justify-center px-1.5 min-w-[18px] h-[18px] rounded border text-[10px] align-middle"
                style={{
                  borderColor: 'var(--color-border-muted)',
                  backgroundColor: 'var(--color-bg-subtle)',
                  color: 'var(--color-text-secondary)',
                }}
              >
                ↵
              </span>{' '}
              to add
            </div>
            <div className="flex items-center gap-2">
              <button
                type="button"
                onClick={() => onOpenChange(false)}
                className="px-4 py-2 text-sm rounded-lg border transition-colors"
                style={{
                  borderColor: 'var(--color-border-muted)',
                  color: 'var(--color-text-primary)',
                  backgroundColor: 'var(--color-bg-card)',
                }}
                onMouseEnter={(e) => (e.currentTarget.style.backgroundColor = 'var(--color-bg-hover)')}
                onMouseLeave={(e) => (e.currentTarget.style.backgroundColor = 'var(--color-bg-card)')}
              >
                Cancel
              </button>
              <button
                type="button"
                disabled={!selectedDef || selectedDisabled}
                onClick={() => selectedDef && !selectedDisabled && handleAdd(selectedDef.type)}
                className="px-4 py-2 text-sm font-medium rounded-lg transition-colors inline-flex items-center gap-1.5 disabled:cursor-not-allowed disabled:opacity-40"
                style={{
                  backgroundColor: 'var(--color-text-primary)',
                  color: 'var(--color-bg-card)',
                }}
              >
                {selectedDef && !selectedDisabled ? `Add ${selectedDef.title.toLowerCase()}` : 'Add widget'}
                <ArrowRight size={14} />
              </button>
            </div>
          </div>
        </div>
      </DialogContent>
    </Dialog>
  );
}

interface WidgetCardProps {
  def: WidgetDefinition<unknown>;
  disabled: boolean;
  selected: boolean;
  onSelect: () => void;
  onAdd: () => void;
}

function WidgetCard({ def, disabled, selected, onSelect, onAdd }: WidgetCardProps) {
  const Icon = def.icon;
  const isSingleton = !!def.singleton;
  const { w, h } = def.defaultSize;

  return (
    <button
      type="button"
      onClick={onSelect}
      onDoubleClick={() => !disabled && onAdd()}
      disabled={disabled}
      className="group text-left p-4 rounded-xl border transition-all disabled:cursor-not-allowed"
      style={{
        backgroundColor: selected ? 'var(--color-bg-subtle)' : 'var(--color-bg-card)',
        borderColor: selected ? 'var(--color-text-primary)' : 'var(--color-border-muted)',
        opacity: disabled ? 0.55 : 1,
        boxShadow: selected ? '0 0 0 1px var(--color-text-primary)' : 'none',
      }}
      onMouseEnter={(e) => {
        if (!selected && !disabled) e.currentTarget.style.borderColor = 'var(--color-border-default)';
      }}
      onMouseLeave={(e) => {
        if (!selected) e.currentTarget.style.borderColor = 'var(--color-border-muted)';
      }}
    >
      <div className="flex items-start gap-4">
        <div
          className="flex-shrink-0 w-20 h-14 rounded-lg flex items-center justify-center"
          style={{
            backgroundColor: 'var(--color-bg-subtle)',
            color: 'var(--color-text-tertiary)',
            border: '1px solid var(--color-border-muted)',
          }}
        >
          <Icon size={20} />
        </div>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 mb-1 flex-wrap">
            <span className="text-sm font-semibold" style={{ color: 'var(--color-text-primary)' }}>
              {def.title}
            </span>
            <span
              className="text-[10px] px-2 py-0.5 rounded-full lowercase"
              style={{
                backgroundColor: isSingleton ? 'var(--color-profit-soft)' : 'var(--color-bg-subtle)',
                color: isSingleton ? 'var(--color-profit)' : 'var(--color-text-tertiary)',
                letterSpacing: '0.02em',
              }}
            >
              {isSingleton ? 'singleton' : 'multi'}
            </span>
          </div>
          {def.description && (
            <div
              className="text-[12px] leading-snug mb-2"
              style={{ color: 'var(--color-text-secondary)' }}
            >
              {def.description}
            </div>
          )}
          <div
            className="text-[11px] dashboard-mono"
            style={{ color: 'var(--color-text-tertiary)' }}
          >
            {w}w × {h}h · {def.settingsComponent ? 'configurable' : 'no settings'}
            {disabled && ' · on dashboard'}
          </div>
        </div>
      </div>
    </button>
  );
}
