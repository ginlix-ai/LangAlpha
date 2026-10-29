import type { ReactNode } from 'react';
import { ChevronLeft, Search } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { BrandMark } from '@/pages/ChatAgent/components/mcp/BrandMark';
import { llmProviderArt } from '@/lib/brandArt';
import { cn } from '@/lib/utils';
import {
  CATEGORIES,
  filterGroups,
  type CategoryFilter,
  type KeyFilter,
  type KeyOption,
  type ProviderGroup,
} from './providerOptions';
import type { TileSize } from '@/pages/ChatAgent/components/mcp/IdentityTile';
import { QuietNote, Segmented } from './parts';
import { ProviderChoice } from './ProviderChoices';
import { regionName } from './optionLabels';

type T = (key: string) => string;

/** The regions a provider offers, for its row; nothing for the international one alone. */
function regionSummary(t: T, g: ProviderGroup): string | undefined {
  const regions = [...new Set(g.options.map((o) => o.region))];
  return regions.length > 1 || regions[0] !== 'intl' ? regions.map((r) => regionName(t, r)).join(' / ') : undefined;
}

/** A provider's group as one choice: its key providers' cell, or a coding plan's row. */
export function ProviderRow({
  g,
  configured,
  onPick,
  layout = 'grid',
}: {
  g: ProviderGroup;
  configured: ReadonlySet<string>;
  onPick: (group: ProviderGroup) => void;
  layout?: 'list' | 'grid';
}) {
  const { t } = useTranslation();
  return (
    <ProviderChoice
      layout={layout}
      markProvider={g.markProvider}
      name={g.name}
      note={regionSummary(t, g)}
      connected={g.options.some((o) => configured.has(o.provider))}
      onPick={() => onPick(g)}
    />
  );
}

/**
 * A provider as a tile, for a page that has to show every provider in a
 * narrower column: name only, since its plans and regions are offered on the
 * provider's own step, and a dot on the mark where a key is already saved.
 */
function ProviderTile({ g, configured, onPick }: { g: ProviderGroup; configured: ReadonlySet<string>; onPick: (group: ProviderGroup) => void }) {
  const { t } = useTranslation();
  const connected = g.options.some((o) => configured.has(o.provider));
  return (
    <li>
      <button
        type="button"
        onClick={() => onPick(g)}
        title={regionSummary(t, g)}
        className="flex w-full min-w-0 items-center gap-2 rounded-md px-2 py-2 text-left transition-colors hover:bg-(--color-bg-hover)"
      >
        <span className="relative inline-flex shrink-0">
          <BrandMark name={g.name} art={llmProviderArt(g.markProvider)} size="sm" className="rounded-md" />
          {connected && (
            <span
              aria-hidden="true"
              className="absolute -bottom-0.5 -right-0.5 h-2.5 w-2.5 rounded-full"
              style={{ background: 'var(--color-success)', boxShadow: '0 0 0 2px var(--color-bg-page)' }}
            />
          )}
        </span>
        <span className="line-clamp-2 text-sm leading-snug" style={{ color: 'var(--color-text-primary)' }}>{g.name}</span>
        {connected && <span className="sr-only">{t('modelAccess.status.connected')}</span>}
      </button>
    </li>
  );
}

/**
 * The add-key steps all take this one size, so moving between them, switching
 * the kind, or picking another provider never resizes the dialog. The step
 * scrolls inside it; the footer stays put.
 */
export function KeyStepFrame({ children, footer }: { children: ReactNode; footer?: ReactNode }) {
  return (
    <div className="flex h-[min(37rem,calc(85vh-5.5rem))] flex-col gap-4">
      <div className="flex min-h-0 flex-1 flex-col">{children}</div>
      {footer && <div className="flex shrink-0 items-center justify-between gap-2">{footer}</div>}
    </div>
  );
}

/** A provider's mark and name, heading its own steps. The mark matches the title's line height. */
export function ProviderTitle({
  name,
  markProvider,
  title,
  size = 'sm',
}: {
  name: string;
  markProvider: string;
  title: string;
  size?: TileSize;
}) {
  return (
    <span className="flex items-center gap-3">
      <BrandMark name={name} art={llmProviderArt(markProvider)} size={size} className="rounded-md" />
      <span className="min-w-0">{title}</span>
    </span>
  );
}

/**
 * Step one: every provider once, by kind: the labs that keep their models
 * closed, the open-weight labs, and the services that host other labs' models.
 */
export function KeyProviderList({
  groups,
  filter,
  onFilterChange,
  configured,
  onPick,
  compact = false,
}: {
  groups: readonly ProviderGroup[];
  filter: KeyFilter;
  onFilterChange: (filter: KeyFilter) => void;
  configured: ReadonlySet<string>;
  onPick: (group: ProviderGroup) => void;
  /** Three columns of names, for the setup page's narrower column. */
  compact?: boolean;
}) {
  const { t } = useTranslation();
  const shown = filterGroups(groups, filter);
  // Sections only on the full list; one kind picked is already one section.
  const sections = filter.category === 'all'
    ? [...CATEGORIES, null].map((c) => ({ category: c, groups: shown.filter((g) => g.category === c) })).filter((s) => s.groups.length > 0)
    : [{ category: null, groups: shown }];

  return (
    <div className="flex min-h-0 flex-1 flex-col gap-3">
      <div className="relative shrink-0">
        <Search aria-hidden="true" className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2" style={{ color: 'var(--color-text-tertiary)' }} />
        <Input
          value={filter.query}
          onChange={(e) => onFilterChange({ ...filter, query: e.target.value })}
          placeholder={t('modelAccess.keyPicker.search')}
          aria-label={t('modelAccess.keyPicker.search')}
          className="pl-9 text-sm"
          autoFocus
        />
      </div>
      <div className="shrink-0">
        <Segmented<CategoryFilter>
          label={t('modelAccess.keyPicker.kind')}
          value={filter.category}
          onChange={(category) => onFilterChange({ ...filter, category })}
          options={[
            { value: 'all', label: t('modelAccess.keyPicker.all') },
            ...CATEGORIES.map((c) => ({ value: c, label: t(`modelAccess.keyPicker.category.${c}`) })),
          ]}
        />
      </div>

      <div className="-mx-1 min-h-0 flex-1 overflow-y-auto px-1">
        {shown.length === 0 ? (
          <QuietNote>{t('modelAccess.keyPicker.empty')}</QuietNote>
        ) : (
          <div className="flex flex-col gap-2">
            {sections.map((section) => (
              <section key={section.category ?? 'other'} className="flex flex-col gap-1">
                {filter.category === 'all' && (
                  <h4 className="px-3 text-xs font-medium" style={{ color: 'var(--color-text-tertiary)' }}>
                    {t(`modelAccess.keyPicker.category.${section.category ?? 'other'}`)}
                  </h4>
                )}
                {/* Columns, so every provider is in view at once. */}
                {compact ? (
                  <ul className="grid grid-cols-2 gap-1 sm:grid-cols-3">
                    {section.groups.map((g) => <ProviderTile key={g.group} g={g} configured={configured} onPick={onPick} />)}
                  </ul>
                ) : (
                  <ul className="grid grid-cols-1 gap-1 sm:grid-cols-2">
                    {section.groups.map((g) => <ProviderRow key={g.group} g={g} configured={configured} onPick={onPick} />)}
                  </ul>
                )}
              </section>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

/**
 * Which of a provider's regions the key is for, at the top of its key form.
 * China and international keys are separate keyspaces, so the region is a
 * choice about the key, not taste. Nothing for a provider with one region.
 */
export function RegionChoice({
  group,
  option,
  onChange,
}: {
  group: ProviderGroup;
  option: KeyOption;
  onChange: (option: KeyOption) => void;
}) {
  const { t } = useTranslation();
  if (group.options.length < 2) return null;
  return (
    <div className="flex flex-col items-start gap-1.5">
      <span className="text-xs font-medium" style={{ color: 'var(--color-text-tertiary)' }}>{t('setup.regionLabel')}</span>
      <Segmented<string>
        label={t('setup.regionLabel')}
        value={option.provider}
        onChange={(provider) => {
          const next = group.options.find((o) => o.provider === provider);
          if (next) onChange(next);
        }}
        options={group.options.map((o) => ({ value: o.provider, label: regionName(t, o.region) }))}
      />
      <p className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>{t('modelAccess.keyPicker.regionHint')}</p>
    </div>
  );
}

export function BackButton({ onClick, disabled, className }: { onClick: () => void; disabled?: boolean; className?: string }) {
  const { t } = useTranslation();
  return (
    <Button type="button" variant="ghost" onClick={onClick} disabled={disabled} className={cn('-ml-3 gap-1', className)}>
      <ChevronLeft aria-hidden="true" className="h-4 w-4" />
      {t('common.back')}
    </Button>
  );
}
