/**
 * The gallery's one toolbar row: search, the computer (a chip naming the one
 * there is, or a filter once there are two), sort. Custom is the order cards
 * are dragged into, and its menu item says so.
 * The computer control is the page's one way to the computers: its menu
 * manages them and adds one, and with none yet it is a New computer button.
 * Before the first workspace there is nothing to search or sort, so the page
 * shows the computer control alone.
 * Below a laptop's width the search takes the first line and the controls the
 * next, filling it; on a wide screen the search stops at a field's width and
 * the controls stay beside it.
 */
import { useId } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { ArrowDownUp, Check, ChevronDown, Plus, Search, Server, Settings } from 'lucide-react';
import { useTranslation } from 'react-i18next';

import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuRadioGroup,
  DropdownMenuRadioItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import { cn } from '@/lib/utils';
import type { Computer } from '@/types/api';

import { ComputerBadge } from '../ComputerBadge';
import { ComputerStatusIndicator } from '../computerStatusUi';
import { isComputerPreparing } from '../../utils/computerStatus';
import { prefetchComputerProviders } from '../../hooks/useComputers';
import { prefetchTierQuota } from '../../hooks/useTierQuota';
import { ComputerChip } from './ComputerChip';
import { DiskPercent } from './DiskPercent';

export type GallerySort = 'activity' | 'name' | 'custom';

/** The sort menu's options, in its order. */
export const GALLERY_SORTS: readonly GallerySort[] = ['activity', 'name', 'custom'];

/** The filter's value for every computer. */
export const ALL_COMPUTERS = 'all';

const CONTROL = 'flex h-9 items-center gap-1.5 rounded-lg border px-2.5 text-sm transition-colors hover:bg-foreground/5';
const CONTROL_STYLE = { borderColor: 'var(--color-border-muted)', color: 'var(--color-text-secondary)' };

export interface ComputerControlProps {
  /** The machines: none is a New computer button, one a chip, more a filter. */
  computers: readonly Computer[];
  isThisMac: (computer: Computer) => boolean;
  computerFilter: string;
  onComputerFilterChange: (computerId: string) => void;
  /** Open the Computers dialog, on a machine or on none. */
  onManageComputers: (computerId: string | null) => void;
  /** Open the New computer form over the page. */
  onNewComputer: () => void;
  className?: string;
}

interface GalleryToolbarProps {
  search: string;
  onSearchChange: (value: string) => void;
  /** Null until the list is read, when its slot stays empty. */
  computerControl: ComputerControlProps | null;
  sortBy: GallerySort;
  onSortChange: (sort: GallerySort) => void;
}

export function GalleryToolbar({ search, onSearchChange, computerControl, sortBy, onSortChange }: GalleryToolbarProps) {
  const { t } = useTranslation();
  const customHintId = useId();
  const sortLabels: Record<GallerySort, string> = {
    activity: t('workspace.sortRecent', 'Recent'),
    name: t('common.name'),
    custom: t('workspace.custom'),
  };

  return (
    <div className="flex flex-wrap items-center gap-2 md:flex-nowrap" data-testid="gallery-toolbar">
      {/* The pill rings for the field inside it: a ring drawn on the field
          alone would cut this border and leave the icon outside the
          indicator. `rings-within` in tokens.css owns the rule. */}
      <div
        className="rings-within owns-its-edge flex h-9 min-w-0 basis-full items-center gap-2 rounded-lg border px-3 transition-colors md:max-w-md md:flex-1 md:basis-auto"
        style={{ backgroundColor: 'var(--color-bg-input)', borderColor: 'var(--color-border-muted)' }}
      >
        <Search className="h-4 w-4 shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
        <input
          type="search"
          className="w-full bg-transparent text-base sm:text-sm"
          style={{ color: 'var(--color-text-primary)' }}
          placeholder={t('workspace.searchWorkspaces')}
          aria-label={t('workspace.searchWorkspaces')}
          value={search}
          onChange={(e) => onSearchChange(e.target.value)}
        />
      </div>

      {/* Below a laptop's width it fills the second line beside sort, and its
          name gives way rather than pushing sort onto a third. */}
      {computerControl && (
        <ComputerControl {...computerControl} className="flex-1 basis-0 md:flex-none md:basis-auto" />
      )}

      <DropdownMenu modal={false}>
        <DropdownMenuTrigger asChild>
          <button
            type="button"
            className={cn(CONTROL, 'shrink-0')}
            style={CONTROL_STYLE}
            aria-label={`${t('workspace.sortBy')}: ${sortLabels[sortBy]}`}
            data-testid="gallery-sort"
          >
            <ArrowDownUp className="h-4 w-4 shrink-0" />
            <span>{sortLabels[sortBy]}</span>
            <ChevronDown className="h-3.5 w-3.5 shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
          </button>
        </DropdownMenuTrigger>
        <DropdownMenuContent align="end" className="min-w-40">
          <DropdownMenuRadioGroup value={sortBy} onValueChange={(v) => onSortChange(v as GallerySort)}>
            {GALLERY_SORTS.map((id) => (
              <DropdownMenuRadioItem
                key={id}
                value={id}
                className="items-start"
                // The hint is read as a description, so the item's name stays the order's.
                aria-label={sortLabels[id]}
                aria-describedby={id === 'custom' ? customHintId : undefined}
              >
                <Mark on={sortBy === id} className="mt-0.5" />
                {id === 'custom' ? (
                  <span className="flex flex-col">
                    <span>{sortLabels[id]}</span>
                    <span id={customHintId} className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
                      {t('workspace.customSortHint')}
                    </span>
                  </span>
                ) : sortLabels[id]}
              </DropdownMenuRadioItem>
            ))}
          </DropdownMenuRadioGroup>
        </DropdownMenuContent>
      </DropdownMenu>
    </div>
  );
}

/**
 * The computer control. With none yet it is a New computer button; with one
 * its menu reads the machine, with more it filters the grid by machine. Its
 * menu and the button read what the New computer form waits on ahead of time.
 */
export function ComputerControl({
  computers,
  isThisMac,
  computerFilter,
  onComputerFilterChange,
  onManageComputers,
  onNewComputer,
  className,
}: ComputerControlProps) {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  // The New computer form waits on the providers and the plan's quota.
  const prefetch = () => {
    void prefetchComputerProviders(queryClient);
    void prefetchTierQuota(queryClient);
  };
  const single = computers.length === 1 ? computers[0] : null;
  const filtered = computers.find((c) => c.computer_id === computerFilter) ?? null;
  const filterLabel = filtered ? filtered.name : t('workspace.allComputers', 'All computers');

  if (computers.length === 0) {
    return (
      <button
        type="button"
        className={cn(CONTROL, 'min-w-0', className)}
        style={CONTROL_STYLE}
        onClick={onNewComputer}
        onPointerEnter={prefetch}
        onFocus={prefetch}
        data-testid="new-computer"
      >
        <Plus className="h-4 w-4 shrink-0" />
        <span className="truncate">{t('computer.newComputer', 'New computer')}</span>
      </button>
    );
  }

  return (
    <DropdownMenu modal={false} onOpenChange={(open) => { if (open) prefetch(); }}>
      <DropdownMenuTrigger asChild>
        {single ? (
          <ComputerChip
            computer={single}
            isThisMac={isThisMac(single)}
            className={cn(CONTROL, 'min-w-0 md:max-w-[22rem]', className)}
            style={CONTROL_STYLE}
          />
        ) : (
          <button
            type="button"
            className={cn(CONTROL, 'min-w-0 md:max-w-[14rem]', className)}
            style={CONTROL_STYLE}
            aria-label={`${t('workspace.filterByComputer', 'Filter by computer')}: ${filterLabel}`}
            data-testid="computer-filter"
          >
            <Server className="h-4 w-4 shrink-0" />
            <span className="min-w-0 flex-1 truncate text-left">{filterLabel}</span>
            <ChevronDown className="h-3.5 w-3.5 shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
          </button>
        )}
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" className="min-w-56">
        {single ? (
          // The machine itself, read rather than picked: with one there is nothing to filter.
          <DropdownMenuLabel className="flex items-center gap-2 font-normal" data-testid="computer-menu-machine">
            <ComputerRowFace computer={single} isThisMac={isThisMac(single)} />
          </DropdownMenuLabel>
        ) : (
          <DropdownMenuRadioGroup value={filtered ? filtered.computer_id : ALL_COMPUTERS} onValueChange={onComputerFilterChange}>
            {/* Every row leads with its glyph in the menu's icon column, and
                only the chosen one carries the check, at its end: the others'
                badges reach the edge rather than stop short of an empty one. */}
            <DropdownMenuRadioItem value={ALL_COMPUTERS}>
              <Server className="h-3.5 w-3.5 shrink-0" aria-hidden="true" />
              <span className="min-w-0 flex-1">{t('workspace.allComputers', 'All computers')}</span>
              {!filtered && <Mark on />}
            </DropdownMenuRadioItem>
            <DropdownMenuSeparator />
            {computers.map((c) => (
              <DropdownMenuRadioItem key={c.computer_id} value={c.computer_id}>
                <ComputerRowFace computer={c} isThisMac={isThisMac(c)} />
                {filtered?.computer_id === c.computer_id && <Mark on />}
              </DropdownMenuRadioItem>
            ))}
          </DropdownMenuRadioGroup>
        )}
        <DropdownMenuSeparator />
        {/* Opens on the one machine, or on the one the grid is narrowed to. */}
        <DropdownMenuItem onSelect={() => onManageComputers(single?.computer_id ?? filtered?.computer_id ?? null)}>
          <Settings className="h-3.5 w-3.5 shrink-0" aria-hidden="true" />
          {single ? t('workspace.manageComputerMenu', 'Manage computer…') : t('workspace.manageComputers', 'Manage computers…')}
        </DropdownMenuItem>
        <DropdownMenuItem onSelect={onNewComputer}>
          <Plus className="h-3.5 w-3.5 shrink-0" aria-hidden="true" />
          {t('workspace.newComputerMenu', 'New computer…')}
        </DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  );
}

/** A machine as the computer menu lists it: state, name, badge, a filling disk. */
function ComputerRowFace({ computer, isThisMac }: { computer: Computer; isThisMac: boolean }) {
  return (
    <>
      {/* As wide as an item's icon, so the name lines up with the items' text. */}
      <span className="grid w-3.5 shrink-0 place-items-center">
        <ComputerStatusIndicator status={computer.status} preparing={isComputerPreparing(computer)} glyphSize={10} glyphOnly />
      </span>
      <span className="min-w-0 flex-1 truncate">{computer.name}</span>
      <ComputerBadge computer={computer} isThisMac={isThisMac} name={computer.name} />
      <DiskPercent computer={computer} className="text-xs" />
    </>
  );
}

/** The chosen row's check. Off, it still holds its place, for a column that must line up. */
function Mark({ on, className }: { on: boolean; className?: string }) {
  return <Check className={cn('h-3.5 w-3.5 shrink-0', className)} style={{ opacity: on ? 1 : 0 }} aria-hidden="true" />;
}

export default GalleryToolbar;
