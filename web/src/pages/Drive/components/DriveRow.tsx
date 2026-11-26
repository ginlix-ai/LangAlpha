import type React from 'react';
import { useTranslation } from 'react-i18next';
import { MoreHorizontal } from 'lucide-react';
import {
  ContextMenu,
  ContextMenuContent,
  ContextMenuItem,
  ContextMenuSeparator,
  ContextMenuTrigger,
} from '@/components/ui/context-menu';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import { useLocale } from '@/hooks/useLocale';
import { useNow } from '@/hooks/useNow';
import { formatBytes, mediumDate, relativeTime } from '@/lib/format';
import { cn } from '@/lib/utils';
import type { DriveEntry } from '@/types/drive';
import { useDriveContext } from '../driveContext';
import { memoState } from '../memoMarks';
import type { DriveDrop } from '../useDriveDrop';
import { whoLabel } from '../who';
import { EntryGlyph } from './EntryGlyph';
import { EntryMenuItems } from './EntryMenuItems';
import { MemoChip, MemoStatus } from './MemoBadges';

/** Moves keyboard focus to the row before or after the one focused. */
function stepFocus(from: HTMLElement, delta: number) {
  const list = from.closest('[data-drive-rows]');
  if (!list) return;
  const rows = Array.from(list.querySelectorAll<HTMLElement>('[data-drive-row]'));
  const at = rows.indexOf(from);
  rows[Math.max(0, Math.min(rows.length - 1, at + delta))]?.focus();
}

/**
 * One entry of a folder. Wide, it carries columns (when, who, size); beside an
 * open file they fold into a line under the name. A folder row takes drops.
 */
export function DriveRow({ entry, compact, active, drop }: {
  entry: DriveEntry;
  compact: boolean;
  active: boolean;
  drop: DriveDrop;
}) {
  const { t } = useTranslation();
  const locale = useLocale();
  const now = useNow();
  const { open, ask, trash } = useDriveContext();
  const folder = entry.kind === 'folder';
  const state = memoState(entry);
  const when = relativeTime(entry.modified_at, locale, now);
  const who = whoLabel(t, entry.modified_by);
  const size = folder || entry.size == null ? '' : formatBytes(entry.size, locale);
  const { isDrop, ...dropHandlers } = folder
    ? drop.targetProps(entry.path, entry.path, 'rows-and-files')
    : { isDrop: false };

  const onKeyDown = (e: React.KeyboardEvent<HTMLElement>) => {
    if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
      e.preventDefault();
      stepFocus(e.currentTarget, e.key === 'ArrowDown' ? 1 : -1);
    } else if (e.key === 'F2') {
      e.preventDefault();
      ask.rename(entry);
    } else if (e.key === 'Delete' || (e.key === 'Backspace' && (e.metaKey || e.ctrlKey))) {
      e.preventDefault();
      trash(entry);
    }
  };

  return (
    <ContextMenu>
      <ContextMenuTrigger asChild>
        <div
          role="listitem"
          className={cn('drive-row', compact && 'is-compact', active && 'is-active', isDrop && 'is-drop')}
          data-testid={`drive-row-${entry.path}`}
          {...dropHandlers}
        >
          <button
            type="button"
            data-drive-row
            className="drive-row-main"
            onClick={() => open(entry)}
            onKeyDown={onKeyDown}
            {...drop.sourceProps(entry)}
            aria-current={active ? 'true' : undefined}
          >
            <EntryGlyph name={entry.name} folder={folder} className="drive-row-icon" />
            <span className="drive-row-text">
              <span className="drive-row-title">
                <span className="drive-row-name">{entry.name}</span>
                {state.kind === 'memo' && (
                  <>
                    <MemoChip />
                    <MemoStatus mark={state.mark} head={entry.version} />
                  </>
                )}
              </span>
              {compact && <span className="drive-row-sub">{[when, who, size].filter(Boolean).join(' · ')}</span>}
            </span>
            {!compact && (
              <>
                <span className="drive-col drive-col-when" title={mediumDate(new Date(entry.modified_at), locale)}>{when}</span>
                <span className="drive-col drive-col-who" title={who}>{who}</span>
                <span className="drive-col drive-col-size">{size}</span>
              </>
            )}
          </button>
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <button type="button" className="drive-row-kebab tap-target" aria-label={t('drive:actions.more', { name: entry.name })}>
                <MoreHorizontal className="h-4 w-4" aria-hidden="true" />
              </button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end">
              <EntryMenuItems entry={entry} parts={{ Item: DropdownMenuItem, Separator: DropdownMenuSeparator }} />
            </DropdownMenuContent>
          </DropdownMenu>
        </div>
      </ContextMenuTrigger>
      <ContextMenuContent>
        <EntryMenuItems entry={entry} parts={{ Item: ContextMenuItem, Separator: ContextMenuSeparator }} />
      </ContextMenuContent>
    </ContextMenu>
  );
}
