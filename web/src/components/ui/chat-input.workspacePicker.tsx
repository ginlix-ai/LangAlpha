import { useRef, useState, type RefObject } from 'react';
import { Check, FolderOpen, Layers, type LucideIcon } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { Menu, MenuItem, MenuSection, useFilter, type Key } from 'react-aria-components';
import { Autocomplete, SearchField } from './aria-search-field';
import {
  ComposerPopover, MENU_ROW, MENU_SECTION, useFocusCheckedRow, usePickToDraft,
} from './chat-input.composerPopover';
import { cn } from '@/lib/utils';
import type { Workspace } from './chat-input.types';

/** The key the Chief of Staff row answers with. No workspace id collides with it. */
export const ALL_WORKSPACES_KEY = '__all__';

// Search appears once the list outgrows the panel and starts to scroll; a short
// list is faster to read than to filter.
const SEARCH_FROM = 8;

/**
 * Where a composer send goes: the Chief of Staff (Home) or one workspace.
 * A menu rather than a listbox because a pick always closes it, the current
 * row included, and single selection still announces which row is current.
 */
export function WorkspacePicker({
  triggerRef,
  isOpen,
  onOpenChange,
  workspaces,
  selectedKey,
  onPick,
  emptyHint,
  direction,
  container,
  draftRef,
}: {
  triggerRef: RefObject<HTMLButtonElement | null>;
  isOpen: boolean;
  onOpenChange: (open: boolean) => void;
  workspaces: Workspace[];
  /** A workspace id, ALL_WORKSPACES_KEY, or null when nothing is chosen. */
  selectedKey: string | null;
  onPick: (key: string) => void;
  /** Shown under the list when there are no workspaces to pick. */
  emptyHint?: string | null;
  direction: 'up' | 'down';
  /** The composer, which a phone portals the picker into. */
  container?: HTMLElement | null;
  /** The message the pick is for, which takes focus after a pick by pointer. */
  draftRef?: RefObject<HTMLElement | null>;
}) {
  const { t } = useTranslation();
  const pickToDraft = usePickToDraft(isOpen, draftRef);
  const pick = (key: Key) => {
    onPick(String(key));
    pickToDraft();
    onOpenChange(false);
  };

  return (
    <ComposerPopover
      triggerRef={triggerRef}
      isOpen={isOpen}
      onOpenChange={onOpenChange}
      direction={direction}
      align="start"
      container={container}
      label={t('agents.pickWhere')}
      className="w-64"
    >
      <WorkspaceList workspaces={workspaces} selectedKey={selectedKey} emptyHint={emptyHint} onPick={pick} />
    </ComposerPopover>
  );
}

/** The picker's content, mounted while it is open, so the query starts empty
 *  on each opening. */
function WorkspaceList({ workspaces, selectedKey, emptyHint, onPick }: {
  workspaces: Workspace[];
  selectedKey: string | null;
  emptyHint?: string | null;
  onPick: (key: Key) => void;
}) {
  const { t } = useTranslation();
  const [query, setQuery] = useState('');
  const { contains } = useFilter({ sensitivity: 'base' });
  const searchable = workspaces.length >= SEARCH_FROM;
  const menuRef = useRef<HTMLDivElement>(null);
  // With the search field up, focus stays in it and never lands on the menu.
  useFocusCheckedRow(menuRef, !searchable);

  return (
    <>
      {/* Without the search field there is no input to hold virtual focus,
          so the rows take real focus and the arrow keys reach them. */}
      <Autocomplete inputValue={query} onInputChange={setQuery} filter={contains} disableVirtualFocus={!searchable}>
        {searchable && (
          <SearchField
            aria-label={t('workspace.searchWorkspaces')}
            placeholder={t('workspace.searchWorkspaces')}
            autoFocus
          />
        )}
        <Menu
          ref={menuRef}
          aria-label={t('agents.pickWhere')}
          selectionMode="single"
          // There is always a current choice, and without this Escape spends
          // itself clearing it instead of closing the picker.
          disallowEmptySelection
          selectedKeys={selectedKey ? [selectedKey] : []}
          onAction={onPick}
          autoFocus={searchable ? undefined : true}
          className="max-h-72 min-h-0 overflow-y-auto p-1 outline-hidden"
          renderEmptyState={() => (
            <p className="px-3 py-4 text-center text-[0.8125rem] text-(--color-text-tertiary)">
              {t('workspace.noWorkspacesFound')}
            </p>
          )}
        >
          <MenuSection id="all" aria-label={t('agents.chiefOfStaff')} className={MENU_SECTION}>
            <Row
              id={ALL_WORKSPACES_KEY}
              icon={Layers}
              label={t('agents.chiefOfStaff')}
              hint={t('agents.chiefOfStaffHint')}
            />
          </MenuSection>
          {workspaces.length > 0 && (
            <MenuSection id="workspaces" aria-label={t('workspace.workspaces')} className={MENU_SECTION}>
              {workspaces.map((ws) => (
                <Row key={ws.workspace_id} id={ws.workspace_id} icon={FolderOpen} label={ws.name} />
              ))}
            </MenuSection>
          )}
        </Menu>
      </Autocomplete>
      {emptyHint && !workspaces.length && (
        <p className="border-t border-(--color-border-muted) px-3 py-2.5 text-xs leading-snug text-(--color-text-tertiary)">
          {emptyHint}
        </p>
      )}
    </>
  );
}

function Row({ id, icon: Icon, label, hint }: { id: string; icon: LucideIcon; label: string; hint?: string }) {
  return (
    <MenuItem id={id} textValue={label} className={MENU_ROW}>
      {({ isSelected }) => (
        <>
          <Icon aria-hidden className="h-4 w-4 flex-none text-(--color-text-tertiary)" />
          <span className="min-w-0 flex-1 truncate" title={hint ?? label}>{label}</span>
          <Check
            aria-hidden
            className={cn('h-4 w-4 flex-none text-(--color-accent-primary)', !isSelected && 'invisible')}
          />
        </>
      )}
    </MenuItem>
  );
}
