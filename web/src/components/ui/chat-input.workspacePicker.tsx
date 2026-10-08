import { useEffect, useEffectEvent, useRef, useState, type RefObject } from 'react';
import { Check, FolderOpen, Layers, type LucideIcon } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import {
  Dialog, Menu, MenuItem, MenuSection, useFilter, type Key,
} from 'react-aria-components';
import { Popover } from './aria-popover';
import { Autocomplete, SearchField } from './aria-search-field';
import { cn } from '@/lib/utils';
import { lastInputWasPointer } from '@/lib/inputModality';
import type { Workspace } from './chat-input.types';

/** The key the All workspaces row answers with. No workspace id collides with it. */
export const ALL_WORKSPACES_KEY = '__all__';

// Search appears once the list outgrows the panel and starts to scroll; a short
// list is faster to read than to filter.
const SEARCH_FROM = 8;

/**
 * Where a composer send goes: All workspaces (when offered) or one workspace.
 * A menu rather than a listbox because a pick always closes it, the current
 * row included, and single selection still announces which row is current.
 */
export function WorkspacePicker({
  triggerRef,
  isOpen,
  onOpenChange,
  workspaces,
  selectedKey,
  includeAll = false,
  onPick,
  emptyHint,
  placement,
  portalContainer,
  draftRef,
}: {
  triggerRef: RefObject<HTMLButtonElement | null>;
  isOpen: boolean;
  onOpenChange: (open: boolean) => void;
  workspaces: Workspace[];
  /** A workspace id, ALL_WORKSPACES_KEY, or null when nothing is chosen. */
  selectedKey: string | null;
  includeAll?: boolean;
  onPick: (key: string) => void;
  /** Shown under the list when there are no workspaces to pick. */
  emptyHint?: string | null;
  placement: 'top start' | 'bottom start';
  portalContainer?: HTMLElement | null;
  /** The message the pick is for, which takes focus after a pick by pointer. */
  draftRef?: RefObject<HTMLElement | null>;
}) {
  const { t } = useTranslation();
  const [query, setQuery] = useState('');
  const { contains } = useFilter({ sensitivity: 'base' });
  const searchable = workspaces.length >= SEARCH_FROM;
  const popoverRef = useRef<HTMLElement>(null);
  const menuRef = useRef<HTMLDivElement>(null);

  const close = (open: boolean) => {
    if (!open) setQuery('');
    onOpenChange(open);
  };
  // A click on a row is a click in the composer, which puts the caret in the
  // draft; a keyboard pick goes back to the pill, where the user left off. The
  // draft takes focus once the picker is shut, since until then the menu keeps
  // handing it back to the row that was pressed.
  const toDraft = useRef(false);
  const pick = (key: Key) => {
    onPick(String(key));
    toDraft.current = lastInputWasPointer();
    close(false);
  };
  useEffect(() => {
    if (isOpen || !toDraft.current) return;
    toDraft.current = false;
    draftRef?.current?.focus();
  }, [isOpen, draftRef]);

  // Non-modal, like the composer's other menus, so the transcript keeps
  // scrolling and the rest of the page answers while it is open. Blur closes
  // it when focus moves to another control, but react-aria ignores focus
  // falling to the page, so a press on something that takes no focus is
  // caught here. The pill's own press toggles it.
  const pressedOutside = useEffectEvent((target: Node) => {
    if (popoverRef.current?.contains(target) || triggerRef.current?.contains(target)) return;
    close(false);
  });
  useEffect(() => {
    if (!isOpen) return;
    const onPointerDown = (e: PointerEvent) => pressedOutside(e.target as Node);
    document.addEventListener('pointerdown', onPointerDown, true);
    return () => document.removeEventListener('pointerdown', onPointerDown, true);
  }, [isOpen]);

  // Opened by a click the browser made up (Enter on the pill, a screen reader),
  // react-aria holds each focus move for a frame and drops any that finds
  // focus already moved. The list is empty on its first render, so the first
  // move lands on the menu itself and voids the one to the current row; the
  // row is marked focused all the same, and this sends focus on to it.
  useEffect(() => {
    const menu = menuRef.current;
    if (!isOpen || searchable || !menu) return;
    const onFocusIn = (e: FocusEvent) => {
      if (e.target === menu) menu.querySelector<HTMLElement>('[data-focused]')?.focus();
    };
    menu.addEventListener('focusin', onFocusIn);
    return () => menu.removeEventListener('focusin', onFocusIn);
  }, [isOpen, searchable]);

  // The pill can fold into the overflow menu or leave the toolbar while the
  // picker is open, which unmounts it with the open state still set. Closing
  // on the way out keeps it from springing open when the pill comes back.
  const closeIfOpen = useEffectEvent(() => {
    if (isOpen) onOpenChange(false);
  });
  useEffect(() => () => closeIfOpen(), []);

  return (
    <Popover
      ref={popoverRef}
      triggerRef={triggerRef}
      isOpen={isOpen}
      onOpenChange={close}
      isNonModal
      placement={placement}
      offset={6}
      UNSTABLE_portalContainer={portalContainer ?? undefined}
      className="flex w-64 max-w-[calc(100vw-32px)] flex-col overflow-hidden"
    >
      {/* A click here still bubbles through the portal to the composer, which
          answers any click by focusing its textarea, and the popover closes
          when focus leaves it. */}
      <Dialog
        aria-label={t('agents.pickWhere')}
        className="flex min-h-0 flex-col outline-hidden"
        onClick={(e) => e.stopPropagation()}
      >
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
            onAction={pick}
            autoFocus={searchable ? undefined : true}
            className="max-h-72 min-h-0 overflow-y-auto p-1 outline-hidden"
            renderEmptyState={() => (
              <p className="px-3 py-4 text-center text-[0.8125rem] text-(--color-text-tertiary)">
                {t('workspace.noWorkspacesFound')}
              </p>
            )}
          >
            {includeAll && (
              <MenuSection id="all" aria-label={t('agents.allWorkspaces')} className={SECTION}>
                <Row
                  id={ALL_WORKSPACES_KEY}
                  icon={Layers}
                  label={t('agents.allWorkspaces')}
                  hint={t('agents.allWorkspacesHint')}
                />
              </MenuSection>
            )}
            {workspaces.length > 0 && (
              <MenuSection id="workspaces" aria-label={t('workspace.workspaces')} className={SECTION}>
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
      </Dialog>
    </Popover>
  );
}

// A hairline between sections, drawn only above a section that follows another,
// so a search that empties the first section never leaves a rule on top. It
// runs edge to edge through the list's padding, as a menu separator does.
const SECTION = '[&+&]:-mx-1 [&+&]:mt-1 [&+&]:border-t [&+&]:border-(--color-border-muted) [&+&]:px-1 [&+&]:pt-1';

function Row({ id, icon: Icon, label, hint }: { id: string; icon: LucideIcon; label: string; hint?: string }) {
  return (
    <MenuItem
      id={id}
      textValue={label}
      className={cn(
        // The list's p-1 plus px-2 puts each icon under the search field's,
        // 12px in from the panel edge.
        'flex cursor-default select-none items-center gap-2 rounded-sm px-2 py-1.5 text-[0.8125rem] text-(--color-text-primary) outline-hidden',
        // The tint every floating list marks its row with (dropdown-menu's
        // ITEM_HIGHLIGHT). The current row takes the check alone, never the
        // tint, so the row under the pointer is the only one that is lit.
        'data-focused:bg-accent/15 data-hovered:bg-accent/15',
        'data-focus-visible:outline-current',
      )}
    >
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
