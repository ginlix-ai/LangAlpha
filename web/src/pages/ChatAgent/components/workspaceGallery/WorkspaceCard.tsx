import type { KeyboardEvent } from 'react';
import { MoreHorizontal, Pin } from 'lucide-react';

import { useTranslation } from 'react-i18next';

import { DropdownMenu, DropdownMenuContent, DropdownMenuTrigger } from '@/components/ui/dropdown-menu';
import { SkeletonBar } from '@/components/ui/skeleton';
import { useWorkspaceText } from '@/hooks/useWorkspaceLabel';
import { useIsMobile } from '@/hooks/useIsMobile';
import { useLocale } from '@/hooks/useLocale';
import { relativeTime } from '@/lib/format';
import type { Computer } from '@/types/api';

import { WorkspaceMenuItems } from '../workspaceActions';
import type { CardDrag } from './CardGrid';
import { MachineSummary } from './MachineSummary';
import type { WorkspaceRecord } from './types';

interface CardMenuProps {
  workspace: WorkspaceRecord;
  onTogglePin: (workspace: WorkspaceRecord) => void;
  onRename: (workspace: WorkspaceRecord) => void;
  onDuplicate: (workspace: WorkspaceRecord) => void;
  onMove?: (workspace: WorkspaceRecord) => void;
  onDelete: (workspace: WorkspaceRecord) => void;
}

/** Card menu dropdown (Pin / Rename / Duplicate / Move / Delete). */
function CardMenu({ workspace, onTogglePin, onRename, onDuplicate, onMove, onDelete }: CardMenuProps) {
  const { t } = useTranslation();
  return (
    <DropdownMenu modal={false}>
      <DropdownMenuTrigger asChild>
        <button
          onPointerDown={(e) => e.stopPropagation()}
          onClick={(e) => e.stopPropagation()}
          className="h-7 w-7 rounded-md transition-colors flex items-center justify-center hover:bg-(--color-border-muted) pointer-coarse:h-8 pointer-coarse:w-8"
          style={{ color: 'var(--color-text-tertiary)' }}
          aria-label={t('workspace.options')}
          data-testid="workspace-card-menu"
        >
          <MoreHorizontal className="h-4 w-4" />
        </button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" sideOffset={4}>
        <WorkspaceMenuItems
          workspace={workspace}
          onTogglePin={onTogglePin}
          onRename={onRename}
          onDuplicate={onDuplicate}
          onMove={onMove}
          onDelete={onDelete}
        />
      </DropdownMenuContent>
    </DropdownMenu>
  );
}

interface WorkspaceCardProps {
  workspace: WorkspaceRecord;
  /** The machine this workspace lives on, given only when the cards sit on
   *  more than one: otherwise every card would name the same machine. */
  computer?: Computer | null;
  /** Whether that machine is the Mac this desktop app runs on. */
  isThisMac?: boolean;
  /** The clock the relative times read from, one for the whole grid. */
  now: number;
  onSelect: (wsId: string, name?: string, status?: string) => void;
  onTogglePin: (workspace: WorkspaceRecord) => void;
  onRenameStart: (workspace: WorkspaceRecord) => void;
  /** Open the Computers dialog on this card's machine. */
  onOpenComputer?: (computerId: string) => void;
  onDuplicate: (workspace: WorkspaceRecord) => void;
  onMove?: (workspace: WorkspaceRecord) => void;
  onDelete: (workspace: WorkspaceRecord) => void;
  prefetchThreads?: (wsId: string) => void;
  /** From the grid: whether and by what the card drags, and how it is drawn mid-drag. */
  drag?: CardDrag;
}

const RESTING: CardDrag = { state: 'resting', dragging: false, held: false };

// The card's box, shared with its skeleton so a placeholder is the card's size.
const CARD_BODY = 'relative flex h-full w-full flex-col gap-1.5 overflow-hidden rounded-xl py-3.5 pl-[18px] pr-4 md:px-5 md:py-4';
const CARD_SURFACE = {
  background: 'var(--color-bg-card-gradient, linear-gradient(to bottom, var(--color-border-muted), var(--color-border-muted)))',
  border: '1px solid var(--lift-edge, var(--color-bg-card-border, var(--color-border-muted)))',
} as const;
// Only the lifted copy blurs what is behind it: it passes over other cards,
// seen through its translucent fill. A resting card has the flat page behind
// it, and a backdrop filter is a compositor surface per card, which Electron
// on macOS drew stale over a dialog open above the gallery.
const LIFTED_SURFACE = { ...CARD_SURFACE, backdropFilter: 'blur(8px)', WebkitBackdropFilter: 'blur(8px)' } as const;

/**
 * A workspace in the gallery grid: its name and when it was last used, its
 * description, and the machine it lives on when there is more than one. Home
 * is not a card; the gallery draws it as the strip above the grid.
 */
export function WorkspaceCard({
  workspace,
  computer,
  isThisMac = false,
  now,
  onSelect,
  onTogglePin,
  onRenameStart,
  onOpenComputer,
  onDuplicate,
  onMove,
  onDelete,
  prefetchThreads,
  drag = RESTING,
}: WorkspaceCardProps) {
  const locale = useLocale();
  const isMobile = useIsMobile();
  const text = useWorkspaceText()(workspace);
  // When it was last used, the time the Recent order sorts on, whatever order
  // is picked: `updated_at` alone moves on a rename, pin or reorder. An
  // unreadable timestamp reads as no timestamp, not as "now".
  const when = relativeTime(workspace.last_activity_at ?? workspace.updated_at, locale, now);

  const menu = (
    <CardMenu
      workspace={workspace}
      onTogglePin={onTogglePin}
      onRename={onRenameStart}
      onDuplicate={onDuplicate}
      onMove={onMove}
      onDelete={onDelete}
    />
  );

  const open = () => onSelect(workspace.workspace_id, text.name, workspace.status);
  const { handle, state, dragging } = drag;
  const lifted = state === 'lifted';
  const placeholder = state === 'placeholder';

  // Enter opens the card, as it opens a link. Space is the drag's when the
  // card can drag (the sensor's own handler runs first and takes it), and
  // opens the card when it cannot, as it presses any button. A card held
  // still for a moment (a drop saving, the list being read) can still drag,
  // so Space stays the drag's and does nothing: a keyboard user who drops
  // one card and reaches for the next must not open it instead.
  const onKeyDown = (e: KeyboardEvent<HTMLDivElement>) => {
    handle?.props.onKeyDown?.(e);
    if (e.defaultPrevented || e.target !== e.currentTarget || drag.dragging) return;
    if (e.key === 'Enter' || (!handle && !drag.held && e.key === ' ')) {
      e.preventDefault();
      open();
    } else if (drag.held && e.key === ' ') {
      e.preventDefault();
    }
  };

  return (
    <div
      className="relative group h-full"
      data-testid={lifted ? 'workspace-card-lifted' : 'workspace-card'}
      data-drag-state={state === 'resting' ? undefined : state}
      onMouseEnter={!isMobile ? () => prefetchThreads?.(workspace.workspace_id) : undefined}
    >
      {/* What rises under the pointer: the card and its menu together, so the
          menu stays in its corner and a pointer on it keeps the card up.
          Mid-drag the cards hold still; only the lifted copy moves. */}
      <div className={`relative h-full rounded-xl${state === 'resting' && !dragging ? ' hover-lift' : ''}`}>
        <div
          role="button"
          tabIndex={0}
          aria-label={text.name}
          {...handle?.props}
          ref={handle?.ref}
          onKeyDown={onKeyDown}
          onClick={open}
          className={[
            CARD_BODY,
            lifted
              ? 'cursor-grabbing scale-[1.02] shadow-xl'
              : 'cursor-pointer press',
            // A held finger lifts the card, so it must not also select its text
            // or raise the system's callout.
            handle ? 'touch-manipulation select-none [-webkit-touch-callout:none]' : '',
            // The slot the lifted card left: its outline, and nothing in it.
            placeholder ? '[&>*]:invisible' : '',
          ].filter(Boolean).join(' ')}
          style={placeholder ? {
            background: 'transparent',
            border: '1.5px dashed var(--color-border-default)',
          } : lifted ? LIFTED_SURFACE : CARD_SURFACE}
        >
          {/* Where the menu always shows (a touch screen), the row leaves it its
              corner and the time sits clear of it. */}
          <div className="flex items-start gap-2 min-w-0 pointer-coarse:pr-9">
            {workspace.is_pinned && (
              <Pin className="mt-1 h-3.5 w-3.5 shrink-0 rotate-45" style={{ color: 'var(--color-text-tertiary)' }} />
            )}
            {/* One line at every width, so every card is the same height. */}
            <div className="font-medium flex-1 min-w-0 line-clamp-1 break-words" style={{ color: 'var(--color-text-primary)' }} title={text.name}>
              {text.name}
            </div>
            {when && (
              // Under a mouse the menu takes this corner on hover, so the time
              // steps aside rather than sitting under it.
              <span
                className={`shrink-0 pt-0.5 text-xs whitespace-nowrap transition-opacity duration-quick${state === 'resting' ? ' pointer-fine:group-hover:opacity-0 pointer-fine:group-focus-within:opacity-0' : ''}`}
                style={{ color: 'var(--color-text-tertiary)' }}
              >
                {when}
              </span>
            )}
          </div>
          {/* The description always holds two lines, filled or not, so every
              card is the same height and the machine line sits level. */}
          <div className="text-sm line-clamp-2 min-h-[2lh]" style={{ color: 'var(--color-text-tertiary)' }}>
            {text.description}
          </div>
          {computer && (
            <div className="mt-auto pt-2.5">
              <MachineSummary computer={computer} isThisMac={isThisMac} onOpen={onOpenComputer} />
            </div>
          )}
        </div>

        {/* Outside the clickable card, and so outside the drag handle: a menu
            item's click would otherwise bubble (through the portal) into opening
            the workspace, and a press on the button into a drag. Under a mouse
            it shows on hover; on a touch screen it stays, at the far right. */}
        {state === 'resting' && (
          <div className="absolute top-2.5 right-3 z-10 transition-opacity opacity-0 group-focus-within:opacity-100 group-hover:opacity-100 pointer-coarse:top-2 pointer-coarse:right-2 pointer-coarse:opacity-100">
            {menu}
          </div>
        )}
      </div>
    </div>
  );
}

/**
 * A card's place while the list is read: the card's box with its name and the
 * two description lines it always holds, so the grid it stands in is the
 * grid the cards land in.
 */
export function WorkspaceCardSkeleton({ index = 0 }: { index?: number }) {
  return (
    <div className={CARD_BODY} style={CARD_SURFACE} data-testid="workspace-card-skeleton" aria-hidden="true">
      <div className="font-medium">
        <SkeletonBar style={{ width: `${36 + (index % 3) * 12}%` }} />
      </div>
      <div className="text-sm min-h-[2lh]">
        <SkeletonBar className="w-11/12" />
        <br />
        <SkeletonBar style={{ width: `${45 + ((index + 1) % 3) * 15}%` }} />
      </div>
    </div>
  );
}

export default WorkspaceCard;
