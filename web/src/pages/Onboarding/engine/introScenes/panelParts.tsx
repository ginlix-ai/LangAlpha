import type { CSSProperties, ReactNode } from 'react';
import {
  ChevronLeft,
  ChevronRight,
  Code2,
  Download,
  ExternalLink,
  Link2,
  FolderTree,
  Maximize2,
  PanelRight,
  Plus,
  Share2,
  type LucideIcon,
} from 'lucide-react';
import { cn } from '@/lib/utils';
import { PanelToggle } from './transcriptParts';

/* The workspace panel's own chrome, shared by every scene that opens it: the
   tab strip (`TabStrip`) with its back and forward, new tab, expand, tree
   toggle and the panel toggle in the corner; an open file's header
   (`FileCrumbs` with `FileHeaderActions`); and the chat header's share button
   and panel toggle (`ChatView`). Sizes are the app's at the scenes' scale: a
   28px button is 16px here, its 16px glyph 9px. */

/** An icon button as the panel draws one (`file-panel-icon-btn`): a quiet
    glyph on no fill; `pressed` is a toggle that is on (the panel toggle while
    the panel is open, the tree toggle while the tree shows); `disabled` is a
    step with nowhere to go. */
function PanelButton({
  icon: Icon,
  pressed = false,
  disabled = false,
}: {
  icon: LucideIcon;
  pressed?: boolean;
  disabled?: boolean;
}) {
  return (
    <span
      className="flex h-4 w-4 shrink-0 items-center justify-center rounded-[2.5px]"
      style={{
        background: pressed ? 'var(--color-bg-hover)' : undefined,
        color: pressed ? 'var(--color-text-primary)' : 'var(--color-text-tertiary)',
        opacity: disabled ? 0.3 : undefined,
      }}
    >
      <Icon className="h-[9px] w-[9px]" />
    </span>
  );
}

/** A tab in the panel's strip: the one in front sits on a raised pill, the
    rest are quiet text. A file opened on loan (`onLoan`, the preview a next
    open takes over) is set in italics, as the strip marks it. The close
    button only shows under the pointer, so a tab at rest ends in the room it
    keeps for it. */
export function PanelTab({
  icon: Icon,
  label,
  active = false,
  onLoan = false,
}: {
  icon: LucideIcon;
  label: string;
  active?: boolean;
  onLoan?: boolean;
}) {
  return (
    <span
      className={cn(
        'flex h-4 max-w-[100px] shrink-0 items-center gap-[3.5px] rounded-[4.5px] border pl-[4.5px] pr-2',
        'text-[7px] leading-none'
      )}
      style={
        active
          ? {
              background: 'var(--color-bg-elevated)',
              borderColor: 'var(--color-border-muted)',
              color: 'var(--color-text-primary)',
            }
          : { borderColor: 'transparent', color: 'var(--color-text-tertiary)' }
      }
    >
      <Icon className="h-2 w-2 shrink-0" />
      <span className={cn('truncate', onLoan && 'italic')}>{label}</span>
    </span>
  );
}

/**
 * The panel's tab strip: its `tabs`, the new tab button against the last of
 * them, then back and forward for the tab in front (`back` when it has a
 * step to go back to; nothing in a scene ever has one forward), expand, the
 * tree toggle where the tab in front is a file (`tree`, pressed while the
 * tree shows beside it), and the panel toggle, pressed, in the corner the
 * chat header keeps it in while the panel is shut.
 */
export function PanelStrip({
  tabs,
  back = false,
  tree,
  className,
  style,
}: {
  tabs: ReactNode;
  back?: boolean;
  tree?: boolean;
  className?: string;
  style?: CSSProperties;
}) {
  return (
    <div
      className={cn('flex shrink-0 items-center gap-[2.5px] px-[4.5px]', className)}
      style={style}
    >
      <span className="flex min-w-0 items-center gap-px overflow-hidden">{tabs}</span>
      <PanelButton icon={Plus} />
      <span className="ml-auto flex shrink-0 items-center gap-px">
        <PanelButton icon={ChevronLeft} disabled={!back} />
        <PanelButton icon={ChevronRight} disabled />
        <PanelButton icon={Maximize2} />
        {tree != null && <PanelButton icon={FolderTree} pressed={tree} />}
        <PanelButton icon={PanelRight} pressed />
      </span>
    </div>
  );
}

/** The buttons on an HTML file's header, in the app's order
    (`FileHeaderActions`): source, share link, open in a new tab, download. */
// eslint-disable-next-line react-refresh/only-export-components -- the buttons travel with the header that draws them
export const HTML_FILE_ACTIONS: LucideIcon[] = [Code2, Link2, ExternalLink, Download];

/** An open file's header (`FileCrumbs`): the path, one segment per folder,
    then how long the file is where it is text, and the file's `actions`. */
export function FileCrumbs({
  path,
  meta,
  actions,
}: {
  path: string[];
  meta?: string;
  actions: LucideIcon[];
}) {
  return (
    <div
      className="flex min-h-[19px] shrink-0 items-center gap-[4.5px] border-b pl-[7px] pr-[4.5px]"
      style={{ borderColor: 'var(--color-border-muted)' }}
    >
      <span className="flex min-w-0 items-center gap-px text-[6.5px] leading-none">
        {path.map((seg, i) => {
          const file = i === path.length - 1;
          return (
            <span key={seg} className="flex min-w-0 items-center gap-px">
              <span
                className="truncate px-[1.5px]"
                style={{ color: file ? 'var(--color-text-primary)' : 'var(--color-text-tertiary)' }}
              >
                {seg}
              </span>
              {!file && (
                <ChevronRight
                  className="h-[7px] w-[7px] shrink-0"
                  style={{ color: 'var(--color-icon-muted)' }}
                />
              )}
            </span>
          );
        })}
      </span>
      <span className="ml-auto" />
      {meta && (
        <span
          className="shrink-0 whitespace-nowrap text-[6.5px] leading-none"
          style={{ color: 'var(--color-text-tertiary)' }}
        >
          {meta}
        </span>
      )}
      {actions.map((icon, i) => (
        <PanelButton key={i} icon={icon} />
      ))}
    </div>
  );
}

/** The chat header's own buttons (`ChatView`): share, then the panel toggle
    while the panel is shut. Open, the panel carries the toggle in the same
    corner, so the header keeps only share. Both are in the header's ink;
    share takes the accent once the conversation is shared (`shared`), and
    the hover fill while its popover is open (`sharing`). */
export function ChatHeaderButtons({
  panelOpen,
  sharing = false,
  shared = false,
}: {
  panelOpen: boolean;
  sharing?: boolean;
  shared?: boolean;
}) {
  return (
    <span className="flex shrink-0 items-center gap-[4.5px]">
      <span
        className="flex h-4 w-4 items-center justify-center rounded-[2.5px]"
        style={{
          color: shared ? 'var(--color-accent-primary)' : 'var(--color-text-primary)',
          background: sharing ? 'var(--color-border-muted)' : undefined,
        }}
      >
        <Share2 className="h-[9px] w-[9px]" />
      </span>
      {!panelOpen && <PanelToggle />}
    </span>
  );
}
