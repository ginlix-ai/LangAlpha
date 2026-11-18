/**
 * What the nav tree's rows add for a shared workspace: who last wrote in an
 * unread thread (`colleagueName` in lib/sharedThreads), the mark of a thread
 * shared with the workspace, and the workspace actions a member is offered
 * there.
 * The rows themselves are the tree's own (./NavigationRows).
 */
import { useTranslation } from 'react-i18next';
import { Pencil, Users } from 'lucide-react';
import { DropdownMenuItem, DropdownMenuSeparator } from '@/components/ui/dropdown-menu';
import { ContextMenuItem, ContextMenuSeparator } from '@/components/ui/context-menu';
import { useThreadUnseen } from '@/lib/threadLifecycle/store';

/** A shared workspace's row, as the viewer stands in it. */
export interface SharedNavRow {
  /** A manager, or any member while no manager is active. */
  canManage: boolean;
  /** A thread there the viewer has not read, as their shared list last said. */
  hasUnread: boolean;
  /** Opens the people panel, which holds what a member may do to the workspace. */
  onOpenPeople: (workspaceId: string) => void;
}

/** What trails a shared workspace's thread title: the colleague who wrote
 *  last, on an unread row only, since the name says who wrote what the viewer
 *  has not seen; then the people mark on a thread shared with the workspace.
 *  A thread kept private carries no mark, as a thread in one's own workspace
 *  carries none. */
export function SharedThreadTrailing({ tid, isCurrentThread, name, sharedMark }: {
  tid: string;
  isCurrentThread: boolean;
  name: string | null | undefined;
  sharedMark: boolean;
}) {
  const { t } = useTranslation();
  const unseen = useThreadUnseen(tid);
  const sender = name && unseen && !isCurrentThread ? name : null;
  if (!sender && !sharedMark) return null;
  return (
    <span className="ml-auto flex shrink-0 items-center gap-1.5 text-xs transition-opacity group-hover:opacity-0" style={{ color: 'var(--color-text-tertiary)' }}>
      {sender}
      {sharedMark && (
        <Users aria-label={t('sharedWorkspace.privacy.sharedMark')} role="img" className="h-3 w-3" />
      )}
    </span>
  );
}

/** A shared workspace's options: rename, to whoever may manage it, and its
 *  people, where leaving it, and for a manager deleting it, are offered under
 *  the rules that panel applies. */
export function SharedWorkspaceMenuItems({ menu, onRename, onOpenPeople }: {
  menu: 'dropdown' | 'context';
  onRename?: () => void;
  onOpenPeople: () => void;
}) {
  const { t } = useTranslation();
  const [Item, Separator] = menu === 'context'
    ? [ContextMenuItem, ContextMenuSeparator]
    : [DropdownMenuItem, DropdownMenuSeparator];
  return (
    <>
      {onRename && (
        <>
          <Item onSelect={onRename}>
            <Pencil className="h-4 w-4" />
            {t('workspace.rename')}
          </Item>
          <Separator />
        </>
      )}
      <Item onSelect={onOpenPeople}>
        <Users className="h-4 w-4" />
        {t('sharedWorkspace.members.open')}
      </Item>
    </>
  );
}
