import React, { Suspense, useMemo, useState } from 'react';
import { useNavigate } from 'react-router';
import { useTranslation } from 'react-i18next';
import { useQueryClient } from '@tanstack/react-query';
import { Pin, Pencil, Copy, FolderOutput, Trash2 } from 'lucide-react';
import { DropdownMenuItem, DropdownMenuSeparator } from '@/components/ui/dropdown-menu';
import { ContextMenuItem, ContextMenuSeparator } from '@/components/ui/context-menu';
import { toast } from '@/components/ui/use-toast';
import { queryKeys } from '@/lib/queryKeys';
import type { Computer } from '@/types/api';
import { deleteWorkspace, duplicateWorkspace } from '../utils/api';
import { entitlementErrorMessage } from '../utils/entitlementErrors';
import { workspaceNameErrorMessage } from '../utils/workspaceName';
import { forgetDeletedWorkspace, invalidateNewWorkspace, invalidateWorkspaceMembership } from '../hooks/workspaceRowActions';
import { useComputers } from '../hooks/useComputers';
import { liveComputers, takesWorkspaces } from '../utils/computerStatus';
import { providerKeepsFilesOnRemove } from './providerUi';
import { lazyWithMotion } from '@/lib/lazyWithMotion';
import DeleteConfirmModal, { type OwnMachineFolder } from './DeleteConfirmModal';
import DuplicateWorkspaceDialog from './DuplicateWorkspaceDialog';
import { WorkspaceMoveOutcomes } from './WorkspaceMoveOutcomes';

// Lazy: the sidebar mounts these actions on every page, and the dialog brings
// framer, which stays off the first load.
const MoveWorkspaceDialog = lazyWithMotion(() => import('./MoveWorkspaceDialog'));

/** Minimal workspace shape the menu + actions need — both the gallery's richer
 *  record and the nav tree's loose entry satisfy it. */
export interface MenuWorkspace {
  workspace_id: string;
  name?: string;
  status?: string;
  is_pinned?: boolean;
  is_home?: boolean;
  computer_id?: string | null;
  dir_name?: string | null;
  [key: string]: unknown;
}

// The same items under either menu: the options button's dropdown or the
// row's right-click menu. Radix ties an item to its own menu's context.
const MENU_PARTS = {
  dropdown: { Item: DropdownMenuItem, Separator: DropdownMenuSeparator },
  context: { Item: ContextMenuItem, Separator: ContextMenuSeparator },
} as const;

interface WorkspaceMenuItemsProps<W extends MenuWorkspace> {
  workspace: W;
  /** Which menu renders them. Defaults to a DropdownMenuContent. */
  menu?: keyof typeof MENU_PARTS;
  onTogglePin?: (workspace: W) => void;
  onRename?: (workspace: W) => void;
  onDuplicate: (workspace: W) => void;
  /** Absent where moving isn't offered; shown only when there is somewhere to move to. */
  onMove?: (workspace: W) => void;
  onDelete: (workspace: W) => void;
}

/**
 * The canonical workspace options menu — identical everywhere a workspace can
 * be managed (gallery card, sidebar tree, mobile drawer). Render inside a
 * DropdownMenuContent, or a ContextMenuContent with `menu="context"`. Spec and always-on are the computer's, so they live in
 * the Computers dialog, not here.
 */
export function WorkspaceMenuItems<W extends MenuWorkspace>({
  workspace,
  menu = 'dropdown',
  onTogglePin,
  onRename,
  onDuplicate,
  onMove,
  onDelete,
}: WorkspaceMenuItemsProps<W>) {
  const { t } = useTranslation();
  // Read from the list the fan-out already holds. The menu mounts only while
  // open, so a page without that list fetches it on the first open alone.
  const { data: computerData } = useComputers({ enabled: !!onMove });
  // Home moves only with the primary, and a row with no computer has no
  // folder to move. Two machines that take workspaces always leave one that
  // is not this workspace's.
  const canMove =
    !workspace.is_home
    && !!workspace.computer_id
    && liveComputers(computerData?.computers).filter(takesWorkspaces).length >= 2;
  const { Item, Separator } = MENU_PARTS[menu];

  return (
    <>
      {onTogglePin && (
        <Item onSelect={() => onTogglePin(workspace)}>
          <Pin className="h-4 w-4" />
          {workspace.is_pinned ? t('workspace.unpin') : t('workspace.pinToTop')}
        </Item>
      )}
      {onRename && (
        <Item onSelect={() => onRename(workspace)}>
          <Pencil className="h-4 w-4" />
          {t('workspace.rename')}
        </Item>
      )}
      {(onTogglePin || onRename) && <Separator />}
      <Item onSelect={() => onDuplicate(workspace)}>
        <Copy className="h-4 w-4" />
        {t('workspace.duplicate', 'Duplicate')}
      </Item>
      {onMove && canMove && (
        <Item onSelect={() => onMove(workspace)}>
          <FolderOutput className="h-4 w-4" />
          {t('workspace.move.menu', 'Move to computer…')}
        </Item>
      )}
      <Separator />
      <Item variant="destructive" onSelect={() => onDelete(workspace)}>
        <Trash2 className="h-4 w-4" />
        {t('common.delete', 'Delete')}
      </Item>
    </>
  );
}

/** Where a workspace's files are on a machine the user brought; null on every other computer. */
function ownMachineFolder(
  workspace: MenuWorkspace | null,
  computers: readonly Computer[] | undefined,
): OwnMachineFolder | null {
  const computer = workspace?.computer_id
    ? computers?.find((c) => c.computer_id === workspace.computer_id)
    : undefined;
  if (!computer || !workspace?.dir_name || !providerKeepsFilesOnRemove(computer.kind)) return null;
  return { folder: `${computer.root_dir.replace(/\/+$/, '')}/${workspace.dir_name}`, computer: computer.name };
}

export interface WorkspaceActions {
  openDuplicate: (workspace: MenuWorkspace) => void;
  openMove: (workspace: MenuWorkspace) => void;
  openDelete: (workspace: MenuWorkspace) => void;
  /** Render once at the host's root — the confirm/config dialogs. */
  dialogs: React.ReactNode;
}

/** Which flow just succeeded, so a host reacts to only the ones it cares about. */
export type WorkspaceMutationOp = 'duplicate';

export interface UseWorkspaceActionsOptions {
  currentWorkspaceId?: string | null;
  /** After a successful CRUD flow. The gallery scrolls to its top on 'duplicate' (the copy lands there). */
  onAfterMutate?: (op: WorkspaceMutationOp) => void;
}

/**
 * Self-contained duplicate / move / delete actions with their dialogs. The canonical implementation for every host (gallery card,
 * sidebar tree, mobile drawer): same mutations, entitlement mapping, toasts,
 * and delete cleanup; deleting the currently-open workspace navigates back to
 * the gallery. Host-specific presentation lands in the callback.
 */
export function useWorkspaceActions({
  currentWorkspaceId,
  onAfterMutate,
}: UseWorkspaceActionsOptions): WorkspaceActions {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const [duplicateTarget, setDuplicateTarget] = useState<MenuWorkspace | null>(null);
  const [duplicateBusy, setDuplicateBusy] = useState(false);
  const [moveTarget, setMoveTarget] = useState<MenuWorkspace | null>(null);
  // Latched once opened, so closing animates out rather than unmounting.
  const [moveNeeded, setMoveNeeded] = useState(false);
  if (moveTarget && !moveNeeded) setMoveNeeded(true);
  const [deleteTarget, setDeleteTarget] = useState<MenuWorkspace | null>(null);
  const [deleteBusy, setDeleteBusy] = useState(false);
  const [deleteError, setDeleteError] = useState<string | null>(null);
  const [deleteFiles, setDeleteFiles] = useState(false);
  // The list is usually cached already; the dialog only reads it. Until it
  // answers, the dialog waits: it would word a machine the user brought as a
  // hosted one, whose folder goes, and send the delete that keeps it. A list
  // that fails to load gets the general warning.
  const computerList = useComputers({ enabled: !!deleteTarget?.computer_id });
  const deleteOwnMachine = ownMachineFolder(deleteTarget, computerList.data?.computers);
  const deleteWaiting = !!deleteTarget?.computer_id && computerList.isPending;

  const handleDuplicateConfirm = async () => {
    if (!duplicateTarget || duplicateBusy) return;
    setDuplicateBusy(true);
    try {
      await duplicateWorkspace(duplicateTarget.workspace_id);
      invalidateNewWorkspace(queryClient);
      queryClient.invalidateQueries({ queryKey: queryKeys.workspaces.quota() });
      setDuplicateTarget(null);
      toast({ title: t('workspace.duplicated', 'Workspace duplicated') });
      onAfterMutate?.('duplicate');
    } catch (err) {
      console.error('Error duplicating workspace:', err);
      toast({
        variant: 'destructive',
        title: t('workspace.duplicateFailed', 'Could not duplicate workspace'),
        description: workspaceNameErrorMessage(err, t, 'duplicate') ?? entitlementErrorMessage(err, t),
      });
    } finally {
      setDuplicateBusy(false);
    }
  };

  const handleConfirmDelete = async () => {
    if (!deleteTarget) return;
    const wsId = deleteTarget.workspace_id;
    setDeleteBusy(true);
    setDeleteError(null);
    try {
      // Only a choice the dialog offered: elsewhere the server deletes the folder anyway.
      await deleteWorkspace(wsId, { deleteFiles: !!deleteOwnMachine && deleteFiles });
      // Gone from every list now; the refetch then confirms it.
      forgetDeletedWorkspace(queryClient, wsId);
      invalidateWorkspaceMembership(queryClient);
      // The breakdown drops a deleted project's folder; only an open one refetches.
      if (typeof deleteTarget.computer_id === 'string') {
        void queryClient.invalidateQueries({ queryKey: queryKeys.computers.storage(deleteTarget.computer_id) });
      }
      if (currentWorkspaceId === wsId) {
        navigate('/chat');
      }
      setDeleteTarget(null);
    } catch (err) {
      console.error('Error deleting workspace:', err);
      setDeleteError(err instanceof Error && err.message ? err.message : t('workspace.failedDeleteWorkspace'));
    } finally {
      setDeleteBusy(false);
    }
  };

  const dialogs = (
    <>
      <DuplicateWorkspaceDialog
        target={duplicateTarget}
        onClose={() => setDuplicateTarget(null)}
        onConfirm={() => void handleDuplicateConfirm()}
        busy={duplicateBusy}
      />
      {moveNeeded && (
        <Suspense fallback={null}>
          <MoveWorkspaceDialog target={moveTarget} onClose={() => setMoveTarget(null)} />
        </Suspense>
      )}
      {/* Every host carries it, so an ending is told wherever the user is;
          each one is handed out once however many are mounted. */}
      <WorkspaceMoveOutcomes />
      <DeleteConfirmModal
        isOpen={!!deleteTarget && !deleteWaiting}
        workspaceName={deleteTarget?.name || ''}
        onConfirm={() => void handleConfirmDelete()}
        onCancel={() => { setDeleteTarget(null); setDeleteError(null); }}
        isDeleting={deleteBusy}
        error={deleteError}
        ownMachine={deleteOwnMachine}
        deleteFiles={deleteFiles}
        onDeleteFilesChange={setDeleteFiles}
      />
    </>
  );

  // The sidebar rows that receive these are memoized on identity: hand out
  // one stable set.
  const actions = useMemo<Omit<WorkspaceActions, 'dialogs'>>(() => ({
    openDuplicate: setDuplicateTarget,
    openMove: setMoveTarget,
    openDelete: (ws) => { setDeleteTarget(ws); setDeleteError(null); setDeleteFiles(false); },
  }), []);

  return { ...actions, dialogs };
}
