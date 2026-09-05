import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { Pin, Pencil, Cpu, Copy, Trash2, Infinity as InfinityIcon } from 'lucide-react';
import { DropdownMenuItem, DropdownMenuSeparator } from '@/components/ui/dropdown-menu';
import { toast } from '@/components/ui/use-toast';
import { isPlatformMode } from '@/config/hostMode';
import type { ResourceTier } from '@/types/api';
import { queryKeys } from '@/lib/queryKeys';
import {
  deleteWorkspace,
  setWorkspaceSpec,
  setWorkspaceAlwaysOn,
  duplicateWorkspace,
  getWorkspaceQuota,
  formatApiErrorDetail,
  apiErrorDetailMessage,
  apiErrorStatus,
} from '../utils/api';
import { useWorkspaceMutation } from '../hooks/useWorkspaceMutation';
import { forgetStableNavOrder } from '../hooks/useNavigationData';
import { forgetSharedWorkspaceThreads } from '@/lib/navThreadsStore';
import { removeStoredThreadId } from '../hooks/useChatMessages';
import { clearAllMarketThreadsForWorkspace } from '../../MarketView/utils/threadPersistence';
import { forgetNavPanelExpansion } from './navExpansionStore';
import { scrollMemory } from '@/lib/scrollMemory';
import ChangeSpecDialog, { tierLabel } from './ChangeSpecDialog';
import DeleteConfirmModal from './DeleteConfirmModal';
import DuplicateWorkspaceDialog from './DuplicateWorkspaceDialog';
import AlwaysOnConfirmDialog from './AlwaysOnConfirmDialog';

/** Minimal workspace shape the menu + actions need — both the gallery's richer
 *  record and the nav tree's loose entry satisfy it. */
export interface MenuWorkspace {
  workspace_id: string;
  name?: string;
  status?: string;
  is_pinned?: boolean;
  is_always_on?: boolean;
  /** Preselects the change-spec dialog's current tier. */
  resource_tier?: ResourceTier;
  [key: string]: unknown;
}

/** Map entitlement failures (403 plan-gate / 429 quota) to actionable copy in
 *  platform mode; generic API detail otherwise. Single source — the gallery
 *  imports this too. */
export function entitlementErrorMessage(
  err: unknown,
  t: ReturnType<typeof useTranslation>['t'],
  tier?: ResourceTier,
): string {
  if (isPlatformMode) {
    const status = apiErrorStatus(err);
    if (status === 403) {
      return t('workspace.notOnPlan', 'Not available on your plan — upgrade to unlock.');
    }
    if (status === 429) {
      // Prefer the platform's structured quota message when it forwards one
      // (detail: { message, type, current, limit, remaining }); fall back to
      // the localized generic copy otherwise.
      const platformMessage = apiErrorDetailMessage(err);
      if (platformMessage) return platformMessage;
      if (tier) {
        return t('workspace.tierLimitReached', "You've reached your {{tier}} workspace limit.", {
          tier: tierLabel(t, tier),
        });
      }
      return t('workspace.workspaceLimitReached', "You've reached your workspace limit.");
    }
  }
  return formatApiErrorDetail(err);
}

interface WorkspaceMenuItemsProps<W extends MenuWorkspace> {
  workspace: W;
  onTogglePin?: (workspace: W) => void;
  onRename?: (workspace: W) => void;
  onUpgrade: (workspace: W) => void;
  onToggleAlwaysOn: (workspace: W) => void;
  onDuplicate: (workspace: W) => void;
  onDelete: (workspace: W) => void;
}

/**
 * The canonical workspace options menu — identical everywhere a workspace can
 * be managed (gallery card, sidebar tree, mobile drawer). Render inside a
 * DropdownMenuContent.
 */
export function WorkspaceMenuItems<W extends MenuWorkspace>({
  workspace,
  onTogglePin,
  onRename,
  onUpgrade,
  onToggleAlwaysOn,
  onDuplicate,
  onDelete,
}: WorkspaceMenuItemsProps<W>) {
  const { t } = useTranslation();
  const isAlwaysOn = workspace.is_always_on === true;

  return (
    <>
      {onTogglePin && (
        <DropdownMenuItem onSelect={() => onTogglePin(workspace)}>
          <Pin className="h-4 w-4" />
          {workspace.is_pinned ? t('workspace.unpin') : t('workspace.pinToTop')}
        </DropdownMenuItem>
      )}
      {onRename && (
        <DropdownMenuItem onSelect={() => onRename(workspace)}>
          <Pencil className="h-4 w-4" />
          {t('workspace.rename')}
        </DropdownMenuItem>
      )}
      {(onTogglePin || onRename) && <DropdownMenuSeparator />}
      <DropdownMenuItem onSelect={() => onUpgrade(workspace)}>
        <Cpu className="h-4 w-4" />
        {t('workspace.changeSpec', 'Change spec')}
      </DropdownMenuItem>
      <DropdownMenuItem onSelect={() => onToggleAlwaysOn(workspace)}>
        <InfinityIcon className="h-4 w-4" />
        {isAlwaysOn
          ? t('workspace.alwaysOnDisable', 'Turn off always-on')
          : t('workspace.alwaysOnEnable', 'Turn on always-on')}
      </DropdownMenuItem>
      <DropdownMenuItem onSelect={() => onDuplicate(workspace)}>
        <Copy className="h-4 w-4" />
        {t('workspace.duplicate', 'Duplicate')}
      </DropdownMenuItem>
      <DropdownMenuSeparator />
      <DropdownMenuItem variant="destructive" onSelect={() => onDelete(workspace)}>
        <Trash2 className="h-4 w-4" />
        {t('common.delete', 'Delete')}
      </DropdownMenuItem>
    </>
  );
}

export interface WorkspaceActions {
  openUpgrade: (workspace: MenuWorkspace) => void;
  toggleAlwaysOn: (workspace: MenuWorkspace) => void;
  openDuplicate: (workspace: MenuWorkspace) => void;
  openDelete: (workspace: MenuWorkspace) => void;
  /** Render once at the host's root — the confirm/config dialogs. */
  dialogs: React.ReactNode;
}

/** Which flow just succeeded, so a host reacts to only the ones it cares about. */
export type WorkspaceMutationOp = 'spec' | 'always-on' | 'duplicate';

export interface UseWorkspaceActionsOptions {
  currentWorkspaceId?: string | null;
  /** After a successful CRUD flow. The gallery re-snaps to page 0 on 'duplicate' (the copy lands at the top). */
  onAfterMutate?: (op: WorkspaceMutationOp) => void;
  /** After a successful delete, with the removed id — a paginated host may need to step back a page. */
  onAfterDelete?: (wsId: string) => void;
}

/**
 * Self-contained change-spec / always-on / duplicate / delete actions with
 * their dialogs. The canonical implementation for every host (gallery card,
 * sidebar tree, mobile drawer): same mutations, entitlement mapping, toasts,
 * and delete cleanup; deleting the currently-open workspace navigates back to
 * the gallery. Host-specific presentation lands in the two callbacks.
 */
export function useWorkspaceActions({
  currentWorkspaceId,
  onAfterMutate,
  onAfterDelete,
}: UseWorkspaceActionsOptions): WorkspaceActions {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const [upgradeTarget, setUpgradeTarget] = useState<MenuWorkspace | null>(null);
  const [alwaysOnTarget, setAlwaysOnTarget] = useState<MenuWorkspace | null>(null);
  const [duplicateTarget, setDuplicateTarget] = useState<MenuWorkspace | null>(null);
  const [duplicateBusy, setDuplicateBusy] = useState(false);
  const [deleteTarget, setDeleteTarget] = useState<MenuWorkspace | null>(null);
  const [deleteBusy, setDeleteBusy] = useState(false);
  const [deleteError, setDeleteError] = useState<string | null>(null);

  const upgradeMutation = useWorkspaceMutation<ResourceTier>({
    mutationFn: (wsId, tier) => setWorkspaceSpec(wsId, tier),
    optimisticPatch: (tier) => ({ resource_tier: tier }),
    invalidateQuota: true,
    errorTitleKey: 'workspace.specFailed',
    mapError: (err, tier) => entitlementErrorMessage(err, t, tier),
  });
  const alwaysOnMutation = useWorkspaceMutation<boolean>({
    mutationFn: (wsId, next) => setWorkspaceAlwaysOn(wsId, next),
    optimisticPatch: (next) => ({ is_always_on: next }),
    invalidateQuota: true,
    errorTitleKey: 'workspace.alwaysOnFailed',
    mapError: (err) => entitlementErrorMessage(err, t),
  });

  // Per-tier count quotas for the change-spec dialog's "N left" hint.
  // Platform mode only, fetched lazily when the dialog opens; null in OSS mode.
  const { data: workspaceQuota } = useQuery({
    queryKey: queryKeys.workspaces.quota(),
    queryFn: getWorkspaceQuota,
    enabled: isPlatformMode && !!upgradeTarget,
    staleTime: 60_000,
  });

  const handleUpgradeSubmit = async (tier: ResourceTier) => {
    if (!upgradeTarget) return;
    const ok = await upgradeMutation.run(upgradeTarget.workspace_id, tier);
    if (ok) {
      setUpgradeTarget(null);
      toast({ title: t('workspace.specUpdated', 'Workspace spec updated'), description: tierLabel(t, tier) });
      onAfterMutate?.('spec');
    }
  };

  const applyAlwaysOn = async (workspace: MenuWorkspace, next: boolean) => {
    const ok = await alwaysOnMutation.run(workspace.workspace_id, next);
    if (ok) {
      setAlwaysOnTarget((cur) => (cur?.workspace_id === workspace.workspace_id ? null : cur));
      onAfterMutate?.('always-on');
    }
  };

  const toggleAlwaysOn = (workspace: MenuWorkspace) => {
    if (alwaysOnMutation.busyIds.has(workspace.workspace_id)) return;
    if (workspace.is_always_on === true) {
      void applyAlwaysOn(workspace, false);
    } else {
      setAlwaysOnTarget(workspace);
    }
  };

  const handleDuplicateConfirm = async () => {
    if (!duplicateTarget || duplicateBusy) return;
    setDuplicateBusy(true);
    try {
      await duplicateWorkspace(duplicateTarget.workspace_id);
      queryClient.invalidateQueries({ queryKey: queryKeys.workspaces.lists() });
      queryClient.invalidateQueries({ queryKey: queryKeys.workspaces.quota() });
      setDuplicateTarget(null);
      toast({ title: t('workspace.duplicated', 'Workspace duplicated') });
      onAfterMutate?.('duplicate');
    } catch (err) {
      console.error('Error duplicating workspace:', err);
      toast({ variant: 'destructive', title: t('workspace.duplicateFailed', 'Could not duplicate workspace'), description: entitlementErrorMessage(err, t) });
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
      await deleteWorkspace(wsId);
      // Same cleanup set as the gallery's delete: stored thread pointers
      // (chat + market), remembered tree expansion, frozen nav orders, and the
      // shared thread lists — so nothing re-expands or 404s a dead workspace.
      removeStoredThreadId(wsId);
      clearAllMarketThreadsForWorkspace(wsId);
      forgetNavPanelExpansion(wsId);
      forgetStableNavOrder(wsId);
      forgetSharedWorkspaceThreads(wsId);
      scrollMemory.forget(`threads:${wsId}:active`);
      scrollMemory.forget(`threads:${wsId}:archived`);
      queryClient.invalidateQueries({ queryKey: queryKeys.workspaces.lists() });
      onAfterDelete?.(wsId);
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
      <ChangeSpecDialog
        target={upgradeTarget}
        onClose={() => setUpgradeTarget(null)}
        onSubmit={(tier) => void handleUpgradeSubmit(tier)}
        busy={!!upgradeTarget && upgradeMutation.busyIds.has(upgradeTarget.workspace_id)}
        quota={workspaceQuota}
      />
      <AlwaysOnConfirmDialog
        target={alwaysOnTarget}
        onClose={() => setAlwaysOnTarget(null)}
        onConfirm={() => { if (alwaysOnTarget) void applyAlwaysOn(alwaysOnTarget, true); }}
        busy={!!alwaysOnTarget && alwaysOnMutation.busyIds.has(alwaysOnTarget.workspace_id)}
      />
      <DuplicateWorkspaceDialog
        target={duplicateTarget}
        onClose={() => setDuplicateTarget(null)}
        onConfirm={() => void handleDuplicateConfirm()}
        busy={duplicateBusy}
      />
      <DeleteConfirmModal
        isOpen={!!deleteTarget}
        workspaceName={deleteTarget?.name || ''}
        onConfirm={() => void handleConfirmDelete()}
        onCancel={() => { setDeleteTarget(null); setDeleteError(null); }}
        isDeleting={deleteBusy}
        error={deleteError}
      />
    </>
  );

  return {
    openUpgrade: setUpgradeTarget,
    toggleAlwaysOn,
    openDuplicate: setDuplicateTarget,
    openDelete: (ws) => { setDeleteTarget(ws); setDeleteError(null); },
    dialogs,
  };
}
