/**
 * The machine-level mutations: create, start and stop, rename, change spec,
 * make primary, turning always-on off, remove.
 *
 * These used to hang off a workspace's menu, but tier and always-on are the
 * computer's, and every workspace on it changes with them. They are addressed
 * by computer id here, and every success refreshes both the machine rows and
 * the workspace rows, which repeat the machine's values.
 */
import { useCallback } from 'react';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';

import { toast } from '@/components/ui/use-toast';
import { queryKeys } from '@/lib/queryKeys';
import type {
  Computer,
  ComputerCreate,
  ComputerReconnectTarget,
  ComputerReinstall,
  ComputerRemoveResponse,
  ComputersResponse,
  ResourceTier,
} from '@/types/api';

import {
  createComputer,
  deleteComputer,
  makeComputerPrimary,
  reconnectComputer,
  reinstallComputer,
  renameComputer,
  setComputerAlwaysOn,
  setComputerFilesBackup,
  setComputerSpec,
  startComputer,
  stopComputer,
} from '../utils/api';
import { apiErrorObject, apiErrorReason } from '@/api/errors';
import { entitlementErrorMessage } from '../utils/entitlementErrors';
import { releaseRemoteControl } from './useRemoteControl';
import { nameTakenBy } from '../utils/computerName';
import { tierLabel } from '../components/tierUi';
import { closeComputerSpec } from './computerPanelStore';
import {
  addComputerRow,
  CREATE_COMPUTER_MUTATION_KEY,
  invalidateMachine,
  invalidateMachineStorage,
  patchComputerRow,
  patchComputerStatusInCaches,
  RECONNECT_COMPUTER_MUTATION_KEY,
  removeComputerRow,
} from './useComputers';
import { seedWorkspaceMove, watchMove } from './useWorkspaceMove';
import { forgetDeletedWorkspace, invalidateWorkspaceMembership } from './workspaceRowActions';

export type PowerAction = 'start' | 'stop';

export interface RemoveComputerVariables {
  computerId: string;
  name: string;
  removeKey: boolean;
  /** The workspaces on it the user was shown, deleted before it is removed. */
  workspaceIds: readonly string[];
  /** Also delete LangAlpha's files from a machine the user brought. */
  deleteFiles: boolean;
  /** Its root folder, named when the files' outcome is told. */
  folder?: string | null;
  fingerprint?: string | null;
}

type Translate = ReturnType<typeof useTranslation>['t'];

export interface ReconnectComputerVariables {
  computerId: string;
  ssh: ComputerReconnectTarget;
  /** What the pairing found there: the host key's fingerprint, and whether a helper answered. */
  fingerprint: string;
  helper: boolean;
}

/** The row's `ssh` once it is reached at `target`, ahead of the server's next read of it. */
function reconnectedSsh(row: Computer, { ssh: target, fingerprint, helper }: ReconnectComputerVariables): Computer['ssh'] {
  if (!row.ssh) return row.ssh;
  const tunnel = !!target.tunnel_id;
  return {
    ...row.ssh,
    host: target.host,
    port: target.port,
    username: target.username,
    fingerprint,
    helper,
    tunnel,
    tunnel_connected: tunnel ? true : null,
  };
}

/** What became of LangAlpha's files on the machine; undefined when they were not asked to go. */
function filesRemovalNote(
  files: ComputerRemoveResponse['files'],
  folder: string | null | undefined,
  t: Translate,
): string | undefined {
  if (!files) return undefined;
  if (files.outcome === 'unreached') {
    return t('computer.filesUnreached', 'We could not reach the machine, so its files are still there.');
  }
  if (files.outcome === 'failed') {
    return t('computer.filesFailed', 'We could not finish deleting its files from the machine.');
  }
  return [
    t('computer.filesDeleted', 'Its files were deleted from the machine.'),
    files.root_kept && folder
      ? t('computer.filesRootKept', '{{folder}} stays, since it holds files of yours.', { folder })
      : null,
    files.runtime_kept ? t('computer.filesRuntimeKept', '~/.langalpha stays, since another computer uses it.') : null,
  ].filter(Boolean).join(' ');
}

/** What became of our key line, in a sentence or two; undefined when there is nothing to say. */
function keyRemovalNote(key: ComputerRemoveResponse['key'] | undefined, t: Translate): string | undefined {
  if (!key) return undefined;
  const byHand =
    key.comment && key.fingerprint
      ? ` ${t(
          'computer.keyRemoveYourself',
          'To remove it yourself, delete the line ending in {{comment}} from ~/.ssh/authorized_keys. Its fingerprint is {{fingerprint}}.',
          { comment: key.comment, fingerprint: key.fingerprint },
        )}`
      : '';
  switch (key.outcome) {
    case 'removed':
      return t('computer.keyRemoved', 'We also removed our key from ~/.ssh/authorized_keys.');
    case 'kept':
      return t('computer.keyKept', 'Our key stays in ~/.ssh/authorized_keys, since {{computer}} still logs in with it.', {
        computer: key.used_by ?? '',
      });
    case 'skipped':
      return t('computer.keySkipped', 'We could not safely remove our key, so it still lets us log in there.') + byHand;
    case 'unreached':
      return t('computer.keyUnreached', 'We could not reach the machine, so our key is still there.') + byHand;
    default:
      return undefined;
  }
}

export function useComputerActions() {
  const { t } = useTranslation();
  const queryClient = useQueryClient();

  // Start is lazy: the 202 says 'starting' and the status stream reports the rest.
  const power = useMutation({
    mutationFn: ({ computerId, action }: { computerId: string; action: PowerAction }) =>
      action === 'start' ? startComputer(computerId, { lazy: true }) : stopComputer(computerId),
    onSuccess: (res) => {
      // Reflecting the response status is what brings a transitional machine
      // into the fan-out's watch set, so the stream reports the rest.
      patchComputerStatusInCaches(queryClient, res.computer_id, res.status);
      void queryClient.invalidateQueries({ queryKey: queryKeys.workspaces.all });
      // The breakdown read before this action describes the old power state.
      invalidateMachineStorage(queryClient, res.computer_id);
    },
  });

  const rename = useMutation({
    mutationFn: ({ computerId, name }: { computerId: string; name: string }) => renameComputer(computerId, name),
    onMutate: ({ computerId, name }) => {
      const snapshot = queryClient.getQueriesData<ComputersResponse | undefined>({ queryKey: queryKeys.computers.lists() });
      patchComputerRow(queryClient, computerId, { name });
      return { snapshot };
    },
    onError: (err, _vars, ctx) => {
      for (const [key, data] of ctx?.snapshot ?? []) queryClient.setQueryData(key, data);
      // A name another computer has is said beside the field it was typed in.
      if (nameTakenBy(err) !== null) return;
      toast({
        variant: 'destructive',
        title: t('computer.renameFailed', 'Could not rename computer'),
        description: entitlementErrorMessage(err, t),
      });
    },
    onSettled: () => invalidateMachine(queryClient),
  });

  // Settles as soon as the server accepts the change. The change itself runs
  // afterwards; the list polls while it does, and ComputersDialogHost reports
  // how it went.
  const spec = useMutation({
    mutationFn: ({ computerId, tier }: { computerId: string; tier: ResourceTier }) => setComputerSpec(computerId, tier),
    onSuccess: async (row, { computerId, tier }) => {
      // A list read that left before the request would land after this patch
      // and put the row back without its change, which also stops the poll.
      await queryClient.cancelQueries({ queryKey: queryKeys.computers.lists() });
      patchComputerRow(queryClient, computerId, row);
      if (row.spec_change?.state === 'in_progress') {
        // The accepted change already holds a slot of the tier it is going to.
        void queryClient.invalidateQueries({ queryKey: queryKeys.workspaces.quota() });
        return;
      }
      // Already at that tier: nothing was scheduled.
      toast({ title: t('workspace.specUpdated', 'Computer spec updated'), description: tierLabel(t, tier) });
      invalidateMachine(queryClient);
      closeComputerSpec();
    },
    onError: () => invalidateMachine(queryClient),
  });

  // A 200 flipped the primary now. A 202 moves Home first and flips when that
  // move commits, so the move is seeded for the row to follow and watched, so
  // its ending is told wherever the user is by then. The name rides in the
  // variables for that toast.
  const makePrimary = useMutation({
    mutationFn: ({ computerId }: { computerId: string; name: string }) => makeComputerPrimary(computerId),
    onSuccess: async (res, { name }) => {
      if (!res.move) {
        invalidateMachine(queryClient);
        return;
      }
      await seedWorkspaceMove(queryClient, res.move);
      watchMove(res.move, name);
    },
    // A refusal can mean the rows moved under the dialog (a spec change began,
    // the machine was removed), so the list re-reads to show why.
    onError: () => invalidateMachine(queryClient),
  });

  // Always-on is no longer offered, so turning it off is all that is left.
  const turnOffAlwaysOn = useMutation({
    mutationFn: ({ computerId }: { computerId: string }) => setComputerAlwaysOn(computerId, false),
    onMutate: ({ computerId }) => {
      const snapshot = queryClient.getQueriesData<ComputersResponse | undefined>({ queryKey: queryKeys.computers.lists() });
      patchComputerRow(queryClient, computerId, { is_always_on: false });
      return { snapshot };
    },
    onError: (err, _vars, ctx) => {
      for (const [key, data] of ctx?.snapshot ?? []) queryClient.setQueryData(key, data);
      toast({
        variant: 'destructive',
        title: t('workspace.alwaysOnFailed'),
        description: entitlementErrorMessage(err, t),
      });
    },
    onSettled: () => invalidateMachine(queryClient),
  });

  // The switch moves at once; a refusal puts it back and says why.
  const setFilesBackup = useMutation({
    mutationFn: ({ computerId, enabled }: { computerId: string; enabled: boolean }) =>
      setComputerFilesBackup(computerId, enabled),
    onMutate: ({ computerId, enabled }) => {
      const snapshot = queryClient.getQueriesData<ComputersResponse | undefined>({ queryKey: queryKeys.computers.lists() });
      patchComputerRow(queryClient, computerId, { files_backup: enabled });
      return { snapshot };
    },
    onSuccess: (row) => {
      if (typeof row.files_backup === 'boolean') patchComputerRow(queryClient, row.computer_id, { files_backup: row.files_backup });
    },
    onError: (err, _vars, ctx) => {
      for (const [key, data] of ctx?.snapshot ?? []) queryClient.setQueryData(key, data);
      toast({
        variant: 'destructive',
        title: t('computer.filesBackup.failed', "Couldn't change file backup"),
        description: apiErrorReason(err) ?? undefined,
      });
    },
    onSettled: () => queryClient.invalidateQueries({ queryKey: queryKeys.computers.lists() }),
  });

  // The new row goes on the list straight away so the dialog can scroll to it;
  // the refetch then brings the server's count and status. A refusal can mean
  // the plan or the offered providers moved under the form, so both re-read.
  // Keyed, so turning remote control off can wait for a create in flight to
  // put its row on the list.
  const create = useMutation({
    mutationKey: CREATE_COMPUTER_MUTATION_KEY,
    mutationFn: (body: ComputerCreate) => createComputer(body),
    onSuccess: (row) => {
      addComputerRow(queryClient, row);
      invalidateMachine(queryClient);
    },
    onError: () => {
      invalidateMachine(queryClient);
      void queryClient.invalidateQueries({ queryKey: queryKeys.computers.providers() });
    },
  });

  // The row leaves every list at once, so nothing goes on offering a machine
  // that is gone; the refetch then confirms it with the server's list. The
  // name rides in the variables because the row is gone by the time it is read,
  // and so does the host key of an SSH computer: when it was this Mac, remote
  // control turns off with it here, where a dialog closing mid-removal cannot
  // drop it. The workspaces it deleted first leave with it, and only those: one
  // moved off it meanwhile still exists. An older backend does not say which,
  // and there every one named went.
  const remove = useMutation({
    mutationFn: async ({ computerId, removeKey, workspaceIds, deleteFiles }: RemoveComputerVariables) => {
      const res = await deleteComputer(computerId, { removeKey, deleteWorkspaces: workspaceIds, deleteFiles });
      return { ...res, workspaces_deleted: res.workspaces_deleted ?? [...workspaceIds] };
    },
    onSuccess: (res, { computerId, name, fingerprint, workspaceIds, folder }) => {
      removeComputerRow(queryClient, computerId);
      queryClient.removeQueries({ queryKey: queryKeys.computers.storage(computerId) });
      for (const id of res.workspaces_deleted) forgetDeletedWorkspace(queryClient, id);
      if (workspaceIds.length) invalidateWorkspaceMembership(queryClient);
      invalidateMachine(queryClient);
      if (fingerprint) void releaseRemoteControl(queryClient, { onlyFor: fingerprint });
      // A key or files left behind are the user's to remove, and the
      // fingerprint is something to copy, so those stay until closed. Still
      // the plain variant: the removal itself succeeded.
      const stays = res.key?.outcome === 'skipped' || res.key?.outcome === 'unreached'
        || (!!res.files && res.files.outcome !== 'deleted');
      const description = [filesRemovalNote(res.files, folder, t), keyRemovalNote(res.key, t)].filter(Boolean).join(' ');
      toast({
        title: t('computer.removed', '{{name}} removed', { name }),
        description: description || undefined,
        ...(stays ? { duration: Infinity } : {}),
      });
    },
    // A refusal can mean the row moved under the dialog (a workspace landed,
    // a start began), so the list re-reads to show why. One that came once
    // some named workspaces were deleted says which, and those are forgotten.
    onError: (err, { workspaceIds }) => {
      invalidateMachine(queryClient);
      for (const id of deletedBeforeRefusal(err)) forgetDeletedWorkspace(queryClient, id);
      if (workspaceIds.length) invalidateWorkspaceMembership(queryClient);
    },
  });

  // Accepted, the start runs in the background like a lazy one: the 202's
  // 'starting' arms the status stream, and the failure that sent the user
  // here is gone from the row, which the server cleared too.
  const settleRestart = (computerId: string, status: string, patch: Partial<Computer> = {}) => {
    patchComputerRow(queryClient, computerId, { ...patch, start_failure: null });
    patchComputerStatusInCaches(queryClient, computerId, status);
    invalidateMachine(queryClient);
    invalidateMachineStorage(queryClient, computerId);
  };

  // A new folder moves every workspace's folder with it, so the workspace
  // rows re-read along with the machine's.
  const reinstall = useMutation({
    mutationFn: ({ computerId, body }: { computerId: string; body: ComputerReinstall }) =>
      reinstallComputer(computerId, body),
    onSuccess: async (res, { computerId }) => {
      await queryClient.cancelQueries({ queryKey: queryKeys.computers.lists() });
      settleRestart(res.computer_id || computerId, res.status);
    },
    onError: () => invalidateMachine(queryClient),
  });

  // The row takes the new host key before anything else reads it: this Mac
  // is told apart by that key, and remote control turns itself off once no
  // row names the helper's. Keyed, so that check waits for this to land.
  const reconnect = useMutation({
    mutationKey: RECONNECT_COMPUTER_MUTATION_KEY,
    mutationFn: ({ computerId, ssh }: ReconnectComputerVariables) => reconnectComputer(computerId, ssh),
    onSuccess: async (res, vars) => {
      await queryClient.cancelQueries({ queryKey: queryKeys.computers.lists() });
      queryClient.setQueriesData<ComputersResponse | undefined>(
        { queryKey: queryKeys.computers.lists() },
        (prev) => !prev?.computers ? prev : {
          ...prev,
          computers: prev.computers.map((c) =>
            c.computer_id === vars.computerId ? { ...c, ssh: reconnectedSsh(c, vars) } : c),
        },
      );
      settleRestart(res.computer_id || vars.computerId, res.status);
    },
    onError: () => invalidateMachine(queryClient),
  });

  // Settles with the request, so the field it came from can show a refusal.
  const renameComputerTo = useCallback(
    async (computerId: string, name: string): Promise<void> => {
      await rename.mutateAsync({ computerId, name });
    },
    [rename],
  );

  return {
    power,
    rename,
    renameComputerTo,
    spec,
    makePrimary,
    turnOffAlwaysOn,
    setFilesBackup,
    create,
    remove,
    reinstall,
    reconnect,
  };
}

/** The workspaces a refused removal had already deleted, which it names. */
export function deletedBeforeRefusal(err: unknown): string[] {
  const ids = apiErrorObject(err)?.workspaces_deleted;
  return Array.isArray(ids) ? ids.filter((id): id is string => typeof id === 'string') : [];
}
