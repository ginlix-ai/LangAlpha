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
import { useMutation, useQueryClient, type QueryClient } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';

import { toast } from '@/components/ui/use-toast';
import { homeWorkspaceQuery } from '@/hooks/useHomeWorkspace';
import { queryKeys } from '@/lib/queryKeys';
import type {
  Computer,
  ComputerCreate,
  ComputerReconnectTarget,
  ComputerReinstall,
  ComputerRemoval,
  ComputerRemoveResponse,
  ComputersResponse,
  ResourceTier,
} from '@/types/api';

import {
  createComputer,
  deleteComputer,
  getComputerRemoval,
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
  fetchComputerList,
  invalidateMachine,
  invalidateMachineStorage,
  patchComputerRow,
  patchComputerStatusInCaches,
  RECONNECT_COMPUTER_MUTATION_KEY,
  removeComputerRow,
} from './useComputers';
import { isMoveShown, seedWorkspaceMove, waitForMoveEnd, watchMove, WORKSPACE_MOVE_POLL_MS } from './useWorkspaceMove';
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
  /** For the primary: the computer Home moves to first, the removal following once it has. */
  homeTo?: string;
}

/**
 * How a removal went. `kept` says why the computer stays once Home's move
 * off it ended, with the computer Home is on by then; null when it went.
 */
export interface RemoveOutcome extends ComputerRemoveResponse {
  workspaces_deleted: string[];
  kept: { removal: ComputerRemoval; home: string; homeId: string } | null;
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
    paired_elsewhere: false,
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

/** The Claude Code token left on the machine, for its owner to delete; undefined when none is. */
function tokenRemovalNote(token: ComputerRemoveResponse['token'], t: Translate): string | undefined {
  switch (token?.outcome) {
    case 'unreached':
      return t(
        'computer.tokenUnreached',
        'We could not reach the machine, so its Claude Code token is still there. To remove it yourself, delete {{path}} on it.',
        { path: token.path },
      );
    case 'failed':
      return t(
        'computer.tokenRemoveFailed',
        'We could not remove its Claude Code token from the machine. To remove it yourself, delete {{path}} on it.',
        { path: token.path },
      );
    default:
      return undefined;
  }
}

/**
 * A refused remove, in this app's words for the refusals the route names,
 * else the server's own sentence, else a plain line. `homeMachine` names the
 * machine Home lives on, for a hand-back still owed.
 */
export function removeRefusalMessage(
  code: string | undefined,
  serverMessage: string | null | undefined,
  t: Translate,
  homeMachine: string | null | undefined,
): string {
  switch (code) {
    case 'primary':
      return t(
        'computer.removeRefused.primary',
        'This is your primary computer. Make another computer primary first, and Home moves there.',
      );
    case 'only_computer':
      return t(
        'computer.removeRefused.onlyComputer',
        "This is your only computer, so it can't be removed. Add another computer first.",
      );
    case 'handback_owed':
      // Named by the machine Home is on; the server's own sentence where that is unknown.
      if (homeMachine) {
        return t(
          'computer.removeRefused.handbackOwed',
          "Files written on this computer while {{mac}} was away haven't been copied back into Home yet. Try again once {{mac}} is back.",
          { mac: homeMachine },
        );
      }
      break;
    case 'has_workspaces':
      // Only a workspace the confirmation did not name keeps it now.
      return t('computer.removeRefused.hasWorkspaces', 'A workspace not listed here is on it now. Check the list and try again.');
    case 'busy':
      return t('computer.removeRefused.busy', 'This computer is busy. Try again in a moment.');
  }
  return serverMessage || t('computer.removeFailed', "Couldn't remove this computer. Try again.");
}

/**
 * Why a removal that waited on Home's move off the computer did not happen;
 * null while it still waits. `home` is the computer Home is on now.
 */
export function removalEndedMessage(
  removal: ComputerRemoval,
  { name, home }: { name: string; home: string },
  t: Translate,
): string | null {
  switch (removal.state) {
    case 'home_did_not_move':
      return removal.error?.message || t('computer.removeHome.failed', "Couldn't move Home, so nothing was removed. Try again.");
    case 'not_removed':
      return `${t('computer.removeHome.movedNotRemoved', "Home moved to {{home}}, but {{name}} wasn't removed.", { home, name })} ${removeRefusalMessage(removal.error?.code, removal.error?.message, t, home)}`;
    default:
      return null;
  }
}

/** The primary's name on `computers`, which is where Home is. */
export function primaryName(computers: readonly Computer[] | undefined): string {
  return computers?.find((c) => c.is_primary && c.status !== 'deleted')?.name ?? '';
}

/**
 * Read the removal waiting on Home's move off `computerId` until it has
 * settled. Home's move has ended by now, and its runner settles the removal
 * as it ends, so this waits seconds, not the move.
 */
async function removalSettled(computerId: string): Promise<ComputerRemoval> {
  for (;;) {
    const removal = await getComputerRemoval(computerId);
    if (removal.state !== 'waiting_for_home') return removal;
    await new Promise((resolve) => setTimeout(resolve, WORKSPACE_MOVE_POLL_MS));
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
    mutationFn: ({ computerId, tier, planChanged }: { computerId: string; tier: ResourceTier; planChanged?: boolean }) =>
      setComputerSpec(computerId, tier, planChanged),
    onSuccess: async (row, { computerId, tier }) => {
      // A list read that left before the request would land after this patch
      // and put the row back without its change, which also stops the poll.
      await queryClient.cancelQueries({ queryKey: queryKeys.computers.lists() });
      patchComputerRow(queryClient, computerId, row);
      if (row.spec_change?.state === 'in_progress') {
        // The accepted change already holds a slot of the tier it is going to,
        // and Claude Code refuses a computer moving below its minimum from now.
        void queryClient.invalidateQueries({ queryKey: queryKeys.workspaces.quota() });
        void queryClient.invalidateQueries({ queryKey: queryKeys.features.claudeCodeComputers() });
        void queryClient.invalidateQueries({ queryKey: queryKeys.features.claudeCodeSetup() });
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
  // variables for that toast. Backup turned on in the same request shows on
  // the row however the rest went, so the list re-reads either way; a refusal
  // can also mean the rows moved under the dialog (a spec change began, the
  // machine was removed).
  const makePrimary = useMutation({
    mutationFn: ({ computerId, fromBackup = false, turnOnBackup = false }: {
      computerId: string;
      name: string;
      fromBackup?: boolean;
      turnOnBackup?: boolean;
    }) => makeComputerPrimary(computerId, { fromBackup, turnOnBackup }),
    onSuccess: async (res, { name }) => {
      if (!res.move) return;
      await seedWorkspaceMove(queryClient, res.move);
      watchMove(res.move, name);
    },
    onSettled: () => invalidateMachine(queryClient),
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
  //
  // Removing the primary moves Home first, and the server removes the
  // computer once Home has moved: the removal follows Home's move here and
  // reads how it ended off the row. All on the hook, not on `mutate`, so it
  // still settles once the dialog is gone: a removal that did not happen is
  // told by the dialog showing Home's move, or else by a toast.
  const remove = useMutation({
    mutationFn: async ({ computerId, removeKey, workspaceIds, deleteFiles, homeTo }: RemoveComputerVariables): Promise<RemoveOutcome> => {
      const res = await deleteComputer(computerId, { removeKey, deleteWorkspaces: workspaceIds, deleteFiles, homeTo });
      const removed = { ...res, workspaces_deleted: res.workspaces_deleted ?? [...workspaceIds], kept: null };
      if (!res.move_id) return removed;
      const homeId = (await queryClient.ensureQueryData(homeWorkspaceQuery(queryClient))).workspace_id;
      await waitForMoveEnd(queryClient, { workspace_id: homeId, move_id: res.move_id });
      const removal = await removalSettled(computerId);
      if (removal.state === 'removed') {
        // What it left behind, as a removal answering at once would have said.
        return { ...removed, ...removal.removed, workspaces_deleted: removal.removed?.workspaces_deleted ?? [...workspaceIds] };
      }
      const { computers } = await fetchComputerList(queryClient);
      return { ...removed, workspaces_deleted: [], kept: { removal, home: primaryName(computers), homeId } };
    },
    onSuccess: (res, { computerId, name, fingerprint, workspaceIds, folder }) => {
      if (res.kept) {
        // Some of the workspaces named may have gone before a refusal.
        if (workspaceIds.length) invalidateWorkspaceMembership(queryClient);
        invalidateMachine(queryClient);
        const { removal, home, homeId } = res.kept;
        const message = removalEndedMessage(removal, { name, home }, t);
        if (message && !isMoveShown(homeId)) {
          toast({
            variant: 'destructive',
            title: t('computer.removeFailedNamed', "Couldn't remove {{name}}", { name }),
            description: message,
          });
        }
        return;
      }
      removeComputerRow(queryClient, computerId);
      queryClient.removeQueries({ queryKey: queryKeys.computers.storage(computerId) });
      for (const id of res.workspaces_deleted) forgetDeletedWorkspace(queryClient, id);
      if (workspaceIds.length) invalidateWorkspaceMembership(queryClient);
      invalidateMachine(queryClient);
      if (fingerprint) void releaseRemoteControl(queryClient, { onlyFor: fingerprint });
      // A key, files or a token left behind are the user's to remove, and the
      // fingerprint and the token's path are something to copy, so those stay
      // until closed. Still the plain variant: the removal itself succeeded.
      const tokenNote = tokenRemovalNote(res.token, t);
      const stays = res.key?.outcome === 'skipped' || res.key?.outcome === 'unreached'
        || (!!res.files && res.files.outcome !== 'deleted') || !!tokenNote;
      const description = [filesRemovalNote(res.files, folder, t), keyRemovalNote(res.key, t), tokenNote]
        .filter(Boolean).join(' ');
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
