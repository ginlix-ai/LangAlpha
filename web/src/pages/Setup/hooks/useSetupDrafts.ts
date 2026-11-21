/**
 * What the setup step and Settings hold before it is saved. Each form renders
 * only once the status has loaded, so a draft starts from it and a later refetch
 * (coming back to the tab, checking for Docker) never takes back what was typed.
 */
import { useState } from 'react';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { apiErrorDetailMessage, apiErrorStatus } from '@/api/errors';
import { completeSetup, saveSetupKeys, saveSetupSandbox } from '@/api/setup';
import type { InstallGroup, SandboxKind, SetupStatus } from '@/api/setup';
import { queryKeys } from '@/lib/queryKeys';
import {
  cardAvailable,
  cardOf,
  initialCard,
  initialCloudKind,
  optionOf,
  selectedGroupIds,
  toggleInstallGroup,
  type CloudKind,
  type SandboxCard,
} from '../utils/sandboxChoice';

export type KeyDrafts = Readonly<Record<string, string>>;

/** Whether a save went through. Its error stays on its mutation, where the
 *  form reads what to say. */
function succeeded(save: Promise<unknown>): Promise<boolean> {
  return save.then(() => true, () => false);
}

/** Runs saves one after another, stopping at the first that fails, so what
 *  follows never runs on what did not land. */
export async function saveInOrder(saves: readonly (() => Promise<boolean>)[]): Promise<boolean> {
  for (const save of saves) {
    if (!(await save())) return false;
  }
  return true;
}

function useApplyStatus() {
  const queryClient = useQueryClient();
  return (status: SetupStatus) => {
    queryClient.setQueryData(queryKeys.setup.status(), status);
    // What a new computer may be created on follows what was just saved.
    void queryClient.invalidateQueries({ queryKey: queryKeys.computers.providers() });
  };
}

/** Without the key that protects these rows nothing can be saved, and the server says only that. */
export function useSaveErrorCopy() {
  const { t } = useTranslation();
  return (error: unknown): string => {
    if (apiErrorStatus(error) === 503) return t('setup.errors.noEncryptionKey');
    return apiErrorDetailMessage(error) ?? t('setup.errors.saveFailed');
  };
}

export function useRemoveSetupKey() {
  const apply = useApplyStatus();
  return useMutation({
    mutationFn: (name: string) => saveSetupKeys({ [name]: null }),
    onSuccess: apply,
  });
}

export function useSandboxDraft(status: SetupStatus) {
  const apply = useApplyStatus();
  const [card, setCard] = useState<SandboxCard | null>(() => initialCard(status));
  const [cloudKind, setCloudKind] = useState<CloudKind>(() => initialCloudKind(status));
  const [groups, setGroups] = useState<InstallGroup[]>(() => optionOf(status, 'local')?.install_groups ?? []);
  const [keyDrafts, setKeyDrafts] = useState<KeyDrafts>({});
  const [keyMissing, setKeyMissing] = useState(false);

  const provider: SandboxKind | null = card === 'cloud' ? cloudKind : card;
  const cloudKey = status.keys.find((key) => key.group === 'sandbox' && key.provider === cloudKind);
  const typedKey = cloudKey ? (keyDrafts[cloudKey.name] ?? '').trim() : '';

  const mutation = useMutation({
    mutationFn: async () => {
      if (!provider) throw new Error('No choice to save');
      if (cloudKey && card === 'cloud' && typedKey) await saveSetupKeys({ [cloudKey.name]: typedKey });
      return saveSetupSandbox(provider, card === 'local' ? selectedGroupIds(groups) : undefined);
    },
    onSuccess: (saved) => {
      apply(saved);
      setKeyDrafts({});
    },
  });

  // A cloud kind may be saved before its key, but then nothing runs on it, and
  // the server falls back without a word, so its key is asked for here.
  const needsKey = card === 'cloud' && !typedKey && cloudKey?.state === 'unset';

  const originalGroups = selectedGroupIds(optionOf(status, 'local')?.install_groups ?? []).join();
  const dirty =
    provider !== null &&
    (cardOf(status.sandbox.provider) !== card ||
      (card === 'cloud' && status.sandbox.provider !== cloudKind) ||
      (card === 'local' && selectedGroupIds(groups).join() !== originalGroups) ||
      typedKey !== '');

  return {
    card,
    cloudKind,
    groups,
    keyDrafts,
    keyMissing,
    provider,
    dirty,
    pickCard: (next: SandboxCard) => {
      setCard(next);
      setKeyMissing(false);
    },
    pickCloudKind: (next: CloudKind) => {
      setCloudKind(next);
      setKeyMissing(false);
    },
    toggleGroup: (id: string) => setGroups((current) => toggleInstallGroup(current, id)),
    setKeyDraft: (name: string, value: string) => {
      setKeyDrafts((current) => ({ ...current, [name]: value }));
      setKeyMissing(false);
    },
    /** Whether the choice can be saved at all as things stand. */
    ready: card !== null && cardAvailable(status, card),
    saving: mutation.isPending,
    saved: mutation.isSuccess && !dirty,
    error: mutation.error,
    /** The cloud key typed first, then the choice, since a cloud kind saved
     *  without its key runs nothing. */
    save: (): Promise<boolean> => {
      if (needsKey) {
        setKeyMissing(true);
        return Promise.resolve(false);
      }
      return succeeded(mutation.mutateAsync());
    },
  };
}

export type SandboxDraft = ReturnType<typeof useSandboxDraft>;

export function useServiceKeysDraft() {
  const apply = useApplyStatus();
  const [drafts, setDrafts] = useState<KeyDrafts>({});

  const typed = Object.fromEntries(
    Object.entries(drafts)
      .map(([name, value]) => [name, value.trim()] as const)
      .filter(([, value]) => value),
  );
  const dirty = Object.keys(typed).length > 0;

  const mutation = useMutation({
    mutationFn: () => saveSetupKeys(typed),
    onSuccess: (saved) => {
      apply(saved);
      setDrafts({});
    },
  });

  return {
    drafts,
    setDraft: (name: string, value: string) => setDrafts((current) => ({ ...current, [name]: value })),
    dirty,
    saving: mutation.isPending,
    saved: mutation.isSuccess && !dirty,
    error: mutation.error,
    /** Nothing typed is nothing to save, which counts as saved. */
    save: () => (dirty ? succeeded(mutation.mutateAsync()) : Promise.resolve(true)),
  };
}

export type ServiceKeysDraft = ReturnType<typeof useServiceKeysDraft>;

/** Marks the server's own step answered, so no account is sent to it again. */
export function useCompleteServerSetup() {
  const apply = useApplyStatus();
  const mutation = useMutation({ mutationFn: completeSetup, onSuccess: apply });
  return {
    saving: mutation.isPending,
    error: mutation.error,
    save: () => succeeded(mutation.mutateAsync()),
  };
}
