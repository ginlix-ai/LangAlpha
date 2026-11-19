import { useQueryClient } from '@tanstack/react-query';
import type { TFunction } from 'i18next';
import {
  removeClaudeCodeComputerToken, removeClaudeCodeToken, saveClaudeCodeComputerToken, saveClaudeCodeToken,
  startClaudeCodeComputerSignIn, startClaudeCodeSignIn, submitClaudeCodeComputerSignInCode, submitClaudeCodeSignInCode,
  type ClaudeCodeComputer, type ClaudeCodeComputers, type ClaudeCodeStatus,
} from '@/api/model';
import { readOAuthError } from '@/api/oauth';
import { authSessionCheck } from '@/lib/authResets';
import { updateComputers } from '@/hooks/useClaudeCode';
import { CLAUDE_CODE_UNRUNNABLE, type TokenTarget } from '@/lib/claudeCode';
import { queryKeys } from '@/lib/queryKeys';

export { SAVED_TOKEN, computerTokenTarget, type TokenTarget } from '@/lib/claudeCode';

/** What a token sheet does: add a token, or replace the one there. */
export type SheetAction = 'add' | 'replace';

const SAVED_COPY = {
  separate: 'modelAccess.claudeCode.separate',
  signInIntro: 'modelAccess.claudeCode.signIn.intro',
  addTitle: 'modelAccess.claudeCode.addTitle',
  replaceTitle: 'modelAccess.claudeCode.replaceTitle',
  removeTitle: 'modelAccess.claudeCode.removeTitle',
  removeBody: 'modelAccess.claudeCode.removeBody',
};

const COMPUTER_COPY: typeof SAVED_COPY = {
  separate: 'modelAccess.claudeCode.computer.separate',
  signInIntro: 'modelAccess.claudeCode.computer.signInIntro',
  addTitle: 'modelAccess.claudeCode.computer.addTitle',
  replaceTitle: 'modelAccess.claudeCode.computer.replaceTitle',
  removeTitle: 'modelAccess.claudeCode.computer.removeTitle',
  removeBody: 'modelAccess.claudeCode.computer.removeBody',
};

/** The copy a token's sheet and removal show, by whose token it is: the
 *  keys, read with `values`. */
export function tokenCopy(target: TokenTarget) {
  return target.kind === 'saved' ? { keys: SAVED_COPY, values: {} } : { keys: COMPUTER_COPY, values: { name: target.name } };
}

/** A token sheet's title, for whose token and what it does. */
export function tokenSheetTitle(t: TFunction, target: TokenTarget, action: SheetAction): string {
  const { keys, values } = tokenCopy(target);
  return t(action === 'replace' ? keys.replaceTitle : keys.addTitle, values);
}

/**
 * A token's routes, and what each change leaves the cache holding. A save
 * answers with its own read, so only the other one is read again: a
 * computer runs on the saved token where it has none of its own. A removal
 * answers with nothing, so both are. A sign-in or a token refused because
 * Claude Code cannot run on the computer (`CLAUDE_CODE_UNRUNNABLE`) is news
 * its row does not have yet, so the list is read again.
 */
export function useTokenRoutes(target: TokenTarget) {
  const queryClient = useQueryClient();
  const reread = (queryKey: readonly unknown[]) => queryClient.invalidateQueries({ queryKey });
  // An answer that lands after a sign-out or an account switch was the last
  // account's, so it is not kept.
  const thisAccount = async <T>(call: () => Promise<T>, keep: (answer: T) => Promise<void> | void) => {
    const sameAccount = authSessionCheck();
    const answer = await call();
    if (sameAccount()) await keep(answer);
  };
  if (target.kind === 'saved') {
    // A status read already on its way left before the save, so it is dropped.
    const store = async (status: ClaudeCodeStatus) => {
      await queryClient.cancelQueries({ queryKey: queryKeys.features.claudeCodeStatus() });
      queryClient.setQueryData(queryKeys.features.claudeCodeStatus(), status);
      void reread(queryKeys.features.claudeCodeComputers());
      void reread(queryKeys.features.claudeCodeSetup());
    };
    return {
      save: (token: string) => thisAccount(() => saveClaudeCodeToken(token), store),
      start: () => startClaudeCodeSignIn(),
      submit: (code: string) => thisAccount(() => submitClaudeCodeSignInCode(code), store),
      remove: async () => {
        try {
          await removeClaudeCodeToken();
        } finally {
          await reread(queryKeys.features.claudeCode());
        }
      },
    };
  }
  const { computerId } = target;
  const rereadIfBlocked = async <T>(call: Promise<T>): Promise<T> => {
    try {
      return await call;
    } catch (err) {
      if (CLAUDE_CODE_UNRUNNABLE.has(readOAuthError(err)?.code ?? '')) {
        void reread(queryKeys.features.claudeCodeComputers());
        void reread(queryKeys.features.claudeCodeSetup());
      }
      throw err;
    }
  };
  const store = async (entry: ClaudeCodeComputer) => {
    // A list read already on its way left before the save, so it is dropped;
    // a computer the list was read without is added, as of that read, so
    // whatever the token unlocks reads it at once. The save's answer is the
    // computer's row, so the list is not read again for it, unless there was
    // no list to add it to.
    const key = queryKeys.features.claudeCodeComputers();
    await queryClient.cancelQueries({ queryKey: key });
    queryClient.setQueryData<ClaudeCodeComputers>(key, (list) => (
      list && !list.computers.some((c) => c.computer_id === entry.computer_id)
        ? { ...list, computers: [...list.computers, entry] }
        : list
    ), { updatedAt: queryClient.getQueryState(key)?.dataUpdatedAt });
    if (!queryClient.getQueryData(key)) void reread(key);
    updateComputers(queryClient, (c) => c.computer_id === entry.computer_id, () => entry);
    void reread(queryKeys.features.claudeCodeStatus());
    void reread(queryKeys.features.claudeCodeSetup());
  };
  return {
    save: (token: string) => thisAccount(() => rereadIfBlocked(saveClaudeCodeComputerToken(computerId, token)), store),
    start: () => rereadIfBlocked(startClaudeCodeComputerSignIn(computerId)),
    submit: (code: string) => thisAccount(() => rereadIfBlocked(submitClaudeCodeComputerSignInCode(computerId, code)), store),
    remove: async () => {
      await removeClaudeCodeComputerToken(computerId);
      await reread(queryKeys.features.claudeCode());
    },
  };
}

export type TokenRoutes = ReturnType<typeof useTokenRoutes>;
