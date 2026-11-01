import { useQueryClient } from '@tanstack/react-query';
import type { TFunction } from 'i18next';
import {
  removeClaudeCodeComputerToken, removeClaudeCodeToken, saveClaudeCodeComputerToken, saveClaudeCodeToken,
  startClaudeCodeComputerSignIn, startClaudeCodeSignIn, submitClaudeCodeComputerSignInCode, submitClaudeCodeSignInCode,
  type ClaudeCodeComputer, type ClaudeCodeStatus,
} from '@/api/model';
import { readOAuthError } from '@/api/oauth';
import { CLAUDE_CODE_UNRUNNABLE } from '@/lib/claudeCode';
import { queryKeys } from '@/lib/queryKeys';

/** Whose setup token: the one saved in Settings, or the one kept on one of
 *  the user's own computers. */
export type TokenTarget = { kind: 'saved' } | { kind: 'computer'; computerId: string; name: string };

export const SAVED_TOKEN: TokenTarget = { kind: 'saved' };

export const computerTokenTarget = (c: Pick<ClaudeCodeComputer, 'computer_id' | 'name'>): TokenTarget =>
  ({ kind: 'computer', computerId: c.computer_id, name: c.name });

/** What a token sheet does: add a token, or replace the one there. */
export type SheetAction = 'add' | 'replace';

const SAVED_COPY = {
  intro: 'modelAccess.claudeCode.intro',
  separate: 'modelAccess.claudeCode.separate',
  signInIntro: 'modelAccess.claudeCode.signIn.intro',
  addTitle: 'modelAccess.claudeCode.addTitle',
  replaceTitle: 'modelAccess.claudeCode.replaceTitle',
  removeTitle: 'modelAccess.claudeCode.removeTitle',
  removeBody: 'modelAccess.claudeCode.removeBody',
};

const COMPUTER_COPY: typeof SAVED_COPY = {
  intro: 'modelAccess.claudeCode.computer.intro',
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
  if (target.kind === 'saved') {
    const store = (status: ClaudeCodeStatus) => {
      queryClient.setQueryData(queryKeys.features.claudeCodeStatus(), status);
      void reread(queryKeys.features.claudeCodeComputers());
    };
    return {
      save: async (token: string) => store(await saveClaudeCodeToken(token)),
      start: () => startClaudeCodeSignIn(),
      submit: async (code: string) => store(await submitClaudeCodeSignInCode(code)),
      remove: async () => {
        await removeClaudeCodeToken();
        await reread(queryKeys.features.claudeCode());
      },
    };
  }
  const { computerId } = target;
  const rereadIfBlocked = async <T>(call: Promise<T>): Promise<T> => {
    try {
      return await call;
    } catch (err) {
      if (CLAUDE_CODE_UNRUNNABLE.has(readOAuthError(err)?.code ?? '')) void reread(queryKeys.features.claudeCodeComputers());
      throw err;
    }
  };
  const store = (entry: ClaudeCodeComputer) => {
    queryClient.setQueryData<{ computers: ClaudeCodeComputer[] }>(queryKeys.features.claudeCodeComputers(), (list) => list && ({
      computers: list.computers.map((c) => (c.computer_id === entry.computer_id ? entry : c)),
    }));
    void reread(queryKeys.features.claudeCodeStatus());
  };
  return {
    save: async (token: string) => store(await rereadIfBlocked(saveClaudeCodeComputerToken(computerId, token))),
    start: () => rereadIfBlocked(startClaudeCodeComputerSignIn(computerId)),
    submit: async (code: string) => store(await rereadIfBlocked(submitClaudeCodeComputerSignInCode(computerId, code))),
    remove: async () => {
      await removeClaudeCodeComputerToken(computerId);
      await reread(queryKeys.features.claudeCode());
    },
  };
}

export type TokenRoutes = ReturnType<typeof useTokenRoutes>;
