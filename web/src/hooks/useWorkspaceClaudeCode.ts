import { apiErrorStatus } from '@/api/errors';
import type { ClaudeCodeComputerState } from '@/api/model';
import { useClaudeCodeComputers, useClaudeCodeStatus } from './useClaudeCode';
import { useWorkspace } from './useWorkspace';

/** Where Claude Code stands for a turn in one workspace, and the computer
 *  whose token that is, when it is one of the user's own. */
export interface WorkspaceClaudeCode {
  /** `needs_own_computer`: a hosted computer where this server runs no turn
   *  on a saved token, so Claude Code runs nowhere for it. */
  state: ClaudeCodeComputerState | 'needs_own_computer';
  computerId: string | null;
  /** Whether the workspace's computer, and which computers are the user's
   *  own, are read; until they are, `computerId` says nothing. */
  known: boolean;
}

/**
 * A workspace's turn runs Claude Code where its computer is. On the user's
 * own computer (one Claude Code lists) the CLI runs there, with the token
 * that computer's entry names. Elsewhere it runs on the token saved in
 * Settings where this server runs turns on one, and nowhere otherwise.
 * Until the workspace and the list are read, the rows wait as on a check,
 * and so they do when either could not be read, since which computer it is
 * on, or which computers are the user's own, is then unknown. A workspace's
 * read says which computer it is on when the server gave a row, or a 403 or
 * 404, since a workspace gone or not the user's is on no computer of theirs;
 * a placeholder (Home as of its first read, maybe before a start gave it a
 * computer) or any other failure does not.
 */
export function useWorkspaceClaudeCode(workspaceId: string | null | undefined): WorkspaceClaudeCode {
  const status = useClaudeCodeStatus();
  const workspace = useWorkspace(workspaceId);
  const own = useClaudeCodeComputers();
  const workspaceKnown = !workspaceId
    || (workspace.data !== undefined && !workspace.isPlaceholderData)
    || [403, 404].includes(apiErrorStatus(workspace.error) ?? 0);
  const known = workspaceKnown && !own.isLoading && !own.unread;
  if (!status.enabled) return { state: 'off', computerId: null, known };
  if (!known) return { state: 'checking', computerId: null, known };
  const entry = own.computers.find((c) => c.computer_id === workspace.data?.computer_id);
  if (entry) return { state: entry.state, computerId: entry.computer_id, known };
  if (status.runsHere) return { state: status.state, computerId: null, known };
  return { state: 'needs_own_computer', computerId: null, known };
}
