import { apiErrorStatus } from '@/api/errors';
import type { ClaudeCodeComputerState } from '@/api/model';
import { useClaudeCodeComputers, useClaudeCodeStatus } from './useClaudeCode';
import { useClaudeCodeCustody } from './useClaudeCodeCustody';
import { useWorkspace } from './useWorkspace';

/** Why a turn on a cloud computer is refused, as the server's verdict gives it. */
type CloudRefusal = 'hosted_off' | 'hosted_paused' | 'needs_api_key' | 'api_key_endpoint';
const CLOUD_REFUSALS: ReadonlySet<string> = new Set<CloudRefusal>(['hosted_off', 'hosted_paused', 'needs_api_key', 'api_key_endpoint']);

/** Where Claude Code stands for a turn in one workspace, and the computer
 *  whose token that is, when it is one of the user's own. */
export interface WorkspaceClaudeCode {
  /** On a hosted computer where this server runs no turn on a saved token:
   *  `needs_own_computer` where it offers no other way, or storing a token
   *  is blocked for the user and nothing else is offered, so Claude Code runs
   *  nowhere for it; else as the server's verdict for the user's choice for
   *  cloud computers says: on their Anthropic API key (`api_key`), or the
   *  reason it is refused for, a choice whose switch the server turned off
   *  (`hosted_paused`) included. */
  state: ClaudeCodeComputerState | 'needs_own_computer' | CloudRefusal | 'api_key';
  computerId: string | null;
  /** Whether the workspace's computer, and which computers are the user's
   *  own, are read; until they are, `computerId` says nothing. */
  known: boolean;
}

/**
 * A workspace's turn runs Claude Code where its computer is. On the user's
 * own computer (one Claude Code lists) the CLI runs there, with the token
 * that computer's entry names. Elsewhere it runs on the token saved in
 * Settings where this server runs turns on one, and otherwise as the user
 * chose for cloud computers where the server offers that choice, which waits
 * as on a check until it is read. A stored token is one this server runs
 * turns on, so the status reads it; until the status says so after the
 * choice, it waits as on a check too. Until the workspace and the list are read,
 * the rows wait as on a check, and so they do when either could not be read,
 * since which computer it is on, or which computers are the user's own, is
 * then unknown. A workspace's
 * read says which computer it is on when the server gave a row, or a 403 or
 * 404, since a workspace gone or not the user's is on no computer of theirs;
 * a placeholder (Home as of its first read, maybe before a start gave it a
 * computer) or any other failure does not.
 */
export function useWorkspaceClaudeCode(workspaceId: string | null | undefined): WorkspaceClaudeCode {
  const status = useClaudeCodeStatus();
  const workspace = useWorkspace(workspaceId);
  const own = useClaudeCodeComputers();
  const cloud = useClaudeCodeCustody();
  const workspaceKnown = !workspaceId
    || (workspace.data !== undefined && !workspace.isPlaceholderData)
    || [403, 404].includes(apiErrorStatus(workspace.error) ?? 0);
  const known = workspaceKnown && !own.isLoading && !own.unread;
  if (!status.enabled) return { state: 'off', computerId: null, known };
  if (!known) return { state: 'checking', computerId: null, known };
  const entry = own.computers.find((c) => c.computer_id === workspace.data?.computer_id);
  if (entry) return { state: entry.state, computerId: entry.computer_id, known };
  if (status.runsHere) return { state: status.state, computerId: null, known };
  if (cloud.isLoading || cloud.unread) return { state: 'checking', computerId: null, known };
  if (!cloud.custody) return { state: 'needs_own_computer', computerId: null, known };
  const { runs_on: runsOn, refusal } = cloud.custody;
  if (runsOn === 'api_key') return { state: 'api_key', computerId: null, known };
  if (runsOn === 'stored_token') return { state: 'checking', computerId: null, known };
  if (refusal === 'stored_blocked') return { state: 'needs_own_computer', computerId: null, known };
  return { state: refusal && CLOUD_REFUSALS.has(refusal) ? refusal as CloudRefusal : 'hosted_off', computerId: null, known };
}
