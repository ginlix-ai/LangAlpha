import { apiErrorStatus } from '@/api/errors';
import type { ClaudeCodeComputerState } from '@/api/model';
import { TOKEN_WORKS } from '@/lib/claudeCode';
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
   *  (`hosted_paused`) included. `needs_bigger_computer`: the credential
   *  would run, but the server names the computer as too small for it. */
  state: ClaudeCodeComputerState | 'needs_own_computer' | CloudRefusal | 'api_key' | 'needs_bigger_computer';
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
 * computer) or any other failure does not. A cloud computer the server
 * names as too small is told only once the credential would run, the order
 * admission refuses in.
 */
export function useWorkspaceClaudeCode(workspaceId: string | null | undefined): WorkspaceClaudeCode {
  const status = useClaudeCodeStatus();
  const workspace = useWorkspace(workspaceId);
  const own = useClaudeCodeComputers();
  const choice = useClaudeCodeCustody();
  const workspaceKnown = !workspaceId
    || (workspace.data !== undefined && !workspace.isPlaceholderData)
    || [403, 404].includes(apiErrorStatus(workspace.error) ?? 0);
  const known = workspaceKnown && !own.isLoading && !own.unread;
  if (!status.enabled) return { state: 'off', computerId: null, known };
  if (!known) return { state: 'checking', computerId: null, known };
  const computerId = workspace.data?.computer_id;
  const entry = own.computers.find((c) => c.computer_id === computerId);
  if (entry) return { state: entry.state, computerId: entry.computer_id, known };
  const credential = cloudCredentialState(status, choice);
  const tooSmall = computerId != null && own.cloudRefusals[computerId] === 'needs_bigger_computer';
  return { state: CREDENTIAL_RUNS.has(credential) && tooSmall ? 'needs_bigger_computer' : credential, computerId: null, known };
}

/** The states in which a turn on a cloud computer has a credential that runs. */
const CREDENTIAL_RUNS: ReadonlySet<string> = new Set([...TOKEN_WORKS, 'api_key']);

/** Where a turn on a cloud computer stands by its credential alone: the
 *  token saved in Settings where this server runs turns on one, else the
 *  server's verdict for the user's choice for cloud computers. A verdict that
 *  no cloud computer runs it comes first: a stored token still runs on the
 *  user's own computers, so the status alone would call it runnable. */
function cloudCredentialState(
  status: ReturnType<typeof useClaudeCodeStatus>, choice: ReturnType<typeof useClaudeCodeCustody>,
): WorkspaceClaudeCode['state'] {
  if (choice.custody?.refusal === 'needs_own_computer') return 'needs_own_computer';
  if (status.runsHere) return status.state;
  if (choice.isLoading || choice.unread) return 'checking';
  if (!choice.custody) return 'needs_own_computer';
  const { runs_on: runsOn, refusal } = choice.custody;
  if (runsOn === 'api_key') return 'api_key';
  if (runsOn === 'stored_token') return 'checking';
  if (refusal === 'stored_blocked' || refusal === 'needs_own_computer') return 'needs_own_computer';
  return refusal && CLOUD_REFUSALS.has(refusal) ? refusal as CloudRefusal : 'hosted_off';
}
