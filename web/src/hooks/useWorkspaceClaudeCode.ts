import { useTranslation } from 'react-i18next';
import { apiErrorStatus } from '@/api/errors';
import type { ClaudeCodeComputerState, ClaudeCodePlan, PlanCredential } from '@/api/model';
import type { PickerMeter } from '@/components/modelAccess/PlanUsage';
import { TOKEN_WORKS } from '@/lib/claudeCode';
import { planCredentialOf, savedTokenInSentence } from '@/lib/claudeCodePlan';
import { useClaudeCodeComputers, useClaudeCodeStatus } from './useClaudeCode';
import { useClaudeCodeCustody } from './useClaudeCodeCustody';
import { useLocale } from './useLocale';
import { useWorkspace } from './useWorkspace';

/** Where Claude Code stands for a turn in one workspace, and the computer
 *  whose token that is, when it is one of the user's own. */
export interface WorkspaceClaudeCode {
  /** On a hosted computer where this server runs no turn on a saved token:
   *  `needs_own_computer` where it offers no other way, or storing a token
   *  is blocked for the user and nothing else is offered, so Claude Code runs
   *  nowhere for it; else as the server's verdict for the user's choice for
   *  cloud computers says: `hosted_off` where the choice keeps Claude Code off
   *  them. `needs_bigger_computer`: the credential
   *  would run, but the server names the computer as too small for it. */
  state: ClaudeCodeComputerState | 'needs_own_computer' | 'hosted_off' | 'needs_bigger_computer';
  computerId: string | null;
  /** Whether the workspace's computer, and which computers are the user's
   *  own, are read; until they are, `computerId` says nothing. */
  known: boolean;
  /** The Claude plan behind the token its turns run on: that computer's
   *  entry's, or the saved token's where it runs on that. Absent until it is
   *  known. */
  plan?: ClaudeCodePlan | null;
  /** The model picker's meter of that plan, while the token works or is
   *  being checked: whose plan, named by the computer of the user's own the
   *  turns run on or as the saved token, and Check now only while it works. */
  meter?: PickerMeter;
}

/** The picker's meter for a credential in `state`, or none where its token
 *  neither works nor is being checked; Check now only where it works and a
 *  check would not be billed (`checkBills`). */
function meterOf(
  state: WorkspaceClaudeCode['state'], credential: PlanCredential | null, name: string,
  plan: ClaudeCodePlan | null | undefined, checkAfter: number | null, checkBills: boolean,
): PickerMeter | undefined {
  if (!credential || !(TOKEN_WORKS.has(state) || state === 'checking')) return undefined;
  return { name, plan, credential: TOKEN_WORKS.has(state) && !checkBills ? credential : null, checkAfter };
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
  const { t } = useTranslation();
  const locale = useLocale();
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
  if (entry) {
    const meter = meterOf(entry.state, planCredentialOf(entry), entry.name, entry.plan, own.checkAfterOf(entry), entry.check_bills === true);
    return { state: entry.state, computerId: entry.computer_id, known, plan: entry.plan, meter };
  }
  const credential = cloudCredentialState(status, choice);
  const tooSmall = computerId != null && own.cloudRefusals[computerId] === 'needs_bigger_computer';
  const state = TOKEN_WORKS.has(credential) && tooSmall ? 'needs_bigger_computer' : credential;
  if (!status.runsHere) return { state, computerId: null, known };
  const savedName = savedTokenInSentence(t('modelAccess.claudeCode.setup.savedName'), locale);
  return { state, computerId: null, known, plan: status.plan, meter: meterOf(state, { kind: 'saved' }, savedName, status.plan, status.checkAfter, status.checkBills) };
}

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
  if (runsOn === 'stored_token') return 'checking';
  if (refusal === 'stored_blocked' || refusal === 'needs_own_computer') return 'needs_own_computer';
  return 'hosted_off';
}
