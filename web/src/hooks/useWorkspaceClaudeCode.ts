import { useTranslation } from 'react-i18next';
import { apiErrorStatus } from '@/api/errors';
import type {
  ClaudeCodeCliPlacement, ClaudeCodeComputer, ClaudeCodeComputerState, ClaudeCodePlacementRefusal, ClaudeCodePlan, PlanCredential,
} from '@/api/model';
import type { PickerMeter } from '@/components/modelAccess/PlanUsage';
import { TOKEN_WORKS, computerTokenWorks } from '@/lib/claudeCode';
import { planCredentialOf, savedTokenInSentence } from '@/lib/claudeCodePlan';
import { useClaudeCodeComputers, useClaudeCodeSetup, useClaudeCodeStatus } from './useClaudeCode';
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
   *  would run, but the server names the computer as too small for it.
   *  `no_own_computer` and `cli_computer_offline`: no own computer can run
   *  its CLI for it, as `ClaudeCodeComputers.cli_computer` says. */
  state: ClaudeCodeComputerState | 'needs_own_computer' | 'hosted_off' | 'needs_bigger_computer' | 'no_own_computer' | 'cli_computer_offline';
  computerId: string | null;
  /** On `cli_computer_offline`, the own computer that is offline. */
  cliComputerName?: string | null;
  /** On `needs_bigger_computer`, the cloud computer too small, where known. */
  smallComputerId?: string | null;
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

/** The reads where a turn's Claude Code stands is told from. */
function useClaudeCodeReads() {
  const { t } = useTranslation();
  const locale = useLocale();
  const status = useClaudeCodeStatus();
  const own = useClaudeCodeComputers();
  const choice = useClaudeCodeCustody();
  return { status, own, choice, savedName: savedTokenInSentence(t('modelAccess.claudeCode.setup.savedName'), locale) };
}

type ClaudeCodeReads = ReturnType<typeof useClaudeCodeReads>;

/** A refusal as the rows word it: a token missing or refused there reads as
 *  that token's own state would. */
const REFUSAL_STATES: Readonly<Record<ClaudeCodePlacementRefusal, WorkspaceClaudeCode['state']>> = {
  no_own_computer: 'no_own_computer',
  cli_computer_offline: 'cli_computer_offline',
  needs_own_computer: 'needs_own_computer',
  needs_bigger_computer: 'needs_bigger_computer',
  policy_blocked: 'policy_blocked',
  signed_out: 'no_token',
  token_refused: 'rejected',
  needs_perl: 'needs_perl',
  computer_unsupported: 'computer_unsupported',
};

/** A turn whose CLI runs on one of the user's own computers: that
 *  computer's entry, its token, plan and meter. A token past its expiry
 *  when the list was read, which is read again as one expires, reads as
 *  refused, as admission refuses it, whatever its last check said. No minute
 *  clock is read: the composer would draw again each minute for nothing. */
function onOwnComputer({ own }: ClaudeCodeReads, entry: ClaudeCodeComputer): WorkspaceClaudeCode {
  const state = TOKEN_WORKS.has(entry.state) && !computerTokenWorks(entry, own.readAt) ? 'rejected' : entry.state;
  const meter = meterOf(state, planCredentialOf(entry), entry.name, entry.plan, own.checkAfterOf(entry), entry.check_bills === true);
  return { state, computerId: entry.computer_id, known: true, plan: entry.plan, meter };
}

/** A turn whose CLI runs on no computer of the user's own, by its credential
 *  (`credential`). The cloud computer `computerId` the server names as too
 *  small is told only once the credential would run, the order admission
 *  refuses in. */
function onCloud(
  { status, own, savedName }: ClaudeCodeReads, credential: WorkspaceClaudeCode['state'], computerId: string | null | undefined,
): WorkspaceClaudeCode {
  const tooSmall = computerId != null && own.cloudRefusals[computerId] === 'needs_bigger_computer';
  const state = TOKEN_WORKS.has(credential) && tooSmall ? 'needs_bigger_computer' : credential;
  const small = state === 'needs_bigger_computer' ? { smallComputerId: computerId ?? null } : {};
  if (!status.runsHere) return { state, computerId: null, known: true, ...small };
  return { state, computerId: null, known: true, ...small, plan: status.plan, meter: meterOf(state, { kind: 'saved' }, savedName, status.plan, status.checkAfter, status.checkBills) };
}

/** A turn whose CLI the server placed (`placement`): the reason it runs
 *  nowhere, naming the computer that is offline; the own computer that runs
 *  it, which waits as on a check until the list of computers has it; or, on
 *  Home's cloud computer or this server, by the credential. */
function onPlacement(reads: ClaudeCodeReads, placement: ClaudeCodeCliPlacement): WorkspaceClaudeCode {
  const { kind, refusal, computer_id: computerId } = placement;
  if (refusal) {
    const state = REFUSAL_STATES[refusal];
    if (state === 'cli_computer_offline') return { state, computerId: null, known: true, cliComputerName: placement.name };
    if (state === 'needs_bigger_computer') return { state, computerId: null, known: true, smallComputerId: computerId };
    return { state, computerId: null, known: true };
  }
  if (kind === 'home_cloud' || kind === 'server') return onCloud(reads, cloudCredentialState(reads.status, reads.choice), computerId);
  const entry = reads.own.computers.find((c) => c.computer_id === computerId);
  return entry ? onOwnComputer(reads, entry) : { state: 'checking', computerId: null, known: false };
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
 *
 * Where the user's cloud computers run no CLI themselves, a cloud workspace's
 * CLI runs on the own computer the server picked (`cli_computer`) while its
 * tools stay on the cloud computer, so the turn reads as on that computer:
 * its token, plan and meter, or the reason none can run it. Only a cloud
 * credential that runs nowhere gives way to it, which a server from before
 * the pick still says.
 */
export function useWorkspaceClaudeCode(workspaceId: string | null | undefined): WorkspaceClaudeCode {
  const reads = useClaudeCodeReads();
  const { status, own, choice } = reads;
  const workspace = useWorkspace(workspaceId);
  const workspaceKnown = !workspaceId
    || (workspace.data !== undefined && !workspace.isPlaceholderData)
    || [403, 404].includes(apiErrorStatus(workspace.error) ?? 0);
  const known = workspaceKnown && !own.isLoading && !own.unread;
  if (!status.enabled) return { state: 'off', computerId: null, known };
  if (!known) return { state: 'checking', computerId: null, known };
  const computerId = workspace.data?.computer_id;
  const entry = own.computers.find((c) => c.computer_id === computerId);
  if (entry) return onOwnComputer(reads, entry);
  const credential = cloudCredentialState(status, choice);
  const cli = credential === 'needs_own_computer' || credential === 'hosted_off' ? own.cliComputer : null;
  return cli ? onPlacement(reads, cli) : onCloud(reads, credential, computerId);
}

/**
 * Where Claude Code stands for a Chief of Staff turn: where the server places
 * its CLI (`ClaudeCodeSetup.chief_of_staff`), read as a workspace's turn is
 * on that placement. Off while the server names none, and waits as on a check
 * until the setup and the list of computers are read. Not read while
 * `enabled` is false.
 */
export function useChiefOfStaffClaudeCode(enabled: boolean): WorkspaceClaudeCode {
  const reads = useClaudeCodeReads();
  const { setup, isLoading, unread } = useClaudeCodeSetup({ enabled });
  const known = !isLoading && !unread && !reads.own.isLoading && !reads.own.unread;
  if (!reads.status.enabled) return { state: 'off', computerId: null, known };
  if (!known) return { state: 'checking', computerId: null, known };
  const placement = setup?.chief_of_staff;
  return placement ? onPlacement(reads, placement) : { state: 'off', computerId: null, known };
}

/** Where Claude Code stands for a turn: the Chief of Staff's (`home`), else
 *  the workspace's. */
export function useTurnClaudeCode(home: boolean, workspaceId: string | null | undefined): WorkspaceClaudeCode {
  const workspace = useWorkspaceClaudeCode(home ? null : workspaceId);
  const chiefOfStaff = useChiefOfStaffClaudeCode(home);
  return home ? chiefOfStaff : workspace;
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
