import type { ClaudeCodeComputer, ClaudeCodeCustodyMode, ClaudeCodeSetup, ClaudeCodeSetupState, PlanRefusal } from '@/api/model';
import type { RowStatus } from '@/components/modelAccess/parts';
import { DOCS_URL, PLAN_URL } from '@/config/supportLinks';
import { readPlanRefusal } from '@/lib/claudeCodePlan';
import type { ConfiguredProvider } from '@/hooks/useConfiguredProviders';
import type { WorkspaceClaudeCode } from '@/hooks/useWorkspaceClaudeCode';
import type { ErrorLinkSpec } from '@/utils/rateLimitError';

/** The provider and sdk of the models a turn runs on Claude Code. */
export const CLAUDE_CODE = 'claude-code';

/** The flag whose per-user override is the user's opt-in, made by adding a
 *  first token under Model access. */
export const CLAUDE_CODE_FEATURE = 'claude_code_runtime';

/** A model a turn runs on Claude Code. */
export const isClaudeCodeModel = (meta?: { sdk?: string }) => meta?.sdk === CLAUDE_CODE;

/** What the access filters count as connected while Claude Code is listed:
 *  its models run on the setup token the user saved for it, never on one of
 *  their other connections. */
export const CLAUDE_CODE_CONNECTION: ConfiguredProvider = {
  provider: CLAUDE_CODE,
  display_name: 'Claude Code',
  access_type: 'oauth',
  state: 'connected',
};

/** Where the user docs explain Claude Code mode, and the sections its
 *  surfaces link to in place of explaining it themselves. */
export const CLAUDE_CODE_DOCS_URL = `${DOCS_URL}/concepts/claude-code/`;
export const CLAUDE_CODE_DOCS = {
  howItRuns: `${CLAUDE_CODE_DOCS_URL}#how-it-runs`,
  requirements: `${CLAUDE_CODE_DOCS_URL}#requirements`,
  whereItRuns: `${CLAUDE_CODE_DOCS_URL}#where-it-runs-and-when-it-can-run`,
  setUp: `${CLAUDE_CODE_DOCS_URL}#set-it-up`,
  fromClaudeSignIn: `${CLAUDE_CODE_DOCS_URL}#moving-from-the-claude-sign-in`,
} as const;

/** Claude Code's status word on its row under Connect a subscription and in
 *  Model access, by where it stands as a whole. */
export const CLAUDE_CODE_SETUP_STATUS: Readonly<Record<ClaudeCodeSetupState, RowStatus>> = {
  connected: 'connected',
  checking: 'working',
  refused: 'needs_reauth',
  not_connected: 'not_connected',
  offline: 'offline',
  unsupported: 'unavailable',
  no_computer: 'needs_computer',
};

/** The version of the consent copy this bundle shows before a user stores
 *  their setup token, which the store sends: a tab still showing an older
 *  copy is refused rather than recorded as accepting the current one. The
 *  server's `hosted.DISCLOSURE`, pinned equal by test_hosted. */
export const CLAUDE_CODE_DISCLOSURE = '2026-11-v2';

/** The states of a Claude Code token that works. */
export const TOKEN_WORKS: ReadonlySet<string> = new Set(['signed_in', 'unverified']);

/** Whether a computer's token works by its last check and has not passed
 *  its expiry, as its own line reads it and as admission refuses one past it. */
export function computerTokenWorks(c: ClaudeCodeComputer, now: number): boolean {
  return TOKEN_WORKS.has(c.state) && !(c.expires_at && Date.parse(c.expires_at) <= now);
}

/** Settings → Model access. */
export const MODEL_ACCESS_URL = '/settings?tab=model';

/** `biggerComputer`'s wording, with a plan page or without, for a workspace
 *  or Home. */
const BIGGER_COMPUTER_KEYS = {
  plan: {
    workspace: { rowKey: 'chat.modelSelector.claudeCodeNeedsBiggerComputerPlan', messageKey: 'chat.claudeCode.off.needs_bigger_computer_plan' },
    home: { rowKey: 'chat.modelSelector.claudeCodeNeedsBiggerComputerPlan', messageKey: 'chat.claudeCode.off.needs_bigger_computer_plan_home' },
  },
  spec: {
    workspace: { rowKey: 'chat.modelSelector.claudeCodeNeedsBiggerComputer', messageKey: 'chat.claudeCode.off.needs_bigger_computer' },
    home: { rowKey: 'chat.modelSelector.claudeCodeNeedsBiggerComputerHome', messageKey: 'chat.claudeCode.off.needs_bigger_computer_home' },
  },
} as const;

/**
 * A cloud computer too small for Claude Code, as the picker row and the
 * refusal word it, the workspace's or Home's (`home`). Where the build names
 * a plan page, a larger computer comes with a plan: worded so, and linked
 * where every Upgrade plan button goes. Elsewhere its spec is changed where
 * the computer is, so nothing links, and Home, which no move puts on a
 * computer of the user's own, has only that.
 */
export function biggerComputer(home: boolean): { rowKey: string; messageKey: string; link: ErrorLinkSpec | null } {
  const keys = BIGGER_COMPUTER_KEYS[PLAN_URL ? 'plan' : 'spec'][home ? 'home' : 'workspace'];
  return PLAN_URL
    ? { ...keys, link: { url: PLAN_URL, label: 'Upgrade plan', labelKey: 'computer.spec.upgradePlan', external: true } }
    : { ...keys, link: null };
}

/** Settings → Model access with the setup-token sheet open, the one place a
 *  token is added, which `ModelAccessSection` reads off the query. */
export const CLAUDE_CODE_TOKEN_PARAM = 'connect';
export const CLAUDE_CODE_TOKEN_URL = `${MODEL_ACCESS_URL}&${CLAUDE_CODE_TOKEN_PARAM}=${CLAUDE_CODE}`;

/** What each way Claude Code can run on cloud computers is called, in its
 *  sheet and on its row in Model access. */
export const CLAUDE_CODE_MODE_LABELS: Readonly<Record<ClaudeCodeCustodyMode, string>> = {
  local: 'modelAccess.claudeCode.hosted.local',
  stored: 'modelAccess.claudeCode.hosted.stored',
};

/** Settings → Model access with Claude Code's sheet open, where the user
 *  chooses how it runs on cloud computers, by the same query. */
export const CLAUDE_CODE_CHOICE = 'claude-code-cloud';
export const CLAUDE_CODE_CHOICE_URL = `${MODEL_ACCESS_URL}&${CLAUDE_CODE_TOKEN_PARAM}=${CLAUDE_CODE_CHOICE}`;

/** With this too, Settings opens the sheet for the token that workspace's
 *  turns run on, which it resolves when it opens: a link made before the
 *  workspace's computer was known still goes to that computer's sheet. */
export const CLAUDE_CODE_WORKSPACE_PARAM = 'workspace';
export const claudeCodeTokenUrl = (workspaceId?: string | null) =>
  (workspaceId ? `${CLAUDE_CODE_TOKEN_URL}&${CLAUDE_CODE_WORKSPACE_PARAM}=${encodeURIComponent(workspaceId)}` : CLAUDE_CODE_TOKEN_URL);

export const claudeCodeTokenLink = (workspaceId?: string | null): ErrorLinkSpec => ({
  url: claudeCodeTokenUrl(workspaceId),
  label: 'Add setup token',
  labelKey: 'modelAccess.claudeCode.add',
});

/** The refusals, each also the state the server records for the computer as
 *  it finds it, of a computer where Claude Code cannot run whatever its
 *  token: the organization's policy there, no perl, or musl. One is news the
 *  list of computers does not have yet, and no token fixes it. */
export const CLAUDE_CODE_UNRUNNABLE: ReadonlySet<string> = new Set(['policy_blocked', 'needs_perl', 'computer_unsupported']);

/** The refusals the app words itself, in the user's language; any other
 *  keeps the server's English words. */
const CLAUDE_CODE_OFF_KEYS: Readonly<Record<string, string>> = {
  account: 'chat.claudeCode.off.account',
  token_refused: 'chat.claudeCode.off.token_refused',
  no_own_computer: 'chat.claudeCode.off.no_own_computer',
  cli_computer_offline: 'chat.claudeCode.off.cli_computer_offline',
  choice_changed: 'chat.claudeCode.off.choice_changed',
  too_many_turns: 'chat.claudeCode.off.too_many_turns',
  needs_bigger_computer: 'chat.claudeCode.off.needs_bigger_computer',
};

const CLAUDE_CODE_CHOICE_LINK: ErrorLinkSpec = { url: CLAUDE_CODE_CHOICE_URL, label: 'Model access', labelKey: 'modelAccess.title' };

/** Why a Claude Code model is no pick, as its picker row says, and the link
 *  that fixes it where the user can. */
export interface ClaudeCodeHold {
  messageKey: string;
  messageValues?: Record<string, string>;
  link: ErrorLinkSpec | null;
}

/** What holds a Claude Code model back where its turns stand in `state`;
 *  null where it runs, a token that could not be checked included, as
 *  admission runs it, and the workspace's own computer out of reach, which
 *  may be back by the time a turn starts. The own computer the server picked
 *  to run the CLI is refused offline (`cli_computer_offline`), named by
 *  `cliComputerName`, and a computer too small is worded for Home where the
 *  turn is the Chief of Staff's (`home`). */
export function claudeCodeHold(
  state: WorkspaceClaudeCode['state'], workspaceId?: string | null,
  { home = false, cliComputerName = null }: { home?: boolean; cliComputerName?: string | null } = {},
): ClaudeCodeHold | null {
  switch (state) {
    case 'checking': return { messageKey: 'modelAccess.status.working', link: null };
    case 'no_token': return { messageKey: 'chat.modelSelector.claudeCodeSignedOut', link: claudeCodeTokenLink(workspaceId) };
    case 'rejected': return { messageKey: 'chat.modelSelector.claudeCodeRejected', link: claudeCodeTokenLink(workspaceId) };
    case 'needs_own_computer': return { messageKey: 'chat.modelSelector.claudeCodeNeedsOwnComputer', link: null };
    case 'hosted_off': return { messageKey: 'chat.modelSelector.claudeCodeHostedOff', link: CLAUDE_CODE_CHOICE_LINK };
    case 'no_own_computer': return { messageKey: 'chat.modelSelector.claudeCodeNoOwnComputer', link: CLAUDE_CODE_CHOICE_LINK };
    case 'cli_computer_offline': return {
      messageKey: 'chat.modelSelector.claudeCodeCliComputerOffline', messageValues: { name: cliComputerName ?? '' }, link: null,
    };
    case 'needs_bigger_computer': {
      const { rowKey, link } = biggerComputer(home);
      return { messageKey: rowKey, link };
    }
    case 'policy_blocked': return { messageKey: 'chat.modelSelector.claudeCodePolicyBlocked', link: null };
    case 'needs_perl': return { messageKey: 'modelAccess.claudeCode.fail.needs_perl', link: null };
    case 'computer_unsupported': return { messageKey: 'modelAccess.claudeCode.fail.computer_unsupported', link: null };
    default: return null;
  }
}

/** A held pin's fix: Model access itself, where each computer's own token
 *  sits beside the saved one. */
const CLAUDE_CODE_PIN_TOKEN_LINK: ErrorLinkSpec = { ...claudeCodeTokenLink(), url: MODEL_ACCESS_URL };

/** What holds a subagent's Claude Code pin back. A pin applies in every
 *  workspace, so it reads the user's Claude Code as a whole, as a pin can
 *  sign in (`pin_state` from a server that still says so, else `state`),
 *  whose first state that holds is a token working somewhere: it is held only where
 *  none works anywhere, worded as the composer words the same state. Unread
 *  waits as on a check, as the composer's rows do. */
export function accountClaudeCodeHold(setup: ClaudeCodeSetup | null): ClaudeCodeHold | null {
  switch (setup?.pin_state ?? setup?.state) {
    case 'connected':
    case 'offline': return null;
    case 'refused': return { messageKey: 'chat.modelSelector.claudeCodeRejected', link: CLAUDE_CODE_PIN_TOKEN_LINK };
    case 'not_connected': return { messageKey: 'chat.modelSelector.claudeCodeSignedOut', link: CLAUDE_CODE_PIN_TOKEN_LINK };
    case 'no_computer': return claudeCodeHold('needs_own_computer');
    case 'unsupported': return { messageKey: 'modelAccess.claudeCode.setup.unsupported.title', link: null };
    default: return claudeCodeHold('checking');
  }
}

/** A cloud computer's refusals that say the user's choice for cloud
 *  computers, or which of their computers are too small, is not what the
 *  view read. */
const CLAUDE_CODE_CHOICE_CHANGED: ReadonlySet<string> = new Set(['choice_changed', 'needs_bigger_computer']);

/**
 * What a turn's `claude_code_off` refusal means, wherever it arrives:
 * whether the server's next read of the tokens and of the user's choice for
 * cloud computers changed, so they are read again, the link that fixes it,
 * and the key it is worded by where the app words it (`messageKey`, with
 * `messageValues`). Claude Code off for the account (`account`) is turned
 * back on in its view under Model access. No token (`signed_out`) or one Claude refused
 * (`token_refused`), stored or not, is fixed with a new one in Settings; a
 * computer where Claude Code cannot run (`CLAUDE_CODE_UNRUNNABLE`) is fixed
 * by no token. A turn whose CLI runs on an own computer the server picks
 * finds none with its own token online (`no_own_computer`), fixed in Claude
 * Code's view where those tokens are, or the picked one, `name`, offline
 * (`cli_computer_offline`), fixed there. A computer too small for Claude
 * Code reads as `biggerComputer` says for the workspace or Home (`home`). Too many
 * turns at once names the user's cap, `limit`, and keeps the server's words
 * without one. A Claude
 * plan's limit (`plan_stopped`, `plan_limited`) is news the plan the view
 * read does not have yet, and carries its window and reset (`planRefusal`)
 * for the card to word in the reader's zone, or keeps the server's words
 * without them. Any other reason keeps the server's words alone.
 */
export function claudeCodeRefusal(frame: Record<string, unknown>, workspaceId: string | null | undefined): {
  reread: boolean;
  link: ErrorLinkSpec | null;
  messageKey: string | null;
  messageValues?: Record<string, string | number>;
  planRefusal?: PlanRefusal;
} {
  const { reason, limit, name } = frame;
  if (typeof reason !== 'string') return { reread: false, link: null, messageKey: null };
  if (reason === 'plan_stopped' || reason === 'plan_limited') {
    return { reread: true, link: null, messageKey: null, planRefusal: readPlanRefusal(frame) ?? undefined };
  }
  if (reason === 'too_many_turns') {
    return typeof limit === 'number'
      ? { reread: false, link: null, messageKey: CLAUDE_CODE_OFF_KEYS[reason], messageValues: { n: limit } }
      : { reread: false, link: null, messageKey: null };
  }
  if (reason === 'needs_bigger_computer') {
    const { link, messageKey } = biggerComputer(frame.home === true);
    return { reread: true, link, messageKey };
  }
  if (reason === 'cli_computer_offline') {
    return typeof name === 'string'
      ? { reread: true, link: null, messageKey: CLAUDE_CODE_OFF_KEYS[reason], messageValues: { name } }
      : { reread: true, link: null, messageKey: null };
  }
  const messageKey = CLAUDE_CODE_OFF_KEYS[reason] ?? null;
  if (reason === 'signed_out' || reason === 'token_refused') {
    return { reread: true, link: claudeCodeTokenLink(workspaceId), messageKey };
  }
  if (reason === 'account' || reason === 'no_own_computer') return { reread: true, link: CLAUDE_CODE_CHOICE_LINK, messageKey };
  return {
    reread: CLAUDE_CODE_UNRUNNABLE.has(reason) || CLAUDE_CODE_CHOICE_CHANGED.has(reason),
    link: null,
    messageKey,
  };
}
