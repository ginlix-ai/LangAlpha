import type { ClaudeCodeCustodyMode } from '@/api/model';
import type { ConfiguredProvider } from '@/hooks/useConfiguredProviders';
import type { ErrorLinkSpec } from '@/utils/rateLimitError';

/** The provider and sdk of the models a turn runs on Claude Code. */
export const CLAUDE_CODE = 'claude-code';

/** A model a turn runs on Claude Code: an analyst's conversation only. */
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

/** Settings → Model access. */
export const MODEL_ACCESS_URL = '/settings?tab=model';

/** Settings → Model access with the setup-token sheet open, the one place a
 *  token is added, which `ModelAccessSection` reads off the query. */
export const CLAUDE_CODE_TOKEN_PARAM = 'connect';
export const CLAUDE_CODE_TOKEN_URL = `${MODEL_ACCESS_URL}&${CLAUDE_CODE_TOKEN_PARAM}=${CLAUDE_CODE}`;

/** The provider whose API key Claude Code runs on on cloud computers, and
 *  Settings → Model access with that key's sheet open, by the same query. */
export const ANTHROPIC = 'anthropic';
export const ANTHROPIC_KEY_URL = `${MODEL_ACCESS_URL}&${CLAUDE_CODE_TOKEN_PARAM}=${ANTHROPIC}`;

/** What each way Claude Code can run on cloud computers is called, in its
 *  sheet and on its row in Model access. */
export const CLAUDE_CODE_MODE_LABELS: Readonly<Record<ClaudeCodeCustodyMode, string>> = {
  local: 'modelAccess.claudeCode.hosted.local',
  api_key: 'modelAccess.claudeCode.hosted.apiKey',
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
  token_refused: 'chat.claudeCode.off.token_refused',
  hosted_off: 'chat.claudeCode.off.hosted_off',
  hosted_paused: 'chat.claudeCode.off.hosted_paused',
  needs_api_key: 'chat.claudeCode.off.needs_api_key',
  api_key_endpoint: 'chat.claudeCode.off.api_key_endpoint',
  stored_blocked: 'chat.claudeCode.off.stored_blocked',
  stored_unattended: 'chat.claudeCode.off.stored_unattended',
  choice_changed: 'chat.claudeCode.off.choice_changed',
  api_key_refused: 'chat.claudeCode.off.api_key_refused',
  busy: 'chat.claudeCode.off.busy',
  too_many_turns: 'chat.claudeCode.off.too_many_turns',
};

const CLAUDE_CODE_CHOICE_LINK: ErrorLinkSpec = { url: CLAUDE_CODE_CHOICE_URL, label: 'Model access', labelKey: 'modelAccess.title' };

/** The links that fix a cloud computer's refusal: choosing how Claude Code
 *  runs there, or adding the Anthropic API key it runs on. */
const CLAUDE_CODE_CLOUD_LINKS: Readonly<Record<string, ErrorLinkSpec>> = {
  hosted_off: CLAUDE_CODE_CHOICE_LINK,
  stored_unattended: CLAUDE_CODE_CHOICE_LINK,
  needs_api_key: { url: ANTHROPIC_KEY_URL, label: 'Add an API key', labelKey: 'chat.errorLinkAddApiKey' },
  api_key_endpoint: { url: ANTHROPIC_KEY_URL, label: 'Model access', labelKey: 'modelAccess.title' },
};

/** A cloud computer's refusals that say the user's choice for cloud
 *  computers, the key it runs on, or what the server offers, is not what the
 *  view read. */
const CLAUDE_CODE_CHOICE_CHANGED: ReadonlySet<string> = new Set(['hosted_off', 'hosted_paused', 'needs_api_key', 'api_key_endpoint', 'stored_blocked', 'choice_changed']);

/**
 * What a turn's `claude_code_off` refusal means, wherever it arrives:
 * whether the server's next read of the tokens and of the user's choice for
 * cloud computers changed, so they are read again, the link that fixes it,
 * and the key it is worded by where the app words it (`messageKey`, with
 * `messageValues`). No token (`signed_out`) or one Claude refused
 * (`token_refused`), stored or not, is fixed with a new one in Settings; a
 * computer where Claude Code cannot run (`CLAUDE_CODE_UNRUNNABLE`) is fixed
 * by no token. On a cloud computer, the choice of how it runs there
 * (`hosted_off`, and `stored_unattended` for automations and wakeups, which
 * a stored token does not run) or the Anthropic API key it runs on
 * (`needs_api_key`, or `api_key_endpoint` for one set to another endpoint)
 * fixes it; a choice paused on the server or storing
 * blocked for the user has nothing to fix. Too many turns at once names the
 * user's cap, `limit`, and keeps the server's words without one. Any other
 * reason keeps the server's words alone.
 */
export function claudeCodeRefusal(
  reason: unknown, workspaceId: string | null | undefined, limit?: unknown,
): { reread: boolean; link: ErrorLinkSpec | null; messageKey: string | null; messageValues?: { n: number } } {
  if (typeof reason !== 'string') return { reread: false, link: null, messageKey: null };
  if (reason === 'too_many_turns') {
    return typeof limit === 'number'
      ? { reread: false, link: null, messageKey: CLAUDE_CODE_OFF_KEYS[reason], messageValues: { n: limit } }
      : { reread: false, link: null, messageKey: null };
  }
  const messageKey = CLAUDE_CODE_OFF_KEYS[reason] ?? null;
  if (reason === 'signed_out' || reason === 'token_refused') {
    return { reread: true, link: claudeCodeTokenLink(workspaceId), messageKey };
  }
  return {
    reread: CLAUDE_CODE_UNRUNNABLE.has(reason) || CLAUDE_CODE_CHOICE_CHANGED.has(reason),
    link: CLAUDE_CODE_CLOUD_LINKS[reason] ?? null,
    messageKey,
  };
}
