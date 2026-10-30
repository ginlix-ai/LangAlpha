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

/** Settings → Model access with the setup-token sheet open, the one place a
 *  token is added, which `ModelAccessSection` reads off the query. */
export const CLAUDE_CODE_TOKEN_PARAM = 'connect';
export const CLAUDE_CODE_TOKEN_URL = `/settings?tab=model&${CLAUDE_CODE_TOKEN_PARAM}=${CLAUDE_CODE}`;

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

/**
 * What a turn's `claude_code_off` refusal means, wherever it arrives:
 * whether the server's next read of the tokens changed, so they are read
 * again, and the link that fixes it. Anthropic refusing the token
 * (`signed_out`) is fixed with a new one in Settings; a computer where
 * Claude Code cannot run (`CLAUDE_CODE_UNRUNNABLE`) is fixed by no token.
 * Any other reason keeps the server's words alone.
 */
export function claudeCodeRefusal(reason: unknown, workspaceId: string | null | undefined): { reread: boolean; link: ErrorLinkSpec | null } {
  if (reason === 'signed_out') return { reread: true, link: claudeCodeTokenLink(workspaceId) };
  return { reread: typeof reason === 'string' && CLAUDE_CODE_UNRUNNABLE.has(reason), link: null };
}
