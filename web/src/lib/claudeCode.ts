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

export const CLAUDE_CODE_TOKEN_LINK: ErrorLinkSpec = {
  url: CLAUDE_CODE_TOKEN_URL,
  label: 'Add setup token',
  labelKey: 'modelAccess.claudeCode.add',
};
