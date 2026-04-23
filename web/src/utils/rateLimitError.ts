/**
 * Shared rate-limit error builder.
 *
 * Constructs a structured error from a 429 response's `rateLimitInfo`
 * so both ChatAgent and MarketView produce identical messages and,
 * when the account portal URL is configured, a "View Usage" deep-link.
 */

export interface RateLimitErrorInfo {
  type?: string;
  used_credits?: number;
  credit_limit?: number;
  current?: number;
  limit?: number;
  message?: string;
  [key: string]: unknown;
}

/** Hints the backend emits for upstream provider failures — each maps to an
 *  i18n-bound bullet the user sees ("check your API key", etc.). Keep in sync
 *  with the ``hints`` list in ``streaming_handler.format_error_event``. */
export type UpstreamErrorHint =
  | 'api_key'
  | 'model_access'
  | 'provider_status'
  | 'try_another_model';

/** The allowlist used to sanitize ``hints`` coming off the SSE wire. Any hint
 *  not in this set is dropped — protects renderers from rendering unknown
 *  strings as i18n keys. */
export const UPSTREAM_HINT_KEYS: readonly UpstreamErrorHint[] = [
  'api_key',
  'model_access',
  'provider_status',
  'try_another_model',
];

/** i18n key lookup for each hint. Keep renderers in sync by importing this
 *  rather than re-declaring the map at each call site. */
export const UPSTREAM_HINT_I18N_KEY: Record<UpstreamErrorHint, string> = {
  api_key: 'chat.errorHintApiKey',
  model_access: 'chat.errorHintModelAccess',
  provider_status: 'chat.errorHintProviderStatus',
  try_another_model: 'chat.errorHintTryAnotherModel',
};

export function isUpstreamHint(value: unknown): value is UpstreamErrorHint {
  return UPSTREAM_HINT_KEYS.includes(value as UpstreamErrorHint);
}

export interface StructuredError {
  message: string;
  link?: { url: string; label: string };
  /** ``upstream`` = LLM provider's fault (their 5xx/401/429). ``internal`` =
   *  our pipeline. Undefined for rate-limit errors built on the client. */
  kind?: 'upstream' | 'internal';
  /** HTTP status from the upstream provider, when known. */
  statusCode?: number;
  /** Bulleted guidance to render under the message. */
  hints?: UpstreamErrorHint[];
}

export function buildRateLimitError(
  info: RateLimitErrorInfo,
  accountUrl?: string | null,
): StructuredError {
  let message: string;

  if (info.type === 'credit_limit') {
    message = `Daily credit limit reached (${info.used_credits}/${info.credit_limit} credits). Resets at midnight UTC.`;
  } else if (info.type === 'negative_balance') {
    message = (info.message as string) || 'Outstanding credit balance. Please add credits to continue.';
  } else if (info.type === 'workspace_limit') {
    message = `Active workspace limit reached (${info.current}/${info.limit}). Stop or delete an existing workspace to free up a slot.`;
  } else if (info.type === 'burst_limit') {
    message = `Too many concurrent requests. Please wait a moment.`;
  } else {
    message = (info.message as string) || 'Rate limit exceeded. Please try again later.';
  }

  const link =
    accountUrl && (info.type === 'credit_limit' || info.type === 'negative_balance')
      ? { url: `${accountUrl}/usage`, label: 'View Usage' }
      : undefined;

  return { message, link };
}
