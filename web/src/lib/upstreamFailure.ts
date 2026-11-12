import type { StructuredError } from '@/utils/rateLimitError';

/** The i18n key for each hint the server puts under an upstream failure
 *  (``_upstream_hints`` in ``stream_errors.py``). A hint not listed here is
 *  dropped off the wire rather than rendered as a raw key. */
export const UPSTREAM_HINT_I18N_KEY = {
  api_key: 'chat.errorHintApiKey',
  model_access: 'chat.errorHintModelAccess',
  provider_balance: 'chat.errorHintProviderBalance',
  provider_status: 'chat.errorHintProviderStatus',
  try_another_model: 'chat.errorHintTryAnotherModel',
  reconnect_account: 'chat.errorHintReconnectAccount',
  retry_later: 'chat.errorHintRetryLater',
  new_thread: 'chat.errorHintNewThread',
  local_server: 'chat.errorHintLocalServer',
} as const;

export type UpstreamErrorHint = keyof typeof UPSTREAM_HINT_I18N_KEY;

/** Who the provider billed for a failed call, as the server's ``payer``
 *  names it. It decides the card: a user who pays reads the provider's own
 *  words and is told the account is theirs; on credits the account is the
 *  service's, and the card stays generic. */
const UPSTREAM_PAYERS = ['own_key', 'subscription', 'local', 'credits', 'server_key'] as const;
export type UpstreamPayer = (typeof UPSTREAM_PAYERS)[number];

const isHint = (value: unknown): value is UpstreamErrorHint =>
  typeof value === 'string' && Object.hasOwn(UPSTREAM_HINT_I18N_KEY, value);
const isPayer = (value: unknown): value is UpstreamPayer =>
  (UPSTREAM_PAYERS as readonly unknown[]).includes(value);

/** The fields only an upstream failure's frame carries, each through an
 *  allowlist, so a frame from an older or newer server reads safely. */
export function readUpstreamDetail(
  event: Record<string, unknown>,
): Pick<StructuredError, 'hints' | 'payer' | 'account'> {
  return {
    hints: Array.isArray(event.hints) ? event.hints.filter(isHint) : undefined,
    payer: isPayer(event.payer) ? event.payer : undefined,
    account: typeof event.account === 'string' && event.account ? event.account : undefined,
  };
}

type Translate = (key: string, opts?: Record<string, unknown>) => string;

export interface UpstreamFailureCopy {
  headline: string;
  /** The provider's own words, or the generic line that stands for them. */
  detail: string | null;
  /** Whose account the failure came from, when it is the user's to fix. */
  source: string | null;
}

/**
 * How an upstream failure reads, by who pays the provider for the call.
 *
 * On credits the account is the service's, so the provider's words are a
 * diagnostic for the team, not something the user can act on: the card says
 * the model could not finish and leaves the rest to the hints. A user who pays
 * reads the provider's message as it came, under a line saying it came from
 * their own key or subscription, since only they can fix that account. A frame
 * with no payer, from before the server named one, reads as it always did.
 */
export function upstreamFailureCopy(
  err: StructuredError,
  t: Translate,
  modelName: (model: string) => string,
): UpstreamFailureCopy {
  const model = err.model ? modelName(err.model) : null;
  if (err.payer === 'credits') {
    return {
      headline: model ? t('chat.errorPlatformHeadlineModel', { model }) : t('chat.errorPlatformHeadline'),
      detail: t('chat.errorPlatformDetail'),
      source: null,
    };
  }
  const status = err.statusCode;
  const headline = model
    ? (status
        ? t('chat.errorUpstreamHeadlineModelStatus', { model, status })
        : t('chat.errorUpstreamHeadlineModel', { model }))
    : (status ? t('chat.errorUpstreamHeadlineStatus', { status }) : t('chat.errorUpstreamHeadline'));
  return { headline, detail: err.message || null, source: sourceLine(err, t) };
}

function sourceLine(err: StructuredError, t: Translate): string | null {
  const account = err.account;
  switch (err.payer) {
    case 'own_key':
      return account ? t('chat.errorSourceOwnKeyNamed', { account }) : t('chat.errorSourceOwnKey');
    case 'subscription':
      return account ? t('chat.errorSourceSubscriptionNamed', { account }) : t('chat.errorSourceSubscription');
    case 'local':
      return t('chat.errorSourceLocal');
    case 'server_key':
      return t('chat.errorSourceServerKey');
    default:
      return null;
  }
}
