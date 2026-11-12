import type { ClaudeCodePlanLimit, PlanRefusal } from '@/api/model';
import type { StructuredError } from '@/utils/rateLimitError';
import { isPlanTime, isPlanWindow, planRefusalText, planTime, type PlanClock } from '@/lib/claudeCodePlan';

/** The i18n key for each hint the server puts under an upstream failure
 *  (``_upstream_hints`` in ``stream_errors.py``). A hint not listed here is
 *  dropped off the wire rather than rendered as a raw key. */
export const UPSTREAM_HINT_I18N_KEY = {
  api_key: 'chat.errorHintApiKey',
  model_access: 'chat.errorHintModelAccess',
  provider_balance: 'chat.errorHintProviderBalance',
  provider_quota: 'chat.errorHintProviderQuota',
  provider_status: 'chat.errorHintProviderStatus',
  provider_overloaded: 'chat.errorHintProviderOverloaded',
  rate_limited: 'chat.errorHintRateLimited',
  content_blocked: 'chat.errorHintContentBlocked',
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

/** A subscription's usage limit that ended a turn: the plan by its display
 *  name, and the window and reset where the provider said them. */
export interface PlanLimit extends ClaudeCodePlanLimit {
  plan: string;
}

const isHint = (value: unknown): value is UpstreamErrorHint =>
  typeof value === 'string' && Object.hasOwn(UPSTREAM_HINT_I18N_KEY, value);
const isPayer = (value: unknown): value is UpstreamPayer =>
  (UPSTREAM_PAYERS as readonly unknown[]).includes(value);
const nonEmpty = (value: unknown): string | undefined =>
  typeof value === 'string' && value.trim() ? value.trim() : undefined;

/** A subscription's usage limit off the wire. The plan names the card, so
 *  without one there is no card; a window or reset the app can't word is
 *  dropped alone, and the card says as much as is left. */
function readPlanLimit(value: unknown): PlanLimit | undefined {
  if (!value || typeof value !== 'object') return undefined;
  const { plan, window, until } = value as Record<string, unknown>;
  const name = nonEmpty(plan);
  if (!name) return undefined;
  return { plan: name, window: isPlanWindow(window) ? window : null, until: isPlanTime(until) ? until : null };
}

/** The fields only an upstream failure's frame carries, each read safely, so
 *  a frame from an older or newer server reads as far as it can. ``reason``
 *  passes as sent: it only labels a report, where a reason this client does
 *  not know yet is the one worth seeing. */
export function readUpstreamDetail(
  event: Record<string, unknown>,
): Pick<StructuredError, 'hints' | 'payer' | 'account' | 'reason' | 'planLimit'> {
  return {
    hints: Array.isArray(event.hints) ? event.hints.filter(isHint) : undefined,
    payer: isPayer(event.payer) ? event.payer : undefined,
    account: nonEmpty(event.account),
    reason: nonEmpty(event.reason),
    planLimit: readPlanLimit(event.plan_limit),
  };
}

type Translate = (key: string, opts?: Record<string, unknown>) => string;

/**
 * A plan's limit that ended a turn: the Claude Code plan's refusal, or
 * another subscription's limit. It is the plan working, not a failure, so its
 * card has a headline and a sentence of its own, with no provider words,
 * account line or hints, and nothing to report.
 */
export type PlanStop = { kind: 'claude'; refusal: PlanRefusal } | { kind: 'subscription'; limit: PlanLimit };

export function planStopOf(err: StructuredError): PlanStop | null {
  if (err.planRefusal) return { kind: 'claude', refusal: err.planRefusal };
  if (err.planLimit) return { kind: 'subscription', limit: err.planLimit };
  return null;
}

/** A failure the server classified that the team can act on: a plan's limit
 *  is neither. */
export const isReportable = (err: StructuredError): boolean =>
  (err.kind === 'upstream' || err.kind === 'internal') && !planStopOf(err);

/**
 * A plan stop's headline and sentence, the reset in the reader's zone and
 * language. A subscription's sentence says as much as the provider did: the
 * window and the reset, the reset alone, or neither. Its window names are
 * this card's own, since Claude's name the Claude plan in Chinese.
 */
export function planStopCopy(t: Translate, stop: PlanStop, clock: PlanClock): { title: string; text: string } {
  if (stop.kind === 'claude') {
    return { title: t('claudeCodePlan.limitTitle'), text: planRefusalText(t, stop.refusal, clock) };
  }
  const { plan, window, until } = stop.limit;
  const title = t('chat.planLimitTitle', { plan });
  if (!until) return { title, text: t('chat.planLimitUnknown', { plan }) };
  const time = planTime(until, clock);
  const text = window
    ? t('chat.planLimitWindowReset', { plan, window: t(`chat.planLimitWindow.${window}`), time })
    : t('chat.planLimitReset', { plan, time });
  return { title, text };
}

export interface UpstreamFailureCopy {
  headline: string;
  /** The provider's own words, or the generic line that stands for them. */
  detail: string | null;
  /** Whose account the failure came from, when it is the user's to fix. */
  source: string | null;
  /** Whether the provider's words for each model tried may show: only where
   *  the user pays, since on credits they are the service's. */
  providerWords: boolean;
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
      providerWords: false,
    };
  }
  const status = err.statusCode;
  const headline = model
    ? (status
        ? t('chat.errorUpstreamHeadlineModelStatus', { model, status })
        : t('chat.errorUpstreamHeadlineModel', { model }))
    : (status ? t('chat.errorUpstreamHeadlineStatus', { status }) : t('chat.errorUpstreamHeadline'));
  return { headline, detail: err.message || null, source: sourceLine(err, t), providerWords: true };
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
