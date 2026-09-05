/**
 * Shared rate-limit error builder.
 *
 * Turns a 429 response's `rateLimitInfo` into a structured error so ChatAgent,
 * MarketView and the workspace modal all render a denial the same way. The
 * wording is the quota service's; this module only decides where the user is
 * sent to resolve it.
 */

/** A 429's `detail`, as it comes off the wire. Only the two fields this module
 *  acts on are named: the counts the quota service also sends are already folded
 *  into `message`, so naming them here only invites reading them back out. */
export interface RateLimitErrorInfo {
  type?: string;
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

export interface ErrorLinkSpec {
  url: string;
  /** Literal text, for a label the producer authored (or the service did, on a
   *  link that arrives off the wire already worded). ``labelKey`` wins when
   *  both are set. */
  label: string;
  /** i18n key, for a label this client writes and therefore has to translate.
   *  Resolved by ``ErrorLink`` — this module stays translation-free, the same
   *  split as ``UPSTREAM_HINT_I18N_KEY``. */
  labelKey?: string;
  /** Marks a destination outside this SPA. The URL cannot say so on its own:
   *  with a path-shaped ``VITE_PLATFORM_URL`` the account portal is same-origin
   *  and indistinguishable from one of our routes. See ``ErrorLink`` for what
   *  the flag buys. */
  external?: boolean;
}

export interface StructuredError {
  message: string;
  /** Rendered in order after the message; the first is the primary action. */
  links?: ErrorLinkSpec[];
  /** ``upstream`` = LLM provider's fault (their 5xx/401/429). ``internal`` =
   *  our pipeline. Undefined for rate-limit errors built on the client. */
  kind?: 'upstream' | 'internal';
  /** HTTP status from the upstream provider, when known. */
  statusCode?: number;
  /** Bulleted guidance to render under the message. */
  hints?: UpstreamErrorHint[];
  /** User-configured (primary) model name, when the failure is model-attributable.
   *  Drives the model-aware error headline. */
  model?: string;
  /** Every model the resilience middleware attempted this turn (primary +
   *  fallbacks). When more than one, the display shows an "Also tried" line. */
  attemptedModels?: Array<{ model: string; error?: string; statusCode?: number | null; attempts?: number }>;
}

/** The one 429 this service raises on its own: the concurrency gate in
 *  ``enforce_chat_limit``. Never travels through the quota service, so it
 *  arrives without a message and we supply one. */
const OUR_OWN_LIMIT = 'burst_limit';

/** Denials that no account page resolves — this service's own gate, and the
 *  quota service's misconfiguration guard. Everything else is a cap on the
 *  user's account, and the portal is where they act on it. */
const NOT_ABOUT_THE_ACCOUNT = new Set([OUR_OWN_LIMIT, 'service_unavailable']);

export function buildRateLimitError(
  info: RateLimitErrorInfo,
  platformUrl?: string | null,
): StructuredError {
  // The message is the quota service's, verbatim. It knows the plan, the
  // counts, the user's language and their local day. Rebuilding it here from
  // `type` put billing wording in an open-source client, hard-coded one
  // deployment's answer for every self-hoster, printed "resets at midnight UTC"
  // to everyone outside UTC, and still covered only 5 of the 9 types the
  // service issues — the other 4 came out as "Rate limit exceeded".
  const message =
    info.type === OUR_OWN_LIMIT
      ? 'Too many concurrent requests. Please wait a moment.'
      : (info.message as string) || 'Rate limit exceeded. Please try again later.';

  // A quota denial is answered on the same two pages whichever cap was hit, so
  // offer both rather than mapping type to page: which one needs a top-up and
  // which needs an upgrade is the quota service's rule to change, not ours.
  //
  // The exclusions are the denials no account page can resolve. Sending a user
  // to "Manage plan" while the service is down invites them to buy their way
  // out of an outage.
  //
  // Always `external`: the portal is a separate app, and the denial it
  // interrupts lives entirely in client state — the banner, and a user message
  // the quota gate rejected before anything was persisted. Navigating in place
  // discards both, so the user returns from topping up to a thread that looks
  // like nothing happened.
  const links: ErrorLinkSpec[] | undefined =
    platformUrl && !NOT_ABOUT_THE_ACCOUNT.has(info.type as string)
      ? [
          {
            url: `${platformUrl}/plans`,
            label: 'Manage plan',
            labelKey: 'chat.errorLinkManagePlan',
            external: true,
          },
          {
            url: `${platformUrl}/usage`,
            label: 'View usage',
            labelKey: 'chat.errorLinkViewUsage',
            external: true,
          },
        ]
      : undefined;

  return { message, links };
}
