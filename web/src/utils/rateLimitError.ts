/**
 * Shared rate-limit error builder.
 *
 * Turns a 429 response's `rateLimitInfo` into a structured error so ChatAgent,
 * MarketView and the workspace modal all render a denial the same way. The
 * wording is the quota service's; this module only decides where the user is
 * sent to resolve it.
 */

import type { PlanRefusal } from '@/api/model';
import type { PlanLimit, UpstreamErrorHint, UpstreamPayer } from '@/lib/upstreamFailure';

/** A 429's `detail`, as it comes off the wire. Only the two fields this module
 *  acts on are named: the counts the quota service also sends are already folded
 *  into `message`, so naming them here only invites reading them back out. */
export interface RateLimitErrorInfo {
  type?: string;
  message?: string;
  [key: string]: unknown;
}

/** Where a subscription whose sign-in lapsed is renewed. The server's refusal
 *  of a turn carries the same link already worded; a failure mid-stream names
 *  only the hint, so the client supplies the destination. */
export const RECONNECT_ACCOUNT_LINK: ErrorLinkSpec = {
  url: '/settings?tab=model',
  label: 'Reconnect',
  labelKey: 'chat.errorLinkReconnect',
};

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
  /** A page of the account console: a new tab in a browser, and this window
   *  in the desktop app, which shows the console there and would send a new
   *  tab to the default browser, signed out (as `ConsoleLink`). */
  console?: boolean;
  /** The cloud computer to bring to the size Claude Code runs at: the link
   *  reads as what does that for this user (`MediumActionLink`). */
  resize?: string;
}

export interface StructuredError {
  message: string;
  /** Rendered in order after the message; the first is the primary action. */
  links?: ErrorLinkSpec[];
  /** ``upstream`` = LLM provider's fault (their 5xx/401/429). ``internal`` =
   *  our pipeline. Undefined for rate-limit errors built on the client.
   *  ``notice`` = nothing failed, so it reads as information, not an error. */
  kind?: 'upstream' | 'internal' | 'notice';
  /** A turn a Claude plan's limit ended, worded where it is drawn, in the
   *  reader's zone and language, under its own headline. */
  planRefusal?: PlanRefusal;
  /** HTTP status from the upstream provider, when known. */
  statusCode?: number;
  /** Bulleted guidance to render under the message. */
  hints?: UpstreamErrorHint[];
  /** Who the provider billed for an upstream failure. Absent on a frame from
   *  before the server named it, which reads as it always did. */
  payer?: UpstreamPayer;
  /** The user's account the failure came from (``DeepSeek``, ``ChatGPT``),
   *  when they pay and the server could name it. */
  account?: string;
  /** What the provider said went wrong, where the server could tell. */
  reason?: string;
  /** A subscription's usage limit, which reads as the plan working rather
   *  than as a failure: its own headline and sentence, in the reader's zone. */
  planLimit?: PlanLimit;
  /** User-configured (primary) model name, when the failure is model-attributable.
   *  Drives the model-aware error headline. */
  model?: string;
  /** Every model the resilience middleware attempted this turn (primary +
   *  fallbacks). When more than one, the display shows an "Also tried" line. */
  attemptedModels?: Array<{ model: string; error?: string; statusCode?: number | null; attempts?: number }>;
  /** The run that failed, when the frame names it, for a report to name. */
  runId?: string;
}

/** The one 429 this service raises on its own: the concurrency gate in
 *  ``chat_limit_for``. Never travels through the quota service, so it
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
