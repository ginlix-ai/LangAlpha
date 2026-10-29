import { isUpstreamHint, RECONNECT_ACCOUNT_LINK, type StructuredError } from '@/utils/rateLimitError';
import type { SSEEvent } from '../types';

/**
 * The failure an `error` frame reports, as the server classified it: its
 * `error_kind` says whose fault it was (the model provider's, or this
 * service's), with the status, hints and models an upstream failure carries.
 * Undefined for a frame the server did not classify. The live stream and a
 * reload both read it, so a failed turn shows the same card either way.
 */
export function readStreamFailure(event: SSEEvent, message: string): StructuredError | undefined {
  const kind = event.error_kind;
  if (kind !== 'upstream' && kind !== 'internal') return undefined;
  // Models the resilience middleware tried this turn (primary + fallbacks).
  // Type-filtered defensively: the backend fields may be absent or partial.
  const attemptedModels = Array.isArray(event.attempted_models)
    ? (event.attempted_models as unknown[])
        .filter((m): m is Record<string, unknown> => !!m && typeof m === 'object')
        .map((m) => ({
          model: typeof m.model === 'string' ? m.model : '',
          error: typeof m.error === 'string' ? m.error : undefined,
          statusCode: typeof m.status_code === 'number'
            ? m.status_code
            : (m.status_code === null ? null : undefined),
          attempts: typeof m.attempts === 'number' ? m.attempts : undefined,
        }))
        .filter((m) => m.model)
    : undefined;
  const hints = kind === 'upstream' && Array.isArray(event.hints)
    ? (event.hints.filter(isUpstreamHint) as StructuredError['hints'])
    : undefined;
  return {
    message,
    kind,
    // The hint says the sign-in must be renewed; this is where.
    links: hints?.includes('reconnect_account') ? [RECONNECT_ACCOUNT_LINK] : undefined,
    statusCode: typeof event.status_code === 'number' ? event.status_code : undefined,
    // ``hints`` are only meaningful for upstream provider errors (the bullets
    // say "check your API key / plan / provider status"). An internal error
    // doesn't get hints even if the backend ever starts emitting them for
    // some future variant.
    hints,
    // User-configured model + the full attempt list, so the display can show
    // a model-aware headline and an "Also tried" line.
    model: typeof event.model === 'string' ? event.model : undefined,
    attemptedModels: attemptedModels && attemptedModels.length > 0 ? attemptedModels : undefined,
  };
}
