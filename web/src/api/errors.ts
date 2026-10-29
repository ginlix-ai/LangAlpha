/**
 * API client and fetch error normalization helpers.
 */

/**
 * The raw `detail` an API client or fetch failure carries, whatever its shape (string,
 * FastAPI validation array, or a structured object). Every reader of a wire
 * error starts here, so the cast ladder down to it is written once: it had
 * already been re-derived in the plugins API layer and again inside a
 * component, each copy free to spell the path differently.
 */
export function apiErrorDetail(err: unknown): unknown {
  return (err as { response?: { data?: { detail?: unknown } } })?.response?.data?.detail;
}

/**
 * The detail when it is a structured object, the shape every coded refusal
 * and the platform's quota payload take; null for a string, a validation
 * list, or no detail at all.
 */
export function apiErrorObject(err: unknown): Record<string, unknown> | null {
  const detail = apiErrorDetail(err);
  return detail && typeof detail === 'object' && !Array.isArray(detail)
    ? (detail as Record<string, unknown>)
    : null;
}

/** The structured detail of a refusal that carries `code`, or null for any
 *  other error. */
export function apiErrorCoded(err: unknown, code: string): Record<string, unknown> | null {
  const detail = apiErrorObject(err);
  return detail?.code === code ? detail : null;
}

/**
 * Extract the `message` from a structured object-shaped `detail` (e.g. the
 * platform's 429 quota payload `{ message, type, current, limit, remaining }`).
 * Returns null for string/array/absent details so callers can fall back to
 * their own copy.
 */
export function apiErrorDetailMessage(err: unknown): string | null {
  const message = apiErrorObject(err)?.message;
  return typeof message === 'string' && message ? message : null;
}

/** One entry of a FastAPI 422: where the request was refused, and why. */
export interface ApiValidationError {
  /** The innermost named field, or null for the request body as a whole. */
  field: string | null;
  /** The whole location as FastAPI wrote it, `body.tier`; '' when it gave none. */
  loc: string;
  /** Pydantic's sentence, without the "Value error, " a model validator
   *  puts before its own text. */
  message: string;
}

/** The entries of a FastAPI validation refusal; empty for any other error. */
export function apiValidationErrors(err: unknown): ApiValidationError[] {
  const detail = apiErrorDetail(err);
  if (!Array.isArray(detail)) return [];
  return detail.flatMap((entry: { loc?: unknown; msg?: unknown } | null) => {
    const msg = entry?.msg;
    if (typeof msg !== 'string' || !msg) return [];
    const loc: unknown[] = Array.isArray(entry?.loc) ? entry.loc : [];
    const named = loc.filter((part): part is string => typeof part === 'string' && part !== 'body');
    return [
      {
        field: named.at(-1) ?? null,
        loc: loc.map(String).join('.'),
        message: msg.replace(/^Value error,\s*/, ''),
      },
    ];
  });
}

/**
 * Normalize an API client or fetch error into a readable message.
 *
 * Reads `err.response.data.detail`. A validation list is flattened to
 * `loc.path: msg` joined with `'; '` so the UI never renders
 * `[object Object]`. A structured object detail (e.g. the platform quota
 * payload) surfaces its `message` field. Falls back to `err.message` then a
 * generic label.
 */
export function formatApiErrorDetail(err: unknown): string {
  const invalid = apiValidationErrors(err);
  if (invalid.length > 0) {
    return invalid.map((e) => (e.loc ? `${e.loc}: ${e.message}` : e.message)).join('; ');
  }
  const objectMessage = apiErrorDetailMessage(err);
  if (objectMessage) return objectMessage;
  const detail = apiErrorDetail(err);
  if (typeof detail === 'string' && detail) return detail;
  const message = (err as { message?: unknown })?.message;
  return typeof message === 'string' && message ? message : 'Request failed';
}

/**
 * The reason the server gave, or null when it gave none. A surface that leads
 * with its own sentence wants only this: the client's own message ("Request
 * failed with status code 500") restates the failure rather than explaining it.
 */
export function apiErrorReason(err: unknown): string | null {
  if (apiErrorDetail(err) == null) return null;
  const reason = formatApiErrorDetail(err);
  return reason === (err as { message?: unknown })?.message ? null : reason;
}

/** Extract the HTTP status code from an API client error, or null if absent. */
export function apiErrorStatus(err: unknown): number | null {
  const status = (err as { response?: { status?: unknown }; status?: unknown })?.response?.status
    ?? (err as { status?: unknown })?.status;
  return typeof status === 'number' ? status : null;
}

/** A 4xx: the request itself was refused, so asking again gets the same answer. */
export function isClientError(err: unknown): boolean {
  const status = apiErrorStatus(err);
  return status !== null && status >= 400 && status < 500;
}

/** Refusals that can pass: an expired session, a timeout, a rate limit. */
const PASSING_CLIENT_ERRORS = new Set([401, 408, 429]);

/** A 4xx that asking again won't change, so not one of the refusals that can pass. */
export function isLastingRefusal(err: unknown): boolean {
  return isClientError(err) && !PASSING_CLIENT_ERRORS.has(apiErrorStatus(err) ?? 0);
}

/** React Query `retry`: twice for a server or network failure, never for a 4xx. */
export function retryUnlessClientError(failureCount: number, err: unknown): boolean {
  return failureCount < 2 && !isClientError(err);
}
