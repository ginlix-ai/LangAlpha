/**
 * Axios/fetch error normalization helpers.
 */

/**
 * The raw `detail` an axios/fetch failure carries, whatever its shape (string,
 * FastAPI validation array, or a structured object). Every reader of a wire
 * error starts here, so the cast ladder down to it is written once: it had
 * already been re-derived in the plugins API layer and again inside a
 * component, each copy free to spell the path differently.
 */
export function apiErrorDetail(err: unknown): unknown {
  return (err as { response?: { data?: { detail?: unknown } } })?.response?.data?.detail;
}

/**
 * Extract the `message` from a structured object-shaped `detail` (e.g. the
 * platform's 429 quota payload `{ message, type, current, limit, remaining }`).
 * Returns null for string/array/absent details so callers can fall back to
 * their own copy.
 */
export function apiErrorDetailMessage(err: unknown): string | null {
  const detail = apiErrorDetail(err);
  if (detail && typeof detail === 'object' && !Array.isArray(detail)) {
    const message = (detail as { message?: unknown }).message;
    if (typeof message === 'string' && message) return message;
  }
  return null;
}

/**
 * Normalize an axios/fetch error into a readable message.
 *
 * Reads `err.response.data.detail`. FastAPI emits a string for most errors,
 * but validation failures come back as a list of `{ loc, msg }` entries — those
 * are flattened to `loc.path: msg` joined with `'; '` so the UI never renders
 * `[object Object]`. A structured object detail (e.g. the platform quota
 * payload) surfaces its `message` field. Falls back to `err.message` then a
 * generic label.
 */
export function formatApiErrorDetail(err: unknown): string {
  const detail = apiErrorDetail(err);
  if (Array.isArray(detail)) {
    const parts = detail
      .map((entry) => {
        const e = entry as { loc?: unknown[]; msg?: unknown };
        const loc = Array.isArray(e?.loc) ? e.loc.map(String).join('.') : '';
        const msg = typeof e?.msg === 'string' ? e.msg : JSON.stringify(entry);
        return loc ? `${loc}: ${msg}` : msg;
      })
      .filter(Boolean);
    if (parts.length > 0) return parts.join('; ');
  }
  const objectMessage = apiErrorDetailMessage(err);
  if (objectMessage) return objectMessage;
  if (typeof detail === 'string' && detail) return detail;
  const message = (err as { message?: unknown })?.message;
  return typeof message === 'string' && message ? message : 'Request failed';
}

/** Extract the HTTP status code from an axios error, or null if absent. */
export function apiErrorStatus(err: unknown): number | null {
  const status = (err as { response?: { status?: unknown }; status?: unknown })?.response?.status
    ?? (err as { status?: unknown })?.status;
  return typeof status === 'number' ? status : null;
}

// --- Workspaces ---
