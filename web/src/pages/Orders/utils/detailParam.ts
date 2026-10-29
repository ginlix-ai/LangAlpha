/**
 * The `?detail=order:<attempt_id>` deep link for the orders overlay.
 *
 * Same shape as the Plugins page's `?detail=` param and deliberately its own
 * copy: that one's kinds are a closed set of catalog rows, and an order is
 * addressed by an opaque id rather than a name.
 */

const PREFIX = 'order:';

export function parseOrderDetail(params: URLSearchParams): string | null {
  const raw = params.get('detail');
  if (!raw || !raw.startsWith(PREFIX)) return null;
  return raw.slice(PREFIX.length) || null;
}

/** A copy of `params` with the detail key set, or cleared for null. */
export function withOrderDetail(
  params: URLSearchParams,
  attemptId: string | null,
): URLSearchParams {
  const next = new URLSearchParams(params);
  if (attemptId) next.set('detail', `${PREFIX}${attemptId}`);
  else next.delete('detail');
  return next;
}
