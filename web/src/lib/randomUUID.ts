/**
 * `crypto.randomUUID()` is restricted to secure contexts (HTTPS or
 * localhost) — it is `undefined` when the app is opened over plain HTTP from
 * another device on the LAN, which silently breaks any unguarded call site.
 * `crypto.getRandomValues()` carries no such restriction, so it backs the
 * fallback: a real RFC 4122 v4 UUID, not just an opaque unique string, since
 * callers (e.g. the request-key idempotency contract) document the format.
 */
export function randomUUID(): string {
  if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
    return crypto.randomUUID();
  }
  if (typeof crypto !== 'undefined' && typeof crypto.getRandomValues === 'function') {
    const bytes = crypto.getRandomValues(new Uint8Array(16));
    bytes[6] = (bytes[6] & 0x0f) | 0x40; // version 4
    bytes[8] = (bytes[8] & 0x3f) | 0x80; // variant 10
    const hex = Array.from(bytes, (b) => b.toString(16).padStart(2, '0')).join('');
    return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
  }
  // Neither Crypto API available (very old browser) — Math.random() isn't
  // cryptographically secure, but the result still has to be UUID-shaped:
  // callers (e.g. the request-key idempotency contract) match against the
  // format, not just uniqueness.
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    const v = c === 'x' ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}
