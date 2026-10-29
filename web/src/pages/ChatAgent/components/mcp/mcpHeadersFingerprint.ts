/**
 * A stable, non-reversible stand-in for a header map, for use in a cache key.
 *
 * The pre-save check's query key has to say which credential the verdict is
 * about, but a query key sits in the QueryClient's cache (and in its devtools)
 * for a gcTime after the form is gone, and the value is a pasted API key. A
 * digest answers the same question, "is this still the same address with the
 * same credential", without spelling the credential out.
 */
export function headersFingerprint(headers: Record<string, string>): string {
  const entries = Object.entries(headers).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
  const text = JSON.stringify(entries);
  // Two FNV-1a passes from different offset bases, concatenated: one 32-bit
  // hash is small enough to collide within a form session, two independent
  // ones are not.
  return `${fnv1a(text, 0x811c9dc5)}${fnv1a(text, 0x01000193)}`;
}

/** FNV-1a over UTF-16 code units, as eight hex digits. */
function fnv1a(text: string, basis: number): string {
  let hash = basis >>> 0;
  for (let i = 0; i < text.length; i += 1) {
    hash ^= text.charCodeAt(i);
    hash = Math.imul(hash, 0x01000193) >>> 0;
  }
  return hash.toString(16).padStart(8, '0');
}
