/**
 * djb2 over a string's UTF-16 units, as a signed 32-bit integer. Not for
 * anything an adversary picks: it keys caches and tints, and some of those
 * keys are stored (a widget's remembered height), so its output must not change.
 */
export function djb2(text: string): number {
  let h = 5381;
  for (let i = 0; i < text.length; i++) h = ((h << 5) + h + text.charCodeAt(i)) | 0;
  return h;
}
