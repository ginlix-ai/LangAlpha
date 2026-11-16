/**
 * The SHA-256 of `text` as UTF-8, lowercase hex, as a save names the text its
 * edit began from. Null where the page has no WebCrypto (a plain-HTTP origin
 * other than localhost); the save then stands on its version alone.
 */
export async function textSha256(text: string): Promise<string | null> {
  try {
    const digest = await globalThis.crypto.subtle.digest('SHA-256', new TextEncoder().encode(text));
    return Array.from(new Uint8Array(digest), (b) => b.toString(16).padStart(2, '0')).join('');
  } catch {
    return null;
  }
}
