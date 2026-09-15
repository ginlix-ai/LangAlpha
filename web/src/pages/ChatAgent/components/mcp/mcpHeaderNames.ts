/**
 * The header names nearly every remote MCP server wants, offered as a pick
 * list so the row says what it will send instead of an empty box labelled
 * "Authorization". The stored config is still a plain `{name: value}` pair;
 * a scheme choice only decides the word written in front of the value, so
 * `Authorization: Bearer` stores `Bearer ${vault:TOKEN}` and the vault holds
 * the bare token.
 */

export type HeaderChoiceId = 'bearer' | 'basic' | 'x-api-key' | 'x-api-key-mixed' | 'api-key' | 'custom';

export interface HeaderChoice {
  id: HeaderChoiceId;
  /** The exact header name sent; empty for custom. */
  name: string;
  /** The word written in front of the value, when the header has one. */
  scheme?: 'Bearer' | 'Basic';
}

export const HEADER_CHOICES: readonly HeaderChoice[] = [
  { id: 'bearer', name: 'Authorization', scheme: 'Bearer' },
  { id: 'basic', name: 'Authorization', scheme: 'Basic' },
  { id: 'x-api-key', name: 'X-API-Key' },
  { id: 'x-api-key-mixed', name: 'X-Api-Key' },
  { id: 'api-key', name: 'api-key' },
  { id: 'custom', name: '' },
];

/** The label shown for a choice: the header line it produces. */
export function choiceLabel(c: HeaderChoice): string {
  return c.scheme ? `${c.name}: ${c.scheme}` : c.name;
}

export interface SplitHeader {
  choice: HeaderChoiceId;
  /** The header name as stored, so a vendor's own spelling survives an edit. */
  name: string;
  scheme: 'Bearer' | 'Basic' | null;
  /** The value without its scheme word. */
  inner: string;
}

const SCHEME_RE = /^(Bearer|Basic)\s+(.*)$/is;

/** Read a stored `{key: value}` pair back into a choice and a bare value. */
export function splitHeader(key: string, value: string): SplitHeader {
  if (key.toLowerCase() === 'authorization') {
    const m = value.match(SCHEME_RE);
    if (m) {
      const scheme = m[1].toLowerCase() === 'bearer' ? 'Bearer' : 'Basic';
      return { choice: scheme === 'Bearer' ? 'bearer' : 'basic', name: key, scheme, inner: m[2] };
    }
    // A bare Authorization value with no scheme is unusual enough to show
    // as what it is rather than silently gain a Bearer prefix.
    if (value !== '') return { choice: 'custom', name: key, scheme: null, inner: value };
    return { choice: 'bearer', name: key, scheme: 'Bearer', inner: '' };
  }
  const listed = HEADER_CHOICES.find((c) => c.id !== 'custom' && !c.scheme && c.name === key);
  if (listed) return { choice: listed.id, name: key, scheme: null, inner: value };
  return { choice: 'custom', name: key, scheme: null, inner: value };
}

/** The stored `{key, value}` pair for a choice, a custom name and a bare value. */
export function joinHeader(
  choice: HeaderChoiceId,
  customName: string,
  inner: string,
): { key: string; value: string } {
  const c = HEADER_CHOICES.find((x) => x.id === choice) ?? HEADER_CHOICES[0];
  const key = c.id === 'custom' ? customName : c.name;
  // No value yet means no scheme word either: a row holding just "Bearer "
  // would read as a filled credential to every guard downstream.
  const value = c.scheme && inner !== '' ? `${c.scheme} ${inner}` : inner;
  return { key, value };
}
