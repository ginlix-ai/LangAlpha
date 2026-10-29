/**
 * The one spelling a vault name takes, wherever a name is typed or proposed.
 *
 * It runs on every keystroke as well as on a generated suggestion, which is
 * what rules out tidying: collapsing repeated underscores or trimming a
 * trailing one makes a name that legitimately holds one impossible to type,
 * because the character is erased the moment it lands. Mapping an illegal
 * character to `_` rather than deleting it is what lets a hand-typed name
 * reproduce the suggestion offered beside it -- the two used to disagree, so
 * retyping `acme-api-key` gave `ACMEAPIKEY` where the suggestion said
 * `ACME_API_KEY`. A leading digit is deleted because no legal name starts
 * with one. The cut at the vault's 64-character limit is for the generated
 * suggestion: a server name near its own 64-character limit plus a header
 * suffix would land in the field past what `maxLength` lets anyone type, and
 * Save would then fail on every attempt until the name was shortened by hand.
 */
export const MAX_SECRET_NAME_LENGTH = 64;

export function normalizeSecretName(raw: string): string {
  return raw
    .toUpperCase()
    .replace(/[^A-Z0-9_]/g, '_')
    .replace(/^[0-9]+/, '')
    .slice(0, MAX_SECRET_NAME_LENGTH);
}

/** The vault's own rule for a name: what it stores, as spelled. */
export function isLegalSecretName(name: string): boolean {
  return /^[A-Za-z_][A-Za-z0-9_]{0,63}$/.test(name);
}

/**
 * The name a typed or prefilled name is stored under. A legal name is kept as
 * it stands, so a lowercase name a connector asks for by a deep link is
 * stored under the spelling that connector reads; anything else takes the
 * spelling every name field types in.
 */
export function savedSecretName(raw: string): string {
  return isLegalSecretName(raw) ? raw : normalizeSecretName(raw);
}

/** The longest value the vault stores. */
export const MAX_SECRET_VALUE_LENGTH = 4096;
