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
