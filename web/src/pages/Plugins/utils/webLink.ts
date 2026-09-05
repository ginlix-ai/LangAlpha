/**
 * Manifest URLs, reduced to the ones worth putting behind an anchor.
 *
 * `homepage` and `repository` are free-form strings in a plugin manifest and
 * the schema keeps them that way, so an installed package can name a
 * `javascript:` URL that the detail view would render as a link the user
 * clicks. Both fields resolve through here rather than each testing the
 * scheme at its own render site, which is how `repository` arrived without
 * the check `homepage` already had.
 */
export function webLink(value: string | null | undefined): string | null {
  return value && /^https?:\/\//i.test(value) ? value : null;
}
