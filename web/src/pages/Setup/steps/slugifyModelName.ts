/**
 * Auto-normalize a user-typed display name into the slug the backend accepts.
 * Backend regex (``src/llms/llm.py:CUSTOM_MODEL_NAME_RE``):
 *   ^[a-zA-Z0-9][a-zA-Z0-9._:/-]{0,62}$
 *
 * Rules: lowercase, spaces → dashes, strip characters outside
 * ``[a-z0-9._:/-]``, ensure alphanumeric start, cap at 63 chars. Lowercasing
 * is a UI convention — the regex accepts upper case, but consistency across
 * menus is friendlier.
 */
export function slugifyModelName(input: string): string {
  return input
    .toLowerCase()
    .trim()
    .replace(/\s+/g, '-')
    .replace(/[^a-z0-9._:/-]/g, '')
    .replace(/^[^a-z0-9]+/, '')
    .slice(0, 63);
}
