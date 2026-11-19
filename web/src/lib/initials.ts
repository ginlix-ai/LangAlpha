/** Up to two letters naming a person, from their name or else their email:
 *  one per word, first characters only, as the account menu draws them. */
export function initialsOf(name: string | null | undefined, email?: string | null): string {
  const source = name || email || '';
  if (!source) return '';
  const letters = source
    .split(/[\s@.]+/)
    .filter(Boolean)
    .slice(0, 2)
    // Array.from: an emoji or an astral CJK character is two code units.
    .map((s) => Array.from(s)[0]?.toUpperCase() ?? '')
    .join('');
  // Bound the characters, not the parts: uppercasing can turn one into two.
  return Array.from(letters).slice(0, 2).join('');
}
