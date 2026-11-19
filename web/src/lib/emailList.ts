/** Addresses as typed: one per line, or separated by commas, semicolons or
 *  spaces, each once. */
export function parseEmails(text: string): string[] {
  return [...new Set(text.split(/[\s,;]+/).map((e) => e.trim()).filter(Boolean))];
}
