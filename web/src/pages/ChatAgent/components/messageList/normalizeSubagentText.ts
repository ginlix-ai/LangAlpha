// --- Helpers ---

/**
 * Returns true if a line is markdown-structural (headings, lists, blockquotes,
 * code fences, horizontal rules, or table rows) and should keep its newline.
 */
const MD_STRUCTURAL_RE =
  /^(?:#|[*\-+] |\d+[.)] |>|```|---+|___+|\*\*\*+|\||\[)/;

function isStructuralLine(line: string): boolean {
  return MD_STRUCTURAL_RE.test(line.trimStart());
}

/**
 * Normalize text content from backend for proper display in subagent views.
 * - Unescape literal \n (backslash-n) if backend sends escaped strings
 * - Collapse single newlines to spaces ONLY between plain prose lines
 * - Preserve newlines adjacent to markdown-structural lines (headings, lists, etc.)
 * - Preserve double newlines (paragraph breaks)
 */
export function normalizeSubagentText(content: string | null | undefined): string {
  if (!content || typeof content !== 'string') return '';
  const s = content
    .replace(/\\n/g, '\n')
    .replace(/\r\n/g, '\n')
    .replace(/\r/g, '\n');

  const blocks = s.split(/\n{2,}/);
  return blocks
    .map((block) => {
      const trimmed = block.trim();
      const lines = trimmed.split('\n');
      if (lines.length <= 1) return trimmed;

      let result = lines[0];
      for (let i = 1; i < lines.length; i++) {
        const prevStructural = isStructuralLine(lines[i - 1]);
        const curStructural = isStructuralLine(lines[i]);
        result += prevStructural || curStructural ? '\n' : ' ';
        result += lines[i];
      }
      return result;
    })
    .join('\n\n');
}
