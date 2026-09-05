import { mapOutsideFences, mapOutsideMultilineCode } from '../../utils/markdownSegments';

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
 * Collapses soft-wrapped prose within one fence-free span.
 *
 * The span's own leading and trailing newlines are put back untouched: they are
 * the boundary with an adjacent fence, and trimming them would glue this text
 * onto the fence line.
 */
function collapseSoftWraps(span: string): string {
  const lead = /^\n*/.exec(span)?.[0] ?? '';
  if (lead.length === span.length) return span;
  const trail = /\n*$/.exec(span)?.[0] ?? '';
  const body = span.slice(lead.length, span.length - trail.length);
  if (!body.trim()) return span;

  const collapsed = body
    .split(/\n{2,}/)
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

  return lead + collapsed + trail;
}

/**
 * Normalize text content from backend for proper display in subagent views.
 * - Unescape literal \n (backslash-n) if backend sends escaped strings
 * - Collapse single newlines to spaces ONLY between plain prose lines
 * - Preserve newlines adjacent to markdown-structural lines (headings, lists, etc.)
 * - Preserve double newlines (paragraph breaks)
 *
 * Both passes skip fenced code. Schema-constrained subagents answer with a JSON
 * object, where a `\n` inside a string value is content rather than an escaping
 * artifact, and reflowing the body would destroy the indentation that makes it
 * readable in the first place.
 */
export function normalizeSubagentText(content: string | null | undefined): string {
  if (!content || typeof content !== 'string') return '';
  const normalized = content.replace(/\r\n/g, '\n').replace(/\r/g, '\n');
  // Unescaping can reveal fences that were not distinct lines beforehand, so
  // the second pass re-splits instead of reusing the first pass's spans. It is
  // also why only the reflow guards inline code: before this runs, a code span
  // written as a literal `\n` does not cross a line yet.
  const unescaped = mapOutsideFences(normalized, (prose) =>
    prose.replace(/\\n/g, '\n')
  );
  return mapOutsideMultilineCode(unescaped, collapseSoftWraps);
}
