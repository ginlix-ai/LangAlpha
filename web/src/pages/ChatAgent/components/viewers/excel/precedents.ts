/**
 * The cells a formula reads. Enough of a parser to tint what feeds the selected
 * cell, plain A1 references, ranges, and a sheet qualifier, and nothing more:
 * a name, a table reference or a computed address is left alone rather than
 * guessed at, because a wrong tint reads as a fact about the model.
 */
import { boxOf, CELL_SOURCE, parseCellRef, SHEET_SOURCE, unquoteSheet, type CellBox } from '@/pages/ChatAgent/utils/a1';

export interface FormulaRef {
  /** Absent when the reference named no sheet, which reads as the cell's own. */
  sheet?: string;
  box: CellBox;
  /** As written, so the formula bar can echo a cross-sheet reference verbatim. */
  text: string;
}

const REF_RE = new RegExp(`(?:(${SHEET_SOURCE})!)?(${CELL_SOURCE})(?::(${CELL_SOURCE}))?`, 'g');

// A reference never continues an identifier, and never introduces a call:
// `LOG10(` is a function and `[1]Sheet1!A1` points into another workbook.
const CONTINUES = /[A-Za-z0-9_$.!\]]/;

/** Quoted text is prose, and blanking it keeps every index aligned. */
function blankStrings(formula: string): string {
  return formula.replace(/"[^"]*"/g, (m) => ' '.repeat(m.length));
}

export function extractRefs(formula: string): FormulaRef[] {
  const src = blankStrings(formula.replace(/^=/, ' '));
  const out: FormulaRef[] = [];
  const seen = new Set<string>();
  REF_RE.lastIndex = 0;
  let m: RegExpExecArray | null;
  while ((m = REF_RE.exec(src)) !== null) {
    const before = m.index > 0 ? src[m.index - 1] : '';
    const after = src[m.index + m[0].length] ?? '';
    if (before && CONTINUES.test(before)) continue;
    if (after === '(' || /[A-Za-z0-9_]/.test(after)) continue;
    const a = parseCellRef(m[2]);
    const b = m[3] ? parseCellRef(m[3]) : a;
    if (!a || !b) continue;
    const text = m[0];
    if (seen.has(text)) continue;
    seen.add(text);
    out.push({ sheet: m[1] ? unquoteSheet(m[1]) : undefined, box: boxOf(a, b), text });
  }
  return out;
}

export interface Precedents {
  /** Boxes on the sheet holding the formula, the ones to tint. */
  local: CellBox[];
  /** References into another sheet, as written. Named in the bar, never tinted:
   *  the cells are not on screen to tint. */
  external: string[];
}

export function precedentsOf(formula: string | undefined | null, sheetName: string): Precedents {
  if (!formula) return { local: [], external: [] };
  const here = sheetName.toLowerCase();
  const local: CellBox[] = [];
  const external: string[] = [];
  for (const ref of extractRefs(formula)) {
    if (!ref.sheet || ref.sheet.toLowerCase() === here) local.push(ref.box);
    else external.push(ref.text);
  }
  return { local, external };
}
