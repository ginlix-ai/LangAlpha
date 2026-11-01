// C0 and C1 controls but newline and tab, DEL, and the bidi overrides.
const UNPRINTABLE = /[\u0000-\u0008\u000b-\u001f\u007f-\u009f\u202a-\u202e\u2066-\u2069]/g;

/** Text the server, the issuer or the agent wrote, safe to show at a
 *  terminal: an escape sequence in a hostile page's error or a thread title
 *  could otherwise rewrite what is shown, set the window title, or write to
 *  the clipboard. ``--json`` is left whole for
 *  the program reading it; JSON escapes the C0 controls there. */
export function printable(text: string): string {
  return text.replace(UNPRINTABLE, "");
}

export function print(json: boolean, value: unknown, human: () => string): void {
  process.stdout.write(json ? `${JSON.stringify(value)}\n` : `${printable(human())}\n`);
}

/** A plain aligned table; long cells are kept whole, since ids get copied. */
export function table(rows: Record<string, unknown>[], columns: string[]): string {
  if (rows.length === 0) return "(none)";
  const cells = rows.map((r) => columns.map((c) => cell(r[c])));
  const widths = columns.map((c, i) => Math.max(c.length, ...cells.map((row) => row[i]!.length)));
  const line = (values: string[]) => values.map((v, i) => v.padEnd(widths[i]!)).join("  ").trimEnd();
  return [line(columns.map((c) => c.toUpperCase())), ...cells.map(line)].join("\n");
}

/** Widest a table cell prints; ``--json`` carries the whole value. */
const CELL_MAX = 60;

function cell(v: unknown): string {
  if (v === null || v === undefined) return "-";
  const text = typeof v === "string" ? v.replace(/\s+/g, " ") : String(v);
  return text.length > CELL_MAX ? `${text.slice(0, CELL_MAX - 1)}…` : text;
}
