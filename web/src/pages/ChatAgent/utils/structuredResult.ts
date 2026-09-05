/**
 * Recognizes a subagent final message that is entirely one JSON object.
 *
 * A schema-constrained workflow child is told to answer with a bare JSON value,
 * so its "message" is a data structure that the transcript would otherwise show
 * as a raw dump. Detecting that case lets the view render fields instead.
 */

export type StructuredValue =
  | string
  | number
  | boolean
  | null
  | StructuredValue[]
  | { [key: string]: StructuredValue };

export interface StructuredResult {
  /** Top-level fields, in the order the child emitted them. */
  entries: [string, StructuredValue][];
  /** Source for the raw-JSON disclosure. */
  raw: string;
  /** Set when `entries` is only the part of a clipped payload that survived. */
  truncated?: boolean;
}

/** A single fence wrapping the whole message, with an optional info string. */
const FENCE_WRAP_RE = /^```[\w-]*[ \t]*\n([\s\S]*?)\n?```$/;

/** Strings first, so every digit run left in the body is a number literal. */
const JSON_STRING_RE = /"(?:[^"\\]|\\.)*"/g;
const JSON_NUMBER_RE = /-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?/g;

/**
 * Whether every number in the source survives a `JSON.parse` round trip.
 *
 * JSON bounds no number; a double does. `9007199254740993` silently becomes
 * `...992`, and `1e400` becomes `Infinity` and reserializes as `null` — and
 * because the raw disclosure is regenerated from the parsed object, the
 * fields and the "raw" view would agree on a value the subagent never sent.
 * Declining renders the message as its own text instead.
 *
 * Only integers are held to exactness. Decimal notation is lossy against
 * binary floating point by construction, so demanding it round-trip would
 * reject ordinary prices.
 */
function numbersSurviveParsing(body: string): boolean {
  const literals = body.replace(JSON_STRING_RE, '""').match(JSON_NUMBER_RE);
  if (!literals) return true;
  return literals.every((literal) => {
    const value = Number(literal);
    if (!Number.isFinite(value)) return false;
    return /[.eE]/.test(literal) || Number.isSafeInteger(value);
  });
}

/**
 * Parses a whole-message JSON object, or returns null.
 *
 * Strict on purpose. A message with prose around the JSON is still prose, and
 * rendering only the object's fields would silently drop that prose. Arrays and
 * scalars are rejected too: with no keys there are no labels to render, so a
 * code block remains the better display.
 */
export function parseStructuredResult(
  content: string | null | undefined
): StructuredResult | null {
  if (!content || typeof content !== 'string') return null;
  const trimmed = content.trim();
  const body = (FENCE_WRAP_RE.exec(trimmed)?.[1] ?? trimmed).trim();
  // Cheap reject ahead of the parse: nearly every message is prose, and none of
  // them should pay for a throwing JSON.parse on each render.
  if (!body.startsWith('{') || !body.endsWith('}')) return null;

  let parsed: unknown;
  try {
    parsed = JSON.parse(body);
  } catch {
    return null;
  }
  if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) {
    return null;
  }
  if (!numbersSurviveParsing(body)) return null;

  const entries = Object.entries(parsed as Record<string, StructuredValue>);
  if (entries.length === 0) return null;
  return { entries, raw: JSON.stringify(parsed, null, 2) };
}

/** Mirrors the marker the driver appends to a clipped payload. */
const TRUNCATION_MARKER_RE = /\n?\.\.\. \[truncated\]\s*$/;

/** How far back to walk looking for a boundary that closes cleanly. */
const MAX_SALVAGE_ATTEMPTS = 64;

/**
 * Recovers the members of a clipped JSON object that arrived whole.
 *
 * Cuts back to a member boundary outside any string and closes the brackets
 * still open there. Everything before that boundary is untouched source, so the
 * result is a faithful prefix rather than a guess — the caller still has to
 * label it as partial, which is why this only ever runs behind
 * `parseResultPreview`.
 */
function salvageTruncatedObject(
  text: string
): Record<string, StructuredValue> | null {
  const open: string[] = [];
  const boundaries: string[] = [];
  const cuts: number[] = [];
  let inString = false;
  let escaped = false;

  for (let i = 0; i < text.length; i++) {
    const ch = text[i];
    if (inString) {
      if (escaped) escaped = false;
      else if (ch === '\\') escaped = true;
      else if (ch === '"') inString = false;
      continue;
    }
    if (ch === '"') inString = true;
    else if (ch === '{' || ch === '[') open.push(ch);
    else if (ch === '}' || ch === ']') open.pop();
    else if (ch === ',' && open.length > 0) {
      cuts.push(i);
      boundaries.push(open.join(''));
      if (cuts.length > MAX_SALVAGE_ATTEMPTS) {
        cuts.shift();
        boundaries.shift();
      }
    }
  }

  for (let i = cuts.length - 1; i >= 0; i--) {
    const closers = boundaries[i]
      .split('')
      .reverse()
      .map((ch) => (ch === '{' ? '}' : ']'))
      .join('');
    try {
      const parsed: unknown = JSON.parse(text.slice(0, cuts[i]) + closers);
      if (
        parsed !== null &&
        typeof parsed === 'object' &&
        !Array.isArray(parsed) &&
        Object.keys(parsed).length > 0
      ) {
        return parsed as Record<string, StructuredValue>;
      }
    } catch {
      // Boundary did not close cleanly — step back to an earlier one.
    }
  }
  return null;
}

/**
 * Parses a workflow run's result preview, falling back to a partial recovery.
 *
 * The preview is byte-clipped before it leaves the driver, so a large result
 * arrives unparseable and used to render as raw text. A preview that fails to
 * parse was necessarily clipped: the driver serializes it with `json.dumps`, so
 * an intact payload always parses.
 */
export function parseResultPreview(
  content: string | null | undefined
): StructuredResult | null {
  const intact = parseStructuredResult(content);
  if (intact) return intact;
  if (!content || typeof content !== 'string') return null;

  const body = content.replace(TRUNCATION_MARKER_RE, '').trim();
  if (!body.startsWith('{')) return null;
  const salvaged = salvageTruncatedObject(body);
  if (!salvaged) return null;

  const entries = Object.entries(salvaged);
  if (entries.length === 0) return null;
  // The disclosure shows what actually arrived, marker and all, rather than a
  // re-serialized copy of the recovered prefix.
  return { entries, raw: content, truncated: true };
}

/** `total_revenue` → `Total revenue`; `marketCap` → `Market Cap`; `EPS` → `EPS`. */
export function humanizeKey(key: string): string {
  const spaced = key
    .replace(/[_-]+/g, ' ')
    .replace(/([a-z0-9])([A-Z])/g, '$1 $2')
    .trim();
  if (!spaced) return key;
  return spaced.charAt(0).toUpperCase() + spaced.slice(1);
}
