import type { McpToolSummary } from '@/pages/ChatAgent/utils/api';

/**
 * A tool's description read the way a person reads it rather than the way the
 * model does. The text is a docstring hard-wrapped at eighty columns, with its
 * parameters spelled out under `Args:` and its result shape under `Returns:`;
 * flattened into a paragraph it read as one run-on line of argument names.
 *
 * Parameters come from the input schema, which is what the tool actually
 * accepts, and borrow their sentence from the docstring when it has one. A
 * description in no recognised shape is all summary and body, so a third-party
 * tool loses nothing to the parse.
 */

export interface ToolParam {
  name: string;
  type: string | null;
  required: boolean;
  defaultValue: string | null;
  choices: string[];
  description: string | null;
}

export interface ToolDoc {
  summary: string;
  /** The paragraphs after the summary, and any section we do not lay out. */
  body: string[];
  params: ToolParam[];
  returns: string | null;
}

type Schema = {
  type?: string | string[];
  enum?: unknown[];
  const?: unknown;
  default?: unknown;
  description?: string;
  items?: Schema;
  anyOf?: Schema[];
  oneOf?: Schema[];
  properties?: Record<string, Schema>;
  required?: string[];
};

const SECTION = /^(args|arguments|parameters|params|returns?|raises|yields|examples?|notes?)\s*:\s*$/i;
const ARG = /^\*{0,2}([A-Za-z_][\w-]*)\s*(?:\([^)]*\))?\s*:\s*(.*)$/;
const LIST_ITEM = /^(?:[-*•]\s|\d+[.)]\s)/;

const indentOf = (line: string) => line.length - line.trimStart().length;

function dedent(lines: string[]): string[] {
  const widths = lines.filter((l) => l.trim()).map(indentOf);
  const cut = widths.length ? Math.min(...widths) : 0;
  return lines.map((l) => l.slice(Math.min(cut, indentOf(l))));
}

/** Python's `inspect.cleandoc`: a docstring's first line sits against the
 *  quotes, so only the lines after it say how far the rest is indented. */
function cleandoc(lines: string[]): string[] {
  const [first = '', ...rest] = lines;
  return [first.trim(), ...dedent(rest)];
}

/** Hard-wrapped lines back into paragraphs; list items keep their own line. */
function paragraphs(lines: string[]): string[] {
  const out: string[] = [];
  let current: string[] = [];
  const flush = () => {
    if (current.length) out.push(current.join('\n'));
    current = [];
  };
  for (const raw of lines) {
    const line = raw.trim();
    if (!line) {
      flush();
    } else if (current.length === 0 || LIST_ITEM.test(line)) {
      current.push(line);
    } else {
      current[current.length - 1] += ` ${line}`;
    }
  }
  flush();
  return out;
}

/** `name: sentence` entries, each continued by the more-indented lines under it. */
function argDocs(lines: string[]): Map<string, string> {
  const docs = new Map<string, string>();
  const body = lines.filter((l) => l.trim());
  if (!body.length) return docs;
  const base = indentOf(body[0]);
  let last: string | null = null;
  for (const line of body) {
    const match = indentOf(line) <= base ? ARG.exec(line.trim()) : null;
    if (match) {
      last = match[1];
      docs.set(last, match[2].trim());
    } else if (last) {
      docs.set(last, `${docs.get(last)} ${line.trim()}`.trim());
    }
  }
  return docs;
}

function typeOf(schema: Schema): string | null {
  const branches = (schema.anyOf ?? schema.oneOf)?.filter((s) => s.type !== 'null');
  if (branches?.length) {
    const names = [...new Set(branches.map(typeOf).filter(Boolean))];
    return names.length ? names.join(' | ') : null;
  }
  const type = Array.isArray(schema.type)
    ? schema.type.filter((t) => t !== 'null').join(' | ')
    : schema.type;
  if (type === 'array') {
    const item = schema.items ? typeOf(schema.items) : null;
    return item ? `${item}[]` : 'array';
  }
  return type || null;
}

function choicesOf(schema: Schema): unknown[] {
  if (schema.enum) return schema.enum;
  if (schema.const !== undefined) return [schema.const];
  return (schema.anyOf ?? schema.oneOf ?? []).flatMap(choicesOf);
}

const shown = (value: unknown) =>
  typeof value === 'string' ? value : JSON.stringify(value);

export function readToolDoc(tool: McpToolSummary): ToolDoc {
  const lines = cleandoc((tool.description ?? '').replace(/\r\n?/g, '\n').split('\n'));

  const prose: string[] = [];
  const sections = new Map<string, string[]>();
  let into = prose;
  for (const line of lines) {
    const head = indentOf(line) === 0 ? SECTION.exec(line.trim()) : null;
    if (head) {
      const key = head[1].toLowerCase();
      into = [];
      sections.set(key, into);
    } else {
      into.push(line);
    }
  }

  const [summary = '', ...body] = paragraphs(prose);
  const argSection = ['args', 'arguments', 'parameters', 'params']
    .map((k) => sections.get(k))
    .find(Boolean);
  const docs = argSection ? argDocs(argSection) : new Map<string, string>();
  const returnSection = sections.get('returns') ?? sections.get('return');
  const returns = returnSection ? paragraphs(dedent(returnSection)).join('\n') || null : null;

  // Anything else the docstring carries stays readable, under its own word.
  for (const [key, section] of sections) {
    if (['args', 'arguments', 'parameters', 'params', 'returns', 'return'].includes(key)) continue;
    const text = paragraphs(dedent(section));
    if (text.length) body.push(`${key[0].toUpperCase()}${key.slice(1)}: ${text.join('\n')}`);
  }

  const schema = (tool.input_schema ?? {}) as Schema;
  const required = new Set(schema.required ?? []);
  const properties = Object.entries(schema.properties ?? {});
  const params: ToolParam[] = properties.length
    ? properties.map(([name, prop]) => ({
        name,
        type: typeOf(prop),
        required: required.has(name),
        defaultValue: prop.default == null ? null : shown(prop.default),
        choices: choicesOf(prop).map(shown),
        description: docs.get(name) ?? prop.description ?? null,
      }))
    : [...docs].map(([name, description]) => ({
        name,
        type: null,
        required: false,
        defaultValue: null,
        choices: [],
        description,
      }));

  return { summary, body, params, returns };
}
