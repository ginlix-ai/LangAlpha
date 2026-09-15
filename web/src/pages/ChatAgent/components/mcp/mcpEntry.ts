import { coerceMcpName, parseMcpServersJson, type ParsedImportServer } from './mcpImport';
import type { McpTransport } from '../../utils/api';

/**
 * The add form's one field: a URL, a command line, or a pasted JSON config.
 * Everything the old form asked for up front (transport, command vs url,
 * args) is read off that field here, and the name is suggested from it, so
 * the user types what the server's own docs hand them and nothing else.
 */

export type EntryParse =
  | { kind: 'empty' }
  | { kind: 'remote'; transport: 'http' | 'sse'; url: string }
  | { kind: 'command'; command: string; args: string[] }
  | { kind: 'json'; server: ParsedImportServer; more: number }
  | { kind: 'json-error'; error: string }
  | { kind: 'command-error'; error: string };

/**
 * Split a command line the way a shell would, reporting a quote that never
 * closed. An open quote makes the argv a guess, and a server saved with argv
 * the field does not spell is the one thing the caller has to refuse.
 */
function scanCommand(line: string): { argv: string[]; unterminated: boolean } {
  const out: string[] = [];
  let cur = '';
  let quote: '"' | "'" | null = null;
  let has = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (quote) {
      if (ch === quote) quote = null;
      else if (ch === '\\' && quote === '"' && i + 1 < line.length) cur += line[++i];
      else cur += ch;
      continue;
    }
    if (ch === '"' || ch === "'") {
      quote = ch;
      has = true;
    } else if (/\s/.test(ch)) {
      if (has || cur) out.push(cur);
      cur = '';
      has = false;
    } else if (ch === '\\' && i + 1 < line.length) {
      cur += line[++i];
    } else {
      cur += ch;
    }
  }
  if (has || cur) out.push(cur);
  return { argv: out, unterminated: quote !== null };
}

/** Split a command line the way a shell would, honoring quotes. Lenient on an
 *  unterminated one: the args editor reads a field still being typed into. */
export function shellSplit(line: string): string[] {
  return scanCommand(line).argv;
}

/** Quote an argv element back into a line only when it needs it. */
function shellQuote(word: string): string {
  if (word === '') return "''";
  if (/^[A-Za-z0-9_@%+=:,./-]+$/.test(word)) return word;
  return `'${word.replace(/'/g, "'\\''")}'`;
}

/**
 * The line the field shows for a stored command + args. Only an absent command
 * is dropped: an argument is kept at its position however empty, because the
 * field is where the argument list lives, and a filter here made the args
 * editor's Add button do nothing and clearing a row delete it.
 */
export function commandLine(command: string | null | undefined, args: string[]): string {
  return (command ? [command, ...args] : args).map(shellQuote).join(' ');
}

/** Read the field. A URL is remote; a `{` is a config; anything else is argv. */
export function parseEntry(raw: string): EntryParse {
  const text = raw.trim();
  if (!text) return { kind: 'empty' };
  if (text.startsWith('{')) {
    const res = parseMcpServersJson(text);
    const first = res.servers.find((s) => !s.error) ?? res.servers[0];
    if (res.error || !first || first.error) {
      return { kind: 'json-error', error: res.error ?? first?.error ?? '' };
    }
    return { kind: 'json', server: first, more: res.servers.length - 1 };
  }
  // A bare URL, possibly followed by nothing: `https://` is the whole tell.
  if (/^https?:\/\//i.test(text) && !/\s/.test(text)) {
    return { kind: 'remote', transport: 'http', url: text };
  }
  const { argv, unterminated } = scanCommand(text);
  if (unterminated) return { kind: 'command-error', error: 'Unterminated quote in command.' };
  if (argv.length === 0) return { kind: 'empty' };
  const [command, ...args] = argv;
  return { kind: 'command', command, args };
}

// Labels that name the protocol or the tier rather than the service, dropped
// before a host or path is turned into a name.
const GENERIC = new Set(['mcp', 'www', 'api', 'sse', 'http', 'v1', 'v2', 'app', 'server', 'servers', 'remote']);
// Public suffix labels a two-label host would otherwise hand us as the name.
const TLD_LIKE = new Set(['com', 'net', 'org', 'io', 'ai', 'dev', 'app', 'cn', 'co', 'uk', 'jp', 'de']);

/**
 * A segment that is not valid percent-encoding still names something. `new URL`
 * accepts a lone `%`, so a user halfway through typing `%20` reaches here, and
 * an uncaught URIError would take the form down mid-keystroke.
 */
function decodeSegment(seg: string): string {
  try {
    return decodeURIComponent(seg);
  } catch {
    return seg;
  }
}

function nameFromUrl(url: string): string | null {
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch {
    return null;
  }
  const hostLabels = parsed.hostname.toLowerCase().split('.').filter(Boolean);
  // The service's own label: the first that is neither generic nor a suffix.
  // `mcp.linear.app` → linear; `fuyao.aicubes.cn` → fuyao.
  const host = hostLabels.find((l, i) => !GENERIC.has(l) && !(i >= hostLabels.length - 2 && TLD_LIKE.has(l)));
  const path = parsed.pathname
    .split('/')
    .filter(Boolean)
    .map((seg) => decodeSegment(seg).toLowerCase())
    .filter((seg) => !GENERIC.has(seg) && !/^[0-9a-f-]{16,}$/.test(seg));
  const parts = [host, ...path].filter((p): p is string => !!p);
  return parts.length ? parts.join('_') : null;
}

function nameFromCommand(command: string, args: string[]): string | null {
  // The first positional that looks like a package or a path is the server;
  // flags and the launcher's own subcommands are not.
  const skip = new Set(['-y', '--yes', 'run', 'exec', 'x', '-m', '--from', 'python', 'python3', 'node']);
  const pkg = args.find((a) => !a.startsWith('-') && !skip.has(a));
  const source = pkg ?? command;
  let base = source.split(/[\\/]/).pop() ?? source;
  base = base.replace(/@[^/]+\/?$/, '').replace(/\.(py|js|mjs|ts)$/i, '');
  base = base
    .replace(/^mcp[-_]?server[-_]?/i, '')
    .replace(/[-_]?mcp[-_]?server$/i, '')
    .replace(/^server[-_]?/i, '')
    .replace(/[-_]?server$/i, '')
    .replace(/^mcp[-_]?/i, '')
    .replace(/[-_]?mcp$/i, '');
  return base || null;
}

/** A legal name to propose for what the field holds; empty when nothing fits. */
export function suggestName(
  transport: McpTransport | null,
  url: string,
  command: string,
  args: string[],
): string {
  const raw =
    transport === 'stdio' ? nameFromCommand(command, args) : url ? nameFromUrl(url) : null;
  if (!raw) return '';
  return coerceMcpName(raw.replace(/[-.]/g, '_')).name ?? '';
}
