/**
 * What is owed to someone whose connect left this tab and came back, with an
 * answer or without one.
 *
 * Some authorization servers allowlist only the RFC 8252 native-app profile: a
 * loopback callback on the user's own machine. They refuse a hosted one at the
 * authorize step, and a refusal there is silent by construction — the vendor's
 * page renders its own error and never redirects, so no code and no error ever
 * reaches us. The desktop shell can hold that listener and complete the flow; a
 * browser tab has nothing to offer.
 *
 * *Which* connectors those are is the brokerage registry's answer and whether
 * this build could finish one is `canBeginMcpOAuth`'s. Neither lives here. What
 * does is the part no advance note can cover: a provider can refuse for reasons
 * nobody listed, and the user is owed a sentence either way.
 */
import { refsOnlyValue } from '@/pages/ChatAgent/components/mcp/mcpSchemas';
import type { McpServerInput, PendingSignIn } from '@/pages/ChatAgent/utils/api';
import { readConnectOutcome, type ConnectOutcome } from './connectOutcome';
import { CONNECTS_KEY } from './hooks/useConnectLanding';

/** Enough for a few sign-ins out at once in the desktop app, and no more. */
const CONNECTS_MAX = 8;

type Pending<K extends PendingSignIn['kind']> = Extract<PendingSignIn, { kind: K }>;

/**
 * How to put the user back if a sign-in does not finish: the save that waited
 * on it, less what cannot be kept (a zip, typed keys), or the Discover entry it
 * was installing.
 */
export type ConnectRetry =
  | { kind: 'catalog'; entry: string }
  | Pick<Pending<'plugin'>, 'kind' | 'sourceUrl' | 'subdir'>
  /** A zip cannot be kept; the wizard opens to ask for it again. */
  | Pick<Pending<'plugin_upload'>, 'kind'>
  | Pending<'server'>
  | Pick<Pending<'server_edit'>, 'kind' | 'name' | 'server'>
  /** Anything with no form to restore: a reconnect, a brokerage, a workspace's. */
  | { kind: 'other' };

/**
 * One connect this tab sent out, kept from the moment it starts until a
 * landing settles it.
 *
 * Session-scoped and same-tab on purpose: that is exactly the span of the flow.
 * A closed tab took its unfinished connect with it, and there is nobody left to
 * explain it to. More than one can be out at a time: inside the shell the
 * authorize URL opens in the system browser, so the page never leaves and the
 * other rows stay clickable.
 */
export interface ConnectRecord {
  /** The name the callback answers with, which is what the record is found by. */
  server: string;
  /** The path of the page the connect started from, which is where Back lands. */
  from: string;
  /** What the user knows it as, which can be a plugin's or a vendor's name. */
  label: string;
  retry: ConnectRetry;
  /** The vendor refuses a sign-in that does not come home through the
   *  desktop app, so in a browser there is nothing to try again. */
  desktopOnly: boolean;
  /**
   * The page went to the vendor. Only such a connect can be backed out of:
   * the desktop app never leaves, and a browser can refuse the navigation, so
   * a record marked at the start read as abandoned while the flow was live or
   * had never gone at all.
   */
  left: boolean;
  /**
   * In the desktop app, when the shell gives the flow up (epoch ms), or null.
   * The vendor's page is in the system browser there, so this page never
   * leaves, and a load of it with nothing to say is a reload while the flow is
   * still live, until the shell's timeout brings the window back with no
   * answer.
   */
  shellGivesUpAt: number | null;
  /**
   * Where the connect asked its callback to land, path and query, or null
   * from a build that did not keep it. A callback the server can no longer
   * tie to its flow (a sign-in slower than the flow's state lives) can only
   * fall back to the Plugins page, and this is the way back from there.
   */
  returnTo: string | null;
}

/**
 * How long the desktop app holds a connect it sent to the system browser
 * before bringing the window back without an answer, which is also how long
 * the flow's state lives on the server. Read a little short, since the shell's
 * clock starts a round trip before this page's does, and a landing that
 * timeout causes must not be read as early.
 */
const SHELL_FLOW_MS = 10 * 60_000 - 30_000;

/**
 * A connector body with every value that could be a key taken out.
 *
 * Session storage outlives the form and is readable by any script on the
 * page, so a value typed into a header or an env var is not kept. One that
 * names a stored secret is only a name, and keeping it is what saves the user
 * from picking it again.
 */
function withoutKeys(server: McpServerInput): McpServerInput {
  const refsOnly = (values?: Record<string, string>) =>
    values && Object.fromEntries(Object.entries(values).map(([k, v]) => [k, refsOnlyValue(v)]));
  return { ...server, env: refsOnly(server.env), headers: refsOnly(server.headers) };
}

function keepable(retry: ConnectRetry): ConnectRetry {
  return retry.kind === 'server' || retry.kind === 'server_edit'
    ? { ...retry, server: withoutKeys(retry.server) }
    : retry;
}

const RETRY_KINDS = new Set(['catalog', 'plugin', 'plugin_upload', 'server', 'server_edit', 'other']);

function readConnects(): ConnectRecord[] {
  try {
    const parsed: unknown = JSON.parse(sessionStorage.getItem(CONNECTS_KEY) ?? 'null');
    if (!Array.isArray(parsed)) return [];
    return parsed.flatMap((v) => {
      const r = v as { [K in keyof ConnectRecord]?: unknown } | null;
      if (!r || typeof r.server !== 'string') return [];
      const retry = r.retry as { kind?: unknown } | undefined;
      // Lenient on purpose: a marker an older build wrote is `{server}` alone,
      // left by a connect that went to the vendor from this page.
      return [
        {
          server: r.server,
          from: typeof r.from === 'string' ? r.from : '/plugins',
          label: typeof r.label === 'string' ? r.label : r.server,
          retry:
            typeof retry?.kind === 'string' && RETRY_KINDS.has(retry.kind)
              ? (r.retry as ConnectRetry)
              : { kind: 'other' },
          desktopOnly: r.desktopOnly === true,
          left: r.left !== false,
          shellGivesUpAt: typeof r.shellGivesUpAt === 'number' ? r.shellGivesUpAt : null,
          returnTo: typeof r.returnTo === 'string' ? r.returnTo : null,
        },
      ];
    });
  } catch {
    // Unreadable or not a shape any build wrote. Either way there is nothing
    // here to explain to anyone.
    return [];
  }
}

function writeConnects(records: ConnectRecord[]): void {
  try {
    if (records.length) {
      sessionStorage.setItem(CONNECTS_KEY, JSON.stringify(records.slice(-CONNECTS_MAX)));
    } else {
      sessionStorage.removeItem(CONNECTS_KEY);
    }
  } catch {
    // Private windows, blocked site data: the user retypes, nothing breaks.
  }
}

/**
 * Keep what this connect is doing, under the page it starts from. A second
 * connect for a server already out replaces the first: the callback names the
 * server and nothing of the flow, so the newer one is the flow still live.
 */
export function rememberConnect(
  record: Pick<ConnectRecord, 'server' | 'label' | 'retry' | 'desktopOnly' | 'returnTo'>,
): void {
  const kept: ConnectRecord = {
    ...record,
    retry: keepable(record.retry),
    from: window.location.pathname,
    left: false,
    shellGivesUpAt: null,
  };
  writeConnects([...readConnects().filter((r) => r.server !== record.server), kept]);
}

/** The page has gone to the vendor for this connect. */
export function markConnectLeft(server: string): void {
  writeConnects(readConnects().map((r) => (r.server === server ? { ...r, left: true } : r)));
}

/** The desktop app has sent this connect to the system browser. */
export function markConnectHandedOff(server: string): void {
  const shellGivesUpAt = Date.now() + SHELL_FLOW_MS;
  writeConnects(readConnects().map((r) => (r.server === server ? { ...r, shellGivesUpAt } : r)));
}

/** What a landing says about the connects this tab sent out. */
export type ConnectReturn =
  /** The callback's answer, and the connect it settles, when this tab sent
   *  it. Its params arrive in the address bar, so without a record they are
   *  not proof of anything the user did. */
  | { kind: 'answered'; outcome: ConnectOutcome; record: ConnectRecord | null }
  /** A connect that went to the vendor and came back with nothing to say. */
  | { kind: 'abandoned'; record: ConnectRecord };

/**
 * Read a landing at `search` once, and take the record it settles out of
 * storage: left behind, it would have some later, ordinary visit announce a
 * connect that resolved long ago. Null when the landing says nothing.
 */
export function settleConnectReturn(search: string): ConnectReturn | null {
  const records = readConnects();
  const outcome = readConnectOutcome(new URLSearchParams(search));
  if (outcome) {
    // One flow answered, whichever page it left from: the callback lands here
    // for every surface. The rest stay, since on the desktop another may still
    // be out. An answer that names no server settles the only connect out when
    // there is exactly one, and otherwise names nobody.
    const settled = outcome.server ?? (records.length === 1 ? records[0].server : null);
    const record = records.find((r) => r.server === settled) ?? null;
    if (record) writeConnects(records.filter((r) => r !== record));
    return { kind: 'answered', outcome, record };
  }
  // No answer at all: backing out of the vendor's page, or in the desktop app
  // the shell giving the flow up. Back returns to the page a connect left
  // from, and the shell to the page it started on, so only this page's are
  // this landing's to explain, and of those, in a browser exactly one can have
  // left. With several out nothing here names which, and blaming a live
  // broker reads worse than staying quiet, so they are left to their own
  // callbacks.
  const now = Date.now();
  const over = (r: ConnectRecord) =>
    r.left || (r.shellGivesUpAt !== null && now >= r.shellGivesUpAt);
  const mine = records.filter((r) => over(r) && r.from === window.location.pathname);
  if (mine.length !== 1) return null;
  writeConnects(records.filter((r) => r !== mine[0]));
  return { kind: 'abandoned', record: mine[0] };
}
