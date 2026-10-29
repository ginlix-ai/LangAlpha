import type React from 'react';
import { AlertCircle, CheckCircle2, KeyRound, ShieldCheck } from 'lucide-react';
import type { McpCheckVerdict, McpProbeResult, ProbeVerdict } from '../../utils/api';

/**
 * How a host-side check's verdict reads, wherever it is shown: the add form's
 * panel and a connector's own page both say it in these words.
 *
 * The verdict is computed on the host, which is the only side that knows
 * whether a key was sent, so nothing here re-derives it: the table below is
 * the whole mapping from word to tone, glyph and sentence. The one reading
 * added here is of an unreachable answer's own words, because "could not
 * reach the server" was said of a server that had answered with a page, a 404
 * or a 429, and each of those is fixed a different way.
 */

export type ProbeTone = 'good' | 'attention' | 'bad';

interface VerdictUi {
  Icon: React.ComponentType<{ className?: string }>;
  tone: ProbeTone;
  messageKey: string;
  /** The quiet second line: the tools this answer came with, or what the wire
   *  reported (labelled, after the sentence). `none` for the verdicts whose
   *  sentence is the whole answer. */
  detail: 'tools' | 'wire' | 'none';
}

const VERDICT_UI: Record<ProbeVerdict, VerdictUi> = {
  ok: { Icon: CheckCircle2, tone: 'good', messageKey: 'mcp.probe.authNone', detail: 'tools' },
  ok_authed: {
    Icon: ShieldCheck,
    tone: 'good',
    messageKey: 'mcp.probe.authCredential',
    detail: 'tools',
  },
  needs_credential: {
    Icon: KeyRound,
    tone: 'attention',
    messageKey: 'mcp.probe.authCredentialMissing',
    detail: 'none',
  },
  credential_rejected: {
    Icon: KeyRound,
    tone: 'bad',
    messageKey: 'mcp.probe.credentialRejected',
    detail: 'wire',
  },
  oauth: { Icon: ShieldCheck, tone: 'attention', messageKey: 'mcp.probe.authOauth', detail: 'none' },
  missing_secrets: {
    Icon: KeyRound,
    tone: 'attention',
    messageKey: 'mcp.probe.missingSecrets',
    detail: 'none',
  },
  unreachable: { Icon: AlertCircle, tone: 'bad', messageKey: 'mcp.probe.failed', detail: 'wire' },
};

/** What a verdict settles, for the surfaces that act on it rather than say it. */
export type VerdictFact =
  /** The server answered and listed its tools. */
  | 'ok'
  /** The answer was about a key: the server asked for one, turned one down,
   *  or took one, so the address is the server the user meant. */
  | 'aboutTheKey'
  /** The key is what is wrong, so a key field is where it gets fixed. */
  | 'keyVerdict'
  /** The check a save runs first refuses it. */
  | 'refusesSave';

// Keyed by the save's check verdicts too, since a refused save reports its
// servers in the same words plus the two only a command can earn.
const VERDICT_FACTS: Record<ProbeVerdict | McpCheckVerdict, readonly VerdictFact[]> = {
  ok: ['ok'],
  ok_authed: ['ok', 'aboutTheKey'],
  needs_credential: ['aboutTheKey', 'keyVerdict', 'refusesSave'],
  credential_rejected: ['aboutTheKey', 'keyVerdict', 'refusesSave'],
  oauth: [],
  missing_secrets: ['refusesSave'],
  unreachable: ['refusesSave'],
  command_failed: ['refusesSave'],
  unsupported: ['refusesSave'],
};

export function verdictIs(verdict: ProbeVerdict | McpCheckVerdict, fact: VerdictFact): boolean {
  return (VERDICT_FACTS[verdict] ?? []).includes(fact);
}

/** A check that never reached the server reads as unreachable: the surface's
 *  job is to say whether this address answers, and from here it did not. */
export function unreachableResult(error: string): McpProbeResult {
  return {
    verdict: 'unreachable',
    tools: [],
    server_info: null,
    error,
    http_status: null,
    missing_secrets: [],
    probed_at: null,
  };
}

/** The glyph and tone of a verdict. A word this build has no entry for reads
 *  as unreachable rather than crashing the form it sits in. */
export function verdictUi(verdict: ProbeVerdict): Pick<VerdictUi, 'Icon' | 'tone'> {
  return VERDICT_UI[verdict] ?? VERDICT_UI.unreachable;
}

// The host's own wording for each way a dial fails before anything answered
// (`_describe` and the URL pinning in mcp_probe.py, the resolver in
// egress_guard.py). Read in this order: a failed lookup is reported both as a
// connect error and as a blocked address, and is a typo either way.
const DNS_FAILED =
  /name or service not known|nodename nor servname|no address associated|getaddrinfo|dns resolution failed|no addresses resolved|could not resolve/i;
const TIMED_OUT = /did not answer within|timed out/i;
const PRIVATE_ADDRESS = /non-global address|blocked url/i;
const REFUSED = /connection refused|could not connect/i;

/**
 * What an unreachable verdict says, by what actually happened. An answer with
 * an HTTP status is a server that answered, so the fix is the path or a wait,
 * not the host; only an answer that never came is "could not reach".
 */
export function unreachableKey(result: Pick<McpProbeResult, 'error' | 'http_status'>): string {
  const error = result.error ?? '';
  const status = result.http_status;
  if (/refused the handshake/i.test(error)) return 'mcp.probe.failedHandshake';
  if (status === 429) return 'mcp.probe.failedRateLimited';
  if (status != null && status >= 500) return 'mcp.probe.failedServerError';
  if (status != null) return 'mcp.probe.failedNotEndpoint';
  if (DNS_FAILED.test(error)) return 'mcp.probe.failedDns';
  if (TIMED_OUT.test(error)) return 'mcp.probe.failedTimeout';
  if (PRIVATE_ADDRESS.test(error)) return 'mcp.probe.failedBlocked';
  if (REFUSED.test(error)) return 'mcp.probe.failedRefused';
  return 'mcp.probe.failed';
}

/** The sentence a verdict leads with. */
export function verdictMessageKey(result: McpProbeResult): string {
  if (result.verdict === 'unreachable') return unreachableKey(result);
  return (VERDICT_UI[result.verdict] ?? VERDICT_UI.unreachable).messageKey;
}

/**
 * The wire's own words for a verdict that carries them, with the status said
 * once, or '' for a verdict whose sentence is the whole answer. The host's
 * error already reads "server answered HTTP 401: ...", and a second "HTTP 401"
 * after it said the same thing twice. Raw and English: it only ever follows
 * the verdict's own sentence, labelled, never stands in for it.
 */
export function verdictWire(result: McpProbeResult): string {
  const { detail } = VERDICT_UI[result.verdict] ?? VERDICT_UI.unreachable;
  if (detail !== 'wire') return '';
  const error = result.error ?? '';
  const status =
    result.http_status != null && !error.includes(`HTTP ${result.http_status}`)
      ? `HTTP ${result.http_status}`
      : '';
  return [error, status].filter(Boolean).join(' · ');
}

/** Whose words `verdictWire` is: the server's when it answered, else the
 *  host's own account of a dial that never got an answer. */
export function verdictWireLabelKey(result: McpProbeResult): string {
  return result.http_status != null || result.verdict === 'credential_rejected'
    ? 'mcp.probe.serverSaid'
    : 'mcp.probe.wireDetail';
}

/**
 * The quiet line under a verdict: what the server reported, each part set off
 * by a middle dot, or the wire's words behind a label that says whose they are.
 */
export function verdictDetail(
  result: McpProbeResult,
  t: (key: string, opts?: Record<string, unknown>) => string,
): string {
  const { detail } = VERDICT_UI[result.verdict] ?? VERDICT_UI.unreachable;
  if (detail === 'tools') {
    const tools = result.tools ?? [];
    const info = result.server_info;
    return [
      tools.length === 0 ? t('mcp.probe.okNoTools') : t('mcp.probe.ok', { count: tools.length }),
      info?.name ? [info.name, info.version].filter(Boolean).join(' ') : '',
    ]
      .filter(Boolean)
      .join(' · ');
  }
  const wire = verdictWire(result);
  return wire ? t(verdictWireLabelKey(result), { detail: wire }) : '';
}
