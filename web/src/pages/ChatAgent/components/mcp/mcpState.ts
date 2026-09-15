import type {
  CatalogServer,
  EffectiveServer,
  McpOauthStatus,
  McpStatus,
  ProbeVerdict,
} from '../../utils/api';

/**
 * Pure derivation shared by the MCP surfaces — the predicates more than one
 * module has to agree on.
 *
 * Each of these had at least two copies before. The discovery gate lived in
 * both `useMcpServers` (the self-stopping poll) and `McpTab` (the auto-probe)
 * and had to stay conjunct-for-conjunct identical or the panel would poll for a
 * probe it never runs; `oauthBroken` was recomputed in `McpServerRow` and again
 * one component down in `McpLifecycle`; the lifecycle label and its `data-phase`
 * were derived twice from overlapping conditions. Holding them here makes
 * "these agree" a property of the code rather than of review discipline.
 *
 * The OAuth predicates are written as NEGATIONS of the states that need
 * nothing, never as an enumeration of the states that do — an enumeration is a
 * list someone has to remember to extend when a status is added, and the
 * Plugins page had already grown two of them.
 */

/**
 * The user-level OAuth connection is unusable (revoked / needs reauth / a
 * refresh whose outcome we can't prove). The only fix is reconnecting in
 * the Plugins page, so this dominates whatever workspace-local status is cached.
 */
export function isOauthBroken(
  status: McpOauthStatus | null | undefined,
): status is Exclude<McpOauthStatus, 'connected'> {
  return !!status && status !== 'connected';
}

/**
 * The next step for this connection is (re-)running the authorize flow. Stated
 * as the negation of the one good state so a new status can't be forgotten
 * here: every status that isn't a live connection — including none at all —
 * needs one.
 */
export function needsOauthConnect(status: McpOauthStatus | null | undefined): boolean {
  return status !== 'connected';
}

/**
 * There is a connection worth tearing down. Also a negation: only a
 * never-connected server (no status) and an already-revoked one have nothing
 * to disconnect.
 */
export function canDisconnectOauth(status: McpOauthStatus | null | undefined): boolean {
  return !!status && status !== 'revoked';
}

/**
 * The row's tools are discovered host-side, so an in-sandbox probe is wrong for
 * it (the backend 409s one). Shared by the auto-probe gate below and the row's
 * "Test connection" menu item, which have to agree.
 */
export function isHostDiscovered(server: Pick<EffectiveServer, 'oauth_status'>): boolean {
  return !!server.oauth_status;
}

/**
 * Whether a row's workspace-local detail (tool count, discovery error, missing
 * secrets) is still worth showing. A broken OAuth connection makes all of it
 * stale — the cached status predates the disconnect — and rendering it beside
 * "Reconnect in Plugins" hands the user two contradictory next steps.
 */
export function showsWorkspaceDetail(server: EffectiveServer): boolean {
  return server.enabled && !isOauthBroken(server.oauth_status);
}

/**
 * The row is waiting on an in-sandbox discovery probe.
 *
 * Two consumers must read this the same way: the workspace list's self-stopping
 * poll and `McpTab`'s auto-probe. If the poll counts a row the tab never probes,
 * the panel polls forever; if the tab probes a row the poll ignores, the result
 * never lands. Builtins are always connected, and OAuth rows are discovered
 * host-side (an in-sandbox probe 409s) — neither qualifies.
 */
export function needsDiscoveryProbe(server: EffectiveServer): boolean {
  return (
    (server.origin === 'workspace' || server.origin === 'user') &&
    !isHostDiscovered(server) &&
    server.enabled &&
    server.status === 'pending'
  );
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

/** One node's state in the Saved → Verify → Ready track. */
export type McpLifecycleStep = 'done' | 'active' | 'todo';

/** The honest current phase of a row that is still moving. */
export type McpLifecyclePhase = 'verifying' | 'starting' | 'applying' | 'waiting';

/**
 * What a row's status area should render: a terminal pill on one of the two
 * vocabularies, or the animated progress track.
 */
export type McpLifecycleView =
  | { kind: 'status'; status: McpStatus; enabled: boolean }
  | { kind: 'oauth'; status: Exclude<McpOauthStatus, 'connected'> }
  | {
      kind: 'progress';
      phase: McpLifecyclePhase;
      /** i18n key — the selector stays pure, the component translates. */
      labelKey: string;
      verifyState: McpLifecycleStep;
      /**
       * Never `'done'`: the track only renders when something is outstanding,
       * and verified-and-applied returns the connected pill above.
       */
      readyState: Exclude<McpLifecycleStep, 'done'>;
    };

export interface McpLifecycleInput {
  status: McpStatus;
  enabled: boolean;
  origin: EffectiveServer['origin'];
  /** A discovery probe is in flight for this row. */
  checking: boolean;
  /** The running session has applied the saved config (apply axis complete). */
  synced: boolean;
  /** Whether the workspace sandbox is running (discovery/apply can happen). */
  sandboxRunning: boolean;
  /** The sandbox is warming up toward running (a background apply kicked it). */
  sandboxWarming?: boolean;
  /** Inherited rows: the status of a connection that still claims the row; a revoked one is dropped upstream. */
  oauthStatus?: McpOauthStatus | null;
}

/**
 * Fuse the verify axis (discovery) and the apply axis (`synced`) into the one
 * thing the row renders. Terminal states collapse to a pill; anything still in
 * motion returns the track's node states plus a phase and label that are now
 * derived once, together, and therefore cannot drift apart.
 */
export function deriveLifecycle({
  status,
  enabled,
  origin,
  checking,
  synced,
  sandboxRunning,
  sandboxWarming = false,
  oauthStatus = null,
}: McpLifecycleInput): McpLifecycleView {
  // Built-ins are process-global: always connected, with no per-workspace
  // discovery or apply state to surface.
  if (origin === 'builtin') return { kind: 'status', status, enabled };
  // A disabled row is always enabled=false (the optimistic toggle writes
  // enabled+status coherently at the source), so this guard alone covers it.
  if (!enabled) return { kind: 'status', status, enabled: false };
  // OAuth rows are discovered host-side, never probed from this workspace — the
  // verify track would be a promise nothing can keep. A broken connection is
  // the dominant truth; a connected one without a snapshot yet reads as Pending.
  if (isOauthBroken(oauthStatus)) return { kind: 'oauth', status: oauthStatus };
  if (oauthStatus && status === 'pending') return { kind: 'status', status: 'pending', enabled: true };
  if (status === 'error' || status === 'needs_secret' || status === 'unknown') {
    return { kind: 'status', status, enabled: true };
  }
  // Fully done: verified AND loaded into the running agent.
  if (status === 'connected' && synced) return { kind: 'status', status: 'connected', enabled: true };

  // Otherwise the server is still moving through the lifecycle.
  const verifying = checking || (status === 'pending' && sandboxRunning);
  // Pending while the sandbox is coming up: discovery can't run yet, but a warm
  // is in flight, so the verify step is active ("Starting workspace…") rather
  // than a dead "Waiting…".
  const warmingUp = status === 'pending' && !sandboxRunning && sandboxWarming;
  // Reaching here with 'connected' implies `!synced` (the pair returned above),
  // which is why `readyState` below tops out at 'active'.
  const verified = status === 'connected';

  const verifyState: McpLifecycleStep = verified
    ? 'done'
    : verifying || warmingUp
      ? 'active'
      : 'todo';
  const readyState = verified ? 'active' : 'todo';

  if (verifying) {
    return { kind: 'progress', phase: 'verifying', labelKey: 'mcp.lifecycle.verifying', verifyState, readyState };
  }
  if (warmingUp) {
    return { kind: 'progress', phase: 'starting', labelKey: 'mcp.lifecycle.starting', verifyState, readyState };
  }
  if (verified) {
    return {
      kind: 'progress',
      phase: 'applying',
      labelKey: sandboxRunning ? 'mcp.lifecycle.applying' : 'mcp.lifecycle.appliesOnStart',
      verifyState,
      readyState,
    };
  }
  return { kind: 'progress', phase: 'waiting', labelKey: 'mcp.lifecycle.waiting', verifyState, readyState };
}

// ---------------------------------------------------------------------------
// Probe verdicts
// ---------------------------------------------------------------------------

/** What a stored verdict asks of the row that carries it. */
export interface McpProbeRowState {
  /** What the check settled about OAuth. `wants` is the 401 that names it and
   *  `no` is a server that answered without one; `unknown` is a check that
   *  never got far enough to learn, which must leave a never-connected row its
   *  Connect button rather than take it away over a dropped packet. */
  oauth: 'wants' | 'no' | 'unknown';
  /** i18n key for the row's inline note, null when the verdict wants none. */
  noteKey: string | null;
  /** `RowNote`'s own vocabulary: a verdict the user has to act on is a fact
   *  about the row, a server that refused us is a consequence worth the
   *  warning colour. */
  tone: 'muted' | 'warning';
  /** Show the server's own line instead of the key, when it sent one. Only
   *  the unreachable arm has anything worth reading there: every other
   *  verdict is already the whole answer. */
  wire: boolean;
}

/**
 * One table for the row vocabulary, the way `taskStatusUi` holds the task one
 * and `McpProbePanel` holds the form's. The row and the form say different
 * things about the same verdict -- a row has a button and a clause, the form
 * a sentence and a tool list -- so they are two vocabularies rather than one
 * restated, but each is declared once.
 */
const PROBE_ROW_STATE: Record<ProbeVerdict, McpProbeRowState> = {
  ok: { oauth: 'no', noteKey: null, tone: 'muted', wire: false },
  ok_authed: { oauth: 'no', noteKey: null, tone: 'muted', wire: false },
  // The note is the fallback for a row Connect cannot speak for: the button is
  // offered on http only, and an sse server that answers 401 still wants one.
  oauth: { oauth: 'wants', noteKey: 'mcp.probe.rowOauth', tone: 'muted', wire: false },
  needs_credential: {
    oauth: 'no',
    noteKey: 'mcp.probe.rowCredential',
    tone: 'muted',
    wire: false,
  },
  credential_rejected: {
    oauth: 'no',
    noteKey: 'mcp.probe.rowCredentialRejected',
    tone: 'warning',
    wire: false,
  },
  // Nothing was dialled: the check stopped at the vault, so it learned as
  // little about auth as a dropped packet did and must leave Connect offered.
  missing_secrets: {
    oauth: 'unknown',
    noteKey: 'mcp.probe.rowMissingSecrets',
    tone: 'muted',
    wire: false,
  },
  unreachable: {
    oauth: 'unknown',
    noteKey: 'mcp.probe.rowUnreachable',
    tone: 'warning',
    wire: true,
  },
};

/** The row's reading of a verdict, or null when nothing has probed the row
 *  yet -- which is not the same as a verdict of `ok` and must not render as
 *  one. An unknown word from a newer backend reads as unprobed too. */
export function probeRowState(
  verdict: ProbeVerdict | null | undefined,
): McpProbeRowState | null {
  return verdict ? (PROBE_ROW_STATE[verdict] ?? null) : null;
}

/**
 * How long a kicked probe is still expected to land. The catalog list re-asks
 * for exactly this long (`useMcpCatalog`), so it is also the whole span in
 * which the page can honestly claim to be checking: past it, nothing is
 * asking.
 */
export const PROBE_KICK_WINDOW_MS = 45_000;

/**
 * Whether a verdict is still on its way for this row. A row nothing ever
 * kicked, and one whose kick has aged out of the window, are both just rows
 * with no verdict -- the copy that used to sit there said "checking" for the
 * life of the tab and never changed its mind.
 *
 * Read at render rather than off a timer: while the window is open the catalog
 * poll re-renders the list every few seconds, and it is the only span in which
 * this answer can change.
 */
export function probeStillLanding(
  server: Pick<CatalogServer, 'enabled' | 'transport' | 'probe' | 'probe_kicked_at'>,
  now: number = Date.now(),
): boolean {
  // `http` is the whole probeable set (the host dials streamable HTTP, so
  // stdio and sse rows are never kicked), a disabled row is dialled by nothing
  // at all however recently it was kicked, and a verdict in hand ends the wait.
  // The catalog poll (`useMcpCatalog`) filters on the same transport and the
  // same `enabled`, so the span the copy promises and the span something is
  // asking in are one span.
  if (!server.enabled || server.transport !== 'http' || server.probe) return false;
  const kickedAt = server.probe_kicked_at ? Date.parse(server.probe_kicked_at) : NaN;
  return Number.isFinite(kickedAt) && now - kickedAt < PROBE_KICK_WINDOW_MS;
}
