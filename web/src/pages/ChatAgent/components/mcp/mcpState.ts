import type { EffectiveServer, McpOauthStatus, McpStatus } from '../../utils/api';

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
// Plugin provenance
// ---------------------------------------------------------------------------

/** The row came from an installed plugin. Its config is the plugin's to
 * define; the in-place actions are enable/disable and secret bindings, and
 * an edit detaches it. */
export function isPluginOwned(row: { plugin_name?: string | null }): boolean {
  return !!row.plugin_name;
}

/** Editable in place is the negation: everything not owned by a plugin.
 * Stated this way so a new provenance can't be forgotten here. */
export function canEditInPlace(row: { plugin_name?: string | null }): boolean {
  return !isPluginOwned(row);
}

/** Suppressed by its plugin being switched off, so the row's own `enabled`
 * is not the truth the agent sees. Undefined plugin state means "not
 * suppressed" — a row we can't prove is held down is shown as it is. */
export function isPluginSuppressed(row: {
  plugin_name?: string | null;
  plugin_enabled?: boolean | null;
}): boolean {
  return isPluginOwned(row) && row.plugin_enabled === false;
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
  /** Inherited rows: the owner's OAuth connection status (incl. 'revoked'). */
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
