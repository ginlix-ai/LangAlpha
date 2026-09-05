import { describe, it, expect } from 'vitest';
import {
  canDisconnectOauth,
  deriveLifecycle,
  isOauthBroken,
  needsDiscoveryProbe,
  needsOauthConnect,
  showsWorkspaceDetail,
  type McpLifecycleInput,
} from '../mcpState';
import type { EffectiveServer, McpOauthStatus, McpStatus } from '../../../utils/api';

/**
 * The shared MCP selectors. These exist because their consumers must agree —
 * so the tests here are mostly about the *disagreements* that used to be
 * possible, not about restating each branch.
 */

function makeServer(overrides: Partial<EffectiveServer> = {}): EffectiveServer {
  return {
    name: 'placeholder_server',
    origin: 'workspace',
    transport: 'stdio',
    enabled: true,
    editable: true,
    deletable: true,
    status: 'pending',
    error: '',
    tool_count: 0,
    tools: [],
    missing_secrets: [],
    env_refs: [],
    header_refs: [],
    description: '',
    instruction: '',
    tool_exposure_mode: 'summary',
    command: 'npx',
    args: [],
    url: null,
    config_version: 1,
    ...overrides,
  };
}

describe('isOauthBroken', () => {
  it('is false when there is no OAuth connection at all', () => {
    expect(isOauthBroken(null)).toBe(false);
    expect(isOauthBroken(undefined)).toBe(false);
  });

  it('is false only for a live connection, true for every other state', () => {
    expect(isOauthBroken('connected')).toBe(false);
    for (const s of ['revoked', 'needs_reauth', 'refresh_ambiguous'] as const) {
      expect(isOauthBroken(s)).toBe(true);
    }
  });
});

describe('the OAuth action predicates', () => {
  // Written as negations so a newly-added status is covered by default. These
  // pin that property: the only status NOT needing a connect is 'connected',
  // and the only ones with nothing to disconnect are none-at-all and 'revoked'.
  const ALL: Array<McpOauthStatus | null | undefined> = [
    null,
    undefined,
    'connected',
    'needs_reauth',
    'refresh_ambiguous',
    'revoked',
  ];

  it('needsOauthConnect covers everything except a live connection', () => {
    for (const s of ALL) expect(needsOauthConnect(s)).toBe(s !== 'connected');
  });

  it('canDisconnectOauth covers every existing connection except an already-revoked one', () => {
    for (const s of ALL) expect(canDisconnectOauth(s)).toBe(!!s && s !== 'revoked');
  });
});

describe('showsWorkspaceDetail', () => {
  it('shows detail for a plain enabled row', () => {
    expect(showsWorkspaceDetail(makeServer({ status: 'connected' }))).toBe(true);
  });

  it('hides detail on a disabled row', () => {
    expect(showsWorkspaceDetail(makeServer({ enabled: false }))).toBe(false);
  });

  it('hides detail behind a broken OAuth connection — the cached status predates the disconnect', () => {
    // This is the invariant the needs_secret gate used to miss: a revoked row
    // whose last-known status is still needs_secret must not offer a local fix.
    expect(
      showsWorkspaceDetail(makeServer({ status: 'needs_secret', oauth_status: 'revoked' })),
    ).toBe(false);
  });

  it('still shows detail on a healthy OAuth row', () => {
    expect(
      showsWorkspaceDetail(makeServer({ status: 'connected', oauth_status: 'connected' })),
    ).toBe(true);
  });
});

describe('needsDiscoveryProbe', () => {
  it('admits an enabled, pending workspace or inherited server', () => {
    expect(needsDiscoveryProbe(makeServer({ origin: 'workspace' }))).toBe(true);
    expect(needsDiscoveryProbe(makeServer({ origin: 'user' }))).toBe(true);
  });

  it('rejects builtins (process-global, always connected)', () => {
    expect(needsDiscoveryProbe(makeServer({ origin: 'builtin' }))).toBe(false);
  });

  it('rejects OAuth rows — discovery is host-side, an in-sandbox probe 409s', () => {
    expect(needsDiscoveryProbe(makeServer({ oauth_status: 'connected' }))).toBe(false);
    expect(needsDiscoveryProbe(makeServer({ oauth_status: 'revoked' }))).toBe(false);
  });

  it('rejects a disabled row', () => {
    expect(needsDiscoveryProbe(makeServer({ enabled: false }))).toBe(false);
  });

  it('rejects any already-resolved status', () => {
    for (const status of ['connected', 'error', 'needs_secret', 'disabled', 'unknown'] as const) {
      expect(needsDiscoveryProbe(makeServer({ status }))).toBe(false);
    }
  });
});

// ---------------------------------------------------------------------------
// deriveLifecycle
// ---------------------------------------------------------------------------

const BASE: McpLifecycleInput = {
  status: 'pending',
  enabled: true,
  origin: 'workspace',
  checking: false,
  synced: false,
  sandboxRunning: true,
};

describe('deriveLifecycle — terminal views', () => {
  it('a builtin is always its plain pill, whatever the apply axis says', () => {
    expect(deriveLifecycle({ ...BASE, origin: 'builtin', status: 'connected', synced: false }))
      .toEqual({ kind: 'status', status: 'connected', enabled: true });
  });

  it('a disabled row is a pill before anything else is considered', () => {
    expect(deriveLifecycle({ ...BASE, enabled: false, status: 'disabled', oauthStatus: 'revoked' }))
      .toEqual({ kind: 'status', status: 'disabled', enabled: false });
  });

  it('a broken OAuth connection is the dominant truth', () => {
    expect(deriveLifecycle({ ...BASE, origin: 'user', oauthStatus: 'revoked' }))
      .toEqual({ kind: 'oauth', status: 'revoked' });
  });

  it('a connected OAuth row with no snapshot yet reads as plain Pending, not Verifying', () => {
    expect(deriveLifecycle({ ...BASE, origin: 'user', oauthStatus: 'connected' }))
      .toEqual({ kind: 'status', status: 'pending', enabled: true });
  });

  it('resolved-but-unhealthy statuses are pills', () => {
    for (const status of ['error', 'needs_secret', 'unknown'] as const) {
      expect(deriveLifecycle({ ...BASE, status })).toEqual({ kind: 'status', status, enabled: true });
    }
  });

  it('collapses to the connected pill once verified AND applied', () => {
    expect(deriveLifecycle({ ...BASE, status: 'connected', synced: true }))
      .toEqual({ kind: 'status', status: 'connected', enabled: true });
  });
});

describe('deriveLifecycle — progress track', () => {
  it('an in-flight probe is Verifying', () => {
    expect(deriveLifecycle({ ...BASE, checking: true })).toMatchObject({
      kind: 'progress',
      phase: 'verifying',
      labelKey: 'mcp.lifecycle.verifying',
      verifyState: 'active',
      readyState: 'todo',
    });
  });

  it('pending on a running sandbox self-verifies', () => {
    expect(deriveLifecycle(BASE)).toMatchObject({ phase: 'verifying', verifyState: 'active' });
  });

  it('pending while a warm is in flight is Starting, not a dead Waiting', () => {
    expect(deriveLifecycle({ ...BASE, sandboxRunning: false, sandboxWarming: true })).toMatchObject({
      phase: 'starting',
      labelKey: 'mcp.lifecycle.starting',
      verifyState: 'active',
    });
  });

  it('pending on a stopped, non-warming sandbox is Waiting', () => {
    expect(deriveLifecycle({ ...BASE, sandboxRunning: false })).toMatchObject({
      phase: 'waiting',
      labelKey: 'mcp.lifecycle.waiting',
      verifyState: 'todo',
      readyState: 'todo',
    });
  });

  it('verified but not applied is Applying, with the verify node done', () => {
    expect(deriveLifecycle({ ...BASE, status: 'connected' })).toMatchObject({
      phase: 'applying',
      labelKey: 'mcp.lifecycle.applying',
      verifyState: 'done',
      readyState: 'active',
    });
  });

  it('verified on a stopped sandbox promises the apply on start instead', () => {
    expect(deriveLifecycle({ ...BASE, status: 'connected', sandboxRunning: false })).toMatchObject({
      phase: 'applying',
      labelKey: 'mcp.lifecycle.appliesOnStart',
    });
  });

  it('a probe in flight on an already-connected row still reads Verifying', () => {
    // Ordering: an explicit "Test connection" beats the apply axis in the label.
    expect(deriveLifecycle({ ...BASE, status: 'connected', checking: true })).toMatchObject({
      phase: 'verifying',
      verifyState: 'done',
      readyState: 'active',
    });
  });
});

describe('deriveLifecycle — the two branches removed as unreachable', () => {
  // Exhaustive sweep of the input space, as proof rather than argument.
  //
  //  - `readyState: 'done'` required verified AND synced, and that pair returns
  //    the connected pill one guard earlier. Dead for ANY input, coherent or not.
  //  - the "Ready" label required a status that is neither pending nor connected
  //    to survive every terminal guard, which leaves only 'disabled' — and
  //    `status === 'disabled'` always travels with `enabled === false`, which
  //    returns the disabled pill. Dead for every input the backend can produce.
  //
  // The type alone can't express that pairing, so `coherent()` states it: the
  // backend sets status='disabled' exactly on the not-enabled branch
  // (`_row_for` in app/mcp_servers.py), and the optimistic toggle in
  // useMcpServers writes enabled+status together for the same reason.
  const STATUSES: McpStatus[] = ['connected', 'error', 'needs_secret', 'disabled', 'pending', 'unknown'];
  const OAUTH: Array<McpOauthStatus | null> = [null, 'connected', 'needs_reauth', 'refresh_ambiguous', 'revoked'];
  const ORIGINS: Array<EffectiveServer['origin']> = ['builtin', 'workspace', 'user'];
  const BOOLS = [true, false];

  function everyView(): Array<{ input: McpLifecycleInput; view: ReturnType<typeof deriveLifecycle> }> {
    const out: Array<{ input: McpLifecycleInput; view: ReturnType<typeof deriveLifecycle> }> = [];
    for (const status of STATUSES)
      for (const oauthStatus of OAUTH)
        for (const origin of ORIGINS)
          for (const enabled of BOOLS)
            for (const checking of BOOLS)
              for (const synced of BOOLS)
                for (const sandboxRunning of BOOLS)
                  for (const sandboxWarming of BOOLS) {
                    const input: McpLifecycleInput = {
                      status, oauthStatus, origin, enabled,
                      checking, synced, sandboxRunning, sandboxWarming,
                    };
                    out.push({ input, view: deriveLifecycle(input) });
                  }
    return out;
  }

  /** Drop the pairing the backend cannot emit: 'disabled' while still enabled. */
  const coherent = ({ input }: { input: McpLifecycleInput }) =>
    !(input.status === 'disabled' && input.enabled);

  it('never marks the ready node done — for ANY input, coherent or not', () => {
    const offenders = everyView().filter(
      ({ view }) => view.kind === 'progress' && String(view.readyState) === 'done',
    );
    expect(offenders).toEqual([]);
  });

  it('never emits the "Ready" label — a fully-ready row is the connected pill', () => {
    const offenders = everyView().filter(
      ({ view }) => view.kind === 'progress' && view.labelKey === 'mcp.lifecycle.ready',
    );
    expect(offenders).toEqual([]);
  });

  it('the only status that reaches the track is pending or connected', () => {
    const reached = new Set(
      everyView().filter(coherent).filter(({ view }) => view.kind === 'progress')
        .map(({ input }) => input.status),
    );
    expect([...reached].sort()).toEqual(['connected', 'pending']);
  });

  it('degrades the impossible enabled+disabled pairing to Waiting, not a contradictory Ready', () => {
    // The residual the old code labelled "Ready" while rendering a track with
    // nothing done. If a backend regression ever produces it, the honest read
    // is "waiting" — which is what the old `data-phase` already said.
    expect(deriveLifecycle({ ...BASE, status: 'disabled', enabled: true })).toMatchObject({
      kind: 'progress',
      phase: 'waiting',
      labelKey: 'mcp.lifecycle.waiting',
      readyState: 'todo',
    });
  });

  it('label and phase agree by construction on every reachable input', () => {
    // The old code derived them separately; the only input where they diverged
    // was the unreachable 'disabled'-while-enabled residual.
    const byPhase: Record<string, string[]> = {
      verifying: ['mcp.lifecycle.verifying'],
      starting: ['mcp.lifecycle.starting'],
      applying: ['mcp.lifecycle.applying', 'mcp.lifecycle.appliesOnStart'],
      waiting: ['mcp.lifecycle.waiting'],
    };
    for (const { view } of everyView()) {
      if (view.kind !== 'progress') continue;
      expect(byPhase[view.phase]).toContain(view.labelKey);
    }
  });
});
