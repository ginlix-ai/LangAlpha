import { describe, it, expect, vi, beforeEach } from 'vitest';
import { screen, fireEvent, waitFor, act } from '@testing-library/react';
import '@testing-library/jest-dom';
import React from 'react';
import { renderWithProviders } from '@/test/utils';
import { SandboxSettingsContent } from '../SandboxSettingsPanel';

// ---------------------------------------------------------------------------
// Mocks. `formatApiErrorDetail` stays real (spread from the original module):
// the panel formats its own load failures with it.
// ---------------------------------------------------------------------------

const mockGetSandboxStats = vi.fn();
const mockGetWorkspace = vi.fn();
const mockStopComputer = vi.fn();

vi.mock('../../utils/api', async (importOriginal) => {
  const actual = await importOriginal<Record<string, unknown>>();
  return {
    ...actual,
    getSandboxStats: (...args: unknown[]) => mockGetSandboxStats(...args),
    getWorkspace: (...args: unknown[]) => mockGetWorkspace(...args),
    getComputers: vi.fn(async () => ({ computers: [], total: 0 })),
    stopComputer: (...args: unknown[]) => mockStopComputer(...args),
    installSandboxPackages: vi.fn(),
    refreshWorkspace: vi.fn(),
  };
});

// The real api module is imported for its error helpers, and its transport
// layer reads `api.defaults.baseURL` at module scope — so the stub needs that
// shape, not just the verbs.
vi.mock('@/api/client', () => ({
  api: {
    get: vi.fn(), post: vi.fn(), put: vi.fn(), patch: vi.fn(), delete: vi.fn(),
    defaults: { baseURL: '' },
  },
}));

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

function defaultStats() {
  return {
    state: 'running',
    sandbox_id: 'sandbox-abc',
    resources: {},
    packages: [],
    skills: [],
    mcp_servers: [],
  };
}

beforeEach(() => {
  vi.clearAllMocks();
  mockGetSandboxStats.mockResolvedValue(defaultStats());
  mockGetWorkspace.mockResolvedValue({ workspace_id: 'ws-1', status: 'running', computer_id: 'computer-1' });
});

// ---------------------------------------------------------------------------
// Live-sandbox tabs gate on state (issue #333)
// ---------------------------------------------------------------------------

/** A populated payload — the sandbox is up and returned real data. */
function liveStats(state: string | null) {
  return {
    ...defaultStats(),
    state,
    resources: { cpu: 2, memory: 4, disk: 10 },
    packages: [{ name: 'pandas', version: '2.2.0' }],
    default_packages: ['pandas'],
    skills: [{ name: 'demo-skill', description: 'a skill that exists' }],
    mcp_servers: ['demo-mcp'],
    disk_usage: { total: '10G', used: '1G', available: '9G', use_percent: '10%' },
  };
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((r) => {
    resolve = r;
  });
  return { promise, resolve };
}

async function openTab(name: RegExp) {
  renderWithProviders(<SandboxSettingsContent workspaceId="ws-1" />);
  await waitFor(() => expect(mockGetSandboxStats).toHaveBeenCalled());
  fireEvent.click(screen.getByRole('button', { name }));
}

describe('gated tabs render when the API reports the canonical running state', () => {
  it('shows skills on the Runtime tab', async () => {
    mockGetSandboxStats.mockResolvedValue(liveStats('running'));
    await openTab(/^runtime$/i);
    await waitFor(() => expect(screen.getByText(/demo-skill/i)).toBeInTheDocument());
  });

  it('shows installed packages on the Packages tab', async () => {
    mockGetSandboxStats.mockResolvedValue(liveStats('running'));
    await openTab(/packages/i);
    await waitFor(() => expect(screen.getByText(/pandas/i)).toBeInTheDocument());
  });

  it('offers Stop, not Start, for a running sandbox on Overview', async () => {
    mockGetSandboxStats.mockResolvedValue(liveStats('running'));
    await openTab(/overview/i);
    // Wait on the button, not the "Running" label — the label capitalizes
    // whatever state arrives, so it reads "Running" on the buggy code too.
    await waitFor(() =>
      expect(screen.getByRole('button', { name: /^stop$/i })).toBeInTheDocument()
    );
    expect(screen.queryByRole('button', { name: /^start$/i })).not.toBeInTheDocument();
  });

  it('still hides the tabs when the sandbox is genuinely stopped', async () => {
    mockGetSandboxStats.mockResolvedValue(liveStats('stopped'));
    await openTab(/^runtime$/i);
    await waitFor(() => expect(screen.getByText(/start the workspace/i)).toBeInTheDocument());
    expect(screen.queryByText(/demo-skill/i)).not.toBeInTheDocument();
  });

  it('survives a null state without crashing on the label', async () => {
    mockGetSandboxStats.mockResolvedValue(liveStats(null));
    await openTab(/overview/i);
    await waitFor(() => expect(screen.getByText('Unknown')).toBeInTheDocument());
  });

  it('keeps the transitional spinner label for a provider-native archiving state', async () => {
    mockGetSandboxStats.mockResolvedValue(liveStats('archiving'));
    await openTab(/overview/i);
    await waitFor(() => expect(screen.getByText('Archiving...')).toBeInTheDocument());
  });
});

describe('non-terminal provider states fail safe', () => {
  // The old three-item transitional list left these rendering as a settled
  // failure with a working Start button, so a user could start a mid-restore
  // sandbox. Anything outside TERMINAL_STATES must now read as in-progress.
  it.each(['restoring', 'resizing', 'creating', 'destroying'])(
    'treats %s as in-progress and disables Start',
    async state => {
      mockGetSandboxStats.mockResolvedValue(liveStats(state));
      await openTab(/overview/i);

      const start = await waitFor(() =>
        screen.getByRole('button', { name: /^start$/i })
      );
      expect(start).toBeDisabled();
    }
  );

  it('does not leak an unrecognized provider identifier into the label', async () => {
    // Daytona's SDK coerces any state it does not know to this sentinel, so a
    // bare capitalize would render "Unknown_default_open_api" as product copy.
    mockGetSandboxStats.mockResolvedValue(liveStats('unknown_default_open_api'));
    await openTab(/overview/i);

    await waitFor(() => expect(screen.getByText('Updating...')).toBeInTheDocument());
    expect(screen.queryByText(/unknown_default_open_api/i)).not.toBeInTheDocument();
  });

  it('renders a mapped label rather than a snake_case identifier', async () => {
    mockGetSandboxStats.mockResolvedValue(liveStats('build_failed'));
    await openTab(/overview/i);
    await waitFor(() => expect(screen.getByText('Build failed')).toBeInTheDocument());
  });
});

describe('provider-specific overview and storage', () => {
  it('reports an always-on sandbox as such instead of "Auto-stop: 0m"', async () => {
    // 0 disables auto-stop; rendering it as a 0-minute interval says the opposite.
    mockGetSandboxStats.mockResolvedValue({
      ...liveStats('running'),
      auto_stop_interval: 0,
    });
    await openTab(/overview/i);

    await waitFor(() => expect(screen.getByText('Always-on')).toBeInTheDocument());
    expect(screen.queryByText(/auto-stop: 0m/i)).not.toBeInTheDocument();
  });

  it('still shows a real auto-stop interval', async () => {
    mockGetSandboxStats.mockResolvedValue({
      ...liveStats('running'),
      auto_stop_interval: 30,
    });
    await openTab(/overview/i);
    await waitFor(() => expect(screen.getByText('Auto-stop: 30m')).toBeInTheDocument());
  });

  it('suppresses the disk totals for docker, whose df reads the host filesystem', async () => {
    mockGetSandboxStats.mockResolvedValue({ ...liveStats('running'), provider: 'docker' });
    await openTab(/storage/i);

    await waitFor(() => expect(screen.getByText(/without a disk quota/i)).toBeInTheDocument());
    expect(screen.queryByText('10G total')).not.toBeInTheDocument();
  });

  it('shows the disk totals for daytona, which is quota-backed', async () => {
    mockGetSandboxStats.mockResolvedValue({ ...liveStats('running'), provider: 'daytona' });
    await openTab(/storage/i);

    await waitFor(() => expect(screen.getByText('10G total')).toBeInTheDocument());
    expect(screen.queryByText(/without a disk quota/i)).not.toBeInTheDocument();
  });
});

describe('the client does not translate provider synonyms', () => {
  // Canonicalization belongs to the API alone. Pinned so nobody re-adds a client-side
  // 'started' shim: two places deciding what "running" means is how the vocabularies
  // drift apart. The raw synonym is just an unrecognized value here, and fails safe.
  it('treats the raw provider synonym as unrecognized, not as running', async () => {
    mockGetSandboxStats.mockResolvedValue(liveStats('started'));
    await openTab(/overview/i);

    await waitFor(() => expect(screen.getByText('Updating...')).toBeInTheDocument());
    expect(screen.queryByRole('button', { name: /^stop$/i })).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: /^start$/i })).toBeDisabled();
  });

  it('keeps the gated tabs closed on the raw synonym', async () => {
    mockGetSandboxStats.mockResolvedValue(liveStats('started'));
    await openTab(/^runtime$/i);

    await waitFor(() => expect(screen.getByText(/start the workspace/i)).toBeInTheDocument());
    expect(screen.queryByText(/demo-skill/i)).not.toBeInTheDocument();
  });
});

describe('refresh is reachable while every other action is disabled', () => {
  // Nothing polls: stats load on mount and after explicit actions only. Without an
  // always-enabled refresh, a transitional state disables Start/Stop and the panel
  // has no way to ever advance.
  it('refetches from a transitional state', async () => {
    mockGetSandboxStats.mockResolvedValue(liveStats('resizing'));
    await openTab(/overview/i);

    const refresh = await waitFor(() =>
      screen.getByRole('button', { name: /refresh sandbox status/i })
    );
    expect(refresh).toBeEnabled();
    expect(screen.getByRole('button', { name: /^start$/i })).toBeDisabled();

    mockGetSandboxStats.mockResolvedValue(liveStats('running'));
    fireEvent.click(refresh);

    await waitFor(() =>
      expect(screen.getByRole('button', { name: /^stop$/i })).toBeInTheDocument()
    );
  });

  it('keeps the panel on screen while the refresh it started is in flight', async () => {
    // Refresh reads through loadStats, which sets `loading`. Blanking to the
    // skeleton would discard the status the user is watching and unmount the
    // button they just pressed.
    mockGetSandboxStats.mockResolvedValue(liveStats('resizing'));
    await openTab(/overview/i);

    const slow = deferred<any>();
    mockGetSandboxStats.mockReturnValueOnce(slow.promise);
    fireEvent.click(screen.getByRole('button', { name: /refresh sandbox status/i }));

    expect(screen.getByRole('button', { name: /refresh sandbox status/i })).toBeInTheDocument();
    expect(screen.getByText(/resizing/i)).toBeInTheDocument();

    await act(async () => {
      slow.resolve(liveStats('running'));
    });
    expect(screen.getByRole('button', { name: /^stop$/i })).toBeInTheDocument();
  });

  it('does not show one workspace under another id while the switch loads', async () => {
    mockGetSandboxStats.mockResolvedValue(liveStats('running'));
    const { rerender } = renderWithProviders(<SandboxSettingsContent workspaceId="ws-1" />);
    await waitFor(() => expect(screen.getByText(/sandbox-abc/i)).toBeInTheDocument());

    const pending = deferred<any>();
    mockGetSandboxStats.mockReturnValueOnce(pending.promise);
    await act(async () => {
      rerender(<SandboxSettingsContent workspaceId="ws-2" />);
    });

    expect(screen.queryByText(/sandbox-abc/i)).not.toBeInTheDocument();

    await act(async () => {
      pending.resolve({ ...liveStats('running'), sandbox_id: 'sandbox-xyz' });
    });
    expect(screen.getByText(/sandbox-xyz/i)).toBeInTheDocument();
  });
});

describe('a superseded stats response cannot overwrite a newer one', () => {
  // The price of an always-enabled Refresh. handleStartStop never sets `loading`,
  // so Refresh stays on screen while a Stop is in flight; the read it starts takes
  // the full path (~15s of probes) while the post-Stop read takes the fast offline
  // one. Committing by arrival order would put a stopped sandbox back into
  // "running" with its live-only tabs open and Stop offered for a machine that
  // is already down.
  it('drops the slow refresh that resolves after the post-stop read', async () => {
    mockGetSandboxStats.mockResolvedValue(liveStats('running'));
    await openTab(/overview/i);

    const stopPost = deferred<any>();
    mockStopComputer.mockReturnValueOnce(stopPost.promise);

    fireEvent.click(await waitFor(() => screen.getByRole('button', { name: /^stop$/i })));

    // Still reachable: the Stop POST is in flight but `loading` is untouched.
    const refresh = screen.getByRole('button', { name: /refresh sandbox status/i });

    const slowRefresh = deferred<any>();
    const fastPostStop = deferred<any>();
    mockGetSandboxStats
      .mockReturnValueOnce(slowRefresh.promise)
      .mockReturnValueOnce(fastPostStop.promise);

    fireEvent.click(refresh);
    await act(async () => {
      stopPost.resolve({ data: { status: 'stopped' } });
    });
    await waitFor(() => expect(mockGetSandboxStats).toHaveBeenCalledTimes(3));

    await act(async () => {
      fastPostStop.resolve({ ...defaultStats(), state: 'stopped' });
    });
    expect(screen.getByRole('button', { name: /^start$/i })).toBeInTheDocument();

    await act(async () => {
      slowRefresh.resolve(liveStats('running'));
    });

    expect(screen.getByRole('button', { name: /^start$/i })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /^stop$/i })).not.toBeInTheDocument();
  });
});
