import { describe, it, expect, vi, beforeEach } from 'vitest';
import { screen, fireEvent, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';
import React from 'react';
import { renderWithProviders } from '@/test/utils';
import type { EffectiveServer, EffectiveServerList } from '../../../utils/api';

// ---------------------------------------------------------------------------
// Mock the MCP hooks so we drive list data + mutation outcomes directly.
// ---------------------------------------------------------------------------

const mutateAsync = {
  add: vi.fn(),
  update: vi.fn(),
  toggle: vi.fn(),
  del: vi.fn(),
  discover: vi.fn(),
  import: vi.fn(),
  promote: vi.fn(),
};

let listData: EffectiveServerList | undefined;
let catalogData:
  | { servers: Array<{ name: string; transport?: string; description?: string }>; max_servers?: number }
  | undefined;

vi.mock('@/hooks/useMcpServers', () => ({
  useWorkspaceMcpServers: () => ({ data: listData, isLoading: false, error: null }),
  useAddWorkspaceMcpServer: () => ({ mutateAsync: mutateAsync.add, isPending: false }),
  useUpdateWorkspaceMcpServer: () => ({ mutateAsync: mutateAsync.update, isPending: false }),
  useToggleWorkspaceMcpServer: () => ({ mutateAsync: mutateAsync.toggle, isPending: false }),
  useDeleteWorkspaceMcpServer: () => ({ mutateAsync: mutateAsync.del, isPending: false }),
  useDiscoverWorkspaceMcpServer: () => ({ mutateAsync: mutateAsync.discover, isPending: false }),
  useImportWorkspaceMcpServers: () => ({ mutateAsync: mutateAsync.import, isPending: false }),
  usePromoteMcpServerToTemplate: () => ({ mutateAsync: mutateAsync.promote, isPending: false }),
  useMcpCatalog: () => ({ data: catalogData, isLoading: false, error: null }),
  // Catalog CRUD hooks are exercised via the Templates sub-view.
  useCreateMcpCatalogServer: () => ({ mutateAsync: vi.fn(), isPending: false }),
  useUpdateMcpCatalogServer: () => ({ mutateAsync: vi.fn(), isPending: false }),
  useDeleteMcpCatalogServer: () => ({ mutateAsync: vi.fn(), isPending: false }),
  // Pass-through (no fake timers in this suite): the anti-flicker is unit-tested
  // separately in useMcpServers.test; here `synced` should reflect the raw value.
  useDelayedFalse: (v: boolean) => v,
}));

// Error feedback is a toast — mock the module so we can assert it was raised
// (matches the existing toast-mock pattern across the codebase).
vi.mock('@/components/ui/use-toast', () => ({ toast: vi.fn() }));

// getVaultSecrets is called on mount for the secret picker; keep it benign.
vi.mock('../../../utils/api', async (importOriginal) => {
  const actual = await importOriginal<Record<string, unknown>>();
  return { ...actual, getVaultSecrets: vi.fn().mockResolvedValue([]), createVaultSecret: vi.fn() };
});

// Stub the row so the promote action is a plain button — the real Radix kebab
// needs portal/pointer machinery jsdom doesn't drive (the row's own test mocks
// the dropdown for the same reason). McpServerRow's item wiring is covered there.
vi.mock('../McpServerRow', () => ({
  McpServerRow: ({
    server,
    onPromoteToTemplate,
  }: {
    server: { name: string };
    // Mirror the real row's stable-handler contract: hand the row's own server
    // back at call time so the parent can pass a single stable useCallback.
    onPromoteToTemplate?: (server: { name: string }) => void;
  }) => (
    <div data-testid={`row-${server.name}`}>
      <span>{server.name}</span>
      {onPromoteToTemplate && (
        <button type="button" onClick={() => onPromoteToTemplate(server)}>
          {`save-template-${server.name}`}
        </button>
      )}
    </div>
  ),
}));

import { McpTab } from '../McpTab';
import { toast } from '@/components/ui/use-toast';

function makeServer(name: string, overrides: Partial<EffectiveServer> = {}): EffectiveServer {
  return {
    name,
    origin: 'workspace',
    transport: 'stdio',
    enabled: true,
    editable: true,
    deletable: true,
    status: 'connected',
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

function makeList(servers: EffectiveServer[], maxServers = 20): EffectiveServerList {
  return { servers, sandbox_running: true, max_servers: maxServers, config_version: 1 };
}

beforeEach(() => {
  vi.clearAllMocks();
  listData = makeList([]);
  catalogData = { servers: [] };
});

describe('McpTab — submit error formatting', () => {
  it('renders FastAPI array-shaped validation detail as readable text (not [object Object])', async () => {
    // FastAPI 422 validation list — must be flattened, not stringified.
    mutateAsync.add.mockRejectedValue({
      response: {
        data: {
          detail: [
            { loc: ['body', 'url'], msg: 'field required', type: 'value_error.missing' },
          ],
        },
      },
    });

    renderWithProviders(<McpTab workspaceId="ws-1" />);

    // Open the add-server modal, give it a valid name, and submit.
    fireEvent.click(screen.getByRole('button', { name: /add server/i }));
    fireEvent.change(screen.getByPlaceholderText('my_server'), { target: { value: 'good_name' } });
    fireEvent.click(screen.getByRole('button', { name: /^add$/i }));

    const expected = 'body.url: field required';
    await waitFor(() => expect(screen.getByText(expected)).toBeInTheDocument());
    // The flattened message must not collapse to the object placeholder.
    expect(screen.queryByText('[object Object]')).not.toBeInTheDocument();
  });
});

describe('McpTab — promote workspace server to template', () => {
  it('promotes a new-name server straight away (no overwrite, no confirm)', async () => {
    listData = makeList([makeServer('fresh_server')]);
    catalogData = { servers: [] }; // name not in catalog
    mutateAsync.promote.mockResolvedValue({});
    renderWithProviders(<McpTab workspaceId="ws-1" />);

    fireEvent.click(screen.getByText('save-template-fresh_server'));

    await waitFor(() =>
      expect(mutateAsync.promote).toHaveBeenCalledWith({ name: 'fresh_server', overwrite: false }),
    );
    // No overwrite confirm for a fresh name.
    expect(screen.queryByText(/already exists/i)).not.toBeInTheDocument();
  });

  it('confirms before overwriting an existing template, then promotes with overwrite', async () => {
    listData = makeList([makeServer('dup_server')]);
    catalogData = { servers: [{ name: 'dup_server' }] }; // name already a template
    mutateAsync.promote.mockResolvedValue({});
    renderWithProviders(<McpTab workspaceId="ws-1" />);

    fireEvent.click(screen.getByText('save-template-dup_server'));

    // Clash → confirm banner, NOT an immediate promote.
    await waitFor(() => expect(screen.getByText(/already exists/i)).toBeInTheDocument());
    expect(mutateAsync.promote).not.toHaveBeenCalled();

    fireEvent.click(screen.getByRole('button', { name: /overwrite/i }));

    await waitFor(() =>
      expect(mutateAsync.promote).toHaveBeenCalledWith({ name: 'dup_server', overwrite: true }),
    );
  });

  it('cancels the overwrite confirm without promoting', async () => {
    listData = makeList([makeServer('dup_server')]);
    catalogData = { servers: [{ name: 'dup_server' }] };
    renderWithProviders(<McpTab workspaceId="ws-1" />);

    fireEvent.click(screen.getByText('save-template-dup_server'));
    await waitFor(() => expect(screen.getByText(/already exists/i)).toBeInTheDocument());

    fireEvent.click(screen.getByRole('button', { name: /cancel/i }));

    await waitFor(() => expect(screen.queryByText(/already exists/i)).not.toBeInTheDocument());
    expect(mutateAsync.promote).not.toHaveBeenCalled();
  });
});

describe('McpTab — auto-resolve pending servers', () => {
  it('probes a pending workspace server once when the sandbox is running', async () => {
    listData = makeList([makeServer('pend', { status: 'pending' })]);
    mutateAsync.discover.mockResolvedValue({ status: 'connected', tools: [], error: '' });
    renderWithProviders(<McpTab workspaceId="ws-1" />);

    // A fresh pending server shouldn't sit on a dead pill — it gets probed.
    await waitFor(() => expect(mutateAsync.discover).toHaveBeenCalledWith('pend'));
    expect(mutateAsync.discover).toHaveBeenCalledTimes(1);
  });

  it('does NOT probe when the sandbox is stopped (nothing to discover against)', async () => {
    listData = { ...makeList([makeServer('pend', { status: 'pending' })]), sandbox_running: false };
    renderWithProviders(<McpTab workspaceId="ws-1" />);

    await waitFor(() => expect(screen.getByTestId('row-pend')).toBeInTheDocument());
    expect(mutateAsync.discover).not.toHaveBeenCalled();
  });

  it('does NOT probe a disabled pending server (it reads as Disabled)', async () => {
    listData = makeList([makeServer('off', { status: 'pending', enabled: false })]);
    renderWithProviders(<McpTab workspaceId="ws-1" />);

    await waitFor(() => expect(screen.getByTestId('row-off')).toBeInTheDocument());
    expect(mutateAsync.discover).not.toHaveBeenCalled();
  });

  it('does NOT re-probe a connected server', async () => {
    listData = makeList([makeServer('ok', { status: 'connected' })]);
    renderWithProviders(<McpTab workspaceId="ws-1" />);

    await waitFor(() => expect(screen.getByTestId('row-ok')).toBeInTheDocument());
    expect(mutateAsync.discover).not.toHaveBeenCalled();
  });
});

describe('McpTab — add-from-template error surfacing (FIX 2)', () => {
  it('surfaces a toast (and does not throw) when adding a template fails', async () => {
    // Catalog has one template; the from_template add rejects.
    catalogData = {
      servers: [{ name: 'svc', transport: 'stdio', description: '' }],
      max_servers: 20,
    };
    mutateAsync.add.mockRejectedValue({
      response: { data: { detail: 'workspace at server cap' } },
    });
    renderWithProviders(<McpTab workspaceId="ws-1" />);

    // Switch to the Templates sub-view and add the template to the workspace.
    fireEvent.click(screen.getByRole('button', { name: /^templates$/i }));
    fireEvent.click(await screen.findByRole('button', { name: /add to workspace/i }));

    await waitFor(() =>
      expect(mutateAsync.add).toHaveBeenCalledWith({ from_template: 'svc' }),
    );
    // The rejection is caught and surfaced — no unhandled rejection, user sees it.
    await waitFor(() =>
      expect(toast).toHaveBeenCalledWith(
        expect.objectContaining({
          variant: 'destructive',
          description: 'workspace at server cap',
        }),
      ),
    );
  });
});

describe('McpTab — Add button cap gating', () => {
  it('disables "Add server" when the workspace is at max_servers', async () => {
    listData = makeList([makeServer('a'), makeServer('b')], 2);
    renderWithProviders(<McpTab workspaceId="ws-1" />);
    // Flush the on-mount getVaultSecrets effect so its setState lands in act().
    await waitFor(() =>
      expect(screen.getByRole('button', { name: /add server/i })).toBeDisabled(),
    );
  });

  it('enables "Add server" below the cap', async () => {
    listData = makeList([makeServer('a')], 2);
    renderWithProviders(<McpTab workspaceId="ws-1" />);
    await waitFor(() =>
      expect(screen.getByRole('button', { name: /add server/i })).not.toBeDisabled(),
    );
  });
});
