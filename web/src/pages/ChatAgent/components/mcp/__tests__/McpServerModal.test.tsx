import { describe, it, expect, vi, beforeEach } from 'vitest';
import { screen, fireEvent, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';
import React, { type ReactElement } from 'react';
import { QueryClient } from '@tanstack/react-query';
import { renderWithProviders } from '@/test/utils';
import { queryKeys } from '@/lib/queryKeys';
import { McpServerModal } from '../McpServerModal';
import type { McpProbeResult, McpServerDraft } from '../../../utils/api';

// The modal reads its check through React Query, so it needs a client.
const render = (ui: ReactElement, queryClient?: QueryClient) =>
  renderWithProviders(ui, { queryClient });

// The form rests for 700ms before it asks the host anything. Waiting that out
// with a real timer costs a second per assertion, and faking the clock
// deadlocks Testing Library (its async helpers wait on a real setTimeout), so
// the rest is removed here and pinned on its own in lib/__tests__.
vi.mock('@/lib/useDebouncedValue', () => ({
  useDebouncedValue: <T,>(value: T) => value,
}));

function probeResult(overrides: Partial<McpProbeResult> = {}): McpProbeResult {
  return {
    verdict: 'ok',
    tools: [],
    server_info: null,
    error: '',
    http_status: null,
    missing_secrets: [],
    probed_at: '2026-01-01T00:00:00Z',
    ...overrides,
  };
}

const baseProps = {
  secretNames: ['EXISTING_TOKEN'],
  onClose: vi.fn(),
  onSubmit: vi.fn().mockResolvedValue(undefined),
  createSecret: vi.fn().mockResolvedValue({ name: 'NEW' }),
};

/**
 * An edited server as the modal now receives it — the config-shaped `Pick`, not
 * a whole row. Both surfaces hand it one of their real rows; nothing about
 * status, permissions or tool counts reaches this component.
 */
function makeDraft(overrides: Partial<McpServerDraft> = {}): McpServerDraft {
  return {
    name: 'srv',
    transport: 'stdio',
    command: 'npx',
    args: [],
    url: null,
    env_refs: [],
    header_refs: [],
    description: '',
    instruction: '',
    tool_exposure_mode: 'summary',
    ...overrides,
  };
}

beforeEach(() => {
  vi.clearAllMocks();
});

const ENTRY = 'mcp-entry';
const openAdvanced = () => fireEvent.click(screen.getByRole('button', { name: /advanced/i }));
const typeEntry = (value: string) =>
  fireEvent.change(screen.getByTestId(ENTRY), { target: { value } });
/** Leaving the field is the user saying the address is the one they meant. */
const commitEntry = () => fireEvent.blur(screen.getByTestId(ENTRY));
/** Let a check that was allowed to start actually start, so "did not fire" is
 *  an assertion about the gate and not about timing. */
const settle = () => new Promise((resolve) => setTimeout(resolve, 0));

describe('McpServerModal: one field, kind detected from it', () => {
  it('shows nothing kind-specific until the field says what the server is', () => {
    render(<McpServerModal {...baseProps} />);
    expect(screen.queryByText('Arguments')).not.toBeInTheDocument();
    expect(screen.queryByText('Headers')).not.toBeInTheDocument();
    expect(screen.queryByTestId('mcp-entry-kind')).not.toBeInTheDocument();
  });

  it('reads a command line as a local server: args and env, no headers', () => {
    render(<McpServerModal {...baseProps} />);
    typeEntry('npx -y @modelcontextprotocol/server-filesystem /tmp');
    expect(screen.getByTestId('mcp-entry-kind')).toHaveTextContent(/local command/i);
    expect(screen.getByText('Arguments')).toBeInTheDocument();
    expect(screen.getByText('Environment variables')).toBeInTheDocument();
    expect(screen.queryByText('Headers')).not.toBeInTheDocument();
    // The name is suggested from the package, affixes stripped.
    expect(screen.getByTestId('mcp-name')).toHaveValue('filesystem');
  });

  it('reads a URL as a remote server: headers, no args', () => {
    render(<McpServerModal {...baseProps} />);
    typeEntry('https://mcp.linear.app/mcp');
    expect(screen.getByTestId('mcp-entry-kind')).toHaveTextContent(/remote/i);
    expect(screen.getByText('Headers')).toBeInTheDocument();
    expect(screen.queryByText('Arguments')).not.toBeInTheDocument();
    expect(screen.getByTestId('mcp-name')).toHaveValue('linear');
  });

  it('fills the form from a pasted mcpServers config', () => {
    render(<McpServerModal {...baseProps} />);
    typeEntry(JSON.stringify({
      mcpServers: {
        'fuyao-meta': {
          type: 'http',
          url: 'https://fuyao.aicubes.cn/mcp/meta',
          headers: { 'X-api-key': '${vault:FUYAO}' },
        },
      },
    }));
    // The field shows the line the config amounts to, never the JSON.
    expect(screen.getByTestId(ENTRY)).toHaveValue('https://fuyao.aicubes.cn/mcp/meta');
    expect(screen.getByTestId('mcp-name')).toHaveValue('fuyao_meta');
    expect(screen.getByDisplayValue('X-api-key')).toBeInTheDocument();
    expect(screen.getByText(/filled from "fuyao-meta"/i)).toBeInTheDocument();
  });

  it('keeps nothing submittable from a malformed config pasted over a valid one', async () => {
    const onSubmit = vi.fn().mockResolvedValue(undefined);
    render(<McpServerModal {...baseProps} onSubmit={onSubmit} />);
    typeEntry(
      JSON.stringify({ mcpServers: { good: { type: 'http', url: 'https://good.example.com/mcp' } } }),
    );
    expect(screen.getByRole('button', { name: /^add$/i })).not.toBeDisabled();

    // The form used to keep the previous payload behind the broken text, and
    // Save sent it. Nothing a server can be made of survives this paste.
    typeEntry('{"mcpServers": {');
    expect(screen.queryByTestId('mcp-entry-kind')).not.toBeInTheDocument();
    expect(screen.queryByText('Headers')).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: /^add$/i })).toBeDisabled();
  });

  it('keeps a name the user typed when the field changes', () => {
    render(<McpServerModal {...baseProps} />);
    typeEntry('https://mcp.linear.app/mcp');
    fireEvent.change(screen.getByTestId('mcp-name'), { target: { value: 'mine' } });
    typeEntry('https://mcp.notion.com/mcp');
    expect(screen.getByTestId('mcp-name')).toHaveValue('mine');
  });

  it('keeps the prompt-tuning fields under Advanced, each with its own helper text', () => {
    render(<McpServerModal {...baseProps} />);
    expect(screen.queryByText(/decide when to reach for this server/i)).not.toBeInTheDocument();
    openAdvanced();
    expect(screen.getByText(/decide when to reach for this server/i)).toBeInTheDocument();
    expect(screen.getByText(/before it calls this server's tools/i)).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /summary/i })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /detailed/i })).toBeInTheDocument();
    const checkbox = screen.getByRole('checkbox', { name: /use my secrets during discovery/i });
    expect(checkbox).not.toBeChecked();
  });

  it('lets Advanced override the detected transport', () => {
    render(<McpServerModal {...baseProps} />);
    typeEntry('https://mcp.linear.app/mcp');
    openAdvanced();
    fireEvent.click(screen.getByRole('button', { name: 'stdio' }));
    expect(screen.getByTestId('mcp-entry-kind')).toHaveTextContent(/local command/i);
    expect(screen.getByText('Arguments')).toBeInTheDocument();
  });
});

describe('McpServerModal: header picker', () => {
  it('starts a header row as Authorization: Bearer and stores the scheme in front of the value', async () => {
    const onSubmit = vi.fn().mockResolvedValue(undefined);
    render(<McpServerModal {...baseProps} onSubmit={onSubmit} />);
    typeEntry('https://mcp.example.com/mcp');
    fireEvent.click(screen.getByRole('button', { name: /add entry/i }));
    expect(screen.getByTestId('mcp-header-choice-0')).toHaveValue('bearer');

    fireEvent.change(screen.getByPlaceholderText(/^value$/i), { target: { value: 'k' } });
    expect(screen.getByTestId('mcp-header-preview-0')).toHaveTextContent('Authorization: Bearer <value>');

    fireEvent.click(screen.getByTestId('mcp-submit'));
    await waitFor(() => expect(onSubmit).toHaveBeenCalledTimes(1));
    expect(onSubmit).toHaveBeenCalledWith(expect.objectContaining({ headers: { Authorization: 'Bearer k' } }));
  });

  it('sends the exact listed name for an API-key header, and nothing for a row with no value', async () => {
    const onSubmit = vi.fn().mockResolvedValue(undefined);
    render(<McpServerModal {...baseProps} onSubmit={onSubmit} />);
    typeEntry('https://mcp.example.com/mcp');
    fireEvent.click(screen.getByRole('button', { name: /add entry/i }));
    fireEvent.change(screen.getByTestId('mcp-header-choice-0'), { target: { value: 'x-api-key' } });
    fireEvent.change(screen.getByPlaceholderText(/^value$/i), { target: { value: 'k' } });
    fireEvent.click(screen.getByRole('button', { name: /add entry/i }));

    fireEvent.click(screen.getByTestId('mcp-submit'));
    await waitFor(() => expect(onSubmit).toHaveBeenCalledTimes(1));
    expect(onSubmit).toHaveBeenCalledWith(expect.objectContaining({ headers: { 'X-API-Key': 'k' } }));
  });

  it("edits a vendor's own header spelling as Custom and keeps it on save", async () => {
    const onSubmit = vi.fn().mockResolvedValue(undefined);
    render(
      <McpServerModal
        {...baseProps}
        initial={makeDraft({
          transport: 'http',
          command: null,
          url: 'https://fuyao.example.com/mcp',
          headers: { 'X-api-key': '${vault:FUYAO}' },
          header_refs: ['FUYAO'],
        })}
        onSubmit={onSubmit}
      />,
    );
    expect(screen.getByTestId('mcp-header-choice-0')).toHaveValue('custom');
    expect(screen.getByTestId('mcp-header-name-0')).toHaveValue('X-api-key');
    expect(screen.getByTestId('vault-ref-chip')).toHaveTextContent('FUYAO');
    // Only a scheme header needs the composed line spelled out.
    expect(screen.queryByTestId('mcp-header-preview-0')).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: /^save$/i }));
    await waitFor(() => expect(onSubmit).toHaveBeenCalledTimes(1));
    expect(onSubmit).toHaveBeenCalledWith(expect.objectContaining({ headers: { 'X-api-key': '${vault:FUYAO}' } }));
  });

  it('refuses the save while two rows would send the same header name', async () => {
    const onSubmit = vi.fn().mockResolvedValue(undefined);
    render(<McpServerModal {...baseProps} onSubmit={onSubmit} />);
    typeEntry('https://mcp.example.com/mcp');
    fireEvent.click(screen.getByRole('button', { name: /add entry/i }));
    fireEvent.click(screen.getByRole('button', { name: /add entry/i }));
    const values = screen.getAllByPlaceholderText(/^value$/i);
    fireEvent.change(values[0], { target: { value: 'EXAMPLE-TOKEN' } });
    fireEvent.change(values[1], { target: { value: 'EXAMPLE-BASIC' } });
    fireEvent.change(screen.getByTestId('mcp-header-choice-1'), { target: { value: 'basic' } });

    // Both rows send `Authorization`, and the map is last-wins, so saving would
    // hand the server one credential and drop the other in silence.
    fireEvent.click(screen.getByTestId('mcp-submit'));
    expect(await screen.findByText(/each header name can appear only once/i)).toBeInTheDocument();
    expect(onSubmit).not.toHaveBeenCalled();

    fireEvent.change(screen.getByTestId('mcp-header-choice-1'), { target: { value: 'x-api-key' } });
    fireEvent.click(screen.getByTestId('mcp-submit'));
    await waitFor(() => expect(onSubmit).toHaveBeenCalledTimes(1));
    expect(onSubmit).toHaveBeenCalledWith(
      expect.objectContaining({
        headers: { Authorization: 'Bearer EXAMPLE-TOKEN', 'X-API-Key': 'EXAMPLE-BASIC' },
      }),
    );
  });
});

describe('McpServerModal: host-side check of a remote address', () => {
  it('probes the address with its headers once the form rests, and shows the verdict', async () => {
    const onProbe = vi.fn().mockResolvedValue(
      probeResult({
        tools: [
          { name: 'a', description: '' },
          { name: 'b', description: '' },
        ],
        server_info: { name: 'demo', version: '1' },
        http_status: 200,
      }),
    );
    render(<McpServerModal {...baseProps} onProbe={onProbe} />);
    typeEntry('https://mcp.linear.app/mcp');
    await waitFor(() => expect(onProbe).toHaveBeenCalledTimes(1));
    // No transport on the wire: the server decides that from the address.
    expect(onProbe).toHaveBeenCalledWith(
      { url: 'https://mcp.linear.app/mcp', headers: {} },
      // The query's signal, so an address typed past stops holding a probe slot.
      expect.any(AbortSignal),
    );
    const panel = await screen.findByTestId('mcp-probe');
    expect(panel).toHaveAttribute('data-verdict', 'ok');
    expect(panel).toHaveTextContent(/reachable, 2 tools/i);
  });

  it('names OAuth for what it is instead of painting the 401 as a failure', async () => {
    const onProbe = vi.fn().mockResolvedValue(
      probeResult({
        verdict: 'oauth',
        error: 'server answered HTTP 401: it wants an OAuth connection',
        http_status: 401,
      }),
    );
    render(<McpServerModal {...baseProps} onProbe={onProbe} />);
    typeEntry('https://mcp.linear.app/mcp');
    const panel = await screen.findByTestId('mcp-probe');
    expect(panel).toHaveAttribute('data-verdict', 'oauth');
    expect(panel).toHaveTextContent(/uses oauth/i);
    // The verdict never blocks the save: OAuth is completed from the row.
    expect(screen.getByRole('button', { name: /^add$/i })).not.toBeDisabled();
  });

  it('says a rejected credential was rejected, not that the server is down', async () => {
    const onProbe = vi.fn().mockResolvedValue(
      probeResult({ verdict: 'credential_rejected', error: 'invalid token', http_status: 403 }),
    );
    render(<McpServerModal {...baseProps} onProbe={onProbe} />);
    typeEntry('https://mcp.linear.app/mcp');
    const panel = await screen.findByTestId('mcp-probe');
    expect(panel).toHaveAttribute('data-verdict', 'credential_rejected');
    expect(panel).toHaveTextContent(/rejected that credential/i);
    expect(panel).toHaveTextContent(/HTTP 403/);
  });

  it('asks once per question: same address cached, a new header asked again', async () => {
    const onProbe = vi.fn().mockResolvedValue(probeResult());
    // The shared test client drops a cache entry the moment its last observer
    // leaves, which is exactly what this asserts survives.
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    render(<McpServerModal {...baseProps} onProbe={onProbe} />, client);
    typeEntry('https://mcp.linear.app/mcp');
    await waitFor(() => expect(onProbe).toHaveBeenCalledTimes(1));

    typeEntry('https://mcp.notion.com/mcp');
    await waitFor(() => expect(onProbe).toHaveBeenCalledTimes(2));
    // Typing back to an address already answered reads the cached verdict.
    typeEntry('https://mcp.linear.app/mcp');
    await screen.findByTestId('mcp-probe');
    expect(onProbe).toHaveBeenCalledTimes(2);

    // A credential makes it a different question, and one the address has to
    // be committed for, which in the browser the click into the value field
    // already did.
    fireEvent.click(screen.getByRole('button', { name: /add entry/i }));
    fireEvent.change(screen.getByPlaceholderText(/^value$/i), { target: { value: 'k' } });
    commitEntry();
    await waitFor(() => expect(onProbe).toHaveBeenCalledTimes(3));
    expect(onProbe).toHaveBeenLastCalledWith({
      url: 'https://mcp.linear.app/mcp',
      headers: { Authorization: 'Bearer k' },
    }, expect.any(AbortSignal));
  });

  it('never probes a local command', async () => {
    const onProbe = vi.fn();
    render(<McpServerModal {...baseProps} onProbe={onProbe} />);
    typeEntry('npx -y @scope/thing');
    await screen.findByText('Arguments');
    expect(onProbe).not.toHaveBeenCalled();
  });
});

/**
 * A query key is cached state: it outlives the form by a gcTime and is legible
 * in devtools. The check's key has to say which credential the verdict is
 * about, and a digest says that without being the credential.
 */
describe('McpServerModal: the check caches the question, not the credential', () => {
  const SECRET = 'sk-live-9f3c1e7a4b2d';
  const cachedKeys = (client: QueryClient) =>
    JSON.stringify(client.getQueryCache().getAll().map((q) => q.queryKey));

  it('keeps the pasted credential out of every cached key', async () => {
    const onProbe = vi.fn().mockResolvedValue(probeResult());
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    render(<McpServerModal {...baseProps} onProbe={onProbe} />, client);
    typeEntry('https://mcp.linear.app/mcp');
    fireEvent.click(screen.getByRole('button', { name: /add entry/i }));
    fireEvent.change(screen.getByPlaceholderText(/^value$/i), { target: { value: SECRET } });
    commitEntry();
    // It still rides to the host, it just never lands in the cache.
    await waitFor(() =>
      expect(onProbe).toHaveBeenLastCalledWith(
        { url: 'https://mcp.linear.app/mcp', headers: { Authorization: `Bearer ${SECRET}` } },
        expect.any(AbortSignal),
      ),
    );
    expect(cachedKeys(client)).not.toContain(SECRET);
  });

  it("drops this scope's verdicts when the form closes", async () => {
    const onProbe = vi.fn().mockResolvedValue(probeResult());
    // A real client keeps an entry after its last observer leaves, which is the
    // window this asserts is shut; the shared test client sets gcTime to 0.
    const client = new QueryClient({
      defaultOptions: { queries: { retry: false, gcTime: Infinity } },
    });
    const probes = () => client.getQueryCache().findAll({ queryKey: queryKeys.mcp.probes('ws-1') });
    const { unmount } = render(
      <McpServerModal {...baseProps} onProbe={onProbe} probeScope="ws-1" />,
      client,
    );
    typeEntry('https://mcp.linear.app/mcp');
    await waitFor(() => expect(onProbe).toHaveBeenCalledTimes(1));
    expect(probes().length).toBeGreaterThan(0);

    unmount();
    expect(probes()).toHaveLength(0);
  });
});

/**
 * `https://acme.co` is a complete, reachable host on the way to
 * `https://acme.corp.example`, and the rest fires on every one of them. With a
 * credential already in the form that walked it through each host the user
 * paused on, so once a header is filled the automatic check waits for the
 * address to be committed instead of for the form to go quiet.
 */
describe('McpServerModal: a credential waits for a committed address', () => {
  const fillHeader = () => {
    fireEvent.click(screen.getByRole('button', { name: /add entry/i }));
    fireEvent.change(screen.getByPlaceholderText(/^value$/i), { target: { value: 'k' } });
  };

  it('does not check a half-typed host while a header is filled', async () => {
    const onProbe = vi.fn().mockResolvedValue(probeResult());
    render(<McpServerModal {...baseProps} onProbe={onProbe} />);
    typeEntry('https://mcp.example.com/mcp');
    await waitFor(() => expect(onProbe).toHaveBeenCalledTimes(1));

    fillHeader();
    typeEntry('https://acme.co');
    await settle();
    expect(onProbe).toHaveBeenCalledTimes(1);
    expect(onProbe).not.toHaveBeenCalledWith(
      expect.objectContaining({ url: 'https://acme.co' }),
      expect.anything(),
    );
  });

  it('checks the address once, with its header, as soon as the field is left', async () => {
    const onProbe = vi.fn().mockResolvedValue(probeResult());
    render(<McpServerModal {...baseProps} onProbe={onProbe} />);
    typeEntry('https://mcp.example.com/mcp');
    await waitFor(() => expect(onProbe).toHaveBeenCalledTimes(1));

    fillHeader();
    typeEntry('https://acme.corp.example.com/mcp');
    await settle();
    expect(onProbe).toHaveBeenCalledTimes(1);

    commitEntry();
    await waitFor(() => expect(onProbe).toHaveBeenCalledTimes(2));
    expect(onProbe).toHaveBeenLastCalledWith({
      url: 'https://acme.corp.example.com/mcp',
      headers: { Authorization: 'Bearer k' },
    }, expect.any(AbortSignal));
  });

  it('does not reuse a commit made before the credential existed', async () => {
    const onProbe = vi.fn().mockResolvedValue(probeResult());
    render(<McpServerModal {...baseProps} onProbe={onProbe} />);
    typeEntry('https://acme.co');
    await waitFor(() => expect(onProbe).toHaveBeenCalledTimes(1));

    // Clicking "Add entry" under Headers is what leaves the address field, so
    // the host the user was still halfway through typing is committed a beat
    // before the key is pasted into the form.
    commitEntry();
    fillHeader();
    await settle();
    expect(onProbe).toHaveBeenCalledTimes(1);
    expect(onProbe).not.toHaveBeenCalledWith(
      expect.objectContaining({ headers: { Authorization: 'Bearer k' } }),
      expect.anything(),
    );

    // Saying the address again, with the credential now in hand, is what sends
    // it -- and the same commit that would have been silently reused.
    commitEntry();
    await waitFor(() => expect(onProbe).toHaveBeenCalledTimes(2));
    expect(onProbe).toHaveBeenLastCalledWith({
      url: 'https://acme.co',
      headers: { Authorization: 'Bearer k' },
    }, expect.any(AbortSignal));
  });

  it('commits on Enter in the field as well as on leaving it', async () => {
    const onProbe = vi.fn().mockResolvedValue(probeResult());
    render(<McpServerModal {...baseProps} onProbe={onProbe} />);
    typeEntry('https://mcp.example.com/mcp');
    await waitFor(() => expect(onProbe).toHaveBeenCalledTimes(1));

    fillHeader();
    typeEntry('https://acme.corp.example.com/mcp');
    await settle();
    expect(onProbe).toHaveBeenCalledTimes(1);

    fireEvent.keyDown(screen.getByTestId(ENTRY), { key: 'Enter' });
    await waitFor(() => expect(onProbe).toHaveBeenCalledTimes(2));
  });

  it('leaves the rest alone for a draft with nothing to hand over', async () => {
    const onProbe = vi.fn().mockResolvedValue(probeResult());
    render(<McpServerModal {...baseProps} onProbe={onProbe} />);
    // No header, no commit: every address the form rests on is still checked.
    typeEntry('https://mcp.linear.app/mcp');
    await waitFor(() => expect(onProbe).toHaveBeenCalledTimes(1));
    typeEntry('https://mcp.notion.com/mcp');
    await waitFor(() => expect(onProbe).toHaveBeenCalledTimes(2));
    expect(onProbe).toHaveBeenLastCalledWith({
      url: 'https://mcp.notion.com/mcp',
      headers: {},
    }, expect.any(AbortSignal));
  });

  it('still checks on demand while the address is uncommitted', async () => {
    const onProbe = vi.fn().mockResolvedValue(probeResult());
    render(<McpServerModal {...baseProps} onProbe={onProbe} />);
    typeEntry('https://mcp.example.com/mcp');
    await waitFor(() => expect(onProbe).toHaveBeenCalledTimes(1));

    fillHeader();
    typeEntry('https://acme.corp.example.com/mcp');
    await settle();
    expect(onProbe).toHaveBeenCalledTimes(1);

    // The gate is on the automatic check only: asking for one is the user
    // saying this is the address, the same as leaving the field.
    fireEvent.click(screen.getByTestId('mcp-probe-check'));
    await waitFor(() => expect(onProbe).toHaveBeenCalledTimes(2));
    expect(onProbe).toHaveBeenLastCalledWith({
      url: 'https://acme.corp.example.com/mcp',
      headers: { Authorization: 'Bearer k' },
    }, expect.any(AbortSignal));
  });
});

/**
 * An edited server's headers hold its real credential (or the refs the host
 * resolves into it). Re-pointing it would otherwise walk that credential
 * through every hostname typed on the way to the intended one, one keystroke
 * at a time, before the user has said the new address is the right one.
 */
describe('McpServerModal: re-pointing a saved server', () => {
  const saved = () =>
    makeDraft({
      transport: 'http',
      command: null,
      url: 'https://old.example.com/mcp',
      headers: { Authorization: '${vault:AUTH}' },
      header_refs: ['AUTH'],
    });

  it('holds the check while the host differs from the saved one', async () => {
    const onProbe = vi.fn().mockResolvedValue(probeResult());
    render(<McpServerModal {...baseProps} initial={saved()} onProbe={onProbe} />);
    // The saved pairing is checked as before: that credential already went there.
    await waitFor(() => expect(onProbe).toHaveBeenCalledTimes(1));

    typeEntry('https://new.example.com/mcp');
    expect(await screen.findByTestId('mcp-probe-held')).toHaveTextContent(
      /save to check the new address/i,
    );
    // Neither automatically nor by hand: the panel itself is gone.
    expect(screen.queryByTestId('mcp-probe-check')).not.toBeInTheDocument();
    expect(onProbe).toHaveBeenCalledTimes(1);
    expect(onProbe).not.toHaveBeenCalledWith(
      expect.objectContaining({ url: 'https://new.example.com/mcp' }),
      expect.anything(),
    );
  });

  it('still checks a new path on the host the credential already reaches', async () => {
    const onProbe = vi.fn().mockResolvedValue(probeResult());
    render(<McpServerModal {...baseProps} initial={saved()} onProbe={onProbe} />);
    await waitFor(() => expect(onProbe).toHaveBeenCalledTimes(1));

    typeEntry('https://old.example.com/other');
    commitEntry();
    await waitFor(() => expect(onProbe).toHaveBeenCalledTimes(2));
    expect(onProbe).toHaveBeenLastCalledWith({
      url: 'https://old.example.com/other',
      headers: { Authorization: '${vault:AUTH}' },
    }, expect.any(AbortSignal));
    expect(screen.queryByTestId('mcp-probe-held')).not.toBeInTheDocument();
  });

  it('leaves the live check on a new server alone', async () => {
    const onProbe = vi.fn().mockResolvedValue(probeResult());
    render(<McpServerModal {...baseProps} onProbe={onProbe} />);
    typeEntry('https://new.example.com/mcp');
    await waitFor(() => expect(onProbe).toHaveBeenCalledTimes(1));
    expect(screen.queryByTestId('mcp-probe-held')).not.toBeInTheDocument();
  });
});

describe('McpServerModal: discovery_uses_secrets toggle', () => {
  it('defaults discovery_uses_secrets to false in the submit payload', async () => {
    const onSubmit = vi.fn().mockResolvedValue(undefined);
    render(<McpServerModal {...baseProps} onSubmit={onSubmit} />);
    typeEntry('npx -y @scope/thing');
    fireEvent.click(screen.getByRole('button', { name: /^add$/i }));
    await waitFor(() => expect(onSubmit).toHaveBeenCalledTimes(1));
    expect(onSubmit).toHaveBeenCalledWith(
      expect.objectContaining({ discovery_uses_secrets: false }),
    );
  });

  it('includes discovery_uses_secrets=true in the payload when toggled on', async () => {
    const onSubmit = vi.fn().mockResolvedValue(undefined);
    render(<McpServerModal {...baseProps} onSubmit={onSubmit} />);
    typeEntry('npx -y @scope/thing');
    openAdvanced();
    fireEvent.click(screen.getByRole('checkbox', { name: /use my secrets during discovery/i }));
    fireEvent.click(screen.getByRole('button', { name: /^add$/i }));
    await waitFor(() => expect(onSubmit).toHaveBeenCalledTimes(1));
    expect(onSubmit).toHaveBeenCalledWith(
      expect.objectContaining({ discovery_uses_secrets: true }),
    );
  });

  it('pre-fills the toggle from the edited server', () => {
    render(<McpServerModal {...baseProps} initial={makeDraft({ discovery_uses_secrets: true })} />);
    openAdvanced();
    expect(
      screen.getByRole('checkbox', { name: /use my secrets during discovery/i }),
    ).toBeChecked();
  });
});
describe('McpServerModal: edit-mode env/header hydration (data-loss guard)', () => {
  // FIX 1: in edit mode env/headers must hydrate from the stored reference maps
  // (real keys + ${vault:NAME}/literal values), so an unrelated edit re-saves the
  // existing config intact. The old code seeded BLANK keys from env_refs, and
  // kvsToMap drops blank-key rows → PUT silently erased every entry on save.
  const editingStdio = makeDraft({
    env_refs: ['API_TOKEN'],
    env: { API_TOKEN: '${vault:API_TOKEN}', REGION: 'us-east-1' },
    headers: {},
    description: 'old description',
  });

  it('preserves env entries from the stored map when saving an unrelated edit', async () => {
    const onSubmit = vi.fn().mockResolvedValue(undefined);
    render(<McpServerModal {...baseProps} initial={editingStdio} onSubmit={onSubmit} />);

    // The env editor should be pre-filled with the REAL keys (not blank).
    expect(screen.getByDisplayValue('API_TOKEN')).toBeInTheDocument();
    expect(screen.getByDisplayValue('REGION')).toBeInTheDocument();

    // Touch an unrelated field, then save.
    openAdvanced();
    fireEvent.change(screen.getByPlaceholderText('What this server does'), {
      target: { value: 'new description' },
    });
    fireEvent.click(screen.getByRole('button', { name: /^save$/i }));

    await waitFor(() => expect(onSubmit).toHaveBeenCalledTimes(1));
    expect(onSubmit).toHaveBeenCalledWith(
      expect.objectContaining({
        name: 'srv',
        description: 'new description',
        // The full env config survives the save — no silent erasure.
        env: { API_TOKEN: '${vault:API_TOKEN}', REGION: 'us-east-1' },
      }),
    );
  });

  it('preserves header entries from the stored map when saving an http server', async () => {
    const onSubmit = vi.fn().mockResolvedValue(undefined);
    const editingHttp: McpServerDraft = {
      ...editingStdio,
      transport: 'http',
      command: null,
      url: 'https://example.com/mcp',
      env: {},
      env_refs: [],
      headers: { Authorization: '${vault:AUTH}', 'X-Region': 'eu' },
      header_refs: ['AUTH'],
    };
    render(<McpServerModal {...baseProps} initial={editingHttp} onSubmit={onSubmit} />);

    // `Authorization: ${vault:AUTH}` has no scheme word, and `X-Region` is not
    // a listed name, so both rows show as Custom with their stored spelling.
    expect(screen.getByTestId('mcp-header-choice-0')).toHaveValue('custom');
    expect(screen.getByDisplayValue('Authorization')).toBeInTheDocument();
    expect(screen.getByDisplayValue('X-Region')).toBeInTheDocument();

    openAdvanced();
    fireEvent.change(screen.getByPlaceholderText('What this server does'), {
      target: { value: 'edited' },
    });
    fireEvent.click(screen.getByRole('button', { name: /^save$/i }));

    await waitFor(() => expect(onSubmit).toHaveBeenCalledTimes(1));
    expect(onSubmit).toHaveBeenCalledWith(
      expect.objectContaining({
        headers: { Authorization: '${vault:AUTH}', 'X-Region': 'eu' },
      }),
    );
  });

  it('blocks save on refs-only hydration (blank keys) until the user re-labels them', async () => {
    const onSubmit = vi.fn().mockResolvedValue(undefined);
    // No `env`/`headers` maps — only the legacy refs. Keys come back blank, and
    // kvsToMap silently drops blank-key rows — so save must REFUSE rather than
    // erase the entries.
    const legacy = { ...editingStdio, env: undefined, headers: undefined };
    render(<McpServerModal {...baseProps} initial={legacy} onSubmit={onSubmit} />);

    // Refs-only hydration seeds a BLANK key (it can't recover the real key name).
    expect(screen.queryByDisplayValue('API_TOKEN')).not.toBeInTheDocument();
    expect(screen.queryByDisplayValue('REGION')).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: /^save$/i }));
    expect(await screen.findByText(/key is required/i)).toBeInTheDocument();
    expect(onSubmit).not.toHaveBeenCalled();

    // Re-labeling the blank key clears the guard; the entry survives the save.
    fireEvent.change(screen.getByPlaceholderText('ENV_VAR'), {
      target: { value: 'API_TOKEN' },
    });
    fireEvent.click(screen.getByRole('button', { name: /^save$/i }));
    await waitFor(() => expect(onSubmit).toHaveBeenCalledTimes(1));
    expect(onSubmit).toHaveBeenCalledWith(
      expect.objectContaining({ env: { API_TOKEN: '${vault:API_TOKEN}' } }),
    );
  });
});

describe('McpServerModal: validation gating', () => {
  it('disables Add until the field and a valid name are filled', () => {
    render(<McpServerModal {...baseProps} />);
    const addBtn = screen.getByRole('button', { name: /^add$/i });
    expect(addBtn).toBeDisabled();
    typeEntry('npx -y @scope/thing');
    expect(addBtn).not.toBeDisabled();
    fireEvent.change(screen.getByTestId('mcp-name'), { target: { value: '' } });
    expect(addBtn).toBeDisabled();
  });

  it('keeps Add disabled for an http server with a private-IP url (SSRF policy)', () => {
    render(<McpServerModal {...baseProps} />);
    typeEntry('https://169.254.169.254/');
    fireEvent.change(screen.getByTestId('mcp-name'), { target: { value: 'remote' } });
    expect(screen.getByRole('button', { name: /^add$/i })).toBeDisabled();
  });

  it('submits the built payload on Add', async () => {
    const onSubmit = vi.fn().mockResolvedValue(undefined);
    render(<McpServerModal {...baseProps} onSubmit={onSubmit} />);
    typeEntry('npx -y @scope/thing --flag "two words"');
    fireEvent.change(screen.getByTestId('mcp-name'), { target: { value: 'good_name' } });
    fireEvent.click(screen.getByRole('button', { name: /^add$/i }));
    await waitFor(() => expect(onSubmit).toHaveBeenCalledTimes(1));
    expect(onSubmit).toHaveBeenCalledWith(
      expect.objectContaining({
        name: 'good_name',
        transport: 'stdio',
        command: 'npx',
        args: ['-y', '@scope/thing', '--flag', 'two words'],
      }),
    );
  });

  it('locks the name field when editing', () => {
    render(<McpServerModal {...baseProps} initial={makeDraft({ name: 'locked_name' })} />);
    const nameInput = screen.getByDisplayValue('locked_name') as HTMLInputElement;
    expect(nameInput).toBeDisabled();
  });
});

describe('McpServerModal: backdrop dismissal', () => {
  // The backdrop is the form's ground: `fixed inset-0`, the modal root. Checked
  // rather than assumed, so a wrapper added above it fails here instead of
  // letting the negative tests below pass without ever reaching the handler.
  const backdropOf = (container: HTMLElement) => {
    const backdrop = container.firstElementChild as HTMLElement;
    expect(backdrop).toHaveClass('fixed', 'inset-0');
    return backdrop;
  };

  it('closes on a press and a release that both land on the backdrop', () => {
    const onClose = vi.fn();
    const { container } = render(<McpServerModal {...baseProps} onClose={onClose} />);
    const backdrop = backdropOf(container);
    fireEvent.mouseDown(backdrop);
    fireEvent.mouseUp(backdrop);
    fireEvent.click(backdrop);
    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it('survives a drag that starts in a field and releases past the card edge', () => {
    // Selecting text and releasing outside makes the browser fire click on the
    // nearest common ancestor of press and release — the backdrop. A bare
    // onClick there threw away everything the user had typed.
    const onClose = vi.fn();
    const { container } = render(<McpServerModal {...baseProps} onClose={onClose} />);
    const backdrop = backdropOf(container);
    fireEvent.mouseDown(screen.getByPlaceholderText('my_server'));
    fireEvent.mouseUp(backdrop);
    fireEvent.click(backdrop);
    expect(onClose).not.toHaveBeenCalled();
  });

  it('does not close when a click inside the card bubbles to the backdrop', () => {
    const onClose = vi.fn();
    render(<McpServerModal {...baseProps} onClose={onClose} />);
    const field = screen.getByPlaceholderText('my_server');
    fireEvent.mouseDown(field);
    fireEvent.mouseUp(field);
    fireEvent.click(field);
    expect(onClose).not.toHaveBeenCalled();
  });
});

/**
 * Closing mid-save used to be allowed, and the save still finished: a failure
 * landed in a form that was no longer there, and a success closed whichever
 * modal the user had opened in the meantime. Each route is its own mechanism,
 * so each is asserted, against a baseline that proves the route works at all.
 */
describe('McpServerModal: dismissal while saving', () => {
  const dismissAllWays = (container: HTMLElement) => {
    fireEvent.keyDown(screen.getByRole('dialog'), { key: 'Escape' });
    const backdrop = container.firstElementChild as HTMLElement;
    fireEvent.mouseDown(backdrop);
    fireEvent.mouseUp(backdrop);
    fireEvent.click(backdrop);
    fireEvent.click(screen.getByRole('button', { name: 'Close' }));
    fireEvent.click(screen.getByRole('button', { name: 'Cancel' }));
  };

  it('offers Escape, the backdrop, the X and Cancel while idle', () => {
    const onClose = vi.fn();
    const { container } = render(<McpServerModal {...baseProps} onClose={onClose} />);
    dismissAllWays(container);
    expect(onClose).toHaveBeenCalledTimes(4);
  });

  it('holds every route until the save settles', () => {
    const onClose = vi.fn();
    const { container } = render(<McpServerModal {...baseProps} onClose={onClose} saving />);
    expect(screen.getByRole('button', { name: 'Close' })).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Cancel' })).toBeDisabled();
    dismissAllWays(container);
    expect(onClose).not.toHaveBeenCalled();
  });
});
