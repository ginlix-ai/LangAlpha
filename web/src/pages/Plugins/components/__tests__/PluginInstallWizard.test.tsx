import { describe, it, expect, vi, beforeEach } from 'vitest';
import { act, fireEvent, screen, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';
import { renderWithProviders } from '@/test/utils';
import { ApiError } from '@/api/client';
import type { PluginInstallResponse } from '@/pages/ChatAgent/utils/api';
import type { ConnectRequest } from '../../hooks/useMcpOauthActions';
import { PluginInstallWizard } from '../PluginInstallWizard';

const installUrl = vi.fn();
vi.mock('@/hooks/usePlugins', () => ({
  useInstallPluginFromUrl: () => ({ mutateAsync: installUrl }),
  useInstallPluginFromZip: () => ({ mutateAsync: vi.fn() }),
  useUpgradePluginSseEntries: () => ({ mutateAsync: vi.fn(), isPending: false }),
  useMarketplace: () => ({ data: undefined, error: null }),
}));

// The connect lifecycle is the hook's, tested with it; here it only records
// what the wizard handed it.
const oauth = vi.hoisted(() => ({
  connectingName: null,
  pendingConfirm: null,
  connect: vi.fn(),
  cancelPending: vi.fn(),
}));
vi.mock('../../hooks/useMcpOauthActions', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../../hooks/useMcpOauthActions')>()),
  useMcpOauthActions: () => oauth,
}));

const SOURCE = 'https://github.com/acme/ledger';

function signInRequired(): ApiError {
  return new ApiError('Request failed with status code 409', 'ERR_BAD_REQUEST', {
    data: {
      detail: {
        code: 'mcp_sign_in_required',
        message: 'Sign in to ledger first.',
        servers: [{ server: 'ledger', url: 'https://mcp.ledger.example/mcp' }],
      },
    },
    status: 409,
    statusText: '',
    headers: {},
  });
}

/** Submit a git source whose server signs in, and return what the wizard
 *  handed the connect. */
async function startSignIn(): Promise<ConnectRequest> {
  installUrl.mockRejectedValueOnce(signInRequired());
  fireEvent.change(screen.getByPlaceholderText('https://github.com/owner/repo'), {
    target: { value: SOURCE },
  });
  fireEvent.click(screen.getByRole('button', { name: 'Install' }));
  await waitFor(() => expect(oauth.connect).toHaveBeenCalledTimes(1));
  return oauth.connect.mock.calls[0][0] as ConnectRequest;
}

beforeEach(() => {
  vi.clearAllMocks();
});

describe('PluginInstallWizard: an install that signs in', () => {
  // The step cannot be dismissed while the sign-in opens, which was right
  // for a page about to leave and wrong for the desktop app, whose page stays
  // while the vendor's opens in the system browser.
  it('closes once the sign-in has gone on in the browser', async () => {
    const onClose = vi.fn();
    renderWithProviders(<PluginInstallWizard onClose={onClose} />);
    const request = await startSignIn();

    expect(request.pending).toEqual({ kind: 'plugin', sourceUrl: SOURCE });
    expect(screen.getByText('Opening the sign-in page…')).toBeInTheDocument();

    act(() => request.onHandedOff?.());
    expect(onClose).toHaveBeenCalledTimes(1);
  });

  // A page that never left for the vendor stands the connect down the way a
  // connect taken back does, and the wizard goes back to where it was.
  it('goes back to the source, as entered, when the page never left', async () => {
    const onClose = vi.fn();
    renderWithProviders(<PluginInstallWizard onClose={onClose} />);
    const request = await startSignIn();

    act(() => request.onNotStarted?.());
    expect(screen.queryByText('Opening the sign-in page…')).not.toBeInTheDocument();
    expect(screen.getByPlaceholderText('https://github.com/owner/repo')).toHaveValue(SOURCE);
    expect(onClose).not.toHaveBeenCalled();
  });
});

function refusal(status: number, detail: Record<string, unknown>): ApiError {
  return new ApiError(`Request failed with status code ${status}`, 'ERR_BAD_REQUEST', {
    data: { detail },
    status,
    statusText: '',
    headers: {},
  });
}

const checkFailed = (failure: Record<string, unknown>) =>
  refusal(422, {
    code: 'mcp_check_failed',
    message: 'A server did not pass its check.',
    failures: [failure],
  });

const installed: PluginInstallResponse = {
  plugin: {
    name: 'ledger',
    version: '1.0.0',
    description: '',
    author: null,
    homepage: null,
    source_type: 'git',
    source_ref: SOURCE,
    enabled: true,
    installed_at: null,
    updated_at: null,
    components: [{ kind: 'mcp', name: 'foo', key: 'foo' }],
  },
  report: {
    components: [
      { kind: 'mcp', key: 'foo', name: 'foo', renamed: false, status: 'created', reason: '', warnings: [] },
    ],
    diagnostics: [],
    secrets_created: [],
    secrets_required: [],
    dropped_files: [],
    servers_created: 1,
    skills_created: 0,
  },
};

describe('PluginInstallWizard: a key a connector reads that the plugin does not declare', () => {
  // The install takes the key with the declared ones and saves it only once
  // every check passes, so a refused key is never left behind in Secrets, and
  // a refusal of it is said under its own field on every retry.
  it('sends it with the install, keeps its field, and says a refusal under it', async () => {
    installUrl
      .mockRejectedValueOnce(
        refusal(422, {
          code: 'plugin_secrets_required',
          message: 'ledger needs a key.',
          secrets: [{ name: 'VENDOR_KEY', label: 'Vendor key' }],
        }),
      )
      .mockRejectedValueOnce(
        checkFailed({
          server: 'foo',
          verdict: 'missing_secrets',
          reason: 'FOO_TOKEN has no value',
          missing_secrets: ['FOO_TOKEN'],
        }),
      )
      .mockRejectedValueOnce(
        checkFailed({ server: 'foo', verdict: 'credential_rejected', reason: 'The key was rejected.' }),
      )
      .mockResolvedValueOnce(installed);
    renderWithProviders(<PluginInstallWizard onClose={vi.fn()} />);

    fireEvent.change(screen.getByPlaceholderText('https://github.com/owner/repo'), {
      target: { value: SOURCE },
    });
    fireEvent.click(screen.getByRole('button', { name: 'Install' }));
    fireEvent.change(await screen.findByLabelText(/Vendor key/), { target: { value: 'v-1' } });
    fireEvent.click(screen.getByRole('button', { name: 'Check and install' }));

    // The missing key becomes a field, said by its description, not in red.
    const foo = await screen.findByLabelText('FOO_TOKEN');
    expect(screen.getByText('foo asks for this key, though the plugin does not declare it.')).toBeInTheDocument();
    expect(foo).not.toHaveAttribute('aria-invalid');
    fireEvent.change(foo, { target: { value: 'bad-token' } });
    fireEvent.click(screen.getByRole('button', { name: 'Check and install' }));
    await waitFor(() => expect(installUrl).toHaveBeenCalledTimes(3));
    expect(installUrl.mock.calls[2][0].secrets).toEqual({
      VENDOR_KEY: 'v-1',
      FOO_TOKEN: 'bad-token',
    });

    // The retry no longer names the key missing; its field stays, and the
    // rejection is said under it rather than under the one declared key.
    const kept = await screen.findByLabelText('FOO_TOKEN');
    expect(kept).toHaveValue('bad-token');
    expect(kept).toHaveAttribute('aria-invalid', 'true');
    expect(kept).toHaveAccessibleDescription('foo: The key was rejected.');
    expect(screen.getByLabelText(/Vendor key/)).not.toHaveAttribute('aria-invalid');
    expect(screen.queryByTestId('mcp-check-failures')).not.toBeInTheDocument();

    fireEvent.change(kept, { target: { value: 'good-token' } });
    fireEvent.click(screen.getByRole('button', { name: 'Check and install' }));
    await waitFor(() => expect(installUrl).toHaveBeenCalledTimes(4));
    expect(installUrl.mock.calls[3][0].secrets).toEqual({
      VENDOR_KEY: 'v-1',
      FOO_TOKEN: 'good-token',
    });
    // Both were saved by the install itself, and the report says so.
    expect(await screen.findByRole('link', { name: 'Manage FOO_TOKEN in Secrets' })).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Manage VENDOR_KEY in Secrets' })).toBeInTheDocument();
  });

  // With nothing declared there is no plugin_secrets_required to open the
  // keys step, so the check's missing key has to open it instead.
  it('opens the keys step for it when the plugin declares none', async () => {
    const missing = () =>
      checkFailed({
        server: 'foo',
        verdict: 'missing_secrets',
        reason: 'FOO_TOKEN has no value',
        missing_secrets: ['FOO_TOKEN'],
      });
    installUrl
      .mockRejectedValueOnce(missing())
      .mockRejectedValueOnce(missing())
      .mockRejectedValueOnce(
        checkFailed({ server: 'foo', verdict: 'credential_rejected', reason: 'The key was rejected.' }),
      )
      .mockResolvedValueOnce(installed);
    renderWithProviders(<PluginInstallWizard onClose={vi.fn()} />);

    const sourceField = () => screen.getByPlaceholderText('https://github.com/owner/repo');
    fireEvent.change(sourceField(), { target: { value: SOURCE } });
    fireEvent.click(screen.getByRole('button', { name: 'Install' }));
    expect(await screen.findByLabelText('FOO_TOKEN')).toBeInTheDocument();
    expect(screen.queryByTestId('mcp-check-failures')).not.toBeInTheDocument();

    // Back returns to the source as it was sent.
    fireEvent.click(screen.getByRole('button', { name: 'Back' }));
    expect(sourceField()).toHaveValue(SOURCE);
    fireEvent.click(screen.getByRole('button', { name: 'Install' }));

    fireEvent.change(await screen.findByLabelText('FOO_TOKEN'), { target: { value: 'bad-token' } });
    fireEvent.click(screen.getByRole('button', { name: 'Check and install' }));
    await waitFor(() => expect(installUrl).toHaveBeenCalledTimes(3));
    expect(installUrl.mock.calls[2][0].secrets).toEqual({ FOO_TOKEN: 'bad-token' });

    // The retry keeps the field and says the rejection under it.
    const kept = await screen.findByLabelText('FOO_TOKEN');
    expect(kept).toHaveValue('bad-token');
    expect(kept).toHaveAccessibleDescription('foo: The key was rejected.');

    fireEvent.change(kept, { target: { value: 'good-token' } });
    fireEvent.click(screen.getByRole('button', { name: 'Check and install' }));
    await waitFor(() => expect(installUrl).toHaveBeenCalledTimes(4));
    expect(installUrl.mock.calls[3][0].secrets).toEqual({ FOO_TOKEN: 'good-token' });
    expect(await screen.findByRole('link', { name: 'Manage FOO_TOKEN in Secrets' })).toBeInTheDocument();
  });
});
