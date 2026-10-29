import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';
import React from 'react';
import type { CatalogServer } from '../../../utils/api';
import { VaultSecretPicker } from '../VaultSecretPicker';

// What the connector list holds: who reads a stored key comes from it.
const catalog = vi.hoisted(() => ({
  current: { data: undefined } as { data: { servers: CatalogServer[] } | undefined },
}));
vi.mock('@/hooks/useMcpServers', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/hooks/useMcpServers')>()),
  useMcpCatalog: () => catalog.current,
  useBuiltinMcpServers: () => ({ data: undefined }),
  useBrokerages: () => ({ data: undefined }),
}));
// Every ref resolves against the account's Secrets, so the vault hooks ARE the
// boundary under test: what is stored, and the write that keeps a value.
const vault = vi.hoisted(() => ({ stored: [] as Array<{ name: string; description: string }> }));
const createSecret = vi.hoisted(() => vi.fn());
vi.mock('@/hooks/useUserVault', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/hooks/useUserVault')>()),
  useUserVaultSecrets: () => ({
    data: { secrets: vault.stored.map((s) => ({ ...s, masked_value: '••••' })) },
  }),
  useCreateUserVaultSecret: () => ({ mutateAsync: createSecret }),
  useUpdateUserVaultSecret: () => ({ mutateAsync: vi.fn() }),
}));

const baseProps = { value: '' };

beforeEach(() => {
  vi.clearAllMocks();
  catalog.current = { data: undefined };
  vault.stored = [];
});

/** The picker as the connector form mounts it, with one key stored. */
function FormPicker() {
  vault.stored = [{ name: 'ACME_KEY', description: 'Acme API' }];
  return <VaultSecretPicker {...baseProps} onChange={vi.fn()} />;
}

function openKeyMenu() {
  fireEvent.keyDown(screen.getByTestId('vault-key-menu'), { key: 'Enter' });
  return screen.findByTestId('vault-key-option-ACME_KEY');
}

describe('VaultSecretPicker', () => {
  it('takes a literal by default and offers to save it only once there is something to save', () => {
    const onChange = vi.fn();
    const { rerender } = render(<VaultSecretPicker {...baseProps} onChange={onChange} />);
    expect(screen.queryByRole('button', { name: /save to secrets/i })).not.toBeInTheDocument();
    fireEvent.change(screen.getByPlaceholderText('Value'), { target: { value: 'sk-live' } });
    expect(onChange).toHaveBeenCalledWith('sk-live');
    rerender(<VaultSecretPicker {...baseProps} value="sk-live" onChange={onChange} />);
    expect(screen.getByRole('button', { name: /save to secrets/i })).toBeInTheDocument();
  });

  it('saves a typed value under the suggested name and swaps the ref in', async () => {
    createSecret.mockResolvedValue({ name: 'FUYAO_FUND_X_API_KEY' });
    const onChange = vi.fn();
    render(
      <VaultSecretPicker
        {...baseProps}
        value="super-secret"
        suggestedName="fuyao_fund_X-api-key"
        onChange={onChange}
      />,
    );
    fireEvent.click(screen.getByRole('button', { name: /save to secrets/i }));
    expect(screen.getByPlaceholderText('SECRET_NAME')).toHaveValue('FUYAO_FUND_X_API_KEY');
    fireEvent.click(screen.getByRole('button', { name: /^save$/i }));

    await waitFor(() => expect(createSecret).toHaveBeenCalledTimes(1));
    expect(createSecret).toHaveBeenCalledWith({ name: 'FUYAO_FUND_X_API_KEY', value: 'super-secret' });
    expect(onChange).toHaveBeenCalledWith('${vault:FUYAO_FUND_X_API_KEY}');
  });

  it('surfaces a save failure and leaves the typed value in place', async () => {
    createSecret.mockRejectedValue({ response: { data: { detail: 'secret name already in use' } } });
    const onChange = vi.fn();
    render(<VaultSecretPicker {...baseProps} value="v" suggestedName="DUP" onChange={onChange} />);
    fireEvent.click(screen.getByRole('button', { name: /save to secrets/i }));
    fireEvent.click(screen.getByRole('button', { name: /^save$/i }));
    await waitFor(() => expect(screen.getByText('secret name already in use')).toBeInTheDocument());
    expect(onChange).not.toHaveBeenCalled();
  });

  it('shows a chosen ref as a chip by name and clears back to a literal field', () => {
    const onChange = vi.fn();
    vault.stored = [{ name: 'TOKEN', description: '' }];
    render(<VaultSecretPicker {...baseProps} value="${vault:TOKEN}" onChange={onChange} />);
    expect(screen.getByTestId('vault-ref-chip')).toHaveTextContent('TOKEN');
    expect(screen.queryByPlaceholderText('Value')).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: /clear/i }));
    expect(onChange).toHaveBeenCalledWith('');
  });

  it('says nothing about who reads a key until the connectors have loaded', async () => {
    // Loading, or failed with nothing cached: either way the list is unknown.
    const { unmount } = render(<FormPicker />);
    const option = await openKeyMenu();
    expect(option).toHaveTextContent('Acme API');
    expect(option).not.toHaveTextContent('Not used yet');
    unmount();

    catalog.current = { data: { servers: [] } };
    render(<FormPicker />);
    expect(await openKeyMenu()).toHaveTextContent('Acme API · Not used yet');
  });

  it('keeps the key menu with nothing stored yet, since a new key starts there', () => {
    render(<VaultSecretPicker {...baseProps} onChange={vi.fn()} />);
    expect(screen.getByRole('button', { name: /stored keys/i })).toBeInTheDocument();
  });
});
