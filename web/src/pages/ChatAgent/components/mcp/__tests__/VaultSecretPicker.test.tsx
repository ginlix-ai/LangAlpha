import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';
import React from 'react';
import { VaultSecretPicker } from '../VaultSecretPicker';

// Stub the api module so inline-create hits a controllable mock, not a backend.
vi.mock('../../../utils/api', async (importOriginal) => {
  const actual = await importOriginal<Record<string, unknown>>();
  return { ...actual, createVaultSecret: vi.fn() };
});

import { createVaultSecret } from '../../../utils/api';

const baseProps = {
  workspaceId: 'ws-1',
  value: '',
  secretNames: [] as string[],
};

beforeEach(() => {
  vi.clearAllMocks();
});

/** Open the inline-create form and fill name + value. */
function openCreateForm(name: string, value: string) {
  fireEvent.click(screen.getByRole('button', { name: /new secret/i }));
  fireEvent.change(screen.getByPlaceholderText('SECRET_NAME'), { target: { value: name } });
  fireEvent.change(screen.getByPlaceholderText('Secret value'), { target: { value } });
}

describe('VaultSecretPicker — inline create success', () => {
  it('emits the ${vault:NAME} ref (uppercased) and fires onSecretCreated', async () => {
    (createVaultSecret as Mock).mockResolvedValue({ name: 'MY_TOKEN' });
    const onChange = vi.fn();
    const onSecretCreated = vi.fn();
    render(
      <VaultSecretPicker
        {...baseProps}
        onChange={onChange}
        onSecretCreated={onSecretCreated}
      />,
    );

    // The name input force-uppercases on change; pass lowercase to prove it.
    openCreateForm('my_token', 'super-secret');
    fireEvent.click(screen.getByRole('button', { name: /create & use/i }));

    await waitFor(() => expect(createVaultSecret).toHaveBeenCalledTimes(1));
    expect(createVaultSecret).toHaveBeenCalledWith('ws-1', {
      name: 'MY_TOKEN',
      value: 'super-secret',
    });
    expect(onChange).toHaveBeenCalledWith('${vault:MY_TOKEN}');
    expect(onSecretCreated).toHaveBeenCalledWith('MY_TOKEN');
  });
});

describe('VaultSecretPicker — inline create failure', () => {
  it('surfaces the error detail and does NOT call onChange', async () => {
    (createVaultSecret as Mock).mockRejectedValue({
      response: { data: { detail: 'secret name already in use' } },
    });
    const onChange = vi.fn();
    const onSecretCreated = vi.fn();
    render(
      <VaultSecretPicker
        {...baseProps}
        onChange={onChange}
        onSecretCreated={onSecretCreated}
      />,
    );

    openCreateForm('DUP', 'value');
    fireEvent.click(screen.getByRole('button', { name: /create & use/i }));

    await waitFor(() =>
      expect(screen.getByText('secret name already in use')).toBeInTheDocument(),
    );
    expect(onChange).not.toHaveBeenCalled();
    expect(onSecretCreated).not.toHaveBeenCalled();
  });
});
