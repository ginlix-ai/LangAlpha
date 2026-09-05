import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';
import React from 'react';
import { VaultSecretPicker } from '../VaultSecretPicker';

// The picker no longer knows which vault it writes to — `createSecret` is the
// caller's, so a mock of it IS the boundary under test.
const createSecret = vi.fn();

const baseProps = {
  value: '',
  secretNames: [] as string[],
  createSecret,
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
  it('creates through the injected vault and emits the ${vault:NAME} ref (uppercased)', async () => {
    createSecret.mockResolvedValue({ name: 'MY_TOKEN' });
    const onChange = vi.fn();
    render(<VaultSecretPicker {...baseProps} onChange={onChange} />);

    // The name input force-uppercases on change; pass lowercase to prove it.
    openCreateForm('my_token', 'super-secret');
    fireEvent.click(screen.getByRole('button', { name: /create & use/i }));

    await waitFor(() => expect(createSecret).toHaveBeenCalledTimes(1));
    expect(createSecret).toHaveBeenCalledWith({ name: 'MY_TOKEN', value: 'super-secret' });
    expect(onChange).toHaveBeenCalledWith('${vault:MY_TOKEN}');
  });
});

describe('VaultSecretPicker — inline create failure', () => {
  it('surfaces the error detail and does NOT call onChange', async () => {
    createSecret.mockRejectedValue({
      response: { data: { detail: 'secret name already in use' } },
    });
    const onChange = vi.fn();
    render(<VaultSecretPicker {...baseProps} onChange={onChange} />);

    openCreateForm('DUP', 'value');
    fireEvent.click(screen.getByRole('button', { name: /create & use/i }));

    await waitFor(() =>
      expect(screen.getByText('secret name already in use')).toBeInTheDocument(),
    );
    expect(onChange).not.toHaveBeenCalled();
  });
});
