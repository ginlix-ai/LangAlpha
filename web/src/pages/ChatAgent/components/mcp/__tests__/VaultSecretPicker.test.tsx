import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';
import React from 'react';
import { VaultSecretPicker } from '../VaultSecretPicker';

// The picker does not know which vault it writes to: `createSecret` is the
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

describe('VaultSecretPicker', () => {
  it('takes a literal by default and offers to save it only once there is something to save', () => {
    const onChange = vi.fn();
    const { rerender } = render(<VaultSecretPicker {...baseProps} onChange={onChange} />);
    expect(screen.queryByRole('button', { name: /save to vault/i })).not.toBeInTheDocument();
    fireEvent.change(screen.getByPlaceholderText('Value'), { target: { value: 'sk-live' } });
    expect(onChange).toHaveBeenCalledWith('sk-live');
    rerender(<VaultSecretPicker {...baseProps} value="sk-live" onChange={onChange} />);
    expect(screen.getByRole('button', { name: /save to vault/i })).toBeInTheDocument();
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
    fireEvent.click(screen.getByRole('button', { name: /save to vault/i }));
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
    fireEvent.click(screen.getByRole('button', { name: /save to vault/i }));
    fireEvent.click(screen.getByRole('button', { name: /^save$/i }));
    await waitFor(() => expect(screen.getByText('secret name already in use')).toBeInTheDocument());
    expect(onChange).not.toHaveBeenCalled();
  });

  it('shows a chosen ref as a chip by name and clears back to a literal field', () => {
    const onChange = vi.fn();
    render(<VaultSecretPicker {...baseProps} value="${vault:TOKEN}" secretNames={['TOKEN']} onChange={onChange} />);
    expect(screen.getByTestId('vault-ref-chip')).toHaveTextContent('TOKEN');
    expect(screen.queryByPlaceholderText('Value')).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: /clear/i }));
    expect(onChange).toHaveBeenCalledWith('');
  });

  it('hides the vault button when the vault is empty', () => {
    render(<VaultSecretPicker {...baseProps} onChange={vi.fn()} />);
    expect(screen.queryByRole('button', { name: /choose from vault/i })).not.toBeInTheDocument();
  });
});
