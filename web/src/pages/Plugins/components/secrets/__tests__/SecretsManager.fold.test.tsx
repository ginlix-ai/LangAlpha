import { describe, it, expect, vi, beforeEach } from 'vitest';
import { act, screen, fireEvent, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';
import React from 'react';
import { renderWithProviders } from '@/test/utils';
import type { UserVaultSecret } from '@/pages/ChatAgent/utils/api';
import { SecretsManager } from '../SecretsManager';

/**
 * The secret forms fold closed instead of popping, so a closing form keeps
 * painting the draft it held for as long as the exit animation runs. That
 * draft holds the plaintext the user typed, so the two halves are pinned
 * together here: the closing form still shows its value through the fold, and
 * once the fold is gone nothing is holding it. Clearing at close time would
 * break the first half; never clearing would break the second.
 *
 * The component is driven through its props rather than through its adapter,
 * since `SecretsManager.test.tsx` in `pages/Plugins/__tests__` already covers
 * that wiring.
 */

const ops = {
  create: vi.fn(),
  update: vi.fn(),
  del: vi.fn(),
  reveal: vi.fn(),
};

function secret(name: string): UserVaultSecret {
  return {
    user_vault_secret_id: `uvs-${name}`,
    name,
    description: 'rotated quarterly',
    masked_value: 'tok-…9f21',
    created_at: '2026-01-01T00:00:00Z',
    updated_at: '2026-01-01T00:00:00Z',
  };
}

function manager(secrets: UserVaultSecret[]) {
  return (
    <SecretsManager
      secrets={secrets}
      maxSecrets={20}
      blueprints={[]}
      readers={undefined}
      deepLink={null}
      onDeepLinkConsumed={() => {}}
      onCreate={ops.create}
      onUpdate={ops.update}
      onDelete={ops.del}
      onReveal={ops.reveal}
    />
  );
}

function renderManager(secrets: UserVaultSecret[] = []) {
  return renderWithProviders(manager(secrets));
}

beforeEach(() => {
  vi.clearAllMocks();
  ops.create.mockResolvedValue({});
  ops.update.mockResolvedValue({});
  ops.del.mockResolvedValue({});
  ops.reveal.mockResolvedValue('plaintext');
});

describe('SecretsManager: what a closed fold keeps', () => {
  it('paints the cancelled add form through its fold and re-opens it empty', async () => {
    renderManager();

    fireEvent.click(screen.getByRole('button', { name: /add key/i }));
    fireEvent.change(screen.getByPlaceholderText('SECRET_NAME'), { target: { value: 'FOLD_TOKEN' } });
    const value = screen.getByPlaceholderText('Paste the key');
    fireEvent.change(value, { target: { value: 'typed-plaintext' } });

    // Escape backs out of the form, the same as its Cancel button.
    fireEvent.keyDown(value, { key: 'Escape' });

    // Mid-exit: the fields are still on screen, still filled, because a fold
    // closing on an empty box is the thing the retained draft prevents.
    expect(screen.getByPlaceholderText('Paste the key')).toHaveValue('typed-plaintext');

    await waitFor(() =>
      expect(screen.queryByPlaceholderText('Paste the key')).not.toBeInTheDocument(),
    );
    expect(screen.queryByDisplayValue('typed-plaintext')).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: /add key/i }));
    expect(screen.getByPlaceholderText('Paste the key')).toHaveValue('');
    expect(screen.getByPlaceholderText('SECRET_NAME')).toHaveValue('');
  });

  it('paints the saved row editor through its fold and re-opens it empty', async () => {
    renderManager([secret('ROW_TOKEN')]);

    fireEvent.click(screen.getByTitle('Edit'));
    fireEvent.change(screen.getByPlaceholderText('Leave empty to keep this key'), {
      target: { value: 'rotated-plaintext' },
    });
    fireEvent.click(screen.getByRole('button', { name: /^save$/i }));

    await waitFor(() =>
      expect(ops.update).toHaveBeenCalledWith('ROW_TOKEN', {
        value: 'rotated-plaintext',
        description: 'rotated quarterly',
      }),
    );
    await waitFor(() =>
      expect(
        screen.queryByPlaceholderText('Leave empty to keep this key'),
      ).not.toBeInTheDocument(),
    );
    expect(screen.queryByDisplayValue('rotated-plaintext')).not.toBeInTheDocument();

    fireEvent.click(screen.getByTitle('Edit'));
    expect(screen.getByPlaceholderText('Leave empty to keep this key')).toHaveValue('');
  });

  it('paints a row that folds shut as another opens, and re-opens it empty', async () => {
    renderManager([secret('FIRST_TOKEN'), secret('SECOND_TOKEN')]);
    const newValue = 'Leave empty to keep this key';

    const [editFirst, editSecond] = screen.getAllByTitle('Edit');
    fireEvent.click(editFirst);
    fireEvent.change(screen.getByPlaceholderText(newValue), {
      target: { value: 'first-plaintext' },
    });

    // The list folds one editor closed as it opens the other, so for a beat
    // both are on screen and the closing one still carries its value.
    fireEvent.click(editSecond);
    expect(screen.getByDisplayValue('first-plaintext')).toBeInTheDocument();
    expect(screen.getAllByPlaceholderText(newValue)).toHaveLength(2);

    await waitFor(() =>
      expect(screen.queryByDisplayValue('first-plaintext')).not.toBeInTheDocument(),
    );

    fireEvent.click(screen.getAllByTitle('Edit')[0]);
    await waitFor(() => expect(screen.getAllByPlaceholderText(newValue)).toHaveLength(1));
    expect(screen.getByPlaceholderText(newValue)).toHaveValue('');
  });
});

/**
 * A key deleted in another tab leaves the list on the next read, and the edit
 * open on it used to leave with the row: draft, refusal and all, with no word
 * of why, while the typed value stayed in memory. It becomes the add form for
 * the same name instead, carrying what was typed.
 */
describe('SecretsManager: an edit whose key was deleted elsewhere', () => {
  const NEW_VALUE = 'Leave empty to keep this key';
  const NOTE = 'This key was deleted elsewhere. Saving adds it again.';

  it('becomes the add form, value kept, when a refetch drops the key', async () => {
    const { rerender } = renderManager([secret('GONE_TOKEN'), secret('KEPT_TOKEN')]);
    fireEvent.click(screen.getAllByTitle('Edit')[0]);
    fireEvent.change(screen.getByPlaceholderText(NEW_VALUE), { target: { value: 'typed-plaintext' } });

    rerender(manager([secret('KEPT_TOKEN')]));

    expect(screen.getByPlaceholderText('SECRET_NAME')).toHaveValue('GONE_TOKEN');
    expect(screen.getByPlaceholderText('Paste the key')).toHaveValue('typed-plaintext');
    expect(screen.getByText(NOTE)).toBeInTheDocument();
    expect(screen.queryByPlaceholderText(NEW_VALUE)).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: /^save$/i }));
    await waitFor(() =>
      expect(ops.create).toHaveBeenCalledWith({
        name: 'GONE_TOKEN',
        value: 'typed-plaintext',
        description: 'rotated quarterly',
      }),
    );
  });

  it('becomes the add form on a save refused with 404, before the list catches up', async () => {
    ops.update.mockRejectedValue({ response: { status: 404, data: { detail: 'Secret not found' } } });
    renderManager([secret('GONE_TOKEN')]);
    fireEvent.click(screen.getByTitle('Edit'));
    fireEvent.change(screen.getByPlaceholderText(NEW_VALUE), { target: { value: 'typed-plaintext' } });
    fireEvent.click(screen.getByRole('button', { name: /^save$/i }));

    await waitFor(() => expect(screen.getByPlaceholderText('SECRET_NAME')).toHaveValue('GONE_TOKEN'));
    expect(screen.getByPlaceholderText('Paste the key')).toHaveValue('typed-plaintext');
    expect(screen.getByText(NOTE)).toBeInTheDocument();
    expect(screen.queryByText(/Not saved/)).not.toBeInTheDocument();
    await waitFor(() => expect(screen.queryByPlaceholderText(NEW_VALUE)).not.toBeInTheDocument());
    // The row is still listed until the refetch lands; it is not a clash.
    expect(screen.queryByText(/is already stored/)).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: /^save$/i })).toBeEnabled();
  });
});

/**
 * A save settles the form it was sent from, not one the user opened while it
 * was on the wire: closing that one would throw away what was typed into it.
 */
describe('SecretsManager: a save that lands after another form opened', () => {
  it('leaves the editor opened meanwhile open', async () => {
    let resolveUpdate!: () => void;
    ops.update.mockImplementation(() => new Promise<void>((resolve) => { resolveUpdate = resolve; }));
    renderManager([secret('FIRST_TOKEN'), secret('SECOND_TOKEN')]);

    const [editFirst, editSecond] = screen.getAllByTitle('Edit');
    fireEvent.click(editFirst);
    fireEvent.change(screen.getByPlaceholderText('Leave empty to keep this key'), {
      target: { value: 'first-plaintext' },
    });
    fireEvent.click(screen.getByRole('button', { name: /^save$/i }));
    await waitFor(() => expect(ops.update).toHaveBeenCalled());

    fireEvent.click(editSecond);
    // The second row has folded away under its editor: only the first offers Edit.
    await waitFor(() => expect(screen.getAllByTitle('Edit')).toHaveLength(1));

    await act(async () => { resolveUpdate(); });

    expect(screen.getAllByTitle('Edit')).toHaveLength(1);
    expect(screen.getByText('SECOND_TOKEN').closest('.vault-form')).not.toBeNull();
  });
});
