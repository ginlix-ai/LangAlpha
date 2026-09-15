import { describe, it, expect, vi, beforeEach } from 'vitest';
import { screen, fireEvent, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';
import React from 'react';
import { renderWithProviders } from '@/test/utils';
import { SecretsManager, type SecretItem } from '../SecretsManager';

/**
 * The secret forms fold closed instead of popping, which means the manager has
 * to keep the draft it is no longer editing for as long as the exit animation
 * is still painting it. That draft holds the plaintext the user typed, so the
 * two halves are pinned together here: the closing form still shows its value
 * through the fold, and once the fold is gone the manager is no longer holding
 * it. Clearing at close time would break the first half; never clearing (the
 * shape before this) would break the second.
 *
 * The component is driven through its props rather than through either adapter,
 * since `SecretsManager.test.tsx` in `pages/Plugins/__tests__` already covers
 * that wiring.
 */

const ops = {
  create: vi.fn(),
  update: vi.fn(),
  del: vi.fn(),
  reveal: vi.fn(),
};

function secret(name: string): SecretItem {
  return { id: `wvs-${name}`, name, description: 'rotated quarterly', masked_value: 'tok-…9f21' };
}

function renderManager(secrets: SecretItem[] = []) {
  return renderWithProviders(
    <SecretsManager
      title="Secrets"
      secrets={secrets}
      maxSecrets={20}
      loading={false}
      emptyText="No secrets stored."
      onCreate={ops.create}
      onUpdate={ops.update}
      onDelete={ops.del}
      onReveal={ops.reveal}
    />,
  );
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

    fireEvent.click(screen.getByRole('button', { name: /add secret/i }));
    fireEvent.change(screen.getByPlaceholderText('SECRET_NAME'), { target: { value: 'FOLD_TOKEN' } });
    const value = screen.getByPlaceholderText('Secret value');
    fireEvent.change(value, { target: { value: 'typed-plaintext' } });

    // Escape backs out of the form, the same as its Cancel button.
    fireEvent.keyDown(value, { key: 'Escape' });

    // Mid-exit: the fields are still on screen, still filled, because a fold
    // closing on an empty box is the thing the retained draft prevents.
    expect(screen.getByPlaceholderText('Secret value')).toHaveValue('typed-plaintext');

    await waitFor(() =>
      expect(screen.queryByPlaceholderText('Secret value')).not.toBeInTheDocument(),
    );
    expect(screen.queryByDisplayValue('typed-plaintext')).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: /add secret/i }));
    expect(screen.getByPlaceholderText('Secret value')).toHaveValue('');
    expect(screen.getByPlaceholderText('SECRET_NAME')).toHaveValue('');
  });

  it('paints the saved row editor through its fold and re-opens it empty', async () => {
    renderManager([secret('ROW_TOKEN')]);

    fireEvent.click(screen.getByTitle('Edit'));
    fireEvent.change(screen.getByPlaceholderText('New value (leave empty to keep current)'), {
      target: { value: 'rotated-plaintext' },
    });
    fireEvent.click(screen.getByRole('button', { name: /^update$/i }));

    await waitFor(() =>
      expect(ops.update).toHaveBeenCalledWith('ROW_TOKEN', {
        value: 'rotated-plaintext',
        description: 'rotated quarterly',
      }),
    );
    await waitFor(() =>
      expect(
        screen.queryByPlaceholderText('New value (leave empty to keep current)'),
      ).not.toBeInTheDocument(),
    );
    expect(screen.queryByDisplayValue('rotated-plaintext')).not.toBeInTheDocument();

    fireEvent.click(screen.getByTitle('Edit'));
    expect(screen.getByPlaceholderText('New value (leave empty to keep current)')).toHaveValue('');
  });

  it('paints a row that folds shut as another opens, and re-opens it empty', async () => {
    renderManager([secret('FIRST_TOKEN'), secret('SECOND_TOKEN')]);
    const newValue = 'New value (leave empty to keep current)';

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
