/**
 * A key set in the server's environment wins over anything saved here, so the
 * field offers nothing to change. A saved one shows masked, never in full. A
 * test's verdict reads in the reader's language where the server's sentence
 * is one the app knows.
 */
import { describe, expect, it, vi } from 'vitest';
import { fireEvent, screen } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';
import type { SetupKey } from '@/api/setup';

const { post } = vi.hoisted(() => ({ post: vi.fn() }));
vi.mock('@/api/client', () => ({ api: { post, put: vi.fn() } }));

import { KeyField } from '../KeyField';

const key = (overrides: Partial<SetupKey>): SetupKey => ({
  name: 'TAVILY_API_KEY',
  group: 'search',
  provider: 'tavily',
  state: 'unset',
  ...overrides,
});

const field = (setupKey: SetupKey, draft = '') =>
  renderWithProviders(<KeyField setupKey={setupKey} draft={draft} onDraft={vi.fn()} />);

describe('KeyField', () => {
  it('takes a key when none is held, with Test waiting on one', () => {
    field(key({}));
    expect(screen.getByLabelText('Tavily')).toHaveAttribute('type', 'password');
    expect(screen.getByRole('button', { name: 'Test' })).toBeDisabled();
  });

  it('shows a saved key masked, to replace or remove', () => {
    field(key({ state: 'saved', masked: 'tvly...a1b2' }));
    expect(screen.getByText('tvly...a1b2')).toBeInTheDocument();
    expect(screen.queryByRole('textbox')).toBeNull();
    expect(screen.getByRole('button', { name: 'Replace' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Remove' })).toBeInTheDocument();
  });

  it('offers nothing to change on a key set on the server', () => {
    field(key({ state: 'environment' }));
    expect(screen.getByText('Set on the server, so it is read-only here.')).toBeInTheDocument();
    expect(screen.queryByRole('button')).toBeNull();
    expect(screen.queryByRole('link')).toBeNull();
    expect(screen.queryByLabelText('Tavily')).toBeNull();
  });

  it("tests the key typed and says the server's verdict in the reader's words", async () => {
    post.mockResolvedValueOnce({ data: { ok: false, error: 'The key was refused' } });
    field(key({}), ' tvly-123 ');
    fireEvent.click(screen.getByRole('button', { name: 'Test' }));
    expect(await screen.findByText('The key was refused.')).toBeInTheDocument();
    expect(post).toHaveBeenCalledWith('/api/v1/setup/keys/test', { name: 'TAVILY_API_KEY', value: 'tvly-123' });
  });

  it('keeps a verdict it does not know in the server\'s own words', async () => {
    post.mockResolvedValueOnce({ data: { ok: false, error: 'Something new went wrong' } });
    field(key({}), 'tvly-123');
    fireEvent.click(screen.getByRole('button', { name: 'Test' }));
    expect(await screen.findByText('Something new went wrong')).toBeInTheDocument();
  });
});
