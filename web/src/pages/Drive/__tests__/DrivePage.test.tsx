import { screen } from '@testing-library/react';
import { useLocation } from 'react-router';
import { describe, expect, it, vi } from 'vitest';
import { getChanges, getSpaces, listEntries } from '@/api/drive';
import { renderWithProviders } from '@/test/utils';
import Drive from '../Drive';

vi.mock('@/hooks/useIsMobile', () => ({ useIsMobile: () => true }));
vi.mock('@/api/drive', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/api/drive')>()),
  getSpaces: vi.fn(),
  listEntries: vi.fn(),
  getChanges: vi.fn(),
}));

function Where() {
  const loc = useLocation();
  return <output data-testid="where">{loc.pathname + loc.search}</output>;
}

describe('Drive page on a phone', () => {
  it('shows the spaces failing to load, with its retry, when opened on a file link', async () => {
    vi.mocked(getSpaces).mockRejectedValue(new Error('Network Error'));
    const { container } = renderWithProviders(<Drive />, { route: '/drive?file=notes%2Fq3.md' });
    expect(await screen.findByText('Could not load this. Check your connection and try again.')).toBeInTheDocument();
    // Not the file layout, which hides the folder side on a phone.
    expect(container.querySelector('.drive-shell.has-file')).toBeNull();
  });
});

describe('Drive page on a space no longer there', () => {
  it('goes to the personal space instead of loading for ever', async () => {
    vi.mocked(getSpaces).mockResolvedValue([{
      id: 'sp-personal', kind: 'personal', created_at: '2026-11-01T00:00:00Z', change_seq: 0, trash_days: 14,
      usage: { bytes: 0, entries: 0, max_bytes: 1, max_entries: 1, max_file_bytes: 1 },
    }] as never);
    vi.mocked(listEntries).mockResolvedValue([]);
    vi.mocked(getChanges).mockResolvedValue({ cursor: 0, entries: [], resync: false } as never);
    renderWithProviders(<><Drive /><Where /></>, { route: '/drive?space=sp-gone&dir=a&file=a%2Fx.md' });
    await vi.waitFor(() => expect(screen.getByTestId('where')).toHaveTextContent(/^\/drive$/));
  });
});
