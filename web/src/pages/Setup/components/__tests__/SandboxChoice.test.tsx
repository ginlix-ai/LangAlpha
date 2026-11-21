/** The Docker card's line has to match why it is off: only one of the two reasons can be fixed by starting Docker. */
import { describe, expect, it, vi } from 'vitest';
import { fireEvent, screen, waitFor } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';
import type { SandboxDraft } from '../../hooks/useSetupDrafts';
import type { SetupStatus } from '@/api/setup';

const getSetupStatus = vi.hoisted(() => vi.fn());
vi.mock('@/api/setup', async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  getSetupStatus,
}));

import { SandboxChoice } from '../SandboxChoice';

const statusWith = (detail: string): SetupStatus => ({
  completed: false,
  needs_setup: true,
  sandbox: {
    provider: 'docker',
    provider_source: 'detected',
    options: [
      { kind: 'docker', available: false, detail },
      { kind: 'e2b', available: true, detail: null, key_set: false },
    ],
  },
  keys: [],
});

const draft = {
  card: null,
  cloudKind: 'e2b',
  groups: [],
  keyDrafts: {},
  keyMissing: false,
  error: null,
  pickCard: vi.fn(),
  pickCloudKind: vi.fn(),
  toggleGroup: vi.fn(),
  setKeyDraft: vi.fn(),
} as unknown as SandboxDraft;

const render = (detail: string) =>
  renderWithProviders(
    <SandboxChoice status={statusWith(detail)} draft={draft} labelledBy="x" />,
  );

describe('the Docker card when it cannot be picked', () => {
  it('says why, with nothing to retry, where a container cannot reach the server', () => {
    render('not_offered');
    expect(screen.getByText(/can't reach the app's server/)).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Check again' })).toBeNull();
    expect(screen.queryByRole('link', { name: 'Install Docker' })).toBeNull();
  });

  it('asks for Docker where the daemon is what is missing', () => {
    render('daemon_unreachable');
    expect(screen.getByText(/isn't answering/)).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Check again' })).toBeInTheDocument();
  });
});

describe('checking for Docker again', () => {
  it('reads the status again and says so when Docker still does not answer', async () => {
    getSetupStatus.mockResolvedValue(statusWith('daemon_unreachable'));
    render('daemon_unreachable');
    const check = screen.getByRole('button', { name: 'Check again' });
    await waitFor(() => expect(check).toBeEnabled());
    fireEvent.click(check);
    await waitFor(() => expect(getSetupStatus).toHaveBeenCalledTimes(2));
    expect(await screen.findByText(/still/i)).toBeInTheDocument();
  });
});
