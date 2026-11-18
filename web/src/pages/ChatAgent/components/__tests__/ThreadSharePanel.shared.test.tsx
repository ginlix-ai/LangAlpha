/**
 * The switch's request in a shared workspace: turning a link on asks for the
 * members' one, since the server refuses any other, and a member the
 * managers hold cannot turn it at all. What the panel says is
 * `linkStanding`'s, pinned in its own test.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';

import { renderWithProviders } from '@/test/utils';

const shared = vi.hoisted(() => ({
  value: true,
  out: null as null | { rule: 'managers' | 'members' | 'none' | null; canShareOut: boolean; canChangeLinks: boolean; people: number | null },
}));
vi.mock('@/hooks/useSharedWorkspaces', () => ({
  useShareOut: () => (shared.value ? shared.out ?? { rule: 'members', canShareOut: true, canChangeLinks: true, people: null } : null),
}));
// The account is read for real, from a member of Northfield with no rules.
vi.mock('@/hooks/useUser', () => ({
  useUser: () => ({ user: { user_id: 'u-1', organization: { id: 'org-1', name: 'Northfield' }, policy: null } }),
}));
vi.mock('../../hooks/useThreadShareStatus', () => ({
  useThreadShareStatus: () => ({
    data: { is_shared: false, permissions: { allow_files: false, allow_download: false }, audiences: shared.value ? ['organization'] : ['public', 'organization'] },
    isPending: false,
    isError: false,
    refetch: vi.fn(),
  }),
  saveThreadSharing: vi.fn(async () => undefined),
}));

import { saveThreadSharing } from '../../hooks/useThreadShareStatus';
import { ThreadSharePanel } from '../ThreadSharePanel';

const mockSave = saveThreadSharing as Mock;

describe('ThreadSharePanel in a shared workspace', () => {
  beforeEach(() => {
    mockSave.mockClear();
    shared.out = null;
  });

  it("asks for the members' link when turning one on", async () => {
    shared.value = true;
    renderWithProviders(<ThreadSharePanel threadId="th-1" workspaceId="ws-shared" />);
    await userEvent.click(screen.getByRole('switch', { name: 'Share conversation' }));
    expect(mockSave).toHaveBeenCalledTimes(1);
    expect(mockSave.mock.calls[0][2]).toEqual({
      is_shared: true,
      permissions: { allow_files: false, allow_download: false, audience: 'organization' },
    });
  });

  it('leaves a personal workspace link as its rule has it', async () => {
    shared.value = false;
    renderWithProviders(<ThreadSharePanel threadId="th-1" workspaceId="ws-own" />);

    await userEvent.click(screen.getByRole('switch', { name: 'Share conversation' }));
    expect(mockSave.mock.calls[0][2]).toEqual({
      is_shared: true,
      permissions: { allow_files: false, allow_download: false },
    });
  });

  it("holds a member's switch where only managers share out", async () => {
    shared.value = true;
    shared.out = { rule: 'managers', canShareOut: false, canChangeLinks: false, people: null };
    renderWithProviders(<ThreadSharePanel threadId="th-1" workspaceId="ws-shared" />);
    const toggle = screen.getByRole('switch', { name: 'Share conversation' });
    expect(toggle).toBeDisabled();
    await userEvent.click(toggle);
    expect(mockSave).not.toHaveBeenCalled();
  });
});
