/**
 * The dashboard composer sends to Home while its scope is the Chief of Staff, the
 * default, and to the picked workspace otherwise. Only the Home send carries
 * Home's route state, which is what names Home to the chat view before the
 * row loads.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import { act } from '@testing-library/react';

import { renderHookWithProviders } from '@/test/utils';

const api = vi.hoisted(() => ({
  getHomeWorkspace: vi.fn(async () => ({ workspace_id: 'home-1', status: 'running', is_home: true })),
}));
vi.mock('@/pages/ChatAgent/utils/api/workspaces', async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  getHomeWorkspace: api.getHomeWorkspace,
}));
vi.mock('@/hooks/useWorkspaces', () => ({
  useWorkspaces: () => ({ data: { workspaces: [{ workspace_id: 'ws-1', name: 'Research', status: 'active' }], total: 1 } }),
}));
const navigate = vi.hoisted(() => vi.fn());
vi.mock('react-router', async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  useNavigate: () => navigate,
}));

import { useChatInput } from '../useChatInput';

afterEach(() => {
  navigate.mockReset();
});

const sentState = () => (navigate.mock.calls[0]?.[1] as { state: Record<string, unknown> }).state;

describe('useChatInput', () => {
  it('sends to Home under the Chief of Staff', async () => {
    const { result } = renderHookWithProviders(() => useChatInput());
    expect(result.current.scope).toBe('all');

    await act(() => result.current.handleSend('hello'));
    expect(sentState()).toMatchObject({ workspaceId: 'home-1' });
    expect(sentState()).not.toHaveProperty('workspaceStatus');
  });

  it('sends to the picked workspace under its scope', async () => {
    const { result } = renderHookWithProviders(() => useChatInput());
    act(() => result.current.setScope('workspace'));

    await act(() => result.current.handleSend('hello'));
    expect(sentState().workspaceId).toBe('ws-1');
  });
});
