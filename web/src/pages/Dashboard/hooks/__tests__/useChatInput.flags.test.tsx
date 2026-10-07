/**
 * Until the flags answer, the dashboard composer shows Flash's controls. A
 * send made then waits for the flags, so it goes to the user's default rather
 * than where those controls point.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import { act } from '@testing-library/react';

import { renderHookWithProviders } from '@/test/utils';
import type { FeatureState } from '@/types/api';

const api = vi.hoisted(() => ({
  getFeatures: vi.fn(),
  getFlashWorkspace: vi.fn(async () => ({ workspace_id: 'home-1' })),
}));
vi.mock('@/api/features', async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  getFeatures: api.getFeatures,
}));
vi.mock('@/pages/ChatAgent/utils/api/workspaces', async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  getFlashWorkspace: api.getFlashWorkspace,
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
  api.getFeatures.mockReset();
  navigate.mockReset();
});

const sentTo = () => (navigate.mock.calls[0]?.[1] as { state: { workspaceId: string } }).state.workspaceId;

describe('useChatInput', () => {
  it('sends to Home when the flag arrives after the send', async () => {
    let answer: (features: FeatureState[]) => void = () => {};
    api.getFeatures.mockImplementation(() => new Promise((resolve) => { answer = resolve; }));
    const { result } = renderHookWithProviders(() => useChatInput());
    expect(result.current.composerProps).toHaveProperty('mode', 'ptc');

    let sent: Promise<void> = Promise.resolve();
    act(() => { sent = result.current.handleSend('hello'); });
    expect(navigate).not.toHaveBeenCalled();

    await act(async () => {
      answer([{ key: 'all_workspaces_agent', enabled: true } as FeatureState]);
      await sent;
    });
    expect(sentTo()).toBe('home-1');
  });

  it('sends where the Flash controls point once the flags fail', async () => {
    api.getFeatures.mockRejectedValue(new Error('offline'));
    const { result } = renderHookWithProviders(() => useChatInput());

    await act(() => result.current.handleSend('hello'));
    expect(sentTo()).toBe('ws-1');
  });
});
