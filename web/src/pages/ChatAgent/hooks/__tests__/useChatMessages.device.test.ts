/**
 * A send names the layout the app is in as it leaves (`ChatRequest.device`),
 * so the agent shapes the reply for that device first. It is read at send
 * time, not at render: a person who rotates or resizes between two sends
 * gets the second shaped for where they are now.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';
import { settleMountEffect } from './chatHookHarness';

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (k: string) => k }),
}));

vi.mock('@/lib/supabase', () => ({ supabase: null }));

vi.mock('../utils/threadStorage', () => ({
  getStoredThreadId: vi.fn().mockReturnValue(null),
  setStoredThreadId: vi.fn(),
  removeStoredThreadId: vi.fn(),
}));

const mobile = vi.hoisted(() => ({ value: false }));
vi.mock('@/hooks/useIsMobile', () => ({
  getIsMobileSnapshot: () => mobile.value,
  useIsMobile: () => mobile.value,
}));

vi.mock('../../utils/api', async () => (await import('./chatHookHarness')).apiMockModule());

import { fetchThreadTurns, getWorkflowStatus, replayThreadHistory, sendChatMessageStream } from '../../utils/api';
import { useChatMessages } from '../useChatMessages';

const mockSend = sendChatMessageStream as Mock;

describe('useChatMessages: the device a send names', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mobile.value = false;
    (replayThreadHistory as Mock).mockResolvedValue(undefined);
    (fetchThreadTurns as Mock).mockResolvedValue({ turns: [], retry_checkpoint_id: null });
    (getWorkflowStatus as Mock).mockResolvedValue({ can_reconnect: false, status: 'completed' });
    mockSend.mockResolvedValue({ disconnected: false });
  });

  it('is read as each message leaves', async () => {
    const { result } = renderHookWithProviders(() => useChatMessages('ws-x', 'th-x', { platform: 'web' }));
    await waitFor(() => expect(replayThreadHistory).toHaveBeenCalled());
    await settleMountEffect();

    await act(async () => {
      await result.current.handleSendMessage('on a desktop');
    });
    expect(mockSend.mock.calls[0][3]).toMatchObject({ platform: 'web', device: 'desktop' });

    mobile.value = true;
    await act(async () => {
      await result.current.handleSendMessage('now on a phone');
    });
    expect(mockSend.mock.calls[1][3]).toMatchObject({ platform: 'web', device: 'phone' });
  });
});
