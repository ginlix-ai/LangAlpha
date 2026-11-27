/**
 * Send now: offered only on the reader's own message still waiting in a
 * running turn or task, and settled once asked for until the delivery clears
 * the bubble.
 */
import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { MessageBubble } from '../MessageBubble';
import { MessageActionsProvider, type MessageActions } from '../MessageActionsContext';
import type { MessageRecord } from '../types';

vi.mock('@/hooks/useUser', () => ({
  useUser: () => ({ user: null }),
}));

vi.mock('@/contexts/ThemeContext', () => ({
  useTheme: () => ({ theme: 'dark' }),
}));

function steer(overrides: Partial<Record<string, unknown>> = {}): MessageRecord {
  return {
    id: 'm-1',
    role: 'user',
    content: 'Stop and check the 10-K first',
    contentType: 'text',
    timestamp: new Date('2026-08-01T00:00:00Z'),
    isStreaming: false,
    steering: true,
    steerAccepted: true,
    requestKey: 'key-1',
    ...overrides,
  } as MessageRecord;
}

function renderBubble(
  message: MessageRecord,
  { onSendNow = vi.fn().mockResolvedValue(true), live = true, isSubagentView = false, held = false }: {
    onSendNow?: MessageActions['onSendNow'] | null;
    live?: boolean;
    isSubagentView?: boolean;
    held?: boolean;
  } = {},
) {
  return render(
    <MessageActionsProvider actions={onSendNow ? { onSendNow } : {}}>
      <MessageBubble message={message} turnIndex={0} isTurnTail={false} isTurnLive={live} isSubagentView={isSubagentView} held={held} />
    </MessageActionsProvider>,
  );
}

describe('Send now', () => {
  it('shows on an accepted steer in a running turn', () => {
    renderBubble(steer());
    expect(screen.getByRole('button', { name: /send now/i })).toBeTruthy();
  });

  it.each([
    ['the turn has ended', steer(), { live: false }],
    ['the steer is not yet accepted', steer({ steerAccepted: undefined }), {}],
    ['it was delivered', steer({ steering: false, steeringDelivered: true }), {}],
    ['it is parked for a compaction', steer({ queued: true }), {}],
    ['it was refused', steer({ steering: false, queueError: 'Failed to send message' }), {}],
    ['the turn is held', steer(), { held: true }],
    ['it has no request key', steer({ requestKey: undefined }), {}],
  ])('hides when %s', (_, message, opts) => {
    renderBubble(message, opts);
    expect(screen.queryByTestId('send-now')).toBeNull();
  });

  it('hides where the host offers none', () => {
    renderBubble(steer(), { onSendNow: null });
    expect(screen.queryByTestId('send-now')).toBeNull();
  });

  it('names a task instruction by its input id', () => {
    const onSendNow = vi.fn().mockResolvedValue(true);
    renderBubble(steer({ steering: undefined, requestKey: undefined, isPending: true, inputId: 'in-1' }), { onSendNow, isSubagentView: true });
    fireEvent.click(screen.getByTestId('send-now'));
    expect(onSendNow).toHaveBeenCalledWith('in-1');
  });

  it('waits for the server to hold a task instruction', () => {
    renderBubble(steer({ steering: undefined, steerAccepted: undefined, requestKey: undefined, isPending: true, inputId: 'in-1' }), { isSubagentView: true });
    expect(screen.queryByTestId('send-now')).toBeNull();
  });

  it('settles as sent and is not asked twice', async () => {
    const onSendNow = vi.fn().mockResolvedValue(true);
    renderBubble(steer(), { onSendNow });
    const button = screen.getByTestId('send-now');
    fireEvent.click(button);
    expect(button.textContent).toMatch(/sending now/i);
    await waitFor(() => expect(button.textContent).toMatch(/sent now/i));
    fireEvent.click(button);
    expect(onSendNow).toHaveBeenCalledTimes(1);
    expect(onSendNow).toHaveBeenCalledWith('key-1');
  });

  it('goes away when there was nothing left to do', async () => {
    renderBubble(steer(), { onSendNow: vi.fn().mockResolvedValue(false) });
    fireEvent.click(screen.getByTestId('send-now'));
    await waitFor(() => expect(screen.queryByTestId('send-now')).toBeNull());
  });

  it('comes back after a failure', async () => {
    const onSendNow = vi.fn().mockRejectedValueOnce(new Error('Network Error')).mockResolvedValue(true);
    renderBubble(steer(), { onSendNow });
    const button = screen.getByTestId('send-now');
    fireEvent.click(button);
    await waitFor(() => expect(button.textContent).toMatch(/^send now$/i));
    fireEvent.click(button);
    await waitFor(() => expect(button.textContent).toMatch(/sent now/i));
    expect(onSendNow).toHaveBeenCalledTimes(2);
  });
});
