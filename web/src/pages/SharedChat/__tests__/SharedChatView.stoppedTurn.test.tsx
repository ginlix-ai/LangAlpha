/**
 * A stopped turn keeps its Stopped chip in a shared transcript.
 *
 * This view replays the public stream itself rather than going through
 * `replayHistory`, so the content-less `finish_reason: "stopped"` close has to
 * reach the handler that stamps `stopped` here too.
 */
import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, waitFor } from '@testing-library/react';

const capturedMessages: Record<string, unknown>[][] = [];

vi.mock('react-router', () => ({
  Link: ({ children }: { children?: React.ReactNode }) => <span>{children}</span>,
}));

vi.mock('../../../contexts/AuthContext', () => ({ useAuth: () => ({ isLoggedIn: false }) }));
vi.mock('../../../contexts/ThemeContext', () => ({
  useTheme: () => ({ theme: 'light' }),
}));

// MessageList is mocked, so nothing registers with the dispatch-status
// provider the view hosts above it.
vi.mock('../../ChatAgent/hooks/usePTCDispatchStatus', () => ({
  DispatchStatusProvider: ({ children }: { children: React.ReactNode }) => children,
}));

vi.mock('../../ChatAgent/components/MessageList', () => ({
  default: ({ messages }: { messages: Record<string, unknown>[] }) => {
    capturedMessages.push(messages);
    return <div data-testid="message-list" />;
  },
}));

vi.mock('../../ChatAgent/components/FilePanel', () => ({ default: () => null }));

vi.mock('../../ChatAgent/contexts/WorkspaceContext', () => ({
  WorkspaceProvider: ({ children }: { children?: React.ReactNode }) => <>{children}</>,
}));

const CHUNK = { turn_index: 0, role: 'assistant', id: 'lc_run--x', agent: 'model:abc' };

const replayEvents = [
  { event: 'user_message', turn_index: 0, role: 'user', content: 'Summarise the filing' },
  { event: 'message_chunk', ...CHUNK, content_type: 'reasoning_signal', content: 'start' },
  { event: 'message_chunk', ...CHUNK, content_type: 'reasoning', content: 'Reading the risk factors' },
  { event: 'message_chunk', ...CHUNK, content_type: 'reasoning_signal', content: 'complete' },
  { event: 'message_chunk', ...CHUNK, content_type: 'text', content: 'The filing flags three' },
  { event: 'message_chunk', ...CHUNK, finish_reason: 'stopped' },
  { event: 'replay_done' },
];

vi.mock('../api', () => ({
  replaySharedThread: vi.fn(async (_token: string, onEvent: (e: unknown) => void) => {
    replayEvents.forEach(onEvent);
  }),
  getSharedFiles: vi.fn(async () => []),
  readSharedFile: vi.fn(async () => ''),
  downloadSharedFile: vi.fn(async () => undefined),
  servedObjectUrl: vi.fn(async () => ''),
  servedBytes: vi.fn(async () => new ArrayBuffer(0)),
  sharedServePrefix: (token: string) => `/api/v1/public/shared/${token}/files/serve/`,
}));

import SharedChatView from '../SharedChatView';
import type { SharedThreadMetadata } from '../api';

const metadata = { kind: 'thread', thread_id: 't1', title: 'Shared', workspace_name: '', msg_type: 'chat', created_at: '', updated_at: '', permissions: {} } as SharedThreadMetadata;

const latest = () => capturedMessages[capturedMessages.length - 1];

describe('SharedChatView stopped turn', () => {
  beforeEach(() => {
    capturedMessages.length = 0;
  });

  it('stamps the message the stop closed', async () => {
    render(<SharedChatView shareToken="tok" metadata={metadata} />);
    await waitFor(() => expect(capturedMessages.length).toBeGreaterThan(0));

    await waitFor(() => {
      const assistant = latest().find((m) => m.role === 'assistant');
      expect(assistant?.content).toBe('The filing flags three');
      expect(assistant?.stopped).toBe(true);
    });
    const assistant = latest().find((m) => m.role === 'assistant');
    expect(assistant?.isStreaming).toBe(false);
  });
});
