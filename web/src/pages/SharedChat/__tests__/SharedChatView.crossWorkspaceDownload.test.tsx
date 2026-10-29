/**
 * A shared Chief of Staff transcript relays worker deliverables, and it
 * qualifies those links with the workspace that holds them. The share token
 * authorizes this thread's workspace alone, so a card naming another one has
 * no bytes to save here: resolving its name against the shared workspace would
 * look it up in the wrong place and save whatever namesake it found.
 */
import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { act, render, screen, waitFor } from '@testing-library/react';
import type { MessageActions } from '../../ChatAgent/components/messageList/MessageActionsContext';
import type { PanelTarget } from '../../ChatAgent/components/FilePanel';

let captured: MessageActions | null = null;

vi.mock('react-router', () => ({
  Link: ({ children }: { children?: React.ReactNode }) => <span>{children}</span>,
}));

vi.mock('../../../contexts/AuthContext', () => ({ useAuth: () => ({ isLoggedIn: false }) }));
vi.mock('../../../contexts/ThemeContext', () => ({ useTheme: () => ({ theme: 'light' }) }));

// MessageList is mocked, so nothing registers with the dispatch-status
// provider the view hosts above it.
vi.mock('../../ChatAgent/hooks/usePTCDispatchStatus', () => ({
  DispatchStatusProvider: ({ children }: { children: React.ReactNode }) => children,
}));

vi.mock('../../ChatAgent/components/MessageList', async () => {
  const { useMessageActions } = await import(
    '../../ChatAgent/components/messageList/MessageActionsContext'
  );
  // Named and capitalized so `rules-of-hooks` reads it as the component it is.
  const MessageListProbe = () => {
    captured = useMessageActions();
    return <div data-testid="message-list" />;
  };
  return { default: MessageListProbe };
});

// Shows what the panel was asked to open, which is where an unplaceable
// card's click lands.
vi.mock('../../ChatAgent/components/FilePanel', () => ({
  default: ({ target }: { target: PanelTarget | null }) => (
    <div data-testid="file-panel" data-path={target?.kind === 'file' ? target.path ?? '' : ''} />
  ),
}));
vi.mock('../../ChatAgent/contexts/WorkspaceContext', () => ({
  WorkspaceProvider: ({ children }: { children?: React.ReactNode }) => <>{children}</>,
}));

// Hoisted with the `vi.mock` factory below, which runs before this module's
// own top-level statements.
const { resolveSharedFile, downloadSharedFile } = vi.hoisted(() => ({
  resolveSharedFile: vi.fn(async () => ({
    status: 'resolved',
    path: 'results/report.md',
    matches: ['results/report.md'],
  })),
  downloadSharedFile: vi.fn(async () => undefined),
}));

vi.mock('../api', () => ({
  replaySharedThread: vi.fn(async (_token: string, onEvent: (e: unknown) => void) => {
    onEvent({ event: 'user_message', turn_index: 0, role: 'user', content: 'build it' });
    onEvent({ event: 'replay_done' });
  }),
  getSharedFiles: vi.fn(async () => ({ files: [] })),
  readSharedFile: vi.fn(async () => ''),
  resolveSharedFile,
  downloadSharedFile,
  servedObjectUrl: vi.fn(async () => ''),
  servedBytes: vi.fn(async () => new ArrayBuffer(0)),
  sharedServePrefix: (token: string) => `/api/v1/public/shared/${token}/files/serve/`,
}));

import SharedChatView from '../SharedChatView';
import type { SharedThreadMetadata } from '../api';

const metadata = { kind: 'thread', thread_id: 't1', title: 'Shared', workspace_name: '', msg_type: 'chat', created_at: '', updated_at: '', permissions: { allow_files: true, allow_download: true } } as SharedThreadMetadata;

async function actions(): Promise<MessageActions> {
  render(<SharedChatView shareToken="tok" metadata={metadata} />);
  await waitFor(() => expect(captured).not.toBeNull());
  return captured!;
}

describe('SharedChatView deliverable download', () => {
  beforeEach(() => {
    captured = null;
    resolveSharedFile.mockClear();
    downloadSharedFile.mockClear();
  });

  // `onDownloadFile` is typed to return nothing, so each case waits on what
  // the click did rather than on the handler.
  it('does not resolve or save a card that names another workspace', async () => {
    const { onDownloadFile } = await actions();
    act(() => { onDownloadFile!('results/report.md', 'other-workspace-id'); });
    expect(await screen.findByTestId('file-panel')).toHaveAttribute('data-path', 'results/report.md');
    expect(resolveSharedFile).not.toHaveBeenCalled();
    expect(downloadSharedFile).not.toHaveBeenCalled();
  });

  it('saves a card from this thread s own workspace', async () => {
    // The control that makes the line above mean something: the same handler,
    // the same path, no qualifier, and the save goes through.
    const { onDownloadFile } = await actions();
    act(() => { onDownloadFile!('results/report.md'); });
    await waitFor(() => expect(downloadSharedFile).toHaveBeenCalledWith('tok', 'results/report.md'));
    expect(resolveSharedFile).toHaveBeenCalledWith('tok', ['results/report.md'], []);
    expect(screen.queryByTestId('file-panel')).not.toBeInTheDocument();
  });
});
