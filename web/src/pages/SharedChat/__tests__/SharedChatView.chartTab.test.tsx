/**
 * A chart card on a shared link opens the symbol as a chart tab in the share's
 * own panel, prices only. The card names the owner's workspace, whose drawings
 * and events the share does not reach, so the tab is opened with none; and a
 * chart needs no file permission, so the panel opens for it even where there
 * are no files to browse, locked to the tabs it has. Market data is served
 * per account, so a signed-out reader is told to sign in and stays put; while
 * the session is still resolving the tab opens, since signed-out is not yet
 * known. Where the share permits browsing, the file listing loads for the
 * panel however it was opened, once.
 */
import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { act, fireEvent, render, screen, waitFor } from '@testing-library/react';
import type { MessageActions } from '../../ChatAgent/components/messageList/MessageActionsContext';

let captured: MessageActions | null = null;
let panelProps: Record<string, unknown> | null = null;

vi.mock('react-router', () => ({
  Link: ({ children }: { children?: React.ReactNode }) => <span>{children}</span>,
}));

vi.mock('../../../contexts/ThemeContext', () => ({ useTheme: () => ({ theme: 'light' }) }));

const { auth, toast, viewport } = vi.hoisted(() => ({
  auth: { isLoggedIn: true, isInitialized: true },
  toast: vi.fn(),
  viewport: { isMobile: false },
}));
vi.mock('../../../contexts/AuthContext', () => ({ useAuth: () => auth }));
vi.mock('@/components/ui/use-toast', () => ({ toast }));
vi.mock('@/hooks/useIsMobile', () => ({ useIsMobile: () => viewport.isMobile }));

// MessageList is mocked, so nothing registers with the dispatch-status
// provider the view hosts above it.
vi.mock('../../ChatAgent/hooks/usePTCDispatchStatus', () => ({
  DispatchStatusProvider: ({ children }: { children: React.ReactNode }) => children,
}));

vi.mock('../../ChatAgent/components/MessageList', async () => {
  const { useMessageActions } = await import(
    '../../ChatAgent/components/messageList/MessageActionsContext'
  );
  const MessageListProbe = () => {
    captured = useMessageActions();
    return <div data-testid="message-list" />;
  };
  return { default: MessageListProbe };
});

vi.mock('../../ChatAgent/components/FilePanel', () => ({
  default: (props: Record<string, unknown>) => {
    panelProps = props;
    return <div data-testid="file-panel" />;
  },
}));
vi.mock('../../ChatAgent/contexts/WorkspaceContext', () => ({
  WorkspaceProvider: ({ children }: { children?: React.ReactNode }) => <>{children}</>,
}));

const { permissions, getSharedFiles } = vi.hoisted(() => ({
  permissions: { allow_files: false, allow_download: false },
  getSharedFiles: vi.fn(async () => ({ files: ['report.md'] })),
}));

vi.mock('../api', () => ({
  replaySharedThread: vi.fn(async (_token: string, onEvent: (e: unknown) => void) => {
    onEvent({ event: 'user_message', turn_index: 0, role: 'user', content: 'chart it' });
    onEvent({ event: 'replay_done' });
  }),
  getSharedFiles,
  readSharedFile: vi.fn(async () => ''),
  resolveSharedFile: vi.fn(),
  downloadSharedFile: vi.fn(),
  servedObjectUrl: vi.fn(async () => ''),
  servedBytes: vi.fn(async () => new ArrayBuffer(0)),
  sharedServePrefix: (token: string) => `/api/v1/public/shared/${token}/files/serve/`,
}));

import SharedChatView from '../SharedChatView';
import type { SharedThreadMetadata } from '../api';

const metadata = { kind: 'thread', thread_id: 't1', title: 'Shared', workspace_name: '', msg_type: 'chat', created_at: '', updated_at: '', permissions } as SharedThreadMetadata;

async function mount(): Promise<MessageActions> {
  render(<SharedChatView shareToken="tok" metadata={metadata} />);
  await waitFor(() => expect(captured).not.toBeNull());
  return captured!;
}

describe('SharedChatView chart cards', () => {
  beforeEach(() => {
    captured = null;
    panelProps = null;
    permissions.allow_files = false;
    auth.isLoggedIn = true;
    auth.isInitialized = true;
    viewport.isMobile = false;
    toast.mockClear();
    getSharedFiles.mockClear();
  });

  it('opens a chart tab with nothing of the owner\'s workspace, even with no files to browse', async () => {
    const actions = await mount();
    expect(panelProps).toBeNull();

    act(() => actions.onOpenChart!({ symbol: 'NVDA', timeframe: '1hour', workspaceId: 'ws-owner' }));

    await waitFor(() => expect(panelProps).not.toBeNull());
    expect(panelProps!.target).toEqual({ kind: 'chart', symbol: 'NVDA', timeframe: '1hour', seq: 1 });
    expect(panelProps!.workspaceId).toBe('');
    expect(panelProps!.singleFileMode).toBe(true);
    expect(panelProps!.onOpenInMarketView).toBeUndefined();
  });

  it('opens the chart beside the files where the share permits browsing', async () => {
    permissions.allow_files = true;
    const actions = await mount();

    act(() => actions.onOpenChart!({ symbol: 'NVDA' }));

    await waitFor(() => expect(panelProps).not.toBeNull());
    expect(panelProps!.target).toMatchObject({ kind: 'chart', symbol: 'NVDA' });
    expect(panelProps!.singleFileMode).toBe(false);
  });

  it('asks a signed-out reader to sign in rather than open a chart that cannot load', async () => {
    auth.isLoggedIn = false;
    const actions = await mount();

    act(() => actions.onOpenChart!({ symbol: 'NVDA' }));

    expect(toast).toHaveBeenCalledTimes(1);
    expect(panelProps).toBeNull();
  });

  it('opens the chart while the session is still resolving rather than calling the reader signed out', async () => {
    auth.isLoggedIn = false;
    auth.isInitialized = false;
    const actions = await mount();

    act(() => actions.onOpenChart!({ symbol: 'NVDA' }));

    await waitFor(() => expect(panelProps).not.toBeNull());
    expect(panelProps!.target).toMatchObject({ kind: 'chart', symbol: 'NVDA' });
    expect(toast).not.toHaveBeenCalled();
  });

  it('loads the file listing once for a panel a chart card opened, and the header toggle reuses it', async () => {
    permissions.allow_files = true;
    const actions = await mount();
    expect(getSharedFiles).not.toHaveBeenCalled();

    act(() => actions.onOpenChart!({ symbol: 'NVDA' }));

    await waitFor(() => expect(panelProps?.files).toEqual(['report.md']));
    expect(panelProps!.filesLoading).toBe(false);
    expect(getSharedFiles).toHaveBeenCalledTimes(1);

    // The open panel covers the header's toggle and closes by its own copy of it.
    expect(screen.queryByTitle('Workspace Files')).toBeNull();
    expect(panelProps!.panelToggle).toBe(true);
    act(() => (panelProps!.onClose as () => void)());
    await waitFor(() => expect(document.querySelector('[data-testid="file-panel"]')).toBeNull());
    fireEvent.click(screen.getByTitle('Workspace Files'));
    await waitFor(() => expect(document.querySelector('[data-testid="file-panel"]')).not.toBeNull());
    expect(panelProps!.files).toEqual(['report.md']);
    expect(getSharedFiles).toHaveBeenCalledTimes(1);
  });

  it('never asks for a listing on a share that permits no browsing', async () => {
    const actions = await mount();
    act(() => actions.onOpenChart!({ symbol: 'NVDA' }));
    await waitFor(() => expect(panelProps).not.toBeNull());
    expect(getSharedFiles).not.toHaveBeenCalled();
  });

  it('renders the panel as a full-screen sheet on a phone, with no divider', async () => {
    viewport.isMobile = true;
    const actions = await mount();

    act(() => actions.onOpenChart!({ symbol: 'NVDA' }));

    await waitFor(() => expect(panelProps).not.toBeNull());
    expect(document.querySelector('.mobile-panel-overlay')).not.toBeNull();
    expect(document.querySelector('.chat-split-divider')).toBeNull();

    act(() => (panelProps!.onClose as () => void)());
    await waitFor(() => expect(document.querySelector('.mobile-panel-overlay')).toBeNull());
    expect(document.querySelector('[data-testid="message-list"]')).not.toBeNull();
  });

  it('keeps the divider and side column on desktop', async () => {
    const actions = await mount();
    act(() => actions.onOpenChart!({ symbol: 'NVDA' }));
    await waitFor(() => expect(panelProps).not.toBeNull());
    expect(document.querySelector('.chat-split-divider')).not.toBeNull();
    expect(document.querySelector('.mobile-panel-overlay')).toBeNull();
  });
});
